package actions

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"

	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/injectors"

	"github.com/influx6/npkg/nerror"
)

type Job struct {
	To        string
	DI        *injectors.Injector
	Msg       *sabuhp.Message
	Transport sabuhp.Transport
}

type ActionFunc func(ctx context.Context, job Job)

func (a ActionFunc) Do(ctx context.Context, job Job) {
	a(ctx, job)
}

type Action interface {
	Do(ctx context.Context, j Job)
}

// WorkGroupCreator exposes a method which takes a WorkerConfig
// which the function can modify as it sees fit and return
// a WorkerGroup which will be managed by the a ActionHub.
type WorkGroupCreator func(config WorkerConfig) *WorkerGroup

type SlaveWorkerRequest struct {
	// ActionName represent the action name for this
	// request work group, which identifiers in cases
	// where the PubSubAddress has no value is the
	// topic to listen for commands on.
	ActionName string

	// WorkerCreator is the creation function which will be supplied
	// the initial WorkerConfig which should be populated accordingly
	// by the creator to create and return a WorkerGroup which will
	// service all commands for this action.
	Action Action

	// Attributes which are allowed for configuration in the WorkerGroupCreator
	// as slaves are very special, users do not have rights to decide their behaviour
	// and instance types.
	MessageBufferSize   int
	MinWorker           int
	MaxWorkers          int
	MaxIdleness         time.Duration
	MessageDeliveryWait time.Duration
}

func (wc *SlaveWorkerRequest) ensure() {
	if wc.ActionName == "" {
		panic("WorkerConfig.ActionName must be provided")
	}
	if wc.Action == nil {
		panic("WorkerConfig.Action must have provided")
	}
}

// WorkerRequest defines a request template used by the ActionHub to
// request the creation of a worker group for a giving action.
type WorkerRequest struct {
	// Err provides a means of response detailing possible error
	// that occurred during the processing of creating this worker.
	Err chan error

	// ActionName represent the action name for this
	// request work group, which identifiers in cases
	// where the PubSubAddress has no value is the
	// topic to listen for commands on.
	ActionName string

	// PubSubTopic provides alternate means of redirecting
	// a specific unique topic to this action worker, where
	// commands for these are piped accordingly.
	PubSubTopic string

	// WorkerCreator is the creation function which will be supplied
	// the initial WorkerConfig which should be populated accordingly
	// by the creator to create and return a WorkerGroup which will
	// service all commands for this action.
	WorkerCreator WorkGroupCreator

	// Slaves are personalized workers we want generated with this worker.
	// Slaves have strict naming format which will be generated automatically
	// internally.
	//
	// Name format: [MasterActionName]/slaves/[SlaveActionName]
	//
	// WARNING: Use sparingly, most of your use case are doable with shared
	// workgroups.
	//
	// There are cases where we want a worker specific for a work group
	// which can be used to hand off specific tasks to handle very
	// special cases, this then allows specific and limited use
	// around non-generalistic work group slaves.
	//
	// Example of such is a dangerous operation that is not intrinsic to
	// the behaviour of the action but must be done, this can be offloaded
	// a dangerous task or non-secure operation to the slaves who will
	// always be running and are never ever going die because they will be
	// restarted and respawned.
	// They will exists as far as the master exists.
	Slaves []SlaveWorkerRequest
}

func (wc *WorkerRequest) ensure() {
	if wc.ActionName == "" {
		panic("WorkerConfig.ActionName must be provided")
	}
	if strings.TrimSpace(wc.ActionName) == "" {
		panic("WorkerRequest.ActionName must be provided")
	}
	if wc.PubSubTopic == "" {
		panic("WorkerConfig.Mailbox must be provided")
	}
	if wc.WorkerCreator == nil {
		panic("WorkerConfig.WorkerCreator must be provided")
	}
	for _, slaveRequest := range wc.Slaves {
		slaveRequest.ensure()
	}
}

// WorkerTemplateRegistry implements a registry of WorkerRequest templates
// which predefine specific ActionWorkerRequests that define what should be
// created to handle work for such actions.
//
// It provides the ActionHub a predefined set of templates to boot up action
// worker groups based on commands for specific addresses.
type WorkerTemplateRegistry struct {
	templates map[string]WorkerRequest
}

// NewWorkerTemplateRegistry returns a new instance of the WorkerTemplateRegistry.
func NewWorkerTemplateRegistry() *WorkerTemplateRegistry {
	return &WorkerTemplateRegistry{
		templates: map[string]WorkerRequest{},
	}
}

func (ar *WorkerTemplateRegistry) Template(actionName string) WorkerRequest {
	return ar.templates[actionName]
}

func (ar *WorkerTemplateRegistry) Has(actionName string) bool {
	var _, hasAction = ar.templates[actionName]
	return hasAction
}

func (ar *WorkerTemplateRegistry) Delete(actionName string) {
	delete(ar.templates, actionName)
}

func (ar *WorkerTemplateRegistry) Register(template WorkerRequest) {
	template.ensure()
	if template.Err != nil {
		panic("WorkerConfig.Err must not be provided")
	}

	ar.templates[template.ActionName] = template
}

type EscalationNotification func(escalation Escalation, hub *ActionHub)

// ActionHub is a worker pool supervisor which sits to manage
// the execution of functions deployed for processing of commands
// asynchronously, each responding with another message if needed.
type ActionHub struct {
	Pubsub         sabuhp.Transport
	Logger         sabuhp.Logger
	Injector       *injectors.Injector
	WorkRegistry   *WorkerTemplateRegistry
	Context        context.Context
	CancelFn       context.CancelFunc
	EscalationFunc EscalationNotification

	ender              *sync.Once
	starter            *sync.Once
	actionCommands     chan WorkerRequest
	actionChannel      chan func()
	escalationCommands chan Escalation
	waiter             sync.WaitGroup

	masterGroupsMutex sync.RWMutex
	masterGroups      map[string]*MasterWorkerGroup

	workerChannelMutex  sync.RWMutex
	workerGroupChannels map[string]sabuhp.Channel
	autoChannels        map[string]bool
}

// NewActionHub returns a new instance of a ActionHub.
func NewActionHub(
	ctx context.Context,
	escalationHandler EscalationNotification,
	templates *WorkerTemplateRegistry,
	injector *injectors.Injector,
	pubsub sabuhp.Transport,
	logger sabuhp.Logger,
) *ActionHub {
	var cntx, cancelFn = context.WithCancel(ctx)
	return &ActionHub{
		Context:        cntx,
		Injector:       injector,
		Logger:         logger,
		CancelFn:       cancelFn,
		Pubsub:         pubsub,
		WorkRegistry:   templates,
		EscalationFunc: escalationHandler,
	}
}

// Do registers giving action (based on actionName) with using action
// name as address with the pubsub for receiving commands.
//
// Actions generally are unique self contained pure functions describing
// special case behaviours that perform some action and can if required
// respond with other commands in return. There payloads are specific
// and known at implementation time, so that the contract of what an
// action does are clear.
//
// Generally Actions based on configuration of their work group templates
// will scaling accordingly if possible to meet processing needs based on
// worker group constraints. Hence generally unless special cases occur
// creating multiple worker workerGroupInstances of the same action is not necessary.
//
// Do be aware the actionName is very unique and must be different from others both
// in the workers template registry as well.
func (ah *ActionHub) Do(actionName string, creator WorkGroupCreator, slaves ...SlaveWorkerRequest) error {
	return ah.do(actionName, actionName, creator, slaves...)
}

// DoAlias registers a WorkerCreator based on the action name but instead of using the action
// name as the pubsub topic, uses the value of pubsubTopic provided allowing you to link
// an action work group to an external pre-defined topic.
//
// We always encourage the actionName to be the topic because they based on configuration are able
// to scale without the need to spawn separate work groups. But there will be cases for the need
// to provide support to handle outside events that may not necessary be termed with the action name
// due to lack of control or due to integration, hence DoAlias exists for this.
//
// The general rule is when you can control the topic name, use the actionName, and if you don't
// know which to use, then its a safe bet to say: just use the action name, hence using ActionHub.Do.
func (ah *ActionHub) DoAlias(
	actionName string,
	pubsubTopic string,
	creator WorkGroupCreator,
	slaves ...SlaveWorkerRequest,
) error {
	return ah.do(actionName, pubsubTopic, creator, slaves...)
}

// do handles registration of a giving worker requests, if the actionName already exits
// and is in use (as this represents the worker group then, it will fail with an error.
func (ah *ActionHub) do(
	actionName string,
	pubsubTopic string,
	creator WorkGroupCreator,
	slaves ...SlaveWorkerRequest,
) error {
	var req = WorkerRequest{
		ActionName:    actionName,
		PubSubTopic:   pubsubTopic,
		WorkerCreator: creator,
		Err:           make(chan error),
		Slaves:        slaves,
	}

	req.ensure()

	select {
	case <-ah.Context.Done():
		return nerror.New("ActionHub has being closed")
	case ah.actionCommands <- req:
		// break and continue
		break
	}

	return <-req.Err
}

func (ah *ActionHub) Stats() []WorkerStat {
	return ah.getStats()
}

func (ah *ActionHub) Wait() {
	ah.waiter.Wait()
}

func (ah *ActionHub) Start() {
	ah.init()
	ah.starter.Do(func() {
		ah.startManagement()
	})
}

// Stop ends the action hub and ends all operation.
// This also makes this instance unusable.
func (ah *ActionHub) Stop() {
	ah.ender.Do(func() {
		ah.CancelFn()
	})
}

func (ah *ActionHub) init() {
	if ah.masterGroups == nil {
		ah.masterGroups = map[string]*MasterWorkerGroup{}
	}
	if ah.workerGroupChannels == nil {
		ah.workerGroupChannels = map[string]sabuhp.Channel{}
	}
	if ah.workerGroupChannels == nil {
		ah.workerGroupChannels = map[string]sabuhp.Channel{}
	}
	if ah.actionChannel == nil {
		ah.actionChannel = make(chan func())
	}
	if ah.actionCommands == nil {
		ah.actionCommands = make(chan WorkerRequest)
	}
	if ah.escalationCommands == nil {
		ah.escalationCommands = make(chan Escalation)
	}
	if ah.ender == nil {
		var doer sync.Once
		ah.ender = &doer
	}
	if ah.starter == nil {
		var doer sync.Once
		ah.starter = &doer
	}
}

func (ah *ActionHub) startManagement() {
	ah.waiter.Add(1)

	go func() {
		// register all auto-topics/actions
		ah.createAutoActionWorkers()

		// manage
		ah.manage()
	}()
}

func (ah *ActionHub) manage() {
	defer ah.waiter.Done()

loopRunner:
	for {
		select {
		case <-ah.Context.Done():
			break loopRunner
		case doFn := <-ah.actionChannel:
			doFn()
		case req := <-ah.actionCommands:
			ah.createActionWorker(req)
			continue loopRunner
		}
	}

	// end all worker workerGroupInstances immediately.
	ah.endActionWorkers()
}

func (ah *ActionHub) createAutoActionWorkers() {
	for _, template := range ah.WorkRegistry.templates {
		var copiedTemplate = template
		if strings.TrimSpace(copiedTemplate.PubSubTopic) == "" {
			copiedTemplate.PubSubTopic = copiedTemplate.ActionName
		}
		copiedTemplate.Err = make(chan error, 1)
		ah.createAutoActionWorker(copiedTemplate)
	}
}

func (ah *ActionHub) createAutoActionWorker(req WorkerRequest) {
	var logger = ah.Logger
	var workerChannel = ah.Pubsub.Listen(req.PubSubTopic, sabuhp.TransportResponseFunc(
		func(data *sabuhp.Message, sub sabuhp.Transport) sabuhp.MessageErr {

			// get or create worker group for request topic
			var workerGroup *MasterWorkerGroup
			if !ah.hasMasterGroup(req.ActionName) {
				workerGroup = ah.createMasterGroup(req)
			} else {
				workerGroup = ah.getMasterGroup(req.ActionName)
			}

			// if it's still nil then (should be impossible)
			// but report message failure if allowed
			if workerGroup == nil {
				return sabuhp.WrapErr(nerror.New("worker group not found"), false)
			}

			if err := workerGroup.Master.HandleMessage(data, sub); err != nil {
				logger.Log(njson.JSONB(func(event npkg.Encoder) {
					event.String("error", err.Error())
					event.Bool("auto_action", true)
					event.String("channel_topic", req.PubSubTopic)
					event.String("channel_action", req.ActionName)
				}))

				return sabuhp.WrapErr(nerror.WrapOnly(err), false)
			}
			return nil
		}))

	ah.addWorkerChannel(req.PubSubTopic, workerChannel)
	close(req.Err)
}

func (ah *ActionHub) createActionWorker(req WorkerRequest) {
	// if we have existing action already, return error of duplicate action request
	if ah.hasMasterGroup(req.ActionName) {
		req.Err <- nerror.New("duplication action worker request, already exists")
		return
	}

	var logger = ah.Logger
	var workerGroup = ah.createMasterGroup(req)
	var workerChannel = ah.Pubsub.Listen(req.PubSubTopic, sabuhp.TransportResponseFunc(func(data *sabuhp.Message, sub sabuhp.Transport) sabuhp.MessageErr {
		if err := workerGroup.Master.HandleMessage(data, sub); err != nil {
			logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("error", err.Error())
				event.String("channel_topic", req.PubSubTopic)
				event.String("channel_action", req.ActionName)
			}))
			return sabuhp.WrapErr(nerror.WrapOnly(err), false)
		}
		return nil
	}))

	ah.addWorkerChannel(req.PubSubTopic, workerChannel)
	close(req.Err)
}

func (ah *ActionHub) createSlaveForMaster(req SlaveWorkerRequest, masterGroup *MasterWorkerGroup) {
	var slaveName = fmt.Sprintf("%s/slaves/%s", masterGroup.Master.config.ActionName, req.ActionName)
	var workerConfig = WorkerConfig{
		ActionName:             slaveName,
		Addr:                   slaveName,
		Injector:               ah.Injector,
		MessageBufferSize:      req.MessageBufferSize,
		Action:                 req.Action,
		MinWorker:              req.MinWorker,
		MaxWorkers:             req.MaxWorkers,
		Behaviour:              RestartAll,
		Instance:               ScalingInstances,
		Context:                masterGroup.Master.Ctx(),
		EscalationNotification: ah.handleEscalation(masterGroup),
		MaxIdleness:            req.MaxIdleness,
		MessageDeliveryWait:    req.MessageDeliveryWait,
	}

	workerConfig.ensure()

	var logger = ah.Logger
	var slaveWorkerGroup = NewWorkGroup(workerConfig)
	masterGroup.Slaves[slaveName] = slaveWorkerGroup

	var workerChannel = ah.Pubsub.Listen(slaveName, sabuhp.TransportResponseFunc(func(data *sabuhp.Message, sub sabuhp.Transport) sabuhp.MessageErr {
		if err := slaveWorkerGroup.HandleMessage(data, sub); err != nil {
			logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("error", err.Error())
				event.String("channel_topic", slaveName)
				event.String("channel_action", req.ActionName)
			}))
			return sabuhp.WrapErr(nerror.WrapOnly(err), false)
		}
		return nil
	}))

	ah.addWorkerChannel(slaveName, workerChannel)
}

func (ah *ActionHub) createMasterGroup(req WorkerRequest) *MasterWorkerGroup {
	var masterGroup = &MasterWorkerGroup{
		Master: nil,
		Slaves: make(map[string]*WorkerGroup, len(req.Slaves)),
	}

	var workerConfig = WorkerConfig{
		ActionName:             req.ActionName,
		Context:                ah.Context,
		Addr:                   req.PubSubTopic,
		Injector:               ah.Injector,
		EscalationNotification: ah.handleEscalation(masterGroup),
	}

	// create master worker.
	var workerGroup = req.WorkerCreator(workerConfig)
	masterGroup.Master = workerGroup

	// generate slave worker groups.
	for _, slave := range req.Slaves {
		ah.createSlaveForMaster(slave, masterGroup)
	}

	// Add master group
	ah.addMasterGroup(req.ActionName, masterGroup)

	go ah.manageWorker(masterGroup)

	return masterGroup
}

func (ah *ActionHub) manageWorker(masterGroup *MasterWorkerGroup) { //nolint:unused,unused
	ah.waiter.Add(1)
	masterGroup.Start()
	go func(group *MasterWorkerGroup) {
		defer ah.waiter.Done()

		// wait till master  is stopped
		group.Master.Wait()

		// send signal to group to Stop as well.
		group.Stop()

		// remove worker group
		ah.rmMasterGroup(group.Master.config.Addr)

		// close channel if not an auto channel.
		ah.closeWorkerChannel(group.Master.config.Addr)

		// close all slave channels
		for _, slave := range group.Slaves {
			ah.closeWorkerChannel(slave.config.Addr)
		}
	}(masterGroup)
}

func (ah *ActionHub) handleMasterEscalation(
	escalation *Escalation,
	group *WorkerGroup,
	masterGroup *MasterWorkerGroup,
) {
	switch escalation.GroupProtocol {
	case RestartProtocol:
		// do nothing
	case KillAndEscalateProtocol:
		if !ah.isAutoChannel(group.config.Addr) {
			var channel = ah.getWorkerChannel(group.config.Addr)
			channel.Close()
		}
	}
}

func (ah *ActionHub) handleEscalation(masterGroup *MasterWorkerGroup) WorkerEscalationNotification {
	return func(escalation *Escalation, group *WorkerGroup) {
		ah.handleMasterEscalation(escalation, group, masterGroup)
	}
}

func (ah *ActionHub) hasMasterGroup(groupName string) bool {
	var hasMaster bool
	ah.masterGroupsMutex.RLock()
	{
		_, hasMaster = ah.masterGroups[groupName]
	}
	ah.masterGroupsMutex.RUnlock()
	return hasMaster
}

func (ah *ActionHub) getMasterGroup(actionName string) *MasterWorkerGroup {
	var group *MasterWorkerGroup
	ah.masterGroupsMutex.RLock()
	{
		group = ah.masterGroups[actionName]
	}
	ah.masterGroupsMutex.RUnlock()
	return group
}

func (ah *ActionHub) getStats() []WorkerStat {
	var stats []WorkerStat
	ah.masterGroupsMutex.RLock()
	{
		for _, worker := range ah.masterGroups {
			stats = append(stats, worker.Stats()...)
		}
	}
	ah.masterGroupsMutex.RUnlock()
	return stats
}

func (ah *ActionHub) rmMasterGroup(actionName string) {
	ah.masterGroupsMutex.RLock()
	{
		delete(ah.masterGroups, actionName)
	}
	ah.masterGroupsMutex.RUnlock()
}

func (ah *ActionHub) addMasterGroup(actionName string, group *MasterWorkerGroup) {
	ah.masterGroupsMutex.Lock()
	{
		ah.masterGroups[actionName] = group
	}
	ah.masterGroupsMutex.Unlock()
}

func (ah *ActionHub) hasWorkerChannel(channelTopic string) bool {
	var hasWorkerGroup bool
	ah.workerChannelMutex.RLock()
	{
		_, hasWorkerGroup = ah.workerGroupChannels[channelTopic]
	}
	ah.workerChannelMutex.RUnlock()
	return hasWorkerGroup
}

func (ah *ActionHub) addWorkerChannel(channelTopic string, channel sabuhp.Channel) {
	ah.workerChannelMutex.Lock()
	{
		ah.workerGroupChannels[channelTopic] = channel
	}
	ah.workerChannelMutex.Unlock()
}

func (ah *ActionHub) closeWorkerChannel(channelTopic string) {
	var channel sabuhp.Channel
	var hasChannel bool
	var isAutoChannel bool

	ah.workerChannelMutex.Lock()
	{
		isAutoChannel = ah.autoChannels[channelTopic]
		channel, hasChannel = ah.workerGroupChannels[channelTopic]

		if hasChannel && !isAutoChannel {
			delete(ah.workerGroupChannels, channelTopic)
		}
	}
	ah.workerChannelMutex.Unlock()

	if hasChannel && !isAutoChannel {
		channel.Close()
	}
}

func (ah *ActionHub) getWorkerChannel(channelTopic string) sabuhp.Channel {
	var channel sabuhp.Channel
	ah.workerChannelMutex.RLock()
	{
		channel = ah.workerGroupChannels[channelTopic]
	}
	ah.workerChannelMutex.RUnlock()
	return channel
}

func (ah *ActionHub) isAutoChannel(channelTopic string) bool {
	var isAuto bool
	ah.workerChannelMutex.RLock()
	{
		isAuto = ah.autoChannels[channelTopic]
	}
	ah.workerChannelMutex.RUnlock()
	return isAuto
}

func (ah *ActionHub) endActionWorkers() {
	var channels map[string]sabuhp.Channel
	var masterGroups map[string]*MasterWorkerGroup
	// var workers map[string]*WorkerGroup

	// swap channel maps
	ah.workerChannelMutex.Lock()
	{
		channels = ah.workerGroupChannels
		ah.workerGroupChannels = map[string]sabuhp.Channel{}
	}
	ah.workerChannelMutex.Unlock()

	// swap master groups maps
	ah.masterGroupsMutex.Lock()
	{
		masterGroups = ah.masterGroups
		ah.masterGroups = map[string]*MasterWorkerGroup{}
	}
	ah.masterGroupsMutex.Unlock()

	// Close all worker group channels
	for _, channel := range channels {
		channel.Close()
	}

	// Stop all worker groups
	var stopGroup sync.WaitGroup
	for _, worker := range masterGroups {
		stopGroup.Add(1)
		go func(w *MasterWorkerGroup) {
			defer stopGroup.Done()
			w.Stop()
		}(worker)
	}
	stopGroup.Wait()
}
