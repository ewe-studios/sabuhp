package supabaiza

import (
	"context"
	"strings"
	"sync"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"
)

type Action func(ctx context.Context, to string, message *Message, pubsub PubSub)

// ActionWorkerGroupCreator exposes a method which takes a ActionWorkerConfig
// which the function can modify as it sees fit and return
// a ActionWorkerGroup which will be managed by the a ActionHub.
type ActionWorkerGroupCreator func(config ActionWorkerConfig) *ActionWorkerGroup

// ActionWorkerRequest defines a request template used by the ActionHub to
// request the creation of a worker group for a giving action.
type ActionWorkerRequest struct {
	// Err provides a means of response detailing possible error
	// that occurred during the processing of this request.
	Err chan error

	// ActionName represent the action name for this
	// request work group, which identifiers in cases
	// where the PubSubAddress has no value is the
	// topic to listen for messages on.
	ActionName string

	// PubSubTopic provides alternate means of redirecting
	// a specific unique topic to this action worker, where
	// messages for these are piped accordingly.
	PubSubTopic string

	// WorkerCreator is the creation function which will be supplied
	// the initial ActionWorkerConfig which should be populated accordingly
	// by the creator to create and return a ActionWorkerGroup which will
	// service all messages for this action.
	WorkerCreator ActionWorkerGroupCreator
}

// WorkerTemplateRegistry implements a registry of ActionWorkerRequest templates
// which predefine specific ActionWorkerRequests that define what should be
// created to handle work for such actions.
//
// It provides the ActionHub a predefined set of templates to boot up action
// worker groups based on messages for specific addresses.
type WorkerTemplateRegistry struct {
	templates map[string]ActionWorkerRequest
}

// NewWorkerTemplateRegistry returns a new instance of the WorkerTemplateRegistry.
func NewWorkerTemplateRegistry() *WorkerTemplateRegistry {
	return &WorkerTemplateRegistry{
		templates: map[string]ActionWorkerRequest{},
	}
}

func (ar *WorkerTemplateRegistry) Template(actionName string) ActionWorkerRequest {
	return ar.templates[actionName]
}

func (ar *WorkerTemplateRegistry) Has(actionName string) bool {
	var _, hasAction = ar.templates[actionName]
	return hasAction
}

func (ar *WorkerTemplateRegistry) Delete(actionName string) {
	delete(ar.templates, actionName)
}

func (ar *WorkerTemplateRegistry) Register(actionName string, template ActionWorkerRequest) {
	if template.Err != nil {
		panic("ActionWorkerRequest.Err should have no value when using the registry")
	}
	if template.WorkerCreator == nil {
		panic("ActionWorkerRequest.WorkerCreator must be provided")
	}
	if strings.TrimSpace(template.ActionName) == "" {
		panic("ActionWorkerRequest.ActionName must be provided")
	}
	ar.templates[actionName] = template
}

type EscalationHandler func(escalation Escalation, hub ActionHub)

// ActionHub is a worker pool supervisor which sits to manage
// the execution of functions deployed for processing of messages
// asynchronously, each responding with another message if needed.
type ActionHub struct {
	Pubsub         PubSub
	Logger         sabuhp.Logger
	WorkRegistry   *WorkerTemplateRegistry
	Context        context.Context
	CancelFn       context.CancelFunc
	EscalationFunc EscalationHandler

	ender              *sync.Once
	starter            *sync.Once
	actionCommands     chan ActionWorkerRequest
	escalationCommands chan Escalation
	waiter             sync.WaitGroup

	workerInstancesMutex sync.RWMutex
	workerGroupInstances map[string]*ActionWorkerGroup

	workerChannelMutex  sync.RWMutex
	workerGroupChannels map[string]Channel
	autoChannels        map[string]bool
}

// NewActionHub returns a new instance of a ActionHub.
func NewActionHub(
	ctx context.Context,
	escalationHandler EscalationHandler,
	templates *WorkerTemplateRegistry,
	pubsub PubSub,
	logger sabuhp.Logger,
) *ActionHub {
	var context, cancelFn = context.WithCancel(ctx)
	return &ActionHub{
		Context:        context,
		Logger:         logger,
		CancelFn:       cancelFn,
		Pubsub:         pubsub,
		WorkRegistry:   templates,
		EscalationFunc: escalationHandler,
	}
}

// Do registers giving action (based on actionName) with using action
// name as address with the pubsub for receiving messages.
//
// Actions generally are unique self contained pure functions describing
// special case behaviours that perform some action and can if required
// respond with other messages in return. There payloads are specific
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
func (ah *ActionHub) Do(actionName string, creator ActionWorkerGroupCreator) error {
	return ah.do(actionName, actionName, creator)
}

// do handles registration of a giving worker requests, if the actionName already exits
// and is in use (as this represents the worker group then, it will fail with an error.
func (ah *ActionHub) do(actionName string, pubsubTopic string, creator ActionWorkerGroupCreator) error {
	var req = ActionWorkerRequest{
		ActionName:    actionName,
		PubSubTopic:   pubsubTopic,
		WorkerCreator: creator,
		Err:           make(chan error),
	}

	select {
	case <-ah.Context.Done():
		return nerror.New("ActionHub has being closed")
	case ah.actionCommands <- req:
		// break and continue
		break
	}

	return <-req.Err
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
	if ah.workerGroupInstances == nil {
		ah.workerGroupInstances = map[string]*ActionWorkerGroup{}
	}
	if ah.workerGroupChannels == nil {
		ah.workerGroupChannels = map[string]Channel{}
	}
	if ah.workerGroupChannels == nil {
		ah.workerGroupChannels = map[string]Channel{}
	}
	if ah.actionCommands == nil {
		ah.actionCommands = make(chan ActionWorkerRequest)
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

	// register all auto-topics/actions
	ah.createAutoActionWorkers()

	// manage
	go ah.manage()
}

func (ah *ActionHub) manage() {
	defer ah.waiter.Done()

loopRunner:
	for {
		select {
		case <-ah.Context.Done():
			break loopRunner
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

func (ah *ActionHub) createWorkerGroup(req ActionWorkerRequest) *ActionWorkerGroup {
	var workerConfig = ActionWorkerConfig{
		Pubsub:            ah.Pubsub,
		Context:           ah.Context,
		Addr:              req.PubSubTopic,
		EscalationHandler: ah.handleEscalation,
	}

	var workerGroup = req.WorkerCreator(workerConfig)
	ah.addWorkerGroup(req.ActionName, workerGroup)

	go ah.manageWorker(workerGroup)
	return workerGroup
}

func (ah *ActionHub) createAutoActionWorker(req ActionWorkerRequest) {
	var logger = ah.Logger
	var workerChannel = ah.Pubsub.Channel(req.PubSubTopic, func(data *Message, sub PubSub) {

		// get or create worker group for request topic
		var workerGroup *ActionWorkerGroup
		if !ah.hasWorkerGroup(req.ActionName) {
			workerGroup = ah.createWorkerGroup(req)
		} else {
			workerGroup = ah.getWorkerGroup(req.ActionName)
		}

		// if it's still nil then (should be impossible)
		// but report message failure if allowed
		if workerGroup == nil {
			if data.Nack != nil {
				data.Nack <- struct{}{}
			}
			return
		}

		if err := workerGroup.HandleMessage(data); err != nil {
			logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("error", err.Error())
				event.Bool("auto_action", true)
				event.String("channel_topic", req.PubSubTopic)
				event.String("channel_action", req.ActionName)
			}))
		}
	})

	ah.addWorkerChannel(req.PubSubTopic, workerChannel)
}

func (ah *ActionHub) createActionWorker(req ActionWorkerRequest) {
	// if we have existing action already, return error of duplicate action request
	if ah.hasWorkerGroup(req.ActionName) {
		req.Err <- nerror.New("duplication action worker request, already exists")
		return
	}

	var logger = ah.Logger
	var workerGroup = ah.createWorkerGroup(req)
	var workerChannel = ah.Pubsub.Channel(req.PubSubTopic, func(data *Message, sub PubSub) {
		if err := workerGroup.HandleMessage(data); err != nil {
			logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("error", err.Error())
				event.String("channel_topic", req.PubSubTopic)
				event.String("channel_action", req.ActionName)
			}))
		}
	})

	ah.addWorkerChannel(req.PubSubTopic, workerChannel)
}

func (ah *ActionHub) manageWorker(group *ActionWorkerGroup) {
	ah.waiter.Add(1)
	group.Start()
	go func(workerGroup *ActionWorkerGroup) {
		defer ah.waiter.Done()
		workerGroup.Wait()

		// close channel if not an auto channel.
		ah.closeWorkerChannel(workerGroup.config.Addr)
	}(group)
}

func (ah *ActionHub) handleEscalation(escalation *Escalation, group *ActionWorkerGroup) {
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

func (ah *ActionHub) getWorkerGroup(actionName string) *ActionWorkerGroup {
	var workerGroup *ActionWorkerGroup
	ah.workerInstancesMutex.RLock()
	{
		workerGroup = ah.workerGroupInstances[actionName]
	}
	ah.workerInstancesMutex.RLock()
	return workerGroup
}

func (ah *ActionHub) hasWorkerGroup(actionName string) bool {
	var hasWorkerGroup bool
	ah.workerInstancesMutex.RLock()
	{
		_, hasWorkerGroup = ah.workerGroupInstances[actionName]
	}
	ah.workerInstancesMutex.RLock()
	return hasWorkerGroup
}

func (ah *ActionHub) hasWorkerChannel(channelTopic string) bool {
	var hasWorkerGroup bool
	ah.workerChannelMutex.RLock()
	{
		_, hasWorkerGroup = ah.workerGroupChannels[channelTopic]
	}
	ah.workerChannelMutex.RLock()
	return hasWorkerGroup
}

func (ah *ActionHub) addWorkerGroup(actionName string, group *ActionWorkerGroup) {
	ah.workerInstancesMutex.Lock()
	{
		ah.workerGroupInstances[actionName] = group
	}
	ah.workerInstancesMutex.Lock()
}

func (ah *ActionHub) addWorkerChannel(channelTopic string, channel Channel) {
	ah.workerChannelMutex.Lock()
	{
		ah.workerGroupChannels[channelTopic] = channel
	}
	ah.workerChannelMutex.Lock()
}

func (ah *ActionHub) closeWorkerChannel(channelTopic string) {
	var channel Channel
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
	ah.workerChannelMutex.Lock()

	if hasChannel && !isAutoChannel {
		channel.Close()
	}
}

func (ah *ActionHub) getWorkerChannel(channelTopic string) Channel {
	var channel Channel
	ah.workerChannelMutex.RLock()
	{
		channel = ah.workerGroupChannels[channelTopic]
	}
	ah.workerChannelMutex.RLock()
	return channel
}

func (ah *ActionHub) isAutoChannel(channelTopic string) bool {
	var isAuto bool
	ah.workerChannelMutex.RLock()
	{
		isAuto = ah.autoChannels[channelTopic]
	}
	ah.workerChannelMutex.RLock()
	return isAuto
}

func (ah *ActionHub) endActionWorkers() {
	var channels map[string]Channel
	var workers map[string]*ActionWorkerGroup

	// swap channel maps
	ah.workerChannelMutex.Lock()
	{
		channels = ah.workerGroupChannels
		ah.workerGroupChannels = map[string]Channel{}
	}
	ah.workerChannelMutex.Unlock()

	// swap instance maps
	ah.workerInstancesMutex.Lock()
	{
		workers = ah.workerGroupInstances
		ah.workerGroupInstances = map[string]*ActionWorkerGroup{}
	}
	ah.workerInstancesMutex.Unlock()

	// Close all worker group channels
	for _, channel := range channels {
		channel.Close()
	}

	// Stop all worker groups
	var stopGroup sync.WaitGroup
	for _, worker := range workers {
		stopGroup.Add(1)
		go func(w *ActionWorkerGroup) {
			defer stopGroup.Done()
			w.Stop()
		}(worker)
	}
	stopGroup.Wait()
}
