package supabaiza

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influx6/npkg/nerror"
)

const (
	DefaultMaxWorkers        = 10
	DefaultMessageBuffer     = 10
	DefaultMinWorkers        = 1
	DefaultIdleness          = time.Minute
	DefaultMessageAcceptWait = time.Second / 2
)

type InstanceType int

const (
	NullInstance InstanceType = iota
	SingleInstance
	ScalingInstances
	OneTimeInstance
)

type BehaviourType int

const (
	DoNothing BehaviourType = iota
	RestartAll
	RestartOne
	StopAllAndEscalate
)

type WorkerProtocol int

const (
	PanicProtocol WorkerProtocol = iota
)

type EscalationProtocol int

const (
	RestartProtocol EscalationProtocol = iota
	KillAndEscalateProtocol
)

type Escalation struct {
	// Err is the generated error with tracing data.
	Err error

	// Additional data to be attached, could be the returned value
	// of recover() for a panic protocol.
	Data interface{}

	// WorkerProtocol is the escalation protocol communicated by the worker.
	WorkerProtocol WorkerProtocol

	// GroupProtocol is the escalation protocol being used by the worker group
	// for handling the worker protocol.
	GroupProtocol EscalationProtocol

	// PendingMessages are the messages left to be processed when
	// an escalation occurred. It's only set when it's a KillAndEscalate
	// protocol.
	PendingMessages chan *Message

	// OffendingMessage is the message which caused the PanicProtocol
	// Only has a value when it's a PanicProtocol.
	OffendingMessage *Message
}

type WorkerEscalationHandler func(escalation *Escalation, wk *ActionWorkerGroup)

type ActionWorkerConfig struct {
	ActionName          string
	Addr                string
	MessageBufferSize   int
	Action              Action
	MinWorker           int
	MaxWorkers          int
	Pubsub              PubSub
	Behaviour           BehaviourType
	Instance            InstanceType
	Context             context.Context
	EscalationHandler   WorkerEscalationHandler
	MaxIdleness         time.Duration
	MessageDeliveryWait time.Duration
}

func (wc *ActionWorkerConfig) ensure() {
	if wc.ActionName == "" {
		panic("ActionWorkerConfig.ActionName must be provided")
	}
	if wc.Addr == "" {
		panic("ActionWorkerConfig.Addr must be provided")
	}
	if wc.Context == nil {
		panic("ActionWorkerConfig.Context must be provided")
	}
	if wc.Action == nil {
		panic("ActionWorkerConfig.Action must have provided")
	}
	if wc.EscalationHandler == nil {
		panic("ActionWorkerConfig.EscalationHandler must have provided")
	}

	if wc.MessageBufferSize <= 0 {
		wc.MessageBufferSize = DefaultMessageBuffer
	}
	if wc.MessageDeliveryWait <= 0 {
		wc.MessageDeliveryWait = DefaultMessageAcceptWait
	}
	if wc.MaxIdleness <= 0 {
		wc.MaxIdleness = DefaultIdleness
	}
	if wc.MinWorker <= 0 {
		wc.MinWorker = DefaultMinWorkers
	}
	if wc.MaxWorkers <= 0 {
		wc.MinWorker = DefaultMaxWorkers
	}
	if wc.Behaviour == DoNothing {
		wc.Behaviour = RestartOne
	}
	if wc.Instance == NullInstance {
		wc.Instance = ScalingInstances
	}
	if wc.Instance == SingleInstance {
		wc.MinWorker = 1
		wc.MaxWorkers = 1
	}
}

// ActionWorkerGroup embodies a small action based workgroup which at their default
// state are scaling functions for execution across their maximum allowed
// range. ActionWorkerGroup provide other settings like SingleInstance where only
// one function is allowed or OneTime instance type where for a function
// runs once and dies off.
type ActionWorkerGroup struct {
	activeWorkers     int64
	totalIdled        int64
	totalCreated      int64
	totalMessages     int64
	totalProcessed    int64
	totalEscalations  int64
	totalPanics       int64
	totalRestarts     int64
	totalKilled       int64
	availableSlots    int64
	ctxCancelFn       func()
	cancelDo          sync.Once
	starterDo         sync.Once
	context           context.Context
	config            ActionWorkerConfig
	waiter            sync.WaitGroup
	workers           sync.WaitGroup
	restartSignal     sync.WaitGroup
	addWorker         chan struct{}
	endWorker         chan struct{}
	stoppedWorker     chan struct{}
	escalationChannel chan Escalation
	jobs              chan *Message
	rm                sync.Mutex
	restarting        bool
}

func NewWorkGroup(config ActionWorkerConfig) *ActionWorkerGroup {
	config.ensure()

	var ctx, cancelFn = context.WithCancel(config.Context)

	var w ActionWorkerGroup
	w.context = ctx
	w.config = config
	w.ctxCancelFn = cancelFn
	w.addWorker = make(chan struct{})
	w.endWorker = make(chan struct{})
	w.stoppedWorker = make(chan struct{})
	w.availableSlots = int64(config.MaxWorkers)
	w.jobs = make(chan *Message, config.MessageBufferSize)
	w.escalationChannel = make(chan Escalation, config.MaxWorkers)
	return &w
}

type WorkerStat struct {
	Addr                    string
	MaxWorkers              int
	MinWorkers              int
	TotalMessageReceived    int
	TotalMessageProcessed   int
	TotalEscalations        int
	TotalPanics             int
	TotalRestarts           int
	AvailableWorkerCapacity int
	TotalCurrentWorkers     int
	TotalCreatedWorkers     int
	TotalKilledWorkers      int
	TotalIdledWorkers       int
	Instance                InstanceType
	BehaviourType           BehaviourType
}

func (w *ActionWorkerGroup) Stats() WorkerStat {
	var maxActive = atomic.LoadInt64(&w.activeWorkers)
	var maxSlots = atomic.LoadInt64(&w.availableSlots)
	var totalIdled = atomic.LoadInt64(&w.totalIdled)
	var totalCreated = atomic.LoadInt64(&w.totalCreated)
	var totalMessages = atomic.LoadInt64(&w.totalMessages)
	var totalProcessed = atomic.LoadInt64(&w.totalProcessed)
	var totalEscalations = atomic.LoadInt64(&w.totalEscalations)
	var totalPanics = atomic.LoadInt64(&w.totalPanics)
	var totalRestarts = atomic.LoadInt64(&w.totalRestarts)
	var totalKilled = atomic.LoadInt64(&w.totalKilled)

	return WorkerStat{
		Instance:                w.config.Instance,
		BehaviourType:           w.config.Behaviour,
		Addr:                    w.config.Addr,
		MaxWorkers:              w.config.MaxWorkers,
		MinWorkers:              w.config.MinWorker,
		TotalKilledWorkers:      int(totalKilled),
		TotalRestarts:           int(totalRestarts),
		TotalPanics:             int(totalPanics),
		TotalMessageReceived:    int(totalMessages),
		TotalMessageProcessed:   int(totalProcessed),
		TotalEscalations:        int(totalEscalations),
		TotalCurrentWorkers:     int(maxActive),
		AvailableWorkerCapacity: int(maxSlots),
		TotalIdledWorkers:       int(totalIdled),
		TotalCreatedWorkers:     int(totalCreated),
	}
}

func (w *ActionWorkerGroup) Ctx() context.Context {
	return w.context
}

func (w *ActionWorkerGroup) Start() {
	w.starterDo.Do(func() {
		w.startManager()
		w.bootMinWorker()
	})
}

func (w *ActionWorkerGroup) Stop() {
	w.cancelDo.Do(func() {
		w.ctxCancelFn()
	})
	w.workers.Wait()
	w.waiter.Wait()
}

// Wait block till the group is stopped or killed
func (w *ActionWorkerGroup) Wait() {
	w.workers.Wait()
	w.waiter.Wait()
}

// WaitRestart will block if there is a restart
// process occurring when it's called.
func (w *ActionWorkerGroup) WaitRestart() {
	w.restartSignal.Wait()
}

func (w *ActionWorkerGroup) HandleMessage(message *Message) error {
	// attempt to handle message, if after 2 seconds,
	// check if we still have capacity for workers
	// if so increase it by adding a new one then send.
	// else block till message is accepted.
	select {
	case <-w.context.Done():
	case w.jobs <- message:
		return nil
	case <-time.After(w.config.MessageDeliveryWait):
		break
	}

	// check capacity and increase if still available.
	// Add a new worker to handle this job then.
	var maxSlots = atomic.LoadInt64(&w.availableSlots)
	if maxSlots > 0 {
		w.addWorker <- struct{}{}
	}

	select {
	case <-w.context.Done():
		return nerror.New("failed to handle message from %q", w.config.ActionName)
	case w.jobs <- message:
		return nil
	}
}

func (w *ActionWorkerGroup) beginWork() {
	w.workers.Add(1)
	go w.doWork()
}

func (w *ActionWorkerGroup) doWork() {
	atomic.AddInt64(&w.activeWorkers, 1)
	atomic.AddInt64(&w.availableSlots, -1)

	var currentMessage *Message

	defer func() {
		// signal active and available slots.
		atomic.AddInt64(&w.activeWorkers, -1)
		atomic.AddInt64(&w.availableSlots, 1)
		atomic.AddInt64(&w.totalKilled, 1)

		// we still want to recover in-case of a panic
		var err = recover()
		if err != nil {
			atomic.AddInt64(&w.totalPanics, 1)
		}

		if w.isRestarting() {

			// signal wait group
			w.workers.Done()

			return
		}

		if err != nil {
			// send to escalation channel.
			var esc = Escalation{
				Err:              nerror.New("panic occurred"),
				OffendingMessage: currentMessage,
				WorkerProtocol:   PanicProtocol,
				Data:             err,
			}

			select {
			case <-w.context.Done():
				break
			case w.escalationChannel <- esc:
				break
			}

			// signal wait group
			w.workers.Done()

			return
		}

		// signal worker is dead
		select {
		case <-w.context.Done():
			break
		case w.stoppedWorker <- struct{}{}:
			break
		}

		// signal wait group
		w.workers.Done()
	}()

	var ctx = w.context
	var action = w.config.Action
	var pubsub = w.config.Pubsub
	var maxIdleness = w.config.MaxIdleness

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(maxIdleness):
			atomic.AddInt64(&w.totalIdled, 1)
			return
		case currentMessage = <-w.jobs:
			atomic.AddInt64(&w.totalMessages, 1)
			action(w.context, w.config.Addr, currentMessage, pubsub)
			atomic.AddInt64(&w.totalProcessed, 1)

			if w.config.Instance == OneTimeInstance {
				return
			}
		case <-w.endWorker:
			return
		}
	}
}

func (w *ActionWorkerGroup) startManager() {
	w.waiter.Add(1)
	go w.manage()
}

func (w *ActionWorkerGroup) manage() {
	defer w.waiter.Done()

	var ctx = w.context
	var behaviour = w.config.Behaviour

manageLoop:
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.addWorker:
			atomic.AddInt64(&w.totalCreated, 1)
			w.beginWork()
		case <-w.stoppedWorker:
			go w.bootMinWorker()
		case esc := <-w.escalationChannel:
			atomic.AddInt64(&w.totalEscalations, 1)

			switch behaviour {
			case RestartAll:
				atomic.AddInt64(&w.totalRestarts, 1)
				w.enterRestart()

				esc.GroupProtocol = RestartProtocol
				go w.config.EscalationHandler(&esc, w)
				break manageLoop
			case StopAllAndEscalate:
				esc.PendingMessages = w.jobs
				esc.GroupProtocol = KillAndEscalateProtocol

				w.cancelDo.Do(func() {
					w.ctxCancelFn()
					go w.config.EscalationHandler(&esc, w)
				})
				break manageLoop
			case DoNothing:
				continue manageLoop
			}
		}
	}

	w.endWorkers()
	if w.isRestarting() {
		w.restartAll()
	}
}

func (w *ActionWorkerGroup) restartAll() {
	w.workers.Wait()
	go w.startManager()
	w.endRestart()
	w.bootMinWorker()
	w.restartSignal.Done()
}

func (w *ActionWorkerGroup) isRestarting() bool {
	w.rm.Lock()
	if w.restarting {
		w.rm.Unlock()
		return true
	}
	w.rm.Unlock()
	return false
}

func (w *ActionWorkerGroup) enterRestart() {
	w.rm.Lock()
	w.restarting = true
	w.rm.Unlock()
	w.restartSignal.Add(1)
}

func (w *ActionWorkerGroup) endRestart() {
	w.rm.Lock()
	w.restarting = false
	w.rm.Unlock()
}

func (w *ActionWorkerGroup) endWorkers() {
	var maxActive = int(atomic.LoadInt64(&w.activeWorkers))
	for i := 0; i < maxActive; i++ {
		select {
		case <-w.context.Done():
			// if context was cancelled then workers would
			// not need end worker signal
			return
		case w.endWorker <- struct{}{}:
		}

	}
}

func (w *ActionWorkerGroup) bootMinWorker() {
	if w.isRestarting() {
		return
	}

	// check capacity and increase if still available.
	var currentlyActive = int(atomic.LoadInt64(&w.activeWorkers))
	var diff = w.config.MinWorker - currentlyActive

	// if diff is above 0, then this means we still have more space
	// spawn new workers.
	if diff > 0 {
		for i := 0; i < diff; i++ {
			select {
			case <-w.context.Done():
				return
			case w.addWorker <- struct{}{}:
				atomic.AddInt64(&w.totalCreated, 1)
			}
		}
	}
}

// MasterWorkerGroup implements a group of worker-group nodes where
// a master node has specific actions group along as dependent actions
// where the death of the master leads to the death of the slaves.
type MasterWorkerGroup struct {
	Master *ActionWorkerGroup
	Slaves map[string]*ActionWorkerGroup
}

func (mg *MasterWorkerGroup) AddSlave(slave *ActionWorkerGroup) {
	mg.Slaves[slave.config.Addr] = slave
}

func (mg *MasterWorkerGroup) Start() {
	mg.Master.Start()
	for _, slave := range mg.Slaves {
		slave.Start()
	}
}

func (mg *MasterWorkerGroup) Stop() {
	var waiter sync.WaitGroup

	// close the master worker group.
	waiter.Add(1)
	go func() {
		defer waiter.Done()
		mg.Master.Stop()
	}()

	// close all slave workgroup.
	for _, slave := range mg.Slaves {
		waiter.Add(1)
		go func(slaveGroup *ActionWorkerGroup) {
			defer waiter.Done()
			slaveGroup.Stop()
		}(slave)
	}

	waiter.Wait()
}
