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

type EscalationProtocol int

const (
	PanicProtocol EscalationProtocol = iota
	KillAndEscalateProtocol
)

type Escalation struct {
	// Err is the generated error with tracing data.
	Err error

	// Additional data to be attached, could be the returned value
	// of recover() for a panic protocol.
	Data interface{}

	// Protocol is the escalation protocol being communicated.
	Protocol EscalationProtocol

	// PendingMessages are the messages left to be processed when
	// an escalation occurred. It's only set when it's a KillAndEscalate
	// protocol.
	PendingMessages chan *Message

	// OffendingMessage is the message which caused the PanicProtocol
	// Only has a value when it's a PanicProtocol.
	OffendingMessage *Message
}

type WorkerEscalationHandler func(escalation Escalation, wk *WorkGroup)

type WorkGroupConfig struct {
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

func (wc *WorkGroupConfig) ensure() {
	if wc.Addr == "" {
		panic("WorkGroupConfig.Addr must be provided")
	}
	if wc.Context == nil {
		panic("WorkGroupConfig.Context must be provided")
	}
	if wc.Action == nil {
		panic("WorkGroupConfig.Action must have provided")
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

// WorkGroup embodies a small action based workgroup which at their default
// state are scaling functions for execution across their maximum allowed
// range. WorkGroup provide other settings like SingleInstance where only
// one function is allowed or OneTime instance type where for a function
// runs once and dies off.
type WorkGroup struct {
	activeWorkers     int64
	totalIdled        int64
	totalCreated      int64
	totalMessages     int64
	totalProcessed    int64
	totalEscalations  int64
	totalPanics       int64
	totalRestarts     int64
	availableSlots    int64
	ctxCancelFn       func()
	cancelDo          sync.Once
	starterDo         sync.Once
	context           context.Context
	config            WorkGroupConfig
	waiter            sync.WaitGroup
	workers           sync.WaitGroup
	addWorker         chan struct{}
	endWorker         chan struct{}
	stoppedWorker     chan struct{}
	escalationChannel chan Escalation
	jobs              chan *Message
	rm                sync.Mutex
	restarting        bool
}

func NewWorkGroup(config WorkGroupConfig) *WorkGroup {
	config.ensure()

	var ctx, cancelFn = context.WithCancel(config.Context)

	var w WorkGroup
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
	TotalIdledWorkers       int
	Instance                InstanceType
	BehaviourType           BehaviourType
}

func (w *WorkGroup) Stat() WorkerStat {
	var maxActive = atomic.LoadInt64(&w.activeWorkers)
	var maxSlots = atomic.LoadInt64(&w.availableSlots)
	var totalIdled = atomic.LoadInt64(&w.totalIdled)
	var totalCreated = atomic.LoadInt64(&w.totalCreated)
	var totalMessages = atomic.LoadInt64(&w.totalMessages)
	var totalProcessed = atomic.LoadInt64(&w.totalProcessed)
	var totalEscalations = atomic.LoadInt64(&w.totalEscalations)
	var totalPanics = atomic.LoadInt64(&w.totalPanics)
	var totalRestarts = atomic.LoadInt64(&w.totalRestarts)

	return WorkerStat{
		Instance:                w.config.Instance,
		BehaviourType:           w.config.Behaviour,
		Addr:                    w.config.Addr,
		MaxWorkers:              w.config.MaxWorkers,
		MinWorkers:              w.config.MinWorker,
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

func (w *WorkGroup) Start() {
	w.starterDo.Do(func() {
		go w.manage()
		w.bootMinWorker()
	})
}

func (w *WorkGroup) Stop() {
	w.cancelDo.Do(func() {
		w.ctxCancelFn()
	})
	w.workers.Wait()
	w.waiter.Wait()
}

func (w *WorkGroup) Wait() {
	w.waiter.Wait()
}

func (w *WorkGroup) HandleMessage(message *Message) error {
	if message.Ack != nil && cap(message.Ack) == 0 {
		panic("Message ack channels must have capacity of 1 atleast")
	}

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
	// add a new worker to handle this job then.
	var maxSlots = atomic.LoadInt64(&w.availableSlots)
	if maxSlots > 0 {
		w.addWorker <- struct{}{}
	}

	select {
	case <-w.context.Done():
		return nerror.New("failed to handle message")
	case w.jobs <- message:
		return nil
	}
}

func (w *WorkGroup) doWork() {
	w.workers.Add(1)
	atomic.AddInt64(&w.activeWorkers, 1)
	atomic.AddInt64(&w.availableSlots, -1)

	var currentMessage *Message

	defer func() {
		// signal active and available slots.
		atomic.AddInt64(&w.activeWorkers, -1)
		atomic.AddInt64(&w.availableSlots, 1)

		// we still want to recover in-case of a panic
		var err = recover()
		if w.isRestarting() {
			return
		}

		if err != nil {
			atomic.AddInt64(&w.totalPanics, 1)
		}

		if err != nil && w.config.Behaviour != RestartOne {
			// send to escalation channel.
			var esc = Escalation{
				Err:              nerror.New("panic occurred"),
				OffendingMessage: currentMessage,
				Protocol:         PanicProtocol,
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

			// Users must be careful here.
			if currentMessage.Ack != nil {
				currentMessage.Ack <- struct{}{}
			}

			if w.config.Instance == OneTimeInstance {
				return
			}
		case <-w.endWorker:
			return
		}
	}
}

func (w *WorkGroup) manage() {
	w.waiter.Add(1)
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
			go w.doWork()
		case <-w.stoppedWorker:
			go w.bootMinWorker()
		case esc := <-w.escalationChannel:
			atomic.AddInt64(&w.totalEscalations, 1)
			go w.config.EscalationHandler(esc, w)

			switch behaviour {
			case RestartAll:
				atomic.AddInt64(&w.totalRestarts, 1)
				w.enterRestart()
				break manageLoop
			case StopAllAndEscalate:
				w.cancelDo.Do(func() {
					w.ctxCancelFn()

					atomic.AddInt64(&w.totalEscalations, 1)
					go w.config.EscalationHandler(Escalation{
						Err:             nerror.New("ending worker and escalating"),
						PendingMessages: w.jobs,
						Protocol:        KillAndEscalateProtocol,
					}, w)
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

func (w *WorkGroup) restartAll() {
	w.workers.Wait()
	go w.manage()
	w.endRestart()
	w.bootMinWorker()
}

func (w *WorkGroup) isRestarting() bool {
	w.rm.Lock()
	if w.restarting {
		w.rm.Unlock()
		return true
	}
	w.rm.Unlock()
	return false
}

func (w *WorkGroup) enterRestart() {
	w.rm.Lock()
	w.restarting = true
	w.rm.Unlock()
}

func (w *WorkGroup) endRestart() {
	w.rm.Lock()
	w.restarting = false
	w.rm.Unlock()
}

func (w *WorkGroup) endWorkers() {
	var maxActive = int(atomic.LoadInt64(&w.activeWorkers))
	for i := 0; i < maxActive; i++ {
		w.endWorker <- struct{}{}
	}
}

func (w *WorkGroup) bootMinWorker() {
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
