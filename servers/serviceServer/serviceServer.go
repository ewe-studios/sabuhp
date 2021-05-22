package serviceServer

import (
	"context"
	"sync"

	"github.com/influx6/npkg/njson"

	"github.com/ewe-studios/sabuhp/actions"

	"github.com/ewe-studios/sabuhp/injectors"

	"golang.org/x/sync/errgroup"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/bus/redispub"
)

type Mod func(cs *ServiceServer)

func WithWorkerRegistry(registry *actions.WorkerTemplateRegistry) Mod {
	return func(cs *ServiceServer) {
		cs.Registry = registry
	}
}

func WithInjector(injector *injectors.Injector) Mod {
	return func(cs *ServiceServer) {
		cs.Injector = injector
	}
}

func WithCtx(this context.Context) Mod {
	return func(cs *ServiceServer) {
		cs.Ctx, cs.CancelFunc = context.WithCancel(this)
	}
}

func WithRedisPubSub(config redispub.Config) Mod {
	return func(cs *ServiceServer) {
		var redisBus, busErr = redispub.PubSub(config)
		if busErr != nil {
			panic(busErr)
		}
		cs.Bus = redisBus
	}
}

func WithRedisStreams(config redispub.Config) Mod {
	return func(cs *ServiceServer) {
		var redisBus, busErr = redispub.Stream(config)
		if busErr != nil {
			panic(busErr)
		}
		cs.Bus = redisBus
	}
}

// ServiceServer exists to provide a central connection point to the message bus
// for the client (browser, CLI app, ...etc).
// They will never host any functions or processing providers but exists to provide
// a direct and distributed (by creating horizontally scaled replicas) that allow clients
// to deliver requests into the underline pubsub bus which will deliver these to service
// servers who host nothing else but functions and processors.
type ServiceServer struct {
	initer      sync.Once
	Ctx         context.Context
	CancelFunc  context.CancelFunc
	Logger      sabuhp.Logger
	ErrGroup    *errgroup.Group
	BusRelay    *sabuhp.BusRelay
	Injector    *injectors.Injector
	Registry    *actions.WorkerTemplateRegistry
	Escalations actions.EscalationNotification
	Workers     *actions.ActionHub
	Bus         sabuhp.MessageBus
}

func New(ctx context.Context, logger sabuhp.Logger, bus sabuhp.MessageBus, mods ...Mod) *ServiceServer {
	var cs = new(ServiceServer)
	cs.Bus = bus
	cs.Logger = logger
	cs.Injector = injectors.NewInjector()
	cs.Ctx, cs.CancelFunc = context.WithCancel(ctx)
	cs.BusRelay = sabuhp.NewBusRelay(cs.Ctx, cs.Logger, cs.Bus)

	for _, mod := range mods {
		mod(cs)
	}

	return cs
}

func (c *ServiceServer) Start() {
	c.initer.Do(c.initializeComponents)

	// start web socket server
	c.Workers.Start()
	c.ErrGroup.Go(func() error {
		c.Workers.Wait()
		return nil
	})
}

func (c *ServiceServer) Wait() error {
	return c.ErrGroup.Wait()
}

func (c *ServiceServer) initializeComponents() {
	if c.Escalations == nil {
		c.Escalations = func(escalation actions.Escalation, hub *actions.ActionHub) {
			var logStack = njson.Log(c.Logger)

			var log = logStack.New().
				LDebug().
				Message("worker escalation occurred").
				Formatted("data", "%+v", escalation.Data)

			if escalation.OffendingMessage != nil {
				log.Object("offending_message", escalation.OffendingMessage)
			}

			log.Formatted("worker_protocol", "%+v", escalation.WorkerProtocol).
				Formatted("group_protocol", "%+v", escalation.GroupProtocol).
				List("stats", actions.WorkerStats(hub.Stats())).
				Int("pending_messages", len(escalation.PendingMessages)).
				End()
		}
	}

	c.Workers = actions.NewActionHub(
		c.Ctx,
		c.Escalations,
		c.Registry,
		c.Injector,
		c.Bus,
		c.BusRelay,
		c.Logger,
	)
}
