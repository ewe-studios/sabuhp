package supabaiza

import (
	"context"
	"sync"
)

type Action func(ctx context.Context, to string, message *Message, pubsub PubSub)

type ActionRegistry struct {
	creators map[string]Action
}

func NewActionRegistry() *ActionRegistry {
	return &ActionRegistry{
		creators: map[string]Action{},
	}
}

func (ar *ActionRegistry) HasAction(actionName string) bool {
	var _, hasAction = ar.creators[actionName]
	return hasAction
}

func (ar *ActionRegistry) DeleteAction(actionName string) {
	delete(ar.creators, actionName)
}

func (ar *ActionRegistry) Register(actionName string, handler Action) {
	ar.creators[actionName] = handler
}

type EscalationHandler func(escalation Escalation, hub ActionHub)

// ActionHub is a worker pool supervisor which sits to manage
// the execution of functions deployed for processing of messages
// asynchronously, each responding with another message if needed.
type ActionHub struct {
	pubsub         PubSub
	context        context.Context
	registry       *ActionRegistry
	waiter         sync.WaitGroup
	actionCommand  chan string
	EscalationFunc EscalationHandler
}

func NewActionHub(ctx context.Context, pubsub PubSub) *ActionHub {
	var ach = ActionHub{
		context:       ctx,
		pubsub:        pubsub,
		actionCommand: make(chan string),
		registry:      NewActionRegistry(),
	}
	go ach.lunchWorkers()
	return &ach
}

func (ah *ActionHub) lunchWorkers() {
	ah.waiter.Add(1)
	defer ah.waiter.Done()

	for {
		select {
		case <-ah.context.Done():
			return
			//case job := <-ah.worker:
		}
	}
}
