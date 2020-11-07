package supabaiza

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp/pubsub"

	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"
)

var testName = "test_action"

func createWorkerConfig(ctx context.Context, action Action, buffer int, max int) ActionWorkerConfig { //nolint:lll
	return ActionWorkerConfig{
		Context:             ctx,
		MessageBufferSize:   buffer,
		Pubsub:              &noPubSub{},
		Addr:                testName,
		ActionName:          testName,
		Action:              action,
		MaxWorkers:          max,
		MessageDeliveryWait: time.Millisecond * 100,
		EscalationHandler: func(escalation *Escalation, wk *ActionWorkerGroup) {

		},
	}
}

func TestNewWorkGroup(t *testing.T) {
	var count sync.WaitGroup
	count.Add(10)
	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *sabuhp.Message, pubsub pubsub.PubSub) {
			require.NotNil(t, ctx)
			require.NotNil(t, message)
			require.NotNil(t, pubsub)
			require.NotNil(t, testName, to)
			count.Done()
		},
		3,
		3,
	)

	var group = NewWorkGroup(config)
	group.Start()

	var textPayload = []byte("Welcome to life")
	for i := 0; i < 10; i++ {
		require.NoError(t, group.HandleMessage(&sabuhp.Message{
			Topic:    "find_user",
			FromAddr: "component_1",
			Payload:  textPayload,
			Metadata: nil,
		}))
	}

	count.Wait()

	var stats = group.Stats()
	group.Stop()

	require.Equal(t, 10, stats.TotalMessageReceived)
	require.Equal(t, 10, stats.TotalMessageProcessed)
}

func TestNewWorkGroup_ExpandingWorkforce(t *testing.T) {
	var count sync.WaitGroup
	count.Add(10)

	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *sabuhp.Message, pubsub pubsub.PubSub) {
			require.NotNil(t, ctx)
			require.NotNil(t, message)
			require.NotNil(t, pubsub)
			require.NotNil(t, testName, to)

			// create 1 second delay.
			<-time.After(500 * time.Millisecond)
			count.Done()
		},
		1,
		3,
	)

	var group = NewWorkGroup(config)
	group.Start()

	<-time.After(time.Second / 3)

	var stats = group.Stats()
	require.Equal(t, 2, stats.AvailableWorkerCapacity)
	require.Equal(t, 1, stats.TotalCurrentWorkers)

	var textPayload = []byte("Welcome to life")
	for i := 0; i < 10; i++ {
		require.NoError(t, group.HandleMessage(&sabuhp.Message{
			Topic:    "find_user",
			FromAddr: "component_1",
			Payload:  textPayload,
			Metadata: nil,
		}))
	}

	count.Wait()

	var stats2 = group.Stats()
	require.Equal(t, 0, stats2.AvailableWorkerCapacity)
	require.Equal(t, 3, stats2.TotalCurrentWorkers)

	group.Stop()

	var stats3 = group.Stats()
	require.Equal(t, 10, stats3.TotalMessageReceived)
	require.Equal(t, 10, stats3.TotalMessageProcessed)
}

func TestNewWorkGroup_PanicRestartPolicy(t *testing.T) {
	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *sabuhp.Message, pubsub pubsub.PubSub) {
			panic("Killed to restart")
		},
		1,
		3,
	)

	var notify = make(chan struct{}, 1)

	config.MinWorker = 2
	config.Behaviour = RestartAll
	config.EscalationHandler = func(escalation *Escalation, wk *ActionWorkerGroup) {
		require.NotNil(t, escalation)
		require.NotNil(t, escalation.Data)
		require.NotNil(t, escalation.OffendingMessage)
		require.Equal(t, PanicProtocol, escalation.WorkerProtocol)
		require.Equal(t, RestartProtocol, escalation.GroupProtocol)

		notify <- struct{}{}
	}

	var group = NewWorkGroup(config)
	group.Start()

	<-time.After(time.Second / 2)

	var textPayload = []byte("Welcome to life")
	var msg = &sabuhp.Message{
		Topic:    "find_user",
		FromAddr: "component_1",
		Payload:  textPayload,
		Metadata: nil,
	}
	require.NoError(t, group.HandleMessage(msg))

	<-notify

	group.WaitRestart()

	group.Stop()

	var stats3 = group.Stats()
	require.Equal(t, 4, stats3.TotalKilledWorkers)
	require.Equal(t, 1, stats3.TotalEscalations)
	require.Equal(t, 1, stats3.TotalRestarts)
	require.Equal(t, 1, stats3.TotalPanics)
}

func TestNewWorkGroup_PanicStopAll(t *testing.T) {
	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *sabuhp.Message, pubsub pubsub.PubSub) {
			panic("Killed to restart")
		},
		1,
		3,
	)

	var notify = make(chan struct{}, 1)

	config.MinWorker = 2
	config.Behaviour = StopAllAndEscalate
	config.EscalationHandler = func(escalation *Escalation, wk *ActionWorkerGroup) {
		require.NotNil(t, escalation)
		require.NotNil(t, escalation.Data)
		require.NotNil(t, escalation.OffendingMessage)
		require.Equal(t, PanicProtocol, escalation.WorkerProtocol)
		require.Equal(t, KillAndEscalateProtocol, escalation.GroupProtocol)

		notify <- struct{}{}
	}

	var group = NewWorkGroup(config)
	group.Start()

	<-time.After(time.Second / 2)
	var textPayload = []byte("Welcome to life")
	require.NoError(t, group.HandleMessage(&sabuhp.Message{
		Topic:    "find_user",
		FromAddr: "component_1",
		Payload:  textPayload,
		Metadata: nil,
	}))

	<-notify

	group.Wait()

	var stats = group.Stats()
	require.Equal(t, 2, stats.TotalKilledWorkers)
	require.Equal(t, 1, stats.TotalEscalations)
	require.Equal(t, 0, stats.TotalRestarts)
	require.Equal(t, 1, stats.TotalPanics)
}
