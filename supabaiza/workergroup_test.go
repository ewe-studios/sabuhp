package supabaiza_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influx6/sabuhp/supabaiza"
)

var testName = "test_action"

func createWorkerConfig(ctx context.Context, action supabaiza.Action, buffer int, max int) supabaiza.ActionWorkerConfig { //nolint:lll
	return supabaiza.ActionWorkerConfig{
		Context:             ctx,
		MessageBufferSize:   buffer,
		Pubsub:              &NoPubSub{},
		Addr:                testName,
		ActionName:          testName,
		Action:              action,
		MaxWorkers:          max,
		MessageDeliveryWait: time.Millisecond * 100,
		EscalationHandler: func(escalation *supabaiza.Escalation, wk *supabaiza.ActionWorkerGroup) {

		},
	}
}

func TestNewWorkGroup(t *testing.T) {
	var count sync.WaitGroup
	count.Add(10)
	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
			require.NotNil(t, ctx)
			require.NotNil(t, message)
			require.NotNil(t, pubsub)
			require.NotNil(t, testName, to)
			count.Done()
		},
		3,
		3,
	)

	var group = supabaiza.NewWorkGroup(config)
	group.Start()

	var textPayload = supabaiza.TextPayload("Welcome to life")
	for i := 0; i < 10; i++ {
		require.NoError(t, group.HandleMessage(&supabaiza.Message{
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
		func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
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

	var group = supabaiza.NewWorkGroup(config)
	group.Start()

	<-time.After(time.Second / 3)

	var stats = group.Stats()
	require.Equal(t, 2, stats.AvailableWorkerCapacity)
	require.Equal(t, 1, stats.TotalCurrentWorkers)

	var textPayload = supabaiza.TextPayload("Welcome to life")
	for i := 0; i < 10; i++ {
		require.NoError(t, group.HandleMessage(&supabaiza.Message{
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
		func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
			panic("Killed to restart")
		},
		1,
		3,
	)

	var notify = make(chan struct{}, 1)

	config.MinWorker = 2
	config.Behaviour = supabaiza.RestartAll
	config.EscalationHandler = func(escalation *supabaiza.Escalation, wk *supabaiza.ActionWorkerGroup) {
		require.NotNil(t, escalation)
		require.NotNil(t, escalation.Data)
		require.NotNil(t, escalation.OffendingMessage)
		require.Equal(t, supabaiza.PanicProtocol, escalation.WorkerProtocol)
		require.Equal(t, supabaiza.RestartProtocol, escalation.GroupProtocol)

		notify <- struct{}{}
	}

	var group = supabaiza.NewWorkGroup(config)
	group.Start()

	<-time.After(time.Second / 2)

	var textPayload = supabaiza.TextPayload("Welcome to life")
	var msg = &supabaiza.Message{
		Topic:    "find_user",
		FromAddr: "component_1",
		Payload:  textPayload,
		Metadata: nil,
		Nack:     make(chan struct{}, 1),
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
		func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
			panic("Killed to restart")
		},
		1,
		3,
	)

	var notify = make(chan struct{}, 1)

	config.MinWorker = 2
	config.Behaviour = supabaiza.StopAllAndEscalate
	config.EscalationHandler = func(escalation *supabaiza.Escalation, wk *supabaiza.ActionWorkerGroup) {
		require.NotNil(t, escalation)
		require.NotNil(t, escalation.Data)
		require.NotNil(t, escalation.OffendingMessage)
		require.Equal(t, supabaiza.PanicProtocol, escalation.WorkerProtocol)
		require.Equal(t, supabaiza.KillAndEscalateProtocol, escalation.GroupProtocol)

		notify <- struct{}{}
	}

	var group = supabaiza.NewWorkGroup(config)
	group.Start()

	<-time.After(time.Second / 2)
	var textPayload = supabaiza.TextPayload("Welcome to life")
	require.NoError(t, group.HandleMessage(&supabaiza.Message{
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
