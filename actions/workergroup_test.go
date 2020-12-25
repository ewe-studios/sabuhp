package actions

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp/injectors"
	"github.com/influx6/sabuhp/testingutils"

	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"
)

var testName = "test_action"
var transport = &testingutils.NoPubSub{}
var injector = injectors.NewInjector()

func createWorkerConfig(ctx context.Context, action Action, buffer int, max int) WorkerConfig { //nolint:lll
	return WorkerConfig{
		Context:             ctx,
		MessageBufferSize:   buffer,
		Addr:                testName,
		ActionName:          testName,
		Action:              action,
		MaxWorkers:          max,
		Injector:            injector,
		MessageDeliveryWait: time.Millisecond * 100,
		EscalationNotification: func(escalation *Escalation, wk *WorkerGroup) {

		},
	}
}

func TestNewWorkGroup(t *testing.T) {
	var count sync.WaitGroup
	count.Add(10)
	var config = createWorkerConfig(
		context.Background(),
		ActionFunc(func(ctx context.Context, job Job) {
			require.NotNil(t, ctx)
			require.NotNil(t, job.DI)
			require.NotNil(t, job.Msg)
			require.NotNil(t, job.Transport)
			require.NotNil(t, testName, job.To)
			count.Done()
		}),
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
		}, transport))
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
		ActionFunc(func(ctx context.Context, job Job) {
			require.NotNil(t, ctx)
			require.NotNil(t, job.DI)
			require.NotNil(t, job.Msg)
			require.NotNil(t, job.Transport)
			require.NotNil(t, testName, job.To)

			// create 1 second delay.
			<-time.After(500 * time.Millisecond)
			count.Done()
		}),
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
		}, transport))
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
		ActionFunc(func(ctx context.Context, job Job) {
			panic("Killed to restart")
		}),
		1,
		3,
	)

	var notify = make(chan struct{}, 1)

	config.MinWorker = 2
	config.Behaviour = RestartAll
	config.EscalationNotification = func(escalation *Escalation, wk *WorkerGroup) {
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
	require.NoError(t, group.HandleMessage(msg, transport))

	<-notify

	group.WaitRestart()

	group.Stop()

	var stats3 = group.Stats()
	require.True(t, stats3.TotalKilledWorkers > 1)
	require.Equal(t, 1, stats3.TotalEscalations)
	require.Equal(t, 1, stats3.TotalRestarts)
	require.Equal(t, 1, stats3.TotalPanics)
}

func TestNewWorkGroup_PanicStopAll(t *testing.T) {
	var config = createWorkerConfig(
		context.Background(),
		ActionFunc(func(ctx context.Context, job Job) {
			panic("Killed to restart")
		}),
		1,
		3,
	)

	var notify = make(chan struct{}, 1)

	config.MinWorker = 2
	config.Behaviour = StopAllAndEscalate
	config.EscalationNotification = func(escalation *Escalation, wk *WorkerGroup) {
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
	}, transport))

	<-notify

	group.Wait()

	var stats = group.Stats()
	require.Equal(t, 2, stats.TotalKilledWorkers)
	require.Equal(t, 1, stats.TotalEscalations)
	require.Equal(t, 0, stats.TotalRestarts)
	require.Equal(t, 1, stats.TotalPanics)
}
