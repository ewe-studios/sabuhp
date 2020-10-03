package supabaiza_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influx6/sabuhp/supabaiza"
)

var testName = "test_action"

func createWorkerConfig(ctx context.Context, action supabaiza.Action, buffer int, max int) supabaiza.WorkGroupConfig {
	return supabaiza.WorkGroupConfig{
		Context:             ctx,
		MessageBufferSize:   buffer,
		Pubsub:              &NoPubSub{},
		Addr:                testName,
		Action:              action,
		MaxWorkers:          max,
		MessageDeliveryWait: time.Millisecond * 100,
	}
}

func TestNewWorkGroup(t *testing.T) {
	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
			require.NotNil(t, ctx)
			require.NotNil(t, message)
			require.NotNil(t, pubsub)
			require.NotNil(t, testName, to)
		},
		3,
		3,
	)

	var group = supabaiza.NewWorkGroup(config)
	group.Start()

	var textPayload = supabaiza.TextPayload("Welcome to life")
	for i := 0; i < 10; i++ {
		var ack = make(chan struct{}, 1)
		require.NoError(t, group.HandleMessage(&supabaiza.Message{
			Topic:    "find_user",
			FromAddr: "component_1",
			Payload:  textPayload,
			Metadata: nil,
			Ack:      ack,
		}))
		<-ack
	}

	var stats = group.Stat()
	group.Stop()

	fmt.Printf("Check stats: %#v\n", stats)
	require.Equal(t, 10, stats.TotalMessageReceived)
	require.Equal(t, 10, stats.TotalMessageProcessed)
}

func TestNewWorkGroup_ExpandingWorkforce(t *testing.T) {
	var config = createWorkerConfig(
		context.Background(),
		func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
			require.NotNil(t, ctx)
			require.NotNil(t, message)
			require.NotNil(t, pubsub)
			require.NotNil(t, testName, to)

			// create 1 second delay.
			<-time.After(500 * time.Millisecond)
		},
		1,
		3,
	)

	var group = supabaiza.NewWorkGroup(config)
	group.Start()

	var stats = group.Stat()
	require.Equal(t, 2, stats.AvailableWorkerCapacity)
	require.Equal(t, 1, stats.TotalCurrentWorkers)

	var acks []chan struct{}
	var textPayload = supabaiza.TextPayload("Welcome to life")
	for i := 0; i < 10; i++ {
		var ack = make(chan struct{}, 1)
		acks = append(acks, ack)
		require.NoError(t, group.HandleMessage(&supabaiza.Message{
			Topic:    "find_user",
			FromAddr: "component_1",
			Payload:  textPayload,
			Metadata: nil,
			Ack:      ack,
		}))
	}

	for _, ack := range acks {
		<-ack
	}

	var stats2 = group.Stat()
	require.Equal(t, 0, stats2.AvailableWorkerCapacity)
	require.Equal(t, 3, stats2.TotalCurrentWorkers)

	group.Stop()

	var stats3 = group.Stat()
	require.Equal(t, 10, stats3.TotalMessageReceived)
	require.Equal(t, 10, stats3.TotalMessageProcessed)
}
