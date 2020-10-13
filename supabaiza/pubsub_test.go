package supabaiza_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influx6/sabuhp/supabaiza"
)

func TestPubSub(t *testing.T) {
	var logger = &LoggerPub{}

	var listeners = map[string][]supabaiza.TransportResponse{}

	var transport = &TransportImpl{
		ConnFunc: func() supabaiza.Conn {
			return nil
		},
		ListenFunc: func(topic string, handler supabaiza.TransportResponse) supabaiza.Channel {
			listeners[topic] = append(listeners[topic], handler)
			return &NoPubSubChannel{}
		},
		SendToAllFunc: func(data *supabaiza.Message, timeout time.Duration) error {
			for _, handler := range listeners[data.Topic] {
				handler(data)
			}
			return nil
		},
		SendToOneFunc: func(data *supabaiza.Message, timeout time.Duration) error {
			var targetListeners = listeners[data.Topic]
			if len(targetListeners) > 0 {
				targetListeners[0](data)
			}
			return nil
		},
	}

	var message = &supabaiza.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Payload:  supabaiza.BinaryPayload("alex"),
		Metadata: nil,
	}

	var ctx, canceler = context.WithCancel(context.Background())
	var pubsub = supabaiza.NewPubSubImpl(
		ctx,
		10,
		logger,
		transport,
	)

	var sendWaiter sync.WaitGroup
	sendWaiter.Add(2)

	var channel = pubsub.Channel("hello", func(data *supabaiza.Message, sub supabaiza.PubSub) {
		defer sendWaiter.Done()
		require.NotNil(t, data)
		require.NotNil(t, sub)
	})
	require.NotNil(t, channel)

	require.NoError(t, pubsub.Delegate(message, 0))
	require.NoError(t, pubsub.Broadcast(message, 0))

	sendWaiter.Wait()

	canceler()
	channel.Close()

	pubsub.Wait()
}
