package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"
)

func TestPubSub(t *testing.T) {
	var logger = &loggerPub{}

	var listeners = map[string][]sabuhp.TransportResponse{}

	var transport = &transportImpl{
		ConnFunc: func() sabuhp.Conn {
			return nil
		},
		ListenFunc: func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
			listeners[topic] = append(listeners[topic], handler)
			return &noPubSubChannel{}
		},
		SendToAllFunc: func(data *sabuhp.Message, timeout time.Duration) error {
			for _, handler := range listeners[data.Topic] {
				handler(data, nil)
			}
			return nil
		},
		SendToOneFunc: func(data *sabuhp.Message, timeout time.Duration) error {
			var targetListeners = listeners[data.Topic]
			if len(targetListeners) > 0 {
				targetListeners[0](data, nil)
			}
			return nil
		},
	}

	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Payload:  sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var ctx, canceler = context.WithCancel(context.Background())
	var pubsub = NewPubSubImpl(
		ctx,
		10,
		logger,
		transport,
	)

	var sendWaiter sync.WaitGroup
	sendWaiter.Add(2)

	var channel = pubsub.Channel("hello", func(data *sabuhp.Message, sub PubSub) {
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
