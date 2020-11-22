package mbox

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp/testingutils"

	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"
)

func TestPubSub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}

	var listeners = map[string][]sabuhp.TransportResponse{}

	var transport = &testingutils.TransportImpl{
		ConnFunc: func() sabuhp.Conn {
			return nil
		},
		ListenFunc: func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
			listeners[topic] = append(listeners[topic], handler)
			return &testingutils.NoPubSubChannel{}
		},
		SendToAllFunc: func(data *sabuhp.Message, timeout time.Duration) error {
			for _, handler := range listeners[data.Topic] {
				handler.Handle(data, nil)
			}
			return nil
		},
		SendToOneFunc: func(data *sabuhp.Message, timeout time.Duration) error {
			var targetListeners = listeners[data.Topic]
			if len(targetListeners) > 0 {
				targetListeners[0].Handle(data, nil)
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
	var pubsub = NewMailer(
		ctx,
		10,
		logger,
		transport,
	)

	var sendWaiter sync.WaitGroup
	sendWaiter.Add(2)

	var channel = pubsub.Listen("hello", sabuhp.TransportResponseFunc(func(data *sabuhp.Message, sub sabuhp.Transport) sabuhp.MessageErr {
		defer sendWaiter.Done()
		require.NotNil(t, data)
		require.NotNil(t, sub)
		return nil
	}))
	require.NotNil(t, channel)

	require.NoError(t, pubsub.SendToOne(message, 0))
	require.NoError(t, pubsub.SendToAll(message, 0))

	sendWaiter.Wait()

	canceler()
	channel.Close()

	pubsub.Wait()
}
