package mbox

import (
	"context"
	"sync"
	"testing"

	"github.com/ewe-studios/sabuhp/testingutils"

	"github.com/ewe-studios/sabuhp"

	"github.com/stretchr/testify/require"
)

func TestLocalMailer(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Bytes:    sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var ctx, canceler = context.WithCancel(context.Background())
	var pubsub = NewLocalMailer(
		ctx,
		10,
		logger,
	)

	var sendWaiter sync.WaitGroup
	sendWaiter.Add(2)

	var channel = pubsub.Listen("hello", sabuhp.TransportResponseFunc(func(data *sabuhp.Message, sub sabuhp.MessageBus) sabuhp.MessageErr {
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
