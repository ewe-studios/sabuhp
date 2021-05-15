package mbox

import (
	"context"
	"testing"
	"time"

	"github.com/ewe-studios/sabuhp/testingutils"

	"github.com/ewe-studios/sabuhp"

	"github.com/stretchr/testify/require"
)

var logger = &testingutils.LoggerPub{}
var mailer = &Mailer{}
var transport = &testingutils.TransportImpl{
	ConnFunc: func() sabuhp.Conn {
		return nil
	},
	ListenFunc: func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
		return nil
	},
	SendToAllFunc: func(data *sabuhp.Message, timeout time.Duration) error {
		return nil
	},
	SendToOneFunc: func(data *sabuhp.Message, timeout time.Duration) error {
		return nil
	},
}

func TestMailbox_StartAndStop(t *testing.T) {
	var helloMailbox = NewMailbox(
		context.Background(),
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	<-time.After(time.Second)

	helloMailbox.Stop()
}

func TestMailbox_StartAndStopWithCancel(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	var helloMailbox = NewMailbox(
		ctx,
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	go func() {
		<-time.After(time.Second)
		canceler()
	}()

	helloMailbox.Wait()
}

func TestMailbox_MessageDelivery(t *testing.T) {

	var ctx, canceler = context.WithCancel(context.Background())
	var helloMailbox = NewMailbox(
		ctx,
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Bytes:    sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var delivered = make(chan struct{})
	var channel = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))

	require.NotNil(t, channel)
	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered

	canceler()
}

func TestMailbox_2Subscribers(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	var helloMailbox = NewMailbox(
		ctx,
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Bytes:    sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var delivered = make(chan struct{}, 2)
	var channel1 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel1)

	var channel2 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel2)

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered

	canceler()
}

func TestMailbox_3Subscribers_Channel3_Unsubscribed(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	var helloMailbox = NewMailbox(
		ctx,
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Bytes:    sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var delivered = make(chan struct{}, 3)

	var channel1 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel1)

	var channel2 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel2)

	var channel3 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel3)

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered
	<-delivered

	// close channel 3
	channel3.Close()

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered

	require.Len(t, delivered, 0)

	canceler()
}

func TestMailbox_3Subscribers_Channel2_Unsubscribed(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	var helloMailbox = NewMailbox(
		ctx,
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Bytes:    sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var delivered = make(chan struct{}, 3)

	var channel1 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel1)

	var channel2 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel2)

	var channel3 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel3)

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered
	<-delivered

	// close channel 2
	channel2.Close()

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered

	require.Len(t, delivered, 0)

	canceler()
}

func TestMailbox_3Subscribers_Channel1_Unsubscribed(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	var helloMailbox = NewMailbox(
		ctx,
		"hello",
		logger,
		1,
		transport,
	)

	helloMailbox.Start()

	var message = &sabuhp.Message{
		Topic:    "hello",
		FromAddr: "yay",
		Bytes:    sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}

	var delivered = make(chan struct{}, 3)

	var channel1 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel1)

	var channel2 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel2)

	var channel3 = helloMailbox.Add(sabuhp.TransportResponseFunc(func(data *sabuhp.Message, tt sabuhp.MessageBus) sabuhp.MessageErr {
		require.Equal(t, message, data)
		require.NotNil(t, tt)
		delivered <- struct{}{}
		return nil
	}))
	require.NotNil(t, channel3)

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered
	<-delivered

	// close channel 1
	channel1.Close()

	require.NoError(t, helloMailbox.Deliver(message))

	<-delivered
	<-delivered

	require.Len(t, delivered, 0)

	canceler()
}
