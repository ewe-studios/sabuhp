package pubsub

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/njson"

	"github.com/stretchr/testify/require"
)

type Sub struct {
	Topic    string
	Callback ChannelResponse
	Channel  sabuhp.Channel
}

func BasicMsg(topic string, message string, fromAddr string) *sabuhp.Message {
	return &sabuhp.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

type noPubSub struct {
	DelegateFunc  func(message *sabuhp.Message, timeout time.Duration) error
	BroadcastFunc func(message *sabuhp.Message, timeout time.Duration) error
	ChannelFunc   func(topic string, callback ChannelResponse) sabuhp.Channel
}

func (n noPubSub) Channel(topic string, callback ChannelResponse) sabuhp.Channel {
	if n.ChannelFunc != nil {
		return n.ChannelFunc(topic, callback)
	}
	return &noPubSubChannel{}
}

func (n noPubSub) Delegate(message *sabuhp.Message, timeout time.Duration) error {
	if n.DelegateFunc != nil {
		return n.DelegateFunc(message, timeout)
	}
	return nil
}

func (n noPubSub) Broadcast(message *sabuhp.Message, timeout time.Duration) error {
	if n.BroadcastFunc != nil {
		return n.BroadcastFunc(message, timeout)
	}
	return nil
}

type transportImpl struct {
	ConnFunc      func() sabuhp.Conn
	SendToOneFunc func(data *sabuhp.Message, timeout time.Duration) error
	SendToAllFunc func(data *sabuhp.Message, timeout time.Duration) error
	ListenFunc    func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel
}

func (t transportImpl) Conn() sabuhp.Conn {
	return t.ConnFunc()
}

func (t transportImpl) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	return t.ListenFunc(topic, handler)
}

func (t transportImpl) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToOneFunc(data, timeout)
}

func (t transportImpl) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToAllFunc(data, timeout)
}

type noPubSubChannel struct {
	Error error
}

func (n noPubSubChannel) Err() error {
	return n.Error
}

func (n noPubSubChannel) Close() {
	// do nothing
}

type loggerPub struct{}

func (l loggerPub) Log(cb *njson.JSON) {
	log.Println(cb.Message())
	log.Println("")
}

func TestNewTransportManager(t *testing.T) {
	var logger = &loggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var listeners = map[string][]sabuhp.TransportResponse{}
	var transport = &transportImpl{
		ListenFunc: func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
			listeners[topic] = append(listeners[topic], handler)
			return &noPubSubChannel{}
		},
		SendToAllFunc: func(data *sabuhp.Message, timeout time.Duration) error {
			var sector = listeners[data.Topic]
			for _, handler := range sector {
				if err := handler(data, nil); err != nil {
					var cj = njson.MJSON("error occurred")
					cj.String("error", err.Error())
					logger.Log(cj)
				}
			}

			var cj = njson.MJSON("sent message")
			cj.Object("message", data)
			logger.Log(cj)
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

	var reply = BasicMsg("hello", "hello ", "you")
	var manager = NewTransportManager(controlCtx, transport, logger)

	var called = make(chan struct{}, 2)
	var doAction = func(message *sabuhp.Message, tr sabuhp.Transport) sabuhp.MessageErr {
		require.Equal(t, reply, message)
		called <- struct{}{}
		return nil
	}

	var topicChannel = manager.Listen("hello", doAction)
	require.NotNil(t, topicChannel)

	var topicChannel2 = manager.Listen("hello", doAction)
	require.NotNil(t, topicChannel2)
	require.NotEqual(t, topicChannel2, topicChannel)

	require.Len(t, listeners["hello"], 1)

	require.NoError(t, manager.SendToAll(reply, 0))

	<-called
	<-called

	topicChannel2.Close()

	require.NoError(t, manager.SendToAll(reply, 0))

	<-called

	controlStopFunc()

	manager.Wait()
}
