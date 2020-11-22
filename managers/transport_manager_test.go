package managers

import (
	"context"
	"testing"
	"time"

	"github.com/influx6/sabuhp/testingutils"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/njson"

	"github.com/stretchr/testify/require"
)

type Sub struct {
	Topic    string
	Callback sabuhp.TransportResponse
	Channel  sabuhp.Channel
}

func BasicMsg(topic string, message string, fromAddr string) *sabuhp.Message {
	return &sabuhp.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func TestNewTransportManager(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var listeners = map[string][]sabuhp.TransportResponse{}
	var transport = &testingutils.TransportImpl{
		ListenFunc: func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
			listeners[topic] = append(listeners[topic], handler)
			return &testingutils.NoPubSubChannel{}
		},
		SendToAllFunc: func(data *sabuhp.Message, timeout time.Duration) error {
			var sector = listeners[data.Topic]
			for _, handler := range sector {
				if err := handler.Handle(data, nil); err != nil {
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
				_ = targetListeners[0].Handle(data, nil)
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

	var topicChannel = manager.Listen("hello", sabuhp.TransportResponseFunc(doAction))
	require.NotNil(t, topicChannel)

	var topicChannel2 = manager.Listen("hello", sabuhp.TransportResponseFunc(doAction))
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
