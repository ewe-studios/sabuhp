package ssepub

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nxid"
	"github.com/influx6/sabuhp/codecs"
	"github.com/influx6/sabuhp/managers"

	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/testingutils"
)

func TestNewSSEHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var addedListener = make(chan struct{}, 1)
	var listeners = map[string][]sabuhp.TransportResponse{}
	var transport = &testingutils.TransportImpl{
		ListenFunc: func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
			listeners[topic] = append(listeners[topic], handler)
			addedListener <- struct{}{}
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

	var codec = &codecs.JsonCodec{}

	var managerConfig = managers.ManagerConfig{
		ID:        nxid.New().String(),
		Transport: transport,
		Codec:     codec,
		Ctx:       controlCtx,
		Logger:    logger,
	}
	var manager = managers.NewManager(managerConfig)

	var sseServer = ManagedSSEServer(controlCtx, logger, manager, nil, codec)
	require.NotNil(t, sseServer)

	var httpServer = httptest.NewServer(sseServer)

	var clientHub = NewSSEHub(controlCtx, 5, httpServer.Client(), codec, logger, linearBackOff)

	var recvMsg = make(chan *sabuhp.Message, 1)
	var socket, socketErr = clientHub.Get(
		func(message *sabuhp.Message, socket *SSEClient) error {
			require.NotEmpty(t, message)
			require.NotNil(t, socket)
			recvMsg <- message
			return nil
		},
		nxid.New(),
		httpServer.URL,
	)

	require.NoError(t, socketErr)
	require.NotNil(t, socket)

	var subscribeMessage = testingutils.Msg(sabuhp.SUBSCRIBE, "hello", "me")
	var sendErr = socket.Send("POST", &subscribeMessage, 0)
	require.NoError(t, sendErr)

	<-addedListener
	require.Len(t, listeners["hello"], 1)

	var topicMessage = testingutils.Msg("hello", "alex", "me")
	require.NotEmpty(t, topicMessage)

	var sendErr2 = socket.Send("POST", &topicMessage, 0)
	require.NoError(t, sendErr2)

	var okMessage = <-recvMsg
	require.NotNil(t, okMessage)
	require.Equal(t, okMessage.Topic, sabuhp.DONE)

	var helloMessage = <-recvMsg
	require.NotNil(t, helloMessage)
	require.Equal(t, "alex", string(helloMessage.Payload))

	controlStopFunc()

	httpServer.Close()
	socket.Wait()
	manager.Wait()
}

func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}
