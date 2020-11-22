package ssepub

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nxid"
	"github.com/influx6/sabuhp/codecs"
	"github.com/influx6/sabuhp/pubsub"

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
	var managerConfig = pubsub.ManagerConfig{
		ID:        nxid.New(),
		Transport: transport,
		Codec:     codec,
		Ctx:       controlCtx,
		Logger:    logger,
	}
	var manager = pubsub.NewManager(managerConfig)

	var sseServer = ManagedSSEServer(controlCtx, logger, manager, nil)
	require.NotNil(t, sseServer)

	var httpServer = httptest.NewServer(sseServer)

	var clientHub = NewSSEHub(controlCtx, 5, httpServer.Client(), logger, linearBackOff)

	var recvMsg = make(chan *sabuhp.Message, 1)
	var socket, socketErr = clientHub.Get(
		func(message []byte, socket *SSEClient) error {
			require.NotEmpty(t, message)
			require.NotNil(t, socket)

			var decoded, decodeErr = codec.Decode(message)
			require.NoError(t, decodeErr)
			recvMsg <- decoded
			return nil
		},
		nxid.New(),
		httpServer.URL,
	)

	require.NoError(t, socketErr)
	require.NotNil(t, socket)

	var subscribeMessage, subErr = testingutils.EncodedMsg(codec, sabuhp.SUBSCRIBE, "hello", "me")
	require.NoError(t, subErr)
	require.NotEmpty(t, subscribeMessage)

	var _, sendErr = socket.Send(subscribeMessage, 0)
	require.NoError(t, sendErr)

	<-addedListener
	require.Len(t, listeners["hello"], 1)

	var topicMessage, topicErr = testingutils.EncodedMsg(codec, "hello", "alex", "me")
	require.NoError(t, topicErr)
	require.NotEmpty(t, topicMessage)

	var _, sendErr2 = socket.Send(topicMessage, 0)
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
