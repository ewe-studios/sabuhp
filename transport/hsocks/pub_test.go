package hsocks

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp/codecs"
	"github.com/influx6/sabuhp/managers"

	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/testingutils"
)

func TestNewHub(t *testing.T) {
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
		ID:        nxid.New(),
		Transport: transport,
		Codec:     codec,
		Ctx:       controlCtx,
		Logger:    logger,
	}
	var manager = managers.NewManager(managerConfig)
	manager.Listen("hello", sabuhp.TransportResponseFunc(func(message *sabuhp.Message, transport sabuhp.Transport) sabuhp.MessageErr {
		require.NotEmpty(t, message)
		require.NotNil(t, transport)

		if err := transport.SendToOne(sabuhp.BasicMsg(
			"word",
			"alex",
			"me",
		), 0); err != nil {
			return sabuhp.WrapErr(err, false)
		}
		return nil
	}))

	var sseServer = ManagedHttpServlet(controlCtx, logger, sabuhp.NewCodecTransposer(codec, logger), manager, nil)
	require.NotNil(t, sseServer)

	var httpServer = httptest.NewServer(sseServer)

	var clientHub = NewHub(controlCtx, 5, httpServer.Client(), logger, linearBackOff)

	var socket, socketErr = clientHub.For(
		nxid.New(),
		httpServer.URL,
	)

	require.NoError(t, socketErr)
	require.NotNil(t, socket)

	<-addedListener
	require.Len(t, listeners["hello"], 1)

	var topicMessage, topicErr = testingutils.EncodedMsg(codec, "hello", "alex", "me")
	require.NoError(t, topicErr)
	require.NotEmpty(t, topicMessage)

	var header = sabuhp.Header{}
	header.Set("Content-Type", sabuhp.MessageContentType)

	var response, sendErr2 = socket.Send("POST", topicMessage, 0, header)
	require.NoError(t, sendErr2)

	var messageReceived, messageErr = codec.Decode(response)
	require.NoError(t, messageErr)
	require.Equal(t, "alex", string(messageReceived.Payload))

	controlStopFunc()

	httpServer.Close()
	manager.Wait()
}

func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}
