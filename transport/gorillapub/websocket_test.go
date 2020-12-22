package gorillapub

import (
	"bytes"
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/codecs"

	"github.com/stretchr/testify/require"

	"github.com/Ewe-Studios/websocket"
	"github.com/influx6/sabuhp/testingutils"
)

var codec = &codecs.JsonCodec{}
var upgrader = &websocket.Upgrader{
	HandshakeTimeout:  time.Second * 5,
	ReadBufferSize:    DefaultMaxMessageSize,
	WriteBufferSize:   DefaultMaxMessageSize,
	EnableCompression: false,
}

func TestGorillaClient(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Logger: logger,
		Codec:  codec,
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			return from.Send(append([]byte("hello "), b.Payload...), sabuhp.MessageMeta{}, 0)
		},
	})

	hub.Start()

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConnAddr = testingutils.NewWSServerOnly(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotEmpty(t, wsConnAddr)

	var message = make(chan *sabuhp.Message, 1)
	var client, clientErr = GorillaClient(SocketConfig{
		Info: &SocketInfo{
			Path:    "yo",
			Query:   url.Values{},
			Headers: sabuhp.Header{},
		},
		Ctx:      controlCtx,
		Logger:   logger,
		Codec:    codec,
		MaxRetry: 5,
		RetryFn: func(last int) time.Duration {
			return time.Duration(last) + (time.Millisecond * 100)
		},
		Endpoint: DefaultEndpoint(wsConnAddr, 2*time.Second),
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			require.NotEmpty(t, b)
			require.NotNil(t, from)
			message <- b
			return nil
		},
	})
	require.NoError(t, clientErr)
	require.NotNil(t, client)

	client.Start()

	require.NoError(t, client.Send([]byte("alex"), sabuhp.MessageMeta{}, 0))

	var serverResponse = <-message
	require.Equal(t, []byte("hello alex"), serverResponse.Payload)

	controlStopFunc()

	client.Wait()
	hub.Wait()
}

func TestGorillaClientReconnect(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Codec:  codec,
		Logger: logger,
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			return from.Send(append([]byte("hello "), b.Payload...), sabuhp.MessageMeta{}, 0)
		},
	})

	hub.Start()

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConnAddr = testingutils.NewWSServerOnly(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotEmpty(t, wsConnAddr)

	defer httpServer.Close()

	var message = make(chan *sabuhp.Message, 1)
	var client, clientErr = GorillaClient(SocketConfig{
		Info: &SocketInfo{
			Path:    "yo",
			Query:   url.Values{},
			Headers: sabuhp.Header{},
		},
		Codec:    codec,
		Ctx:      controlCtx,
		Logger:   logger,
		MaxRetry: 5,
		RetryFn: func(last int) time.Duration {
			return time.Duration(last) + (time.Millisecond * 100)
		},
		Endpoint: DefaultEndpoint(wsConnAddr, 1*time.Second),
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			require.NotEmpty(t, b)
			require.NotNil(t, from)
			message <- b
			return nil
		},
	})
	require.NoError(t, clientErr)
	require.NotNil(t, client)

	client.Start()

	var clientConn = client.Conn()
	require.NotNil(t, clientConn)

	require.NoError(t, client.Send([]byte("alex"), sabuhp.MessageMeta{}, 0))

	var serverResponse = <-message
	require.Equal(t, []byte("hello alex"), serverResponse.Payload)

	// close current connection
	_ = clientConn.Close()

	<-time.After(time.Millisecond * 30)

	require.NoError(t, client.Send([]byte("alex"), sabuhp.MessageMeta{}, 0))

	var serverResponse2 = <-message
	require.Equal(t, []byte("hello alex"), serverResponse2.Payload)

	controlStopFunc()

	client.Wait()
	hub.Wait()
}

func TestGorillaHub_WithMessage(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Codec:  codec,
		Logger: logger,
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			var topicMessage, topicErr = testingutils.EncodedMsg(codec, "hello", string(append([]byte("hello "), b.Payload...)), "me")
			require.NoError(t, topicErr)
			return from.Send(topicMessage, sabuhp.MessageMeta{ContentType: sabuhp.MessageContentType}, 0)
		},
	})

	hub.Start()

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	testingutils.SendMessage(t, wsConn, []byte("alex"))

	var received = testingutils.ReceiveWSMessage(t, wsConn)
	require.NotNil(t, received)
	require.True(t, bytes.HasPrefix(received, []byte("0|")))

	var stats, statsErr = hub.Stats()
	require.NoError(t, statsErr)
	require.NotEmpty(t, stats)
	require.Len(t, stats, 1)
	require.Equal(t, 1, int(stats[0].Sent))
	require.Equal(t, 1, int(stats[0].Handled))
	require.Equal(t, 1, int(stats[0].Received))

	controlStopFunc()

	hub.Wait()
}

func TestGorillaHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Codec:  codec,
		Logger: logger,
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			return from.Send(append([]byte("hello "), b.Payload...), sabuhp.MessageMeta{}, 0)
		},
	})

	hub.Start()

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	testingutils.SendMessage(t, wsConn, []byte("alex"))

	var received = testingutils.ReceiveWSMessage(t, wsConn)
	require.NotNil(t, received)
	require.Equal(t, []byte("1|hello alex"), received)

	var stats, statsErr = hub.Stats()
	require.NoError(t, statsErr)
	require.NotEmpty(t, stats)
	require.Len(t, stats, 1)
	require.Equal(t, 1, int(stats[0].Sent))
	require.Equal(t, 1, int(stats[0].Handled))
	require.Equal(t, 1, int(stats[0].Received))

	controlStopFunc()

	hub.Wait()
}

func TestGorillaHub_FailedMessage(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Codec:  codec,
		Logger: logger,
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			return nerror.New("bad socket")
		},
	})

	hub.Start()

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	testingutils.SendMessage(t, wsConn, []byte("alex"))

	var received, err = testingutils.GetWSMessage(t, wsConn)
	require.Error(t, err)
	require.Nil(t, received)

	controlStopFunc()

	hub.Wait()
}

func TestGorillaHub_StatAfterClosure(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var sent = make(chan struct{})
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Logger: logger,
		Codec:  codec,
		Handler: func(b *sabuhp.Message, from sabuhp.Socket) error {
			defer close(sent)
			return from.Send(append([]byte("hello "), b.Payload...), sabuhp.MessageMeta{}, 0)
		},
	})

	hub.Start()

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	testingutils.SendMessage(t, wsConn, []byte("alex"))

	<-sent

	var received = testingutils.ReceiveWSMessage(t, wsConn)
	require.NotNil(t, received)
	require.Equal(t, []byte("1|hello alex"), received)

	controlStopFunc()

	hub.Wait()

	var stats, statsErr = hub.Stats()
	require.Error(t, statsErr)
	require.Empty(t, stats)
}
