package gorillapub

import (
	"context"
	"testing"
	"time"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"

	"github.com/Ewe-Studios/websocket"
	"github.com/influx6/sabuhp/testingutils"
)

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
		Handler: func(b []byte, from sabuhp.Socket) error {
			return from.Send(append([]byte("hello "), b...), 0)
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

	var message = make(chan []byte, 1)
	var client, clientErr = GorillaClient(SocketConfig{
		Ctx:      controlCtx,
		Logger:   logger,
		MaxRetry: 5,
		RetryFn: func(last time.Duration) time.Duration {
			return last + (time.Millisecond * 100)
		},
		Endpoint: DefaultEndpoint(wsConnAddr, 2*time.Second),
		Handler: func(b []byte, from sabuhp.Socket) error {
			require.NotEmpty(t, b)
			require.NotNil(t, from)
			message <- b
			return nil
		},
	})
	require.NoError(t, clientErr)
	require.NotNil(t, client)

	client.Start()

	require.NoError(t, client.Send([]byte("alex"), 0))

	var serverResponse = <-message
	require.Equal(t, []byte("hello alex"), serverResponse)

	controlStopFunc()

	client.Wait()
	hub.Wait()
}

func TestGorillaClientReconnect(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Logger: logger,
		Handler: func(b []byte, from sabuhp.Socket) error {
			return from.Send(append([]byte("hello "), b...), 0)
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

	var message = make(chan []byte, 1)
	var client, clientErr = GorillaClient(SocketConfig{
		Ctx:      controlCtx,
		Logger:   logger,
		MaxRetry: 5,
		RetryFn: func(last time.Duration) time.Duration {
			return last + (time.Millisecond * 100)
		},
		Endpoint: DefaultEndpoint(wsConnAddr, 1*time.Second),
		Handler: func(b []byte, from sabuhp.Socket) error {
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

	require.NoError(t, client.Send([]byte("alex"), 0))

	var serverResponse = <-message
	require.Equal(t, []byte("hello alex"), serverResponse)

	// close current connection
	_ = clientConn.Close()

	<-time.After(time.Millisecond * 30)

	require.NoError(t, client.Send([]byte("alex"), 0))

	var serverResponse2 = <-message
	require.Equal(t, []byte("hello alex"), serverResponse2)

	controlStopFunc()

	client.Wait()
	hub.Wait()
}

func TestGorillaHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Logger: logger,
		Handler: func(b []byte, from sabuhp.Socket) error {
			return from.Send(append([]byte("hello "), b...), 0)
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

	testingutils.SendMessage(t, wsConn, []byte("alex"))

	var received = testingutils.ReceiveWSMessage(t, wsConn)
	require.NotNil(t, received)
	require.Equal(t, []byte("hello alex"), received)

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
		Logger: logger,
		Handler: func(b []byte, from sabuhp.Socket) error {
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
		Handler: func(b []byte, from sabuhp.Socket) error {
			defer close(sent)
			return from.Send(append([]byte("hello "), b...), 0)
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

	testingutils.SendMessage(t, wsConn, []byte("alex"))

	<-sent

	var received = testingutils.ReceiveWSMessage(t, wsConn)
	require.NotNil(t, received)
	require.Equal(t, []byte("hello alex"), received)

	controlStopFunc()

	hub.Wait()

	var stats, statsErr = hub.Stats()
	require.Error(t, statsErr)
	require.Empty(t, stats)
}
