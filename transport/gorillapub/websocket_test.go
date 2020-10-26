package gorillapub

import (
	"context"
	"testing"
	"time"

	"github.com/influx6/npkg/nerror"

	"github.com/stretchr/testify/require"

	"github.com/Ewe-Studios/websocket"
	"github.com/influx6/sabuhp/testingutils"
)

var upgrader = &websocket.Upgrader{
	HandshakeTimeout:  time.Second * 5,
	ReadBufferSize:    defaultMaxMessageSize,
	WriteBufferSize:   defaultMaxMessageSize,
	EnableCompression: false,
}

func TestGorillaHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Logger: logger,
		Handler: func(b []byte, from *GorillaSocket) error {
			return from.Send(append([]byte("hello "), b...), 0)
		},
	})

	hub.Start()

	var wsHandler = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsHandler)
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
		Handler: func(b []byte, from *GorillaSocket) error {
			return nerror.New("bad socket")
		},
	})

	hub.Start()

	var wsHandler = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsHandler)
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
		Handler: func(b []byte, from *GorillaSocket) error {
			defer close(sent)
			return from.Send(append([]byte("hello "), b...), 0)
		},
	})

	hub.Start()

	var wsHandler = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsHandler)
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
