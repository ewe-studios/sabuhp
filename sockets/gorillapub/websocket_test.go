package gorillapub

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/influx6/npkg/nthen"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/codecs"
	"github.com/stretchr/testify/require"

	"github.com/ewe-studios/sabuhp/testingutils"
	"github.com/ewe-studios/websocket"
)

var codec = &codecs.MessageJsonCodec{}
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
	})

	hub.Start()

	var mx sabuhp.StreamFunc
	mx.Listen = func(b sabuhp.Message, socket sabuhp.Socket) error {
		fmt.Println("Received send request: ", b.Bytes, b.Topic)
		var rm = b.ReplyTo()
		rm.WithPayload([]byte("yay!"))
		socket.Send(rm)
		return nil
	}

	hub.Stream(&mx)

	var wsUpgrader = HttpUpgrader(
		logger,
		hub,
		upgrader,
		nil,
	)

	var httpServer, wsConnAddr = testingutils.NewWSServerOnly(t, wsUpgrader)
	require.NotNil(t, httpServer)
	require.NotEmpty(t, wsConnAddr)

	var message = make(chan sabuhp.Message, 1)
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
		Handler: func(b sabuhp.Message, from sabuhp.Socket) error {
			require.NotEmpty(t, b)
			require.NotNil(t, from)
			message <- b
			return nil
		},
	})
	require.NoError(t, clientErr)
	require.NotNil(t, client)

	client.Start()

	var subscribeMessage = testingutils.Msg("hello", "alex", "me")
	subscribeMessage.Future = nthen.NewFuture()

	client.Send(subscribeMessage)

	subscribeMessage.Future.Wait()

	require.NoError(t, subscribeMessage.Future.Err())

	var serverResponse = <-message
	require.Equal(t, []byte("yay!"), serverResponse.Bytes)

	controlStopFunc()

	client.Wait()
	hub.Wait()
}

func TestGorillaHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())
	var hub = NewGorillaHub(HubConfig{
		Ctx:    controlCtx,
		Codec:  codec,
		Logger: logger,
	})

	hub.Start()

	var mx sabuhp.StreamFunc
	mx.Listen = func(b sabuhp.Message, socket sabuhp.Socket) error {
		fmt.Printf("Received send request: %+q -> %q\n", b.Bytes, b.Topic)
		var rm = b.ReplyTo()
		rm.WithPayload([]byte("yay!"))
		socket.Send(rm)
		return nil
	}

	hub.Stream(&mx)

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

	fmt.Printf("Recieved reply: %+q\n", received)

	var rest = bytes.TrimPrefix(received, []byte("0|"))
	fmt.Printf("Trimmed reply: %+q\n", rest)

	var mcg, mcerr = codec.Decode(rest)
	require.NoError(t, mcerr)
	require.NotNil(t, mcg)
	require.Equal(t, []byte("yay!"), mcg.Bytes)

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
