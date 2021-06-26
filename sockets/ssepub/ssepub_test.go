package ssepub

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/codecs"
	"github.com/ewe-studios/sabuhp/testingutils"
)

func TestNewSSEHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var codec = &codecs.MessageJsonCodec{}
	var sseServer = ManagedSSEServer(controlCtx, logger, nil, codec)
	require.NotNil(t, sseServer)

	var mx sabuhp.StreamFunc
	mx.Listen = func(b sabuhp.Message, socket sabuhp.Socket) error {
		fmt.Println("Received send request: ", b.Bytes, b.Topic)
		var rm = b.ReplyTo()
		rm.WithPayload([]byte("yay!"))
		socket.Send(rm)
		return nil
	}

	sseServer.Stream(&mx)

	var httpServer = httptest.NewServer(sseServer)

	var recvMsg = make(chan sabuhp.Message, 1)
	var socket, err = NewSSEClient2(
		controlCtx,
		httpServer.URL,
		"GET",
		func(b sabuhp.Message, socket *SSEClient) error {
			fmt.Println("Received response: ", b.Bytes, b.Topic)
			require.NotEmpty(t, b)
			require.NotNil(t, socket)
			recvMsg <- b
			return nil
		},
		codec,
		logger,
		httpServer.Client(),
	)
	require.NoError(t, err)

	var subscribeMessage = testingutils.Msg(sabuhp.T("hello"), "alex", "me")
	socket.Send(subscribeMessage)

	var okMessage = <-recvMsg
	require.NotNil(t, okMessage)
	require.Equal(t, "yay!", string(okMessage.Bytes))

	controlStopFunc()

	httpServer.Close()
	socket.Wait()
}
