package ssepub

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ewe-studios/sabuhp/codecs"
	"github.com/influx6/npkg/nxid"

	"github.com/ewe-studios/sabuhp"
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

	var clientHub = NewSSEHub(controlCtx, 5, httpServer.Client(), codec, logger, linearBackOff)

	var recvMsg = make(chan sabuhp.Message, 1)
	var socket, socketErr = clientHub.Get(
		func(b sabuhp.Message, socket *SSEClient) error {
			fmt.Println("Received response: ", b.Bytes, b.Topic)
			require.NotEmpty(t, b)
			require.NotNil(t, socket)
			recvMsg <- b
			return nil
		},
		nxid.New(),
		httpServer.URL,
	)

	require.NoError(t, socketErr)
	require.NotNil(t, socket)

	var subscribeMessage = testingutils.Msg("hello", "alex", "me")
	var sendErr = socket.Send("POST", subscribeMessage, 0)
	require.NoError(t, sendErr)

	var okMessage = <-recvMsg
	require.NotNil(t, okMessage)
	require.Equal(t, "yay!", string(okMessage.Bytes))

	controlStopFunc()

	httpServer.Close()
	socket.Wait()
}

func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}
