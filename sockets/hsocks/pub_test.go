package hsocks

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influx6/npkg/nthen"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nxid"

	"github.com/ewe-studios/sabuhp/codecs"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/testingutils"
)

type subChannel struct {
	topic string
	gp    string
}

func (s subChannel) Topic() string {
	return s.topic
}

func (s subChannel) Group() string {
	return s.gp
}

func (s subChannel) Close() {
	return
}

func (s subChannel) Err() error {
	return nil
}

func TestNewHub(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var mb sabuhp.BusBuilder

	var mx sabuhp.StreamFunc
	mx.Listen = func(b sabuhp.Message, socket sabuhp.Socket) error {
		fmt.Println("Received send request: ", b.Bytes, b.Topic)
		var rm = b.ReplyTo()
		rm.WithPayload([]byte("yay!"))
		socket.Send(rm)
		return nil
	}

	var codec = &codecs.JsonCodec{}
	var servletServer = ManagedHttpServlet(
		controlCtx,
		logger,
		sabuhp.NewHttpDecoderImpl(codec, logger, -1),
		sabuhp.NewHttpEncoderImpl(codec, logger),
		nil,
		&mb,
	)
	require.NotNil(t, servletServer)

	servletServer.Stream(&mx)

	var httpServer = httptest.NewServer(servletServer)

	var clientHub = NewClientSockets(controlCtx, 5, codec, httpServer.Client(), logger, linearBackOff)

	var socket, socketErr = clientHub.For(
		nxid.New(),
		httpServer.URL,
	)

	require.NoError(t, socketErr)
	require.NotNil(t, socket)

	var topicMessage = testingutils.Msg("hello", "alex", "me")
	topicMessage.Future = nthen.NewFuture()

	var ft = socket.Send("POST", topicMessage)

	ft.Wait()

	require.NoError(t, ft.Err())

	var response = ft.Value().(sabuhp.Message)
	require.Equal(t, "yay!", string(response.Bytes))

	controlStopFunc()

	httpServer.Close()
}

func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}
