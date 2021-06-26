package hsocks

import (
	"context"
	"fmt"
	"github.com/influx6/npkg/nthen"
	"net/http/httptest"
	"testing"

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

	var codec = &codecs.MessageJsonCodec{}
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

	var socket = NewClient(
		controlCtx,
		nxid.New(),
		httpServer.URL,
		5,
		"POST",
		codec,
		linearBackOff,
		logger,
		httpServer.Client(),
	)

	var topicMessage = testingutils.Msg(sabuhp.T("hello"), "alex", "me")
	topicMessage.Future = nthen.NewFuture()

	socket.Send(topicMessage)

	topicMessage.Future.Wait()

	require.NoError(t, topicMessage.Future.Err())

	var response = topicMessage.Future.Value().(sabuhp.Message)
	require.Equal(t, "yay!", string(response.Bytes))

	controlStopFunc()

	httpServer.Close()
}
