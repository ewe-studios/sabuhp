package hsocks

import (
	"context"
	"fmt"
	"github.com/ewe-studios/sabuhp/sabu"
	"github.com/influx6/npkg/nthen"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nxid"

	"github.com/ewe-studios/sabuhp/codecs"

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

	var mb sabu.BusBuilder

	var mx sabu.StreamFunc
	mx.Listen = func(b sabu.Message, socket sabu.Socket) error {
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
		sabu.NewHttpDecoderImpl(codec, logger, -1),
		sabu.NewHttpEncoderImpl(codec, logger),
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

	var topicMessage = testingutils.Msg(sabu.T("hello"), "alex", "me")
	topicMessage.Future = nthen.NewFuture()

	socket.Send(topicMessage)

	topicMessage.Future.Wait()

	require.NoError(t, topicMessage.Future.Err())

	var response = topicMessage.Future.Value().(sabu.Message)
	require.Equal(t, "yay!", string(response.Bytes))

	controlStopFunc()

	httpServer.Close()
}
