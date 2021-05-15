package hsocks

import (
	"context"
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

	var addedListener = make(chan struct{}, 1)
	var listeners = map[string][]sabuhp.TransportResponse{}

	var bus = &sabuhp.BusBuilder{
		SendFunc: func(data ...sabuhp.Message) {

		},
		ListenFunc: func(topic string, grp string, handler sabuhp.TransportResponse) sabuhp.Channel {
			return subChannel{
				topic: topic,
				gp:    grp,
			}
		},
	}

	var manager = sabuhp.NewPbRelay(controlCtx, logger)

	var codec = &codecs.JsonCodec{}
	var servletServer = ManagedHttpServlet(
		controlCtx,
		logger,
		sabuhp.NewHttpDecoderImpl(codec, logger, -1),
		sabuhp.NewHttpCodec(codec, logger),
		nil,
	)
	require.NotNil(t, servletServer)

	_ = sabuhp.WithRelay(logger, servletServer, bus, manager)

	var httpServer = httptest.NewServer(servletServer)

	var clientHub = NewHub(controlCtx, 5, codec, httpServer.Client(), logger, linearBackOff)

	var socket, socketErr = clientHub.For(
		nxid.New(),
		httpServer.URL,
	)

	require.NoError(t, socketErr)
	require.NotNil(t, socket)

	<-addedListener
	require.Len(t, listeners["hello"], 1)

	var topicMessage = testingutils.Msg("hello", "alex", "me")
	topicMessage.Future = nthen.NewFuture()

	var ft = socket.Send("POST", topicMessage)

	ft.Wait()

	require.NoError(t, ft.Err())

	var response = ft.Value().(sabuhp.Message)
	require.Equal(t, "alex", string(response.Bytes))

	controlStopFunc()

	httpServer.Close()
}

func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}
