package gorillapub

import (
	"context"
	"testing"

	"github.com/influx6/sabuhp/supabaiza"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp/codecs"

	"github.com/stretchr/testify/require"

	"github.com/influx6/sabuhp/testingutils"
)

func TestGorillaPub_StatAfterClosure(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var newSocket = make(chan struct{})
	var codec = &codecs.JsonCodec{}
	var pub = NewGorillaPub(PubConfig{
		ID:     nxid.New(),
		Ctx:    controlCtx,
		Logger: logger,
		Codec:  codec,
		OnOpen: func(socket *GorillaSocket) {
			close(newSocket)
		},
	})

	pub.Start()

	var wsHandler = HttpUpgrader(
		logger,
		pub.Hub(),
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsHandler)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	<-newSocket

	controlStopFunc()

	pub.Wait()
}

func TestGorillaPub_SelfSubscribingSocket(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var newSocket = make(chan struct{})
	var codec = &codecs.JsonCodec{}
	var pub = NewGorillaPub(PubConfig{
		ID:     nxid.New(),
		Ctx:    controlCtx,
		Logger: logger,
		Codec:  codec,
		OnOpen: func(socket *GorillaSocket) {
			close(newSocket)
		},
	})

	pub.Start()

	var wsHandler = HttpUpgrader(
		logger,
		pub.Hub(),
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsHandler)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	<-newSocket

	var subscribeMessage, subErr = testingutils.EncodedMsg(codec, SUBSCRIBE, "hello", "me")
	require.NoError(t, subErr)
	require.NotEmpty(t, subscribeMessage)

	testingutils.SendMessage(t, wsConn, subscribeMessage)

	var okMsg, okErr = testingutils.ReceiveMsg(t, wsConn, codec)
	require.NoError(t, okErr)
	require.NotEmpty(t, okMsg)
	require.Equal(t, DONE, okMsg.Topic)

	var topicMessage, topicErr = testingutils.EncodedMsg(codec, "hello", "alex", "me")
	require.NoError(t, topicErr)
	require.NotEmpty(t, topicMessage)

	testingutils.SendMessage(t, wsConn, topicMessage)

	okMsg, okErr = testingutils.ReceiveMsg(t, wsConn, codec)
	require.NoError(t, okErr)
	require.NotEmpty(t, okMsg)
	require.Equal(t, "alex", string(okMsg.Payload))

	controlStopFunc()

	pub.Wait()
}

func TestGorillaPub_FunctionAndSocket(t *testing.T) {
	var logger = &testingutils.LoggerPub{}
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var newSocket = make(chan struct{})
	var codec = &codecs.JsonCodec{}
	var pub = NewGorillaPub(PubConfig{
		ID:     nxid.New(),
		Ctx:    controlCtx,
		Logger: logger,
		Codec:  codec,
		OnOpen: func(socket *GorillaSocket) {
			close(newSocket)
		},
	})

	pub.Start()

	var helloSubFuncChannel = pub.Listen(
		"hello",
		func(message *supabaiza.Message, transport supabaiza.Transport) supabaiza.MessageErr {
			var reply = BasicMsg("hello-reply", "hello "+string(message.Payload), "you")
			var sendErr = transport.SendToOne(reply, 0)
			if sendErr != nil {
				return supabaiza.WrapErr(sendErr, true)
			}
			return nil
		},
	)
	require.NotNil(t, helloSubFuncChannel)
	require.NoError(t, helloSubFuncChannel.Err())

	defer helloSubFuncChannel.Close()

	var wsHandler = HttpUpgrader(
		logger,
		pub.Hub(),
		upgrader,
		nil,
	)

	var httpServer, wsConn = testingutils.NewWSServer(t, wsHandler)
	require.NotNil(t, httpServer)
	require.NotNil(t, wsConn)

	defer httpServer.Close()

	<-newSocket

	var subscribeMessage, subErr = testingutils.EncodedMsg(codec, SUBSCRIBE, "hello-reply", "me")
	require.NoError(t, subErr)
	require.NotEmpty(t, subscribeMessage)

	testingutils.SendMessage(t, wsConn, subscribeMessage)

	var okMsg, okErr = testingutils.ReceiveMsg(t, wsConn, codec)
	require.NoError(t, okErr)
	require.NotEmpty(t, okMsg)
	require.Equal(t, DONE, okMsg.Topic)

	var topicMessage, topicErr = testingutils.EncodedMsg(codec, "hello", "alex", "me")
	require.NoError(t, topicErr)
	require.NotEmpty(t, topicMessage)

	testingutils.SendMessage(t, wsConn, topicMessage)

	okMsg, okErr = testingutils.ReceiveMsg(t, wsConn, codec)
	require.NoError(t, okErr)
	require.NotEmpty(t, okMsg)
	require.Equal(t, "hello alex", string(okMsg.Payload))

	controlStopFunc()

	pub.Wait()
}
