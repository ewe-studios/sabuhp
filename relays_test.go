package sabuhp

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestPbRelay(t *testing.T) {
	var controlCtx, controlStopFunc = context.WithCancel(context.Background())

	var logger GoLogImpl
	var mb BusBuilder

	var reply = BasicMsg("hello", "hello ", "you")
	var manager = NewPbRelay(controlCtx,  logger)

	var w sync.WaitGroup
	w.Add(2)

	var doAction = func(message Message, tr Transport) MessageErr {
		fmt.Println("Received message")
		w.Done()
		return nil
	}

	var group1 = manager.Group("hello", "g1")
	var group2 = manager.Group("hello", "g2")

	require.NotEqual(t, group2, group1)

	var topicChannel = group1.Listen(TransportResponseFunc(doAction))
	var topicChannel2 = group2.Listen(TransportResponseFunc(doAction))

	require.NoError(t, manager.Handle(reply, Transport{Bus: &mb}))

	topicChannel2.Close()
	topicChannel.Close()

	w.Wait()

	controlStopFunc()

	manager.Wait()
}
