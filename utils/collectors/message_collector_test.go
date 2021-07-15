package collectors

import (
	"context"
	"github.com/ewe-studios/sabuhp/sabu"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var msg = sabu.NewMessage("why", "me", []byte("yes"))

func TestNewMessageCollectorUsingMaxBatch(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var done = make(chan struct{})
	var collector = NewMessageCollection(ctx, 3, 500*time.Millisecond, func(messages chan *sabu.Message) {
		require.Len(t, messages, 3)
		close(done)
	})

	collector.Start()

	require.NoError(t, collector.Take(msg))
	require.NoError(t, collector.Take(msg))
	require.NoError(t, collector.Take(msg))

	select {
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "Should have completed")
	case <-done:
		break
	}

	canceler()
	collector.Wait()
}

func TestNewMessageCollectionUsingTimeout(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var done = make(chan struct{})
	var collector = NewMessageCollection(ctx, 10, 500*time.Millisecond, func(messages chan *sabu.Message) {
		require.Len(t, messages, 3)
		close(done)
	})

	collector.Start()

	require.NoError(t, collector.Take(msg))
	require.NoError(t, collector.Take(msg))
	require.NoError(t, collector.Take(msg))

	<-done

	canceler()
	collector.Wait()
}
