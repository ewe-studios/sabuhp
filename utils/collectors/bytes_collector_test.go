package collectors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewBytesCollectorUsingMaxBatch(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var done = make(chan struct{})
	var collector = NewBytesCollector(ctx, 3, 500*time.Millisecond, func(messages chan []byte) {
		require.Len(t, messages, 3)
		close(done)
	})

	collector.Start()

	require.NoError(t, collector.Take([]byte("gh")))
	require.NoError(t, collector.Take([]byte("gh")))
	require.NoError(t, collector.Take([]byte("gh")))

	select {
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "Should have completed")
	case <-done:
		break
	}

	canceler()
	collector.Wait()
}

func TestNewBytesCollectorUsingTimeout(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var done = make(chan struct{})
	var collector = NewBytesCollector(ctx, 10, 500*time.Millisecond, func(messages chan []byte) {
		close(done)
		require.Len(t, messages, 3)
	})

	collector.Start()

	require.NoError(t, collector.Take([]byte("gh")))
	require.NoError(t, collector.Take([]byte("gh")))
	require.NoError(t, collector.Take([]byte("gh")))

	<-done

	canceler()
	collector.Wait()
}
