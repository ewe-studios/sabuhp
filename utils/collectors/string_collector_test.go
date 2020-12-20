package collectors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewStringCollectorUsingMaxBatch(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var done = make(chan struct{})
	var collector = NewStringCollector(ctx, 3, 500*time.Millisecond, func(messages chan string) {
		require.Len(t, messages, 3)
		close(done)
	})

	collector.Start()

	require.NoError(t, collector.Take("gh"))
	require.NoError(t, collector.Take("gh"))
	require.NoError(t, collector.Take("gh"))

	select {
	case <-time.After(500 * time.Millisecond):
		require.Fail(t, "Should have completed")
	case <-done:
		break
	}

	canceler()
	collector.Wait()
}

func TestNewStringCollectorUsingTimeout(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var done = make(chan struct{})
	var collector = NewStringCollector(ctx, 10, 500*time.Millisecond, func(messages chan string) {
		close(done)
		require.Len(t, messages, 3)
	})

	collector.Start()

	require.NoError(t, collector.Take("gh"))
	require.NoError(t, collector.Take("gh"))
	require.NoError(t, collector.Take("gh"))

	<-done

	canceler()
	collector.Wait()
}
