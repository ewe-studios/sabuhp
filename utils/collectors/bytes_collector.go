package collectors

import (
	"context"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"
)

type BytesBatchHandler func(chan []byte)

type BytesCollector struct {
	batchCount int
	starter    sync.Once
	ender      sync.Once
	handler    BytesBatchHandler
	ctx        context.Context
	canceler   context.CancelFunc
	batch      chan []byte
	batchTime  time.Duration
	waiter     sync.WaitGroup
}

func NewBytesCollector(ctx context.Context, maxCount int, batchWait time.Duration, handler BytesBatchHandler) *BytesCollector {
	var newCtx, newCanceler = context.WithCancel(ctx)
	var mc = &BytesCollector{
		ctx:        newCtx,
		handler:    handler,
		batchCount: maxCount,
		batchTime:  batchWait,
		canceler:   newCanceler,
		batch:      make(chan []byte, maxCount),
	}
	return mc
}

func (mc *BytesCollector) Wait() {
	mc.waiter.Wait()
}

func (mc *BytesCollector) Stop() {
	mc.ender.Do(func() {
		mc.canceler()
		mc.waiter.Wait()
	})
}

func (mc *BytesCollector) Take(msg []byte) error {
	select {
	case <-mc.ctx.Done():
		return nerror.WrapOnly(mc.ctx.Err())
	case mc.batch <- msg:
		return nil
	}
}

func (mc *BytesCollector) TakeTill(msg []byte, timeout time.Duration) error {
	var tChan <-chan time.Time
	if timeout > 0 {
		tChan = time.After(timeout)
	}
	select {
	case <-tChan:
		return nerror.New("timeout expired")
	case <-mc.ctx.Done():
		return nerror.WrapOnly(mc.ctx.Err())
	case mc.batch <- msg:
		return nil
	}
}

func (mc *BytesCollector) Start() {
	mc.starter.Do(func() {
		mc.waiter.Add(1)
		go mc.manage()
	})
}

func (mc *BytesCollector) manage() {
	defer mc.waiter.Done()

	var nx = make(chan []byte, mc.batchCount)
	var ticker = time.NewTimer(mc.batchTime)
	defer ticker.Stop()

	for {
		select {
		case <-mc.ctx.Done():
			return
		case <-ticker.C:
			if len(nx) > 0 {
				mc.handler(nx)
				nx = make(chan []byte, mc.batchCount)
			}
		case item := <-mc.batch:
			nx <- item
			if len(nx) == mc.batchCount {
				mc.handler(nx)
				nx = make(chan []byte, mc.batchCount)
				continue
			}
			ticker.Reset(mc.batchTime)
		}
	}
}
