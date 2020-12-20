package collectors

import (
	"context"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"
)

type StringBatchHandler func(chan string)

type StringCollector struct {
	batchCount int
	starter    sync.Once
	ender      sync.Once
	handler    StringBatchHandler
	ctx        context.Context
	canceler   context.CancelFunc
	batch      chan string
	batchTime  time.Duration
	waiter     sync.WaitGroup
}

func NewStringCollector(ctx context.Context, maxCount int, batchWait time.Duration, handler StringBatchHandler) *StringCollector {
	var newCtx, newCanceler = context.WithCancel(ctx)
	var mc = &StringCollector{
		ctx:        newCtx,
		handler:    handler,
		batchCount: maxCount,
		batchTime:  batchWait,
		canceler:   newCanceler,
		batch:      make(chan string, maxCount),
	}
	return mc
}

func (mc *StringCollector) Wait() {
	mc.waiter.Wait()
}

func (mc *StringCollector) Stop() {
	mc.ender.Do(func() {
		mc.canceler()
		mc.waiter.Wait()
	})
}

func (mc *StringCollector) Take(msg string) error {
	select {
	case <-mc.ctx.Done():
		return nerror.WrapOnly(mc.ctx.Err())
	case mc.batch <- msg:
		return nil
	}
}

func (mc *StringCollector) TakeTill(msg string, timeout time.Duration) error {
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

func (mc *StringCollector) Start() {
	mc.starter.Do(func() {
		mc.waiter.Add(1)
		go mc.manage()
	})
}

func (mc *StringCollector) manage() {
	defer mc.waiter.Done()

	var nx = make(chan string, mc.batchCount)
	var ticker = time.NewTimer(mc.batchTime)
	defer ticker.Stop()

	for {
		select {
		case <-mc.ctx.Done():
			return
		case <-ticker.C:
			if len(nx) > 0 {
				mc.handler(nx)
				nx = make(chan string, mc.batchCount)
			}
		case item := <-mc.batch:
			nx <- item
			if len(nx) == mc.batchCount {
				mc.handler(nx)
				nx = make(chan string, mc.batchCount)
				continue
			}
			ticker.Reset(mc.batchTime)
		}
	}
}
