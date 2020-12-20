package collectors

import (
	"context"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/sabuhp"
)

type BatchHandler func(chan *sabuhp.Message)

type MessageCollection struct {
	batchCount int
	starter    sync.Once
	ender      sync.Once
	handler    BatchHandler
	ctx        context.Context
	canceler   context.CancelFunc
	batch      chan *sabuhp.Message
	batchTime  time.Duration
	waiter     sync.WaitGroup
}

func NewMessageCollection(ctx context.Context, maxCount int, batchWait time.Duration, handler BatchHandler) *MessageCollection {
	var newCtx, newCanceler = context.WithCancel(ctx)
	var mc = &MessageCollection{
		ctx:        newCtx,
		handler:    handler,
		batchCount: maxCount,
		batchTime:  batchWait,
		canceler:   newCanceler,
		batch:      make(chan *sabuhp.Message, maxCount),
	}
	return mc
}

func (mc *MessageCollection) Wait() {
	mc.waiter.Wait()
}

func (mc *MessageCollection) Stop() {
	mc.ender.Do(func() {
		mc.canceler()
		mc.waiter.Wait()
	})
}

func (mc *MessageCollection) TakeTill(msg *sabuhp.Message, timeout time.Duration) error {
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

func (mc *MessageCollection) Take(msg *sabuhp.Message) error {
	select {
	case <-mc.ctx.Done():
		return nerror.WrapOnly(mc.ctx.Err())
	case mc.batch <- msg:
		return nil
	}
}

func (mc *MessageCollection) Start() {
	mc.starter.Do(func() {
		mc.waiter.Add(1)
		go mc.manage()
	})
}

func (mc *MessageCollection) manage() {
	defer mc.waiter.Done()

	var nx = make(chan *sabuhp.Message, mc.batchCount)
	var ticker = time.NewTimer(mc.batchTime)
	defer ticker.Stop()

	for {
		select {
		case <-mc.ctx.Done():
			return
		case <-ticker.C:
			if len(nx) > 0 {
				mc.handler(nx)
				nx = make(chan *sabuhp.Message, mc.batchCount)
			}
		case item := <-mc.batch:
			nx <- item
			if len(nx) == mc.batchCount {
				mc.handler(nx)
				nx = make(chan *sabuhp.Message, mc.batchCount)
				continue
			}
			ticker.Reset(mc.batchTime)
		}
	}
}
