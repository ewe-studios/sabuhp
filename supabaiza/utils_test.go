package supabaiza_test

import (
	"log"
	"time"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/sabuhp/supabaiza"
)

var _ supabaiza.Channel = (*NoPubSubChannel)(nil)

type NoPubSubChannel struct{}

func (n NoPubSubChannel) Close() {
	// do nothing
}

var _ supabaiza.PubSub = (*NoPubSub)(nil)

type NoPubSub struct {
	DelegateFunc  func(message *supabaiza.Message, timeout time.Duration) error
	BroadcastFunc func(message *supabaiza.Message, timeout time.Duration) error
	ChannelFunc   func(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel
}

func (n NoPubSub) Channel(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel {
	if n.ChannelFunc != nil {
		return n.ChannelFunc(topic, callback)
	}
	return &NoPubSubChannel{}
}

func (n NoPubSub) Delegate(message *supabaiza.Message, timeout time.Duration) error {
	if n.DelegateFunc != nil {
		return n.DelegateFunc(message, timeout)
	}
	return nil
}

func (n NoPubSub) Broadcast(message *supabaiza.Message, timeout time.Duration) error {
	if n.BroadcastFunc != nil {
		return n.BroadcastFunc(message, timeout)
	}
	return nil
}

type LoggerPub struct{}

func (l LoggerPub) Log(cb *njson.JSON) {
	log.Println(cb.Message())
}
