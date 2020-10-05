package supabaiza_test

import (
	"time"

	"github.com/influx6/sabuhp/supabaiza"
)

var _ supabaiza.Channel = (*NoPubSubChannel)(nil)

type NoPubSubChannel struct{}

func (n NoPubSubChannel) Close() {
	// do nothing
}

var _ supabaiza.PubSub = (*NoPubSub)(nil)

type NoPubSub struct{}

func (n NoPubSub) Channel(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel {
	return &NoPubSubChannel{}
}

func (n NoPubSub) Send(message *supabaiza.Message, timeout time.Duration) error {
	// do nothing
	return nil
}
