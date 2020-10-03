package supabaiza_test

import (
	"time"

	"github.com/influx6/sabuhp/supabaiza"
)

type NoPubSub struct{}

func (n NoPubSub) Channel(topic string, callback supabaiza.ChannelResponse) {
	// do nothing
}

func (n NoPubSub) Send(message *supabaiza.Message, timeout time.Duration) error {
	// do nothing
	return nil
}
