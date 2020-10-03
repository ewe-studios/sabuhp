package supabaiza

import (
	"time"

	"github.com/influx6/sabuhp"
)

type Transport interface {
	Request() *sabuhp.Request
}

// a message.
type ChannelResponse func(data interface{}, sub PubSub)

type PubSub interface {
	// Channel creates a callback which exists to receive specific
	// messages on a giving topic.
	Channel(topic string, callback ChannelResponse)

	// Send message across the underline transport
	// if timeout is > 0 then using it as send timeout.
	Send(message *Message, timeout time.Duration) error
}
