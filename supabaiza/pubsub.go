package supabaiza

import (
	"context"
	"sync"
	"time"

	"github.com/influx6/sabuhp"
)

type Transport interface {
	Request() *sabuhp.Request
}

// ChannelResponse represents a message giving callback for
// underline response.
type ChannelResponse func(data *Message, sub PubSub)

// Channel represents a generated subscription on a
// topic which provides the giving callback an handler
// to define the point at which the channel should be
// closed and stopped from receiving updates.
type Channel interface {
	Close()
}

type PubSub interface {
	// Channel creates a callback which exists to receive specific
	// messages on a giving topic.
	Channel(topic string, callback ChannelResponse) Channel

	// Send message across the underline transport
	// if timeout is > 0 then using it as send timeout.
	Send(message *Message, timeout time.Duration) error
}

type PubSubTopic struct {
	topic       string
	ctx         context.Context
	messages    chan *Message
	subscribers []ChannelResponse
}

func (ps *PubSubTopic) deliver(message *Message) {
	select {
	case ps.messages <- message:
		return
	case <-ps.ctx.Done():
	}
}

type PubSubImpl struct {
	Transport     Transport
	tml           sync.RWMutex
	topicMappings map[string]*PubSubTopic
}

func (p PubSubImpl) Channel(topic string, callback ChannelResponse) Channel {
	panic("implement me")
}

func (p PubSubImpl) Send(message *Message, timeout time.Duration) error {
	panic("implement me")
}
