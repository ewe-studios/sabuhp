package testingutils

import (
	"log"
	"time"

	"github.com/influx6/sabuhp/pubsub"

	"github.com/influx6/sabuhp"

	"github.com/influx6/sabuhp/supabaiza"

	"github.com/influx6/npkg/njson"
)

var _ supabaiza.Channel = (*NoPubSubChannel)(nil)

type NoPubSubChannel struct {
	Error error
}

func (n NoPubSubChannel) Err() error {
	return n.Error
}

func (n NoPubSubChannel) Close() {
	// do nothing
}

var _ pubsub.PubSub = (*NoPubSub)(nil)

type NoPubSub struct {
	DelegateFunc  func(message *sabuhp.Message, timeout time.Duration) error
	BroadcastFunc func(message *sabuhp.Message, timeout time.Duration) error
	ChannelFunc   func(topic string, callback pubsub.ChannelResponse) supabaiza.Channel
}

func (n NoPubSub) Channel(topic string, callback pubsub.ChannelResponse) supabaiza.Channel {
	if n.ChannelFunc != nil {
		return n.ChannelFunc(topic, callback)
	}
	return &NoPubSubChannel{}
}

func (n NoPubSub) Delegate(message *sabuhp.Message, timeout time.Duration) error {
	if n.DelegateFunc != nil {
		return n.DelegateFunc(message, timeout)
	}
	return nil
}

func (n NoPubSub) Broadcast(message *sabuhp.Message, timeout time.Duration) error {
	if n.BroadcastFunc != nil {
		return n.BroadcastFunc(message, timeout)
	}
	return nil
}

type LoggerPub struct{}

func (l LoggerPub) Log(cb *njson.JSON) {
	log.Println(cb.Message())
	log.Println("")
}

type TransportImpl struct {
	ConnFunc      func() supabaiza.Conn
	SendToOneFunc func(data *sabuhp.Message, timeout time.Duration) error
	SendToAllFunc func(data *sabuhp.Message, timeout time.Duration) error
	ListenFunc    func(topic string, handler supabaiza.TransportResponse) supabaiza.Channel
}

func (t TransportImpl) Conn() supabaiza.Conn {
	return t.ConnFunc()
}

func (t TransportImpl) Listen(topic string, handler supabaiza.TransportResponse) supabaiza.Channel {
	return t.ListenFunc(topic, handler)
}

func (t TransportImpl) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToOneFunc(data, timeout)
}

func (t TransportImpl) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToAllFunc(data, timeout)
}
