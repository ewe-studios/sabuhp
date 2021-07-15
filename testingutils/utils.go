package testingutils

import (
	"github.com/ewe-studios/sabuhp/sabu"
	"log"
	"time"

	"github.com/influx6/npkg/njson"
)

type SubChannel struct {
	T       string
	G       string
	Handler sabu.TransportResponse
}

func (s SubChannel) Topic() string {
	return s.T
}

func (s SubChannel) Group() string {
	return s.G
}

func (s SubChannel) Close() {
	return
}

func (s SubChannel) Err() error {
	return nil
}

type LoggerPub struct{}

func (l LoggerPub) Log(cb *njson.JSON) {
	log.Println(cb.Message())
	log.Println("")
}

type TransportImpl struct {
	ConnFunc      func() sabu.Conn
	SendToOneFunc func(data *sabu.Message, timeout time.Duration) error
	SendToAllFunc func(data *sabu.Message, timeout time.Duration) error
	ListenFunc    func(topic string, handler sabu.TransportResponse) sabu.Channel
}

func (t TransportImpl) Conn() sabu.Conn {
	return t.ConnFunc()
}

func (t TransportImpl) Listen(topic string, handler sabu.TransportResponse) sabu.Channel {
	return t.ListenFunc(topic, handler)
}

func (t TransportImpl) SendToOne(data *sabu.Message, timeout time.Duration) error {
	return t.SendToOneFunc(data, timeout)
}

func (t TransportImpl) SendToAll(data *sabu.Message, timeout time.Duration) error {
	return t.SendToAllFunc(data, timeout)
}
