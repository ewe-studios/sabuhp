package testingutils

import (
	"log"
	"time"

	"github.com/ewe-studios/sabuhp"

	"github.com/influx6/npkg/njson"
)

type SubChannel struct {
	T       string
	G       string
	Handler sabuhp.TransportResponse
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
	ConnFunc      func() sabuhp.Conn
	SendToOneFunc func(data *sabuhp.Message, timeout time.Duration) error
	SendToAllFunc func(data *sabuhp.Message, timeout time.Duration) error
	ListenFunc    func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel
}

func (t TransportImpl) Conn() sabuhp.Conn {
	return t.ConnFunc()
}

func (t TransportImpl) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	return t.ListenFunc(topic, handler)
}

func (t TransportImpl) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToOneFunc(data, timeout)
}

func (t TransportImpl) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToAllFunc(data, timeout)
}
