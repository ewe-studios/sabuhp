package sabuhp

import (
	"time"

	"github.com/influx6/npkg/njson"
)

type Logger interface {
	njson.Logger
}

// Channel represents a generated subscription on a
// topic which provides the giving callback an handler
// to define the point at which the channel should be
// closed and stopped from receiving updates.
type Channel interface {
	Close()
	Err() error
}

type MessageErr interface {
	error
	ShouldAck() bool
}

func WrapErr(err error, shouldAck bool) MessageErr {
	return messageErr{
		error:     err,
		shouldAck: shouldAck,
	}
}

type messageErr struct {
	error
	shouldAck bool
}

func (m messageErr) ShouldAck() bool {
	return m.shouldAck
}

// Conn defines the connection type which we can retrieve
// and understand the type.
type Conn interface{}

type TransportResponse func(*Message, Transport) MessageErr

// Transport defines what an underline transport system provides.
type Transport interface {
	Conn() Conn
	Listen(topic string, handler TransportResponse) Channel
	SendToOne(data *Message, timeout time.Duration) error
	SendToAll(data *Message, timeout time.Duration) error
}
