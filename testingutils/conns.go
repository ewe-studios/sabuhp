package testingutils

import (
	"io"
	"net"
	"time"

	"github.com/Ewe-Studios/websocket"
)

var (
	ErrWriteTimeout = &NetError{msg: "websocket: write timeout", timeout: true, temporary: true}
)

// NetError satisfies the net Error interface.
type NetError struct {
	msg       string
	temporary bool
	timeout   bool
}

func (e *NetError) Error() string   { return e.msg }
func (e *NetError) Temporary() bool { return e.temporary }
func (e *NetError) Timeout() bool   { return e.timeout }

var _ net.Error = ErrWriteTimeout

var (
	LocalAddr  = FakeAddr(1)
	RemoteAddr = FakeAddr(2)
)

type FakeNetConn struct {
	io.Reader
	io.Writer
}

func (c FakeNetConn) Close() error                       { return nil }
func (c FakeNetConn) LocalAddr() net.Addr                { return LocalAddr }
func (c FakeNetConn) RemoteAddr() net.Addr               { return RemoteAddr }
func (c FakeNetConn) SetDeadline(t time.Time) error      { return nil }
func (c FakeNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (c FakeNetConn) SetWriteDeadline(t time.Time) error { return nil }

type FakeAddr int

func (a FakeAddr) Network() string {
	return "net"
}

func (a FakeAddr) String() string {
	return "str"
}

// NewTestConn creates a connection backed by a fake network connection using
// default values for buffering.
func NewTestConn(r io.Reader, w io.Writer, isServer bool) *websocket.Conn {
	return websocket.NewConn(FakeNetConn{Reader: r, Writer: w}, isServer, 1024, 1024, nil, nil, nil)
}
