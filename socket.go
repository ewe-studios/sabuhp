package sabuhp

import (
	"net"
	"time"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nxid"
)

// MessageHandler defines the function contract a sabuhp.Socket uses
// to handle a message.
//
// Be aware that returning an error from the handler to the Gorilla sabuhp.Socket
// will cause the immediate closure of that socket and ending communication
// with the client and the error will be logged. So unless your intention is to
// end the connection, handle it yourself.
type MessageHandler func(b []byte, from Socket) error

type SocketStat struct {
	Addr       net.Addr
	RemoteAddr net.Addr
	Id         string
	Sent       int64
	Received   int64
	Handled    int64
}

func (g SocketStat) EncodeObject(encoder npkg.ObjectEncoder) {
	encoder.String("id", g.Id)
	encoder.Int64("total_sent", g.Sent)
	encoder.Int64("total_handled", g.Handled)
	encoder.Int64("total_received", g.Received)
	encoder.String("addr", g.Addr.String())
	encoder.String("addr_network", g.Addr.Network())
	encoder.String("remote_addr", g.RemoteAddr.String())
	encoder.String("remote_addr_network", g.RemoteAddr.Network())
}

type Socket interface {
	ID() nxid.ID
	Stat() SocketStat
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
	Send([]byte, time.Duration) error
}
