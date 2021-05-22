package sabuhp

import (
	"net"
	"sync"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nthen"
	"github.com/influx6/npkg/nxid"
)

// SocketHandler defines the function contract to be called for a socket instace.
type SocketHandler func(from Socket)

// SocketByteHandler defines the function contract a manager uses
// to handle a message.
//
// Be aware that returning an error from the handler to the Gorilla socket
// will cause the immediate closure of that socket and ending communication
// with the client and the error will be logged. So unless your intention is to
// end the connection, handle it yourself.
type SocketByteHandler func(b []byte, from Socket) error

// SocketMessageHandler defines the function contract a sabuhp.Socket uses
// to handle a message.
//
// Be aware that returning an error from the handler to the Gorilla sabuhp.Socket
// will cause the immediate closure of that socket and ending communication
// with the client and the error will be logged. So unless your intention is to
// end the connection, handle it yourself.
type SocketMessageHandler func(b Message, from Socket) error

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

// Socket defines an underline connection handler which handles
// delivery of messages to the underline stream.
type Socket interface {
	ID() nxid.ID
	Send(...Message)
	Stat() SocketStat
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Listen(SocketMessageHandler)
}

type SocketService interface {
	SocketOpened(Socket)
	SocketClosed(Socket)
}

type SocketServer interface {
	Stream(SocketService)
}

type StreamFunc struct {
	Listen      func(Message, Socket) error
	Subscribe   func(Message, Socket) error
	Unsubscribe func(Message, Socket) error
	Closed      func(Socket)
}

func (st StreamFunc) SocketClosed(socket Socket) {
	if st.Closed == nil {
		return
	}
	st.Closed(socket)
}

func (st StreamFunc) SocketOpened(socket Socket) {
	socket.Listen(func(message Message, from Socket) error {
		if message.Topic == UNSUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				return nerror.New("no unsubscription topic or group provided")
			}
			return st.Unsubscribe(message, from)
		}

		if message.Topic == SUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				return nerror.New("no subscription topic or group provided")
			}
			return st.Subscribe(message, from)
		}

		if err := st.Listen(message, from); err != nil {
			return nerror.WrapOnly(err)
		}

		return nil
	})
}

type StreamBus struct {
	Logger   Logger
	Bus      MessageBus
	ml       sync.RWMutex
	channels map[nxid.ID][]Channel
}

func NewStreamBus(logger Logger, bus MessageBus) *StreamBus {
	return &StreamBus{Logger: logger, Bus: bus, channels: map[nxid.ID][]Channel{}}
}

// WithBus returns the instance of a StreamBus which will be connected to the provided SocketServer
// and handle delivery of messages to a message bus and subscription/unsubcriptions as well.
func WithBus(logger Logger, socketServer SocketServer, bus MessageBus) *StreamBus {
	var stream = NewStreamBus(logger, bus)
	socketServer.Stream(stream)
	return stream
}

func (st *StreamBus) SocketClosed(socket Socket) {
	st.ml.RLock()
	defer st.ml.RUnlock()
	if channels, hasChannel := st.channels[socket.ID()]; hasChannel {
		for _, channel := range channels {
			channel.Close()
		}
	}
}

func (st *StreamBus) SocketOpened(socket Socket) {
	socket.Listen(func(message Message, from Socket) error {
		if message.Topic == UNSUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				st.Logger.Log(njson.MJSON("failed to handle message for subscription without group or topic", func(event npkg.Encoder) {
					event.Int("_level", int(npkg.ERROR))
					event.String("subscription_topic", message.SubscribeTo)
					event.String("subscription_group", message.SubscribeGroup)
					event.String("socket_id", from.ID().String())
					event.Object("socket_stat", from.Stat())
				}))
			}
			return st.SocketUnsubscribe(message, from)
		}

		if message.Topic == SUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				st.Logger.Log(njson.MJSON("failed to handle message for subscription without group or topic", func(event npkg.Encoder) {
					event.Int("_level", int(npkg.ERROR))
					event.String("subscription_topic", message.SubscribeTo)
					event.String("subscription_group", message.SubscribeGroup)
					event.String("socket_id", from.ID().String())
					event.Object("socket_stat", from.Stat())
				}))
			}
			return st.SocketSubscribe(message, from)
		}

		if err := st.SocketBusSend(message, from); err != nil {
			st.Logger.Log(njson.MJSON("failed to handle message from from", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.Error("error", err)
				event.String("socket_id", from.ID().String())
				event.Object("socket_stat", from.Stat())
			}))
			return err
		}

		return nil
	})
}

func (st *StreamBus) SocketSubscribe(b Message, socket Socket) MessageErr {
	var channel = st.Bus.Listen(b.SubscribeTo, b.SubscribeGroup, TransportResponseFunc(func(message Message, transport Transport) MessageErr {
		var ft = message.Future
		if message.Future == nil {
			ft = nthen.NewFuture()
			message.Future = ft
		}

		socket.Send(message)
		if sendErr := ft.Err(); sendErr != nil {
			st.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("error", sendErr.Error())
				event.String("socket_id", socket.ID().String())
				event.Object("socket_stat", socket.Stat())
			}))

			var unwrappedErr = nerror.UnwrapDeep(sendErr)
			if unwrappedSendErr, ok := unwrappedErr.(MessageErr); ok {
				return unwrappedSendErr
			}
			return WrapErr(sendErr, false)
		}
		return nil
	}))

	st.ml.Lock()
	var socketChannels, hasSocketChannels = st.channels[socket.ID()]
	if !hasSocketChannels {
		socketChannels = append(socketChannels, channel)
	}
	st.channels[socket.ID()] = socketChannels
	st.ml.Unlock()

	return nil
}

func (st *StreamBus) SocketUnsubscribe(b Message, socket Socket) MessageErr {
	st.ml.RLock()
	defer st.ml.RUnlock()
	if channels, hasChannel := st.channels[socket.ID()]; hasChannel {
		for _, channel := range channels {
			if channel.Topic() == b.SubscribeTo && channel.Group() == b.SubscribeGroup {
				channel.Close()
			}
		}
	}
	return nil
}

func (st *StreamBus) SocketBusSend(b Message, _ Socket) MessageErr {
	var mb = &b
	if mb.Future == nil {
		mb.Future = nthen.NewFuture()
	}

	st.Bus.Send(b)
	return WrapErr(mb.Future.Err(), false)
}

type StreamBusRelay struct {
	Logger   Logger
	BusRelay *BusRelay
	Bus      MessageBus
}

func NewStreamBusRelay(logger Logger, bus MessageBus, relay *BusRelay) *StreamBusRelay {
	return &StreamBusRelay{Logger: logger, Bus: bus, BusRelay: relay}
}

// WithBusRelay returns the instance of a StreamBusRelay which will be connected to the provided SocketServer
// and handle delivery of messages to a message bus and subscription to a target relay.
func WithBusRelay(logger Logger, socketServer SocketServer, bus MessageBus, relay *BusRelay) *StreamBusRelay {
	var stream = NewStreamBusRelay(logger, bus, relay)
	socketServer.Stream(stream)
	return stream
}

func (st *StreamBusRelay) SocketClosed(socket Socket) {
	st.BusRelay.Relay.UnlistenAllWithId(socket.ID())
}

func (st *StreamBusRelay) SocketOpened(socket Socket) {
	socket.Listen(func(message Message, from Socket) error {
		if message.Topic == UNSUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				st.Logger.Log(njson.MJSON("failed to handle message for subscription without group or topic", func(event npkg.Encoder) {
					event.Int("_level", int(npkg.ERROR))
					event.String("subscription_topic", message.SubscribeTo)
					event.String("subscription_group", message.SubscribeGroup)
					event.String("socket_id", from.ID().String())
					event.Object("socket_stat", from.Stat())
				}))
			}
			return st.SocketUnsubscribe(message, from)
		}

		if message.Topic == SUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				st.Logger.Log(njson.MJSON("failed to handle message for subscription without group or topic", func(event npkg.Encoder) {
					event.Int("_level", int(npkg.ERROR))
					event.String("subscription_topic", message.SubscribeTo)
					event.String("subscription_group", message.SubscribeGroup)
					event.String("socket_id", from.ID().String())
					event.Object("socket_stat", from.Stat())
				}))
			}
			return st.SocketSubscribe(message, from)
		}

		if err := st.SocketBusSend(message, from); err != nil {
			st.Logger.Log(njson.MJSON("failed to handle message from from", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.Error("error", err)
				event.String("socket_id", from.ID().String())
				event.Object("socket_stat", from.Stat())
			}))
			return err
		}

		return nil
	})
}

func (st *StreamBusRelay) SocketSubscribe(b Message, socket Socket) MessageErr {
	var group = st.BusRelay.Group(b.SubscribeTo, b.SubscribeGroup)
	var channel = group.Listen(TransportResponseFunc(func(message Message, transport Transport) MessageErr {
		var ft = message.Future
		if message.Future == nil {
			ft = nthen.NewFuture()
			message.Future = ft
		}

		socket.Send(message)
		if sendErr := ft.Err(); sendErr != nil {
			st.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("error", sendErr.Error())
				event.String("socket_id", socket.ID().String())
				event.Object("socket_stat", socket.Stat())
			}))

			var unwrappedErr = nerror.UnwrapDeep(sendErr)
			if unwrappedSendErr, ok := unwrappedErr.(MessageErr); ok {
				return unwrappedSendErr
			}
			return WrapErr(sendErr, false)
		}
		return nil
	}))
	if b.Future != nil {
		b.Future.WithValue(channel)
	}
	return nil
}

func (st *StreamBusRelay) SocketUnsubscribe(b Message, socket Socket) MessageErr {
	var group = st.BusRelay.Group(b.SubscribeTo, b.SubscribeGroup)
	group.Remove(socket.ID())
	if b.Future != nil {
		b.Future.WithValue(nil)
	}
	return nil
}

func (st *StreamBusRelay) SocketBusSend(b Message, _ Socket) MessageErr {
	var mb = &b
	if mb.Future == nil {
		mb.Future = nthen.NewFuture()
	}

	st.Bus.Send(b)
	var itemErr = mb.Future.Err()
	if itemErr != nil {
		return WrapErr(mb.Future.Err(), false)
	}
	return nil
}

type StreamRelay struct {
	Logger Logger
	Relay  *PbRelay
	Bus    MessageBus
}

func NewStreamRelay(logger Logger, bus MessageBus, relay *PbRelay) *StreamRelay {
	return &StreamRelay{Logger: logger, Bus: bus, Relay: relay}
}

// WithRelay returns the instance of a StreamRelay which will be connected to the provided SocketServer
// and handle delivery of messages to a message bus and subscription to a target relay.
func WithRelay(logger Logger, socketServer SocketServer, bus MessageBus, relay *PbRelay) *StreamRelay {
	var stream = NewStreamRelay(logger, bus, relay)
	socketServer.Stream(stream)
	return stream
}

func (st *StreamRelay) SocketClosed(socket Socket) {
	st.Relay.UnlistenAllWithId(socket.ID())
}

func (st *StreamRelay) SocketOpened(socket Socket) {
	socket.Listen(func(message Message, from Socket) error {
		if message.Topic == UNSUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				st.Logger.Log(njson.MJSON("failed to handle message for subscription without group or topic", func(event npkg.Encoder) {
					event.Int("_level", int(npkg.ERROR))
					event.String("subscription_topic", message.SubscribeTo)
					event.String("subscription_group", message.SubscribeGroup)
					event.String("socket_id", from.ID().String())
					event.Object("socket_stat", from.Stat())
				}))
			}
			return st.SocketUnsubscribe(message, from)
		}

		if message.Topic == SUBSCRIBE {
			if len(message.SubscribeTo) == 0 || len(message.SubscribeGroup) == 0 {
				st.Logger.Log(njson.MJSON("failed to handle message for subscription without group or topic", func(event npkg.Encoder) {
					event.Int("_level", int(npkg.ERROR))
					event.String("subscription_topic", message.SubscribeTo)
					event.String("subscription_group", message.SubscribeGroup)
					event.String("socket_id", from.ID().String())
					event.Object("socket_stat", from.Stat())
				}))
			}
			return st.SocketSubscribe(message, from)
		}

		if err := st.SocketBusSend(message, from); err != nil {
			st.Logger.Log(njson.MJSON("failed to handle message from from", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.Error("error", err)
				event.String("socket_id", from.ID().String())
				event.Object("socket_stat", from.Stat())
			}))
			return err
		}

		return nil
	})
}

func (st *StreamRelay) SocketSubscribe(b Message, socket Socket) MessageErr {
	var group = st.Relay.Group(b.SubscribeTo, b.SubscribeGroup)
	var channel = group.Listen(TransportResponseFunc(func(message Message, transport Transport) MessageErr {
		var ft = message.Future
		if message.Future == nil {
			ft = nthen.NewFuture()
			message.Future = ft
		}

		socket.Send(message)
		if sendErr := ft.Err(); sendErr != nil {
			st.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("error", sendErr.Error())
				event.String("socket_id", socket.ID().String())
				event.Object("socket_stat", socket.Stat())
			}))

			var unwrappedErr = nerror.UnwrapDeep(sendErr)
			if unwrappedSendErr, ok := unwrappedErr.(MessageErr); ok {
				return unwrappedSendErr
			}
			return WrapErr(sendErr, false)
		}
		return nil
	}))
	if b.Future != nil {
		b.Future.WithValue(channel)
	}
	return nil
}

func (st *StreamRelay) SocketUnsubscribe(b Message, socket Socket) MessageErr {
	var group = st.Relay.Group(b.SubscribeTo, b.SubscribeGroup)
	group.Remove(socket.ID())
	if b.Future != nil {
		b.Future.WithValue(nil)
	}
	return nil
}

func (st *StreamRelay) SocketBusSend(b Message, _ Socket) MessageErr {
	var mb = &b
	if mb.Future == nil {
		mb.Future = nthen.NewFuture()
	}

	st.Bus.Send(b)
	var itemErr = mb.Future.Err()
	if itemErr != nil {
		return WrapErr(mb.Future.Err(), false)
	}
	return nil
}

type SocketServers struct {
	sm      sync.RWMutex
	streams []SocketService
}

func NewSocketServers() *SocketServers {
	return &SocketServers{}
}

func (htp *SocketServers) SocketClosed(socket Socket) {
	htp.sm.RLock()
	defer htp.sm.RUnlock()
	for _, stream := range htp.streams {
		stream.SocketClosed(socket)
	}
}

func (htp *SocketServers) SocketOpened(socket Socket) {
	htp.sm.RLock()
	defer htp.sm.RUnlock()
	for _, stream := range htp.streams {
		stream.SocketOpened(socket)
	}
}

func (htp *SocketServers) Stream(server SocketService) {
	htp.sm.Lock()
	defer htp.sm.Unlock()
	htp.streams = append(htp.streams, server)
}
