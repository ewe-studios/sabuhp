package managers

import (
	"context"
	"time"

	"github.com/influx6/npkg/nunsafe"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"
	"github.com/influx6/sabuhp"
)

var (
	DefaultMaxWaitToSend = 5 * time.Second
)

// Manager implements a concept which allows separation of actual
// network transport protocol management from the actual pubsub operation
// of subscription and message delivery.
//
// It uses a TransportManager to support multiple subscribers per topic (which somewhat
// limits the capacity of tracking if each subscriber received all messages and redelivery)
// but allows us use multiple protocols to talk to the same mailer transportManager to handle message
// delivery to and from these protocols.
type Manager struct {
	ctx              context.Context
	transportManager *TransportManager
	canceler         context.CancelFunc
	config           ManagerConfig
}

type ManagerConfig struct {
	ID            nxid.ID
	Transport     sabuhp.Transport
	Codec         sabuhp.Codec
	Ctx           context.Context
	Logger        sabuhp.Logger
	OnClosure     SocketNotification
	OnOpen        SocketNotification
	MaxWaitToSend time.Duration
}

func (b *ManagerConfig) ensure() {
	if b.ID.IsNil() {
		panic("PubConfig.ID is required")
	}
	if b.Ctx == nil {
		panic("PubConfig.ctx is required")
	}
	if b.Logger == nil {
		panic("PubConfig.Logger is required")
	}
	if b.Codec == nil {
		panic("PubConfig.Codec is required")
	}
	if b.MaxWaitToSend <= 0 {
		b.MaxWaitToSend = DefaultMaxWaitToSend
	}
}

func NewManager(config ManagerConfig) *Manager {
	config.ensure()

	var newCtx, canceler = context.WithCancel(config.Ctx)
	var manager = NewTransportManager(newCtx, config.Transport, config.Logger)
	return &Manager{
		config:           config,
		ctx:              newCtx,
		canceler:         canceler,
		transportManager: manager,
	}
}

// ServiceConfig provides attributes that a transport
// can use to deliver sockets to a Manager.
//
// This intends allow other transport protocols (think http, tcp) take provided
// config and route their messages through the
// transportManager.
type ServiceConfig struct {
	Ctx       context.Context
	Handler   sabuhp.MessageHandler
	OnClosure SocketNotification
	OnOpen    SocketNotification
}

func (gp *Manager) Config() ServiceConfig {
	return ServiceConfig{
		Ctx:       gp.ctx,
		Handler:   gp.HandleSocketBytesMessage,
		OnClosure: gp.ManageSocketClosed,
		OnOpen:    gp.ManageSocketOpened,
	}
}

func (gp *Manager) Wait() {
	gp.transportManager.Wait()
}

func (gp *Manager) Ctx() context.Context {
	return gp.ctx
}

func (gp *Manager) Stop() {
	gp.canceler()
	gp.transportManager.Wait()
}

func (gp *Manager) Codec() sabuhp.Codec {
	return gp.config.Codec
}

func (gp *Manager) Conn() sabuhp.Conn {
	return gp.transportManager.Conn()
}

// Listen creates a local subscription for listening to an underline message
// for a giving topic.
func (gp *Manager) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	return gp.transportManager.Listen(topic, handler)
}

// SendToOne selects a random recipient which will receive the message to be delivered
// for processing.
func (gp *Manager) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	if sendErr := gp.transportManager.SendToOne(data, timeout); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to send message on transport", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", sendErr.Error())
			event.String("message", data.String())
		}))
	}

	gp.config.Logger.Log(njson.MJSON("sent message", func(event npkg.Encoder) {
		event.String("hub_id", gp.config.ID.String())
		event.String("message", data.String())
	}))
	return nil
}

// SendToAll delivers to all listeners the provided message within specific timeout.
func (gp *Manager) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	if sendErr := gp.transportManager.SendToAll(data, timeout); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to send message on transport", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", sendErr.Error())
			event.String("message", data.String())
		}))
	}

	gp.config.Logger.Log(njson.MJSON("sent message", func(event npkg.Encoder) {
		event.String("hub_id", gp.config.ID.String())
		event.String("message", data.String())
	}))
	return nil
}

func (gp *Manager) ManageSocketOpened(socket sabuhp.Socket) {
	if gp.config.OnOpen != nil {
		gp.config.OnOpen(socket)
	}
}

func (gp *Manager) ManageSocketClosed(socket sabuhp.Socket) {
	gp.transportManager.UnlistenAllWithId(socket.ID())

	if gp.config.OnClosure != nil {
		gp.config.OnClosure(socket)
	}
}

func (gp *Manager) HandleSocketBytesMessage(message []byte, socket sabuhp.Socket) error {
	var msg, msgErr = gp.config.Codec.Decode(message)
	if msgErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to decoded message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", msgErr.Error())
			event.String("message", string(message))
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))
		return msgErr
	}

	return gp.HandleSocketMessage(msg, socket)
}

func (gp *Manager) HandleSocketBytesMessageFromOverriding(message []byte, socket sabuhp.Socket, overridingTransport sabuhp.Transport) error {
	var msg, msgErr = gp.config.Codec.Decode(message)
	if msgErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to decoded message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", msgErr.Error())
			event.String("message", string(message))
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))
		return msgErr
	}

	msg.OverridingTransport = overridingTransport
	return gp.HandleSocketMessage(msg, socket)
}

func (gp *Manager) HandleSocketMessage(msg *sabuhp.Message, socket sabuhp.Socket) error {
	var topicString = string(msg.Payload)
	switch msg.Topic {
	case sabuhp.SUBSCRIBE:
		gp.config.Logger.Log(njson.MJSON("handling subscription message", func(event npkg.Encoder) {
			event.String("topic", msg.Topic)
			event.String("subscription_topic", topicString)
			event.String("message", msg.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))

		gp.SocketListenToTopic(topicString, socket)
	case sabuhp.UNSUBSCRIBE:
		gp.config.Logger.Log(njson.MJSON("handling unsubscription message", func(event npkg.Encoder) {
			event.String("topic", msg.Topic)
			event.String("subscription_topic", topicString)
			event.String("message", msg.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))

		gp.transportManager.UnlistenWithId(topicString, socket.ID())
	default:
		if msg.Delivery == sabuhp.SendToOne {
			if sendErr := gp.transportManager.SendToOne(msg, gp.config.MaxWaitToSend); sendErr != nil {
				gp.config.Logger.Log(njson.MJSON("failed to send message", func(event npkg.Encoder) {
					event.String("hub_id", gp.config.ID.String())
					event.String("error", sendErr.Error())
					event.String("socket_id", socket.ID().String())
					event.Object("socket_stat", socket.Stat())
				}))
				return sendErr
			}
			return nil
		}

		if sendErr := gp.transportManager.SendToAll(msg, gp.config.MaxWaitToSend); sendErr != nil {
			gp.config.Logger.Log(njson.MJSON("failed to send message", func(event npkg.Encoder) {
				event.String("hub_id", gp.config.ID.String())
				event.String("error", sendErr.Error())
				event.String("socket_id", socket.ID().String())
				event.Object("socket_stat", socket.Stat())
			}))
			return sendErr
		}
	}
	return nil
}

func (gp *Manager) SocketListenToTopic(topic string, socket sabuhp.Socket) {
	_ = gp.transportManager.ListenWithId(
		socket.ID(),
		topic,
		sabuhp.TransportResponseFunc(func(message *sabuhp.Message, transport sabuhp.Transport) sabuhp.MessageErr {
			var msgBytes, msgBytesErr = gp.config.Codec.Encode(message)
			if msgBytesErr != nil {
				gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
					event.String("hub_id", gp.config.ID.String())
					event.String("error", msgBytesErr.Error())
					event.String("socket_id", socket.ID().String())
					event.Object("socket_stat", socket.Stat())
				}))
				return sabuhp.WrapErr(msgBytesErr, false)
			}

			gp.config.Logger.Log(njson.MJSON("encoded message", func(event npkg.Encoder) {
				event.String("message", nunsafe.Bytes2String(msgBytes))
				event.String("hub_id", gp.config.ID.String())
				event.String("socket_id", socket.ID().String())
				event.Object("socket_stat", socket.Stat())
			}))

			if sendErr := socket.Send(msgBytes, message.MessageMeta, gp.config.MaxWaitToSend); sendErr != nil {
				gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
					event.String("hub_id", gp.config.ID.String())
					event.String("error", sendErr.Error())
					event.String("socket_id", socket.ID().String())
					event.Object("socket_stat", socket.Stat())
				}))
				return sabuhp.WrapErr(sendErr, false)
			}

			return nil
		}))

	var okMessage = sabuhp.OK(gp.config.ID.String(), socket.LocalAddr().String())
	var okBytes, okBytesErr = gp.config.Codec.Encode(okMessage)
	if okBytesErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", okBytesErr.Error())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))
		return
	}

	if sendErr := socket.Send(okBytes, okMessage.MessageMeta, gp.config.MaxWaitToSend); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", sendErr.Error())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))
		return
	}

	gp.config.Logger.Log(njson.MJSON("added socket as subscriber to topic", func(event npkg.Encoder) {
		event.String("topic", topic)
		event.String("hub_id", gp.config.ID.String())
		event.String("socket_id", socket.ID().String())
		event.Object("socket_stat", socket.Stat())
	}))
}
