package gorillapub

import (
	"context"
	"time"

	"github.com/influx6/sabuhp/pubsub"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/supabaiza"

	"github.com/influx6/npkg/nxid"
)

type PubSub struct {
	ctx       context.Context
	manager   *pubsub.TransportManager
	canceler  context.CancelFunc
	config    PubSubConfig
	hub       *GorillaHub
	doActions chan func()
}

type PubSubConfig struct {
	ID            nxid.ID
	Transport     supabaiza.Transport
	Codec         sabuhp.Codec
	Ctx           context.Context
	Logger        sabuhp.Logger
	OnClosure     SocketNotification
	OnOpen        SocketNotification
	ConfigHandler ConfigCreator
	MaxWaitToSend time.Duration
}

func (b *PubSubConfig) ensure() {
	if b.ID.IsNil() {
		panic("PubConfig.ID is required")
	}
	if b.Ctx == nil {
		panic("PubConfig.Ctx is required")
	}
	if b.Logger == nil {
		panic("PubConfig.Logger is required")
	}
	if b.Codec == nil {
		panic("PubConfig.Codec is required")
	}
	if b.MaxWaitToSend <= 0 {
		b.MaxWaitToSend = time.Second * 5
	}
}

func NewPubSub(config PubSubConfig) *PubSub {
	config.ensure()

	var newCtx, canceler = context.WithCancel(config.Ctx)
	var manager = pubsub.NewTransportManager(newCtx, config.Transport, config.Logger)
	var pub = PubSub{
		config:   config,
		ctx:      newCtx,
		canceler: canceler,
		manager:  manager,
	}

	pub.hub = NewGorillaHub(HubConfig{
		Ctx:           newCtx,
		Logger:        config.Logger,
		Handler:       pub.handleSocketMessage,
		OnClosure:     pub.manageSocketClosed,
		OnOpen:        pub.manageSocketOpened,
		ConfigHandler: config.ConfigHandler,
	})
	return &pub
}

func (gp *PubSub) Wait() {
	gp.manager.Wait()
	gp.hub.Wait()
}

func (gp *PubSub) Hub() *GorillaHub {
	return gp.hub
}

func (gp *PubSub) Stop() {
	gp.canceler()
	gp.hub.Wait()
	gp.manager.Wait()
}

func (gp *PubSub) Start() {
	gp.hub.Start()
}

// Listen creates a local subscription for listening to an underline message
// for a giving topic.
func (gp *PubSub) Listen(topic string, handler supabaiza.TransportResponse) supabaiza.Channel {
	return gp.manager.Listen(topic, handler)
}

// SendToOne selects a random recipient which will receive the message to be delivered
// for processing.
func (gp *PubSub) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	if sendErr := gp.manager.SendToOne(data, timeout); sendErr != nil {
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
func (gp *PubSub) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	if sendErr := gp.manager.SendToAll(data, timeout); sendErr != nil {
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

func (gp *PubSub) manageSocketOpened(socket *GorillaSocket) {
	if gp.config.OnOpen != nil {
		gp.config.OnOpen(socket)
	}
}

func (gp *PubSub) manageSocketClosed(socket *GorillaSocket) {
	gp.manager.UnlistenAllWithId(socket.ID())

	if gp.config.OnClosure != nil {
		gp.config.OnClosure(socket)
	}
}

func (gp *PubSub) handleSocketMessage(message []byte, socket *GorillaSocket) error {
	var msg, msgErr = gp.config.Codec.Decode(message)
	if msgErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to decoded message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", msgErr.Error())
			event.String("message", string(message))
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))
		return msgErr
	}

	var topicString = string(msg.Payload)
	switch msg.Topic {
	case SUBSCRIBE:
		gp.config.Logger.Log(njson.MJSON("handling subscription message", func(event npkg.Encoder) {
			event.String("topic", msg.Topic)
			event.String("subscription_topic", topicString)
			event.String("message", msg.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))

		gp.socketListenToTopic(topicString, socket)
	case UNSUBSCRIBE:
		gp.config.Logger.Log(njson.MJSON("handling unsubscription message", func(event npkg.Encoder) {
			event.String("topic", msg.Topic)
			event.String("subscription_topic", topicString)
			event.String("message", msg.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))

		gp.manager.UnlistenWithId(topicString, socket.ID())
	default:
		if sendErr := gp.manager.SendToAll(msg, gp.config.MaxWaitToSend); sendErr != nil {
			gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
				event.String("hub_id", gp.config.ID.String())
				event.String("error", sendErr.Error())
				event.String("socket_id", socket.id.String())
				event.Object("socket_stat", socket.Stat())
			}))
		}
	}
	return nil
}

func (gp *PubSub) socketListenToTopic(topic string, socket *GorillaSocket) {
	_ = gp.manager.ListenWithId(
		socket.ID(),
		topic,
		func(message *sabuhp.Message, transport supabaiza.Transport) supabaiza.MessageErr {
			var msgBytes, msgBytesErr = gp.config.Codec.Encode(message)
			if msgBytesErr != nil {
				gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
					event.String("hub_id", gp.config.ID.String())
					event.String("error", msgBytesErr.Error())
					event.String("socket_id", socket.id.String())
					event.Object("socket_stat", socket.Stat())
				}))
				return supabaiza.WrapErr(msgBytesErr, false)
			}

			if sendErr := socket.Send(msgBytes, gp.config.MaxWaitToSend); sendErr != nil {
				gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
					event.String("hub_id", gp.config.ID.String())
					event.String("error", sendErr.Error())
					event.String("socket_id", socket.id.String())
					event.Object("socket_stat", socket.Stat())
				}))
				return supabaiza.WrapErr(sendErr, false)
			}

			return nil
		})

	var okMessage = OK(gp.config.ID.String(), socket.socket.LocalAddr().String())
	var okBytes, okBytesErr = gp.config.Codec.Encode(okMessage)
	if okBytesErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", okBytesErr.Error())
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))
		return
	}

	if sendErr := socket.Send(okBytes, gp.config.MaxWaitToSend); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", sendErr.Error())
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))
		return
	}

	gp.config.Logger.Log(njson.MJSON("added socket as subscriber to topic", func(event npkg.Encoder) {
		event.String("topic", topic)
		event.String("hub_id", gp.config.ID.String())
		event.String("socket_id", socket.id.String())
		event.Object("socket_stat", socket.Stat())
	}))
}
