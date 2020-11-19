package expr

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/influx6/sabuhp/transport/gorillapub"

	"github.com/influx6/sabuhp/pubsub"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"
	"github.com/influx6/sabuhp"
)

// socketHub can either be a GorillaSocket or
// a message handler function for local listeners.
type socketHub struct {
	id      nxid.ID
	socket  sabuhp.Socket
	localFn sabuhp.TransportResponse
}

type SocketRegistry map[nxid.ID]socketHub

type GorillaPub struct {
	ctx       context.Context
	canceler  context.CancelFunc
	config    PubConfig
	hub       *gorillapub.GorillaHub
	waiter    sync.WaitGroup
	starter   *sync.Once
	ender     *sync.Once
	doActions chan func()
	chl       sync.RWMutex
	channels  map[string]SocketRegistry
}

type PubConfig struct {
	ID            nxid.ID
	Codec         sabuhp.Codec
	Ctx           context.Context
	Logger        sabuhp.Logger
	OnClosure     pubsub.SocketNotification
	OnOpen        pubsub.SocketNotification
	ConfigHandler gorillapub.ConfigCreator
	MaxWaitToSend time.Duration
}

func (b *PubConfig) ensure() {
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
		b.MaxWaitToSend = time.Second * 5
	}
}

func NewGorillaPub(config PubConfig) *GorillaPub {
	config.ensure()

	var newCtx, canceler = context.WithCancel(config.Ctx)
	var pub = GorillaPub{
		config:   config,
		ctx:      newCtx,
		canceler: canceler,
		channels: map[string]SocketRegistry{},
	}

	pub.hub = gorillapub.NewGorillaHub(gorillapub.HubConfig{
		Ctx:           newCtx,
		Logger:        config.Logger,
		Handler:       pub.handleSocketMessage,
		OnClosure:     pub.manageSocketClosed,
		OnOpen:        pub.manageSocketOpened,
		ConfigHandler: config.ConfigHandler,
	})
	pub.init()
	return &pub
}

func (gp *GorillaPub) init() {
	var starter sync.Once
	var ender sync.Once
	gp.starter = &starter
	gp.ender = &ender
	gp.doActions = make(chan func())
}

func (gp *GorillaPub) Wait() {
	gp.waiter.Wait()
}

func (gp *GorillaPub) Hub() *gorillapub.GorillaHub {
	return gp.hub
}

func (gp *GorillaPub) Stop() {
	gp.ender.Do(func() {
		gp.canceler()
		gp.hub.Wait()
		gp.waiter.Wait()
	})
}

func (gp *GorillaPub) Start() {
	gp.waiter.Add(1)
	gp.starter.Do(func() {
		gp.hub.Start()
		go gp.manageSubscriptions()
	})
}

// Conn returns nil as the underline connection
// is not a single instance. But a varying difference
// sockets managed by the pub for message delivery.
func (gp *GorillaPub) Conn() sabuhp.Conn {
	return nil
}

// Listen creates a local subscription for listening to an underline message
// for a giving topic.
func (gp *GorillaPub) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	var sub socketHub
	sub.id = nxid.New()
	sub.localFn = handler

	gp.chl.Lock()
	{
		var subscriptions, hasSubs = gp.channels[topic]
		if !hasSubs {
			subscriptions = map[nxid.ID]socketHub{}
		}

		subscriptions[sub.id] = sub
		gp.channels[topic] = subscriptions
	}
	gp.chl.Unlock()

	return &localSubscription{
		id:    sub.id,
		topic: topic,
		pub:   gp,
	}
}

type localSubscription struct {
	id    nxid.ID
	topic string
	pub   *GorillaPub
}

func (l *localSubscription) Err() error {
	return nil
}

func (l *localSubscription) Close() {
	l.pub.unListen(l.topic, l.id)
}

func (gp *GorillaPub) listenToSocket(topic string, socket sabuhp.Socket) {
	var sub socketHub
	sub.id = nxid.New()
	sub.socket = socket

	gp.chl.Lock()
	{
		var subscriptions, hasSubs = gp.channels[topic]
		if !hasSubs {
			subscriptions = map[nxid.ID]socketHub{}
		}

		subscriptions[sub.id] = sub
		gp.channels[topic] = subscriptions
	}
	gp.chl.Unlock()

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

	if sendErr := socket.Send(okBytes, gp.config.MaxWaitToSend); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", okBytesErr.Error())
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

// SendToOne selects a random recipient which will receive the message to be delivered
// for processing.
func (gp *GorillaPub) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	data.Delivery = sabuhp.SendToOne
	var encoded, encodedErr = gp.config.Codec.Encode(data)
	if encodedErr != nil {
		return nerror.WrapOnly(encodedErr)
	}

	gp.chl.RLock()
	var subscriptions, hasSubs = gp.channels[data.Topic]
	if !hasSubs {
		gp.chl.RUnlock()
		return nerror.New("no subscription for topic")
	}
	gp.chl.RUnlock()

	var listenersSize = len(subscriptions)
	var randomCandidate = rand.Intn(listenersSize)

	var idList = make([]nxid.ID, 0, listenersSize)
	for id := range subscriptions {
		idList = append(idList, id)
	}

	var candidate = subscriptions[idList[randomCandidate]]
	gp.deliverMessage(data, encoded, &candidate)
	return nil
}

// SendToAll delivers to all listeners the provided message within specific timeout.
func (gp *GorillaPub) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	data.Delivery = sabuhp.SendToAll
	var encoded, encodedErr = gp.config.Codec.Encode(data)
	if encodedErr != nil {
		return nerror.WrapOnly(encodedErr)
	}

	gp.chl.RLock()
	var subscriptions, hasSubs = gp.channels[data.Topic]
	if !hasSubs {
		gp.chl.RUnlock()
		return nerror.New("no subscription for topic")
	}
	gp.chl.RUnlock()

	for _, sub := range subscriptions {
		gp.deliverMessage(data, encoded, &sub)
	}

	return nil
}

func (gp *GorillaPub) deliverMessage(data *sabuhp.Message, encodedMessage []byte, sub *socketHub) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			gp.config.Logger.Log(njson.MJSON("panic occurred", func(event npkg.Encoder) {
				event.String("hub_id", gp.config.ID.String())
				event.String("panic_data", fmt.Sprintf("%#v", recoverErr))
			}))
		}
	}()

	// its a local function
	if sub.socket != nil {
		if socketSendErr := sub.socket.Send(encodedMessage, gp.config.MaxWaitToSend); socketSendErr != nil {
			gp.config.Logger.Log(njson.MJSON("failed to deliver message to socket", func(event npkg.Encoder) {
				event.String("hub_id", gp.config.ID.String())
				event.String("error", socketSendErr.Error())
				event.String("socket_id", sub.socket.ID().String())
				event.Object("socket_stat", sub.socket.Stat())
			}))
		}
		return
	}

	if err := sub.localFn.Handle(data, gp); nil != err {
		gp.config.Logger.Log(njson.MJSON("failed to handle message by function", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", err.Error())
			event.String("socket_id", sub.socket.ID().String())
			event.Object("socket_stat", sub.socket.Stat())
		}))
	}
}

func (gp *GorillaPub) deliverMessageToSubs(b []byte, data *sabuhp.Message, socket sabuhp.Socket) {
	gp.config.Logger.Log(njson.MJSON("received message from socket", func(event npkg.Encoder) {
		event.String("topic", data.Topic)
		event.String("message", data.String())
		event.String("hub_id", gp.config.ID.String())
		event.String("socket_id", socket.ID().String())
		event.Object("socket_stat", socket.Stat())
	}))

	gp.chl.RLock()
	var subscriptions, hasSubscription = gp.channels[data.Topic]
	gp.chl.RUnlock()

	if !hasSubscription {
		gp.config.Logger.Log(njson.MJSON("no topic subscription exists", func(event npkg.Encoder) {
			event.String("topic", data.Topic)
			event.String("message", data.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))
		return
	}

	gp.config.Logger.Log(njson.MJSON("topic subscription exists", func(event npkg.Encoder) {
		event.Int("total_subscriptions", len(subscriptions))
		event.String("topic", data.Topic)
		event.String("message", data.String())
		event.String("hub_id", gp.config.ID.String())
		event.String("socket_id", socket.ID().String())
		event.Object("socket_stat", socket.Stat())
	}))

	for _, subs := range subscriptions {
		gp.deliverMessage(data, b, &subs)
	}
}

func (gp *GorillaPub) deliverMessageToSocket(data *sabuhp.Message, sub *gorillapub.GorillaSocket) error {
	var encoded, encodedErr = gp.config.Codec.Encode(data)
	if encodedErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", encodedErr.Error())
			event.String("socket_id", sub.ID().String())
			event.Object("socket_stat", sub.Stat())
		}))
		return encodedErr
	}

	if sendErr := sub.Send(encoded, gp.config.MaxWaitToSend); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", encodedErr.Error())
			event.String("socket_id", sub.ID().String())
			event.Object("socket_stat", sub.Stat())
		}))
		return sendErr
	}
	return nil
}

func (gp *GorillaPub) unListen(topic string, id nxid.ID) {
	gp.chl.Lock()
	defer gp.chl.Unlock()
	if subscriptions, hasSubs := gp.channels[topic]; hasSubs {
		delete(subscriptions, id)
	}
}

func (gp *GorillaPub) unListenSocket(topic string, socket sabuhp.Socket) {
	gp.chl.Lock()
	if subscriptions, hasSubs := gp.channels[topic]; hasSubs {
		for key, sub := range subscriptions {
			if sub.socket == socket {
				delete(subscriptions, key)
			}
		}
	}
	gp.chl.Unlock()

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

	if sendErr := socket.Send(okBytes, gp.config.MaxWaitToSend); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to send ok message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", okBytesErr.Error())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))
	}
}

func (gp *GorillaPub) manageSocketOpened(socket sabuhp.Socket) {
	if gp.config.OnOpen != nil {
		gp.config.OnOpen(socket)
	}
}

func (gp *GorillaPub) manageSocketClosed(socket sabuhp.Socket) {
	gp.chl.Lock()
	// loop through all subscriptions and remove cases where
	// such a socket is registered.
	for _, subs := range gp.channels {
		delete(subs, socket.ID())
	}
	gp.chl.Unlock()

	if gp.config.OnClosure != nil {
		gp.config.OnClosure(socket)
	}
}

func (gp *GorillaPub) handleSocketMessage(message []byte, socket sabuhp.Socket) error {
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

		gp.listenToSocket(topicString, socket)
	case sabuhp.UNSUBSCRIBE:
		gp.config.Logger.Log(njson.MJSON("handling unsubscription message", func(event npkg.Encoder) {
			event.String("topic", msg.Topic)
			event.String("subscription_topic", topicString)
			event.String("message", msg.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))

		gp.unListenSocket(topicString, socket)
	default:
		gp.config.Logger.Log(njson.MJSON("sending message to possible subscribers", func(event npkg.Encoder) {
			event.String("topic", msg.Topic)
			event.String("subscription_topic", topicString)
			event.String("message", msg.String())
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.ID().String())
			event.Object("socket_stat", socket.Stat())
		}))

		gp.deliverMessageToSubs(message, msg, socket)
	}
	return nil
}

func (gp *GorillaPub) manageSubscriptions() {
	defer gp.waiter.Done()

doLoop:
	for {
		select {
		case <-gp.ctx.Done():
			break doLoop
		case action := <-gp.doActions:
			action()
		}
	}
}
