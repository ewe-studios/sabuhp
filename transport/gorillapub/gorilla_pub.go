package gorillapub

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp/utils"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/supabaiza"

	"github.com/influx6/npkg/nxid"
)

const (
	SUBSCRIBE   = "+SUB"
	UNSUBSCRIBE = "-USUB"
	DONE        = "+OK"
	NOTDONE     = "-NOK"
)

func NOTOK(message string, fromAddr string) supabaiza.Message {
	return supabaiza.Message{
		Topic:    NOTDONE,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func OK(message string, fromAddr string) supabaiza.Message {
	return supabaiza.Message{
		Topic:    DONE,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func UnsubscribeMessage(topic string, fromAddr string) supabaiza.Message {
	return supabaiza.Message{
		Topic:    UNSUBSCRIBE,
		FromAddr: fromAddr,
		Payload:  []byte(topic),
	}
}

func SubscribeMessage(topic string, fromAddr string) supabaiza.Message {
	return supabaiza.Message{
		Topic:    SUBSCRIBE,
		FromAddr: fromAddr,
		Payload:  []byte(topic),
	}
}

// socketHub can either be a GorillaSocket or
// a message handler function for local listeners.
type socketHub struct {
	id      nxid.ID
	socket  *GorillaSocket
	localFn supabaiza.TransportResponse
}

type SocketRegistry map[nxid.ID]socketHub

type GorillaPub struct {
	ctx       context.Context
	canceler  context.CancelFunc
	config    PubConfig
	hub       *GorillaHub
	waiter    sync.WaitGroup
	starter   *sync.Once
	ender     *sync.Once
	doActions chan func()
	channels  map[string]SocketRegistry
}

type PubConfig struct {
	ID            nxid.ID
	Codec         supabaiza.Codec
	Ctx           context.Context
	Logger        sabuhp.Logger
	OnClosure     SocketNotification
	OnOpen        SocketNotification
	ConfigHandler ConfigCreator
	MaxWaitToSend time.Duration
}

func (b *PubConfig) ensure() {
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

func NewGorillaPub(config PubConfig) *GorillaPub {
	config.ensure()

	var newCtx, canceler = context.WithCancel(config.Ctx)
	var pub = GorillaPub{
		config:   config,
		ctx:      newCtx,
		canceler: canceler,
		channels: map[string]SocketRegistry{},
	}

	pub.hub = NewGorillaHub(HubConfig{
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

func (gp *GorillaPub) Hub() *GorillaHub {
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
func (gp *GorillaPub) Conn() supabaiza.Conn {
	return nil
}

func (gp *GorillaPub) unListen(topic string, id nxid.ID) {
	var doAction = func() {
		if subscriptions, hasSubs := gp.channels[topic]; hasSubs {
			delete(subscriptions, id)
		}
	}

	select {
	case gp.doActions <- doAction:
		break
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to register socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
		}))
	}
}

func (gp *GorillaPub) unListenSocket(topic string, socket *GorillaSocket) {
	var doAction = func() {
		if subscriptions, hasSubs := gp.channels[topic]; hasSubs {
			for key, sub := range subscriptions {
				if sub.socket == socket {
					delete(subscriptions, key)
				}
			}
		}
	}

	select {
	case gp.doActions <- doAction:
		break
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to register socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
		}))
	}
}

// Listen creates a local subscription for listening to an underline message
// for a giving topic.
func (gp *GorillaPub) Listen(topic string, handler supabaiza.TransportResponse) supabaiza.Channel {
	var result = make(chan supabaiza.Channel, 1)

	var doAction = func() {
		var sub socketHub
		sub.id = nxid.New()
		sub.localFn = handler

		var subscriptions, hasSubs = gp.channels[topic]
		if !hasSubs {
			subscriptions = map[nxid.ID]socketHub{}
			gp.channels[topic] = subscriptions
		}

		subscriptions[sub.id] = sub
		result <- &localSubscription{
			id:    sub.id,
			topic: topic,
			pub:   gp,
		}
	}

	select {
	case gp.doActions <- doAction:
		return <-result
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to register socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
		}))
		return &utils.CloseErrorChannel{Error: nerror.WrapOnly(gp.ctx.Err())}
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

func (gp *GorillaPub) listenToSocket(topic string, socket *GorillaSocket) {
	var doAction = func() {
		var sub socketHub
		sub.id = nxid.New()
		sub.socket = socket

		var subscriptions, hasSubs = gp.channels[topic]
		if !hasSubs {
			subscriptions = map[nxid.ID]socketHub{}
			gp.channels[topic] = subscriptions
		}

		subscriptions[sub.id] = sub
	}

	select {
	case gp.doActions <- doAction:
		return
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to register socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))
		return
	}
}

// SendToOne selects a random recipient which will receive the message to be delivered
// for processing.
func (gp *GorillaPub) SendToOne(data *supabaiza.Message, timeout time.Duration) error {
	var result = make(chan error, 1)

	var doAction = func() {
		var encoded, encodedErr = gp.config.Codec.Encode(data)
		if encodedErr != nil {
			result <- nerror.WrapOnly(encodedErr)
			return
		}

		var subscriptions, hasSubs = gp.channels[data.Topic]
		if !hasSubs {
			result <- nerror.New("no subscription for topic")
			return
		}

		var listenersSize = len(subscriptions)
		var randomCandidate = rand.Intn(listenersSize)

		var idList = make([]nxid.ID, 0, listenersSize)
		for id := range subscriptions {
			idList = append(idList, id)
		}

		var candidate = subscriptions[idList[randomCandidate]]
		gp.deliverMessage(data, encoded, &candidate)

		close(result)
	}

	select {
	case gp.doActions <- doAction:
		return <-result
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to register socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
		}))
		return nerror.WrapOnly(gp.ctx.Err())
	}
}

// SendToAll delivers to all listeners the provided message within specific timeout.
func (gp *GorillaPub) SendToAll(data *supabaiza.Message, timeout time.Duration) error {
	var result = make(chan error, 1)

	var doAction = func() {
		var encoded, encodedErr = gp.config.Codec.Encode(data)
		if encodedErr != nil {
			result <- nerror.WrapOnly(encodedErr)
			return
		}

		var subscriptions, hasSubs = gp.channels[data.Topic]
		if !hasSubs {
			result <- nerror.New("no subscription for topic")
			return
		}

		for _, sub := range subscriptions {
			gp.deliverMessage(data, encoded, &sub)
		}

		close(result)
	}

	select {
	case gp.doActions <- doAction:
		return <-result
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to register socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
		}))
		return nerror.WrapOnly(gp.ctx.Err())
	}
}

func (gp *GorillaPub) deliverMessage(data *supabaiza.Message, encodedMessage []byte, sub *socketHub) {
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
				event.String("socket_id", sub.socket.id.String())
				event.Object("socket_stat", sub.socket.Stat())
			}))
		}
		return
	}

	if err := sub.localFn(data, gp); nil != err {
		gp.config.Logger.Log(njson.MJSON("failed to handle message by function", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", err.Error())
			event.String("socket_id", sub.socket.id.String())
			event.Object("socket_stat", sub.socket.Stat())
		}))
	}
}

func (gp *GorillaPub) deliverMessageToSocket(data *supabaiza.Message, sub *GorillaSocket) error {
	var encoded, encodedErr = gp.config.Codec.Encode(data)
	if encodedErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", encodedErr.Error())
			event.String("socket_id", sub.id.String())
			event.Object("socket_stat", sub.Stat())
		}))
		return encodedErr
	}

	if sendErr := sub.Send(encoded, gp.config.MaxWaitToSend); sendErr != nil {
		gp.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", encodedErr.Error())
			event.String("socket_id", sub.id.String())
			event.Object("socket_stat", sub.Stat())
		}))
		return sendErr
	}
	return nil
}

func (gp *GorillaPub) manageSocketOpened(socket *GorillaSocket) {
	if gp.config.OnOpen != nil {
		gp.config.OnOpen(socket)
	}
}

func (gp *GorillaPub) manageSocketClosed(socket *GorillaSocket) {
	var doAction = func() {
		// loop through all subscriptions and remove cases where
		// such a socket is registered.
		for _, subs := range gp.channels {
			delete(subs, socket.ID())
		}

		if gp.config.OnClosure != nil {
			gp.config.OnClosure(socket)
		}
	}

	select {
	case gp.doActions <- doAction:
		return
	case <-gp.ctx.Done():
		gp.config.Logger.Log(njson.MJSON("failed to deregister socket due to ctx closure", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
		}))
		return
	}
}

func (gp *GorillaPub) handleSocketMessage(message []byte, socket *GorillaSocket) error {
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

	var targetTopic, ok = msg.Payload.(string)
	if !ok {
		gp.config.Logger.Log(njson.MJSON("failed to check topic", func(event npkg.Encoder) {
			event.String("hub_id", gp.config.ID.String())
			event.String("error", msgErr.Error())
			event.String("topic", targetTopic)
			event.String("message", string(message))
			event.String("socket_id", socket.id.String())
			event.Object("socket_stat", socket.Stat())
		}))
		return nerror.New("invalid message payload, expected string")
	}

	switch msg.Topic {
	case SUBSCRIBE:
		gp.listenToSocket(targetTopic, socket)
	case UNSUBSCRIBE:
		gp.unListenSocket(targetTopic, socket)
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
