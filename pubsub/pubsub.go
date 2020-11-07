package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"

	"github.com/influx6/sabuhp"
)

// ChannelResponse represents a message giving callback for
// underline response.
type ChannelResponse func(data *sabuhp.Message, sub PubSub)

// PubSub defines the expectation for a pubsub delivery mechanism
// which can be a redis, websocket or http endpoint or whatever
// implementation is based on.
type PubSub interface {

	// Channel creates a callback which exists to receive specific
	// commands on a giving topic. It returns a Channel specifically
	// for stopping responses to giving callback.
	//
	// See PubSub.EndChannel for ending all commands for a topic.
	Channel(topic string, callback ChannelResponse) sabuhp.Channel

	// Delegate delivers a message across the underline transport
	// or to a local recipient if available with the express
	// intent that only one receiver should get that message for
	// processing. This means if a local receiver subscribers for
	// such event then that handler processes said message else
	// it's sent to the underline transport.
	//
	// Not all transport support this feature hence it's fine for
	// this method to return an error in an implementation if the
	// underline transport is unable to support such usage.
	//
	// Understand
	Delegate(message *sabuhp.Message, timeout time.Duration) error

	// Broadcast acts like traditional pubsub where the message
	// is sent to all listening subscribers both local and remote.
	// Allowing all process message accordingly.
	Broadcast(message *sabuhp.Message, timeout time.Duration) error
}

// mailboxChannel houses a channel for a mailbox responder.
type mailboxChannel struct {
	mailbox  *Mailbox
	callback ChannelResponse

	left  *mailboxChannel
	right *mailboxChannel
}

func (mc *mailboxChannel) Err() error {
	return nil
}

func (mc *mailboxChannel) deliver(msg *sabuhp.Message) {
	mc.mailbox.deliverTo(mc.callback, msg)
	if mc.right != nil {
		mc.right.deliver(msg)
	}
}

func (mc *mailboxChannel) addCallback(callback ChannelResponse) *mailboxChannel {
	var channel mailboxChannel
	channel.callback = callback
	mc.add(&channel)
	return &channel
}

func (mc *mailboxChannel) add(channel *mailboxChannel) {
	mc.right = channel
	channel.left = mc
	mc.mailbox.tailChannel = channel
}

func (mc *mailboxChannel) disconnect() {
	// Disconnect us
	if mc.left != nil {
		mc.left.right = mc.right
	}

	if mc.right != nil {
		mc.right.left = mc.left
	}

	if mc.mailbox.rootChannel == mc {
		mc.mailbox.rootChannel = mc.right
	}
	if mc.mailbox.tailChannel == mc {
		mc.mailbox.tailChannel = mc.left
	}

	mc.mailbox = nil
	mc.left = nil
	mc.right = nil
}

func (mc *mailboxChannel) Close() {
	mc.mailbox.Disconnect(mc)
}

// Mailbox implements the underline logic necessary for providing a buffered
// mailbox for message delivery to specific handlers.
type Mailbox struct {
	topic    string
	canceler context.CancelFunc
	logger   sabuhp.Logger
	ctx      context.Context

	pubsub           PubSub
	transport        sabuhp.Transport
	transportChannel sabuhp.Channel

	messages    chan *sabuhp.Message
	doChannel   chan func()
	rootChannel *mailboxChannel
	tailChannel *mailboxChannel
	waiter      sync.WaitGroup
	starter     *sync.Once
	stopper     *sync.Once
}

func NewMailbox(
	ctx context.Context,
	topic string,
	logger sabuhp.Logger,
	bufferSize int,
	pubsub PubSub,
	transport sabuhp.Transport,
) *Mailbox {
	var box = &Mailbox{
		topic:     topic,
		logger:    logger,
		pubsub:    pubsub,
		transport: transport,
		messages:  make(chan *sabuhp.Message, bufferSize),
		doChannel: make(chan func()),
	}
	box.setCtx(ctx)
	return box
}

func (ps *Mailbox) setCtx(ctx context.Context) {
	var ender sync.Once
	ps.stopper = &ender

	var starter sync.Once
	ps.starter = &starter

	var newCtx, canceler = context.WithCancel(ctx)
	ps.ctx = newCtx
	ps.canceler = canceler
}

func (ps *Mailbox) DeliverTimeout(message *sabuhp.Message, timeout time.Duration) error {
	select {
	case ps.messages <- message:
		return nil
	case <-time.After(timeout):
		return nerror.New("failed to Deliver, timed out")
	case <-ps.ctx.Done():
		return nerror.New("failed to Deliver, mailbox closed")
	}
}

func (ps *Mailbox) MessageChannel() chan *sabuhp.Message {
	return ps.messages
}

func (ps *Mailbox) Deliver(message *sabuhp.Message) error {
	select {
	case ps.messages <- message:
		return nil
	case <-ps.ctx.Done():
		return nerror.New("failed to Deliver, mailbox closed")
	}
}

func (ps *Mailbox) Add(callback ChannelResponse) *mailboxChannel {
	var newChannel = new(mailboxChannel)
	newChannel.mailbox = ps
	newChannel.callback = callback

	var done = make(chan struct{})
	var addChannel = func() {

		if ps.rootChannel == nil {
			ps.rootChannel = newChannel
			ps.tailChannel = newChannel

			close(done)
			return
		}

		ps.tailChannel.add(newChannel)
		close(done)
	}

	select {
	case ps.doChannel <- addChannel:
	case <-ps.ctx.Done():
		return newChannel
	}

	<-done

	return newChannel
}

func (ps *Mailbox) Disconnect(channel *mailboxChannel) {
	var done = make(chan struct{})
	var rmAction = func() {
		channel.disconnect()
		done <- struct{}{}
	}

	select {
	case ps.doChannel <- rmAction:
		break
	case <-ps.ctx.Done():
		return
	}

	<-done
}

func (ps *Mailbox) Wait() {
	ps.waiter.Wait()
}

func (ps *Mailbox) Start() {
	ps.starter.Do(func() {
		// create listener for the transport
		ps.listen()

		// add waiter
		ps.waiter.Add(1)

		// manage operations
		go ps.manage()
	})
}

func (ps *Mailbox) Stop() {
	ps.stopper.Do(func() {
		ps.canceler()
		ps.waiter.Wait()
	})
}

func (ps *Mailbox) disconnectChannels() {
	ps.rootChannel = nil
	ps.tailChannel = nil
}

func (ps *Mailbox) manage() {
	defer ps.waiter.Done()
	defer ps.disconnectChannels()

	for {
		select {
		case <-ps.ctx.Done():
			ps.unListen()
			ps.disconnectChannels()
			return
		case message := <-ps.messages:
			ps.rootChannel.deliver(message)
		case doAction := <-ps.doChannel:
			doAction()
		}
	}
}

func (ps *Mailbox) listen() {
	ps.transportChannel = ps.transport.Listen(ps.topic, func(data *sabuhp.Message, t sabuhp.Transport) sabuhp.MessageErr {
		if deliveryErr := ps.Deliver(data); deliveryErr != nil {
			ps.logger.Log(njson.MJSON("failed to Deliver decoded message to mailbox", func(event npkg.Encoder) {
				event.String("data", data.String())
				event.String("topic", ps.topic)
				event.String("error", nerror.WrapOnly(deliveryErr).Error())
			}))
		}
		return nil
	})
}

func (ps *Mailbox) unListen() {
	if ps.transportChannel != nil {
		ps.transportChannel.Close()
	}
}

func (ps *Mailbox) deliverTo(responseHandler ChannelResponse, message *sabuhp.Message) {
	defer func() {
		if err := recover(); err != nil {
			ps.logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("message", "failed to Deliver to client")
				event.String("pubsub_topic", ps.topic)
				event.String("panic_error", fmt.Sprintf("%s", err))
				event.String("panic_error_object", fmt.Sprintf("%#v", err))
			}))
		}
	}()

	responseHandler(message, ps.pubsub)
}

type PubSubImpl struct {
	maxBuffer     int
	starter       *sync.Once
	ender         *sync.Once
	waiter        sync.WaitGroup
	logger        sabuhp.Logger
	canceler      context.CancelFunc
	ctx           context.Context
	Transport     sabuhp.Transport
	tml           sync.RWMutex
	topicMappings map[string]*Mailbox
}

func NewPubSubImpl(
	ctx context.Context,
	mailboxBuffer int,
	logger sabuhp.Logger,
	transport sabuhp.Transport,
) *PubSubImpl {
	var puber = &PubSubImpl{
		logger:        logger,
		maxBuffer:     mailboxBuffer,
		Transport:     transport,
		topicMappings: map[string]*Mailbox{},
	}
	puber.setCtx(ctx)
	return puber
}

func (p *PubSubImpl) Wait() {
	p.waiter.Wait()
}

func (p *PubSubImpl) Stop() {
	p.ender.Do(func() {
		p.canceler()
	})
}

func (p *PubSubImpl) setCtx(ctx context.Context) {
	var ender sync.Once
	p.ender = &ender

	var newCtx, canceler = context.WithCancel(ctx)
	p.ctx = newCtx
	p.canceler = canceler
}

func (p *PubSubImpl) getTopic(topic string) *Mailbox {
	var mailbox *Mailbox
	p.tml.RLock()
	mailbox = p.topicMappings[topic]
	p.tml.RUnlock()
	return mailbox
}

func (p *PubSubImpl) addTopic(topic string, maxBuffer int) *Mailbox {
	var mailbox = NewMailbox(p.ctx, topic, p.logger, maxBuffer, p, p.Transport)

	p.tml.Lock()
	p.topicMappings[topic] = mailbox
	p.tml.Unlock()

	p.manageMailbox(mailbox)
	return mailbox
}

func (p *PubSubImpl) manageMailbox(mailbox *Mailbox) {
	p.waiter.Add(1)
	mailbox.Start()
	go func() {
		defer p.waiter.Done()
		mailbox.waiter.Wait()
	}()
}

// Channel creates necessary subscription to target topic on provided transport.
// It also creates an internal mailbox for the delivery of those commands.
func (p *PubSubImpl) Channel(topic string, callback ChannelResponse) sabuhp.Channel {
	var mailbox = p.getTopic(topic)
	if mailbox == nil {
		mailbox = p.addTopic(topic, p.maxBuffer)
	}
	return mailbox.Add(callback)
}

// Delegate will either Deliver message to local mailbox if topic has
// a listener or send across transport using Transport.SendToOne.
func (p *PubSubImpl) Delegate(message *sabuhp.Message, timeout time.Duration) error {
	// do we have a local handler for the message's target
	var mailbox = p.getTopic(message.Topic)
	if mailbox != nil {
		if timeout > 0 {
			return mailbox.DeliverTimeout(message, timeout)
		}
		return mailbox.Deliver(message)
	}

	return p.Transport.SendToOne(message, timeout)
}

// Broadcast will attempt to encode message which will be send sent across
// the transport which means if there exists a listener on this publisher
// then the transport depending on how it's setup will Deliver a copy to
// the publisher as well.
func (p *PubSubImpl) Broadcast(message *sabuhp.Message, timeout time.Duration) error {
	return p.Transport.SendToAll(message, timeout)
}
