package supabaiza

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
type ChannelResponse func(data *Message, sub PubSub)

// Channel represents a generated subscription on a
// topic which provides the giving callback an handler
// to define the point at which the channel should be
// closed and stopped from receiving updates.
type Channel interface {
	Close()
}

// Conn defines the connection type which we can retrieve
// and understand the type.
type Conn interface {
	Type() int
}

// Transport defines what an underline transport system provides.
type Transport interface {
	Conn() Conn
	Listen(topic string, handler func([]byte)) Channel
	SendToOne(topic string, data []byte, timeout time.Duration) error
	SendToAll(topic string, data []byte, timeout time.Duration) error
}

// PubSub defines the expectation for a pubsub delivery mechanism
// which can be a redis, websocket or http endpoint or whatever
// implementation is based on.
type PubSub interface {

	// Channel creates a callback which exists to receive specific
	// messages on a giving topic. It returns a Channel specifically
	// for stopping responses to giving callback.
	//
	// See PubSub.EndChannel for ending all messages for a topic.
	Channel(topic string, callback ChannelResponse) Channel

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
	Delegate(message *Message, timeout time.Duration) error

	// Broadcast acts like traditional pubsub where the message
	// is sent to all listening subscribers both local and remote.
	// Allowing all process message accordingly.
	Broadcast(message *Message, timeout time.Duration) error
}

// mailboxChannel houses a channel for a mailbox responder.
type mailboxChannel struct {
	mailbox  *Mailbox
	left     *mailboxChannel
	right    *mailboxChannel
	callback ChannelResponse
}

func (mc *mailboxChannel) deliver(msg *Message) {
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
	// are we the root?
	if mc == mc.mailbox.rootChannel && mc == mc.mailbox.tailChannel {
		mc.mailbox.rootChannel = mc.right
		mc.mailbox.tailChannel = mc.right
		return
	}

	if mc == mc.mailbox.rootChannel && mc != mc.mailbox.tailChannel {
		mc.mailbox.rootChannel = mc.right
		return
	}

	// disconnect us
	mc.left.right = mc.right
	mc.right.left = mc.left
}

func (mc *mailboxChannel) Close() {
	mc.mailbox.disconnect(mc)
}

// Mailbox implements the underline logic necessary for providing a buffered
// mailbox for message delivery to specific handlers.
type Mailbox struct {
	topic       string
	canceler    context.CancelFunc
	logger      sabuhp.Logger
	ctx         context.Context
	messages    chan *Message
	doChannel   chan func()
	rootChannel *mailboxChannel
	tailChannel *mailboxChannel
	subscribers []ChannelResponse
	pubsub      PubSub
	waiter      sync.WaitGroup
	starter     sync.Once
	stopper     sync.Once
}

func NewMailbox(
	ctx context.Context,
	topic string,
	logger sabuhp.Logger,
	bufferSize int,
	pubsub PubSub,
) *Mailbox {
	newCtx, canceler := context.WithCancel(ctx)
	return &Mailbox{
		canceler:  canceler,
		ctx:       newCtx,
		topic:     topic,
		logger:    logger,
		pubsub:    pubsub,
		messages:  make(chan *Message, bufferSize),
		doChannel: make(chan func()),
	}
}

func (ps *Mailbox) deliverTimeout(message *Message, timeout time.Duration) error {
	select {
	case ps.messages <- message:
		return nil
	case <-time.After(timeout):
		return nerror.New("failed to deliver, timed out")
	case <-ps.ctx.Done():
		return nerror.New("failed to deliver, mailbox closed")
	}
}

func (ps *Mailbox) deliver(message *Message) error {
	select {
	case ps.messages <- message:
		return nil
	case <-ps.ctx.Done():
		return nerror.New("failed to deliver, mailbox closed")
	}
}

func (ps *Mailbox) add(callback ChannelResponse) *mailboxChannel {
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

func (ps *Mailbox) disconnect(channel *mailboxChannel) {
	var rmChannel = func() {
		channel.disconnect()
	}

	select {
	case ps.doChannel <- rmChannel:
		return
	case <-ps.ctx.Done():
		return
	}
}

func (ps *Mailbox) start() {
	ps.starter.Do(func() {
		ps.waiter.Add(1)
		go ps.manage()
	})
}

func (ps *Mailbox) stop() {
	ps.stopper.Do(func() {
		ps.canceler()
		ps.waiter.Wait()
	})
}

func (ps *Mailbox) manage() {
	for {
		select {
		case <-ps.ctx.Done():
			return
		case message := <-ps.messages:
			for _, handler := range ps.subscribers {
				ps.deliverTo(handler, message)
			}
		case doAction := <-ps.doChannel:
			doAction()
		}
	}
}

func (ps *Mailbox) deliverTo(responseHandler ChannelResponse, message *Message) {
	defer func() {
		if err := recover(); err != nil {
			ps.logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("message", "failed to deliver to client")
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
	codec         Codec
	waiter        sync.WaitGroup
	logger        sabuhp.Logger
	ctx           context.Context
	Transport     Transport
	tml           sync.RWMutex
	topicMappings map[string]*Mailbox
	tcl           sync.RWMutex
	topicChannels map[string]Channel
}

func NewPubSubImpl(
	ctx context.Context,
	mailboxBuffer int,
	codec Codec,
	logger sabuhp.Logger,
	transport Transport,
) *PubSubImpl {
	return &PubSubImpl{
		ctx:           ctx,
		codec:         codec,
		logger:        logger,
		maxBuffer:     mailboxBuffer,
		Transport:     transport,
		topicMappings: map[string]*Mailbox{},
	}
}

func (p *PubSubImpl) getTopic(topic string) *Mailbox {
	var mailbox *Mailbox
	p.tml.RLock()
	mailbox = p.topicMappings[topic]
	p.tml.Unlock()
	return mailbox
}

func (p *PubSubImpl) addTopic(topic string, maxBuffer int) *Mailbox {
	var mailbox = NewMailbox(p.ctx, topic, p.logger, maxBuffer, p)
	var topicChannel = p.Transport.Listen(topic, func(data []byte) {
		var message, codecErr = p.codec.Decode(data)
		if codecErr != nil {
			p.logger.Log(njson.MJSON("failed to decode message bytes", func(event npkg.Encoder) {
				event.String("data", string(data))
				event.String("topic", topic)
				event.String("error", nerror.WrapOnly(codecErr).Error())
			}))
			return
		}

		if deliveryErr := mailbox.deliver(message); deliveryErr != nil {
			p.logger.Log(njson.MJSON("failed to deliver decoded message to mailbox", func(event npkg.Encoder) {
				event.String("data", string(data))
				event.String("topic", topic)
				event.String("error", nerror.WrapOnly(deliveryErr).Error())
			}))
		}
	})

	p.tcl.Lock()
	p.topicChannels[topic] = topicChannel
	p.tcl.Unlock()

	p.tml.Lock()
	p.topicMappings[topic] = mailbox
	p.tml.Unlock()

	p.manageMailbox(mailbox)
	return mailbox
}

func (p *PubSubImpl) manageMailbox(mailbox *Mailbox) {
	p.waiter.Add(1)
	mailbox.start()
	go func() {
		defer p.waiter.Done()
		mailbox.waiter.Wait()
	}()
}

func (p *PubSubImpl) forgetTopicChannel(topic string) {
	var channel Channel
	p.tcl.Lock()
	channel = p.topicChannels[topic]
	delete(p.topicChannels, topic)
	p.tcl.Unlock()

	if channel == nil {
		return
	}

	channel.Close()
}

// Channel creates necessary subscription to target topic on provided transport.
// It also creates an internal mailbox for the delivery of those messages.
func (p *PubSubImpl) Channel(topic string, callback ChannelResponse) Channel {
	var mailbox = p.getTopic(topic)
	if mailbox == nil {
		mailbox = p.addTopic(topic, p.maxBuffer)
	}
	return mailbox.add(callback)
}

// Delegate will either deliver message to local mailbox if topic has
// a listener or send across transport using Transport.SendToOne.
func (p *PubSubImpl) Delegate(message *Message, timeout time.Duration) error {
	// do we have a local handler for the message's target
	var mailbox = p.getTopic(message.Topic)
	if mailbox != nil {
		if timeout > 0 {
			return mailbox.deliverTimeout(message, timeout)
		}
		return mailbox.deliver(message)
	}

	// encode and use transport.
	var encodedData, codecErr = p.codec.Encode(message)
	if codecErr != nil {
		return nerror.WrapOnly(codecErr)
	}

	return p.Transport.SendToOne(message.Topic, encodedData, timeout)
}

// Broadcast will attempt to encode message which will be send sent across
// the transport which means if there exists a listener on this publisher
// then the transport depending on how it's setup will deliver a copy to
// the publisher as well.
func (p *PubSubImpl) Broadcast(message *Message, timeout time.Duration) error {
	// encode and use transport.
	var encodedData, codecErr = p.codec.Encode(message)
	if codecErr != nil {
		return nerror.WrapOnly(codecErr)
	}

	return p.Transport.SendToAll(message.Topic, encodedData, timeout)
}

func (p *PubSubImpl) Wait() {
	p.waiter.Wait()
}
