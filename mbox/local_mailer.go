package mbox

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ewe-studios/sabuhp"
	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
)

// localMailboxChannel houses a channel for a mailbox responder.
type localMailboxChannel struct {
	mailbox  *LocalMailbox
	callback sabuhp.TransportResponse

	left  *localMailboxChannel
	right *localMailboxChannel
}

func (mc *localMailboxChannel) Err() error {
	return nil
}

func (mc *localMailboxChannel) deliver(msg *sabuhp.Message) {
	mc.mailbox.deliverTo(mc.callback, msg)
	if mc.right != nil && msg.Type == sabuhp.SendToAll {
		mc.right.deliver(msg)
	}
}

func (mc *localMailboxChannel) addCallback(callback sabuhp.TransportResponse) *localMailboxChannel {
	var channel localMailboxChannel
	channel.callback = callback
	mc.add(&channel)
	return &channel
}

func (mc *localMailboxChannel) add(channel *localMailboxChannel) {
	mc.right = channel
	channel.left = mc
	mc.mailbox.tailChannel = channel
}

func (mc *localMailboxChannel) disconnect() {
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

func (mc *localMailboxChannel) Close() {
	mc.mailbox.Disconnect(mc)
}

// LocalMailbox implements the underline logic necessary for providing a buffered
// mailbox for message delivery to specific handlers.
type LocalMailbox struct {
	topic    string
	mailer   *LocalMailer
	canceler context.CancelFunc
	logger   sabuhp.Logger
	ctx      context.Context

	messages    chan *sabuhp.Message
	doChannel   chan func()
	rootChannel *localMailboxChannel
	tailChannel *localMailboxChannel
	waiter      sync.WaitGroup
	starter     *sync.Once
	stopper     *sync.Once
}

func NewLocalMailbox(
	ctx context.Context,
	mailer *LocalMailer,
	topic string,
	logger sabuhp.Logger,
	bufferSize int,
) *LocalMailbox {
	var box = &LocalMailbox{
		mailer:    mailer,
		topic:     topic,
		logger:    logger,
		messages:  make(chan *sabuhp.Message, bufferSize),
		doChannel: make(chan func()),
	}
	box.setCtx(ctx)
	return box
}

func (ps *LocalMailbox) setCtx(ctx context.Context) {
	var ender sync.Once
	ps.stopper = &ender

	var starter sync.Once
	ps.starter = &starter

	var newCtx, canceler = context.WithCancel(ctx)
	ps.ctx = newCtx
	ps.canceler = canceler
}

func (ps *LocalMailbox) DeliverTimeout(message *sabuhp.Message, timeout time.Duration) error {
	select {
	case ps.messages <- message:
		return nil
	case <-time.After(timeout):
		return nerror.New("failed to Deliver, timed out")
	case <-ps.ctx.Done():
		return nerror.New("failed to Deliver, mailbox closed")
	}
}

func (ps *LocalMailbox) MessageChannel() chan *sabuhp.Message {
	return ps.messages
}

func (ps *LocalMailbox) Deliver(message *sabuhp.Message) error {
	select {
	case ps.messages <- message:
		return nil
	case <-ps.ctx.Done():
		return nerror.New("failed to Deliver, mailbox closed")
	}
}

func (ps *LocalMailbox) Add(callback sabuhp.TransportResponse) *localMailboxChannel {
	var newChannel = new(localMailboxChannel)
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

func (ps *LocalMailbox) Disconnect(channel *localMailboxChannel) {
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

func (ps *LocalMailbox) Wait() {
	ps.waiter.Wait()
}

func (ps *LocalMailbox) Start() {
	ps.starter.Do(func() {
		// add waiter
		ps.waiter.Add(1)

		// manage operations
		go ps.manage()
	})
}

func (ps *LocalMailbox) Stop() {
	ps.stopper.Do(func() {
		ps.canceler()
		ps.waiter.Wait()
	})
}

func (ps *LocalMailbox) disconnectChannels() {
	ps.rootChannel = nil
	ps.tailChannel = nil
}

func (ps *LocalMailbox) manage() {
	defer ps.waiter.Done()
	defer ps.disconnectChannels()

	for {
		select {
		case <-ps.ctx.Done():
			ps.disconnectChannels()
			return
		case message := <-ps.messages:
			ps.rootChannel.deliver(message)
		case doAction := <-ps.doChannel:
			doAction()
		}
	}
}

func (ps *LocalMailbox) deliverTo(responseHandler sabuhp.TransportResponse, message *sabuhp.Message) {
	defer func() {
		if err := recover(); err != nil {
			ps.logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("message", "failed to Deliver to client")
				event.String("pubsub_topic", ps.topic)
				event.Int("_level", int(npkg.ERROR))
				event.String("panic_error", fmt.Sprintf("%s", err))
				event.String("panic_error_object", fmt.Sprintf("%#v", err))
			}))
		}
	}()

	if err := responseHandler.Handle(message, ps.mailer); err != nil {
		ps.logger.Log(njson.JSONB(func(event npkg.Encoder) {
			event.String("message", "failed to handle delivery to client")
			event.String("pubsub_topic", ps.topic)
			event.Int("_level", int(npkg.ERROR))
			event.String("error", nerror.WrapOnly(err).Error())
		}))
	}
}

type LocalMailer struct {
	maxBuffer     int
	ender         *sync.Once
	waiter        sync.WaitGroup
	logger        sabuhp.Logger
	canceler      context.CancelFunc
	ctx           context.Context
	tml           sync.RWMutex
	topicMappings map[string]*LocalMailbox
}

func NewLocalMailer(
	ctx context.Context,
	mailboxBuffer int,
	logger sabuhp.Logger,
) *LocalMailer {
	var mailer = &LocalMailer{
		logger:        logger,
		maxBuffer:     mailboxBuffer,
		topicMappings: map[string]*LocalMailbox{},
	}
	mailer.setCtx(ctx)
	return mailer
}

func (p *LocalMailer) Wait() {
	p.waiter.Wait()
}

func (p *LocalMailer) Stop() {
	p.ender.Do(func() {
		p.canceler()
	})
}

func (p *LocalMailer) setCtx(ctx context.Context) {
	var ender sync.Once
	p.ender = &ender

	var newCtx, canceler = context.WithCancel(ctx)
	p.ctx = newCtx
	p.canceler = canceler
}

func (p *LocalMailer) getTopic(topic string) *LocalMailbox {
	var mailbox *LocalMailbox
	p.tml.RLock()
	mailbox = p.topicMappings[topic]
	p.tml.RUnlock()
	return mailbox
}

func (p *LocalMailer) addTopic(topic string, maxBuffer int) *LocalMailbox {
	var mailbox = NewLocalMailbox(p.ctx, p, topic, p.logger, maxBuffer)

	p.tml.Lock()
	p.topicMappings[topic] = mailbox
	p.tml.Unlock()

	p.manageMailbox(mailbox)
	return mailbox
}

func (p *LocalMailer) manageMailbox(mailbox *LocalMailbox) {
	p.waiter.Add(1)
	mailbox.Start()
	go func() {
		defer p.waiter.Done()
		mailbox.waiter.Wait()
	}()
}

func (p *LocalMailer) Conn() sabuhp.Conn {
	return p
}

// Channel creates necessary subscription to target topic on provided transport.
// It also creates an internal mailbox for the delivery of those commands.
func (p *LocalMailer) Listen(topic string, callback sabuhp.TransportResponse) sabuhp.Channel {
	var mailbox = p.getTopic(topic)
	if mailbox == nil {
		mailbox = p.addTopic(topic, p.maxBuffer)
	}
	return mailbox.Add(callback)
}

func (p *LocalMailer) SendToOne(message *sabuhp.Message, timeout time.Duration) error {
	message.Type = sabuhp.RequestReply

	// do we have a local handler for the message's target
	var mailbox = p.getTopic(message.Topic)
	if mailbox != nil {
		if timeout > 0 {
			return mailbox.DeliverTimeout(message, timeout)
		}
		return mailbox.Deliver(message)
	}

	return nerror.New("has no subscription for topic")
}

func (p *LocalMailer) SendToAll(message *sabuhp.Message, timeout time.Duration) error {
	message.Type = sabuhp.SendToAll
	// do we have a local handler for the message's target
	var mailbox = p.getTopic(message.Topic)
	if mailbox != nil {
		if timeout > 0 {
			return mailbox.DeliverTimeout(message, timeout)
		}
		return mailbox.Deliver(message)
	}

	return nerror.New("has no subscription for topic")
}
