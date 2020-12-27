package mbox

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"
)

// mailboxChannel houses a channel for a mailbox responder.
type mailboxChannel struct {
	mailbox  *Mailbox
	callback sabuhp.TransportResponse

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

func (mc *mailboxChannel) addCallback(callback sabuhp.TransportResponse) *mailboxChannel {
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
	transport sabuhp.Transport,
) *Mailbox {
	var box = &Mailbox{
		topic:     topic,
		logger:    logger,
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

func (ps *Mailbox) Add(callback sabuhp.TransportResponse) *mailboxChannel {
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
	ps.transportChannel = ps.transport.Listen(
		ps.topic,
		sabuhp.TransportResponseFunc(func(
			data *sabuhp.Message,
			t sabuhp.Transport,
		) sabuhp.MessageErr {
			if deliveryErr := ps.Deliver(data); deliveryErr != nil {
				ps.logger.Log(njson.MJSON("failed to Deliver decoded message to mailbox", func(event npkg.Encoder) {
					event.String("data", data.String())
					event.Int("_level", int(npkg.ERROR))
					event.String("topic", ps.topic)
					event.String("error", nerror.WrapOnly(deliveryErr).Error())
				}))
				return sabuhp.WrapErr(deliveryErr, false)
			}
			return nil
		}))
}

func (ps *Mailbox) unListen() {
	if ps.transportChannel != nil {
		ps.transportChannel.Close()
	}
}

func (ps *Mailbox) deliverTo(responseHandler sabuhp.TransportResponse, message *sabuhp.Message) {
	defer func() {
		if err := recover(); err != nil {
			ps.logger.Log(njson.JSONB(func(event npkg.Encoder) {
				event.String("message", "failed to Deliver to client")
				event.String("pubsub_topic", ps.topic)
				event.String("panic_error", fmt.Sprintf("%s", err))
				event.Int("_level", int(npkg.PANIC))
				event.String("panic_error_object", fmt.Sprintf("%#v", err))
			}))
		}
	}()

	if err := responseHandler.Handle(message, ps.transport); err != nil {
		ps.logger.Log(njson.JSONB(func(event npkg.Encoder) {
			event.String("message", "failed to handle delivery to client")
			event.Int("_level", int(npkg.ERROR))
			event.String("pubsub_topic", ps.topic)
			event.String("error", nerror.WrapOnly(err).Error())
		}))
	}
}

type Mailer struct {
	maxBuffer     int
	ender         *sync.Once
	waiter        sync.WaitGroup
	logger        sabuhp.Logger
	canceler      context.CancelFunc
	ctx           context.Context
	transport     sabuhp.Transport
	tml           sync.RWMutex
	topicMappings map[string]*Mailbox
}

func NewMailer(
	ctx context.Context,
	mailboxBuffer int,
	logger sabuhp.Logger,
	transport sabuhp.Transport,
) *Mailer {
	var mailer = &Mailer{
		logger:        logger,
		maxBuffer:     mailboxBuffer,
		transport:     transport,
		topicMappings: map[string]*Mailbox{},
	}
	mailer.setCtx(ctx)
	return mailer
}

func (p *Mailer) Wait() {
	p.waiter.Wait()
}

func (p *Mailer) Stop() {
	p.ender.Do(func() {
		p.canceler()
	})
}

func (p *Mailer) setCtx(ctx context.Context) {
	var ender sync.Once
	p.ender = &ender

	var newCtx, canceler = context.WithCancel(ctx)
	p.ctx = newCtx
	p.canceler = canceler
}

func (p *Mailer) getTopic(topic string) *Mailbox {
	var mailbox *Mailbox
	p.tml.RLock()
	mailbox = p.topicMappings[topic]
	p.tml.RUnlock()
	return mailbox
}

func (p *Mailer) addTopic(topic string, maxBuffer int) *Mailbox {
	var mailbox = NewMailbox(p.ctx, topic, p.logger, maxBuffer, p.transport)

	p.tml.Lock()
	p.topicMappings[topic] = mailbox
	p.tml.Unlock()

	p.manageMailbox(mailbox)
	return mailbox
}

func (p *Mailer) manageMailbox(mailbox *Mailbox) {
	p.waiter.Add(1)
	mailbox.Start()
	go func() {
		defer p.waiter.Done()
		mailbox.waiter.Wait()
	}()
}

func (p *Mailer) Conn() sabuhp.Conn {
	return p
}

// Channel creates necessary subscription to target topic on provided transport.
// It also creates an internal mailbox for the delivery of those commands.
func (p *Mailer) Listen(topic string, callback sabuhp.TransportResponse) sabuhp.Channel {
	var mailbox = p.getTopic(topic)
	if mailbox == nil {
		mailbox = p.addTopic(topic, p.maxBuffer)
	}
	return mailbox.Add(callback)
}

func (p *Mailer) SendToOne(message *sabuhp.Message, timeout time.Duration) error {
	// do we have a local handler for the message's target
	var mailbox = p.getTopic(message.Topic)
	if mailbox != nil {
		if timeout > 0 {
			return mailbox.DeliverTimeout(message, timeout)
		}
		return mailbox.Deliver(message)
	}

	return p.transport.SendToOne(message, timeout)
}

func (p *Mailer) SendToAll(message *sabuhp.Message, timeout time.Duration) error {
	return p.transport.SendToAll(message, timeout)
}
