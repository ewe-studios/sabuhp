package managers

import (
	"context"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"
	"github.com/influx6/sabuhp"
)

type SocketNotification func(socket sabuhp.Socket)

// TransportManager wraps a transport object exposes a wrapper that underline manages
// multiple subscriptions on a per topic basis.
type TransportManager struct {
	logger    sabuhp.Logger
	waiter    sync.WaitGroup
	Transport sabuhp.Transport
	ctx       context.Context
	chl       sync.RWMutex
	channels  map[string]*subscriptionChannel
}

func NewTransportManager(ctx context.Context, transport sabuhp.Transport, logger sabuhp.Logger) *TransportManager {
	var tm = &TransportManager{
		ctx:       ctx,
		logger:    logger,
		Transport: transport,
		channels:  map[string]*subscriptionChannel{},
	}
	return tm
}

func (tm *TransportManager) Conn() sabuhp.Conn {
	return tm.Transport.Conn()
}

// SendToOne implements the Transport interface but handles things in a different
// way. If the message has an overriding transport from its originator then that is
// used to send the message, which bypasses all internal channels or subscriptions.
//
func (tm *TransportManager) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	var logStack = njson.Log(tm.logger)

	data.Delivery = sabuhp.SendToOne
	if data.OverridingTransport != nil {
		logStack.New().LInfo().
			Message("delivery message to overriding transport").
			String("topic", data.Topic).
			End()

		if sendErr := tm.SendWithTimeout(data, data.OverridingTransport, timeout); sendErr != nil {
			var wrappedErr = nerror.WrapOnly(sendErr)
			logStack.New().LError().
				Message("failed to deliver message content to overriding transport").
				String("topic", data.Topic).
				String("error", wrappedErr.Error()).
				Bool("should_ack", sendErr.ShouldAck()).
				End()
			return wrappedErr
		}
		return nil
	}

	logStack.New().LInfo().
		Message("delivery message to one listener through transport").
		String("topic", data.Topic).
		End()
	return tm.Transport.SendToOne(data, timeout)
}

// SendToAll implements the Transport interface but handles things in a different
// way. If the message has an overriding transport from its originator then that is
// used to send the message first before attempting to deliver message to all other
// channels matching giving topic.
//
func (tm *TransportManager) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	var logStack = njson.Log(tm.logger)

	data.Delivery = sabuhp.SendToAll
	if data.OverridingTransport != nil {
		logStack.New().LInfo().
			Message("delivery message to overriding transport first").
			String("topic", data.Topic).
			End()

		if sendErr := tm.SendWithTimeout(data, data.OverridingTransport, timeout); sendErr != nil {
			logStack.New().LError().
				Message("failed to deliver message content to overriding transport").
				String("topic", data.Topic).
				String("error", nerror.WrapOnly(sendErr).Error()).
				Bool("should_ack", sendErr.ShouldAck()).
				End()
			return sendErr
		}

		logStack.New().LInfo().
			Message("delivery message to remaining listeners through transport").
			String("topic", data.Topic).
			End()

		var newData = *data
		newData.OverridingTransport = nil
		return tm.Transport.SendToAll(&newData, timeout)
	}

	logStack.New().LInfo().
		Message("delivery message to listeners through transport").
		String("topic", data.Topic).
		End()

	return tm.Transport.SendToAll(data, timeout)
}

func (tm *TransportManager) Send(message *sabuhp.Message, transport sabuhp.Transport) sabuhp.MessageErr {
	return tm.SendWithTimeout(message, transport, 0)
}

func (tm *TransportManager) SendWithTimeout(message *sabuhp.Message, transport sabuhp.Transport, timeout time.Duration) sabuhp.MessageErr {
	var logStack = njson.Log(tm.logger)

	logStack.New().LInfo().
		Message("message received for topic").
		String("topic", message.Topic).
		End()

	tm.chl.RLock()
	channel, hasChannel := tm.channels[message.Topic]
	tm.chl.RUnlock()

	if !hasChannel {
		return sabuhp.WrapErr(nerror.New("no listeners for topic %q", message.Topic), false)
	}

	if channel.IsEmpty() {
		return sabuhp.WrapErr(nerror.New("channel has no listeners for topic %q", message.Topic), false)
	}

	logStack.New().LInfo().
		Message("notifying subscription of message").
		String("topic", message.Topic).
		End()

	channel.NotifyWithTimeout(message, transport, timeout)
	logStack.New().LInfo().
		Message("notified subscription of message").
		String("topic", message.Topic).
		End()
	return nil
}

// UnlistenAllWithId sends a signal to remove possible handler with giving id from all topics.
func (tm *TransportManager) UnlistenAllWithId(id nxid.ID) {
	var info subInfo
	info.id = id

	tm.chl.RLock()
	for _, channel := range tm.channels {
		channel.Remove(info)
		// if it's empty after half a second then close channel.
		tm.retireAfter(channel, time.Second/2)
	}
	tm.chl.RUnlock()
}

// UnlistenWithId sends a signal to remove possible handler with giving id from specific topic.
func (tm *TransportManager) UnlistenWithId(topic string, id nxid.ID) {
	var info subInfo
	info.id = id
	info.topic = topic

	tm.chl.RLock()
	channel, hasChannel := tm.channels[info.topic]
	tm.chl.RUnlock()

	if hasChannel {
		channel.Remove(info)

		// if it's empty after half a second then close channel.
		tm.retireAfter(channel, time.Second/2)
	}
}

func (tm *TransportManager) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	var sub subInfo
	sub.manager = tm
	sub.topic = topic
	sub.id = nxid.New()
	sub.handler = handler

	return tm.listenTo(&sub)
}

// ListenWithId creates a listener using specified id, this allows the unique id to represent
// a possible subscription.
func (tm *TransportManager) ListenWithId(id nxid.ID, topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	var sub subInfo
	sub.id = id
	sub.manager = tm
	sub.topic = topic
	sub.handler = handler

	return tm.listenTo(&sub)
}

func (tm *TransportManager) listenTo(sub *subInfo) sabuhp.Channel {
	var logStack = njson.Log(tm.logger)

	logStack.New().LInfo().
		Message("adding subscription for topic").
		String("topic", sub.topic).
		End()

	tm.chl.RLock()
	if channel, hasChannel := tm.channels[sub.topic]; hasChannel {
		tm.chl.RUnlock()

		logStack.New().LInfo().
			Message("found channel for topic").
			String("topic", sub.topic).
			End()

		channel.Add(sub)

		logStack.New().LInfo().
			Message("added subscriber to topic").
			String("topic", sub.topic).
			End()
		return sub
	}
	tm.chl.RUnlock()

	logStack.New().LInfo().
		Message("creating transport channel for topic").
		String("topic", sub.topic).
		End()

	// Create listener instruction to transport.
	var transportChannel = tm.Transport.Listen(sub.topic, sabuhp.TransportResponseFunc(tm.Send))

	logStack.New().LInfo().
		Message("created transport channel for topic").
		String("topic", sub.topic).
		End()

	if transportErr := transportChannel.Err(); transportErr != nil {
		sub.err = nerror.WrapOnly(transportErr)

		logStack.New().LError().
			Message("failed to add subscriber to topic").
			String("topic", sub.topic).
			Error("error", transportErr).
			End()
		return sub
	}

	var newCtx, canceler = context.WithCancel(tm.ctx)
	var newChannel = &subscriptionChannel{
		topic:         sub.topic,
		logger:        tm.logger,
		commands:      make(chan func()),
		ctx:           newCtx,
		canceler:      canceler,
		subscriptions: map[nxid.ID]sabuhp.TransportResponse{},
	}

	tm.waiter.Add(1)
	go func() {
		var logStack = njson.Log(tm.logger)

		defer tm.waiter.Done()

		// retire topic management
		defer tm.retire(sub.topic)

		// close channel listener
		defer transportChannel.Close()

		newChannel.Run()

		logStack.New().LInfo().
			Message("removing subscription transportManager for topic").
			String("topic", sub.topic).
			End()
	}()

	logStack.New().LInfo().
		Message("adding new transport channel to map").
		String("topic", sub.topic).
		End()

	tm.chl.Lock()
	tm.channels[sub.topic] = newChannel
	tm.chl.Unlock()

	logStack.New().LInfo().
		Message("adding subscription to transport channel").
		String("topic", sub.topic).
		End()

	newChannel.Add(sub)

	logStack.New().LInfo().
		Message("added subscriber to topic").
		String("topic", sub.topic).
		End()

	logStack.New().LInfo().
		Message("added subscription for topic").
		String("topic", sub.topic).
		End()

	return sub
}

func (tm *TransportManager) Wait() {
	tm.waiter.Wait()
}

func (tm *TransportManager) remove(info subInfo) {
	tm.chl.RLock()
	channel, hasChannel := tm.channels[info.topic]
	tm.chl.RUnlock()

	if hasChannel {
		channel.Remove(info)

		// if it's empty after half a second then close channel.
		tm.retireAfter(channel, time.Second/2)
	}
}

func (tm *TransportManager) retireAfter(channel *subscriptionChannel, dur time.Duration) {
	select {
	case <-tm.ctx.Done():
		return
	case <-time.After(dur):
		if channel.IsEmpty() {
			channel.canceler()
		}
	}
}

func (tm *TransportManager) retire(topic string) {
	tm.chl.Lock()
	defer tm.chl.Unlock()
	if channel, hasChannel := tm.channels[topic]; hasChannel {
		channel.canceler()
		delete(tm.channels, topic)
	}
}

type subInfo struct {
	topic   string
	id      nxid.ID
	err     error
	handler sabuhp.TransportResponse
	manager *TransportManager
}

func (info *subInfo) Err() error {
	return info.err
}

func (info subInfo) Close() {
	info.manager.remove(info)
}

type subscriptionChannel struct {
	topic         string
	logger        sabuhp.Logger
	commands      chan func()
	ctx           context.Context
	canceler      context.CancelFunc
	subscriptions map[nxid.ID]sabuhp.TransportResponse
}

func (sc *subscriptionChannel) IsEmpty() bool {
	var count = make(chan int, 1)
	var doDistribution = func() {
		count <- len(sc.subscriptions)
	}

	select {
	case sc.commands <- doDistribution:
		return 0 == <-count
	case <-sc.ctx.Done():
		return true
	}
}

func (sc *subscriptionChannel) Remove(info subInfo) {
	var doAction = func() {
		var logStack = njson.Log(sc.logger)

		delete(sc.subscriptions, info.id)

		logStack.New().LInfo().
			Message("removing subscriber from topic").
			String("topic", sc.topic).
			String("id", info.id.String()).
			End()
	}

	select {
	case sc.commands <- doAction:
		return
	case <-sc.ctx.Done():
		return
	}
}

func (sc *subscriptionChannel) Add(info *subInfo) {
	var deliveryChan = make(chan struct{})

	var doAction = func() {
		var logStack = njson.Log(sc.logger)

		if _, hasSub := sc.subscriptions[info.id]; !hasSub {
			sc.subscriptions[info.id] = info.handler
		}

		logStack.New().LInfo().
			Message("added subscriber to topic").
			String("topic", sc.topic).
			String("id", info.id.String()).
			End()

		close(deliveryChan)
	}

	select {
	case sc.commands <- doAction:
		select {
		case <-sc.ctx.Done():
			return
		case <-deliveryChan:
			return
		}
	case <-sc.ctx.Done():
		return
	}
}

func (sc *subscriptionChannel) Notify(msg *sabuhp.Message, transport sabuhp.Transport) {
	sc.NotifyWithTimeout(msg, transport, 0)
}

func (sc *subscriptionChannel) NotifyWithTimeout(msg *sabuhp.Message, transport sabuhp.Transport, timeout time.Duration) {
	var logStack = njson.Log(sc.logger)

	logStack.New().LInfo().
		Message("received new message").
		Object("message", msg).
		String("topic", sc.topic).
		End()

	var doDistribution = func() {
		var logStack = njson.Log(sc.logger)

		logStack.New().Message("notifying all handlers with message").
			String("topic", sc.topic).
			End()

		for _, sub := range sc.subscriptions {
			func(subscriber sabuhp.TransportResponse, m *sabuhp.Message) {
				defer func() {
					if panicInfo := recover(); panicInfo != nil {
						logStack.New().LPanic().
							Message("message handler panic during handling").
							Object("message", m).
							String("topic", sc.topic).
							Formatted("panic_data", "%#v", panicInfo).
							End()
					}
				}()

				logStack.New().Message("calling handler with message").
					Object("message", m).
					String("topic", sc.topic).
					End()

				if handleErr := subscriber.Handle(m, transport); handleErr != nil {
					logStack.New().Message("error occurred handled message").
						Object("message", m).
						String("topic", sc.topic).
						String("error", nerror.WrapOnly(handleErr).Error()).
						End()
					return
				}

				logStack.New().Message("handled message delivery successfully").
					Object("message", m).
					String("topic", sc.topic).
					End()
			}(sub, msg)
		}
	}

	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case <-timeoutChan:
		logStack.New().LWarn().
			Message("failed to deliver message to handlers due to timeout").
			Object("message", msg).
			String("topic", sc.topic).
			End()
		return
	case sc.commands <- doDistribution:
		return
	case <-sc.ctx.Done():
		logStack.New().LWarn().
			Message("failed to deliver message to handlers").
			Object("message", msg).
			String("topic", sc.topic).
			End()
		return
	}
}

func (sc *subscriptionChannel) Run() {
	var logStack = njson.Log(sc.logger)

	logStack.New().LInfo().
		Message("starting subscription management loop").
		String("topic", sc.topic).
		End()

	defer func() {
		logStack.New().LInfo().
			Message("ending subscription management loop").
			String("topic", sc.topic).
			End()
	}()

loopHandler:
	for {
		select {
		case <-sc.ctx.Done():
			break loopHandler
		case doAction := <-sc.commands:
			doAction()
		}
	}
}
