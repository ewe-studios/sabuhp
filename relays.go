package sabuhp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"
)

type SocketNotification func(socket Socket)

// PbRelay aka MultiSubscriberSingleTopicManager wraps a transport object
// with a subscription management core that allows multiple subscribers listen to
// messages for one topic, allowing only one listeners to this topic (the manager itself).
//
// This is useful when your underline transport bares a cost with two many subscriptions or
// does not support multiple subscriptions to a single topic.
type PbRelay struct {
	logger   Logger
	waiter   sync.WaitGroup
	ctx      context.Context
	chl      sync.RWMutex
	channels map[string][]*PbGroup
}

func NewPbRelay(ctx context.Context, logger Logger) *PbRelay {
	var tm = &PbRelay{
		ctx:      ctx,
		logger:   logger,
		channels: map[string][]*PbGroup{},
	}
	return tm
}

func (tm *PbRelay) Handle(ctx context.Context, message Message, transport Transport) MessageErr {
	var logStack = njson.Log(tm.logger)

	logStack.New().LInfo().
		Message("message received for topic").
		String("topic", message.Topic).
		End()

	tm.chl.RLock()
	channels, hasChannel := tm.channels[message.Topic]
	tm.chl.RUnlock()

	if !hasChannel {
		return WrapErr(nerror.New("no listeners for topic %q", message.Topic), false)
	}

	logStack.New().LInfo().
		Message("notifying subscription of message").
		String("topic", message.Topic).
		End()

	for _, channel := range channels {
		if channel.IsEmpty() {
			continue
		}

		if err := channel.Notify(ctx, message, transport); err != nil {
			logStack.New().LInfo().Message("channel failed to handle message").
				Error("error", err).
				String("topic", message.Topic)
			if message.Future != nil {
				message.Future.WithError(err)
			}
			continue
		}
		if message.Future != nil {
			message.Future.WithValue(nil)
		}
	}

	logStack.New().LInfo().
		Message("notified subscription of message").
		String("topic", message.Topic).
		End()

	return nil
}

// UnlistenAllWithId sends a signal to remove possible handler with giving id from all topics.
func (tm *PbRelay) UnlistenAllWithId(id nxid.ID) {
	var info subInfo
	info.id = id

	tm.chl.RLock()
	for _, groups := range tm.channels {
		for _, channel := range groups {
			channel.remove(info)
			// if it's empty after half a second then close channel.
			tm.retireAfter(channel, time.Second/2)
		}
	}
	tm.chl.RUnlock()
}

// UnlistenWithId sends a signal to remove possible handler with giving id from specific topic.
func (tm *PbRelay) UnlistenWithId(topic string, id nxid.ID) {
	var info subInfo
	info.id = id
	info.topic = topic

	tm.chl.RLock()
	channels, hasChannel := tm.channels[info.topic]
	tm.chl.RUnlock()

	if !hasChannel {
		return
	}

	for _, channel := range channels {
		channel.remove(info)
		// if it's empty after half a second then close channel.
		tm.retireAfter(channel, time.Second/2)
	}
}

func (tm *PbRelay) Group(topic string, group string) *PbGroup {
	var logStack = njson.Log(tm.logger)

	tm.chl.RLock()
	var channels, hasChannel = tm.channels[topic]
	tm.chl.RUnlock()

	if hasChannel {
		for _, channel := range channels {
			if channel.group == group {
				return channel
			}
		}
	}

	var newCtx, canceler = context.WithCancel(tm.ctx)
	var newChannel = &PbGroup{
		topic:         topic,
		group:         group,
		logger:        tm.logger,
		commands:      make(chan func()),
		ctx:           newCtx,
		canceler:      canceler,
		manager:       tm,
		subscriptions: map[nxid.ID]*subInfo{},
	}

	tm.waiter.Add(1)
	go func(t string) {
		var logStack = njson.Log(tm.logger)

		defer tm.waiter.Done()

		// retire topic management
		defer tm.retire(t)

		newChannel.Run()

		logStack.New().LInfo().
			Message("removing subscription transportManager for topic").
			String("topic", t).
			End()
	}(topic)

	logStack.New().LInfo().
		Message("adding new transport channel to map").
		String("topic", topic).
		End()

	tm.chl.Lock()
	tm.channels[topic] = append(tm.channels[topic], newChannel)
	tm.chl.Unlock()

	logStack.New().LInfo().
		Message("adding subscription to transport channel").
		String("topic", topic).
		End()

	return newChannel
}

func (tm *PbRelay) Wait() {
	tm.waiter.Wait()
}

func (tm *PbRelay) remove(info subInfo) {
	tm.chl.RLock()
	channels, hasChannel := tm.channels[info.topic]
	tm.chl.RUnlock()

	if hasChannel {
		for _, channel := range channels {
			channel.remove(info)

			// if it's empty after half a second then close channel.
			tm.retireAfter(channel, time.Second/2)
		}
	}
}

func (tm *PbRelay) retireAfter(channel *PbGroup, dur time.Duration) {
	select {
	case <-tm.ctx.Done():
		return
	case <-time.After(dur):
		if channel.IsEmpty() {
			channel.canceler()
		}
	}
}

func (tm *PbRelay) retire(topic string) {
	tm.chl.Lock()
	defer tm.chl.Unlock()
	if channels, hasChannel := tm.channels[topic]; hasChannel {
		for _, channel := range channels {
			channel.canceler()
			delete(tm.channels, topic)
		}
	}
}

type subInfo struct {
	topic   string
	group   string
	id      nxid.ID
	err     error
	handler TransportResponse
	sub     *PbGroup
	manager *PbGroup
}

func (info *subInfo) Group() string {
	return info.group
}

func (info *subInfo) Topic() string {
	return info.topic
}

func (info *subInfo) Err() error {
	return info.err
}

func (info subInfo) Close() {
	info.manager.remove(info)
}

type PbGroup struct {
	id            nxid.ID
	topic         string
	group         string
	logger        Logger
	commands      chan func()
	ctx           context.Context
	canceler      context.CancelFunc
	channel       Channel
	manager       *PbRelay
	subscriptions map[nxid.ID]*subInfo
}

func (sc *PbGroup) Listen(handler TransportResponse) Channel {
	var sub subInfo
	sub.sub = sc
	sub.group = sc.group
	sub.topic = sc.topic
	sub.id = nxid.New()
	sub.manager = sc
	sub.handler = handler
	sub.err = sc.add(sub)
	return &sub
}

func (sc *PbGroup) IsEmpty() bool {
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

func (sc *PbGroup) Link(bus MessageBus) {
	var deliveryChan = make(chan struct{})

	var doAction = func() {
		var logStack = njson.Log(sc.logger)

		sc.channel = bus.Listen(sc.topic, sc.group, TransportResponseFunc(sc.Notify))

		logStack.New().LInfo().
			Message("added subscriber to topic").
			String("topic", sc.topic).
			String("id", sc.id.String()).
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

func (sc *PbGroup) Add(id nxid.ID, tr TransportResponse) error {
	var info subInfo
	info.id = id
	info.sub = sc
	info.handler = tr
	info.manager = sc
	return sc.add(info)
}

var ErrAlreadySubscribed = nerror.New("id is already used")

func (sc *PbGroup) add(info subInfo) error {
	var deliveryChan = make(chan error, 1)

	var doAction = func() {
		var logStack = njson.Log(sc.logger)

		if _, hasSub := sc.subscriptions[info.id]; hasSub {
			deliveryChan <- nerror.WrapOnly(ErrAlreadySubscribed)
			return
		}

		sc.subscriptions[info.id] = &info

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
			return sc.ctx.Err()
		case err := <-deliveryChan:
			return err
		}
	case <-sc.ctx.Done():
		return sc.ctx.Err()
	}
}

func (sc *PbGroup) Remove(id nxid.ID) {
	var info subInfo
	info.id = id
	info.sub = sc
	info.manager = sc
	sc.remove(info)
}

func (sc *PbGroup) remove(info subInfo) {
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

// Notify will notify the groups of handlers and will return the first occurrence
// of a message error seen by one of the handlers. This means if one of the handlers returns
// an error then the publisher will be notified as if all handlers failed to handle the
// message. If you dont want that, then dont return an error from any of your registered
// handlers and handle the error appropriately.
func (sc *PbGroup) Notify(ctx context.Context, msg Message, transport Transport) MessageErr {
	var logStack = njson.Log(sc.logger)

	logStack.New().LInfo().
		Message("received new message").
		Object("message", msg).
		String("topic", sc.topic).
		End()

	var errChan = make(chan MessageErr, 1)
	var doDistribution = func() {
		var logStack = njson.Log(sc.logger)

		logStack.New().Message("notifying all handlers with message").
			String("topic", sc.topic).
			End()

		for _, sub := range sc.subscriptions {
			func(subscriber TransportResponse, m Message) {
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

				if handleErr := subscriber.Handle(ctx, m, transport); handleErr != nil {
					logStack.New().Message("error occurred handled message").
						Object("message", m).
						String("topic", sc.topic).
						String("error", nerror.WrapOnly(handleErr).Error()).
						End()

					// if one item failed to handle the message, report this.
					if len(errChan) == 0 {
						errChan <- handleErr
					}
					return
				}

				logStack.New().Message("handled message delivery successfully").
					Object("message", m).
					String("topic", sc.topic).
					End()
			}(sub.handler, msg)
		}

		errChan <- nil
	}

	var timeoutChan <-chan time.Time
	if msg.Within > 0 {
		timeoutChan = time.After(msg.Within)
	}

	select {
	case <-timeoutChan:
		logStack.New().LWarn().
			Message("failed to deliver message to handlers due to timeout").
			Object("message", msg).
			String("topic", sc.topic).
			End()
		return WrapErr(nerror.New("failed to deliver"), false)
	case sc.commands <- doDistribution:
		return <-errChan
	case <-sc.ctx.Done():
		logStack.New().LWarn().
			Message("failed to deliver message to handlers").
			Object("message", msg).
			String("topic", sc.topic).
			End()
		return WrapErr(nerror.New("context was closed"), false)
	}
}

func (sc *PbGroup) Run() {
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

type BusRelay struct {
	Relay       *PbRelay
	Bus         MessageBus
	bl          sync.RWMutex
	busChannels map[string]Channel
}

func BusWithRelay(relay *PbRelay, bus MessageBus) *BusRelay {
	return &BusRelay{Bus: bus, Relay: relay}
}

func NewBusRelay(ctx context.Context, logger Logger, bus MessageBus) *BusRelay {
	return &BusRelay{Bus: bus, Relay: NewPbRelay(ctx, logger)}
}

func (tm *BusRelay) Handle(ctx context.Context, message Message, transport Transport) MessageErr {
	return tm.Relay.Handle(ctx, message, transport)
}

func (br *BusRelay) Group(topic string, grp string) *PbGroup {
	var busTopic = fmt.Sprintf("%s:%s", topic, grp)

	br.bl.RLock()
	var busTopicChannel = br.busChannels[busTopic]
	br.bl.RUnlock()

	if busTopicChannel != nil {
		return br.Relay.Group(topic, grp)
	}

	var busChannel = br.Bus.Listen(topic, grp, TransportResponseFunc(func(ctx context.Context, message Message, transport Transport) MessageErr {
		return br.Relay.Handle(ctx, message, transport)
	}))

	br.bl.Lock()
	br.busChannels[busTopic] = busChannel
	br.bl.Unlock()

	return br.Relay.Group(topic, grp)
}
