package supabaiza

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp/pubsub"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"
)

type Sub struct {
	Topic    string
	Callback pubsub.ChannelResponse
	Channel  sabuhp.Channel
}

func BasicMsg(topic string, message string, fromAddr string) *sabuhp.Message {
	return &sabuhp.Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

type noPubSub struct {
	DelegateFunc  func(message *sabuhp.Message, timeout time.Duration) error
	BroadcastFunc func(message *sabuhp.Message, timeout time.Duration) error
	ChannelFunc   func(topic string, callback pubsub.ChannelResponse) sabuhp.Channel
}

func (n noPubSub) Channel(topic string, callback pubsub.ChannelResponse) sabuhp.Channel {
	if n.ChannelFunc != nil {
		return n.ChannelFunc(topic, callback)
	}
	return &noPubSubChannel{}
}

func (n noPubSub) Delegate(message *sabuhp.Message, timeout time.Duration) error {
	if n.DelegateFunc != nil {
		return n.DelegateFunc(message, timeout)
	}
	return nil
}

func (n noPubSub) Broadcast(message *sabuhp.Message, timeout time.Duration) error {
	if n.BroadcastFunc != nil {
		return n.BroadcastFunc(message, timeout)
	}
	return nil
}

type transportImpl struct {
	ConnFunc      func() sabuhp.Conn
	SendToOneFunc func(data *sabuhp.Message, timeout time.Duration) error
	SendToAllFunc func(data *sabuhp.Message, timeout time.Duration) error
	ListenFunc    func(topic string, handler sabuhp.TransportResponse) sabuhp.Channel
}

func (t transportImpl) Conn() sabuhp.Conn {
	return t.ConnFunc()
}

func (t transportImpl) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	return t.ListenFunc(topic, handler)
}

func (t transportImpl) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToOneFunc(data, timeout)
}

func (t transportImpl) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	return t.SendToAllFunc(data, timeout)
}

type noPubSubChannel struct {
	Error error
}

func (n noPubSubChannel) Err() error {
	return n.Error
}

func (n noPubSubChannel) Close() {
	// do nothing
}

type loggerPub struct{}

func (l loggerPub) Log(cb *njson.JSON) {
	log.Println(cb.Message())
	log.Println("")
}

func TestNewActionHub_StartStop(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sendList []*sabuhp.Message
	var channel []Sub

	var logger = &loggerPub{}
	var pubsub = &noPubSub{
		BroadcastFunc: func(message *sabuhp.Message, timeout time.Duration) error {
			sendList = append(sendList, message)
			return nil
		},
		ChannelFunc: func(topic string, callback pubsub.ChannelResponse) sabuhp.Channel {
			var noChannel noPubSubChannel
			channel = append(channel, Sub{
				Topic:    topic,
				Callback: callback,
				Channel:  &noChannel,
			})
			return &noChannel
		},
	}

	var templateRegistry = NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var hub = NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubsub,
		logger,
	)

	hub.Start()

	go func() {
		<-time.After(time.Second)
		canceler()
	}()

	hub.Wait()
}

func TestNewActionHub_WithTemplateRegistry(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sendList []*sabuhp.Message
	var channels []Sub

	var logger = &loggerPub{}
	var pubb = &noPubSub{}
	pubb.BroadcastFunc = func(message *sabuhp.Message, timeout time.Duration) error {
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubb)
			}
		}
		return nil
	}
	pubb.ChannelFunc = func(topic string, callback pubsub.ChannelResponse) sabuhp.Channel {
		var noChannel noPubSubChannel
		channels = append(channels, Sub{
			Topic:    topic,
			Callback: callback,
			Channel:  &noChannel,
		})
		return &noChannel
	}

	var ctx, canceler = context.WithCancel(context.Background())

	var ack = make(chan struct{}, 1)
	var templateRegistry = NewWorkerTemplateRegistry()
	templateRegistry.Register(ActionWorkerRequest{
		ActionName:    "say_hello",
		PubSubTopic:   "say_hello",
		WorkerCreator: sayHelloAction(ctx, ack),
	})

	var hub = NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubb,
		logger,
	)

	hub.Start()

	require.Len(t, channels, 1)

	require.NoError(t, pubb.Broadcast(&sabuhp.Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Payload:  sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}, 0))

	<-ack

	require.Len(t, sendList, 2)
	require.Equal(t, "say_hello", sendList[0].Topic)
	require.Equal(t, "yay", sendList[1].Topic)

	canceler()
	hub.Wait()
}

func TestNewActionHub_WithEmptyTemplateRegistryWithSlaves(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sl sync.Mutex
	var sendList []*sabuhp.Message

	var chl sync.Mutex
	var channels []Sub

	var logger = &loggerPub{}
	var pubb = &noPubSub{}
	pubb.BroadcastFunc = func(message *sabuhp.Message, timeout time.Duration) error {
		sl.Lock()
		defer sl.Unlock()
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubb)
			}
		}
		return nil
	}
	pubb.ChannelFunc = func(topic string, callback pubsub.ChannelResponse) sabuhp.Channel {
		chl.Lock()
		defer chl.Unlock()
		var noChannel noPubSubChannel
		channels = append(channels, Sub{
			Topic:    topic,
			Callback: callback,
			Channel:  &noChannel,
		})
		return &noChannel
	}

	var templateRegistry = NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var hub = NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubb,
		logger,
	)

	hub.Start()

	var ack = make(chan struct{}, 3)
	require.NoError(t, hub.Do("say_hello", sayHelloAction(ctx, ack), SlaveWorkerRequest{
		ActionName: "hello_slave",
		Action: func(ctx context.Context, to string, message *sabuhp.Message, sub pubsub.PubSub) {
			if err := sub.Broadcast(&sabuhp.Message{
				Topic:    "say_hello",
				FromAddr: to,
				Payload:  sabuhp.BinaryPayload("slave from hello"),
				Metadata: nil,
			}, 0); err != nil {
				log.Print(err.Error())
			}
		},
	}))

	chl.Lock()
	require.Len(t, channels, 2)
	chl.Unlock()

	require.NoError(t, pubb.Broadcast(&sabuhp.Message{
		FromAddr: "yay",
		Topic:    "say_hello/slaves/hello_slave",
		Payload:  sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}, 0))

	<-ack

	sl.Lock()
	require.Equal(t, "say_hello/slaves/hello_slave", sendList[0].Topic)
	require.Equal(t, "say_hello", sendList[1].Topic)
	sl.Unlock()

	canceler()
	hub.Wait()
}

func TestNewActionHub_WithEmptyTemplateRegistry(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sl sync.Mutex
	var sendList []*sabuhp.Message

	var chl sync.Mutex
	var channels []Sub

	var logger = &loggerPub{}
	var pubb = &noPubSub{}
	pubb.BroadcastFunc = func(message *sabuhp.Message, timeout time.Duration) error {
		sl.Lock()
		defer sl.Unlock()

		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubb)
			}
		}
		return nil
	}
	pubb.ChannelFunc = func(topic string, callback pubsub.ChannelResponse) sabuhp.Channel {
		chl.Lock()
		defer chl.Unlock()
		var noChannel noPubSubChannel
		channels = append(channels, Sub{
			Topic:    topic,
			Callback: callback,
			Channel:  &noChannel,
		})
		return &noChannel
	}

	var templateRegistry = NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var hub = NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubb,
		logger,
	)

	hub.Start()

	var ack = make(chan struct{}, 1)
	require.NoError(t, hub.Do("say_hello", sayHelloAction(ctx, ack)))

	chl.Lock()
	require.Len(t, channels, 1)
	chl.Unlock()

	require.NoError(t, pubb.Broadcast(&sabuhp.Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Payload:  sabuhp.BinaryPayload("alex"),
		Metadata: nil,
	}, 0))

	<-ack

	sl.Lock()
	require.Len(t, sendList, 2)
	require.Equal(t, "say_hello", sendList[0].Topic)
	require.Equal(t, "yay", sendList[1].Topic)
	sl.Unlock()

	canceler()
	hub.Wait()
}

func sayHelloAction(ctx context.Context, ack chan struct{}) ActionWorkerGroupCreator {
	return func(config ActionWorkerConfig) *ActionWorkerGroup {
		config.Instance = ScalingInstances
		config.Behaviour = RestartAll
		config.Action = func(ctx context.Context, to string, message *sabuhp.Message, sub pubsub.PubSub) {
			sub.Broadcast(&sabuhp.Message{
				Topic:    message.FromAddr,
				FromAddr: to,
				Payload:  sabuhp.BinaryPayload("Hello"),
				Metadata: nil,
			}, 0)

			select {
			case ack <- struct{}{}:
				return
			case <-ctx.Done():
				return
			}

		}
		return NewWorkGroup(config)
	}
}
