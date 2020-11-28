package slaves

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp/testingutils"

	"github.com/influx6/sabuhp"

	"github.com/stretchr/testify/require"
)

type Sub struct {
	Topic    string
	Callback sabuhp.TransportResponse
	Channel  sabuhp.Channel
}

func TestNewActionHub_StartStop(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sendList []*sabuhp.Message
	var channel []Sub

	var logger = &testingutils.LoggerPub{}
	var pubsub = &testingutils.NoPubSub{
		BroadcastFunc: func(message *sabuhp.Message, timeout time.Duration) error {
			sendList = append(sendList, message)
			return nil
		},
		ChannelFunc: func(topic string, callback sabuhp.TransportResponse) sabuhp.Channel {
			var noChannel testingutils.NoPubSubChannel
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

	var logger = &testingutils.LoggerPub{}
	var pubb = &testingutils.NoPubSub{}
	pubb.BroadcastFunc = func(message *sabuhp.Message, timeout time.Duration) error {
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				_ = channel.Callback.Handle(message, pubb)
			}
		}
		return nil
	}

	var addedChannel = make(chan struct{}, 1)
	pubb.ChannelFunc = func(topic string, callback sabuhp.TransportResponse) sabuhp.Channel {
		var noChannel testingutils.NoPubSubChannel
		channels = append(channels, Sub{
			Topic:    topic,
			Callback: callback,
			Channel:  &noChannel,
		})
		addedChannel <- struct{}{}
		return &noChannel
	}

	var ctx, canceler = context.WithCancel(context.Background())

	var ack = make(chan struct{}, 1)
	var templateRegistry = NewWorkerTemplateRegistry()
	templateRegistry.Register(WorkerRequest{
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

	<-addedChannel
	require.Len(t, channels, 1)

	require.NoError(t, pubb.SendToAll(&sabuhp.Message{
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

	var logger = &testingutils.LoggerPub{}
	var pubb = &testingutils.NoPubSub{}
	pubb.BroadcastFunc = func(message *sabuhp.Message, timeout time.Duration) error {
		sl.Lock()
		defer sl.Unlock()
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback.Handle(message, pubb)
			}
		}
		return nil
	}
	pubb.ChannelFunc = func(topic string, callback sabuhp.TransportResponse) sabuhp.Channel {
		chl.Lock()
		defer chl.Unlock()
		var noChannel testingutils.NoPubSubChannel
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
		Action: ActionFunc(func(ctx context.Context, to string, message *sabuhp.Message, sub sabuhp.Transport) {
			if err := sub.SendToAll(&sabuhp.Message{
				Topic:    "say_hello",
				FromAddr: to,
				Payload:  sabuhp.BinaryPayload("slave from hello"),
				Metadata: nil,
			}, 0); err != nil {
				log.Print(err.Error())
			}
		}),
	}))

	chl.Lock()
	require.Len(t, channels, 2)
	chl.Unlock()

	require.NoError(t, pubb.SendToAll(&sabuhp.Message{
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

	var logger = &testingutils.LoggerPub{}
	var pubb = &testingutils.NoPubSub{}
	pubb.BroadcastFunc = func(message *sabuhp.Message, timeout time.Duration) error {
		sl.Lock()
		defer sl.Unlock()

		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback.Handle(message, pubb)
			}
		}
		return nil
	}
	pubb.ChannelFunc = func(topic string, callback sabuhp.TransportResponse) sabuhp.Channel {
		chl.Lock()
		defer chl.Unlock()
		var noChannel testingutils.NoPubSubChannel
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

	require.NoError(t, pubb.SendToAll(&sabuhp.Message{
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

func sayHelloAction(ctx context.Context, ack chan struct{}) WorkGroupCreator {
	return func(config WorkerConfig) *WorkerGroup {
		config.Instance = ScalingInstances
		config.Behaviour = RestartAll
		config.Action = ActionFunc(func(ctx context.Context, to string, message *sabuhp.Message, sub sabuhp.Transport) {
			_ = sub.SendToAll(&sabuhp.Message{
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

		})
		return NewWorkGroup(config)
	}
}
