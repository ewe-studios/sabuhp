package supabaiza

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type Sub struct {
	Topic    string
	Callback ChannelResponse
	Channel  Channel
}

func TestNewActionHub_StartStop(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sendList []*Message
	var channel []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{
		BroadcastFunc: func(message *Message, timeout time.Duration) error {
			sendList = append(sendList, message)
			return nil
		},
		ChannelFunc: func(topic string, callback ChannelResponse) Channel {
			var noChannel NoPubSubChannel
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

	var sendList []*Message
	var channels []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{}
	pubsub.BroadcastFunc = func(message *Message, timeout time.Duration) error {
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubsub)
			}
		}
		return nil
	}
	pubsub.ChannelFunc = func(topic string, callback ChannelResponse) Channel {
		var noChannel NoPubSubChannel
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
		pubsub,
		logger,
	)

	hub.Start()

	require.Len(t, channels, 1)

	require.NoError(t, pubsub.Broadcast(&Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Payload:  BinaryPayload("alex"),
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
	var sendList []*Message

	var chl sync.Mutex
	var channels []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{}
	pubsub.BroadcastFunc = func(message *Message, timeout time.Duration) error {
		sl.Lock()
		defer sl.Unlock()
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubsub)
			}
		}
		return nil
	}
	pubsub.ChannelFunc = func(topic string, callback ChannelResponse) Channel {
		chl.Lock()
		defer chl.Unlock()
		var noChannel NoPubSubChannel
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
		pubsub,
		logger,
	)

	hub.Start()

	var ack = make(chan struct{}, 3)
	require.NoError(t, hub.Do("say_hello", sayHelloAction(ctx, ack), SlaveWorkerRequest{
		ActionName: "hello_slave",
		Action: func(ctx context.Context, to string, message *Message, pubsub PubSub) {
			if err := pubsub.Broadcast(&Message{
				Topic:    "say_hello",
				FromAddr: to,
				Payload:  BinaryPayload("slave from hello"),
				Metadata: nil,
			}, 0); err != nil {
				log.Print(err.Error())
			}
		},
	}))

	chl.Lock()
	require.Len(t, channels, 2)
	chl.Unlock()

	require.NoError(t, pubsub.Broadcast(&Message{
		FromAddr: "yay",
		Topic:    "say_hello/slaves/hello_slave",
		Payload:  BinaryPayload("alex"),
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
	var sendList []*Message

	var chl sync.Mutex
	var channels []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{}
	pubsub.BroadcastFunc = func(message *Message, timeout time.Duration) error {
		sl.Lock()
		defer sl.Unlock()

		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubsub)
			}
		}
		return nil
	}
	pubsub.ChannelFunc = func(topic string, callback ChannelResponse) Channel {
		chl.Lock()
		defer chl.Unlock()
		var noChannel NoPubSubChannel
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
		pubsub,
		logger,
	)

	hub.Start()

	var ack = make(chan struct{}, 1)
	require.NoError(t, hub.Do("say_hello", sayHelloAction(ctx, ack)))

	chl.Lock()
	require.Len(t, channels, 1)
	chl.Unlock()

	require.NoError(t, pubsub.Broadcast(&Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Payload:  BinaryPayload("alex"),
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
		config.Action = func(ctx context.Context, to string, message *Message, pubsub PubSub) {
			pubsub.Broadcast(&Message{
				Topic:    message.FromAddr,
				FromAddr: to,
				Payload:  BinaryPayload("Hello"),
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
