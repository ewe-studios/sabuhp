package supabaiza_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go/log"

	"github.com/stretchr/testify/require"

	"github.com/influx6/sabuhp/supabaiza"
)

type Sub struct {
	Topic    string
	Callback supabaiza.ChannelResponse
	Channel  supabaiza.Channel
}

func TestNewActionHub_StartStop(t *testing.T) {
	var escalationHandling = func(escalation supabaiza.Escalation, hub *supabaiza.ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sendList []*supabaiza.Message
	var channel []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{
		SendFunc: func(message *supabaiza.Message, timeout time.Duration) error {
			sendList = append(sendList, message)
			return nil
		},
		ChannelFunc: func(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel {
			var noChannel NoPubSubChannel
			channel = append(channel, Sub{
				Topic:    topic,
				Callback: callback,
				Channel:  &noChannel,
			})
			return &noChannel
		},
	}

	var templateRegistry = supabaiza.NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var hub = supabaiza.NewActionHub(
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
	var escalationHandling = func(escalation supabaiza.Escalation, hub *supabaiza.ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sendList []*supabaiza.Message
	var channels []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{}
	pubsub.SendFunc = func(message *supabaiza.Message, timeout time.Duration) error {
		sendList = append(sendList, message)
		for _, channel := range channels {
			if channel.Topic == message.Topic {
				channel.Callback(message, pubsub)
			}
		}
		return nil
	}
	pubsub.ChannelFunc = func(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel {
		var noChannel NoPubSubChannel
		channels = append(channels, Sub{
			Topic:    topic,
			Callback: callback,
			Channel:  &noChannel,
		})
		return &noChannel
	}

	var templateRegistry = supabaiza.NewWorkerTemplateRegistry()
	templateRegistry.Register(supabaiza.ActionWorkerRequest{
		ActionName:    "say_hello",
		PubSubTopic:   "say_hello",
		WorkerCreator: sayHelloAction,
	})

	var ctx, canceler = context.WithCancel(context.Background())
	var hub = supabaiza.NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubsub,
		logger,
	)

	hub.Start()

	require.Len(t, channels, 1)

	var ack = make(chan struct{}, 1)
	require.NoError(t, pubsub.Send(&supabaiza.Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Payload:  supabaiza.BinaryPayload("alex"),
		Metadata: nil,
		Ack:      ack,
		Nack:     nil,
	}, 0))

	<-ack

	require.Len(t, sendList, 2)
	require.Equal(t, "say_hello", sendList[0].Topic)
	require.Equal(t, "yay", sendList[1].Topic)

	canceler()
	hub.Wait()
}

func TestNewActionHub_WithEmptyTemplateRegistryWithSlaves(t *testing.T) {
	var escalationHandling = func(escalation supabaiza.Escalation, hub *supabaiza.ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sl sync.Mutex
	var sendList []*supabaiza.Message

	var chl sync.Mutex
	var channels []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{}
	pubsub.SendFunc = func(message *supabaiza.Message, timeout time.Duration) error {
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
	pubsub.ChannelFunc = func(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel {
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

	var templateRegistry = supabaiza.NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var hub = supabaiza.NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubsub,
		logger,
	)

	hub.Start()

	require.NoError(t, hub.Do("say_hello", sayHelloAction, supabaiza.SlaveWorkerRequest{
		ActionName: "hello_slave",
		Action: func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
			if err := pubsub.Send(&supabaiza.Message{
				Topic:    "say_hello",
				FromAddr: to,
				Payload:  supabaiza.BinaryPayload("slave from hello"),
				Metadata: nil,
				Ack:      nil,
				Nack:     nil,
			}, 0); err != nil {
				log.Error(err)
			}
		},
	}))

	chl.Lock()
	require.Len(t, channels, 2)
	chl.Unlock()

	var ack = make(chan struct{}, 1)
	require.NoError(t, pubsub.Send(&supabaiza.Message{
		FromAddr: "yay",
		Topic:    "say_hello/slaves/hello_slave",
		Payload:  supabaiza.BinaryPayload("alex"),
		Metadata: nil,
		Ack:      ack,
		Nack:     nil,
	}, 0))

	<-ack

	sl.Lock()
	require.Len(t, sendList, 2)
	require.Equal(t, "say_hello/slaves/hello_slave", sendList[0].Topic)
	require.Equal(t, "say_hello", sendList[1].Topic)
	sl.Unlock()

	canceler()
	hub.Wait()
}

func TestNewActionHub_WithEmptyTemplateRegistry(t *testing.T) {
	var escalationHandling = func(escalation supabaiza.Escalation, hub *supabaiza.ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var sl sync.Mutex
	var sendList []*supabaiza.Message

	var chl sync.Mutex
	var channels []Sub

	var logger = &LoggerPub{}
	var pubsub = &NoPubSub{}
	pubsub.SendFunc = func(message *supabaiza.Message, timeout time.Duration) error {
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
	pubsub.ChannelFunc = func(topic string, callback supabaiza.ChannelResponse) supabaiza.Channel {
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

	var templateRegistry = supabaiza.NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var hub = supabaiza.NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		pubsub,
		logger,
	)

	hub.Start()

	require.NoError(t, hub.Do("say_hello", sayHelloAction))

	chl.Lock()
	require.Len(t, channels, 1)
	chl.Unlock()

	var ack = make(chan struct{}, 1)
	require.NoError(t, pubsub.Send(&supabaiza.Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Payload:  supabaiza.BinaryPayload("alex"),
		Metadata: nil,
		Ack:      ack,
		Nack:     nil,
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

func sayHelloAction(config supabaiza.ActionWorkerConfig) *supabaiza.ActionWorkerGroup {
	config.Instance = supabaiza.ScalingInstances
	config.Behaviour = supabaiza.RestartAll
	config.Action = func(ctx context.Context, to string, message *supabaiza.Message, pubsub supabaiza.PubSub) {
		if err := pubsub.Send(&supabaiza.Message{
			Topic:    message.FromAddr,
			FromAddr: to,
			Payload:  supabaiza.BinaryPayload("Hello"),
			Metadata: nil,
			Ack:      nil,
			Nack:     nil,
		}, 0); err != nil {
			panic(err)
		}
	}
	return supabaiza.NewWorkGroup(config)
}
