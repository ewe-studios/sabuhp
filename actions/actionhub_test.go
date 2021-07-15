package actions

import (
	"context"
	"github.com/ewe-studios/sabuhp/sabu"
	"testing"
	"time"

	"github.com/ewe-studios/sabuhp/injectors"
	"github.com/ewe-studios/sabuhp/testingutils"

	"github.com/stretchr/testify/require"
)

func TestNewActionHub_StartStop(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var mb sabu.BusBuilder
	var sendList []sabu.Message
	var logger = &testingutils.LoggerPub{}
	mb.SendFunc = func(msgs ...sabu.Message) {
		sendList = append(sendList, msgs...)
	}

	var injector = injectors.NewInjector()
	var templateRegistry = NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var relay = sabu.NewBusRelay(ctx, logger, &mb)
	var hub = NewActionHub(
		ctx,
		escalationHandling,
		templateRegistry,
		injector,
		&mb,
		relay,
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

	var mb sabu.BusBuilder
	var sendList []sabu.Message
	var channels []testingutils.SubChannel

	var logger = &testingutils.LoggerPub{}
	mb.SendFunc = func(msgs ...sabu.Message) {
		sendList = append(sendList, msgs...)
		for _, channel := range channels {
			for _, message := range msgs {
				if channel.T == message.Topic.String() {
					_ = channel.Handler.Handle(context.Background(), message, sabu.Transport{Bus: &mb})
				}
			}
		}
		return
	}

	var addedChannel = make(chan struct{}, 1)
	mb.ListenFunc = func(topic string, group string, callback sabu.TransportResponse) sabu.Channel {
		var noChannel testingutils.SubChannel
		noChannel.Handler = callback
		noChannel.T = topic
		noChannel.G = group
		channels = append(channels, noChannel)
		addedChannel <- struct{}{}
		return &noChannel
	}

	var ctx, canceler = context.WithCancel(context.Background())
	var relay = sabu.NewBusRelay(ctx, logger, &mb)

	var ack = make(chan struct{}, 1)
	var injector = injectors.NewInjector()
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
		injector,
		&mb,
		relay,
		logger,
	)

	hub.Start()

	<-addedChannel
	require.Len(t, channels, 1)

	mb.Send(sabu.Message{
		Topic:    sabu.T("say_hello"),
		FromAddr: "yay",
		Bytes:    []byte("alex"),
		Metadata: nil,
	})

	<-ack

	require.Len(t, sendList, 2)
	require.Equal(t, "say_hello", sendList[0].Topic.String())
	require.Equal(t, "yay", sendList[1].Topic.String())

	canceler()
	hub.Wait()
}

func sayHelloAction(ctx context.Context, ack chan struct{}) WorkGroupCreator {
	return func(config WorkerConfig) *WorkerGroup {
		config.Instance = ScalingInstances
		config.Behaviour = RestartAll
		config.Action = ActionFunc(func(ctx context.Context, job Job) {
			var to = job.To
			var message = job.Msg
			var sub = job.Transport

			sub.Bus.Send(sabu.Message{
				Topic:    sabu.T(message.FromAddr),
				FromAddr: to,
				Bytes:    []byte("Hello"),
				Metadata: nil,
			})

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
