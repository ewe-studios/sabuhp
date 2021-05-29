package actions

import (
	"context"
	"testing"
	"time"

	"github.com/ewe-studios/sabuhp/injectors"
	"github.com/ewe-studios/sabuhp/testingutils"

	"github.com/ewe-studios/sabuhp"

	"github.com/stretchr/testify/require"
)

func TestNewActionHub_StartStop(t *testing.T) {
	var escalationHandling = func(escalation Escalation, hub *ActionHub) {
		require.NotNil(t, escalation)
		require.NotNil(t, hub)
	}

	var mb sabuhp.BusBuilder
	var sendList []sabuhp.Message
	var logger = &testingutils.LoggerPub{}
	mb.SendFunc = func(msgs ...sabuhp.Message) {
		sendList = append(sendList, msgs...)
	}

	var injector = injectors.NewInjector()
	var templateRegistry = NewWorkerTemplateRegistry()
	var ctx, canceler = context.WithCancel(context.Background())
	var relay = sabuhp.NewBusRelay(ctx, logger, &mb)
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

	var mb sabuhp.BusBuilder
	var sendList []sabuhp.Message
	var channels []testingutils.SubChannel

	var logger = &testingutils.LoggerPub{}
	mb.SendFunc = func(msgs ...sabuhp.Message) {
		sendList = append(sendList, msgs...)
		for _, channel := range channels {
			for _, message := range msgs {
				if channel.T == message.Topic {
					_ = channel.Handler.Handle(context.Background(), message, sabuhp.Transport{Bus: &mb})
				}
			}
		}
		return
	}

	var addedChannel = make(chan struct{}, 1)
	mb.ListenFunc = func(topic string, group string, callback sabuhp.TransportResponse) sabuhp.Channel {
		var noChannel testingutils.SubChannel
		noChannel.Handler = callback
		noChannel.T = topic
		noChannel.G = group
		channels = append(channels, noChannel)
		addedChannel <- struct{}{}
		return &noChannel
	}

	var ctx, canceler = context.WithCancel(context.Background())
	var relay = sabuhp.NewBusRelay(ctx, logger, &mb)

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

	mb.Send(sabuhp.Message{
		Topic:    "say_hello",
		FromAddr: "yay",
		Bytes:    []byte("alex"),
		Metadata: nil,
	})

	<-ack

	require.Len(t, sendList, 2)
	require.Equal(t, "say_hello", sendList[0].Topic)
	require.Equal(t, "yay", sendList[1].Topic)

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

			sub.Bus.Send(sabuhp.Message{
				Topic:    message.FromAddr,
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
