package redispub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/codecs"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"

	"github.com/stretchr/testify/require"

	"github.com/ewe-studios/sabuhp/testingutils"
	redis "github.com/go-redis/redis/v8"
)

var codec = &codecs.JsonCodec{}

func TestRedis_Start_Stop_WithCancel(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var logger = &testingutils.LoggerPub{}
	var config Config
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger
	config.Redis = redis.Options{
		Network: "tcp",
	}

	var pb, err = NewMessageRail(config)
	require.NoError(t, err)
	require.NotNil(t, pb)

	pb.Start()

	go func() {
		<-time.After(time.Second * 1)
		canceler()
	}()

	pb.Wait()
}

func TestRedis_Start_Stop(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	defer canceler()

	var logger = &testingutils.LoggerPub{}
	var config Config
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger
	config.Redis = redis.Options{
		Network: "tcp",
	}

	var pb, err = NewMessageRail(config)
	require.NoError(t, err)
	require.NotNil(t, pb)

	pb.Start()

	go func() {
		<-time.After(time.Second * 1)
		pb.Stop()
	}()

	pb.Wait()
}

func TestRedis_PubSub_SendToAll(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	defer canceler()

	var logger = &testingutils.LoggerPub{}
	var config Config
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger
	config.Redis = redis.Options{
		Network: "tcp",
	}

	var pb, err = NewMessageRail(config)
	require.NoError(t, err)
	require.NotNil(t, pb)

	pb.Start()

	var content = []byte("\"yes\"")
	var whyMessage = sabuhp.NewMessage("why", "me", content)
	var whatMessage = sabuhp.NewMessage("what", "me", content)

	var delivered sync.WaitGroup
	delivered.Add(2)

	var channel = pb.Listen(
		"what",
		sabuhp.TransportResponseFunc(
			func(message *sabuhp.Message, transport sabuhp.MessageBus) sabuhp.MessageErr {
				delivered.Done()

				if err := transport.SendToAll(whyMessage, 0); err != nil {
					logger.Log(njson.MJSON("failed to send message", func(event npkg.Encoder) {
						event.String("error", err.Error())
					}))
				}
				return nil
			}))

	require.NoError(t, channel.Err())

	defer channel.Close()

	var channel2 = pb.Listen("why", sabuhp.TransportResponseFunc(
		func(message *sabuhp.Message, transport sabuhp.MessageBus) sabuhp.MessageErr {
			delivered.Done()
			return nil
		}))

	require.NoError(t, channel2.Err())

	defer channel2.Close()

	require.NoError(t, pb.SendToAll(whatMessage, time.Second*2))

	delivered.Wait()

	canceler()
	pb.Wait()
}

func TestRedis_PubSub_SendToOne(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())
	defer canceler()

	var logger = &testingutils.LoggerPub{}
	var config Config
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger
	config.Redis = redis.Options{
		Network: "tcp",
	}

	var pb, err = NewMessageRail(config)
	require.NoError(t, err)
	require.NotNil(t, pb)

	pb.Start()

	var whyMessage = sabuhp.NewMessage("why2", "me", []byte("yes"))
	var whatMessage = sabuhp.NewMessage("what2", "me", []byte("yes"))

	var delivered sync.WaitGroup
	delivered.Add(2)

	var channel = pb.Listen("what2",
		sabuhp.TransportResponseFunc(func(message *sabuhp.Message, transport sabuhp.MessageBus) sabuhp.MessageErr {
			delivered.Done()
			if err := transport.SendToOne(whyMessage, 0); err != nil {
				logger.Log(njson.MJSON("failed to send message", func(event npkg.Encoder) {
					event.String("error", err.Error())
				}))
			}
			return nil
		}))

	require.NoError(t, channel.Err())

	defer channel.Close()

	var channel2 = pb.Listen("why2",
		sabuhp.TransportResponseFunc(func(message *sabuhp.Message, transport sabuhp.MessageBus) sabuhp.MessageErr {
			delivered.Done()
			return nil
		}))

	require.NoError(t, channel2.Err())

	defer channel2.Close()

	require.NoError(t, pb.SendToOne(whatMessage, time.Second*2))

	delivered.Wait()

	canceler()

	pb.Wait()
}
