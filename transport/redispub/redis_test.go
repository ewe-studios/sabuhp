package redispub

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp/supabaiza"
	"github.com/influx6/sabuhp/testingutils"
)

var _ sabuhp.Codec = (*jsonCodec)(nil)

type jsonCodec struct{}

func (j *jsonCodec) Encode(message *sabuhp.Message) ([]byte, error) {
	encoded, encodedErr := json.Marshal(message)
	if encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}

	return encoded, nil
}

func (j *jsonCodec) Decode(b []byte) (*sabuhp.Message, error) {
	var message sabuhp.Message
	if jsonErr := json.Unmarshal(b, &message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}

func TestRedis_Start_Stop_WithCancel(t *testing.T) {
	var ctx, canceler = context.WithCancel(context.Background())

	var logger = &testingutils.LoggerPub{}
	var codec = &jsonCodec{}
	var config PubSubConfig
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger

	var pb, err = NewRedisPubSub(config)
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
	var codec = &jsonCodec{}
	var config PubSubConfig
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger

	var pb, err = NewRedisPubSub(config)
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
	var codec = &jsonCodec{}
	var config PubSubConfig
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger

	var pb, err = NewRedisPubSub(config)
	require.NoError(t, err)
	require.NotNil(t, pb)

	pb.Start()

	var whyMessage = sabuhp.NewMessage("why", "me", sabuhp.BinaryPayload("yes"), map[string]string{})
	var whatMessage = sabuhp.NewMessage("what", "me", sabuhp.BinaryPayload("yes"), map[string]string{})

	var delivered sync.WaitGroup
	delivered.Add(2)

	var channel = pb.Listen("what", func(message *sabuhp.Message, transport supabaiza.Transport) supabaiza.MessageErr {
		delivered.Done()

		if err := transport.SendToAll(whyMessage, 0); err != nil {
			logger.Log(njson.MJSON("failed to send message", func(event npkg.Encoder) {
				event.String("error", err.Error())
			}))
		}
		return nil
	})

	require.NoError(t, channel.Err())

	defer channel.Close()

	var channel2 = pb.Listen("why", func(message *sabuhp.Message, transport supabaiza.Transport) supabaiza.MessageErr {
		delivered.Done()
		return nil
	})

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
	var codec = &jsonCodec{}
	var config PubSubConfig
	config.Ctx = ctx
	config.Codec = codec
	config.Logger = logger

	var pb, err = NewRedisPubSub(config)
	require.NoError(t, err)
	require.NotNil(t, pb)

	pb.Start()

	var whyMessage = sabuhp.NewMessage("why", "me", sabuhp.BinaryPayload("yes"), map[string]string{})
	var whatMessage = sabuhp.NewMessage("what", "me", sabuhp.BinaryPayload("yes"), map[string]string{})

	var delivered sync.WaitGroup
	delivered.Add(2)

	var channel = pb.Listen("what", func(message *sabuhp.Message, transport supabaiza.Transport) supabaiza.MessageErr {
		delivered.Done()
		if err := transport.SendToOne(whyMessage, 0); err != nil {
			logger.Log(njson.MJSON("failed to send message", func(event npkg.Encoder) {
				event.String("error", err.Error())
			}))
		}
		return nil
	})

	require.NoError(t, channel.Err())

	defer channel.Close()

	var channel2 = pb.Listen("why", func(message *sabuhp.Message, transport supabaiza.Transport) supabaiza.MessageErr {
		delivered.Done()
		return nil
	})

	require.NoError(t, channel2.Err())

	defer channel2.Close()

	require.NoError(t, pb.SendToOne(whatMessage, time.Second*2))

	delivered.Wait()

	canceler()

	pb.Wait()
}
