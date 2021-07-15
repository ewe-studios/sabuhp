package redispub

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/ewe-studios/sabuhp/sabu"
	"strings"
	"sync"
	"time"

	"github.com/influx6/npkg/nthen"

	"github.com/ewe-studios/sabuhp/codecs"

	"github.com/influx6/npkg/nunsafe"

	"github.com/ewe-studios/sabuhp/utils"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nerror"

	redis "github.com/go-redis/redis/v8"

	"github.com/influx6/npkg/nxid"
)

const (
	GroupExistErrorMsg        = "BUSYGROUP Consumer Group name already exists"
	SubscriptionExistsAlready = "String is already subscribed to"
)

var (
	DefaultMessageBatchCount = 200
	DefaultMessageBatchWait  = 700 * time.Millisecond
)

// Channel implements the sabuhp.Channel interface.
type Channel struct {
	id           nxid.ID
	ctx          context.Context
	closeChannel chan nxid.ID
}

func (r *Channel) Close() {
	select {
	case r.closeChannel <- r.id:
		return
	case <-r.ctx.Done():
		return
	}
}

var _ sabu.MessageBus = (*RedisMessageBus)(nil)

var _ sabu.Channel = (*redisSubscription)(nil)

type redisSubscription struct {
	host       *RedisMessageBus
	id         nxid.ID
	topic      string
	group      string
	pub        *redis.PubSub
	ctx        context.Context
	cancel     context.CancelFunc
	initialMsg chan interface{}
	stream     *redis.StatusCmd
	err        error
}

func (r *redisSubscription) Topic() string {
	return r.topic
}

func (r *redisSubscription) Group() string {
	return r.group
}

func (r *redisSubscription) Close() {
	r.cancel()
}

func (r *redisSubscription) Err() error {
	return r.err
}

type MessageChannel int

const (
	RedisPubSub MessageChannel = iota
	RedisStreams
)

type Config struct {
	Logger                    sabu.Logger
	Ctx                       context.Context
	Codec                     sabu.Codec
	Redis                     redis.Options
	MaxWaitForSubConfirmation time.Duration
	StreamMessageInterval     time.Duration
	MaxWaitForSubRetry        int
	MaxMessageBatch           int
	MaxMessageBatchWait       time.Duration
}

func (b *Config) ensure() {
	if b.Logger == nil {
		panic("Config.Logger is required")
	}
	if b.Ctx == nil {
		panic("Config.ctx is required")
	}
	if b.Codec == nil {
		b.Codec = &codecs.MessageMsgPackCodec{}
	}
	if b.StreamMessageInterval <= 0 {
		b.StreamMessageInterval = time.Second * 1
	}
	if b.MaxWaitForSubConfirmation <= 0 {
		b.MaxWaitForSubConfirmation = time.Second * 3
	}
	if b.MaxWaitForSubRetry <= 0 {
		b.MaxWaitForSubRetry = 5
	}
	if b.MaxMessageBatchWait <= 0 {
		b.MaxMessageBatchWait = DefaultMessageBatchWait
	}
	if b.MaxMessageBatch <= 0 {
		b.MaxMessageBatch = DefaultMessageBatchCount
	}
}

type RedisMessageBus struct {
	config        Config
	logger        sabu.Logger
	client        *redis.Client
	ctx           context.Context
	canceller     context.CancelFunc
	waiter        sync.WaitGroup
	starter       sync.Once
	stopper       sync.Once
	doAction      chan func()
	channel       MessageChannel
	subscriptions []sabu.Channel
}

func Stream(config Config) (*RedisMessageBus, error) {
	var client = redis.NewClient(&config.Redis)
	var status = client.Ping(config.Ctx)
	if statusErr := status.Err(); statusErr != nil {
		return nil, nerror.WrapOnly(statusErr)
	}
	return NewRedisMessageBus(config, client, RedisStreams), nil
}

func PubSub(config Config) (*RedisMessageBus, error) {
	var client = redis.NewClient(&config.Redis)
	var status = client.Ping(config.Ctx)
	if statusErr := status.Err(); statusErr != nil {
		return nil, nerror.WrapOnly(statusErr)
	}
	return NewRedisMessageBus(config, client, RedisPubSub), nil
}

func NewRedisMessageBus(config Config, client *redis.Client, channel MessageChannel) *RedisMessageBus {
	config.ensure()
	var newCtx, canceler = context.WithCancel(config.Ctx)
	var pubsub = &RedisMessageBus{
		config:    config,
		logger:    config.Logger,
		client:    client,
		ctx:       newCtx,
		canceller: canceler,
		channel:   channel,
		doAction:  make(chan func()),
	}
	return pubsub
}

func (r *RedisMessageBus) Wait() {
	r.waiter.Wait()
}

func (r *RedisMessageBus) Start() {
	r.starter.Do(func() {
		r.waiter.Add(1)
		go r.manage()
	})
}

func (r *RedisMessageBus) Stop() {
	r.stopper.Do(func() {
		r.canceller()
		r.waiter.Wait()
	})
}

func (r *RedisMessageBus) Listen(topic string, grp string, handler sabu.TransportResponse) sabu.Channel {
	if r.channel == RedisStreams {
		return r.ListenStream(topic, grp, handler)
	}
	return r.ListenPubSub(topic, grp, handler)
}

func (r *RedisMessageBus) ListenStream(streamTopic string, grp string, handler sabu.TransportResponse) sabu.Channel {
	var result = make(chan sabu.Channel, 1)

	r.waiter.Add(1)
	var doFunc = func() {
		var rs = new(redisSubscription)
		rs.id = nxid.New()
		rs.group = grp
		rs.topic = streamTopic
		rs.host = r

		r.logger.Log(njson.MJSON("Creating stream group for topic", func(encoder npkg.Encoder) {
			encoder.String("topic", streamTopic)
			encoder.Int("_level", int(npkg.INFO))
			encoder.String("stream_name", streamTopic)
			encoder.String("stream_group_name", grp)
		}))

		var streamGroup = r.client.XGroupCreateMkStream(r.ctx, streamTopic, grp, "$")
		rs.stream = streamGroup

		if streamResponseErr := streamGroup.Err(); streamResponseErr != nil {
			if !strings.Contains(streamResponseErr.Error(), GroupExistErrorMsg) {
				// close waiter
				r.waiter.Done()
				r.waiter.Done()

				rs.err = streamResponseErr
				result <- rs
				return
			}
		}

		var ctx, canceler = context.WithCancel(r.ctx)

		rs.ctx = ctx
		rs.cancel = canceler

		// register sub with subscriptions
		r.subscriptions = append(r.subscriptions, rs)

		go r.listenForStream(ctx, handler, rs, streamTopic, grp)

		r.logger.Log(njson.MJSON("Launched pubsub channel and stream readers", func(encoder npkg.Encoder) {
			encoder.String("topic", streamTopic)
			encoder.String("stream_name", streamTopic)
			encoder.Int("_level", int(npkg.INFO))
			encoder.String("stream_group_name", grp)
		}))

		result <- rs
	}

	select {
	case r.doAction <- doFunc:
		return <-result
	case <-r.ctx.Done():
		return &utils.CloseErrorChannel{T: streamTopic, G: grp, Error: nerror.WrapOnly(r.ctx.Err())}
	}
}

func (r *RedisMessageBus) ListenPubSub(topic string, grp string, handler sabu.TransportResponse) sabu.Channel {
	var result = make(chan sabu.Channel, 1)

	r.waiter.Add(1)
	var doFunc = func() {
		var pub = r.client.PSubscribe(r.ctx, topic)

		var rs = new(redisSubscription)
		rs.id = nxid.New()
		rs.topic = topic
		rs.host = r

		r.logger.Log(njson.MJSON("Creating stream group for topic", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.Int("_level", int(npkg.INFO))
		}))

		var err error
		var firstMessage interface{}

		r.logger.Log(njson.MJSON("Checking for pubsub registered event", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.Int("_level", int(npkg.INFO))
		}))

		for i := 0; i < r.config.MaxWaitForSubRetry; i++ {
			if firstMessage, err = pub.ReceiveTimeout(r.ctx, r.config.MaxWaitForSubConfirmation); err == nil {
				break
			}
		}

		r.logger.Log(njson.MJSON("Received pubsub first message", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.Int("_level", int(npkg.INFO))
			encoder.String("message", fmt.Sprintf("%#v", firstMessage))
		}))

		if firstErrMsg, ok := firstMessage.(redis.Error); ok {

			// close waiter
			r.waiter.Done()
			r.waiter.Done()

			r.logger.Log(njson.MJSON("Received pubsub error", func(encoder npkg.Encoder) {
				encoder.String("topic", topic)
				encoder.Int("_level", int(npkg.INFO))
				encoder.String("error", firstErrMsg.Error())
			}))

			rs.err = firstErrMsg
			result <- rs
			return
		}

		var initialMessage = make(chan interface{}, 1)
		initialMessage <- firstMessage

		var ctx, canceler = context.WithCancel(r.ctx)

		rs.pub = pub
		rs.topic = topic
		rs.ctx = ctx
		rs.group = grp
		rs.cancel = canceler
		rs.initialMsg = initialMessage

		r.subscriptions = append(r.subscriptions, rs)

		var pubChan = pub.Channel()
		go r.listenForChannel(ctx, handler, rs, pubChan)

		r.logger.Log(njson.MJSON("Launched pubsub channel and stream readers", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.Int("_level", int(npkg.INFO))
		}))

		result <- rs
	}

	select {
	case r.doAction <- doFunc:
		return <-result
	case <-r.ctx.Done():
		return &utils.CloseErrorChannel{T: topic, G: grp, Error: nerror.WrapOnly(r.ctx.Err())}
	}
}

func (r *RedisMessageBus) listenForStream(
	ctx context.Context,
	handler sabu.TransportResponse,
	pub *redisSubscription,
	streamName string,
	streamGroupName string,
) {
	defer func() {
		r.waiter.Done()

		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.PANIC))
				event.String("panic_data", fmt.Sprintf("%#v", panicInfo))
				event.String("stream_name", streamName)
				event.String("stream_group_name", streamGroupName)
			}))
		}
	}()

	var msgTicker = time.NewTicker(r.config.StreamMessageInterval)
	defer msgTicker.Stop()

doLoop:
	for {
		select {
		case <-ctx.Done():
			break doLoop
		case <-msgTicker.C:
		}

		var stream = r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    streamGroupName,
			Consumer: fmt.Sprintf("%s_consumer_%s", pub.topic, pub.id.String()),
			Streams:  []string{streamName, ">"},
			Count:    1,
			Block:    time.Second * 3,
			NoAck:    false,
		})

		if streamErr := stream.Err(); streamErr != nil && streamErr != redis.Nil {
			r.logger.Log(njson.MJSON("stream err occurred", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("error", streamErr.Error())
				event.String("stream_name", streamName)
				event.String("stream_group_name", streamGroupName)
			}))
			continue doLoop
		}

		// re-loop as there was not a message pending.
		if streamErr := stream.Err(); streamErr != nil && streamErr == redis.Nil {
			continue doLoop
		}

		r.logger.Log(njson.MJSON("stream responded", func(event npkg.Encoder) {
			event.String("value", fmt.Sprintf("%#v", stream.Val()))
			event.String("id", stream.FullName())
			event.String("stream_name", streamName)
			event.String("stream_group_name", streamGroupName)
			event.Int("_level", int(npkg.INFO))
		}))

		for _, xstream := range stream.Val() {
			var ackIdList = make([]string, 0, len(xstream.Messages))
			for _, message := range xstream.Messages {
				if shouldAck := r.handleXMessage(streamName, handler, message); shouldAck {
					ackIdList = append(ackIdList, message.ID)
				}
			}

			if len(ackIdList) > 0 {
				func(ackIds []string) {
					var ackCmd = r.client.XAck(ctx, streamName, streamGroupName, ackIdList...)
					if ackErr := ackCmd.Err(); nil != ackErr {
						r.logger.Log(njson.MJSON("failed to ack messages", func(event npkg.Encoder) {
							event.String("value", fmt.Sprintf("%#v", stream.Val()))
							event.Int("_level", int(npkg.ERROR))
							event.ListFor("ack_ids", func(idList npkg.ListEncoder) {
								for _, id := range ackIds {
									idList.AddString(id)
								}
							})
							event.String("error", ackErr.Error())
							event.String("stream_name", streamName)
							event.String("stream_group_name", streamGroupName)
							event.String("response_string", ackCmd.String())
							event.Int64("response_code", ackCmd.Val())
							event.String("response_name", ackCmd.Name())
							event.String("response_full_name", ackCmd.FullName())
						}))
						return
					}
					r.logger.Log(njson.MJSON("sent acknowledgment for messages", func(event npkg.Encoder) {
						event.String("value", fmt.Sprintf("%#v", stream.Val()))
						event.String("stream_name", streamName)
						event.String("response_string", ackCmd.String())
						event.Int("_level", int(npkg.INFO))
						event.Int64("response_code", ackCmd.Val())
						event.String("response_name", ackCmd.Name())
						event.String("response_full_name", ackCmd.FullName())
						event.String("stream_group_name", streamGroupName)
						event.ListFor("ack_ids", func(idList npkg.ListEncoder) {
							for _, id := range ackIds {
								idList.AddString(id)
							}
						})
					}))
				}(ackIdList)
			}
		}
	}
}

func (r *RedisMessageBus) handleXMessage(topicName string, handler sabu.TransportResponse, message redis.XMessage) bool {
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred processing message", func(event npkg.Encoder) {
				event.String("topic", topicName)
				event.Int("_level", int(npkg.PANIC))
				event.String("message_id", message.ID)
				event.String("panic_data", fmt.Sprintf("%#v", panicInfo))
				event.ObjectFor("message_values", func(valueEncoder npkg.ObjectEncoder) {
					for key, val := range message.Values {
						valueEncoder.String(key, fmt.Sprintf("%+v", val))
					}
				})
			}))
		}
	}()

	var messageData, hasMessageData = message.Values["data"]
	if !hasMessageData {
		r.logger.Log(njson.MJSON("failed to find 'data' key in message key-value map", func(event npkg.Encoder) {
			event.String("topic", topicName)
			event.String("message_id", message.ID)
			event.Int("_level", int(npkg.WARN))
			event.ObjectFor("message_values", func(valueEncoder npkg.ObjectEncoder) {
				for key, val := range message.Values {
					valueEncoder.String(key, fmt.Sprintf("%+v", val))
				}
			})
		}))
		return false
	}

	r.logger.Log(njson.MJSON("received data from xmessage", func(event npkg.Encoder) {
		event.String("topic", topicName)
		event.Int("_level", int(npkg.INFO))
		event.String("message_id", message.ID)
		event.String("message_data", fmt.Sprintf("%s", messageData))
		event.String("message_data_type", fmt.Sprintf("%T", messageData))
	}))

	var messageBytes []byte
	switch msg := messageData.(type) {
	case string:
		messageBytes = nunsafe.String2Bytes(msg)
	case []byte:
		messageBytes = msg
	}

	r.logger.Log(njson.MJSON("decoded message type into bytes", func(event npkg.Encoder) {
		event.String("topic", topicName)
		event.Int("_level", int(npkg.INFO))
		event.String("message_id", message.ID)
		event.String("message_bytes", fmt.Sprintf("%+q", messageBytes))
		event.String("message_data_type", fmt.Sprintf("%T", messageBytes))
	}))

	var decodedMessage, decodedErr = r.config.Codec.Decode(messageBytes)
	if decodedErr != nil {
		r.logger.Log(njson.MJSON("failed to decode message", func(event npkg.Encoder) {
			event.String("topic", topicName)
			event.Int("_level", int(npkg.ERROR))
			event.String("message_id", message.ID)
			event.String("error", fmt.Sprintf("%#v", decodedErr))
			event.ObjectFor("message_values", func(valueEncoder npkg.ObjectEncoder) {
				for key, val := range message.Values {
					valueEncoder.String(key, fmt.Sprintf("%+v", val))
				}
			})
		}))
	}

	if handleErr := handler.Handle(r.ctx, decodedMessage, sabu.Transport{Bus: r}); handleErr != nil {
		r.logger.Log(njson.MJSON("failed to handle message", func(event npkg.Encoder) {
			event.String("topic", topicName)
			event.String("message_id", message.ID)
			event.Int("_level", int(npkg.ERROR))
			event.String("error", handleErr.Error())
			event.ObjectFor("message_values", func(valueEncoder npkg.ObjectEncoder) {
				for key, val := range message.Values {
					valueEncoder.String(key, fmt.Sprintf("%+v", val))
				}
			})
		}))
		return handleErr.ShouldAck()
	}
	return true
}

func (r *RedisMessageBus) listenForChannel(
	ctx context.Context,
	handler sabu.TransportResponse,
	pub *redisSubscription,
	messages <-chan *redis.Message,
) {
	defer func() {
		r.waiter.Done()

		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.PANIC))
				event.String("panic_data", fmt.Sprintf("%#v", panicInfo))
			}))
		}

		if closeErr := pub.pub.Close(); closeErr != nil {
			r.logger.Log(njson.MJSON("error out during subscription closing", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("error", nerror.WrapOnly(closeErr).Error())
			}))
		}

		r.canceller()

		r.logger.Log(njson.MJSON("closed listener for channel", func(event npkg.Encoder) {
			event.Int("_level", int(npkg.INFO))
		}))
	}()

doLoop:
	for {
		select {
		case <-ctx.Done():
			break doLoop
		case msg := <-pub.initialMsg:
			if redisMsg, ok := msg.(*redis.Message); ok {
				r.handleMessage(handler, redisMsg)
			}
		case msg := <-messages:
			r.logger.Log(njson.MJSON("Received new msg", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.INFO))
				event.String("topic", pub.topic)
				event.String("message", fmt.Sprintf("%#v", msg))
			}))
			r.handleMessage(handler, msg)
		}
	}
}

func (r *RedisMessageBus) handleMessage(handler sabu.TransportResponse, message *redis.Message) {
	defer func() {
		var panicErr = nerror.New("panic occurred in redis.handleMessage")
		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred handling pubsub message", func(event npkg.Encoder) {
				event.Error("error", panicErr)
				event.Int("_level", int(npkg.ERROR))
				event.String("message", message.String())
				event.String("panic_data", fmt.Sprintf("%#v", panicInfo))
				event.String("panic_data", fmt.Sprintf("%s", panicInfo))
			}))
		}
	}()

	r.logger.Log(njson.MJSON("received message to decode", func(event npkg.Encoder) {
		event.String("topic", message.Channel)
		event.Int("_level", int(npkg.INFO))
		event.String("pattern", message.Pattern)
		event.String("payload", message.Payload)
	}))

	var payloadBytes = nunsafe.String2Bytes(message.Payload)
	var decodedMessage, decodedErr = r.config.Codec.Decode(payloadBytes)
	if decodedErr != nil {
		r.logger.Log(njson.MJSON("failed to decode message", func(event npkg.Encoder) {
			event.String("topic", message.Channel)
			event.String("pattern", message.Pattern)
			event.Int("_level", int(npkg.ERROR))
			event.String("payload", message.Payload)
			event.String("error", decodedErr.Error())
		}))
	}

	decodedMessage.Future = nthen.NewFuture()

	if handleErr := handler.Handle(r.ctx, decodedMessage, sabu.Transport{Bus: r}); handleErr != nil {
		decodedMessage.Future.WithError(handleErr)
		r.logger.Log(njson.MJSON("failed to handle message", func(event npkg.Encoder) {
			event.String("topic", message.Channel)
			event.String("pattern", message.Pattern)
			event.Int("_level", int(npkg.ERROR))
			event.String("payload", message.Payload)
			event.String("error", handleErr.Error())
		}))
		return
	}

	decodedMessage.Future.WithValue(nil)
}

func (r *RedisMessageBus) Send(data ...sabu.Message) {
	r.sendChannelBatch(data, r.channel)
}

func (r *RedisMessageBus) SendForReply(tm time.Duration, fromTopic sabu.Topic, replyGroup string, data ...sabu.Message) *nthen.Future {
	var ft = nthen.Fn(func(ft *nthen.Future) {
		var replyChannel = r.Listen(fromTopic.ReplyTopic().String(), replyGroup, sabu.TransportResponseFunc(func(ctx context.Context, message sabu.Message, transport sabu.Transport) sabu.MessageErr {
			// delete reply stream
			var intCmd = r.client.Del(ctx, fromTopic.ReplyTopic().String())
			if intCmd.Err() != nil {
				r.logger.Log(njson.MJSON("received message to decode", func(event npkg.Encoder) {
					event.String("topic", message.Topic.String())
					event.Int("_level", int(npkg.INFO))
					event.Error("error", intCmd.Err())
				}))
			}

			ft.WithValue(message)
			return nil
		}))

		// send message after listening for reply
		r.sendChannelBatch(data, r.channel)

		<-time.After(tm)
		replyChannel.Close()

		// delete reply stream
		var intCmd = r.client.Del(r.ctx, fromTopic.ReplyTopic().String())
		if intCmd.Err() != nil {
			r.logger.Log(njson.MJSON("received message to decode", func(event npkg.Encoder) {
				event.String("topic", fromTopic.String())
				event.Int("_level", int(npkg.INFO))
				event.Error("error", intCmd.Err())
			}))
		}

		ft.WithError(nerror.New("timed out waiting for reply"))
	})
	return ft
}

func (r *RedisMessageBus) sendChannelBatch(batch []sabu.Message, channel MessageChannel) {
	var pipelining = r.client.Pipeline()

	for _, msg := range batch {
		var ft = msg.Future

		var encodedData, encodeErr = r.config.Codec.Encode(msg)
		if encodeErr != nil {
			if ft != nil {
				ft.WithError(encodeErr)
			}

			r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
				event.String("topic", msg.Topic.String())
				event.Int("_level", int(npkg.ERROR))
				event.String("from_addr", msg.FromAddr)
				event.String("payload", fmt.Sprintf("%#v", msg.Bytes))
				event.String("error", encodeErr.Error())
			}))
			continue
		}

		// publish to streams
		if channel == RedisStreams {
			if addErr := r.sendStream(msg.Topic.String(), encodedData, pipelining); addErr != nil {
				if ft != nil {
					ft.WithError(addErr)
				}

				r.logger.Log(njson.MJSON("failed to add to pipelined", func(event npkg.Encoder) {
					event.String("topic", msg.Topic.String())
					event.String("from_addr", msg.FromAddr)
					event.Int("_level", int(npkg.ERROR))
					event.String("payload", fmt.Sprintf("%#v", msg.Bytes))
					event.String("error", addErr.Error())
				}))
			}
			continue
		}

		// publish to pubsub
		if addErr := r.sendPubSub(msg.Topic.String(), encodedData, pipelining); addErr != nil {
			if ft != nil {
				ft.WithError(addErr)
			}

			r.logger.Log(njson.MJSON("failed to add to pipelined", func(event npkg.Encoder) {
				event.String("topic", msg.Topic.String())
				event.String("from_addr", msg.FromAddr)
				event.Int("_level", int(npkg.ERROR))
				event.String("payload", fmt.Sprintf("%#v", msg.Bytes))
				event.String("error", addErr.Error())
			}))
		}
	}

	var execResults, execErr = pipelining.Exec(r.ctx)
	if execErr != nil {
		for _, msg := range batch {
			if msg.Future == nil {
				continue
			}
			msg.Future.WithError(execErr)
		}

		r.logger.Log(njson.MJSON("failed to execute pipeline", func(event npkg.Encoder) {
			event.String("error", execErr.Error())
			event.Int("_level", int(npkg.ERROR))
		}))

		return
	}

	for index, execResult := range execResults {
		var msg = batch[index]
		var ft = msg.Future

		if execErr := execResult.Err(); execErr != nil {
			if ft != nil {
				ft.WithError(execErr)
			}

			r.logger.Log(njson.MJSON("failed to publish message", func(event npkg.Encoder) {
				event.String("from_addr", msg.FromAddr)
				event.String("payload", fmt.Sprintf("%#v", msg.Bytes))
				event.Int("_level", int(npkg.ERROR))
				event.String("error", execErr.Error())
			}))

			continue
		}

		r.logger.Log(njson.MJSON("published message to pubsub", func(event npkg.Encoder) {
			event.String("from_addr", msg.FromAddr)
			event.Int("_level", int(npkg.INFO))
			event.String("payload", fmt.Sprintf("%#v", msg.Bytes))
		}))
	}
}

func (r *RedisMessageBus) sendStream(streamName string, encodedData []byte, pipelined redis.Pipeliner) error {
	var xmessage = redis.XAddArgs{
		Stream:       streamName,
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "*",
		Values: map[string]interface{}{
			"data": nunsafe.Bytes2String(encodedData),
		},
	}

	var responseCmd = pipelined.XAdd(r.ctx, &xmessage)
	if resErr := responseCmd.Err(); resErr != nil {
		r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("topic", streamName)
			event.String("error", resErr.Error())
			event.Int("_level", int(npkg.ERROR))
			event.String("payload", fmt.Sprintf("%#v", encodedData))
		}))
		return nerror.WrapOnly(resErr)
	}

	r.logger.Log(njson.MJSON("sent new consumer group message", func(event npkg.Encoder) {
		event.String("encoded_message", nunsafe.Bytes2String(encodedData))
		event.String("redis_command_value", responseCmd.Val())
		event.Int("_level", int(npkg.INFO))
		event.String("redis_command_name", responseCmd.Name())
		event.String("redis_command_full_name", responseCmd.FullName())
	}))
	return nil
}

func (r *RedisMessageBus) sendPubSub(topic string, encodedData []byte, pipelined redis.Pipeliner) error {
	var responseCmd = pipelined.Publish(r.ctx, topic, encodedData)
	if resErr := responseCmd.Err(); resErr != nil {
		r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("topic", topic)
			event.String("error", resErr.Error())
			event.Int("_level", int(npkg.ERROR))
			event.String("payload", fmt.Sprintf("%#v", encodedData))
		}))
		return nerror.WrapOnly(resErr)
	}

	r.logger.Log(njson.MJSON("sent new pubsub message", func(event npkg.Encoder) {
		event.String("encoded_message", nunsafe.Bytes2String(encodedData))
		event.Int64("redis_pubsub_id", responseCmd.Val())
		event.Int("_level", int(npkg.INFO))
		event.String("redis_command_name", responseCmd.Name())
		event.String("redis_command_full_name", responseCmd.FullName())
	}))
	return nil
}

func (r *RedisMessageBus) manage() {
	defer func() {
		var subs = r.subscriptions
		r.subscriptions = nil

		// close all subscriptions
		for _, subs := range subs {
			subs.Close()
		}

		r.waiter.Done()
	}()

runLoop:
	for {
		select {
		case <-r.ctx.Done():
			break runLoop
		case theAction := <-r.doAction:
			theAction()
		}
	}
}

func init() {
	gob.Register(redis.XMessage{})
	gob.Register(redis.Message{})
}
