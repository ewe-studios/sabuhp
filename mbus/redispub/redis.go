package redispub

import (
	"context"
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/influx6/sabuhp/codecs"

	"github.com/influx6/npkg/nunsafe"

	"github.com/influx6/sabuhp/utils"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"

	redis "github.com/go-redis/redis/v8"

	"github.com/influx6/npkg/nxid"
)

const (
	GroupExistErrorMsg        = "BUSYGROUP Consumer Group name already exists"
	SubscriptionExistsAlready = "Topic is already subscribed to"
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

var _ sabuhp.Transport = (*PubSub)(nil)

type PubSubConfig struct {
	Logger                    sabuhp.Logger
	Ctx                       context.Context
	Codec                     sabuhp.Codec
	Redis                     redis.Options
	MaxWaitForSubConfirmation time.Duration
	StreamMessageInterval     time.Duration
	MaxWaitForSubRetry        int
}

func (b *PubSubConfig) ensure() {
	if b.Logger == nil {
		panic("PubSubConfig.Logger is required")
	}
	if b.Ctx == nil {
		panic("PubSubConfig.ctx is required")
	}
	if b.Codec == nil {
		b.Codec = &codecs.MessagePackCodec{}
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
}

type PubSub struct {
	config        PubSubConfig
	logger        sabuhp.Logger
	client        *redis.Client
	ctx           context.Context
	canceller     context.CancelFunc
	waiter        sync.WaitGroup
	starter       sync.Once
	ender         sync.Once
	doAction      chan func()
	subscriptions map[string]redisSub
}

type redisSub struct {
	id         nxid.ID
	pub        *redis.PubSub
	topic      string
	ctx        context.Context
	cancel     context.CancelFunc
	initialMsg chan interface{}
}

func NewRedisPubSub(config PubSubConfig) (*PubSub, error) {
	config.ensure()

	var newCtx, canceler = context.WithCancel(config.Ctx)

	var client = redis.NewClient(&config.Redis)
	var status = client.Ping(newCtx)
	if statusErr := status.Err(); statusErr != nil {
		canceler()
		return nil, nerror.WrapOnly(statusErr)
	}

	return &PubSub{
		config:        config,
		logger:        config.Logger,
		client:        client,
		ctx:           newCtx,
		canceller:     canceler,
		doAction:      make(chan func()),
		subscriptions: map[string]redisSub{},
	}, nil
}

func (r *PubSub) Wait() {
	r.waiter.Wait()
}

func (r *PubSub) Start() {
	r.starter.Do(func() {
		r.waiter.Add(1)
		go r.manage()
	})
}

func (r *PubSub) Stop() {
	r.ender.Do(func() {
		r.canceller()
		r.waiter.Wait()
	})
}

func (r *PubSub) Conn() sabuhp.Conn {
	return r.client
}

func (r *PubSub) UnListen(topic string) {
	var doFunc = func() {
		var sub = r.subscriptions[topic]
		delete(r.subscriptions, topic)
		sub.cancel()
	}

	select {
	case r.doAction <- doFunc:
		return
	case <-r.ctx.Done():
		return
	}
}

func (r *PubSub) Listen(topic string, handler sabuhp.TransportResponse) sabuhp.Channel {
	var result = make(chan sabuhp.Channel, 1)

	r.waiter.Add(2)
	var doFunc = func() {
		if _, hasSub := r.subscriptions[topic]; hasSub {
			var rs = new(redisSubscription)
			rs.topic = topic
			rs.host = r
			rs.err = nerror.New(SubscriptionExistsAlready)
			result <- rs
			return
		}

		var pub = r.client.Subscribe(r.ctx, topic)
		var streamName = fmt.Sprintf("%s_stream", topic)
		var streamGroupName = fmt.Sprintf("%s_stream_group", topic)

		var rs = new(redisSubscription)
		rs.topic = topic
		rs.host = r

		r.logger.Log(njson.MJSON("Creating stream group for topic", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.String("stream_name", streamName)
			encoder.String("stream_group_name", streamGroupName)
		}))

		var streamGroup = r.client.XGroupCreateMkStream(r.ctx, streamName, streamGroupName, "$")
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

		var err error
		var firstMessage interface{}

		r.logger.Log(njson.MJSON("Checking for pubsub registered event", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.String("stream_name", streamName)
			encoder.String("stream_group_name", streamGroupName)
		}))

		for i := 0; i < r.config.MaxWaitForSubRetry; i++ {
			if firstMessage, err = pub.ReceiveTimeout(r.ctx, r.config.MaxWaitForSubConfirmation); err == nil {
				break
			}
		}

		r.logger.Log(njson.MJSON("Received pubsub first message", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.String("stream_name", streamName)
			encoder.String("stream_group_name", streamGroupName)
			encoder.String("message", fmt.Sprintf("%#v", firstMessage))
		}))

		if firstErrMsg, ok := firstMessage.(redis.Error); ok {

			// close waiter
			r.waiter.Done()
			r.waiter.Done()

			r.logger.Log(njson.MJSON("Received pubsub error", func(encoder npkg.Encoder) {
				encoder.String("topic", topic)
				encoder.String("error", firstErrMsg.Error())
				encoder.String("stream_name", streamName)
				encoder.String("stream_group_name", streamGroupName)
			}))

			rs.err = firstErrMsg
			result <- rs
			return
		}

		var initialMessage = make(chan interface{}, 1)
		initialMessage <- firstMessage

		var ctx, canceler = context.WithCancel(r.ctx)

		var rss redisSub
		rss.id = nxid.New()
		rss.pub = pub
		rss.topic = topic
		rss.ctx = ctx
		rss.cancel = canceler
		rss.initialMsg = initialMessage

		// register sub with subscriptions
		r.subscriptions[topic] = rss

		var pubChan = pub.Channel()
		go r.listenForChannel(ctx, handler, &rss, pubChan)
		go r.listenForStream(ctx, handler, &rss, streamName, streamGroupName)

		r.logger.Log(njson.MJSON("Launched pubsub channel and stream readers", func(encoder npkg.Encoder) {
			encoder.String("topic", topic)
			encoder.String("stream_name", streamName)
			encoder.String("stream_group_name", streamGroupName)
		}))

		result <- rs
	}

	select {
	case r.doAction <- doFunc:
		return <-result
	case <-r.ctx.Done():
		return &utils.CloseErrorChannel{Error: nerror.WrapOnly(r.ctx.Err())}
	}
}

var _ sabuhp.Channel = (*redisSubscription)(nil)

type redisSubscription struct {
	host  *PubSub
	topic string
	err   error
}

func (r *redisSubscription) Close() {
	r.host.UnListen(r.topic)
}

func (r *redisSubscription) Err() error {
	return r.err
}

func (r *PubSub) listenForStream(
	ctx context.Context,
	handler sabuhp.TransportResponse,
	pub *redisSub,
	streamName string,
	streamGroupName string,
) {
	defer func() {
		r.waiter.Done()

		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred", func(event npkg.Encoder) {
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

func (r *PubSub) handleXMessage(topicName string, handler sabuhp.TransportResponse, message redis.XMessage) bool {
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred processing message", func(event npkg.Encoder) {
				event.String("topic", topicName)
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
		event.String("message_id", message.ID)
		event.String("message_bytes", fmt.Sprintf("%#v", messageBytes))
		event.String("message_data_type", fmt.Sprintf("%T", messageBytes))
	}))

	var decodedMessage, decodedErr = r.config.Codec.Decode(messageBytes)
	if decodedErr != nil {
		r.logger.Log(njson.MJSON("failed to decode message", func(event npkg.Encoder) {
			event.String("topic", topicName)
			event.String("message_id", message.ID)
			event.String("error", fmt.Sprintf("%#v", decodedErr))
			event.ObjectFor("message_values", func(valueEncoder npkg.ObjectEncoder) {
				for key, val := range message.Values {
					valueEncoder.String(key, fmt.Sprintf("%+v", val))
				}
			})
		}))
	}

	if handleErr := handler.Handle(decodedMessage, r); handleErr != nil {
		r.logger.Log(njson.MJSON("failed to handle message", func(event npkg.Encoder) {
			event.String("topic", topicName)
			event.String("message_id", message.ID)
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

func (r *PubSub) listenForChannel(
	ctx context.Context,
	handler sabuhp.TransportResponse,
	pub *redisSub,
	messages <-chan *redis.Message,
) {
	defer func() {
		r.waiter.Done()

		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred", func(event npkg.Encoder) {
				event.String("panic_data", fmt.Sprintf("%#v", panicInfo))
			}))
		}

		if closeErr := pub.pub.Close(); closeErr != nil {
			r.logger.Log(njson.MJSON("error out during subscription closing", func(event npkg.Encoder) {
				event.String("error", nerror.WrapOnly(closeErr).Error())
			}))
		}

		r.canceller()
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
				event.String("topic", pub.topic)
				event.String("message", fmt.Sprintf("%#v", msg))
			}))
			r.handleMessage(handler, msg)
		}
	}
}

func (r *PubSub) handleMessage(handler sabuhp.TransportResponse, message *redis.Message) {
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			r.logger.Log(njson.MJSON("panic occurred handling pubsub message", func(event npkg.Encoder) {
				event.String("message", message.String())
				event.String("panic_data", fmt.Sprintf("%#v", panicInfo))
				event.String("panic_data", fmt.Sprintf("%+s", panicInfo))
			}))
		}
	}()

	var decodedMessage, decodedErr = r.config.Codec.Decode(nunsafe.String2Bytes(message.Payload))
	if decodedErr != nil {
		r.logger.Log(njson.MJSON("failed to decode message", func(event npkg.Encoder) {
			event.String("topic", message.Channel)
			event.String("pattern", message.Pattern)
			event.String("payload", message.Payload)
			event.String("error", decodedErr.Error())
		}))
	}

	if handleErr := handler.Handle(decodedMessage, r); handleErr != nil {
		r.logger.Log(njson.MJSON("failed to handle message", func(event npkg.Encoder) {
			event.String("topic", message.Channel)
			event.String("pattern", message.Pattern)
			event.String("payload", message.Payload)
			event.String("error", handleErr.Error())
		}))
	}
}

func (r *PubSub) SendToOne(data *sabuhp.Message, timeout time.Duration) error {
	data.Delivery = sabuhp.SendToOne
	var encodedData, encodeErr = r.config.Codec.Encode(data)
	if encodeErr != nil {
		r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("topic", data.Topic)
			event.String("from_addr", data.FromAddr)
			event.String("payload", fmt.Sprintf("%#v", data.Payload))
			event.String("error", encodeErr.Error())
		}))
		return nerror.WrapOnly(encodeErr)
	}

	var targetCtx = r.ctx
	var canceler context.CancelFunc = func() {}
	if timeout > 0 {
		targetCtx, canceler = context.WithTimeout(r.ctx, timeout)
	}

	defer canceler()

	var streamName = fmt.Sprintf("%s_stream", data.Topic)
	var xmessage = redis.XAddArgs{
		Stream:       streamName,
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "*",
		Values: map[string]interface{}{
			"data": nunsafe.Bytes2String(encodedData),
		},
	}

	var responseCmd = r.client.XAdd(targetCtx, &xmessage)
	if resErr := responseCmd.Err(); resErr != nil {
		r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("topic", data.Topic)
			event.String("error", resErr.Error())
			event.String("from_addr", data.FromAddr)
			event.String("payload", fmt.Sprintf("%#v", data.Payload))
		}))
		return nerror.WrapOnly(encodeErr)
	}

	r.logger.Log(njson.MJSON("sent new consumer group message", func(event npkg.Encoder) {
		event.String("encoded_message", nunsafe.Bytes2String(encodedData))
		event.String("redis_command_value", responseCmd.Val())
		event.String("redis_command_name", responseCmd.Name())
		event.String("redis_command_full_name", responseCmd.FullName())
	}))
	return nil
}

func (r *PubSub) SendToAll(data *sabuhp.Message, timeout time.Duration) error {
	data.Delivery = sabuhp.SendToAll
	var encodedData, encodeErr = r.config.Codec.Encode(data)
	if encodeErr != nil {
		r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("topic", data.Topic)
			event.String("from_addr", data.FromAddr)
			event.String("payload", fmt.Sprintf("%#v", data.Payload))
			event.String("error", encodeErr.Error())
		}))
		return nerror.WrapOnly(encodeErr)
	}

	var targetCtx = r.ctx
	var canceler context.CancelFunc = func() {}
	if timeout > 0 {
		targetCtx, canceler = context.WithTimeout(r.ctx, timeout)
	}

	defer canceler()

	var responseCmd = r.client.Publish(targetCtx, data.Topic, encodedData)
	if resErr := responseCmd.Err(); resErr != nil {
		r.logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.String("topic", data.Topic)
			event.String("error", resErr.Error())
			event.String("from_addr", data.FromAddr)
			event.String("payload", fmt.Sprintf("%#v", data.Payload))
		}))
		return nerror.WrapOnly(encodeErr)
	}

	r.logger.Log(njson.MJSON("sent new pubsub message", func(event npkg.Encoder) {
		event.String("encoded_message", nunsafe.Bytes2String(encodedData))
		event.Int64("redis_pubsub_id", responseCmd.Val())
		event.String("redis_command_name", responseCmd.Name())
		event.String("redis_command_full_name", responseCmd.FullName())
	}))
	return nil
}

func (r *PubSub) manage() {
	defer func() {
		var subs = r.subscriptions
		r.subscriptions = map[string]redisSub{}

		// close all subscriptions
		for _, subs := range subs {
			subs.cancel()
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
