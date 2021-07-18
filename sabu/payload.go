package sabu

import (
	"fmt"
	"github.com/influx6/npkg/nerror"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/influx6/npkg/nstr"

	"github.com/influx6/npkg/nthen"

	"github.com/influx6/npkg/nunsafe"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg"
)

// WriterToSplitter takes a desired writerTo object and transforms
// the stream into a series of messages message parts which will
// be assembled on the following receiving end.
type WriterToSplitter interface {
	Split(w io.WriterTo) (chan<- Message, error)
}

// BytesSplitter takes a large block of bytes returning a chan of messages which are
// part messages which represent the whole of said bytes. This allows larger messages
// be sent across message channels with ease.
type BytesSplitter interface {
	SplitBytes(data []byte) (chan<- Message, error)
}

type Topic struct {
	T string
	R string
}

// T creates a topic with a 20 length random string suffix.
func T(t string) Topic {
	return NewTopic(t, nstr.RandomAlphabets(20))
}

// TR allows creating a topic with a environment prefix and a 20 length random string suffix.
func TR(env string, t string) Topic {
	return NewTopic(fmt.Sprintf("%s.%s", env, t), nstr.RandomAlphabets(20))
}

// TRS allows creating a topic with a environment and service prefix and a 20 length random string suffix.
func TRS(env string, service string, t string) Topic {
	return NewTopic(fmt.Sprintf("%s.%s.%s", env, service, t), nstr.RandomAlphabets(20))
}

type TopicPartial func(topicName string) Topic

// CreateTopicPartial returns a TopicPartial which will always generate
// a topic match the target TRS topic naming format: env.service.topic_name.
func CreateTopicPartial(env string, service string) TopicPartial {
	return func(topicName string) Topic {
		return TRS(env, service, topicName)
	}
}

func NewTopic(t string, r string) Topic {
	return Topic{
		T: t,
		R: r,
	}
}

func (t *Topic) FromString(tm string) error {
	if strings.Contains(tm, "reply") {
		var tml = strings.Replace(tm, "-reply-", ".", 1)
		var parts = strings.Split(tml, ".")
		if len(parts) != 2 {
			return nerror.New("invalid topic string %s", tml)
		}
		t.T = parts[0]
		t.R = parts[1]
		return nil
	}

	t.T = tm
	return nil
}

func (t Topic) String() string {
	return t.T
}

func (t Topic) ReplyTopic() Topic {
	return NewTopic(fmt.Sprintf("%s-reply-%s", t.T, t.R), "")
}

type Message struct {
	// Optional future which will indicate if message delivery should
	// notify attached future on result.
	Future *nthen.Future

	// SuggestedStatusCode is an optional field settable by the
	// creator to suggest possible status code of a message.
	SuggestedStatusCode int

	// ContentType is an required value set default to MessageContentType.
	// Its an important use in the translators where its the deciding factor
	// if a message is written as a whole or just the payload into the
	// response object.
	ContentType string

	// FormName is optional attached form name which represents this data.
	FormName string

	// FileName is optional attached file name which represents this data.
	FileName string

	// Path of the request producing this if from http.
	Path string

	// IP of the request producing this if from http.
	IP string

	// LocalIP of the request producing this if from http.
	LocalIP string

	// ExpectReply indicates if the receiver of said message should
	// handle this as a SendReply operation.
	ExpectReply bool

	// ReplyGroup is the when provided the suggested topic to reply to by receiving party.
	ReplyGroup string

	// FromAddr is the logical address of the sender of message.
	FromAddr string

	// Bytes is the payload for giving message.
	Bytes []byte

	// SubscribeGroup for subscribe/unsubscribe message types which
	// allow to indicate which group a topic should fall into.
	SubscribeGroup string

	// SubscribeTo for subscribe/unsubscribe message types which
	// allow to indicate which topic should a subscribe or unsubscribe
	// be applied to.
	SubscribeTo string

	// Id is the unique id attached to giving message
	// for tracking it's delivery and trace its different touch
	// points where it was handled.
	Id nxid.ID

	// EndPartId is the unique id attached to giving messages which
	// indicate the expected end id which when seen as the Id
	// should consider a part stream as completed.
	//
	// This will be created from the start and then tagged to the final
	// message as both the EndPartId and PartId fields, which will identify
	// that a series of broken messages have been completed.
	EndPartId nxid.ID

	// PartId is the unique id attached to giving messages when
	// they are a shared part of a larger messages. There are cases
	// when a message may become sent as broken parts for recollection
	// at the other end.
	PartId nxid.ID

	// Topic for giving message (serving as to address).
	Topic Topic

	// Optional reply error send to indicate message is an error reply
	// and the error that occurred.
	ReplyErr error

	// Headers are related facts attached to a message.
	Headers Header

	// Headers are related facts attached to a message.
	//
	// Only available when set, so it's very optional
	Cookies []Cookie

	// Form contains the parsed form data, including both the URL
	// field's query parameters and the PATCH, POST, or PUT form data.
	//
	// Only available when set, so it's very optional
	Form url.Values

	// Query contains the parsed form data, including both the URL
	// field's query parameters and the PATCH, POST, or PUT form data.
	//
	// Only available when set, so it's very optional
	Query url.Values

	// Within indicates senders intent on how long they are
	// willing to wait for message delivery. Usually this should end
	// with error resolution of attached future if present.
	Within time.Duration

	// Metadata are related facts attached to a message.
	Metadata Params

	// Params are related facts attached to a message based on some route or
	// sender and provide some values to keyed expectation, unlike metadata
	// it has specific input in the message.
	Params Params

	// Parts are possible fragments collected of a message which was split into
	// multiple parts to send over the wire and have being collected through the use
	// of the PartId.
	//
	// We do this because we do not let handlers handle a list of messages but one
	// and to accommodate large messages split in parts or messages which are logical
	// parts of themselves, this field is an option, generally.
	// Codecs should never read this
	Parts []Message
}

// ReplyWithTopic returns a new message with provided topic.
func (m Message) ReplyWithTopic(t Topic) Message {
	return Message{
		Topic:       t,
		ContentType: MessageContentType,
		Id:          nxid.New(),
		Params:      Params{},
		Metadata:    Params{},
	}
}

// ReplyTo returns a new instance of a Message using the FromAddr as the
// topic.
func (m Message) ReplyTo() Message {
	return Message{
		ContentType: MessageContentType,
		Id:          nxid.New(),
		Topic:       T(m.FromAddr),
		Params:      Params{},
		Metadata:    Params{},
	}
}

// ReplyToWith returns a new instance of a Message using the FromAddr as the
// topic.
func (m Message) ReplyToWith(params Params, meta Params, payload []byte) Message {
	return Message{
		ContentType: MessageContentType,
		Params:      params,
		Metadata:    meta,
		Id:          nxid.New(),
		Topic:       T(m.FromAddr),
	}
}

var (
	SUBSCRIBE   = T("+SUB")
	UNSUBSCRIBE = T("-USUB")
	DONE        = T("+OK")
	NOTDONE     = T("-NOK")
)

const MessageContentType = "application/x-event-message"

func NewMessage(topic Topic, fromAddr string, payload []byte) Message {
	return Message{
		Id:          nxid.New(),
		Topic:       topic,
		FromAddr:    fromAddr,
		Bytes:       payload,
		ContentType: MessageContentType,
	}
}

func NOTOK(message string, fromAddr string) Message {
	return Message{
		Id:          nxid.New(),
		Topic:       NOTDONE,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: MessageContentType,
	}
}

func BasicMsg(topic Topic, message string, fromAddr string) Message {
	return Message{
		Id:          nxid.New(),
		Topic:       topic,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: MessageContentType,
	}
}

func OK(message string, fromAddr string) Message {
	return Message{
		Id:          nxid.New(),
		Topic:       DONE,
		FromAddr:    fromAddr,
		Bytes:       []byte(message),
		ContentType: MessageContentType,
	}
}

func UnsubscribeMessage(topic string, grp string, fromAddr string) Message {
	return Message{
		Id:             nxid.New(),
		Topic:          UNSUBSCRIBE,
		FromAddr:       fromAddr,
		SubscribeGroup: grp,
		SubscribeTo:    topic,
		ContentType:    MessageContentType,
	}
}

func SubscribeMessage(topic string, grp string, fromAddr string) Message {
	return Message{
		Id:             nxid.New(),
		Topic:          SUBSCRIBE,
		FromAddr:       fromAddr,
		SubscribeGroup: grp,
		SubscribeTo:    topic,
		ContentType:    MessageContentType,
	}
}

func (m *Message) WithId(t nxid.ID) {
	m.Id = t
}

func (m *Message) WithTopic(t Topic) {
	m.Topic = t
}

func (m *Message) WithPayload(lp []byte) {
	m.Bytes = lp
}

func (m *Message) WithMetadata(meta map[string]string) {
	m.Metadata = meta
}

func (m *Message) WithParams(params map[string]string) {
	m.Params = params
}

func (m Message) EncodeObject(encoder npkg.ObjectEncoder) {
	encoder.String("topic", m.Topic.String())
	encoder.String("from_addr", m.FromAddr)
	encoder.String("Bytes", nunsafe.Bytes2String(m.Bytes))
	encoder.StringMap("meta_data", m.Metadata)
}

func (m Message) String() string {
	var content strings.Builder
	content.WriteString("topic: ")
	content.WriteString(m.Topic.String())
	content.WriteString(",")
	content.WriteString("from: ")
	content.WriteString(m.FromAddr)
	content.WriteString(",")
	content.WriteString("payload: ")
	content.WriteString(fmt.Sprintf("%q", m.Bytes))
	content.WriteString(",")
	content.WriteString("local_payload: ")
	content.WriteString(",")
	content.WriteString("Meta: ")
	for key, val := range m.Metadata {
		content.WriteString(key)
		content.WriteString(": ")
		content.WriteString(val)
		content.WriteString(",")
	}
	content.WriteString(";")
	return content.String()
}

// Copy returns a copy of this commands with underline data
// copied across. The copy
func (m Message) Copy() Message {
	var meta = map[string]string{}
	for key, val := range m.Metadata {
		meta[key] = val
	}
	var clone = m
	clone.Metadata = meta
	clone.Bytes = append([]byte{}, m.Bytes...)
	return clone
}
