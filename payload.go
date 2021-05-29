package sabuhp

import (
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

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

type Message struct {
	// Optional future which will indicate if message delivery should
	// notify attached future on result.
	Future *nthen.Future

	// Path of the request producing this if from http.
	Path string

	// IP of the request producing this if from http.
	IP string

	// LocalIP of the request producing this if from http.
	LocalIP string

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

	// SubscribeGroup for subscribe/unsubscribe message types which
	// allow to indicate which group a topic should fall into.
	SubscribeGroup string

	// SubscribeTo for subscribe/unsubscribe message types which
	// allow to indicate which topic should a subscribe or unsubscribe
	// be applied to.
	SubscribeTo string

	// Topic for giving message (serving as to address).
	Topic string

	// FromAddr is the logical address of the sender of message.
	FromAddr string

	// Bytes is the payload for giving message.
	Bytes []byte

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
	// and to accomodate large messages split in parts or messages which are logical
	// parts of themselves, this field is an option, generally.
	// Codecs should never read this
	Parts []Message
}

// ReplyWithTopic returns a new message with provided topic.
func (m Message) ReplyWithTopic(t string) Message {
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
		Topic:       m.FromAddr,
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
		Topic:       m.FromAddr,
	}
}

const (
	SUBSCRIBE   = "+SUB"
	UNSUBSCRIBE = "-USUB"
	DONE        = "+OK"
	NOTDONE     = "-NOK"
)

const MessageContentType = "application/x-event-message"

func NewMessage(topic string, fromAddr string, payload []byte) Message {
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

func BasicMsg(topic string, message string, fromAddr string) Message {
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

func (m *Message) WithPayload(lp []byte) *Message {
	m.Bytes = lp
	return m
}

func (m *Message) WithMetadata(meta map[string]string) *Message {
	m.Metadata = meta
	return m
}

func (m *Message) WithParams(params map[string]string) *Message {
	m.Params = params
	return m
}

func (m Message) EncodeObject(encoder npkg.ObjectEncoder) {
	encoder.String("topic", m.Topic)
	encoder.String("from_addr", m.FromAddr)
	encoder.String("Bytes", nunsafe.Bytes2String(m.Bytes))
	encoder.StringMap("meta_data", m.Metadata)
}

func (m Message) String() string {
	var content strings.Builder
	content.WriteString("topic: ")
	content.WriteString(m.Topic)
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
