package sabuhp

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/influx6/npkg/nunsafe"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg"
)

const (
	SUBSCRIBE   = "+SUB"
	UNSUBSCRIBE = "-USUB"
	DONE        = "+OK"
	NOTDONE     = "-NOK"
)

func NOTOK(message string, fromAddr string) *Message {
	return &Message{
		ID:       nxid.New(),
		Topic:    NOTDONE,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func BasicMsg(topic string, message string, fromAddr string) *Message {
	return &Message{
		ID:       nxid.New(),
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func OK(message string, fromAddr string) *Message {
	return &Message{
		ID:       nxid.New(),
		Topic:    DONE,
		FromAddr: fromAddr,
		Payload:  []byte(message),
	}
}

func UnsubscribeMessage(topic string, fromAddr string) *Message {
	return &Message{
		ID:       nxid.New(),
		Topic:    UNSUBSCRIBE,
		FromAddr: fromAddr,
		Payload:  []byte(topic),
	}
}

func SubscribeMessage(topic string, fromAddr string) *Message {
	return &Message{
		ID:       nxid.New(),
		Topic:    SUBSCRIBE,
		FromAddr: fromAddr,
		Payload:  []byte(topic),
	}
}

type PayloadType int

const (
	BinaryPayloadType PayloadType = iota
	TextPayloadType
	HTTPRWPayload
)

type Payload interface {
	Type() PayloadType
	Copy() Payload
}

// HttpResponseAsPayload returns a *http.Response object wrapped
// as a payload.
func HttpResponseAsPayload(rw *http.Response) Payload {
	return &HttpResponsePayload{rw}
}

// HttpResponsePayload wrapped a *http.Response as a Payload type.
type HttpResponsePayload struct {
	*http.Response
}

func (h *HttpResponsePayload) Type() PayloadType {
	return HTTPRWPayload
}

func (h *HttpResponsePayload) Copy() Payload {
	return h
}

// HttpResponseWriterAsPayload returns a http.ResponseWriter object wrapped
// as a payload.
func HttpResponseWriterAsPayload(rw http.ResponseWriter) Payload {
	return &HttpResponseWriterPayload{rw}
}

// HttpResponseWriterPayload wrapped a http.ResponseWriter as a Payload type.
type HttpResponseWriterPayload struct {
	http.ResponseWriter
}

func (h *HttpResponseWriterPayload) Type() PayloadType {
	return HTTPRWPayload
}

func (h *HttpResponseWriterPayload) Copy() Payload {
	return h
}

type TextPayload string

func (b TextPayload) Copy() Payload {
	var copyBuffer = make([]byte, len(b))
	_ = copy(copyBuffer, b)
	return TextPayload(copyBuffer)
}

func (b TextPayload) Type() PayloadType {
	return TextPayloadType
}

type BinaryPayload []byte

func (b BinaryPayload) Copy() Payload {
	var copyBuffer = make([]byte, len(b))
	_ = copy(copyBuffer, b)
	return BinaryPayload(copyBuffer)
}

func (b BinaryPayload) Type() PayloadType {
	return BinaryPayloadType
}

type DeliveryMark int

const (
	SendToAll DeliveryMark = iota
	SendToOne
)

type Message struct {
	// ID is the unique id attached to giving message
	// for tracking it's delivery and trace its different touch
	// points where it was handled.
	ID nxid.ID

	// Topic for giving message (serving as to address).
	Topic string

	// FromAddr is the logical address of the sender of message.
	FromAddr string

	// Delivery embodies how this message is to be delivered. It's
	// usually defined by the sender to indicate the target is all
	// fan-out sequence (send to all) or a target sequence (sent to one).
	//
	// This should be set by the transport handling said message and not by
	// creator.
	Delivery DeliveryMark

	// Payload is the payload for giving message.
	Payload []byte

	// Headers are related facts attached to a message.
	Headers Header

	// Headers are related facts attached to a message.
	Cookies []Cookie

	// Metadata are related facts attached to a message.
	Metadata map[string]string

	// Params are related facts attached to a message based on some route or
	// sender and provide some values to keyed expectation, unlike metadata
	// it has specific input in the message.
	Params map[string]string

	// LocalPayload is the payload attached which can be a
	// concrete object for which this message is to communicate
	// such a payload may not be able to be serialized and only
	// serves the purpose of a local runtime communication.
	LocalPayload Payload `json:"-" messagepack:"-" gob:"-"`

	// OverridingTransport allows special messages which may not fall under
	// the same behaviour as normal messages which are delivered over a
	// transport, the general idea is that all handlers will use this
	// specific transport instead of the default one. This makes this message
	// a special occurrence, and will be treated as so.
	OverridingTransport Transport `json:"-" messagepack:"-" gob:"-"`
}

func NewMessage(topic string, fromAddr string, payload []byte) *Message {
	return &Message{
		ID:       nxid.New(),
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  payload,
	}
}

func (m *Message) WithPayload(lp []byte) *Message {
	m.Payload = lp
	return m
}

func (m *Message) WithLocalPayload(lp Payload) *Message {
	m.LocalPayload = lp
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

func (m *Message) EncodeObject(encoder npkg.ObjectEncoder) {
	encoder.String("topic", m.Topic)
	encoder.String("from_addr", m.FromAddr)
	encoder.String("Payload", nunsafe.Bytes2String(m.Payload))
	encoder.StringMap("meta_data", m.Metadata)
	encoder.String("local_payload", fmt.Sprintf("%#v", m.LocalPayload))
}

func (m *Message) String() string {
	var content strings.Builder
	content.WriteString("topic: ")
	content.WriteString(m.Topic)
	content.WriteString(",")
	content.WriteString("from: ")
	content.WriteString(m.FromAddr)
	content.WriteString(",")
	content.WriteString("payload: ")
	content.WriteString(fmt.Sprintf("%q", m.Payload))
	content.WriteString(",")
	content.WriteString("local_payload: ")
	content.WriteString(fmt.Sprintf("%q", m.LocalPayload))
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
func (m *Message) Copy() *Message {
	var meta = map[string]string{}
	for key, val := range m.Metadata {
		meta[key] = val
	}
	var clone = *m
	clone.Metadata = meta
	clone.Payload = append([]byte{}, m.Payload...)
	if m.LocalPayload != nil {
		clone.LocalPayload = m.LocalPayload.Copy()
	}

	return &clone
}
