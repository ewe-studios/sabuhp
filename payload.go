package sabuhp

import (
	"fmt"
	"strings"

	"github.com/influx6/npkg"
)

type PayloadType int

const (
	BinaryPayloadType PayloadType = iota
	TextPayloadType
)

type Payload interface {
	Type() PayloadType
	Copy() Payload
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

	// LocalPayload is the payload attached which can be a
	// concrete object for which this message is to communicate
	// such a payload may not be able to be serialized and only
	// serves the purpose of a local runtime communication.
	LocalPayload Payload

	// Metadata are related facts attached to a message.
	Metadata map[string]string
}

func NewMessage(topic string, fromAddr string, payload []byte, meta map[string]string, localPayload Payload) *Message {
	return &Message{
		Topic:    topic,
		FromAddr: fromAddr,
		Payload:  payload,
		Metadata: meta,
	}
}

func (m *Message) EncodeObject(encoder npkg.ObjectEncoder) {
	encoder.String("topic", m.Topic)
	encoder.String("from_addr", m.FromAddr)
	encoder.Bytes("Payload", m.Payload)
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

// Codec embodies implementation for the serialization of
// a message into bytes and vice-versa.
type Codec interface {
	Encode(msg *Message) ([]byte, error)
	Decode(b []byte) (*Message, error)
}
