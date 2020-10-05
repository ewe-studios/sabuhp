package supabaiza

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

type Message struct {
	// Topic for giving message (serving as to address).
	Topic string

	// FromAddr is the logical address of the sender of message.
	FromAddr string

	// Payload is the payload for giving message.
	Payload Payload

	// Metadata are related facts attached to a message.
	Metadata map[string]string

	// Ack allows you to request acknowledgement that message
	// was processed, if provided. Our advice is to always
	// ensure your ack is a buffered channel of 1 capacity.
	//
	// If this is not the case then you will get a panic
	Ack chan struct{}

	// Nack allows you to request acknowledgement that message
	// was not processed, if provided. Our advice is to always
	// ensure your ack is a buffered channel of 1 capacity.
	//
	// If this is not the case then you will get a panic
	Nack chan struct{}
}

// Copy returns a copy of this messages with underline data
// copied across. The copy
func (m *Message) Copy() *Message {
	var meta = map[string]string{}
	for key, val := range m.Metadata {
		meta[key] = val
	}
	var clone = *m
	clone.Metadata = meta
	clone.Payload = m.Payload.Copy()
	return &clone
}
