package codecs

import (
	"bytes"
	"github.com/ewe-studios/sabuhp/sabu"

	"github.com/influx6/npkg/nerror"
	"github.com/vmihailenco/msgpack/v5"
)

var _ sabu.Codec = (*MessageMsgPackCodec)(nil)

type MessageMsgPackCodec struct{}

func (j *MessageMsgPackCodec) Encode(message sabu.Message) ([]byte, error) {
	message.Parts = nil
	var buf bytes.Buffer
	if encodedErr := msgpack.NewEncoder(&buf).Encode(message); encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}
	return buf.Bytes(), nil
}

func (j *MessageMsgPackCodec) Decode(b []byte) (sabu.Message, error) {
	var message sabu.Message
	if jsonErr := msgpack.NewDecoder(bytes.NewBuffer(b)).Decode(&message); jsonErr != nil {
		return message, nerror.WrapOnly(jsonErr)
	}
	message.Future = nil
	return message, nil
}
