package codecs

import (
	"bytes"
	"encoding/gob"
	"github.com/ewe-studios/sabuhp/sabu"

	"github.com/influx6/npkg/nerror"
)

var _ sabu.Codec = (*MessageGobCodec)(nil)

type MessageGobCodec struct{}

func (j *MessageGobCodec) Encode(message sabu.Message) ([]byte, error) {
	message.Parts = nil
	var buf bytes.Buffer
	if encodedErr := gob.NewEncoder(&buf).Encode(message); encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}
	return buf.Bytes(), nil
}

func (j *MessageGobCodec) Decode(b []byte) (sabu.Message, error) {
	var message sabu.Message
	if jsonErr := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&message); jsonErr != nil {
		return message, nerror.WrapOnly(jsonErr)
	}
	message.Future = nil
	return message, nil
}
