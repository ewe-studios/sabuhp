package codecs

import (
	"encoding/json"
	"github.com/ewe-studios/sabuhp/sabu"

	"github.com/influx6/npkg/nerror"
)

var _ sabu.Codec = (*MessageJsonCodec)(nil)

type MessageJsonCodec struct{}

func (j *MessageJsonCodec) Encode(message sabu.Message) ([]byte, error) {
	message.Parts = nil
	encoded, encodedErr := json.Marshal(message)
	if encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}

	return encoded, nil
}

func (j *MessageJsonCodec) Decode(b []byte) (sabu.Message, error) {
	var message sabu.Message
	if jsonErr := json.Unmarshal(b, &message); jsonErr != nil {
		return message, nerror.WrapOnly(jsonErr)
	}
	message.Future = nil
	return message, nil
}
