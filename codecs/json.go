package codecs

import (
	"encoding/json"

	"github.com/ewe-studios/sabuhp"

	"github.com/influx6/npkg/nerror"
)

var _ sabuhp.Codec = (*MessageJsonCodec)(nil)

type MessageJsonCodec struct{}

func (j *MessageJsonCodec) Encode(message sabuhp.Message) ([]byte, error) {
	message.Parts = nil
	encoded, encodedErr := json.Marshal(message)
	if encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}

	return encoded, nil
}

func (j *MessageJsonCodec) Decode(b []byte) (sabuhp.Message, error) {
	var message sabuhp.Message
	if jsonErr := json.Unmarshal(b, &message); jsonErr != nil {
		return message, nerror.WrapOnly(jsonErr)
	}
	message.Future = nil
	return message, nil
}
