package codecs

import (
	"encoding/json"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"
)

var _ sabuhp.Codec = (*JsonCodec)(nil)

type JsonCodec struct{}

func (j *JsonCodec) Encode(message *sabuhp.Message) ([]byte, error) {
	encoded, encodedErr := json.Marshal(message)
	if encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}

	return encoded, nil
}

func (j *JsonCodec) Decode(b []byte) (*sabuhp.Message, error) {
	var message sabuhp.Message
	if jsonErr := json.Unmarshal(b, &message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}

var _ sabuhp.Codec = (*MessagePackCodec)(nil)
