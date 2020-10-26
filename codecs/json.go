package codecs

import (
	"encoding/json"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp/supabaiza"
)

var _ supabaiza.Codec = (*JsonCodec)(nil)

type JsonCodec struct{}

func (j *JsonCodec) Encode(message *supabaiza.Message) ([]byte, error) {
	encoded, encodedErr := json.Marshal(message)
	if encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}

	return encoded, nil
}

func (j *JsonCodec) Decode(b []byte) (*supabaiza.Message, error) {
	var message supabaiza.Message
	if jsonErr := json.Unmarshal(b, &message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}

var _ supabaiza.Codec = (*MessagePackCodec)(nil)
