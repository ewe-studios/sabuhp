package codecs

import (
	"bytes"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp/supabaiza"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

var _ supabaiza.Codec = (*MessagePackCodec)(nil)

type MessagePackCodec struct{}

func (j *MessagePackCodec) Encode(message *supabaiza.Message) ([]byte, error) {
	var buf bytes.Buffer
	if encodedErr := msgpack.NewEncoder(&buf).Encode(message); encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}
	return buf.Bytes(), nil
}

func (j *MessagePackCodec) Decode(b []byte) (*supabaiza.Message, error) {
	var message supabaiza.Message
	if jsonErr := msgpack.NewDecoder(bytes.NewBuffer(b)).Decode(&message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}
