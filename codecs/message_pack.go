package codecs

import (
	"bytes"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

var _ sabuhp.Codec = (*MessagePackCodec)(nil)

type MessagePackCodec struct{}

func (j *MessagePackCodec) Encode(message *sabuhp.Message) ([]byte, error) {
	var buf bytes.Buffer
	if encodedErr := msgpack.NewEncoder(&buf).Encode(message); encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}
	return buf.Bytes(), nil
}

func (j *MessagePackCodec) Decode(b []byte) (*sabuhp.Message, error) {
	var message sabuhp.Message
	if jsonErr := msgpack.NewDecoder(bytes.NewBuffer(b)).Decode(&message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}
