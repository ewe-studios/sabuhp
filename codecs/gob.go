package codecs

import (
	"bytes"
	"encoding/gob"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"
)

var _ sabuhp.Codec = (*GobCodec)(nil)

type GobCodec struct{}

func (j *GobCodec) Encode(message *sabuhp.Message) ([]byte, error) {
	var buf bytes.Buffer
	if encodedErr := gob.NewEncoder(&buf).Encode(message); encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}
	return buf.Bytes(), nil
}

func (j *GobCodec) Decode(b []byte) (*sabuhp.Message, error) {
	var message sabuhp.Message
	if jsonErr := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}
