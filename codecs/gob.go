package codecs

import (
	"bytes"
	"encoding/gob"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp/supabaiza"
)

var _ supabaiza.Codec = (*GobCodec)(nil)

type GobCodec struct{}

func (j *GobCodec) Encode(message *supabaiza.Message) ([]byte, error) {
	var buf bytes.Buffer
	if encodedErr := gob.NewEncoder(&buf).Encode(message); encodedErr != nil {
		return nil, nerror.WrapOnly(encodedErr)
	}
	return buf.Bytes(), nil
}

func (j *GobCodec) Decode(b []byte) (*supabaiza.Message, error) {
	var message supabaiza.Message
	if jsonErr := gob.NewDecoder(bytes.NewBuffer(b)).Decode(&message); jsonErr != nil {
		return nil, nerror.WrapOnly(jsonErr)
	}
	return &message, nil
}
