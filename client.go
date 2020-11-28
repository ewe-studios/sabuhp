package sabuhp

import (
	"time"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nunsafe"
)

// Codec embodies implementation for the serialization of
// a message into bytes and vice-versa.
type Codec interface {
	Encode(msg *Message) ([]byte, error)
	Decode(b []byte) (*Message, error)
}

type Client interface {
	Send(data []byte, timeout time.Duration) error
}

type CodecWriter struct {
	Client Client
	Codec  Codec
	Logger Logger
}

func NewCodecWriter(client Client, codec Codec, logger Logger) *CodecWriter {
	return &CodecWriter{
		Client: client,
		Codec:  codec,
		Logger: logger,
	}
}

func (c *CodecWriter) Send(msg *Message, timeout time.Duration) error {
	var encoded, encodeErr = c.Codec.Encode(msg)
	if encodeErr != nil {
		var wrappedErr = nerror.WrapOnly(encodeErr)
		njson.Log(c.Logger).New().
			LError().
			Message("encoding message").
			String("error", wrappedErr.Error()).
			Object("data", msg).
			End()
		return wrappedErr
	}

	if sendErr := c.Client.Send(encoded, timeout); sendErr != nil {
		var wrappedErr = nerror.WrapOnly(encodeErr)
		njson.Log(c.Logger).New().
			LError().
			Message("failed to send encoded message message").
			String("encoded", nunsafe.Bytes2String(encoded)).
			String("error", wrappedErr.Error()).
			End()
		return wrappedErr
	}
	return nil
}
