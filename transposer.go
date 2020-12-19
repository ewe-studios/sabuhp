package sabuhp

import (
	"bytes"
	"io"
	"net/http"

	"github.com/influx6/npkg/nerror"
)

type CodecTransposer struct {
	Codec  Codec
	Logger Logger
}

func NewCodecTransposer(codec Codec, logger Logger) *CodecTransposer {
	return &CodecTransposer{Codec: codec, Logger: logger}
}

func (r *CodecTransposer) Transpose(req *http.Request, params Params) (*Message, error) {
	var content bytes.Buffer
	if _, err := io.Copy(&content, req.Body); err != nil {
		return nil, nerror.WrapOnly(err)
	}
	var message, messageErr = r.Codec.Decode(content.Bytes())
	if messageErr != nil {
		return nil, nerror.WrapOnly(messageErr)
	}
	for k, v := range params {
		message.Params[k] = v
	}
	return message, nil
}
