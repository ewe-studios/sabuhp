package sabuhp

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/nxid"
)

const MessageContentType = "application/x-event-message"

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

	var contentType = strings.ToLower(req.Header.Get("Content-Type"))

	// if its not an explicit application/x-event-message type then we
	// assume the body is the message payload.
	if !strings.Contains(contentType, MessageContentType) {
		return &Message{
			ID:                  nxid.New(),
			Topic:               "",
			FromAddr:            req.RemoteAddr,
			Delivery:            SendToAll,
			Payload:             content.Bytes(),
			Metadata:            map[string]string{},
			Headers:             Header(req.Header.Clone()),
			Cookies:             ReadCookies(Header(req.Header), ""),
			Params:              params,
			LocalPayload:        nil,
			OverridingTransport: nil,
		}, nil
	}

	var message, messageErr = r.Codec.Decode(content.Bytes())
	if messageErr != nil {
		return nil, nerror.WrapOnly(messageErr)
	}

	message.Cookies = ReadCookies(Header(req.Header), "")
	message.Headers = Header(req.Header.Clone())

	for k, v := range params {
		message.Params[k] = v
	}
	return message, nil
}
