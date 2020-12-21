package sabuhp

import (
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/nxid"
)

const MessageContentType = "application/x-event-message"

type CodecTransposer struct {
	Codec       Codec
	Logger      Logger
	MaxBodySize int64
}

func NewCodecTransposer(codec Codec, logger Logger) *CodecTransposer {
	return &CodecTransposer{Codec: codec, Logger: logger}
}

func (r *CodecTransposer) Transpose(req *http.Request, params Params) (*Message, error) {
	var contentType = req.Header.Get("Content-Type")
	var contentTypeLower = strings.ToLower(contentType)

	if r.MaxBodySize > 0 {
		req.Body = MaxBytesReader(req.Body, r.MaxBodySize)
	}

	var isFormOrMultiPart bool

	// if it's a multipart form, get multi-part reader: multipart/form-data or a multipart/mixed
	var multiPartReader *multipart.Reader
	if strings.Contains(contentTypeLower, "multipart/form-data") || strings.Contains(contentTypeLower, "multipart/mixed") {
		isFormOrMultiPart = true
		if reader, getReaderErr := req.MultipartReader(); getReaderErr == nil {
			multiPartReader = reader
		}
	}

	if strings.Contains(contentTypeLower, "application/x-www-form-urlencoded") ||
		strings.Contains(contentTypeLower, "form-urlencoded") {
		isFormOrMultiPart = true
		if err := req.ParseForm(); err != nil {
			return nil, nerror.WrapOnly(err)
		}
	}

	if isFormOrMultiPart {
		return &Message{
			ID:                  nxid.New(),
			Topic:               "",
			FromAddr:            req.RemoteAddr,
			Delivery:            SendToAll,
			Payload:             nil,
			Query:               req.URL.Query(),
			Form:                req.Form,
			MultipartReader:     multiPartReader,
			Metadata:            map[string]string{},
			Headers:             Header(req.Header.Clone()),
			Cookies:             ReadCookies(Header(req.Header), ""),
			Params:              params,
			LocalPayload:        nil,
			OverridingTransport: nil,
		}, nil
	}

	var content bytes.Buffer
	if _, err := io.Copy(&content, req.Body); err != nil {
		return nil, nerror.WrapOnly(err)
	}

	// if its not an explicit application/x-event-message type then we
	// assume the body is the message payload.
	if !strings.Contains(contentTypeLower, MessageContentType) {
		return &Message{
			Topic:               "",
			ID:                  nxid.New(),
			FromAddr:            req.RemoteAddr,
			Delivery:            SendToAll,
			Payload:             content.Bytes(),
			Form:                req.Form,
			Query:               req.URL.Query(),
			Metadata:            map[string]string{},
			Headers:             Header(req.Header.Clone()),
			Cookies:             ReadCookies(Header(req.Header), ""),
			Params:              params,
			MultipartReader:     nil,
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

// MaxBytesReader is similar to io.LimitReader but is intended for
// limiting the size of incoming request bodies. In contrast to
// io.LimitReader, MaxBytesReader's result is a ReadCloser, returns a
// non-EOF error for a Read beyond the limit, and closes the
// underlying reader when its Close method is called.
//
// Returns a RequestToLargeErr object when request body is to large.
//
//
// MaxBytesReader prevents clients from accidentally or maliciously
// sending a large request and wasting server resources.
func MaxBytesReader(r io.ReadCloser, n int64) io.ReadCloser {
	return &maxBytesReader{r: r, n: n}
}

var BodyToLargeErr = &requestTooLargeErr{Err: errors.New("http: req body to large")}

type requestTooLargeErr struct {
	Err error
}

func (r *requestTooLargeErr) Error() string {
	return r.Err.Error()
}

type maxBytesReader struct {
	r   io.ReadCloser // underlying reader
	n   int64         // max bytes remaining
	err error         // sticky error
}

func (l *maxBytesReader) Read(p []byte) (n int, err error) {
	if l.err != nil {
		return 0, l.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// If they asked for a 32KB byte read but only 5 bytes are
	// remaining, no need to read 32KB. 6 bytes will answer the
	// question of the whether we hit the limit or go past it.
	if int64(len(p)) > l.n+1 {
		p = p[:l.n+1]
	}
	n, err = l.r.Read(p)

	if int64(n) <= l.n {
		l.n -= int64(n)
		l.err = err
		return n, err
	}

	n = int(l.n)
	l.n = 0

	// The server code and client code both use
	// maxBytesReader. This "requestTooLarge" check is
	// only used by the server code. To prevent binaries
	// which only using the HTTP Client code (such as
	// cmd/go) from also linking in the HTTP server, don't
	// use a static type assertion to the server
	// "*response" type. Check this interface instead:
	l.err = BodyToLargeErr
	return n, l.err
}

func (l *maxBytesReader) Close() error {
	return l.r.Close()
}
