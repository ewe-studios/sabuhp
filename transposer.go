package sabuhp

import (
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"
)

var _ Translator = (*CodecTranslator)(nil)

type CodecTranslator struct {
	Codec  Codec
	Logger Logger
}

func NewCodecTranslator(codec Codec, logger Logger) *CodecTranslator {
	return &CodecTranslator{Codec: codec, Logger: logger}
}

func (r *CodecTranslator) TranslateWriter(res http.ResponseWriter, w io.WriterTo, m MessageMeta) error {
	var stack = njson.Log(r.Logger)
	defer njson.ReleaseLogStack(stack)

	// if the content type is not MessageContentType ("application/x-event-message")
	// it means the user does not intend to write the whole message structure as the
	// response, so:
	// 1. Copy out the content type
	// 2. Write the payload if any as the response
	if m.ContentType != MessageContentType {
		stack.New().
			LInfo().
			Message("message type for response is not application/x-event-message").
			String("content-type", m.ContentType).
			End()

		res.Header().Set("Content-Type", m.ContentType)
	} else {
		res.Header().Set("Content-Type", MessageContentType)
	}

	if m.SuggestedStatusCode > 0 {
		res.WriteHeader(m.SuggestedStatusCode)
	} else {
		res.WriteHeader(http.StatusOK)
	}

	if written, err := w.WriteTo(res); err != nil {
		var wrappedErr = nerror.WrapOnly(err)
		stack.New().
			LError().
			Message("failed to send response message").
			Int64("written", written).
			Error("error", wrappedErr).
			End()
		return wrappedErr
	}

	return nil
}

func (r *CodecTranslator) TranslateBytes(res http.ResponseWriter, data []byte, m MessageMeta) error {
	var stack = njson.Log(r.Logger)
	defer njson.ReleaseLogStack(stack)

	// if the content type is not MessageContentType ("application/x-event-message")
	// it means the user does not intend to write the whole message structure as the
	// response, so:
	// 1. Copy out the content type
	// 2. Write the payload if any as the response
	if m.ContentType != MessageContentType {
		stack.New().
			LInfo().
			Bytes("data", data).
			Message("message type for response is not application/x-event-message").
			String("content-type", m.ContentType).
			End()

		res.Header().Set("Content-Type", m.ContentType)
	} else {
		res.Header().Set("Content-Type", MessageContentType)
	}

	if m.SuggestedStatusCode > 0 {
		res.WriteHeader(m.SuggestedStatusCode)
	} else {
		res.WriteHeader(http.StatusOK)
	}

	if written, err := res.Write(data); err != nil {
		var wrappedErr = nerror.WrapOnly(err)
		stack.New().
			LError().
			Message("failed to send response message").
			Bytes("data", data).
			Int("data", written).
			Error("error", wrappedErr).
			End()
		return wrappedErr
	}

	return nil
}

func (r *CodecTranslator) Translate(res http.ResponseWriter, m *Message) error {
	var stack = njson.Log(r.Logger)
	defer njson.ReleaseLogStack(stack)

	// if the content type is not MessageContentType ("application/x-event-message")
	// it means the user does not intend to write the whole message structure as the
	// response, so:
	// 1. Copy out the content type
	// 2. Write the payload if any as the response
	if m.ContentType != MessageContentType {
		stack.New().
			LInfo().
			Message("message type for response is not application/x-event-message").
			String("content-type", m.ContentType).
			Object("message", m).
			End()

		res.Header().Set("Content-Type", m.ContentType)

		if m.SuggestedStatusCode > 0 {
			res.WriteHeader(m.SuggestedStatusCode)
		} else {
			res.WriteHeader(http.StatusOK)
		}

		if written, err := res.Write(m.Payload); err != nil {
			var wrappedErr = nerror.WrapOnly(err)
			stack.New().
				LError().
				Message("failed to send response message").
				Int("written", written).
				Error("error", wrappedErr).
				End()
			return wrappedErr
		}
		return nil
	}

	res.Header().Set("Content-Type", MessageContentType)
	if m.SuggestedStatusCode > 0 {
		res.WriteHeader(m.SuggestedStatusCode)
	} else {
		res.WriteHeader(http.StatusOK)
	}

	var encoded, encodedErr = r.Codec.Encode(m)
	if encodedErr != nil {
		return nerror.WrapOnly(encodedErr)
	}

	if written, err := res.Write(encoded); err != nil {
		var wrappedErr = nerror.WrapOnly(err)
		stack.New().
			LError().
			Message("failed to send response message").
			Int("written", written).
			Error("error", wrappedErr).
			End()
		return nil
	}

	return nil
}

type WrappedCodecTransposer struct {
	WrappedCodec WrappedCodec
	Codec        Codec
	Logger       Logger
}

func NewWrappedCodecTransposer(codec Codec, wrappedCodec WrappedCodec, logger Logger) *WrappedCodecTransposer {
	return &WrappedCodecTransposer{Codec: codec, WrappedCodec: wrappedCodec, Logger: logger}
}

func (r *WrappedCodecTransposer) Transpose(data []byte) (*Message, *WrappedPayload, error) {
	// decode data into wrapped object
	var wrappedPayload, wrappedPayloadErr = r.WrappedCodec.Decode(data)
	if wrappedPayloadErr != nil {
		return nil, nil, nerror.WrapOnly(wrappedPayloadErr)
	}

	// check if it's a payload content type
	if wrappedPayload.ContentType != MessageContentType {
		return &Message{
			Topic:    "",
			ID:       nxid.New(),
			Delivery: SendToAll,
			MessageMeta: MessageMeta{
				ContentType:     wrappedPayload.ContentType,
				Query:           url.Values{},
				Form:            url.Values{},
				Headers:         Header{},
				Cookies:         nil,
				MultipartReader: nil,
			},
			Payload:             wrappedPayload.Payload,
			Metadata:            map[string]string{},
			Params:              map[string]string{},
			LocalPayload:        nil,
			OverridingTransport: nil,
		}, wrappedPayload, nil
	}

	var message, messageErr = r.Codec.Decode(wrappedPayload.Payload)
	if messageErr != nil {
		return nil, wrappedPayload, nerror.WrapOnly(messageErr)
	}
	return message, wrappedPayload, nil
}

var _ Transposer = (*CodecTransposer)(nil)

type CodecTransposer struct {
	Codec       Codec
	Logger      Logger
	MaxBodySize int64
}

func NewCodecTransposer(codec Codec, logger Logger, maxBody int64) *CodecTransposer {
	return &CodecTransposer{Codec: codec, Logger: logger, MaxBodySize: maxBody}
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
			ID:       nxid.New(),
			Topic:    "",
			FromAddr: req.RemoteAddr,
			Delivery: SendToAll,
			Payload:  nil,
			MessageMeta: MessageMeta{
				Headers:         Header(req.Header.Clone()),
				Cookies:         ReadCookies(Header(req.Header), ""),
				Query:           req.URL.Query(),
				Form:            req.Form,
				MultipartReader: multiPartReader,
				ContentType:     contentType,
			},
			Metadata:            map[string]string{},
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
			Topic:    "",
			ID:       nxid.New(),
			FromAddr: req.RemoteAddr,
			Delivery: SendToAll,
			MessageMeta: MessageMeta{
				MultipartReader: nil,
				Headers:         Header(req.Header.Clone()),
				Cookies:         ReadCookies(Header(req.Header), ""),
				Form:            req.Form,
				Query:           req.URL.Query(),
				ContentType:     contentType,
			},
			Payload:             content.Bytes(),
			Metadata:            map[string]string{},
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

	if len(message.ContentType) == 0 {
		message.ContentType = contentType
	}

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
