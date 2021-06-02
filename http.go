package sabuhp

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"
)

var _ HttpEncoder = (*HttpEncoderImpl)(nil)

// HttpEncoder transforms a message into an appropriate response
// to an http response object.
type HttpEncoder interface {
	Encode(req http.ResponseWriter, message Message) error
}

type HttpEncoderImpl struct {
	Codec  Codec
	Logger Logger
}

func NewHttpEncoderImpl(codec Codec, logger Logger) *HttpEncoderImpl {
	return &HttpEncoderImpl{Codec: codec, Logger: logger}
}

func (r *HttpEncoderImpl) Encode(res http.ResponseWriter, m Message) error {
	var stack = njson.Log(r.Logger)

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

		if written, err := res.Write(m.Bytes); err != nil {
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

var _ HttpDecoder = (*HttpDecoderImpl)(nil)

type HeaderModifications func(header http.Header)

// HttpDecoder transforms a http request into a Message to be
// delivered.
type HttpDecoder interface {
	Decode(req *http.Request, params Params) (Message, error)
}

type HttpDecoderImpl struct {
	Codec       Codec
	Logger      Logger
	MaxBodySize int64
}

func NewHttpDecoderImpl(codec Codec, logger Logger, maxBody int64) *HttpDecoderImpl {
	return &HttpDecoderImpl{Codec: codec, Logger: logger, MaxBodySize: maxBody}
}

func (r *HttpDecoderImpl) Decode(req *http.Request, params Params) (Message, error) {
	var contentType = req.Header.Get("Content-Type")
	var contentTypeLower = strings.ToLower(contentType)

	if r.MaxBodySize > 0 {
		req.Body = MaxBytesReader(req.Body, r.MaxBodySize)
	}

	var (
		topic              = T(req.URL.Path)
		fromAddr           = req.RemoteAddr
		requestForm        = req.Form
		requestContentType = contentType
		requestPath        = req.URL.Path
		requestQuery       = req.URL.Query()
		requestHeaders     = Header(req.Header.Clone())
		requestCookies     = ReadCookies(Header(req.Header), "")
	)

	// if it's a multipart form, get multi-part reader: multipart/form-data or a multipart/mixed
	if strings.Contains(contentTypeLower, "multipart/form-data") || strings.Contains(contentTypeLower, "multipart/mixed") {
		var reader, getReaderErr = req.MultipartReader()
		if getReaderErr != nil {
			return Message{}, nerror.WrapOnly(getReaderErr)
		}

		var (
			endId  = nxid.New()
			partId = nxid.New()
		)

		var messages = make([]Message, 0, 3)
		var readBuffer bytes.Buffer
		for {
			var part, partErr = reader.NextPart()
			if partErr == io.EOF {
				break
			}
			if partErr != nil {
				return Message{}, nerror.WrapOnly(partErr)
			}

			readBuffer.Reset()
			var _, readErr = readBuffer.ReadFrom(part)
			if readErr != nil {
				return Message{}, nerror.WrapOnly(readErr)
			}

			var messageBytes = make([]byte, readBuffer.Len())
			_ = copy(messageBytes, readBuffer.Bytes())

			messages = append(messages, Message{
				Id:          nxid.New(),
				Topic:       topic,
				PartId:      partId,
				EndPartId:   endId,
				FromAddr:    fromAddr,
				Bytes:       messageBytes,
				Params:      params,
				FileName:    part.FileName(),
				FormName:    part.FileName(),
				Path:        requestPath,
				Headers:     requestHeaders,
				Cookies:     requestCookies,
				Query:       requestQuery,
				Form:        requestForm,
				ContentType: requestContentType,
				Metadata:    map[string]string{},
			})
		}

		messages = append(messages, Message{
			Id:          endId,
			Topic:       topic,
			PartId:      partId,
			EndPartId:   endId,
			FromAddr:    fromAddr,
			Params:      params,
			Path:        requestPath,
			Headers:     requestHeaders,
			Cookies:     requestCookies,
			Query:       requestQuery,
			Form:        requestForm,
			ContentType: requestContentType,
			Metadata:    map[string]string{},
		})

		var firstMessage = messages[0]
		firstMessage.Parts = messages[1:]
		return firstMessage, nil
	}

	if strings.Contains(contentTypeLower, "application/x-www-form-urlencoded") ||
		strings.Contains(contentTypeLower, "form-urlencoded") {
		if err := req.ParseForm(); err != nil {
			return Message{}, nerror.WrapOnly(err)
		}

		return Message{
			Id:          nxid.New(),
			Topic:       topic,
			FromAddr:    fromAddr,
			Path:        requestPath,
			Headers:     requestHeaders,
			Cookies:     requestCookies,
			Query:       requestQuery,
			Form:        requestForm,
			ContentType: requestContentType,
			Metadata:    map[string]string{},
			Params:      params,
		}, nil
	}

	var content bytes.Buffer
	if _, err := io.Copy(&content, req.Body); err != nil {
		return Message{}, nerror.WrapOnly(err)
	}

	// if its not an explicit application/x-event-message type then we
	// assume the body is the message payload.
	if !strings.Contains(contentTypeLower, MessageContentType) {
		return Message{
			Id:          nxid.New(),
			Topic:       topic,
			FromAddr:    fromAddr,
			Bytes:       content.Bytes(),
			Path:        requestPath,
			Headers:     requestHeaders,
			Cookies:     requestCookies,
			Query:       requestQuery,
			Form:        requestForm,
			ContentType: requestContentType,
			Metadata:    map[string]string{},
			Params:      params,
		}, nil
	}

	var message, messageErr = r.Codec.Decode(content.Bytes())
	if messageErr != nil {
		return Message{}, nerror.WrapOnly(messageErr)
	}

	message.Cookies = requestCookies
	message.Headers = requestHeaders

	if len(message.ContentType) == 0 {
		message.ContentType = contentType
	}

	if len(message.Path) == 0 {
		message.Path = req.URL.Path
	}

	if message.Params == nil {
		message.Params = params
	} else {
		for k, v := range params {
			message.Params[k] = v
		}
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
