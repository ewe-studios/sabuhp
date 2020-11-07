package httpub

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
)

// Transport defines what we expect from a handler of requests.
// It will be responsible for the serialization of request to server and
// delivery of response or error from server.
type Transport interface {
	Send(ctx context.Context, request *Request) (Response, error)
}

type Params map[string]string

func (h Params) Get(k string) string {
	return h[k]
}

func (h Params) Set(k string, v string) {
	h[k] = v
}

func (h Params) Delete(k string) {
	delete(h, k)
}

type Header map[string][]string

func (h Header) Get(k string) string {
	if values, ok := h[k]; ok {
		return values[0]
	}
	return ""
}

func (h Header) Values(k string) []string {
	return h[k]
}

func (h Header) Add(k string, v string) {
	h[k] = append(h[k], v)
}

func (h Header) Set(k string, v string) {
	h[k] = append([]string{v}, v)
}

func (h Header) Delete(k string) {
	delete(h, k)
}

type Handler interface {
	Handle(req *Request, params Params) Response
}

type HandlerFunc func(req *Request, params Params) Response

func (h HandlerFunc) Handle(req *Request, params Params) Response {
	return h(req, params)
}

// Request implements a underline carrier of a request object which will be used
// by a transport to request giving resource.
type Request struct {
	Proto            string // "HTTP/1.0"
	TransferEncoding []string
	Host             string
	Form             url.Values
	PostForm         url.Values
	MultipartForm    *multipart.Form
	IP               string
	TLS              bool
	Method           string
	URL              *url.URL
	Cookies          []Cookie
	Body             io.ReadCloser
	Headers          Header
	Conn             net.Conn
	Req              *http.Request
}

// Response is an implementation of http.ResponseWriter that
// records its mutations for later inspection in tests.
type Response struct {
	// RedirectURL is used to indicate the url to redirect to
	// if we are doing a Temporary or Permanent redirect response.
	// It's usually should be null unless its a redirect.
	RedirectURL *url.URL

	// Code is the HTTP response code set by WriteHeader.
	//
	// Note that if a Handler never calls WriteHeader or Write,
	// this might end up being 0, rather than the implicit
	// http.StatusOK. To get the implicit value, use the Result
	// method.
	Code int

	// Headers the headers explicitly set by the Handler.
	Headers Header

	// Cookies contains the cookies to be written as part of response.
	Cookies []Cookie

	// Body is the buffer to which the Handler's Write calls are sent.
	// If nil, the Writes are silently discarded.
	Body *bytes.Buffer

	// Flushed is whether the Handler called Flush.
	Flushed bool

	// Err object attached to response.
	Err error
}

// NewResponse returns an initialized Response.
func NewResponse() *Response {
	return &Response{
		Headers: make(map[string][]string),
		Body:    new(bytes.Buffer),
		Code:    0,
	}
}

// Header returns the response headers.
func (rw *Response) Header() map[string][]string {
	m := rw.Headers
	if m == nil {
		m = make(map[string][]string)
		rw.Headers = m
	}
	return m
}

// Write always succeeds and writes to rw.Body, if not nil.
func (rw *Response) Write(buf []byte) (int, error) {
	if rw.Body != nil {
		rw.Body.Write(buf)
	}
	return len(buf), nil
}

// WriteString always succeeds and writes to rw.Body, if not nil.
func (rw *Response) WriteString(str string) (int, error) {
	if rw.Body != nil {
		rw.Body.WriteString(str)
	}
	return len(str), nil
}

// Service defines what we expect from a service server which is responsible for the
// handling of incoming requests for a giving service type and the response for that giving
// request.
type Service interface {
	Serve(w *Response, r *Request)
}
