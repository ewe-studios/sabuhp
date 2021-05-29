package sabuhp

import (
	"context"
	"net/http"
	"time"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nnet"
)

type RetryFunc func(last int) time.Duration

type Logger interface {
	njson.Logger
}

// Channel represents a generated subscription on a
// topic which provides the giving callback an handler
// to define the point at which the channel should be
// closed and stopped from receiving updates.
type Channel interface {
	Topic() string
	Group() string
	Close()
	Err() error
}

// ErrChannel implements the Channel interface
// but has servers purpose to always return an error.
type ErrChannel struct{ Error error }

func (n *ErrChannel) Close()     {}
func (n *ErrChannel) Err() error { return n.Error }

// NoopChannel implements the Channel interface
// but has no internal operational capacity.
// It represents a non functioning channel.
type NoopChannel struct{}

func (n NoopChannel) Close()     {}
func (n NoopChannel) Err() error { return nil }

type MessageErr interface {
	error
	ShouldAck() bool
}

func WrapErr(err error, shouldAck bool) MessageErr {
	return messageErr{
		error:     err,
		shouldAck: shouldAck,
	}
}

type messageErr struct {
	error
	shouldAck bool
}

func (m messageErr) ShouldAck() bool {
	return m.shouldAck
}

// Conn defines the connection type which we can retrieve
// and understand the type.
type Conn interface{}

type Params map[string]string

func (h Params) EncodeObject(encoder npkg.ObjectEncoder) {
	for key, value := range h {
		encoder.String(key, value)
	}
}

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

// BytesDecoder transforms a http request into a Message to be
// delivered.
type BytesDecoder interface {
	Decode([]byte) (*Message, error)
}

// Matcher is the interface that all Matchers should be implemented
// in order to be registered into the Mux via the `Mux#AddRequestHandler/Match/MatchFunc` functions.
//
// Look the `Mux#AddRequestHandler` for more.
type Matcher interface {
	Match(message *Message) bool
}

type Handler interface {
	Handle(http.ResponseWriter, *http.Request, Params)
}

var _ Handler = (*HandlerFunc)(nil)

type HandlerFunc func(http.ResponseWriter, *http.Request, Params)

func (h HandlerFunc) Handle(rw http.ResponseWriter, r *http.Request, p Params) {
	h(rw, r, p)
}

// HttpMatcher embodies a matcher which indicates if the
// request exclusively belongs to it, hence allowing it
// to divert a giving request to itself.
type HttpMatcher interface {
	Handler

	Match(http.ResponseWriter, *http.Request, Params)
}

type Transport struct {
	Bus    MessageBus
	Socket Socket
}

type TransportResponse interface {
	Handle(context.Context, Message, Transport) MessageErr
}

type TransportResponseFunc func(context.Context, Message, Transport) MessageErr

func (t TransportResponseFunc) Handle(ctx context.Context, message Message, tr Transport) MessageErr {
	return t(ctx, message, tr)
}

// MessageBus defines what an underline message transport implementation
// like a message bus or rpc connection that can deliver according to
// required semantics of one-to-one and one-to-many.
type MessageBus interface {
	Send(data ...Message)
	Listen(topic string, grp string, handler TransportResponse) Channel
}

type (
	// Wrapper is just a type of `func(TransportResponse) TransportResponse`
	// which is a common type definition for net/http middlewares.
	Wrapper func(response TransportResponse) TransportResponse

	// Wrappers contains `Wrapper`s that can be registered and used by a "main route handler".
	// Look the `Pre` and `For/ForFunc` functions too.
	Wrappers []Wrapper

	HttpWrapper func(Handler) Handler

	HttpWrappers []HttpWrapper
)

// For registers the wrappers for a specific handler and returns a handler
// that can be passed via the `UseHandle` function.
func (w HttpWrappers) For(main Handler) Handler {
	if len(w) > 0 {
		for i, lidx := 0, len(w)-1; i <= lidx; i++ {
			main = w[lidx-i](main)
		}
	}

	return main
}

// ForFunc registers the wrappers for a specific raw handler function
// and returns a handler that can be passed via the `UseHandle` function.
func (w HttpWrappers) ForFunc(mainFunc HandlerFunc) Handler {
	return w.For(mainFunc)
}

// For registers the wrappers for a specific handler and returns a handler
// that can be passed via the `UseHandle` function.
func (w Wrappers) For(main TransportResponse) TransportResponse {
	if len(w) > 0 {
		for i, lidx := 0, len(w)-1; i <= lidx; i++ {
			main = w[lidx-i](main)
		}
	}

	return main
}

// ForFunc registers the wrappers for a specific raw handler function
// and returns a handler that can be passed via the `UseHandle` function.
func (w Wrappers) ForFunc(mainFunc TransportResponseFunc) TransportResponse {
	return w.For(mainFunc)
}

type LocationService interface {
	Get(ipAddress string) (nnet.Location, error)
}

// Pre starts a chain of handlers for wrapping a "main route handler"
// the registered "middleware" will run before the main handler(see `Wrappers#For/ForFunc`).
//
// Usage:
// mux := muxie.NewMux()
// myMiddlewares :=  muxie.Pre(myFirstMiddleware, mySecondMiddleware)
// mux.UseHandle("/", myMiddlewares.ForFunc(myMainRouteTransportResponse))
func Pre(middleware ...Wrapper) Wrappers {
	return Wrappers(middleware)
}

type LogHandler func([]*Message)

type MessageRouter interface {
	TransportResponse
	Matcher
}
