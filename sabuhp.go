package sabuhp

import (
	"net/http"
	"time"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg/njson"
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

func (h Params) Get(k string) string {
	return h[k]
}

func (h Params) Set(k string, v string) {
	h[k] = v
}

func (h Params) Delete(k string) {
	delete(h, k)
}

type TransportResponse interface {
	Handle(*Message, Transport) MessageErr
}

type TransportResponseFunc func(*Message, Transport) MessageErr

func (t TransportResponseFunc) Handle(message *Message, tr Transport) MessageErr {
	return t(message, tr)
}

// Transpose transforms a http request into a Message to be
// delivered.
type Transpose func(req *http.Request, params Params) (*Message, error)

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

// Transport defines what an underline transport system provides.
type Transport interface {
	Conn() Conn
	Listen(topic string, handler TransportResponse) Channel
	SendToOne(data *Message, timeout time.Duration) error
	SendToAll(data *Message, timeout time.Duration) error
}

type LogHandler func([]*Message)

type MessageLog interface {
	Clear(owner nxid.ID) error
	Persist(owner nxid.ID, message *Message) error
	CatchUp(owner nxid.ID, lastId nxid.ID, handler Handler) error
}

type MessageRouter interface {
	TransportResponse
	Matcher
}
