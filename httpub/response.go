package httpub

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/influx6/npkg"

	"github.com/ewe-studios/sabuhp"

	"github.com/influx6/npkg/njson"
)

// CreateBody writes giving json content into the buffer.
func CreateBody(jsn *njson.JSON) (bytes.Buffer, error) {
	var bu bytes.Buffer
	var _, err = jsn.WriteTo(&bu)
	return bu, err
}

func CreateError(err error, message string, code int) (bytes.Buffer, error) {
	return CreateBody(njson.JSONB(func(event npkg.Encoder) {
		event.ObjectFor("error", func(encoder npkg.ObjectEncoder) {
			encoder.Int("code", code)
			encoder.String("message", message)
			encoder.String("details", err.Error())
		})
	}))
}

func HTTPRequestToRequest(req *http.Request) *Request {
	var headers = sabuhp.Header(req.Header)
	return &Request{
		Host:          req.Host,
		Form:          req.Form,
		PostForm:      req.PostForm,
		MultipartForm: req.MultipartForm,
		Proto:         req.Proto,
		IP:            req.RemoteAddr,
		URL:           req.URL,
		TLS:           req.TLS != nil,
		Method:        req.Method,
		Headers:       headers,
		Body:          req.Body,
		Req:           req,
		Cookies:       sabuhp.ReadCookies(headers, ""),
	}
}

func NewRequest(addr string, method string, body io.ReadCloser) (*Request, error) {
	var reqURL, reqErr = url.Parse(addr)
	if reqErr != nil {
		return nil, reqErr
	}

	return &Request{
		Headers: sabuhp.Header{},
		Host:    reqURL.Host,
		URL:     reqURL,
		Method:  method,
		Body:    body,
	}, nil
}

type (
	// RequestHandler is the matcher and handler link interface.
	// It is used inside the `Mux` to handle requests based on end-developer's custom logic.
	// If a "Matcher" passed then the "Handler" is executing and the rest of the Mux' routes will be ignored.
	RequestHandler interface {
		Handler
		Matcher
	}

	simpleRequestHandler struct {
		Handler
		Matcher
	}

	// Matcher is the interface that all Matchers should be implemented
	// in order to be registered into the Mux via the `Mux#AddRequestHandler/Match/MatchFunc` functions.
	//
	// Look the `Mux#AddRequestHandler` for more.
	Matcher interface {
		Match(*Request) bool
	}

	// MatcherFunc is a shortcut of the Matcher, as a function.
	// See `Matcher`.
	MatcherFunc func(*Request) bool
)

// Match returns the result of the "fn" matcher.
// Implementing the `Matcher` interface.
func (fn MatcherFunc) Match(r *Request) bool {
	return fn(r)
}

// Host is a Matcher for hostlines.
// It can accept exact hosts line like "mysubdomain.localhost:8080"
// or a suffix, i.e ".localhost:8080" will work as a wildcard subdomain for our root domain.
// The domain and the port should match exactly the request's data.
type Host string

// Match validates the host, implementing the `Matcher` interface.
func (h Host) Match(r *Request) bool {
	s := string(h)
	return r.Host == s || (s[0] == '.' && strings.HasSuffix(r.Host, s)) || s == WildcardParamStart
}

// Methods returns a MethodHandler which caller can use
// to register handler for specific HTTP Methods inside the `Mux#UseHandle/UseHandleFunc`.
// Usage:
// mux := muxie.NewMux()
// mux.UseHandle("/user/:id", muxie.Methods().
//     UseHandle("GET", getUserHandler).
//     UseHandle("POST", saveUserHandler))
func Methods() *MethodHandler {
	//
	// Design notes, the latest one is selected:
	//
	// mux := muxie.NewMux()
	//
	// 1. mux.UseHandle("/user/:id", muxie.ByMethod("GET", getHandler).And/AndFunc("POST", postHandlerFunc))
	//
	// 2. mux.UseHandle("/user/:id", muxie.ByMethods{
	// 	  "GET": getHandler,
	// 	  "POST" HandlerFunc(postHandlerFunc),
	//   }) <- the only downside of this is that
	// we lose the "Allow" header, which is not so important but it is RCF so we have to follow it.
	//
	// 3. mux.UseHandle("/user/:id", muxie.Method("GET", getUserHandler).Method("POST", saveUserHandler))
	//
	// 4. mux.UseHandle("/user/:id", muxie.Methods().
	//      UseHandle("GET", getHandler).
	//      UseHandleFunc("POST", postHandler))
	//
	return &MethodHandler{handlers: make(map[string]Handler)}
}

// NoContentHandler defaults to a handler which just sends 204 status.
// See `MethodHandler.NoContent` method.
var NoContentHandler Handler = HandlerFunc(func(r *Request, p sabuhp.Params) Response {
	var res Response
	res.Headers = sabuhp.Header{}
	res.Code = http.StatusNoContent
	return res
})

// MethodHandler implements the `Handler` which can be used on `Mux#UseHandle/UseHandleFunc`
// to declare handlers responsible for specific HTTP method(s).
//
// Look `UseHandle` and `UseHandleFunc`.
type MethodHandler struct {
	// origin *Mux

	handlers          map[string]Handler // method:handler
	methodsAllowedStr string
}

// Handle adds a handler to be responsible for a specific HTTP Method.
// Returns this MethodHandler for further calls.
// Usage:
// Handle("GET", myGetHandler).UseHandleFunc("DELETE", func(w http.ResponseWriter, r *Request){[...]})
// Handle("POST, PUT", saveOrUpdateHandler)
//        ^ can accept many methods for the same handler
//        ^ methods should be separated by comma, comma following by a space or just space
func (m *MethodHandler) Handle(method string, handler Handler) *MethodHandler {
	multiMethods := strings.FieldsFunc(method, func(c rune) bool {
		return c == ',' || c == ' '
	})

	if len(multiMethods) > 1 {
		for _, method := range multiMethods {
			m.Handle(method, handler)
		}

		return m
	}

	method = normalizeMethod(method)

	if m.methodsAllowedStr == "" {
		m.methodsAllowedStr = method
	} else {
		m.methodsAllowedStr += ", " + method
	}

	m.handlers[method] = handler

	return m
}

// NoContent registers a handler to a method
// which sends 204 (no status content) to the client.
//
// Example: _examples/11_cors for more.
func (m *MethodHandler) NoContent(methods ...string) *MethodHandler {
	for _, method := range methods {
		m.handlers[normalizeMethod(method)] = NoContentHandler
	}

	return m
}

// HandleFunc adds a handler function to be responsible for a specific HTTP Method.
// Returns this MethodHandler for further calls.
func (m *MethodHandler) HandleFunc(method string, handlerFunc func(r *Request, p sabuhp.Params) Response) *MethodHandler {
	m.Handle(method, HandlerFunc(handlerFunc))
	return m
}

func (m *MethodHandler) ServeHTTP(r *Request) Handler {
	if handler, ok := m.handlers[r.Method]; ok {
		return handler
	}

	// RCF rfc2616 https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
	// The response MUST include an Allow header containing a list of valid methods for the requested resource.
	//
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Allow#Examples
	return HandlerFunc(func(req *Request, p sabuhp.Params) Response {
		var w Response
		w.Headers = sabuhp.Header{}
		w.Headers.Set("Allow", m.methodsAllowedStr)
		w.Code = http.StatusMethodNotAllowed
		w.Body = NewBufferCloser(bytes.NewBufferString(http.StatusText(http.StatusMethodNotAllowed)))
		return w
	})
}

func normalizeMethod(method string) string {
	return strings.ToUpper(strings.TrimSpace(method))
}

var (
	// Charset is the default content type charset for Request Processors .
	Charset = "utf-8"

	// JSON implements the full `Processor` interface.
	// It is responsible to dispatch JSON results to the client and to read JSON
	// data from the request body.
	//
	// Usage:
	// To read from a request:
	// muxie.Bind(r, muxie.JSON, &myStructValue)
	// To send a response:
	// muxie.Dispatch(w, muxie.JSON, mySendDataValue)
	JSON = &jsonProcessor{Prefix: nil, Indent: "", UnescapeHTML: false}

	// XML implements the full `Processor` interface.
	// It is responsible to dispatch XML results to the client and to read XML
	// data from the request body.
	//
	// Usage:
	// To read from a request:
	// muxie.Bind(r, muxie.XML, &myStructValue)
	// To send a response:
	// muxie.Dispatch(w, muxie.XML, mySendDataValue)
	XML = &xmlProcessor{Indent: ""}
)

func withCharset(cType string) string {
	return cType + "; charset=" + Charset
}

// Binder is the interface which `muxie.Bind` expects.
// It is used to bind a request to a go struct value (ptr).
type Binder interface {
	Bind(*Request, interface{}) error
}

// Bind accepts the current request and any `Binder` to bind
// the request data to the "ptrOut".
func Bind(r *Request, b Binder, ptrOut interface{}) error {
	return b.Bind(r, ptrOut)
}

// Dispatcher is the interface which `muxie.Dispatch` expects.
// It is used to send a response based on a go struct value.
type Dispatcher interface {
	// no io.Writer because we need to set the headers here,
	// Binder and Processor are only for HTTP.
	Dispatch(http.ResponseWriter, interface{}) error
}

// Dispatch accepts the current response writer and any `Dispatcher`
// to send the "v" to the client.
func Dispatch(w http.ResponseWriter, d Dispatcher, v interface{}) error {
	return d.Dispatch(w, v)
}

// Processor implements both `Binder` and `Dispatcher` interfaces.
// It is used for implementations that can `Bind` and `Dispatch`
// the same data form.
//
// Look `JSON` and `XML` for more.
type Processor interface {
	Binder
	Dispatcher
}

var (
	newLineB byte = '\n'
	// the html codes for unescaping
	ltHex = []byte("\\u003c")
	lt    = []byte("<")

	gtHex = []byte("\\u003e")
	gt    = []byte(">")

	andHex = []byte("\\u0026")
	and    = []byte("&")
)

type jsonProcessor struct {
	Prefix       []byte
	Indent       string
	UnescapeHTML bool
}

var _ Processor = (*jsonProcessor)(nil)

func (p *jsonProcessor) Bind(r *Request, v interface{}) error {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}

func (p *jsonProcessor) Dispatch(w http.ResponseWriter, v interface{}) error {
	var (
		result []byte
		err    error
	)

	if indent := p.Indent; indent != "" {
		marshalIndent := json.MarshalIndent

		result, err = marshalIndent(v, "", indent)
		result = append(result, newLineB)
	} else {
		marshal := json.Marshal
		result, err = marshal(v)
	}

	if err != nil {
		return err
	}

	if p.UnescapeHTML {
		result = bytes.Replace(result, ltHex, lt, -1)
		result = bytes.Replace(result, gtHex, gt, -1)
		result = bytes.Replace(result, andHex, and, -1)
	}

	if len(p.Prefix) > 0 {
		result = append([]byte(p.Prefix), result...)
	}

	w.Header().Set("Content-Type", withCharset("application/json"))
	_, err = w.Write(result)
	return err
}

type xmlProcessor struct {
	Indent string
}

var _ Processor = (*xmlProcessor)(nil)

func (p *xmlProcessor) Bind(r *Request, v interface{}) error {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	return xml.Unmarshal(b, v)
}

func (p *xmlProcessor) Dispatch(w http.ResponseWriter, v interface{}) error {
	var (
		result []byte
		err    error
	)

	if indent := p.Indent; indent != "" {
		marshalIndent := xml.MarshalIndent

		result, err = marshalIndent(v, "", indent)
		result = append(result, newLineB)
	} else {
		marshal := xml.Marshal
		result, err = marshal(v)
	}

	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", withCharset("text/xml"))
	_, err = w.Write(result)
	return err
}

// MatchableContextHandler defines a condition which matches expected error
// for performing giving action.
type MatchableContextHandler interface {
	Match(error) bool
	Handle(r *Request, params sabuhp.Params) Response
}

// Matchable returns MatchableContextHandler using provided arguments.
func Matchable(err error, fn Handler) MatchableContextHandler {
	return &errorConditionImpl{
		Err: err,
		Fn:  fn,
	}
}

// errorConditionImpl defines a type which sets the error that occurs and the handler to be called
// for such an error.
type errorConditionImpl struct {
	Err error
	Fn  Handler
}

// Handler calls the internal http.Handler with provided Ctx returning error.
func (ec errorConditionImpl) Handle(r *Request, params sabuhp.Params) Response {
	return ec.Fn.Handle(r, params)
}

// Match validates the provided error matches expected error.
func (ec errorConditionImpl) Match(err error) bool {
	return ec.Err == err
}

// MatchableFunction returns MatchableContextHandler using provided arguments.
func MatchableFunction(err func(error) bool, fn Handler) MatchableContextHandler {
	return fnErrorCondition{
		Err: err,
		Fn:  fn,
	}
}

// fnErrorCondition defines a type which sets the error that occurs and the handler to be called
// for such an error.
type fnErrorCondition struct {
	Fn  Handler
	Err func(error) bool
}

// http.Handler calls the internal http.Handler with provided Ctx returning error.
func (ec fnErrorCondition) Handle(r *Request, params sabuhp.Params) Response {
	return ec.Fn.Handle(r, params)
}

// Match validates the provided error matches expected error.
func (ec fnErrorCondition) Match(err error) bool {
	return ec.Err(err)
}
