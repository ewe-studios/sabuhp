package sabuhp

import (
	"bytes"
	"net/http"
	"strings"
)

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
var NoContentHandler Handler = HandlerFunc(func(r *Request, p Params) Response {
	var res Response
	res.Headers = Header{}
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
func (m *MethodHandler) HandleFunc(method string, handlerFunc func(r *Request, p Params) Response) *MethodHandler {
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
	return HandlerFunc(func(req *Request, p Params) Response {
		var w Response
		w.Headers = Header{}
		w.Headers.Set("Allow", m.methodsAllowedStr)
		w.Code = http.StatusMethodNotAllowed
		w.Body = bytes.NewBufferString(http.StatusText(http.StatusMethodNotAllowed))
		return w
	})
}

func normalizeMethod(method string) string {
	return strings.ToUpper(strings.TrimSpace(method))
}
