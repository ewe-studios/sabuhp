package sabuhp

import (
	"bytes"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Mux is an HTTP request multiplexer.
// It matches the URL of each incoming request against a list of registered
// nodes and calls the handler for the pattern that
// most closely matches the URL.
//
// Patterns name fixed, rooted paths and dynamic like /profile/:name
// or /profile/:name/friends or even /files/*file when ":name" and "*file"
// are the named parameters and wildcard parameters respectfully.
//
// Note that since a pattern ending in a slash names a rooted subtree,
// the pattern "/*myparam" matches all paths not matched by other registered
// patterns, but not the URL with Path == "/", for that you would need the pattern "/".
//
// See `NewMux`.
type Mux struct {
	// PathCorrection removes leading slashes from the request path.
	// Defaults to false, however is highly recommended to turn it on.
	PathCorrection bool

	// PathCorrectionNoRedirect if `PathCorrection` is set to true,
	// it will execute the handlers chain without redirection.
	// Defaults to false.
	PathCorrectionNoRedirect bool
	Routes                   *Trie

	// per mux
	root            string
	requestHandlers []RequestHandler
	beginHandlers   []Wrapper
}

// NewMux returns a new HTTP multiplexer which uses a fast, if not the fastest
// implementation of the trie data structure that is designed especially for path segments.
func NewMux() *Mux {
	return &Mux{
		Routes: NewTrie(),
		root:   "",
	}
}

// AddRequestHandler adds a full `RequestHandler` which is responsible
// to check if a handler should be executed via its `Matcher`,
// if the handler is executed
// then the router stops searching for this Mux' routes,
// RequestHandelrs have priority over the routes and the middlewares.
//
// The "requestHandler"'s Handler can be any Handler
// and a new `muxie.NewMux` as well. The new Mux will
// be not linked to this Mux by-default, if you want to share
// middlewares then you have to use the `muxie.Pre` to declare
// the shared middlewares and register them via the `Mux#Use` function.
func (m *Mux) AddRequestHandler(requestHandler RequestHandler) {
	m.requestHandlers = append(m.requestHandlers, requestHandler)
}

// HandleRequest adds a matcher and a (conditional) handler to be executed when "matcher" passed.
// If the "matcher" passed then the "handler" will be executed
// and this Mux' routes will be ignored.
//
// Look the `Mux#AddRequestHandler` for further details.
func (m *Mux) HandleRequest(matcher Matcher, handler Handler) {
	m.AddRequestHandler(&simpleRequestHandler{
		Matcher: matcher,
		Handler: handler,
	})
}

// Use adds middleware that should be called before each mux route's main handler.
// Should be called before `UseHandle/UseHandleFunc`. Order matters.
//
// A Wrapper is just a type of `func(Handler) Handler`
// which is a common type definition for net/http middlewares.
//
// To add a middleware for a specific route and not in the whole mux
// use the `UseHandle/UseHandleFunc` with the package-level `muxie.Pre` function instead.
// Functionality of `Use` is pretty self-explained but new gophers should
// take a look of the examples for further details.
func (m *Mux) Use(middlewares ...Wrapper) {
	m.beginHandlers = append(m.beginHandlers, middlewares...)
}

type (
	// Wrapper is just a type of `func(Handler) Handler`
	// which is a common type definition for net/http middlewares.
	Wrapper func(Handler) Handler

	// Wrappers contains `Wrapper`s that can be registered and used by a "main route handler".
	// Look the `Pre` and `For/ForFunc` functions too.
	Wrappers []Wrapper
)

// For registers the wrappers for a specific handler and returns a handler
// that can be passed via the `UseHandle` function.
func (w Wrappers) For(main Handler) Handler {
	if len(w) > 0 {
		for i, lidx := 0, len(w)-1; i <= lidx; i++ {
			main = w[lidx-i](main)
		}
	}

	return main
}

// ForFunc registers the wrappers for a specific raw handler function
// and returns a handler that can be passed via the `UseHandle` function.
func (w Wrappers) ForFunc(mainFunc func(*Request, Params) Response) Handler {
	return w.For(HandlerFunc(mainFunc))
}

// Pre starts a chain of handlers for wrapping a "main route handler"
// the registered "middleware" will run before the main handler(see `Wrappers#For/ForFunc`).
//
// Usage:
// mux := muxie.NewMux()
// myMiddlewares :=  muxie.Pre(myFirstMiddleware, mySecondMiddleware)
// mux.UseHandle("/", myMiddlewares.ForFunc(myMainRouteHandler))
func Pre(middleware ...Wrapper) Wrappers {
	return Wrappers(middleware)
}

// Handle registers a route handler for a path pattern.
func (m *Mux) UseHandle(pattern string, handler Handler) {
	m.Routes.Insert(m.root+pattern,
		WithHandler(
			Pre(m.beginHandlers...).For(handler)))
}

// HandleFunc registers a route handler function for a path pattern.
func (m *Mux) UseHandleFunc(pattern string, handlerFunc func(*Request, Params) Response) {
	m.UseHandle(pattern, HandlerFunc(handlerFunc))
}

// Serve exposes and serves the registered routes.
func (m *Mux) Handle(r *Request) Response {
	var params = Params{}
	for _, h := range m.requestHandlers {
		if h.Match(r) {
			return h.Handle(r, params)
		}
	}

	var reqPath = r.URL.Path
	if m.PathCorrection {
		if len(reqPath) > 1 && strings.HasSuffix(reqPath, "/") {
			// Remove trailing slash and client-permanent rule for redirection,
			// if confgiuration allows that and reqPath has an extra slash.

			// update the new reqPath and redirect.
			// use Trim to ensure there is no open redirect due to two leading slashes
			r.URL.Path = pathSep + strings.Trim(reqPath, pathSep)
			if !m.PathCorrectionNoRedirect {
				var targetURL = r.URL.String()
				method := r.Method
				// Fixes https://github.com/kataras/iris/issues/921
				// This is caused for security reasons, imagine a payment shop,
				// you can't just permanently redirect a POST request, so just 307 (RFC 7231, 6.4.7).
				if method == http.MethodPost || method == http.MethodPut {
					return redirect(r, targetURL, http.StatusTemporaryRedirect)
				}

				return redirect(r, targetURL, http.StatusTemporaryRedirect)
			}
		}
	}

	// r.URL.Query() is slow and will allocate a lot, although
	// the first idea was to not introduce a new type to the end-developers
	// so they are using this library as the std one, but we will have to do it
	// for the params, we keep that rule so a new ResponseWriter, which is an interface,
	// and it will be compatible with net/http will be introduced to store the params at least,
	// we don't want to add a third parameter or a global state to this library.

	n := m.Routes.Search(reqPath, params)
	if n != nil {
		return n.Handler.Handle(r, params)
	}

	return Response{
		Code:    http.StatusNotFound,
		Headers: Header{},
	}
}

// SubMux is the child of a main Mux.
type SubMux interface {
	Of(prefix string) SubMux
	Unlink() SubMux
	Use(middlewares ...Wrapper)
	UseHandle(pattern string, handler Handler)
	AbsPath() string
	UseHandleFunc(pattern string, handlerFunc func(*Request, Params) Response)
}

// Of returns a new Mux which its Handle and HandleFunc will register the path based on given "prefix", i.e:
// mux := NewMux()
// v1 := mux.Of("/v1")
// v1.UseHandleFunc("/users", myHandler)
// The above will register the "myHandler" to the "/v1/users" path pattern.
func (m *Mux) Of(prefix string) SubMux {
	if prefix == "" || prefix == pathSep {
		return m
	}

	if prefix == m.root {
		return m
	}

	// modify prefix if it's already there on the parent.
	if strings.HasPrefix(m.root, prefix) {
		prefix = prefix[0:strings.LastIndex(m.root, prefix)]
	}

	// remove last slash "/", if any.
	if lidx := len(prefix) - 1; prefix[lidx] == pathSepB {
		prefix = prefix[0:lidx]
	}

	// remove any duplication of slashes "/".
	prefix = pathSep + strings.Trim(m.root+prefix, pathSep)

	return &Mux{
		Routes: m.Routes,

		root:            prefix,
		requestHandlers: m.requestHandlers[0:],
		beginHandlers:   m.beginHandlers[0:],
	}
}

// AbsPath returns the absolute path of the router for this Mux group.
func (m *Mux) AbsPath() string {
	if m.root == "" {
		return "/"
	}
	return m.root
}

/* Notes:

Four options to solve optionally "inherition" of parent's middlewares but dismissed:

- I could add options for "inherition" of middlewares inside the `Mux#Use` itself.
  But this is a problem because the end-dev will have to use a specific muxie's constant even if he doesn't care about the other option.
- Create a new function like `UseOnly` or `UseExplicit`
  which will remove any previous middlewares and use only the new one.
  But this has a problem of making the `Use` func to act differently and debugging will be a bit difficult if big app if called after the `UseOnly`.
- Add a new func for creating new groups to remove any inherited middlewares from the parent.
  But with this, we will have two functions for the same thing and users may be confused about this API design.
- Put the options to the existing `Of` function, and make them optionally by functional design of options.
  But this will make things ugly and may confuse users as well, there is a better way.

Solution: just add a function like `Unlink`
to remove any inherited fields (now and future feature requests), so we don't have
breaking changes and etc. This `Unlink`, which will return the same SubMux, it can be used like `v1 := mux.Of(..).Unlink()`
*/

// Unlink will remove any inheritance fields from the parent mux (and its parent)
// that are inherited with the `Of` function.
// Returns the current SubMux. Usage:
//
// mux := NewMux()
// mux.Use(myLoggerMiddleware)
// v1 := mux.Of("/v1").Unlink() // v1 will no longer have the "myLoggerMiddleware" or any Matchers.
// v1.UseHandleFunc("/users", myHandler)
func (m *Mux) Unlink() SubMux {
	m.requestHandlers = m.requestHandlers[0:0]
	m.beginHandlers = m.beginHandlers[0:0]

	return m
}

func redirect(r *Request, uri string, code int) Response {
	var h = Header{}
	if u, err := url.Parse(uri); err == nil {
		// If url was relative, make its path absolute by
		// combining with request path.
		// The client would probably do this for us,
		// but doing it ourselves is more reliable.
		// See RFC 7231, section 7.1.2
		if u.Scheme == "" && u.Host == "" {
			oldpath := r.URL.Path
			if oldpath == "" { // should not happen, but avoid a crash if it does
				oldpath = "/"
			}

			// no leading http://server
			if uri == "" || uri[0] != '/' {
				// make relative path absolute
				olddir, _ := path.Split(oldpath)
				uri = olddir + uri
			}

			var query string
			if i := strings.Index(uri, "?"); i != -1 {
				uri, query = uri[:i], uri[i:]
			}

			// clean up but preserve trailing slash
			trailing := strings.HasSuffix(uri, "/")
			uri = path.Clean(uri)
			if trailing && !strings.HasSuffix(uri, "/") {
				uri += "/"
			}
			uri += query
		}
	}

	// RFC 7231 notes that a short HTML body is usually included in
	// the response because older user agents may not understand 301/307.
	// Do it only if the request didn't already have a Content-Type header.
	_, hadCT := h["Content-Type"]

	h.Set("Location", hexEscapeNonASCII(uri))
	if !hadCT && (r.Method == "GET" || r.Method == "HEAD") {
		h.Set("Content-Type", "text/html; charset=utf-8")
	}

	// Shouldn't send the body for POST or HEAD; that leaves GET.
	var content string
	if !hadCT && r.Method == "GET" {
		content = "<a href=\"" + htmlEscape(uri) + "\">" + http.StatusText(code) + "</a>.\n"
	}

	return Response{
		Headers: h,
		Code:    code,
		Body:    bytes.NewBufferString(content),
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// stringContainsCTLByte reports whether s contains any ASCII control character.
func stringContainsCTLByte(s string) bool {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < ' ' || b == 0x7f {
			return true
		}
	}
	return false
}

func hexEscapeNonASCII(s string) string {
	newLen := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			newLen += 3
		} else {
			newLen++
		}
	}
	if newLen == len(s) {
		return s
	}
	b := make([]byte, 0, newLen)
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			b = append(b, '%')
			b = strconv.AppendInt(b, int64(s[i]), 16)
		} else {
			b = append(b, s[i])
		}
	}
	return string(b)
}

var htmlReplacer = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	// "&#34;" is shorter than "&quot;".
	`"`, "&#34;",
	// "&#39;" is shorter than "&apos;" and apos was not in HTML until HTML5.
	"'", "&#39;",
)

func htmlEscape(s string) string {
	return htmlReplacer.Replace(s)
}
