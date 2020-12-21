package httpub

import (
	"bytes"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/influx6/sabuhp"
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
func (w Wrappers) ForFunc(mainFunc func(*Request, sabuhp.Params) Response) Handler {
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
func (m *Mux) UseHandleFunc(pattern string, handlerFunc func(*Request, sabuhp.Params) Response) {
	m.UseHandle(pattern, HandlerFunc(handlerFunc))
}

// Serve exposes and serves the registered routes.
func (m *Mux) Handle(r *Request) Response {
	var params = sabuhp.Params{}
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
		Headers: sabuhp.Header{},
	}
}

// SubMux is the child of a main Mux.
type SubMux interface {
	Of(prefix string) SubMux
	Unlink() SubMux
	Use(middlewares ...Wrapper)
	UseHandle(pattern string, handler Handler)
	AbsPath() string
	UseHandleFunc(pattern string, handlerFunc func(*Request, sabuhp.Params) Response)
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
	var h = sabuhp.Header{}
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
		Body:    NewBufferCloser(bytes.NewBufferString(content)),
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

// Node is the trie's node which path patterns with their data like an HTTP handler are saved to.
// See `Trie` too.
type Node struct {
	parent *Node

	children               map[string]*Node
	hasDynamicChild        bool // does one of the children contains a parameter or wildcard?
	childNamedParameter    bool // is the child a named parameter (single segmnet)
	childWildcardParameter bool // or it is a wildcard (can be more than one path segments) ?

	paramKeys []string // the param keys without : or *.
	end       bool     // it is a complete node, here we stop and we can say that the node is valid.
	key       string   // if end == true then key is filled with the original value of the insertion's key.
	// if key != "" && its parent has childWildcardParameter == true,
	// we need it to track the static part for the closest-wildcard's parameter storage.
	staticKey string

	// insert main data relative to http and a tag for things like route names.
	Handler Handler
	Tag     string

	// other insert data.
	Data interface{}
}

// NewNode returns a new, empty, Node.
func NewNode() *Node {
	n := new(Node)
	return n
}

func (n *Node) addChild(s string, child *Node) {
	if n.children == nil {
		n.children = make(map[string]*Node)
	}

	if _, exists := n.children[s]; exists {
		return
	}

	child.parent = n
	n.children[s] = child
}

func (n *Node) getChild(s string) *Node {
	if n.children == nil {
		return nil
	}

	return n.children[s]
}

func (n *Node) hasChild(s string) bool {
	return n.getChild(s) != nil
}

func (n *Node) findClosestParentWildcardNode() *Node {
	n = n.parent
	for n != nil {
		if n.childWildcardParameter {
			return n.getChild(WildcardParamStart)
		}

		n = n.parent
	}

	return nil
}

// NodeKeysSorter is the type definition for the sorting logic
// that caller can pass on `GetKeys` and `Autocomplete`.
type NodeKeysSorter = func(list []string) func(i, j int) bool

// DefaultKeysSorter sorts as: first the "key (the path)" with the lowest number of slashes.
var DefaultKeysSorter = func(list []string) func(i, j int) bool {
	return func(i, j int) bool {
		return len(strings.Split(list[i], pathSep)) < len(strings.Split(list[j], pathSep))
	}
}

// Keys returns this node's key (if it's a final path segment)
// and its children's node's key. The "sorter" can be optionally used to sort the result.
func (n *Node) Keys(sorter NodeKeysSorter) (list []string) {
	if n == nil {
		return
	}

	if n.end {
		list = append(list, n.key)
	}

	if n.children != nil {
		for _, child := range n.children {
			list = append(list, child.Keys(sorter)...)
		}
	}

	if sorter != nil {
		sort.Slice(list, sorter(list))
	}

	return
}

// Parent returns the parent of that node, can return nil if this is the root node.
func (n *Node) Parent() *Node {
	return n.parent
}

// String returns the key, which is the path pattern for the HTTP Mux.
func (n *Node) String() string {
	return n.key
}

// IsEnd returns true if this Node is a final path, has a key.
func (n *Node) IsEnd() bool {
	return n.end
}

const (
	// ParamStart is the character, as a string, which a path pattern starts to define its named parameter.
	ParamStart = ":"
	// WildcardParamStart is the character, as a string, which a path pattern starts to define its named parameter for wildcards.
	// It allows everything else after that path prefix
	// but the Trie checks for static paths and named parameters before that in order to support everything that other implementations do not,
	// and if nothing else found then it tries to find the closest wildcard path(super and unique).
	WildcardParamStart = "*"
)

// Trie contains the main logic for adding and searching nodes for path segments.
// It supports wildcard and named path parameters.
// Trie supports very coblex and useful path patterns for routes.
// The Trie checks for static paths(path without : or *) and named parameters before that in order to support everything that other implementations do not,
// and if nothing else found then it tries to find the closest wildcard path(super and unique).
type Trie struct {
	root *Node

	// if true then it will handle any path if not other parent wildcard exists,
	// so even 404 (on http services) is up to it, see Trie#Insert.
	hasRootWildcard bool

	hasRootSlash bool
}

// NewTrie returns a new, empty Trie.
// It is only useful for end-developers that want to design their own mux/router based on my trie implementation.
//
// See `Trie`
func NewTrie() *Trie {
	return &Trie{
		root:            NewNode(),
		hasRootWildcard: false,
	}
}

// InsertOption is just a function which accepts a pointer to a Node which can alt its `Handler`, `Tag` and `Data`  fields.
//
// See `WithHandler`, `WithTag` and `WithData`.
type InsertOption func(*Node)

// WithHandler sets the node's `Handler` field (useful for HTTP).
func WithHandler(handler Handler) InsertOption {
	if handler == nil {
		panic("muxie/WithHandler: empty handler")
	}

	return func(n *Node) {
		if n.Handler == nil {
			n.Handler = handler
		}
	}
}

// WithTag sets the node's `Tag` field (may be useful for HTTP).
func WithTag(tag string) InsertOption {
	return func(n *Node) {
		if n.Tag == "" {
			n.Tag = tag
		}
	}
}

// WithData sets the node's optionally `Data` field.
func WithData(data interface{}) InsertOption {
	return func(n *Node) {
		// data can be replaced.
		n.Data = data
	}
}

// Insert adds a node to the trie.
func (t *Trie) Insert(pattern string, options ...InsertOption) {
	if pattern == "" {
		panic("muxie/trie#Insert: empty pattern")
	}

	n := t.insert(pattern, "", nil, nil)
	for _, opt := range options {
		opt(n)
	}
}

const (
	pathSep  = "/"
	pathSepB = '/'
)

func slowPathSplit(path string) []string {
	if path == pathSep {
		return []string{pathSep}
	}

	// remove last sep if any.
	if path[len(path)-1] == pathSepB {
		path = path[:len(path)-1]
	}

	return strings.Split(path, pathSep)[1:]
}

func resolveStaticPart(key string) string {
	i := strings.Index(key, ParamStart)
	if i == -1 {
		i = strings.Index(key, WildcardParamStart)
	}
	if i == -1 {
		i = len(key)
	}

	return key[:i]
}

func (t *Trie) insert(key, tag string, optionalData interface{}, handler Handler) *Node {
	input := slowPathSplit(key)

	n := t.root
	if key == pathSep {
		t.hasRootSlash = true
	}

	var paramKeys []string

	for _, s := range input {
		c := s[0]

		if isParam, isWildcard := c == ParamStart[0], c == WildcardParamStart[0]; isParam || isWildcard {
			n.hasDynamicChild = true
			paramKeys = append(paramKeys, s[1:]) // without : or *.

			// if node has already a wildcard, don't force a value, check for true only.
			if isParam {
				n.childNamedParameter = true
				s = ParamStart
			}

			if isWildcard {
				n.childWildcardParameter = true
				s = WildcardParamStart
				if t.root == n {
					t.hasRootWildcard = true
				}
			}
		}

		if !n.hasChild(s) {
			child := NewNode()
			n.addChild(s, child)
		}

		n = n.getChild(s)
	}

	n.Tag = tag
	n.Handler = handler
	n.Data = optionalData

	n.paramKeys = paramKeys
	n.key = key
	n.staticKey = resolveStaticPart(key)
	n.end = true

	return n
}

// SearchPrefix returns the last node which holds the key which starts with "prefix".
func (t *Trie) SearchPrefix(prefix string) *Node {
	input := slowPathSplit(prefix)
	n := t.root

	for i := 0; i < len(input); i++ {
		s := input[i]
		if child := n.getChild(s); child != nil {
			n = child
			continue
		}

		return nil
	}

	return n
}

// Parents returns the list of nodes that a node with "prefix" key belongs to.
func (t *Trie) Parents(prefix string) (parents []*Node) {
	n := t.SearchPrefix(prefix)
	if n != nil {
		// without this node.
		n = n.Parent()
		for {
			if n == nil {
				break
			}

			if n.IsEnd() {
				parents = append(parents, n)
			}

			n = n.Parent()
		}
	}

	return
}

// HasPrefix returns true if "prefix" is found inside the registered nodes.
func (t *Trie) HasPrefix(prefix string) bool {
	return t.SearchPrefix(prefix) != nil
}

// Autocomplete returns the keys that starts with "prefix",
// this is useful for custom search-engines built on top of my trie implementation.
func (t *Trie) Autocomplete(prefix string, sorter NodeKeysSorter) (list []string) {
	n := t.SearchPrefix(prefix)
	if n != nil {
		list = n.Keys(sorter)
	}
	return
}

// ParamsSetter is the interface which should be implemented by the
// params writer for `Search` in order to store the found named path parameters, if any.
type ParamsSetter interface {
	Set(string, string)
}

// Search is the most important part of the Trie.
// It will try to find the responsible node for a specific query (or a request path for HTTP endpoints).
//
// Search supports searching for static paths(path without : or *) and paths that contain
// named parameters or wildcards.
// Priority as:
// 1. static paths
// 2. named parameters with ":"
// 3. wildcards
// 4. closest wildcard if not found, if any
// 5. root wildcard
func (t *Trie) Search(q string, params ParamsSetter) *Node {
	end := len(q)

	if end == 0 || (end == 1 && q[0] == pathSepB) {
		// fixes only root wildcard but no / registered at.
		if t.hasRootSlash {
			return t.root.getChild(pathSep)
		} else if t.hasRootWildcard {
			// no need to going through setting parameters, this one has not but it is wildcard.
			return t.root.getChild(WildcardParamStart)
		}

		return nil
	}

	n := t.root
	start := 1
	i := 1
	var paramValues []string

	for {
		if i == end || q[i] == pathSepB {
			if child := n.getChild(q[start:i]); child != nil {
				n = child
			} else if n.childNamedParameter { // && n.childWildcardParameter == false {
				n = n.getChild(ParamStart)
				if ln := len(paramValues); cap(paramValues) > ln {
					paramValues = paramValues[:ln+1]
					paramValues[ln] = q[start:i]
				} else {
					paramValues = append(paramValues, q[start:i])
				}
			} else if n.childWildcardParameter {
				n = n.getChild(WildcardParamStart)
				if ln := len(paramValues); cap(paramValues) > ln {
					paramValues = paramValues[:ln+1]
					paramValues[ln] = q[start:]
				} else {
					paramValues = append(paramValues, q[start:])
				}
				break
			} else {
				n = n.findClosestParentWildcardNode()
				if n != nil {
					// means that it has :param/static and *wildcard, we go trhough the :param
					// but the next path segment is not the /static, so go back to *wildcard
					// instead of not found.
					//
					// Fixes:
					// /hello/*p
					// /hello/:p1/static/:p2
					// req: http://localhost:8080/hello/dsadsa/static/dsadsa => found
					// req: http://localhost:8080/hello/dsadsa => but not found!
					// and
					// /second/wild/*p
					// /second/wild/static/otherstatic/
					// req: /second/wild/static/otherstatic/random => but not found!
					params.Set(n.paramKeys[0], q[len(n.staticKey):])
					return n
				}

				return nil
			}

			if i == end {
				break
			}

			i++
			start = i
			continue
		}

		i++
	}

	if n == nil || !n.end {
		if n != nil { // we need it on both places, on last segment (below) or on the first unnknown (above).
			if n = n.findClosestParentWildcardNode(); n != nil {
				params.Set(n.paramKeys[0], q[len(n.staticKey):])
				return n
			}
		}

		if t.hasRootWildcard {
			// that's the case for root wildcard, tests are passing
			// even without it but stick with it for reference.
			// Note ote that something like:
			// Routes: /other2/*myparam and /other2/static
			// Reqs: /other2/staticed will be handled
			// by the /other2/*myparam and not the root wildcard (see above), which is what we want.
			n = t.root.getChild(WildcardParamStart)
			params.Set(n.paramKeys[0], q[1:])
			return n
		}

		return nil
	}

	for i, paramValue := range paramValues {
		if len(n.paramKeys) > i {
			params.Set(n.paramKeys[i], paramValue)
		}
	}

	return n
}
