package radar

import (
	"context"
	"fmt"
	"github.com/ewe-studios/sabuhp/sabu"
	"net/http"
	"strings"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nerror"

	"github.com/ewe-studios/sabuhp/sockets/hsocks"
	"github.com/ewe-studios/sabuhp/utils"
)

type MuxConfig struct {
	RootPath string
	Bus      sabu.MessageBus
	Logger   sabu.Logger
	NotFound sabu.Handler
	Relay    *sabu.BusRelay
	Ctx      context.Context
	Decoder  sabu.HttpDecoder
	Encoder  sabu.HttpEncoder
	Headers  sabu.HeaderModifications
}

func (mc *MuxConfig) validate() {
	if mc.Logger == nil {
		panic("MuxConfig.Logger is required")
	}
	if mc.Decoder == nil {
		panic("MuxConfig.Decoder is required")
	}
	if mc.Encoder == nil {
		panic("MuxConfig.Encoder is required")
	}
	if mc.Bus == nil {
		panic("MuxConfig.Bus is required")
	}
	if mc.Relay == nil {
		panic("MuxConfig.BusRelay is required")
	}
}

// Mux is Request multiplexer.
// It matches an event name or http url pattern to
// a specific TransportHandler which will be registered
// to the provided transport for handling specific events.
type Mux struct {
	config       MuxConfig
	rootPath     string
	trie         *Trie
	logger       sabu.Logger
	NotFound     sabu.Handler
	subRoutes    []sabu.MessageRouter
	relay        *sabu.BusRelay
	pre          sabu.Wrappers
	preHttp      sabu.HttpWrappers
	httpToEvents *hsocks.HttpServlet
	routes       map[string]bool
}

func NewMux(config MuxConfig) *Mux {
	return &Mux{
		config:   config,
		rootPath: config.RootPath,
		relay:    config.Relay,
		trie:     NewTrie(),
		logger:   config.Logger,
		NotFound: config.NotFound,
		routes:   map[string]bool{},
		httpToEvents: hsocks.ManagedHttpServlet(
			config.Ctx,
			config.Logger,
			config.Decoder,
			config.Encoder,
			config.Headers,
			config.Bus,
		),
	}
}

// Routes returns all registered routes on the router.
func (m *Mux) Routes() []string {
	var routes = make([]string, 0, len(m.routes))
	for k := range m.routes {
		routes = append(routes, k)
	}
	return routes
}

func (m *Mux) Http(route string, handler sabu.Handler, methods ...string) {
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	methods = toLower(methods)
	m.routes[searchRoute] = true
	m.trie.Insert(searchRoute, WithHandler(
		m.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabu.Params) {
				if len(methods) > 0 && indexOfList(methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				handler.Handle(writer, request, p)
			},
		),
	))
}

// HttpServiceWithName registers handlers for giving only through the http router
// it does not add said handler into the event manager. This exists
// to allow http requests to be treated as message event.
//
// The event name will be the route of the http request path.
//
// Understand that closing the channel does not close the http endpoint.
func (m *Mux) HttpServiceWithName(eventName string, route string, handler sabu.TransportResponse, methods ...string) {
	var muxHandler = m.pre.For(handler)
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	methods = toLower(methods)
	m.routes[searchRoute] = true
	m.trie.Insert(searchRoute, WithHandler(
		m.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabu.Params) {
				if len(methods) > 0 && indexOfList(methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				m.httpToEvents.HandleMessage(writer, request, p, eventName, func(b sabu.Message, from sabu.Socket) error {
					return muxHandler.Handle(request.Context(), b, sabu.Transport{
						Bus:    m.config.Bus,
						Socket: from,
					})
				})
			},
		),
	))
}

// HttpService registers handlers for giving only through the http router
// it does not add said handler into the event manager. This exists
// to allow http requests to be treated as message event.
//
// The event name will be the route of the http request path.
//
// Understand that closing the channel does not close the http endpoint.
func (m *Mux) HttpService(route string, handler sabu.TransportResponse, methods ...string) {
	var muxHandler = m.pre.For(handler)
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	methods = toLower(methods)
	m.routes[searchRoute] = true
	m.trie.Insert(searchRoute, WithHandler(
		m.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabu.Params) {
				if len(methods) > 0 && indexOfList(methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				m.httpToEvents.HandleMessage(writer, request, p, request.URL.Path, func(b sabu.Message, from sabu.Socket) error {
					return muxHandler.Handle(request.Context(), b, sabu.Transport{
						Bus:    m.config.Bus,
						Socket: from,
					})
				})
			},
		),
	))
}

// Event registers handlers for a giving event returning it's channel.
//
// Understand that closing the channel does not close the http endpoint.
func (m *Mux) Event(eventName string, grp string, handler sabu.TransportResponse) sabu.Channel {
	var muxHandler = m.pre.For(handler)
	var gp = m.relay.Group(eventName, grp)
	return gp.Listen(muxHandler)
}

// Service registers handlers for giving event returning
// events channel.
//
// Understand that closing the channel does not close the http endpoint.
func (m *Mux) Service(eventName string, grp string, route string, handler sabu.TransportResponse, methods ...string) sabu.Channel {
	var muxHandler = m.pre.For(handler)
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	methods = toLower(methods)
	m.routes[searchRoute] = true
	m.trie.Insert(searchRoute, WithHandler(
		m.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabu.Params) {
				if len(methods) > 0 && indexOfList(methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				m.httpToEvents.HandleMessage(writer, request, p, eventName, func(b sabu.Message, from sabu.Socket) error {
					return muxHandler.Handle(request.Context(), b, sabu.Transport{
						Bus:    m.config.Bus,
						Socket: from,
					})
				})
			},
		),
	))

	var gp = m.relay.Group(eventName, grp)
	return gp.Listen(muxHandler)
}

// RedirectAsPath redirects all requests from giving http route into the pubsub using
// path as event name .
//
// Understand that closing the channel does not close the http endpoint.
func (m *Mux) RedirectAsPath(route string, methods ...string) {
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	methods = toLower(methods)
	m.routes[searchRoute] = true
	m.trie.Insert(searchRoute, WithHandler(
		m.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabu.Params) {
				if len(methods) > 0 && indexOfList(methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				m.httpToEvents.HandleMessage(writer, request, p, request.URL.Path, func(b sabu.Message, from sabu.Socket) error {
					m.config.Bus.Send(b)
					return nil
				})
			},
		),
	))
}

// RedirectTo redirects all requests giving http route into the pubsub using
// provided configuration on the HttpToEventService.
//
// Understand that closing the channel does not close the http endpoint.
func (m *Mux) RedirectTo(eventName string, route string, methods ...string) {
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	methods = toLower(methods)
	m.routes[searchRoute] = true
	m.trie.Insert(searchRoute, WithHandler(
		m.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabu.Params) {
				if len(methods) > 0 && indexOfList(methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				m.httpToEvents.HandleMessage(writer, request, p, eventName, func(b sabu.Message, from sabu.Socket) error {
					m.config.Bus.Send(b)
					return nil
				})
			},
		),
	))
}

// Match implements the Matcher interface.
//
// Allow a mux to be used as a matcher and handler elsewhere.
func (m *Mux) Match(msg *sabu.Message) bool {
	var handler = m.trie.Search(msg.Topic.String(), sabu.Params{})
	return handler != nil
}

func (m *Mux) ServeWithMatchers(ctx context.Context, msg sabu.Message, tr sabu.Transport) sabu.MessageErr {
	for _, h := range m.subRoutes {
		if h.Match(&msg) {
			return h.Handle(ctx, msg, tr)
		}
	}

	return sabu.WrapErr(nerror.New("no handler found"), false)
}

type matcherHandler struct {
	sabu.Matcher
	sabu.TransportResponse
}

// AddMatchedRoute adds a priority route which takes priority to all other routes
// local to the multiplexer. If a message is matched by said router then that
// Handler will handle said message accordingly.
func (m *Mux) AddMatchedRoute(matcher sabu.Matcher, response sabu.TransportResponse) {
	m.subRoutes = append(m.subRoutes, matcherHandler{
		Matcher:           matcher,
		TransportResponse: response,
	})
}

// AddPreHttp adds a http wrapper which will act as a middleware
// wrapper for all route's response before the target handler.
func (m *Mux) AddPreHttp(response sabu.HttpWrapper) {
	m.preHttp = append(m.preHttp, response)
}

// AddPre adds a response.Handler which will act as a middleware
// wrapper for all route's response before the target handler.
func (m *Mux) AddPre(response sabu.Wrapper) {
	m.pre = append(m.pre, response)
}

func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var params = sabu.Params{}

	var stack = njson.Log(m.logger)
	stack.New().
		LInfo().
		Message("creating new http request").
		String("url", r.URL.String()).
		String("path", r.URL.Path).
		String("remote_addr", r.RemoteAddr).
		String("headers", fmt.Sprintf("%+q", r.Header)).
		End()

	var reqPath = r.URL.Path
	if len(reqPath) > 1 && strings.HasSuffix(reqPath, "/") {
		// remove trailing slash and client-permanent rule for redirection,
		// if configuration allows that and reqPath has an extra slash.

		// update the new reqPath and redirect.
		// use Trim to ensure there is no open redirect due to two leading slashes
		r.URL.Path = pathSep + strings.Trim(reqPath, pathSep)
	}

	// r.URL.Query() is slow and will allocate a lot, although
	// the first idea was to not introduce a new type to the end-developers
	// so they are using this library as the std one, but we will have to do it
	// for the params, we keep that rule so a new ResponseWriter, which is an interface,
	// and it will be compatible with net/http will be introduced to store the params at least,
	// we don't want to add a third parameter or a global state to this library.

	var targetNode = m.trie.Search(reqPath, params)
	if targetNode == nil {
		if m.NotFound == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		m.NotFound.Handle(w, r, params)
		return
	}

	targetNode.Handler.Handle(w, r, params)
}

func indexOfList(vs []string, m string) int {
	for index, v := range vs {
		if v == m {
			return index
		}
	}
	return -1
}
func toLower(vs []string) []string {
	for index, v := range vs {
		vs[index] = strings.ToLower(v)
	}
	return vs
}
