package radar

import (
	"net/http"
	"strings"

	"github.com/influx6/sabuhp/utils"

	"github.com/influx6/sabuhp/managers"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp"
)

type MuxConfig struct {
	RootPath string
	Logger   sabuhp.Logger
	NotFound sabuhp.Handler
	Manager  *managers.Manager
}

// Mux is Request multiplexer.
// It matches an event name or http url pattern to
// a specific TransportHandler which will be registered
// to the provided transport for handling specific events.
type Mux struct {
	config    MuxConfig
	rootPath  string
	trie      *Trie
	logger    sabuhp.Logger
	NotFound  sabuhp.Handler
	subRoutes []sabuhp.MessageRouter
	pre       sabuhp.Wrappers
	preHttp   sabuhp.HttpWrappers
	manager   *managers.Manager
}

func NewMux(config MuxConfig) *Mux {
	return &Mux{
		config:   config,
		rootPath: config.RootPath,
		trie:     NewTrie(),
		manager:  config.Manager,
		logger:   config.Logger,
		NotFound: config.NotFound,
	}
}

func (m *Mux) Http(route string, methods ...string) *HttpService {
	return &HttpService{
		mux:     m,
		route:   route,
		methods: toLower(methods),
	}
}

type HttpService struct {
	mux     *Mux
	methods []string
	route   string
	handler sabuhp.Handler
}

func (h *HttpService) Handler(handler sabuhp.Handler) *HttpService {
	h.handler = handler
	return h
}

func (h *HttpService) Add() {
	if h.handler == nil {
		panic("Handler is required")
	}
	var searchRoute = h.mux.rootPath + h.route
	h.mux.trie.Insert(searchRoute, WithHandler(
		h.mux.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
				if len(h.methods) > 0 && indexOfList(h.methods, request.Method) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				h.handler.Handle(writer, request, p)
			},
		),
	))
}

// EventService configures an event which
// registers accordingly to the underline
// transport returning appropriate channel.
//
// It also allows registering a http endpoint which
// will generate appropriate message object from the
// request which will be serviced by the TransportResponse
// handler registered to the service endpoint.
//
// The http.ResponseWriter will be attached as a messages.LocalPayload
// for cases where the user wishes to send a response back to the client.
type EventService struct {
	mux *Mux

	// this are the event attributes
	event           string
	responseHandler sabuhp.TransportResponse

	// this are for http redirection.
	route      string
	methods    []string
	transposer sabuhp.Transpose
}

func (e *EventService) Http(route string, transposer sabuhp.Transpose, methods ...string) {
	e.route = route
	e.methods = toLower(methods)
	e.transposer = transposer
}

func (e *EventService) Handler(res sabuhp.TransportResponse) {
	e.responseHandler = e.mux.pre.For(res)
}

func (e *EventService) Add() sabuhp.Channel {
	if e.responseHandler == nil {
		panic("TransportResponse is required")
	}
	if len(e.event) == 0 {
		panic("event is required")
	}
	if len(e.route) == 0 {
		return e.mux.manager.Listen(e.event, e.responseHandler)
	}

	var searchRoute = e.mux.rootPath + e.route
	e.mux.trie.Insert(searchRoute, WithHandler(
		e.mux.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
				if len(e.methods) > 0 && indexOfList(e.methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				// convert request into a message object.
				var transposedMessage, transposeErr = e.transposer(request, p)
				if transposeErr != nil {
					writer.WriteHeader(http.StatusBadRequest)
					if err := utils.CreateError(
						writer,
						transposeErr,
						"Failed to decode request into message",
						http.StatusBadRequest,
					); err != nil {
						njson.Log(e.mux.logger).New().
							Message("failed to write message to response writer").
							String("error", nerror.WrapOnly(err).Error()).
							String("route", e.route).
							String("redirect_to", e.event).
							End()
					}

					njson.Log(e.mux.logger).New().
						Message("failed to send message on transport").
						String("error", nerror.WrapOnly(transposeErr).Error()).
						String("route", e.route).
						String("redirect_to", e.event).
						End()
				}

				transposedMessage.LocalPayload = sabuhp.HttpResponseWriterAsPayload(writer)

				if err := e.responseHandler.Handle(transposedMessage, e.mux.manager); err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					if err := utils.CreateError(
						writer,
						transposeErr,
						"Failed to decode request into message",
						http.StatusInternalServerError,
					); err != nil {
						njson.Log(e.mux.logger).New().
							Message("failed to write message to response writer").
							String("error", nerror.WrapOnly(err).Error()).
							String("route", e.route).
							String("redirect_to", e.event).
							End()
					}

					njson.Log(e.mux.logger).New().
						Message("failed to send message on transport").
						String("error", nerror.WrapOnly(err).Error()).
						String("route", e.route).
						String("redirect_to", e.event).
						End()
				}

				writer.WriteHeader(http.StatusNoContent)
			},
		),
	))

	return e.mux.manager.Listen(e.event, e.responseHandler)
}

// Service registers handlers for giving event returning
// events channel.
func (m *Mux) Service(eventName string) *EventService {
	return &EventService{
		event: eventName,
		mux:   m,
	}
}

// RedirectTo redirects all requests from http into the pubsub using
// provided configuration on the HttpToEventService.
func (m *Mux) RedirectTo(eventName string) *HttpToEventService {
	return &HttpToEventService{
		event: eventName,
		mux:   m,
	}
}

type HttpToEventService struct {
	mux            *Mux
	event          string
	methods        []string
	route          string
	httpTransposer sabuhp.Transpose
}

func (h *HttpToEventService) ConvertWith(tr sabuhp.Transpose) {
	h.httpTransposer = tr
}

func (h *HttpToEventService) Route(route string) {
	h.route = route
}

func (h *HttpToEventService) Method(methods ...string) {
	h.methods = toLower(methods)
}

func (h *HttpToEventService) Add() {
	if len(h.event) == 0 {
		panic("event is required")
	}
	if len(h.route) == 0 {
		panic("route is required")
	}
	var searchRoute = h.mux.rootPath + h.route
	h.mux.trie.Insert(searchRoute, WithHandler(
		h.mux.preHttp.ForFunc(
			func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
				if len(h.methods) > 0 && indexOfList(h.methods, strings.ToLower(request.Method)) == -1 {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}

				// convert request into a message object.
				// convert request into a message object.
				var transposedMessage, transposeErr = h.httpTransposer(request, p)
				if transposeErr != nil {
					writer.WriteHeader(http.StatusBadRequest)
					if err := utils.CreateError(
						writer,
						transposeErr,
						"Failed to decode request into message",
						http.StatusBadRequest,
					); err != nil {
						njson.Log(h.mux.logger).New().
							Message("failed to write message to response writer").
							String("error", nerror.WrapOnly(err).Error()).
							String("route", h.route).
							String("redirect_to", h.event).
							End()
					}

					njson.Log(h.mux.logger).New().
						Message("failed to send message on transport").
						String("error", nerror.WrapOnly(transposeErr).Error()).
						String("route", h.route).
						String("redirect_to", h.event).
						End()
					return
				}

				transposedMessage.LocalPayload = sabuhp.HttpResponseWriterAsPayload(writer)

				// if we are to redirect this
				if transposedMessage.Delivery == sabuhp.SendToAll {
					if err := h.mux.manager.SendToAll(transposedMessage, 0); err != nil {
						writer.WriteHeader(http.StatusInternalServerError)
						if err := utils.CreateError(
							writer,
							transposeErr, "Failed to decode request into message", http.StatusInternalServerError); err != nil {
							njson.Log(h.mux.logger).New().
								Message("failed to send message into transport").
								String("error", nerror.WrapOnly(err).Error()).
								String("route", h.route).
								String("redirect_to", h.event).
								End()
						}

						njson.Log(h.mux.logger).New().
							Message("failed to send message on transport").
							String("error", nerror.WrapOnly(err).Error()).
							String("route", h.route).
							String("redirect_to", h.event).
							End()
					}
					return
				}
				if err := h.mux.manager.SendToOne(transposedMessage, 0); err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					if err := utils.CreateError(
						writer,
						transposeErr,
						"Failed to decode request into message",
						http.StatusInternalServerError,
					); err != nil {
						njson.Log(h.mux.logger).New().
							Message("failed to send message into transport").
							String("error", nerror.WrapOnly(err).Error()).
							String("route", h.route).
							String("redirect_to", h.event).
							End()
					}

					njson.Log(h.mux.logger).New().
						Message("failed to send message on transport").
						String("error", nerror.WrapOnly(err).Error()).
						String("route", h.route).
						String("redirect_to", h.event).
						End()
					return
				}

				writer.WriteHeader(http.StatusNoContent)
			},
		),
	))
}

// Match implements the Matcher interface.
//
// Allow a mux to be used as a matcher and handler elsewhere.
func (m *Mux) Match(msg *sabuhp.Message) bool {
	var handler = m.trie.Search(msg.Topic, sabuhp.Params{})
	return handler != nil
}

func (m *Mux) Handle(msg *sabuhp.Message, tr sabuhp.Transport) sabuhp.MessageErr {
	for _, h := range m.subRoutes {
		if h.Match(msg) {
			return h.Handle(msg, tr)
		}
	}

	return sabuhp.WrapErr(nerror.New("no handler found"), false)
}

type matcherHandler struct {
	sabuhp.Matcher
	sabuhp.TransportResponse
}

// AddMatchedRoute adds a priority route which takes priority to all other routes
// local to the multiplexer. If a message is matched by said router then that
// Handler will handle said message accordingly.
func (m *Mux) AddMatchedRoute(matcher sabuhp.Matcher, response sabuhp.TransportResponse) {
	m.subRoutes = append(m.subRoutes, matcherHandler{
		Matcher:           matcher,
		TransportResponse: response,
	})
}

// AddPreHttp adds a http wrapper which will act as a middleware
// wrapper for all route's response before the target handler.
func (m *Mux) AddPreHttp(response sabuhp.HttpWrapper) {
	m.preHttp = append(m.preHttp, response)
}

// AddPre adds a response.Handler which will act as a middleware
// wrapper for all route's response before the target handler.
func (m *Mux) AddPre(response sabuhp.Wrapper) {
	m.pre = append(m.pre, response)
}

func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var params = sabuhp.Params{}

	var reqPath = r.URL.Path
	if len(reqPath) > 1 && strings.HasSuffix(reqPath, "/") {
		// Remove trailing slash and client-permanent rule for redirection,
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
