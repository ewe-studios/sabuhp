package clientServer

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/ewe-studios/sabuhp/codecs"

	"github.com/influx6/npkg/nerror"

	"github.com/ewe-studios/websocket"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/njson"

	"github.com/ewe-studios/sabuhp/sockets/hsocks"

	"golang.org/x/sync/errgroup"

	"github.com/ewe-studios/sabuhp/sockets/ssepub"

	"github.com/ewe-studios/sabuhp/httpub/serverpub"
	"github.com/ewe-studios/sabuhp/sockets/gorillapub"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/radar"
)

const (
	DefaultMaxSize = 4096
)

var (
	DefaultCodec = &codecs.MessagePackCodec{}
	upgrader     = &websocket.Upgrader{
		HandshakeTimeout:  time.Second * 5,
		ReadBufferSize:    gorillapub.DefaultMaxMessageSize,
		WriteBufferSize:   gorillapub.DefaultMaxMessageSize,
		EnableCompression: true,
	}
)

type Mod func(cs *ClientServer)

func WithWebsocketUpgrader(this *websocket.Upgrader) Mod {
	return func(cs *ClientServer) {
		cs.Upgrader = this
	}
}

func WithHttpServer(this *serverpub.Server) Mod {
	return func(cs *ClientServer) {
		cs.HttpServer = this
	}
}

func WithHttpAddr(httpAddr string) Mod {
	return func(cs *ClientServer) {
		cs.Addr = httpAddr
	}
}

func WithSSEServer(this *ssepub.SSEServer) Mod {
	return func(cs *ClientServer) {
		cs.SSEServer = this
	}
}

func WithWebsocketServer(this *gorillapub.GorillaHub) Mod {
	return func(cs *ClientServer) {
		cs.WebsocketServer = this
	}
}

func WithWebsocketConfigCreator(this gorillapub.ConfigCreator) Mod {
	return func(cs *ClientServer) {
		cs.WebsocketConf = this
	}
}

func WithHeaderMod(this sabuhp.HeaderModifications) Mod {
	return func(cs *ClientServer) {
		cs.HeaderMod = this
	}
}

func WithWebsocketHeader(this gorillapub.ResponseHeadersFromRequest) Mod {
	return func(cs *ClientServer) {
		cs.WebsocketHeader = this
	}
}

func WithHttpDecoder(this sabuhp.HttpDecoder) Mod {
	return func(cs *ClientServer) {
		cs.Decoder = this
	}
}

func WithHttpEncoder(this sabuhp.HttpEncoder) Mod {
	return func(cs *ClientServer) {
		cs.Encoder = this
	}
}

func WithMux(config radar.MuxConfig) Mod {
	return func(cs *ClientServer) {
		if config.NotFound == nil {
			config.NotFound = sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, params sabuhp.Params) {
				http.NotFound(writer, request)
			})
		}
		cs.Mux = radar.NewMux(config)
	}
}

// ClientServer exists to provide a central connection point to the message bus
// for the client (browser, CLI app, ...etc).
// They will never host any functions or processing providers but exists to provide
// a direct and distributed (by creating horizontally scaled replicas) that allow clients
// to deliver requests into the underline pubsub bus which will deliver these to service
// servers who host nothing else but functions and processors.
type ClientServer struct {
	Addr            string
	initer          sync.Once
	Mux             *radar.Mux
	Ctx             context.Context
	CancelFunc      context.CancelFunc
	Logger          sabuhp.Logger
	ErrGroup        *errgroup.Group
	BusRelay        *sabuhp.BusRelay
	SSEServer       *ssepub.SSEServer
	HttpServer      *serverpub.Server
	Bus             sabuhp.MessageBus
	Decoder         sabuhp.HttpDecoder
	Encoder         sabuhp.HttpEncoder
	HttpServlet     *hsocks.HttpServlet
	Upgrader        *websocket.Upgrader
	HeaderMod       sabuhp.HeaderModifications
	WebsocketConf   gorillapub.ConfigCreator
	WebsocketHeader gorillapub.ResponseHeadersFromRequest
	WebsocketServer *gorillapub.GorillaHub
	StreamBinder    *sabuhp.StreamBusRelay
}

func New(ctx context.Context, logger sabuhp.Logger, bus sabuhp.MessageBus, mods ...Mod) *ClientServer {
	var cs = new(ClientServer)
	cs.Bus = bus

	var errCtx context.Context
	cs.ErrGroup, errCtx = errgroup.WithContext(ctx)

	cs.Logger = logger
	cs.Ctx, cs.CancelFunc = context.WithCancel(errCtx)
	cs.BusRelay = sabuhp.NewBusRelay(cs.Ctx, cs.Logger, cs.Bus)
	cs.StreamBinder = sabuhp.NewStreamBusRelay(cs.Logger, cs.Bus, cs.BusRelay)

	for _, mod := range mods {
		mod(cs)
	}
	return cs
}

// Init allows you to initialize all components for setup as
// calling ClientServer.Start with both initialize and start all
// related servers.
//
// If you wish to use the default setup but customize to fit your needs
// it's your best interest to call ClientServer.Init first.
func (c *ClientServer) Init() {
	c.initer.Do(c.initializeComponents)
}

// Start calls ClientServer.Init first then starts all related servers (http, websocket)
// etc.
func (c *ClientServer) Start() {
	c.Init()

	// start up http server
	c.ErrGroup.Go(func() error {
		var logMessage = njson.MJSON("starting http server")
		logMessage.String("addr", c.Addr)
		c.Logger.Log(logMessage)

		if startServerErr := c.HttpServer.Listen(c.Ctx, c.Addr); startServerErr != nil {
			return nerror.WrapOnly(startServerErr)
		}
		return nil
	})

	// start web socket server
	c.WebsocketServer.Start()
	c.ErrGroup.Go(func() error {
		c.WebsocketServer.Wait()
		return nil
	})
}

// Wait will block till all services are closed and existed included created
// goroutines. You can confident use wait to block and know that once done
// there is zero chances of goroutine or memory leak as regards started resources.
func (c *ClientServer) Wait() error {
	return c.ErrGroup.Wait()
}

func (c *ClientServer) notFoundHandler(writer http.ResponseWriter, request *http.Request, params sabuhp.Params) {
	var logStack = njson.Log(c.Logger)

	logStack.New().
		LDebug().
		Message("received non registered route request").
		String("host_addr", request.Host).
		String("remote_addr", request.RemoteAddr).
		String("method", request.Method).
		String("path", request.URL.Path).
		String("uri", request.URL.String()).
		End()

	http.NotFound(writer, request)

	logStack.New().
		LDebug().
		Message("sent http not found response").
		String("host_addr", request.Host).
		String("remote_addr", request.RemoteAddr).
		String("method", request.Method).
		String("path", request.URL.Path).
		String("uri", request.URL.String()).
		End()
}

func (c *ClientServer) initializeComponents() {
	if c.Encoder == nil {
		c.Encoder = sabuhp.NewHttpEncoderImpl(DefaultCodec, c.Logger)
	}

	if c.Decoder == nil {
		c.Decoder = sabuhp.NewHttpDecoderImpl(DefaultCodec, c.Logger, DefaultMaxSize)
	}

	if c.Mux == nil {
		c.Mux = radar.NewMux(radar.MuxConfig{
			RootPath: "",
			Bus:      c.Bus,
			Logger:   c.Logger,
			Relay:    c.BusRelay,
			Ctx:      c.Ctx,
			Decoder:  c.Decoder,
			Encoder:  c.Encoder,
			Headers:  nil,
			NotFound: sabuhp.HandlerFunc(c.notFoundHandler),
		})
	}

	if c.HttpServlet == nil {
		c.HttpServlet = hsocks.ManagedHttpServlet(c.Ctx, c.Logger, c.Decoder, c.Encoder, c.HeaderMod, c.Bus)
	}

	if c.SSEServer == nil {
		c.SSEServer = ssepub.ManagedSSEServer(c.Ctx, c.Logger, c.HeaderMod, DefaultCodec)
	}

	if c.WebsocketServer == nil {
		c.WebsocketServer = gorillapub.ManagedGorillaHub(c.Ctx, c.Logger, c.WebsocketConf, DefaultCodec)
	}

	if c.HttpServer == nil {
		c.HttpServer = serverpub.NewServer(c.Mux, time.Minute)
	}

	c.HttpServer.ReadyFunc = c.readyServer

	if c.Upgrader == nil {
		c.Upgrader = upgrader
	}

	c.SSEServer.Stream(c.StreamBinder)
	c.HttpServlet.Stream(c.StreamBinder)
	c.WebsocketServer.Stream(c.StreamBinder)

	c.Mux.Http("/_routes", sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, params sabuhp.Params) {
		writer.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(writer).Encode(c.Mux.Routes()); err != nil {
			var logMessage = njson.MJSON("failed to render response")
			logMessage.String("addr", c.Addr)
			logMessage.Int("_level", int(npkg.ERROR))
			logMessage.Error("error", err)
			c.Logger.Log(logMessage)
		}
	}), "GET", "HEAD")

	c.Mux.Http("/health", sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, params sabuhp.Params) {
		if err := c.HttpServer.Health.Ping(); err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}), "GET", "HEAD")

	// setup stream routes for http
	c.Mux.Http("/streams/http", c.HttpServlet)

	// setup stream routes for sse
	c.Mux.Http("/streams/sse", c.SSEServer, "GET", "HEAD")

	// setup routes for websocket
	var websocketHandler = gorillapub.UpgraderHandler(c.Logger, c.WebsocketServer, c.Upgrader, c.WebsocketHeader)
	c.Mux.Http("/streams/ws", websocketHandler, "GET", "HEAD")
}

func (c *ClientServer) readyServer() {
	var logMessage = njson.MJSON("http server is ready")
	logMessage.String("addr", c.Addr)
	logMessage.Int("_level", int(npkg.INFO))
	c.Logger.Log(logMessage)
}
