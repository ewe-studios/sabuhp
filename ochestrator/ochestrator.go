package ochestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/errgroup"

	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg/nerror"

	"github.com/Ewe-Studios/websocket"

	"github.com/influx6/sabuhp"
	"github.com/influx6/sabuhp/actions"
	"github.com/influx6/sabuhp/codecs"
	"github.com/influx6/sabuhp/injectors"
	"github.com/influx6/sabuhp/managers"
	"github.com/influx6/sabuhp/mbox"
	"github.com/influx6/sabuhp/mbus/redispub"
	"github.com/influx6/sabuhp/radar"
	"github.com/influx6/sabuhp/transport/gorillapub"
	"github.com/influx6/sabuhp/transport/httpub/serverpub"
	"github.com/influx6/sabuhp/transport/ssepub"
)

const (
	DefaultMaxSize = 4096
)

var (
	upgrader = &websocket.Upgrader{
		HandshakeTimeout:  time.Second * 5,
		ReadBufferSize:    gorillapub.DefaultMaxMessageSize,
		WriteBufferSize:   gorillapub.DefaultMaxMessageSize,
		EnableCompression: true,
	}
)

type CodecCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
) (sabuhp.Codec, error)

func JsonCodec(
	_ context.Context,
	_ sabuhp.Logger,
) (sabuhp.Codec, error) {
	return &codecs.JsonCodec{}, nil
}

func MessagePackCodec(
	_ context.Context,
	_ sabuhp.Logger,
) (sabuhp.Codec, error) {
	return &codecs.MessagePackCodec{}, nil
}

func GobCodec(
	_ context.Context,
	_ sabuhp.Logger,
) (sabuhp.Codec, error) {
	return &codecs.GobCodec{}, nil
}

type TranslatorCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
) (sabuhp.Translator, error)

func DefaultTranslator(
	_ context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
) (sabuhp.Translator, error) {
	return sabuhp.NewCodecTranslator(codec, logger), nil
}

type TransposerCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	maxSize int64,
) (sabuhp.Transposer, error)

func DefaultTransposer(
	_ context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	maxSize int64,
) (sabuhp.Transposer, error) {
	return sabuhp.NewCodecTransposer(codec, logger, DefaultMaxSize), nil
}

type TransportCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
) (sabuhp.Transport, error)

func DefaultLocalMailerTransport(
	ctx context.Context,
	logger sabuhp.Logger,
	_ sabuhp.Codec,
) (sabuhp.Transport, error) {
	return mbox.NewLocalMailer(
		ctx,
		100,
		logger,
	), nil
}

func DefaultRedisTransportWithOptions(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	redisOption redis.Options,
) (sabuhp.Transport, error) {
	var redisTransport, redisTransportErr = redispub.NewRedisPubSub(redispub.PubSubConfig{
		Logger: logger,
		Ctx:    ctx,
		Codec:  codec,
		Redis:  redisOption,
	})
	if redisTransportErr != nil {
		return nil, nerror.WrapOnly(redisTransportErr)
	}
	redisTransport.Start()
	return redisTransport, nil
}

func RedisTransportWithOptions(config redis.Options) TransportCreator {
	return func(
		ctx context.Context,
		logger sabuhp.Logger,
		codec sabuhp.Codec,
	) (sabuhp.Transport, error) {
		return DefaultRedisTransportWithOptions(ctx, logger, codec, config)
	}
}

func DefaultRedisTransport(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
) (sabuhp.Transport, error) {
	return DefaultRedisTransportWithOptions(ctx, logger, codec, redis.Options{})
}

type RouterCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	manager *managers.Manager,
	transposer sabuhp.Transposer,
	translator sabuhp.Translator,
) (*radar.Mux, error)

func DefaultRouter(
	ctx context.Context,
	logger sabuhp.Logger,
	manager *managers.Manager,
	transposer sabuhp.Transposer,
	translator sabuhp.Translator,
) (*radar.Mux, error) {
	return DefaultRouterWithNotFound(ctx, logger, manager, sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, params sabuhp.Params) {
		http.NotFound(writer, request)
	}), transposer, translator)
}

func DefaultRouterWithNotFound(
	ctx context.Context,
	logger sabuhp.Logger,
	manager *managers.Manager,
	notFound sabuhp.Handler,
	transposer sabuhp.Transposer,
	translator sabuhp.Translator,
) (*radar.Mux, error) {
	return radar.NewMux(radar.MuxConfig{
		RootPath:   "",
		Ctx:        ctx,
		Logger:     logger,
		Manager:    manager,
		Transposer: transposer,
		Translator: translator,
		NotFound: sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
			var logStack = njson.Log(logger)
			defer njson.ReleaseLogStack(logStack)

			logStack.New().
				LDebug().
				Message("received non registered route request").
				String("host_addr", request.Host).
				String("remote_addr", request.RemoteAddr).
				String("method", request.Method).
				String("path", request.URL.Path).
				String("uri", request.URL.String()).
				End()

			notFound.Handle(writer, request, p)

			logStack.New().
				LDebug().
				Message("sent http not found response").
				String("host_addr", request.Host).
				String("remote_addr", request.RemoteAddr).
				String("method", request.Method).
				String("path", request.URL.Path).
				String("uri", request.URL.String()).
				End()
		}),
	}), nil
}

type ManagerCreator func(
	ctx context.Context,
	id nxid.ID,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	transport sabuhp.Transport,
) (*managers.Manager, error)

func DefaultManager(
	ctx context.Context,
	id nxid.ID,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	transport sabuhp.Transport,
) (*managers.Manager, error) {
	return managers.NewManager(managers.ManagerConfig{
		ID:        id,
		Transport: transport,
		Codec:     codec,
		Ctx:       ctx,
		Logger:    logger,
		OnClosure: func(socket sabuhp.Socket) {
			var logStack = njson.Log(logger)
			defer njson.ReleaseLogStack(logStack)

			logStack.New().
				LDebug().
				Message("socket closing").
				Object("stats", socket.Stat()).
				String("socket_id", socket.ID().String()).
				String("remote_addr", socket.RemoteAddr().String()).
				String("local_addr", socket.RemoteAddr().String()).
				End()
		},
		OnOpen: func(socket sabuhp.Socket) {
			var logStack = njson.Log(logger)
			defer njson.ReleaseLogStack(logStack)

			logStack.New().
				LDebug().
				Message("new socket received").
				Object("stats", socket.Stat()).
				String("socket_id", socket.ID().String()).
				String("remote_addr", socket.RemoteAddr().String()).
				String("local_addr", socket.RemoteAddr().String()).
				End()
		},
		MaxWaitToSend: 5 * time.Second,
	}), nil
}

type WorkerHubCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	injector *injectors.Injector,
	registry *actions.WorkerTemplateRegistry,
	transport sabuhp.Transport,
) (*actions.ActionHub, error)

type InjectorCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
) *injectors.Injector

func DefaultInjector(_ context.Context, _ sabuhp.Logger) *injectors.Injector {
	return injectors.NewInjector()
}

func DefaultWorkerHub(
	ctx context.Context,
	logger sabuhp.Logger,
	injector *injectors.Injector,
	registry *actions.WorkerTemplateRegistry,
	transport sabuhp.Transport,
) (*actions.ActionHub, error) {
	return DefaultWorkerHubWithEscalation(ctx, logger, injector, registry, transport, func(escalation actions.Escalation, hub *actions.ActionHub) {
		var logStack = njson.Log(logger)
		defer njson.ReleaseLogStack(logStack)

		var log = logStack.New().
			LDebug().
			Message("worker escalation occurred").
			Formatted("data", "%+v", escalation.Data)

		if escalation.OffendingMessage != nil {
			log.Object("offending_message", escalation.OffendingMessage)
		}

		log.Formatted("worker_protocol", "%+v", escalation.WorkerProtocol).
			Formatted("group_protocol", "%+v", escalation.GroupProtocol).
			List("stats", actions.WorkerStats(hub.Stats())).
			Int("pending_messages", len(escalation.PendingMessages)).
			End()
	})
}

func DefaultWorkerHubWithEscalation(
	ctx context.Context,
	logger sabuhp.Logger,
	injector *injectors.Injector,
	registry *actions.WorkerTemplateRegistry,
	transport sabuhp.Transport,
	escalations actions.EscalationNotification,
) (*actions.ActionHub, error) {
	// create worker hub
	if manager, isManager := transport.(*managers.Manager); isManager {
		return actions.NewActionHub(
			ctx,
			escalations,
			registry,
			injector,
			manager,
			logger,
		), nil
	}
	return nil, nerror.New("transport type is not a *managers.Manager implementing type")
}

type HttpServerCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	router *radar.Mux,
) (*serverpub.Server, error)

func DefaultHTTPServer(
	_ context.Context,
	_ sabuhp.Logger,
	router *radar.Mux,
) (*serverpub.Server, error) {
	return serverpub.NewServer(router, 5*time.Second), nil
}

type GorillaHubCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	transport sabuhp.Transport,
) (*gorillapub.GorillaHub, sabuhp.Handler, error)

func DefaultGorillaHub(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	transport sabuhp.Transport,
) (*gorillapub.GorillaHub, sabuhp.Handler, error) {
	if manager, isManager := transport.(*managers.Manager); isManager {
		var wsServer = gorillapub.ManagedGorillaHub(logger, manager, nil, codec)
		var wsHandler = gorillapub.UpgraderHandler(logger, wsServer, upgrader, nil)
		return wsServer, wsHandler, nil
	}
	return nil, nil, nerror.New("transport type is not a *managers.Manager implementing type")
}

type SSEServerCreator func(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	transport sabuhp.Transport,
) (*ssepub.SSEServer, error)

func DefaultSSEServer(
	ctx context.Context,
	logger sabuhp.Logger,
	codec sabuhp.Codec,
	transport sabuhp.Transport,
) (*ssepub.SSEServer, error) {
	if manager, isManager := transport.(*managers.Manager); isManager {
		return ssepub.ManagedSSEServer(ctx, logger, manager, nil, codec), nil
	}
	return nil, nerror.New("transport type is not a *managers.Manager implementing type")
}

type Station struct {
	Id nxid.ID

	// Register will indicate if handler for websocket and sse server if
	// created will be automatically added into the /streams/sse and /streams/ws
	// endpoints. If set to false, then user is giving freedom to set their own paths
	// on the Router.
	RegisterHandlers bool

	Addr           string
	Logger         sabuhp.Logger
	Ctx            context.Context
	WorkerRegistry *actions.WorkerTemplateRegistry

	// creator functions
	CreateCodec      CodecCreator
	CreateInjector   InjectorCreator
	CreateTransposer TransposerCreator
	CreateTransport  TransportCreator
	CreateManager    ManagerCreator
	CreateWorkerHub  WorkerHubCreator
	CreateRouter     RouterCreator
	CreateServer     HttpServerCreator
	CreateTranslator TranslatorCreator
	CreateSSEServer  SSEServerCreator
	CreateWebsocket  GorillaHubCreator

	injector       *injectors.Injector
	httpHealth     serverpub.HealthPinger
	transposer     sabuhp.Transposer
	translator     sabuhp.Translator
	errGroup       *errgroup.Group
	httpServer     *serverpub.Server
	workers        *actions.ActionHub
	codec          sabuhp.Codec
	router         *radar.Mux
	transport      sabuhp.Transport
	manager        *managers.Manager
	sseHub         *ssepub.SSEServer      // optional sse server
	gwsHttpHandler sabuhp.Handler         // optional http handler
	gwsHub         *gorillapub.GorillaHub // optional gorilla server
}

// NewStation returns a new instance of a station.
func NewStation(
	ctx context.Context,
	id nxid.ID,
	addr string,
	logger sabuhp.Logger,
	registry *actions.WorkerTemplateRegistry,
) *Station {
	var errGroup, groupCtx = errgroup.WithContext(ctx)
	return &Station{
		Addr:             addr,
		Id:               id,
		errGroup:         errGroup,
		Ctx:              groupCtx,
		Logger:           logger,
		WorkerRegistry:   registry,
		RegisterHandlers: true,
	}
}

// DefaultStation returns a station using a redis backed transport layer and MessagePack codec.
func DefaultStation(
	ctx context.Context,
	id nxid.ID, addr string,
	logger sabuhp.Logger,
	registry *actions.WorkerTemplateRegistry,
) *Station {
	var station = NewStation(ctx, id, addr, logger, registry)
	station.CreateCodec = MessagePackCodec
	station.CreateInjector = DefaultInjector
	station.CreateSSEServer = DefaultSSEServer
	station.CreateTransport = DefaultRedisTransport
	station.CreateManager = DefaultManager
	station.CreateWorkerHub = DefaultWorkerHub
	station.CreateRouter = DefaultRouter
	station.CreateServer = DefaultHTTPServer
	station.CreateSSEServer = DefaultSSEServer
	station.CreateWebsocket = DefaultGorillaHub
	station.CreateTransposer = DefaultTransposer
	station.CreateTranslator = DefaultTranslator
	return station
}

// DefaultStation returns a station using a LocalMailer backed transport layer and MessagePack codec.
func DefaultLocalTransportStation(
	ctx context.Context,
	id nxid.ID,
	addr string,
	logger sabuhp.Logger,
	registry *actions.WorkerTemplateRegistry,
) *Station {
	var station = NewStation(ctx, id, addr, logger, registry)
	station.CreateCodec = MessagePackCodec
	station.CreateInjector = DefaultInjector
	station.CreateSSEServer = DefaultSSEServer
	station.CreateTransport = DefaultLocalMailerTransport
	station.CreateManager = DefaultManager
	station.CreateWorkerHub = DefaultWorkerHub
	station.CreateRouter = DefaultRouter
	station.CreateServer = DefaultHTTPServer
	station.CreateSSEServer = DefaultSSEServer
	station.CreateWebsocket = DefaultGorillaHub
	station.CreateTransposer = DefaultTransposer
	station.CreateTranslator = DefaultTranslator
	return station
}

func (s *Station) HttpHealth() serverpub.HealthPinger {
	if s.httpHealth == nil {
		panic("Station.Init is not yet called")
	}
	return s.httpHealth
}

func (s *Station) HttpServer() *serverpub.Server {
	if s.httpServer == nil {
		panic("Station.Init is not yet called")
	}
	return s.httpServer
}

func (s *Station) Injector() *injectors.Injector {
	return s.injector
}

func (s *Station) SSEServer() *ssepub.SSEServer {
	return s.sseHub
}

func (s *Station) WebsocketHandler() sabuhp.Handler {
	return s.gwsHttpHandler
}

func (s *Station) WebsocketHub() *gorillapub.GorillaHub {
	return s.gwsHub
}

func (s *Station) Workers() *actions.ActionHub {
	if s.workers == nil {
		panic("Station.Init is not yet called")
	}
	return s.workers
}

func (s *Station) Transport() sabuhp.Transport {
	if s.transport == nil {
		panic("Station.Init is not yet called")
	}
	return s.transport
}

func (s *Station) Translator() sabuhp.Translator {
	if s.translator == nil {
		panic("Station.Init is not yet called")
	}
	return s.translator
}

func (s *Station) Transposer() sabuhp.Transposer {
	if s.transposer == nil {
		panic("Station.Init is not yet called")
	}
	return s.transposer
}

func (s *Station) Codec() sabuhp.Codec {
	if s.codec == nil {
		panic("Station.Init is not yet called")
	}
	return s.codec
}

func (s *Station) Manager() *managers.Manager {
	if s.manager == nil {
		panic("Station.Init is not yet called")
	}
	return s.manager
}

func (s *Station) Router() *radar.Mux {
	if s.router == nil {
		panic("Station.Init is not yet called")
	}
	return s.router
}

func (s *Station) Wait() error {
	return s.errGroup.Wait()
}

func (s *Station) Init() error {
	if s.Addr == "" {
		return nerror.New("Server.Addr is required")
	}

	// create codec for station
	var codec, createCodecErr = s.CreateCodec(s.Ctx, s.Logger)
	if createCodecErr != nil {
		return nerror.WrapOnly(createCodecErr)
	}
	s.codec = codec

	// create injector for station
	s.injector = s.CreateInjector(s.Ctx, s.Logger)

	// create transposer for station
	var transposer, createTransposerErr = s.CreateTransposer(s.Ctx, s.Logger, codec, DefaultMaxSize)
	if createTransposerErr != nil {
		return nerror.WrapOnly(createTransposerErr)
	}
	s.transposer = transposer

	var translator, createTranslatorErr = s.CreateTranslator(s.Ctx, s.Logger, codec)
	if createTranslatorErr != nil {
		return nerror.WrapOnly(createTranslatorErr)
	}
	s.translator = translator

	// create transport
	var transport, createTransportErr = s.CreateTransport(s.Ctx, s.Logger, s.codec)
	if createTransportErr != nil {
		return nerror.WrapOnly(createTransportErr)
	}
	s.transport = transport

	// check if we were provided a transport manager as transport
	var userManager, isManager = transport.(*managers.Manager)
	var _, isTransportManager = transport.(*managers.TransportManager)

	// create transport subscription manager
	if !isTransportManager && !isManager {
		var manager, createManagerErr = s.CreateManager(s.Ctx, s.Id, s.Logger, s.codec, s.transport)
		if createManagerErr != nil {
			return nerror.WrapOnly(createManagerErr)
		}

		s.manager = manager
	}

	if !isManager && isTransportManager {
		panic("Please provide the raw Transport implement type")
	}

	if isManager && !isTransportManager {
		s.manager = userManager
	}

	s.errGroup.Go(func() error {
		s.manager.Wait()
		return nil
	})

	// create resource router
	var router, createRouterErr = s.CreateRouter(s.Ctx, s.Logger, s.manager, s.transposer, s.translator)
	if createRouterErr != nil {
		return nerror.WrapOnly(createRouterErr)
	}
	s.router = router

	// create http server
	var httpServer, createServerErr = s.CreateServer(s.Ctx, s.Logger, s.router)
	if createServerErr != nil {
		return nerror.WrapOnly(createServerErr)
	}

	httpServer.ReadyFunc = func() {
		var logMessage = njson.MJSON("http server is ready")
		logMessage.String("addr", s.Addr)
		s.Logger.Log(logMessage)
	}

	s.errGroup.Go(func() error {
		var logMessage = njson.MJSON("starting http server")
		logMessage.String("addr", s.Addr)
		s.Logger.Log(logMessage)
		if startServerErr := httpServer.Listen(s.Ctx, s.Addr); startServerErr != nil {
			return nerror.WrapOnly(startServerErr)
		}
		return nil
	})

	s.httpHealth = httpServer.Health
	s.httpServer = httpServer

	s.router.Http("/health", sabuhp.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
		params sabuhp.Params,
	) {
		if err := s.httpHealth.Ping(); err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}), "GET", "HEAD")

	s.router.Http("/_routes", sabuhp.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
		params sabuhp.Params,
	) {
		writer.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(writer).Encode(s.router.Routes()); err != nil {
			var logMessage = njson.MJSON("failed to render response")
			logMessage.String("addr", s.Addr)
			logMessage.Error("error", err)
			s.Logger.Log(logMessage)
		}
	}), "GET", "HEAD")

	// create worker hub
	var workerHub, createWorkerHubErr = s.CreateWorkerHub(s.Ctx, s.Logger, s.injector, s.WorkerRegistry, s.manager)
	if createWorkerHubErr != nil {
		return nerror.WrapOnly(createWorkerHubErr)
	}

	workerHub.Start()
	s.errGroup.Go(func() error {
		workerHub.Wait()
		return nil
	})

	s.workers = workerHub

	// create websocket server if available
	if s.CreateWebsocket != nil {
		var websocketHub, websocketHandler, createWSErr = s.CreateWebsocket(s.Ctx, s.Logger, s.codec, s.manager)
		if createWSErr != nil {
			return nerror.WrapOnly(createWSErr)
		}

		websocketHub.Start()
		s.errGroup.Go(func() error {
			websocketHub.Wait()
			return nil
		})

		if s.RegisterHandlers {
			s.router.Http("/streams/ws", websocketHandler, "GET", "HEAD")
		}

		s.gwsHub = websocketHub
		s.gwsHttpHandler = websocketHandler
	}

	// create sse server if available
	if s.CreateSSEServer != nil {
		var sseHub, createSSEErr = s.CreateSSEServer(s.Ctx, s.Logger, s.codec, s.manager)
		if createSSEErr != nil {
			return nerror.WrapOnly(createSSEErr)
		}

		if s.RegisterHandlers {
			s.router.Http("/streams/sse", sseHub, "GET", "HEAD")
		}

		s.sseHub = sseHub
	}

	return nil
}
