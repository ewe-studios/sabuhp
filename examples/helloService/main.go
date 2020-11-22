package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/sabuhp/slaves"

	"github.com/influx6/sabuhp/radar"

	"github.com/Ewe-Studios/websocket"
	"github.com/influx6/sabuhp/transport/gorillapub"

	"github.com/influx6/npkg/nhttp"
	"github.com/influx6/sabuhp/transport/ssepub"

	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nxid"
	"github.com/influx6/sabuhp/managers"

	redis "github.com/go-redis/redis/v8"
	"github.com/influx6/npkg/ndaemon"
	"github.com/influx6/sabuhp/codecs"
	"github.com/influx6/sabuhp/mbus/redispub"
	"github.com/influx6/sabuhp/testingutils"
)

var (
	mainLogger = &testingutils.LoggerPub{}
	mainCodec  = &codecs.JsonCodec{}
	upgrader   = &websocket.Upgrader{
		HandshakeTimeout:  time.Second * 5,
		ReadBufferSize:    gorillapub.DefaultMaxMessageSize,
		WriteBufferSize:   gorillapub.DefaultMaxMessageSize,
		EnableCompression: false,
	}
)

func main() {
	var logStack = njson.Log(mainLogger)
	defer njson.ReleaseLogStack(logStack)

	// worker template registry
	var workerRegistry = slaves.NewWorkerTemplateRegistry()
	workerRegistry.Register(slaves.WorkerRequest{
		ActionName:  "hello_world",
		PubSubTopic: "hello",
		WorkerCreator: func(config slaves.WorkerConfig) *slaves.WorkerGroup {
			config.Instance = slaves.ScalingInstances
			config.Behaviour = slaves.RestartAll
			config.Action = slaves.ActionFunc(func(ctx context.Context, to string, message *sabuhp.Message, t sabuhp.Transport) {
				if sendErr := t.SendToAll(&sabuhp.Message{
					ID:       nxid.New(),
					Topic:    message.FromAddr,
					FromAddr: to,
					Payload:  []byte("hello world"),
					Metadata: nil,
					Params:   nil,
				}, 5*time.Second); sendErr != nil {
					logStack.New().
						Error().
						Message("failed to send response message").
						String("error", nerror.WrapOnly(sendErr).Error()).
						End()
				}
			})
			return slaves.NewWorkGroup(config)
		},
	})

	var masterCtx, masterEnder = context.WithCancel(context.Background())

	// create wait signal
	var waiter = ndaemon.WaiterForCtxSignal(masterCtx, masterEnder)

	// create transport
	var redisTransport, redisTransportErr = redispub.NewRedisPubSub(redispub.PubSubConfig{
		Logger:                    mainLogger,
		Ctx:                       masterCtx,
		Codec:                     mainCodec,
		Redis:                     redis.Options{},
		MaxWaitForSubConfirmation: 0,
		StreamMessageInterval:     0,
		MaxWaitForSubRetry:        0,
	})
	if redisTransportErr != nil {
		log.Fatal(redisTransportErr.Error())
		return
	}

	// wrap transport with manager
	var transportManager = managers.NewManager(managers.ManagerConfig{
		ID:        nxid.New(),
		Transport: redisTransport,
		Codec:     mainCodec,
		Ctx:       masterCtx,
		Logger:    mainLogger,
		OnClosure: func(socket sabuhp.Socket) {
			logStack.New().
				Debug().
				Message("socket closing").
				Object("stats", socket.Stat()).
				String("socket_id", socket.ID().String()).
				String("remote_addr", socket.RemoteAddr().String()).
				String("local_addr", socket.RemoteAddr().String()).
				End()
		},
		OnOpen: func(socket sabuhp.Socket) {
			logStack.New().
				Debug().
				Message("new socket received").
				Object("stats", socket.Stat()).
				String("socket_id", socket.ID().String()).
				String("remote_addr", socket.RemoteAddr().String()).
				String("local_addr", socket.RemoteAddr().String()).
				End()
		},
		MaxWaitToSend: 0,
	})

	// create worker hub
	var workerHub = slaves.NewActionHub(
		masterCtx,
		func(escalation slaves.Escalation, hub *slaves.ActionHub) {
			// log info about escalation.
		},
		workerRegistry,
		transportManager,
		mainLogger,
	)
	workerHub.Start()

	// transports protocols
	var wsServer = gorillapub.ManagedGorillaHub(mainLogger, transportManager, nil)
	var wsHandler = gorillapub.UpgraderHandler(mainLogger, wsServer, upgrader, nil)
	wsServer.Start()

	var sseServer = ssepub.ManagedSSEServer(masterCtx, mainLogger, transportManager, nil)

	// router
	var router = radar.NewMux(radar.MuxConfig{
		RootPath: "",
		Logger:   mainLogger,
		Manager:  transportManager,
		NotFound: sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
			http.NotFound(writer, request)
		}),
	})

	// http service endpoints
	router.Http("/sse/*", "GET").Handler(sseServer).Add()
	router.Http("/ws/*", "GET").Handler(wsHandler).Add()

	// http server
	var httpServer = nhttp.NewServer(router, 5*time.Second)

	if startServerErr := httpServer.Listen(masterCtx, ":7800"); startServerErr != nil {
		log.Fatal(startServerErr)
		return
	}

	logStack.New().
		Info().
		Message("starting service").
		End()

	workerHub.Wait()
	wsServer.Wait()
	transportManager.Wait()
	waiter.Wait()
}
