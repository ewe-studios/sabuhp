package helloService

import (
	"context"
	"log"
	"net/http"
	"time"

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

	var masterCtx, masterEnder = context.WithCancel(context.Background())
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
	}

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

	var waiter = ndaemon.WaiterForCtxSignal(masterCtx, masterEnder)

	var wsServer = gorillapub.ManagedGorillaHub(mainLogger, transportManager, nil)
	var wsHandler = gorillapub.UpgraderHandler(mainLogger, wsServer, upgrader, nil)

	var sseServer = ssepub.ManagedSSEServer(masterCtx, mainLogger, transportManager, nil)

	var router = radar.NewMux(radar.MuxConfig{
		RootPath: "",
		Logger:   mainLogger,
		Manager:  transportManager,
		NotFound: sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
			http.NotFound(writer, request)
		}),
	})

	router.Http("/sse/*", "GET").Handler(sseServer).Add()
	router.Http("/ws/*", "GET").Handler(wsHandler).Add()

	var httpServer = nhttp.NewServer(router, 5*time.Second)

	wsServer.Start()
	if startServerErr := httpServer.Listen(masterCtx, ":7800"); startServerErr != nil {
		log.Fatal(startServerErr)
	}

	wsServer.Wait()
	transportManager.Wait()
	waiter.Wait()
}
