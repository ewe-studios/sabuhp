package gorillapub

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
	"github.com/influx6/npkg/nxid"

	"github.com/ewe-studios/websocket"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/utils"
)

const (
	// default buffer size for socket message channel.
	DefaultBufferForMessages = 100

	// DefaultReconnectionWaitCheck defines the default Wait time for checking reconnection.
	DefaultReconnectionWaitCheck = time.Millisecond * 200

	// DefaultReadWait defines the default Wait time for writing.
	DefaultWriteWait = time.Second * 60

	// DefaultReadWait defines the default Wait time for reading.
	DefaultReadWait = time.Second * 60

	// DefaultPingInterval is the default ping interval for a redelivery
	// of a ping message.
	DefaultPingInterval = (DefaultReadWait * 9) / 10

	// DefaultMessageType defines the default message type expected.
	DefaultMessageType = websocket.BinaryMessage

	// Default maximum message size allowed if user does not set value
	// in SocketConfig.
	DefaultMaxMessageSize = 4096
)

var (
	sabuMessageWSHeader  = []byte("0|")
	anyMessageWSHeader   = []byte("1|")
	websocketHeadMessage = []byte("Websocket Endpoint!\n")
)

type ResponseHeadersFromRequest func(r *http.Request) http.Header

type SocketInfo struct {
	Query   url.Values
	Path    string
	Headers sabuhp.Header
}

func HttpUpgrader(
	logger sabuhp.Logger,
	hub *GorillaHub,
	upgrader *websocket.Upgrader,
	custom ResponseHeadersFromRequest,
) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if strings.ToLower(request.Method) == "head" {
			writer.Header().Add("X-Service-Type", "Websocket")
			writer.Header().Add("X-Service-Name", "SabuHP STREAMS")
			writer.WriteHeader(http.StatusNoContent)
			return
		}

		var customHeaders http.Header
		if custom != nil {
			customHeaders = custom(request)
		}

		var socket, socketCreateErr = upgrader.Upgrade(writer, request, customHeaders)
		if socketCreateErr != nil {
			logger.Log(njson.MJSON("failed to upgrade websocket", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("host", request.Host)
				event.String("method", request.Method)
				event.String("remote_addr", request.RemoteAddr)
				event.String("request_uri", request.RequestURI)
				event.String("error", nerror.WrapOnly(socketCreateErr).Error())

				event.ObjectFor("headers", func(headerEncoder npkg.ObjectEncoder) {
					for k, v := range request.Header {
						func(key string, value []string) {
							headerEncoder.ListFor(key, func(listEncoder npkg.ListEncoder) {
								for _, val := range value {
									listEncoder.AddString(val)
								}
							})
						}(k, v)
					}
				})
			}))
			return
		}

		var info = &SocketInfo{
			Path:    request.URL.Path,
			Query:   request.URL.Query(),
			Headers: sabuhp.Header(request.Header.Clone()),
		}

		var socketHandler = hub.HandleSocket(socket, info)
		if handleSocketErr := socketHandler.Run(); handleSocketErr != nil {
			logger.Log(njson.MJSON("error out during socket handling", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("host", request.Host)
				event.String("method", request.Method)
				event.String("remote_addr", request.RemoteAddr)
				event.String("request_uri", request.RequestURI)
				event.String("error", nerror.WrapOnly(socketCreateErr).Error())

				event.ObjectFor("headers", func(headerEncoder npkg.ObjectEncoder) {
					for k, v := range request.Header {
						func(key string, value []string) {
							headerEncoder.ListFor(key, func(listEncoder npkg.ListEncoder) {
								for _, val := range value {
									listEncoder.AddString(val)
								}
							})
						}(k, v)
					}
				})
			}))
		}
	}
}

func UpgraderHandler(
	logger sabuhp.Logger,
	hub *GorillaHub,
	upgrader *websocket.Upgrader,
	custom ResponseHeadersFromRequest,
) sabuhp.Handler {
	return sabuhp.HandlerFunc(func(writer http.ResponseWriter, request *http.Request, p sabuhp.Params) {
		var customHeaders http.Header
		if custom != nil {
			customHeaders = custom(request)
		}
		var socket, socketCreateErr = upgrader.Upgrade(writer, request, customHeaders)
		if socketCreateErr != nil {
			logger.Log(njson.MJSON("failed to upgrade websocket", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.Object("params", p)
				event.String("host", request.Host)
				event.String("method", request.Method)
				event.String("remote_addr", request.RemoteAddr)
				event.String("request_uri", request.RequestURI)
				event.String("error", nerror.WrapOnly(socketCreateErr).Error())

				event.ObjectFor("headers", func(headerEncoder npkg.ObjectEncoder) {
					for k, v := range request.Header {
						func(key string, value []string) {
							headerEncoder.ListFor(key, func(listEncoder npkg.ListEncoder) {
								for _, val := range value {
									listEncoder.AddString(val)
								}
							})
						}(k, v)
					}
				})
			}))
			return
		}

		var info = &SocketInfo{
			Path:    request.URL.Path,
			Query:   request.URL.Query(),
			Headers: sabuhp.Header(request.Header.Clone()),
		}

		var socketHandler = hub.HandleSocket(socket, info)
		if handleSocketErr := socketHandler.Run(); handleSocketErr != nil {
			logger.Log(njson.MJSON("error out during socket handling", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.Object("params", p)
				event.String("host", request.Host)
				event.String("method", request.Method)
				event.String("remote_addr", request.RemoteAddr)
				event.String("request_uri", request.RequestURI)
				event.String("error", nerror.WrapOnly(socketCreateErr).Error())

				event.ObjectFor("headers", func(headerEncoder npkg.ObjectEncoder) {
					for k, v := range request.Header {
						func(key string, value []string) {
							headerEncoder.ListFor(key, func(listEncoder npkg.ListEncoder) {
								for _, val := range value {
									listEncoder.AddString(val)
								}
							})
						}(k, v)
					}
				})
			}))
		}
	})
}

type ConfigCreator func(config SocketConfig) SocketConfig

type HubConfig struct {
	Ctx           context.Context
	Logger        sabuhp.Logger
	Codec         sabuhp.Codec
	ConfigHandler ConfigCreator
}

type GorillaHub struct {
	config   HubConfig
	canceler context.CancelFunc
	ctx      context.Context
	doFunc   chan func()
	waiter   sync.WaitGroup
	starter  *sync.Once
	ender    *sync.Once
	sockets  map[nxid.ID]*GorillaSocket
	streams  *sabuhp.SocketServers
}

// ManagedGorillaHub returns a new instance of a gorilla hub which uses a managers.Manager
// to manage communication across various websocket connections.
//
// It allows the manager to delegate connections management to a suitable type (i.e GorillaHub)
// and in the future other protocols/transport while the manager uses the central message bus transport
// to communicate to other services and back to the connections.
func ManagedGorillaHub(
	ctx context.Context,
	logger sabuhp.Logger,
	optionalConfigCreator ConfigCreator,
	codec sabuhp.Codec,
) *GorillaHub {
	return NewGorillaHub(HubConfig{
		Logger:        logger,
		Ctx:           ctx,
		Codec:         codec,
		ConfigHandler: optionalConfigCreator,
	})
}

func NewGorillaHub(config HubConfig) *GorillaHub {
	var newCtx, canceler = context.WithCancel(config.Ctx)
	var hub = &GorillaHub{
		config:   config,
		ctx:      newCtx,
		canceler: canceler,
		streams:  sabuhp.NewSocketServers(),
	}
	hub.init()
	return hub
}

func (gh *GorillaHub) init() *GorillaHub {
	if gh.doFunc == nil {
		gh.doFunc = make(chan func())
	}
	if gh.sockets == nil {
		gh.sockets = map[nxid.ID]*GorillaSocket{}
	}
	if gh.ender == nil {
		var doer sync.Once
		gh.ender = &doer
	}
	if gh.starter == nil {
		var doer sync.Once
		gh.starter = &doer
	}
	return gh
}

func (gh *GorillaHub) Wait() {
	gh.waiter.Wait()
}

func (gh *GorillaHub) Stop() {
	gh.ender.Do(func() {
		gh.canceler()
		gh.waiter.Wait()
	})
}

func (gh *GorillaHub) Stream(stream sabuhp.SocketService) {
	gh.streams.Stream(stream)
}

func (gh *GorillaHub) Start() {
	gh.starter.Do(func() {
		gh.waiter.Add(1)
		go gh.manage()
	})
}

type SocketHandler interface {
	Run() error
}

type gorillaSockHandler struct {
	socket *GorillaSocket
}

func (es *gorillaSockHandler) Run() error {
	es.socket.Start()
	es.socket.Wait()
	return nil
}

// HandleSocket implements necessary logic to man and manage the lifecycle
// of a new socket provided to the hub.
func (gh *GorillaHub) HandleSocket(socket *websocket.Conn, info *SocketInfo) SocketHandler {
	var done = make(chan SocketHandler, 1)

	gh.waiter.Add(1)
	var doFunc = func() {
		var config SocketConfig
		config.Ctx = gh.ctx
		config.Info = info
		config.Conn = socket
		config.Codec = gh.config.Codec
		config.Logger = gh.config.Logger

		// run through list of config handlers
		if gh.config.ConfigHandler != nil {
			config = gh.config.ConfigHandler(config)
		}

		// ensure config is valid
		config.ensure()

		var sock = NewGorillaSocket(config)
		gh.sockets[sock.id] = sock

		gh.streams.SocketOpened(sock)

		sock.Start()
		go func() {
			defer gh.waiter.Done()
			<-sock.ctx.Done()

			gh.streams.SocketClosed(sock)

			var deleteFunc = func() {
				delete(gh.sockets, sock.id)
			}

			select {
			case <-gh.ctx.Done():
				return
			case gh.doFunc <- deleteFunc:
				return
			}
		}()

		done <- &gorillaSockHandler{socket: sock}
	}

	// send function into work go-routine.
	select {
	case gh.doFunc <- doFunc:
		break
	case <-gh.ctx.Done():
		gh.waiter.Done()
		break
	}

	// wait for done signal.
	select {
	case handler := <-done:
		return handler
	case <-gh.ctx.Done():
		return &utils.ErrorHandler{Err: nerror.WrapOnly(gh.ctx.Err())}
	}
}

func (gh *GorillaHub) Stats() ([]sabuhp.SocketStat, error) {
	var statChan = make(chan []sabuhp.SocketStat, 1)
	var doAction = func() {
		var stats []sabuhp.SocketStat
		for _, socket := range gh.sockets {
			stats = append(stats, socket.Stat())
		}
		statChan <- stats
	}

	select {
	case gh.doFunc <- doAction:
		break
	case <-gh.ctx.Done():
		return nil, nerror.New("hub has closed")
	}

	select {
	case stats := <-statChan:
		return stats, nil
	case <-gh.ctx.Done():
		return nil, nerror.New("hub has closed")
	}
}

func (gh *GorillaHub) manage() {
	defer gh.waiter.Done()

doLoop:
	for {
		select {
		case <-gh.ctx.Done():
			break doLoop
		case doFunc := <-gh.doFunc:
			doFunc()
		}
	}

	gh.sockets = map[nxid.ID]*GorillaSocket{}
}

type Endpoint interface {
	Dial(ctx context.Context) (*websocket.Conn, *http.Response, error)
}

type SocketConfig struct {
	Info                  *SocketInfo
	Buffer                int
	MessageType           int
	MaxMessageSize        int
	WriteMessageWait      time.Duration
	ReadMessageWait       time.Duration
	ReconnectionCheckWait time.Duration
	PingInterval          time.Duration // should be lesser than ReadMessageWait duration
	Ctx                   context.Context
	Logger                sabuhp.Logger
	Codec                 sabuhp.Codec

	// You can supply the websocket.Conn aif you wish to
	// use an existing connection, the endpoint becomes
	// non useful here, and you should set ShouldNotTry to true.
	Conn           *websocket.Conn
	ShouldNotRetry bool

	// Client related fields
	Res      *http.Response // optional
	MaxRetry int
	RetryFn  sabuhp.RetryFunc
	Endpoint Endpoint

	// SocketByteHandler defines the function contract a GorillaSocket uses
	// to handle a message.
	//
	// Be aware that returning an error from the handler to the Gorilla Socket
	// will cause the immediate closure of that socket and ending communication
	// with the server. So unless your intention is to
	// end the connection, handle the error yourself.
	Handler sabuhp.SocketMessageHandler
}

func (s *SocketConfig) clientConnect(ctx context.Context) error {
	var retryCount int
	for {
		var nextDuration = s.RetryFn(retryCount)
		<-time.After(nextDuration)

		var conn, res, err = s.Endpoint.Dial(ctx)
		if err != nil && retryCount < s.MaxRetry {
			if res != nil && res.Body != nil {
				_ = res.Body.Close()
			}

			s.Logger.Log(njson.MJSON("failed connection attempt", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.INFO))
				event.String("error", nerror.WrapOnly(err).Error())
			}))

			retryCount++
			continue
		}
		if err != nil && retryCount >= s.MaxRetry {
			if res != nil && res.Body != nil {
				_ = res.Body.Close()
			}
			s.Logger.Log(njson.MJSON("failed all connection attempts, ending...", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.String("error", nerror.WrapOnly(err).Error())
			}))
			return nerror.WrapOnly(err)
		}

		s.Conn = conn
		s.Res = res
		return nil
	}
}

func (s *SocketConfig) ensure() {
	if s.Conn == nil && s.Endpoint == nil {
		panic("SocketConfig.Conn or SocketConfig.Endpoint must be provided")
	}
	if s.Info == nil {
		panic("SocketConfig.Info must be provided")
	}
	if s.Logger == nil {
		panic("SocketConfig.Logger must be provided")
	}
	if s.Codec == nil {
		panic("SocketConfig.Codec must be provided")
	}
	if s.ReadMessageWait <= 0 {
		s.ReadMessageWait = DefaultReadWait
	}
	if s.ReconnectionCheckWait <= 0 {
		s.ReconnectionCheckWait = DefaultReconnectionWaitCheck
	}
	if s.WriteMessageWait <= 0 {
		s.WriteMessageWait = DefaultWriteWait
	}
	if s.MaxMessageSize <= 0 {
		s.MaxMessageSize = DefaultMaxMessageSize
	}
	if s.MessageType <= 0 {
		s.MessageType = DefaultMessageType
	}
	if s.PingInterval <= 0 {
		s.PingInterval = DefaultPingInterval
	}
	if s.Buffer <= 0 {
		s.Buffer = DefaultBufferForMessages
	}
}

var _ sabuhp.Socket = (*GorillaSocket)(nil)

type GorillaSocket struct {
	id           nxid.ID
	config       *SocketConfig
	isClient     bool
	canceler     context.CancelFunc
	ctx          context.Context
	pending      chan *sabuhp.Message
	send         chan *sabuhp.Message
	deliver      chan *sabuhp.Message
	handler      *sabuhp.Sock
	waiter       sync.WaitGroup
	starter      sync.Once
	ender        sync.Once
	rl           sync.RWMutex
	reconnecting bool
	sl           sync.RWMutex
	socket       *websocket.Conn
	received     int64
	sent         int64
	handled      int64
}

func (g *GorillaSocket) Listen(handler sabuhp.SocketMessageHandler) {
	g.handler.Use(handler)
}

func GorillaClient(config SocketConfig) (*GorillaSocket, error) {
	config.ensure()
	if config.Endpoint == nil && config.Conn == nil {
		return nil, nerror.New("SocketConfig.Endpoint or SocketConfig.Conn is required")
	}
	if config.RetryFn == nil {
		return nil, nerror.New("SocketConfig.RetryFn is required")
	}

	var localCtx, canceler = context.WithCancel(config.Ctx)
	if config.Conn == nil {
		if connectErr := config.clientConnect(localCtx); connectErr != nil {
			canceler()
			return nil, nerror.WrapOnly(connectErr)
		}
	}

	var wg GorillaSocket
	wg.isClient = true
	wg.socket = config.Conn
	wg.config = &config
	wg.id = nxid.New()
	wg.handler = sabuhp.NewSock(config.Handler)
	wg.pending = make(chan *sabuhp.Message, config.Buffer)
	wg.send = make(chan *sabuhp.Message, config.Buffer)
	wg.deliver = make(chan *sabuhp.Message, config.Buffer)
	wg.canceler = canceler
	wg.ctx = localCtx
	return &wg, nil
}

func NewGorillaSocket(config SocketConfig) *GorillaSocket {
	config.ensure()
	var localCtx, canceler = context.WithCancel(config.Ctx)
	var wg GorillaSocket
	wg.socket = config.Conn
	wg.config = &config
	wg.id = nxid.New()
	wg.handler = sabuhp.NewSock(config.Handler)
	wg.pending = make(chan *sabuhp.Message, config.Buffer)
	wg.send = make(chan *sabuhp.Message, config.Buffer)
	wg.deliver = make(chan *sabuhp.Message, config.Buffer)
	wg.canceler = canceler
	wg.ctx = localCtx
	return &wg
}

func (g *GorillaSocket) Conn() *websocket.Conn {
	g.sl.RLock()
	defer g.sl.RUnlock()
	return g.socket
}

func (g *GorillaSocket) ID() nxid.ID {
	return g.id
}

func (g *GorillaSocket) Wait() {
	g.waiter.Wait()
}

// Send delivers provided message into a batch of messages for delivery.
func (g *GorillaSocket) Send(messages ...sabuhp.Message) {
	for _, message := range messages {
		select {
		case g.send <- &message:
			continue
		case <-g.config.Ctx.Done():
			var merr = nerror.WrapOnly(g.config.Ctx.Err())
			if message.Future != nil {
				message.Future.WithError(merr)
			}
		}
	}
}

func (g *GorillaSocket) LocalAddr() net.Addr {
	g.sl.RLock()
	defer g.sl.RUnlock()
	return g.socket.LocalAddr()
}

func (g *GorillaSocket) RemoteAddr() net.Addr {
	g.sl.RLock()
	defer g.sl.RUnlock()
	return g.socket.RemoteAddr()
}

func (g *GorillaSocket) Stat() sabuhp.SocketStat {
	var stat sabuhp.SocketStat
	stat.Id = g.id.String()
	stat.Addr = g.socket.LocalAddr()
	stat.RemoteAddr = g.socket.RemoteAddr()
	stat.Sent = atomic.LoadInt64(&g.sent)
	stat.Handled = atomic.LoadInt64(&g.handled)
	stat.Received = atomic.LoadInt64(&g.received)
	return stat
}

func (g *GorillaSocket) Start() {
	g.starter.Do(func() {
		g.waiter.Add(3)
		go g.manage()
	})
}

func (g *GorillaSocket) Stop() {
	g.ender.Do(func() {
		g.canceler()
		g.waiter.Wait()
	})
}

func (g *GorillaSocket) manage() {
	defer func() {
		g.sl.RLock()
		defer g.sl.RUnlock()
		if closingErr := g.socket.Close(); closingErr != nil {
			g.config.Logger.Log(njson.MJSON("error handling during connection closure", func(event npkg.Encoder) {
				event.Bool("is_client", g.isClient)
				event.Int("_level", int(npkg.ERROR))
				event.String("socket_id", g.id.String())
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
				event.String("error", nerror.WrapOnly(closingErr).Error())
			}))
		}
		g.waiter.Done()
	}()

	go g.manageWrites()
	go g.manageDelivery()
	g.manageReads()

	g.config.Logger.Log(njson.MJSON("closed all go-routines", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.String("socket_id", g.id.String())
		event.Int("_level", int(npkg.INFO))
		event.String("socket_network", g.socket.RemoteAddr().Network())
		event.String("socket_remote_addr", g.socket.RemoteAddr().String())
		event.String("socket_local_addr", g.socket.LocalAddr().String())
		event.String("socket_local_network", g.socket.LocalAddr().Network())
		event.String("socket_sub_protocols", g.socket.Subprotocol())
	}))
}

func (g *GorillaSocket) manageDelivery() {
loopCall:
	for {
		select {
		case <-g.config.Ctx.Done():
			g.config.Logger.Log(njson.MJSON("closing delivery loop", func(event npkg.Encoder) {
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.Int("_level", int(npkg.INFO))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
			}))
			break loopCall
		case message := <-g.deliver:
			atomic.AddInt64(&g.handled, 1)
			if handleErr := g.handler.Notify(*message, g); handleErr != nil {
				g.config.Logger.Log(njson.MJSON("error handling message from handler", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.Int("_level", int(npkg.ERROR))
					event.Object("message", message)
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(handleErr).Error())
				}))
				break loopCall
			}
		}
	}

	// if we ever get here, then end socket.
	g.canceler()
}

func (g *GorillaSocket) manageReads() {
	defer g.waiter.Done()

	g.socket.SetReadLimit(int64(g.config.MaxMessageSize))
	_ = g.socket.SetReadDeadline(time.Now().Add(g.config.ReadMessageWait))
	g.socket.SetPongHandler(func(_ string) error {
		_ = g.socket.SetReadDeadline(time.Now().Add(g.config.ReadMessageWait))
		return nil
	})

	var info = g.config.Info

	for {
		if g.isReconnecting() {
			<-time.After(g.config.ReconnectionCheckWait)
			continue
		}

		var messageType, message, readErr = g.socket.ReadMessage()
		atomic.AddInt64(&g.received, 1)
		if readErr != nil {
			g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.Int("_level", int(npkg.ERROR))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
				event.String("error", nerror.WrapOnly(readErr).Error())

				if websocket.IsUnexpectedCloseError(readErr, websocket.CloseGoingAway) {
					event.Bool("client_closed_connection", true)
				}

				if websocket.IsUnexpectedCloseError(readErr, websocket.CloseAbnormalClosure) {
					event.Bool("client_closed_abnormally", true)
				}
			}))

			if g.attemptReconnection() {
				continue
			}
			break
		}

		g.config.Logger.Log(njson.MJSON("received new message", func(event npkg.Encoder) {
			event.String("message", string(message))
			event.Int("message_type", messageType)
			event.Bool("is_client", g.isClient)
			event.String("socket_id", g.id.String())
			event.Int("_level", int(npkg.INFO))
			event.String("socket_network", g.socket.RemoteAddr().Network())
			event.String("socket_remote_addr", g.socket.RemoteAddr().String())
			event.String("socket_local_addr", g.socket.LocalAddr().String())
			event.String("socket_local_network", g.socket.LocalAddr().Network())
			event.String("socket_sub_protocols", g.socket.Subprotocol())
		}))

		// get type marker
		if !bytes.HasPrefix(message, sabuMessageWSHeader) && !bytes.HasPrefix(message, anyMessageWSHeader) {
			g.config.Logger.Log(njson.MJSON("received unknown message flag", func(event npkg.Encoder) {
				event.String("message", string(message))
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.Int("_level", int(npkg.INFO))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
			}))

			var payload = &sabuhp.Message{
				Topic:    info.Path,
				Id:       nxid.New(),
				Path:     info.Path,
				Query:    info.Query,
				Form:     url.Values{},
				Headers:  sabuhp.Header{},
				Cookies:  sabuhp.ReadCookies(info.Headers, ""),
				Bytes:    message,
				Metadata: map[string]string{},
				Params:   map[string]string{},
			}

			g.deliver <- payload
			continue
		}

		// get type marker
		if bytes.HasPrefix(message, anyMessageWSHeader) {
			var rest = bytes.TrimPrefix(message, anyMessageWSHeader)
			g.config.Logger.Log(njson.MJSON("received any message flag", func(event npkg.Encoder) {
				event.String("message", string(message))
				event.String("rest", string(rest))
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.Int("_level", int(npkg.INFO))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
			}))

			var payload = &sabuhp.Message{
				Topic:    info.Path,
				Id:       nxid.New(),
				Path:     info.Path,
				Query:    info.Query,
				Form:     url.Values{},
				Headers:  sabuhp.Header{},
				Cookies:  sabuhp.ReadCookies(info.Headers, ""),
				Bytes:    rest,
				Metadata: map[string]string{},
				Params:   map[string]string{},
			}

			g.deliver <- payload
			continue
		}

		var rest = bytes.TrimPrefix(message, sabuMessageWSHeader)
		g.config.Logger.Log(njson.MJSON("received sabuhp message flag", func(event npkg.Encoder) {
			event.String("message", string(message))
			event.String("rest", string(rest))
			event.Bool("is_client", g.isClient)
			event.String("socket_id", g.id.String())
			event.Int("_level", int(npkg.INFO))
			event.String("socket_network", g.socket.RemoteAddr().Network())
			event.String("socket_remote_addr", g.socket.RemoteAddr().String())
			event.String("socket_local_addr", g.socket.LocalAddr().String())
			event.String("socket_local_network", g.socket.LocalAddr().Network())
			event.String("socket_sub_protocols", g.socket.Subprotocol())
		}))

		var deliveredMessage, deliveredErr = g.config.Codec.Decode(rest)
		if deliveredErr != nil {
			g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
				event.Bytes("message", message)
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.Int("_level", int(npkg.ERROR))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
				event.String("error", nerror.WrapOnly(deliveredErr).Error())
			}))

			continue
		}

		if len(deliveredMessage.Path) == 0 {
			deliveredMessage.Path = info.Path
		}

		g.deliver <- &deliveredMessage
	}

	g.config.Logger.Log(njson.MJSON("closing read loop", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.String("socket_id", g.id.String())
		event.Int("_level", int(npkg.INFO))
		event.String("socket_network", g.socket.RemoteAddr().Network())
		event.String("socket_remote_addr", g.socket.RemoteAddr().String())
		event.String("socket_local_addr", g.socket.LocalAddr().String())
		event.String("socket_local_network", g.socket.LocalAddr().Network())
		event.String("socket_sub_protocols", g.socket.Subprotocol())
	}))

	// if we ever get here, then end socket.
	g.canceler()
}

func (g *GorillaSocket) isReconnecting() bool {
	g.rl.RLock()
	if g.reconnecting {
		g.rl.RUnlock()
		return true
	}
	g.rl.RUnlock()
	return false
}

func (g *GorillaSocket) attemptReconnection() (continueLoop bool) {
	if !g.isClient {
		continueLoop = false
		return
	}

	if g.config.ShouldNotRetry {
		continueLoop = false
		return
	}

	if g.isReconnecting() {
		continueLoop = true
		return
	}

	g.config.Logger.Log(njson.MJSON("check ctx before reconnection", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.Int("_level", int(npkg.INFO))
		event.String("socket_id", g.id.String())
		event.String("socket_network", g.socket.RemoteAddr().Network())
	}))

	select {
	case <-g.ctx.Done():
		continueLoop = false
		return
	default:
		// do nothing
	}

	g.rl.Lock()
	g.reconnecting = true
	g.rl.Unlock()

	// attempt reconnection
	g.sl.RLock()
	_ = g.socket.Close()
	g.sl.RUnlock()

	g.config.Logger.Log(njson.MJSON("attempting connection re-establishment", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.String("socket_id", g.id.String())
		event.Int("_level", int(npkg.INFO))
		event.String("socket_network", g.socket.RemoteAddr().Network())
	}))

	var connErr = g.config.clientConnect(g.ctx)
	if connErr != nil {
		g.config.Logger.Log(njson.MJSON("failed connection re-establishment", func(event npkg.Encoder) {
			event.Bool("is_client", g.isClient)
			event.Int("_level", int(npkg.ERROR))
			event.String("socket_id", g.id.String())
			event.String("socket_network", g.socket.RemoteAddr().Network())
			event.String("error", nerror.WrapOnly(connErr).Error())
		}))

		continueLoop = false

		g.rl.Lock()
		g.reconnecting = false
		g.rl.Unlock()

		return
	}

	g.config.Logger.Log(njson.MJSON("received new connection", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.String("socket_id", g.id.String())
		event.Int("_level", int(npkg.INFO))
		event.String("socket_network", g.socket.RemoteAddr().Network())
	}))

	g.sl.Lock()
	g.socket = g.config.Conn
	g.sl.Unlock()

	g.rl.Lock()
	g.reconnecting = false
	g.rl.Unlock()

	continueLoop = true

	g.config.Logger.Log(njson.MJSON("connection re-established", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.Int("_level", int(npkg.INFO))
		event.String("socket_id", g.id.String())
		event.String("socket_network", g.socket.RemoteAddr().Network())
	}))
	return
}

func (g *GorillaSocket) manageWrites() {
	defer g.waiter.Done()

	var pendingMessages int
	var pingTicker = time.NewTicker(g.config.PingInterval)
	defer pingTicker.Stop()

runloop:
	for {
		if g.isReconnecting() {
			<-time.After(g.config.ReconnectionCheckWait)
			continue runloop
		}

		select {
		case <-pingTicker.C:
			if setTimeErr := g.socket.SetWriteDeadline(time.Now().Add(g.config.WriteMessageWait)); setTimeErr != nil {
				g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_id", g.id.String())
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(setTimeErr).Error())
				}))
				continue runloop
			}
			if writeErr := g.socket.WriteMessage(websocket.PingMessage, nil); writeErr != nil {
				g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(writeErr).Error())
				}))

				// attempt reconnection
				if g.attemptReconnection() {
					continue runloop
				}

				break runloop
			}
		case message := <-g.pending:
			// this case should rear-ly happen, only when a send succeeded
			// into the goroutine but the client disconnecting and was
			// about to reconnect.

			g.config.Logger.Log(njson.MJSON("received message on pending", func(event npkg.Encoder) {
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.Int("_level", int(npkg.INFO))
				event.String("message", string(message.Bytes))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
			}))

			atomic.AddInt64(&g.sent, 1)
			if setTimeErr := g.socket.SetWriteDeadline(time.Now().Add(g.config.WriteMessageWait)); setTimeErr != nil {
				g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(setTimeErr).Error())
				}))
			}

			var writer, writerErr = g.socket.NextWriter(g.config.MessageType)
			if writerErr != nil {
				var wrapped = nerror.WrapOnly(writerErr)
				g.config.Logger.Log(njson.MJSON("error creating new writer on socket", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.Error("error", wrapped)
				}))

				_ = writer.Close()

				if message.Future != nil {
					message.Future.WithError(writerErr)
				}

				// attempt reconnection
				if g.attemptReconnection() {
					continue runloop
				}

				break runloop
			}

			if sendErr := g.messageToWriter(message, writer); sendErr != nil {
				var wrapped = nerror.WrapOnly(writerErr)
				g.config.Logger.Log(njson.MJSON("error writing content into writer", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.Error("error", wrapped)
				}))

				_ = writer.Close()

				if message.Future != nil {
					message.Future.WithError(sendErr)
				}

				// attempt reconnection
				if g.attemptReconnection() {
					continue runloop
				}

				break runloop
			}

			if closingErr := writer.Close(); closingErr != nil {
				g.config.Logger.Log(njson.MJSON("error writing to socket", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(closingErr).Error())
				}))

				if message.Future != nil {
					message.Future.WithError(closingErr)
				}

				// attempt reconnection
				if g.attemptReconnection() {
					continue runloop
				}

				break runloop
			}

			// inform message future we sent it well.
			if message.Future != nil {
				message.Future.WithValue(nil)
			}
		case message := <-g.send:

			if g.isReconnecting() {
				g.pending <- message
				g.config.Logger.Log(njson.MJSON("pushed into pending", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.Int("_level", int(npkg.INFO))
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
				}))
				continue runloop
			}

			g.config.Logger.Log(njson.MJSON("received write", func(event npkg.Encoder) {
				event.Bool("is_client", g.isClient)
				event.Int("_level", int(npkg.INFO))
				event.String("socket_id", g.id.String())
				event.String("message", string(message.Bytes))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
			}))

			atomic.AddInt64(&g.sent, 1)
			if setTimeErr := g.socket.SetWriteDeadline(time.Now().Add(g.config.WriteMessageWait)); setTimeErr != nil {
				g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(setTimeErr).Error())
				}))
			}

			var writer, writerErr = g.socket.NextWriter(g.config.MessageType)
			if writerErr != nil {
				g.config.Logger.Log(njson.MJSON("error creating new writer on socket", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(writerErr).Error())
				}))

				_ = writer.Close()

				if message.Future != nil {
					message.Future.WithError(writerErr)
				}

				// attempt reconnection
				if g.attemptReconnection() {

					// add into pending for a retry
					g.pending <- message

					continue runloop
				}
				break runloop
			}

			if sendErr := g.messageToWriter(message, writer); sendErr != nil {
				var wrapped = nerror.WrapOnly(writerErr)
				g.config.Logger.Log(njson.MJSON("error writing content into writer", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.Error("error", wrapped)
				}))

				_ = writer.Close()

				// attempt reconnection
				if g.attemptReconnection() {

					// add into pending for a retry
					g.pending <- message

					continue runloop
				}

				if message.Future != nil {
					message.Future.WithError(sendErr)
				}
				break runloop
			}

			// attempt to delivery message and those pending
			pendingMessages = len(g.send)
			var collectedMessage = make([]*sabuhp.Message, pendingMessages)

			for i := 0; i < pendingMessages; i++ {
				var pendingMsg = <-g.send
				collectedMessage[i] = pendingMsg

				if sendErr := g.messageToWriter(pendingMsg, writer); sendErr != nil {
					var wrapped = nerror.WrapOnly(writerErr)
					g.config.Logger.Log(njson.MJSON("error writing content into writer", func(event npkg.Encoder) {
						event.Bool("is_client", g.isClient)
						event.String("socket_id", g.id.String())
						event.String("message", string(message.Bytes))
						event.String("socket_network", g.socket.RemoteAddr().Network())
						event.Int("_level", int(npkg.ERROR))
						event.String("socket_remote_addr", g.socket.RemoteAddr().String())
						event.String("socket_local_addr", g.socket.LocalAddr().String())
						event.String("socket_local_network", g.socket.LocalAddr().Network())
						event.String("socket_sub_protocols", g.socket.Subprotocol())
						event.Error("error", wrapped)
					}))

					_ = writer.Close()

					// attempt reconnection
					if g.attemptReconnection() {

						// add into pending for a retry
						g.pending <- message

						continue runloop
					}

					if pendingMsg.Future != nil {
						pendingMsg.Future.WithError(sendErr)
					}
					break runloop
				}
			}

			if closingErr := writer.Close(); closingErr != nil {
				g.config.Logger.Log(njson.MJSON("error writing to socket", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("message", string(message.Bytes))
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(writerErr).Error())
				}))

				// attempt reconnection
				if g.attemptReconnection() {

					// add into pending for a retry
					g.pending <- message

					continue runloop
				}

				if message.Future != nil {
					message.Future.WithError(closingErr)
				}
				break runloop
			}

			// inform message future we sent it well.
			if message.Future != nil {
				message.Future.WithValue(nil)
			}

			// inform pending messages futures we sent it well.
			for _, pendingMsg := range collectedMessage {
				if pendingMsg.Future != nil {
					pendingMsg.Future.WithValue(nil)
				}
			}
		case <-g.ctx.Done():
			if setTimeErr := g.socket.SetWriteDeadline(time.Now().Add(g.config.WriteMessageWait)); setTimeErr != nil {
				g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(setTimeErr).Error())
				}))
			}
			if writeErr := g.socket.WriteMessage(websocket.CloseMessage, nil); writeErr != nil {
				g.config.Logger.Log(njson.MJSON("error write deadline", func(event npkg.Encoder) {
					event.Bool("is_client", g.isClient)
					event.String("socket_id", g.id.String())
					event.String("socket_network", g.socket.RemoteAddr().Network())
					event.String("socket_remote_addr", g.socket.RemoteAddr().String())
					event.String("socket_local_addr", g.socket.LocalAddr().String())
					event.Int("_level", int(npkg.ERROR))
					event.String("socket_local_network", g.socket.LocalAddr().Network())
					event.String("socket_sub_protocols", g.socket.Subprotocol())
					event.String("error", nerror.WrapOnly(writeErr).Error())
				}))
			}

			break runloop
		}
	}

	g.config.Logger.Log(njson.MJSON("closing write loop", func(event npkg.Encoder) {
		event.Bool("is_client", g.isClient)
		event.Int("_level", int(npkg.INFO))
		event.String("socket_id", g.id.String())
		event.String("socket_network", g.socket.RemoteAddr().Network())
		event.String("socket_remote_addr", g.socket.RemoteAddr().String())
		event.String("socket_local_addr", g.socket.LocalAddr().String())
		event.String("socket_local_network", g.socket.LocalAddr().Network())
		event.String("socket_sub_protocols", g.socket.Subprotocol())
	}))

	// if we ever get here, then end socket.
	g.canceler()
}

func (g *GorillaSocket) messageToWriter(message *sabuhp.Message, writer io.Writer) error {
	var writeErr error
	if message.ContentType != sabuhp.MessageContentType {
		_, writeErr = writer.Write(anyMessageWSHeader)
		if writeErr != nil {
			var wrapped = nerror.WrapOnly(writeErr)
			g.config.Logger.Log(njson.MJSON("error writing to socket", func(event npkg.Encoder) {
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.String("message", string(message.Bytes))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.Int("_level", int(npkg.ERROR))
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
				event.Error("error", wrapped)
			}))
			return wrapped
		}

		var _, sendErr = writer.Write(message.Bytes)
		if sendErr != nil {
			var wrapped = nerror.WrapOnly(sendErr)
			g.config.Logger.Log(njson.MJSON("error writing to socket", func(event npkg.Encoder) {
				event.Int("_level", int(npkg.ERROR))
				event.Bool("is_client", g.isClient)
				event.String("socket_id", g.id.String())
				event.String("message", string(message.Bytes))
				event.String("socket_network", g.socket.RemoteAddr().Network())
				event.String("socket_remote_addr", g.socket.RemoteAddr().String())
				event.String("socket_local_addr", g.socket.LocalAddr().String())
				event.String("socket_local_network", g.socket.LocalAddr().Network())
				event.String("socket_sub_protocols", g.socket.Subprotocol())
				event.Error("error", wrapped)
			}))
			return wrapped
		}

		return nil
	}

	_, writeErr = writer.Write(sabuMessageWSHeader)

	if writeErr != nil {
		var wrapped = nerror.WrapOnly(writeErr)
		g.config.Logger.Log(njson.MJSON("error writing to socket", func(event npkg.Encoder) {
			event.Bool("is_client", g.isClient)
			event.String("socket_id", g.id.String())
			event.String("message", string(message.Bytes))
			event.String("socket_network", g.socket.RemoteAddr().Network())
			event.String("socket_remote_addr", g.socket.RemoteAddr().String())
			event.Int("_level", int(npkg.ERROR))
			event.String("socket_local_addr", g.socket.LocalAddr().String())
			event.String("socket_local_network", g.socket.LocalAddr().Network())
			event.String("socket_sub_protocols", g.socket.Subprotocol())
			event.Error("error", wrapped)
		}))
		return wrapped
	}

	var encodedMsg, encodedErr = g.config.Codec.Encode(*message)
	if encodedErr != nil {
		g.config.Logger.Log(njson.MJSON("failed to encode message", func(event npkg.Encoder) {
			event.Bool("is_client", g.isClient)
			event.String("socket_id", g.id.String())
			event.String("message", string(message.Bytes))
			event.Int("_level", int(npkg.ERROR))
			event.String("socket_network", g.socket.RemoteAddr().Network())
			event.String("socket_remote_addr", g.socket.RemoteAddr().String())
			event.String("socket_local_addr", g.socket.LocalAddr().String())
			event.String("socket_local_network", g.socket.LocalAddr().Network())
			event.String("socket_sub_protocols", g.socket.Subprotocol())
			event.String("error", nerror.WrapOnly(encodedErr).Error())
		}))

		return nerror.WrapOnly(encodedErr)
	}

	var _, sendErr = writer.Write(encodedMsg)
	if sendErr != nil {
		var wrapped = nerror.WrapOnly(sendErr)
		g.config.Logger.Log(njson.MJSON("error writing to socket", func(event npkg.Encoder) {
			event.Int("_level", int(npkg.ERROR))
			event.Bool("is_client", g.isClient)
			event.String("socket_id", g.id.String())
			event.String("message", string(message.Bytes))
			event.String("socket_network", g.socket.RemoteAddr().Network())
			event.String("socket_remote_addr", g.socket.RemoteAddr().String())
			event.String("socket_local_addr", g.socket.LocalAddr().String())
			event.String("socket_local_network", g.socket.LocalAddr().Network())
			event.String("socket_sub_protocols", g.socket.Subprotocol())
			event.Error("error", wrapped)
		}))
		return wrapped
	}

	return nil
}
