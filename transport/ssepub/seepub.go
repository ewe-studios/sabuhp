package ssepub

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influx6/npkg/nunsafe"

	"github.com/influx6/npkg/njson"
	"github.com/influx6/sabuhp/utils"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp/managers"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/sabuhp"
)

const (
	MinClientIdLength          = 7
	SSEStreamHeader            = "event: sse-streams"
	ClientIdentificationHeader = "X-SSE-Client-Id"
	LastEventIdListHeader      = "X-SSE-Last-Event-Ids"
)

var _ sabuhp.Handler = (*SSEServer)(nil)

func ManagedSSEServer(
	ctx context.Context,
	logger sabuhp.Logger,
	manager *managers.Manager,
	optionalHeaders HeaderModifications,
) *SSEServer {
	return &SSEServer{
		ctx:             ctx,
		logger:          logger,
		manager:         manager,
		optionalHeaders: optionalHeaders,
		sockets:         map[string]*SSESocket{},
	}
}

type SSEServer struct {
	logger          sabuhp.Logger
	optionalHeaders HeaderModifications
	ctx             context.Context
	manager         *managers.Manager
	ssl             sync.RWMutex
	sockets         map[string]*SSESocket
}

// ServeHTTP implements the http.Handler interface.
//
// It collects all values from http.Request.ParseForm() as params map
// and calls them with SSEServer.Handle.
func (sse *SSEServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if parseErr := r.ParseForm(); parseErr != nil {
		njson.Log(sse.logger).New().
			Message("failed to parse forms, might be non form request").
			String("error", nerror.WrapOnly(parseErr).Error()).
			End()
		return
	}

	var param = sabuhp.Params{}
	for key := range r.Form {
		param.Set(key, r.Form.Get(key))
	}

	sse.Handle(w, r, param)
}

func (sse *SSEServer) Handle(w http.ResponseWriter, r *http.Request, p sabuhp.Params) {
	var clientId = r.Header.Get(ClientIdentificationHeader)

	var stack = njson.Log(sse.logger)
	defer njson.ReleaseLogStack(stack)

	stack.New().
		Info().
		Message("Received new sse request").
		String("client_id", clientId).
		End()

	clientIdCount := len(clientId)
	if clientIdCount == 0 {
		var cerr = nerror.New("Request does not have the client identification header")
		w.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			w,
			cerr, "Failed to decode request into message",
			http.StatusInternalServerError,
		); err != nil {
			stack.New().
				Error().
				Message("failed to send message into transport").
				String("error", nerror.WrapOnly(cerr).Error()).
				End()
		}

		stack.New().
			Error().
			Message("failed to send message on transport").
			String("error", nerror.WrapOnly(cerr).Error()).
			End()
		return
	}

	if clientIdCount < MinClientIdLength {
		var cerr = nerror.New("Request client identification is less than minimum id length")
		w.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			w,
			cerr, "Failed to decode request into message",
			http.StatusInternalServerError,
		); err != nil {
			stack.New().
				Error().
				Message("failed to send message into transport").
				String("error", nerror.WrapOnly(cerr).Error()).
				End()
		}

		stack.New().
			Error().
			Message("failed to send message on transport").
			String("error", nerror.WrapOnly(cerr).Error()).
			End()
		return
	}

	stack.New().
		Info().
		Message("valid client id provided").
		String("client_id", clientId).
		End()

	sse.ssl.RLock()
	var existingSocket, hasSocket = sse.sockets[clientId]
	sse.ssl.RUnlock()

	if !hasSocket {
		stack.New().
			Info().
			Message("creating new sse socket for request").
			String("client_id", clientId).
			End()

		var socket = NewSSESocket(
			clientId,
			sse.ctx,
			r,
			w,
			sse.logger,
			sse.manager,
			sse.optionalHeaders,
		)

		stack.New().
			Info().
			Message("starting sse socket").
			String("client_id", clientId).
			End()

		if startSocketErr := socket.Start(); startSocketErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			if err := utils.CreateError(
				w,
				startSocketErr,
				"Failed to decode request into message",
				http.StatusInternalServerError,
			); err != nil {
				stack.New().
					Message("failed to send message into transport").
					String("error", nerror.WrapOnly(startSocketErr).Error()).
					End()
			}

			stack.New().
				Error().
				Message("failed to send message on transport").
				String("error", nerror.WrapOnly(startSocketErr).Error()).
				End()
			return
		}

		stack.New().
			Info().
			Message("started sse socket").
			String("client_id", clientId).
			End()

		sse.ssl.Lock()
		sse.sockets[clientId] = socket
		sse.ssl.Unlock()

		stack.New().
			Info().
			Message("added sse socket client into registry").
			String("client_id", clientId).
			End()

		stack.New().
			Info().
			Message("inform manager for new socket").
			String("client_id", clientId).
			End()

		sse.manager.ManageSocketOpened(socket)

		stack.New().
			Info().
			Message("informed manager for new socket").
			String("client_id", clientId).
			End()

		stack.New().
			Info().
			Message("await socket closure").
			String("client_id", clientId).
			End()

		socket.Wait()

		sse.manager.ManageSocketClosed(socket)

		stack.New().
			Info().
			Message("socket sse closed by connection").
			String("client_id", clientId).
			End()
		return
	}

	stack.New().
		Info().
		Message("existing sse client socket found, must be a request").
		String("client_id", clientId).
		End()

	var buffer bytes.Buffer
	if _, terr := io.Copy(&buffer, r.Body); terr != nil {
		w.WriteHeader(http.StatusBadRequest)
		if err := utils.CreateError(
			w,
			terr,
			"Failed to read request body",
			http.StatusBadRequest,
		); err != nil {
			stack.New().
				Error().
				Message("failed to read request body").
				String("error", nerror.WrapOnly(terr).Error()).
				End()
		}

		stack.New().
			Error().
			Message("failed to read request body").
			String("error", nerror.WrapOnly(terr).Error()).
			End()
		return
	}

	stack.New().
		Info().
		Message("copied request from body").
		String("client_id", clientId).
		Bytes("message", buffer.Bytes()).
		End()

	if deliveryErr := existingSocket.SendRead(buffer.Bytes(), 0); deliveryErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			w,
			deliveryErr,
			"Failed to send request body",
			http.StatusInternalServerError,
		); err != nil {
			stack.New().
				Error().
				Message("failed to write delivery error to response").
				String("error", nerror.WrapOnly(deliveryErr).Error()).
				End()
		}

		stack.New().
			Error().
			Message("failed to send deliver request body").
			String("error", nerror.WrapOnly(deliveryErr).Error()).
			End()
		return
	}

	stack.New().
		Info().
		Message("delivered message to sse socket").
		String("client_id", clientId).
		Bytes("message", buffer.Bytes()).
		End()
}

var _ sabuhp.Socket = (*SSESocket)(nil)

type HeaderModifications func(header http.Header)

type SSESocket struct {
	clientId   string
	xid        nxid.ID
	logger     sabuhp.Logger
	req        *http.Request
	res        http.ResponseWriter
	sentMsgs   chan []byte
	rcvMsgs    chan []byte
	ctx        context.Context
	canceler   context.CancelFunc
	manager    *managers.Manager
	waiter     sync.WaitGroup
	headers    HeaderModifications
	remoteAddr net.Addr
	localAddr  net.Addr

	sent     int64
	handled  int64
	received int64
}

func NewSSESocket(
	clientId string,
	ctx context.Context,
	r *http.Request,
	w http.ResponseWriter,
	logger sabuhp.Logger,
	manager *managers.Manager,
	optionalHeaders HeaderModifications,
) *SSESocket {
	var newCtx, newCanceler = context.WithCancel(ctx)
	return &SSESocket{
		req:      r,
		res:      w,
		logger:   logger,
		clientId: clientId,
		ctx:      newCtx,
		manager:  manager,
		remoteAddr: &sseAddr{
			network: "tcp",
			addr:    r.RemoteAddr,
		},
		localAddr: &sseAddr{
			network: "tcp",
			addr:    "0.0.0.0",
		},
		xid:      nxid.New(),
		headers:  optionalHeaders,
		canceler: newCanceler,
		sentMsgs: make(chan []byte),
		rcvMsgs:  make(chan []byte),
	}
}

type sseAddr struct {
	network string
	addr    string
}

func (se sseAddr) String() string {
	return se.addr
}

func (se sseAddr) Network() string {
	return se.network
}

func (se *SSESocket) ID() nxid.ID {
	return se.xid
}

func (se *SSESocket) Stat() sabuhp.SocketStat {
	var stat sabuhp.SocketStat
	stat.Id = se.xid.String()
	stat.Addr = se.localAddr
	stat.RemoteAddr = se.remoteAddr
	stat.Sent = atomic.LoadInt64(&se.sent)
	stat.Handled = atomic.LoadInt64(&se.handled)
	stat.Received = atomic.LoadInt64(&se.received)
	return stat
}

func (se *SSESocket) RemoteAddr() net.Addr {
	return se.remoteAddr
}

func (se *SSESocket) LocalAddr() net.Addr {
	return se.localAddr
}

func (se *SSESocket) Send(msg []byte, timeout time.Duration) error {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case se.sentMsgs <- msg:
		return nil
	case <-timeoutChan: // nil channel will be ignored
		return nerror.New("message delivery timeout")
	case <-se.ctx.Done():
		return nerror.WrapOnly(se.ctx.Err())
	}
}

func (se *SSESocket) SendRead(msg []byte, timeout time.Duration) error {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case se.rcvMsgs <- msg:
		return nil
	case <-timeoutChan: // nil channel will be ignored
		return nerror.New("message delivery timeout")
	case <-se.ctx.Done():
		return nerror.WrapOnly(se.ctx.Err())
	}
}

func (se *SSESocket) Wait() {
	se.waiter.Wait()
}

func (se *SSESocket) Stop() {
	se.canceler()
}

func (se *SSESocket) Start() error {
	// get response flusher
	var flusher, isFlusher = se.res.(http.Flusher)
	if !isFlusher {
		return nerror.New("ResponseWriter object is not a http.Flusher")
	}

	se.res.WriteHeader(http.StatusOK)

	// Set the headers related to event streaming.
	se.res.Header().Set("Content-Type", "text/event-stream")
	se.res.Header().Set("Cache-Control", "no-cache")
	se.res.Header().Set("Connection", "keep-alive")
	se.res.Header().Set("Transfer-Encoding", "chunked")
	se.res.Header().Set("Access-Control-Allow-Origin", "*")
	se.res.Header().Set(ClientIdentificationHeader, se.clientId)

	if se.headers != nil {
		se.headers(se.res.Header())
	}

	se.waiter.Add(2)
	go se.manageReads()
	go se.manageWrites(flusher)

	return nil
}

func (se *SSESocket) manageReads() {
	defer se.waiter.Done()

	var requestContext = se.req.Context()

	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

doLoop:
	for {
		select {
		case <-requestContext.Done():
			break doLoop
		case <-se.ctx.Done():
			break doLoop
		case msg := <-se.rcvMsgs:
			atomic.AddInt64(&se.received, 1)
			stack.New().
				Info().
				Message("received new data from client").
				Bytes("data", msg).
				End()

			if handleErr := se.manager.HandleSocketMessage(msg, se); handleErr != nil {
				stack.New().
					Message("failed handle socket message").
					String("error", nerror.WrapOnly(handleErr).Error()).
					End()
			}
		}
	}
}

func (se *SSESocket) sendWrite(builder *strings.Builder, msg []byte) error {
	builder.Reset()
	builder.WriteString(SSEStreamHeader)
	builder.WriteString("\n")
	builder.WriteString("data: ")
	builder.Write(msg)
	builder.WriteString("\n\n")

	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

	stack.New().
		Info().
		Message("sending new data into writer").
		String("data", builder.String()).
		End()

	if sentCount, writeErr := se.res.Write(nunsafe.String2Bytes(builder.String())); writeErr != nil {
		stack.New().
			Error().
			Message("failed to write data to http response writer").
			String("error", nerror.WrapOnly(writeErr).Error()).
			Int("written", sentCount).
			End()
		return writeErr
	}
	return nil
}

func (se *SSESocket) manageWrites(flusher http.Flusher) {
	defer se.waiter.Done()

	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

	var requestContext = se.req.Context()
	var builder strings.Builder

	flusher.Flush()

doLoop:
	for {
		select {
		case <-requestContext.Done():
			break doLoop
		case <-se.ctx.Done():
			break doLoop
		case msg := <-se.sentMsgs:
			atomic.AddInt64(&se.sent, 1)

			if err := se.sendWrite(&builder, msg); err != nil {
				stack.New().
					Error().
					Message("write failed").
					String("error", err.Error()).
					End()
			}

			// flush content into response writer.
			flusher.Flush()
		}
	}
}
