package ssepub

import (
	"bytes"
	"context"
	"github.com/ewe-studios/sabuhp/sabu"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/influx6/npkg/nunsafe"

	"github.com/ewe-studios/sabuhp/utils"
	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg/nerror"
)

const (
	MinClientIdLength          = 7
	ClientIdentificationHeader = "X-SSE-Client-Id"
	LastEventIdListHeader      = "X-SSE-Last-Event-Ids"

	eventHeader = "event:"
)

var doubleLine = []byte("\n\n")

var _ sabu.Handler = (*SSEServer)(nil)

func ManagedSSEServer(
	ctx context.Context,
	logger sabu.Logger,
	optionalHeaders sabu.HeaderModifications,
	codec sabu.Codec,
) *SSEServer {
	return &SSEServer{
		ctx:             ctx,
		logger:          logger,
		codec:           codec,
		optionalHeaders: optionalHeaders,
		sockets:         map[string]*SSESocket{},
		streams:         sabu.NewSocketServers(),
	}
}

type SSEServer struct {
	logger          sabu.Logger
	codec           sabu.Codec
	optionalHeaders sabu.HeaderModifications
	ctx             context.Context
	streams         *sabu.SocketServers
	ssl             sync.RWMutex
	sockets         map[string]*SSESocket
}

func (sse *SSEServer) Stream(server sabu.SocketService) {
	sse.streams.Stream(server)
}

// ServeHTTP implements the http.Handler interface.
//
// It collects all values from http.Request.ParseForm() as params map
// and calls them with SSEServer.Handle.
func (sse *SSEServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.ToLower(r.Method) == "head" {
		w.Header().Add("X-Service-Type", "Server Sent Events")
		w.Header().Add("X-Service-Name", "SabuHP STREAMS")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var param = sabu.Params{}
	sse.Handle(w, r, param)
}

func (sse *SSEServer) Handle(w http.ResponseWriter, r *http.Request, p sabu.Params) {
	var clientId = r.Header.Get(ClientIdentificationHeader)

	var stack = njson.Log(sse.logger)

	stack.New().
		LInfo().
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
				LError().
				Message("failed to send message into transport").
				String("error", nerror.WrapOnly(cerr).Error()).
				End()
		}

		stack.New().
			LError().
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
				LError().
				Message("failed to send message into transport").
				String("error", nerror.WrapOnly(cerr).Error()).
				End()
		}

		stack.New().
			LError().
			Message("failed to send message on transport").
			String("error", nerror.WrapOnly(cerr).Error()).
			End()
		return
	}

	stack.New().
		LInfo().
		Message("valid client id provided").
		String("client_id", clientId).
		End()

	sse.ssl.RLock()
	var existingSocket, hasSocket = sse.sockets[clientId]
	sse.ssl.RUnlock()

	if !hasSocket {
		var socket = NewSSESocket(
			clientId,
			sse.ctx,
			r,
			w,
			p,
			sse.codec,
			sse.logger,
			sse.optionalHeaders,
		)

		stack.New().
			LInfo().
			Message("starting sse socket").
			String("client_id", clientId).
			End()

		sse.streams.SocketOpened(socket)

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
				LError().
				Message("failed to send message on transport").
				String("error", nerror.WrapOnly(startSocketErr).Error()).
				End()

			sse.streams.SocketClosed(socket)
			return
		}

		stack.New().
			LInfo().
			Message("started sse socket").
			String("client_id", clientId).
			End()

		sse.ssl.Lock()
		sse.sockets[clientId] = socket
		sse.ssl.Unlock()

		socket.Wait()

		sse.streams.SocketClosed(socket)

		return
	}

	stack.New().
		LInfo().
		Message("existing sse client socket found, must be a request").
		String("client_id", clientId).
		End()

	if deliveryErr := existingSocket.readRequest(r); deliveryErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			w,
			deliveryErr,
			"Failed to send request body",
			http.StatusInternalServerError,
		); err != nil {
			stack.New().
				LError().
				Message("failed to write delivery error to response").
				String("error", nerror.WrapOnly(deliveryErr).Error()).
				End()
		}

		stack.New().
			LError().
			Message("failed to send deliver request body").
			String("error", nerror.WrapOnly(deliveryErr).Error()).
			End()
		return
	}

	stack.New().
		LInfo().
		Message("delivered message to sse socket").
		String("client_id", clientId).
		End()

	w.WriteHeader(http.StatusNoContent)
}

var _ sabu.Socket = (*SSESocket)(nil)

type SSESocket struct {
	clientId   string
	xid        nxid.ID
	logger     sabu.Logger
	req        *http.Request
	res        http.ResponseWriter
	params     sabu.Params
	codec      sabu.Codec
	handlers   *sabu.Sock
	flusher    http.Flusher
	sentMsgs   chan *sabu.Message
	rcvMsgs    chan *sabu.Message
	ctx        context.Context
	canceler   context.CancelFunc
	waiter     sync.WaitGroup
	headers    sabu.HeaderModifications
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
	params sabu.Params,
	codec sabu.Codec,
	logger sabu.Logger,
	optionalHeaders sabu.HeaderModifications,
) *SSESocket {
	var newCtx, newCanceler = context.WithCancel(ctx)
	return &SSESocket{
		req:      r,
		res:      w,
		logger:   logger,
		clientId: clientId,
		ctx:      newCtx,
		params:   params,
		codec:    codec,
		remoteAddr: &sseAddr{
			network: "tcp",
			addr:    r.RemoteAddr,
		},
		localAddr: &sseAddr{
			network: "tcp",
			addr:    r.Host,
		},
		xid:      nxid.New(),
		canceler: newCanceler,
		headers:  optionalHeaders,
		handlers: sabu.NewSock(nil),
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

func (se *SSESocket) Stat() sabu.SocketStat {
	var stat sabu.SocketStat
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

func (se *SSESocket) Send(messages ...sabu.Message) {
	for _, msg := range messages {
		se.sendWrite(msg)
	}
}

func (se *SSESocket) Listen(handler sabu.SocketMessageHandler) {
	se.handlers.Use(handler)
}

func (se *SSESocket) Wait() {
	se.waiter.Wait()
}

func (se *SSESocket) Stop() {
	se.canceler()
}

func (se *SSESocket) readRequest(req *http.Request) error {
	var stack = njson.Log(se.logger)

	stack.New().
		LInfo().
		Message("about to read request body").
		End()

	var buffer bytes.Buffer
	if _, terr := io.Copy(&buffer, req.Body); terr != nil {
		se.res.WriteHeader(http.StatusBadRequest)
		if err := utils.CreateError(
			se.res,
			terr,
			"Failed to read request body",
			http.StatusBadRequest,
		); err != nil {
			stack.New().
				LError().
				Message("failed to read request body").
				String("error", nerror.WrapOnly(terr).Error()).
				End()
		}

		return nerror.WrapOnly(terr)
	}

	stack.New().
		LInfo().
		Message("read request body").
		Int("body_size", buffer.Len()).
		End()

	if buffer.Len() == 0 {
		return nil
	}

	stack.New().
		LInfo().
		Message("decode request body").
		Int("body_size", buffer.Len()).
		End()

	var wrappedPayload, wrappedPayloadErr = se.codec.Decode(buffer.Bytes())
	if wrappedPayloadErr != nil {
		se.res.WriteHeader(http.StatusBadRequest)
		var wrappedErr = nerror.WrapOnly(wrappedPayloadErr)

		if err := utils.CreateError(
			se.res,
			wrappedPayloadErr,
			"Failed to read request body",
			http.StatusBadRequest,
		); err != nil {
			stack.New().
				LError().
				Message("failed to read request body").
				Error("error", wrappedErr).
				End()
		}

		return nerror.WrapOnly(wrappedErr)
	}

	stack.New().
		LInfo().
		Message("notify handler with decoded request body").
		Int("body_size", buffer.Len()).
		End()

	if deliveryErr := se.handlers.Notify(wrappedPayload, se); deliveryErr != nil {
		se.res.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			se.res,
			deliveryErr,
			"Failed to send request body",
			http.StatusInternalServerError,
		); err != nil {
			stack.New().
				LError().
				Message("failed to write delivery error to response").
				String("error", nerror.WrapOnly(deliveryErr).Error()).
				End()
			return err
		}
		return nil
	}
	return nil
}

func (se *SSESocket) Start() error {
	// get response flusher
	var flusher, isFlusher = se.res.(http.Flusher)
	if !isFlusher {
		return nerror.New("ResponseWriter object is not a http.Flusher")
	}

	se.flusher = flusher

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

	if err := se.readRequest(se.req); err != nil {
		return nerror.WrapOnly(err)
	}

	se.flusher.Flush()

	se.waiter.Add(1)
	go func() {
		<-se.ctx.Done()
		se.waiter.Done()
	}()
	return nil
}

func (se *SSESocket) sendWrite(msg sabu.Message) {
	var encodedMessage, encodeErr = se.codec.Encode(msg)
	if encodeErr != nil {
		if msg.Future != nil {
			msg.Future.WithError(nerror.WrapOnly(encodeErr))
		}
		return
	}

	var builder strings.Builder
	builder.Reset()
	builder.WriteString("event: ")
	builder.WriteString(msg.ContentType)
	builder.WriteString("\n")
	builder.WriteString("data: ")
	builder.Write(encodedMessage)
	builder.WriteString("\n\n")

	var stack = njson.Log(se.logger)

	stack.New().
		LInfo().
		Message("sending new data into writer").
		String("data", builder.String()).
		End()

	if sentCount, writeErr := se.res.Write(nunsafe.String2Bytes(builder.String())); writeErr != nil {
		stack.New().
			LError().
			Message("failed to write data to http response writer").
			String("error", nerror.WrapOnly(writeErr).Error()).
			Int("written", sentCount).
			End()
		if msg.Future != nil {
			msg.Future.WithError(nerror.WrapOnly(writeErr))
		}
		return
	}

	se.flusher.Flush()

	if msg.Future != nil {
		msg.Future.WithValue(nil)
	}
}
