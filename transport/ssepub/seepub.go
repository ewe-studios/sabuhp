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
	ClientIdentificationHeader = "X-SSE-Client-Id"
	LastEventIdListHeader      = "X-SSE-Last-Event-Ids"

	eventHeader = "event: "
)

var doubleLine = []byte("\n\n")

var _ sabuhp.Handler = (*SSEServer)(nil)

func ManagedSSEServer(
	ctx context.Context,
	logger sabuhp.Logger,
	manager *managers.Manager,
	optionalHeaders HeaderModifications,
	codec sabuhp.Codec,
) *SSEServer {
	return &SSEServer{
		ctx:             ctx,
		logger:          logger,
		manager:         manager,
		codec:           codec,
		optionalHeaders: optionalHeaders,
		sockets:         map[string]*SSESocket{},
	}
}

type SSEServer struct {
	logger          sabuhp.Logger
	codec           sabuhp.Codec
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
	if strings.ToLower(r.Method) == "head" {
		w.Header().Add("X-Service-Type", "Server Sent Events")
		w.Header().Add("X-Service-Name", "SabuHP STREAMS")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var param = sabuhp.Params{}
	sse.Handle(w, r, param)
}

func (sse *SSEServer) Handle(w http.ResponseWriter, r *http.Request, p sabuhp.Params) {
	var clientId = r.Header.Get(ClientIdentificationHeader)

	var stack = njson.Log(sse.logger)
	defer njson.ReleaseLogStack(stack)

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
		stack.New().
			LInfo().
			Message("creating new sse socket for request").
			String("client_id", clientId).
			End()

		var socket = NewSSESocket(
			clientId,
			sse.ctx,
			r,
			w,
			p,
			sse.codec,
			sse.logger,
			sse.manager,
			sse.optionalHeaders,
		)

		stack.New().
			LInfo().
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
				LError().
				Message("failed to send message on transport").
				String("error", nerror.WrapOnly(startSocketErr).Error()).
				End()
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

		stack.New().
			LInfo().
			Message("added sse socket client into registry").
			String("client_id", clientId).
			End()

		stack.New().
			LInfo().
			Message("inform manager for new socket").
			String("client_id", clientId).
			End()

		sse.manager.ManageSocketOpened(socket)

		stack.New().
			LInfo().
			Message("informed manager for new socket").
			String("client_id", clientId).
			End()

		stack.New().
			LInfo().
			Message("await socket closure").
			String("client_id", clientId).
			End()

		socket.Wait()

		sse.manager.ManageSocketClosed(socket)

		stack.New().
			LInfo().
			Message("socket sse closed by connection").
			String("client_id", clientId).
			End()
		return
	}

	stack.New().
		LInfo().
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
				LError().
				Message("failed to read request body").
				String("error", nerror.WrapOnly(terr).Error()).
				End()
		}

		stack.New().
			LError().
			Message("failed to read request body").
			String("error", nerror.WrapOnly(terr).Error()).
			End()
		return
	}

	stack.New().
		LInfo().
		Message("copied request from body").
		String("client_id", clientId).
		Bytes("message", buffer.Bytes()).
		End()

	var wrappedPayload, wrappedPayloadErr = sse.codec.Decode(buffer.Bytes())
	if wrappedPayloadErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		var wrappedErr = nerror.WrapOnly(wrappedPayloadErr)
		if err := utils.CreateError(
			w,
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

		stack.New().
			LError().
			Message("failed to read request body").
			Error("error", wrappedErr).
			End()
		return
	}

	if deliveryErr := existingSocket.SendRead(wrappedPayload, 0); deliveryErr != nil {
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
		Bytes("message", buffer.Bytes()).
		End()

	w.WriteHeader(http.StatusNoContent)
}

var _ sabuhp.Socket = (*SSESocket)(nil)

type HeaderModifications func(header http.Header)

type SSESocket struct {
	clientId   string
	xid        nxid.ID
	logger     sabuhp.Logger
	req        *http.Request
	res        http.ResponseWriter
	params     sabuhp.Params
	codec      sabuhp.Codec
	sentMsgs   chan sseSend
	rcvMsgs    chan *sabuhp.Message
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
	params sabuhp.Params,
	codec sabuhp.Codec,
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
		headers:  optionalHeaders,
		canceler: newCanceler,
		sentMsgs: make(chan sseSend),
		rcvMsgs:  make(chan *sabuhp.Message),
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

type sseSend struct {
	Data   []byte
	Meta   sabuhp.MessageMeta
	writer *sabuhp.SocketWriterTo
}

func (se *SSESocket) Send(msg []byte, meta sabuhp.MessageMeta, timeout time.Duration) error {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	var reqCtx = se.req.Context()
	select {
	case se.sentMsgs <- sseSend{
		Data:   msg,
		writer: nil,
		Meta:   meta,
	}:
		return nil
	case <-timeoutChan: // nil channel will be ignored
		return nerror.New("message delivery timeout")
	case <-reqCtx.Done():
		return nerror.WrapOnly(reqCtx.Err())
	case <-se.ctx.Done():
		return nerror.WrapOnly(se.ctx.Err())
	}
}

func (se *SSESocket) SendWriter(msgWriter io.WriterTo, meta sabuhp.MessageMeta, timeout time.Duration) sabuhp.ErrorWaiter {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	var socketWriter = sabuhp.NewSocketWriterTo(msgWriter)
	select {
	case <-se.req.Context().Done():
		socketWriter.Abort(se.req.Context().Err())
		se.canceler()
		return socketWriter
	case <-timeoutChan: // nil channel will be ignored
		socketWriter.Abort(nerror.New("message delivery timeout"))
		se.canceler()
		return socketWriter
	case <-se.ctx.Done():
		socketWriter.Abort(nerror.New("not receiving anymore messages"))
		se.canceler()
		return socketWriter
	case se.sentMsgs <- sseSend{
		Data:   nil,
		writer: socketWriter,
		Meta:   meta,
	}:
		return socketWriter
	}
}

func (se *SSESocket) SendRead(msg *sabuhp.Message, timeout time.Duration) error {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	var reqCtx = se.req.Context()
	select {
	case se.rcvMsgs <- msg:
		return nil
	case <-timeoutChan: // nil channel will be ignored
		return nerror.New("message delivery timeout")
	case <-reqCtx.Done():
		return nerror.WrapOnly(reqCtx.Err())
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
				LInfo().
				Message("received new data from client").
				Object("message", msg).
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

func (se *SSESocket) manageWrites(flusher http.Flusher) {
	defer se.waiter.Done()

	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

	var requestContext = se.req.Context()

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

			if msg.writer != nil {
				if err := se.sendWriterTo(msg.writer, msg.Meta); err != nil {
					stack.New().
						LError().
						Message("write failed").
						String("error", err.Error()).
						End()
				}

				// flush content into response writer.
				flusher.Flush()

				continue
			}

			if err := se.sendWrite(msg.Data, msg.Meta); err != nil {
				stack.New().
					LError().
					Message("write failed").
					String("error", err.Error()).
					End()
			}

			// flush content into response writer.
			flusher.Flush()
		}
	}
}

func (se *SSESocket) sendWrite(msg []byte, meta sabuhp.MessageMeta) error {
	var builder strings.Builder
	builder.Reset()
	builder.WriteString("event: ")
	builder.WriteString(meta.ContentType)
	builder.WriteString("\n")
	builder.WriteString("data: ")
	builder.Write(msg)
	builder.WriteString("\n\n")

	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

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
		return writeErr
	}
	return nil
}

func (se *SSESocket) sendWriterTo(writer *sabuhp.SocketWriterTo, meta sabuhp.MessageMeta) error {
	var builder strings.Builder
	builder.Reset()
	builder.WriteString("event: ")
	builder.WriteString(meta.ContentType)
	builder.WriteString("\n")
	builder.WriteString("data: ")

	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

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
		return writeErr
	}

	if sentCount, writeErr := writer.WriteTo(se.res); writeErr != nil {
		stack.New().
			LError().
			Message("failed to write data from WriterTo to http response writer").
			String("error", nerror.WrapOnly(writeErr).Error()).
			Int64("written", sentCount).
			End()
		return writeErr
	}

	if sentCount, writeErr := se.res.Write(doubleLine); writeErr != nil {
		stack.New().
			LError().
			Message("failed to write data ending newlines to response writer").
			String("error", nerror.WrapOnly(writeErr).Error()).
			Int("written", sentCount).
			End()
		return writeErr
	}
	return nil
}
