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

	"github.com/influx6/sabuhp/pubsub"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/sabuhp"
)

const (
	MinClientIdLength          = 7
	SSEStreamHeader            = "event: sse-streams"
	ClientIdentificationHeader = "X-SEE-ClientId"
	LastEventIdListHeader      = "X-SSE-Last-Event-IDS"
)

var _ sabuhp.Handler = (*SSEServer)(nil)

func ManagedSSEServer(
	ctx context.Context,
	logger sabuhp.Logger,
	manager *pubsub.Manager,
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
	manager         *pubsub.Manager
	ssl             sync.RWMutex
	sockets         map[string]*SSESocket
}

func (sse *SSEServer) Handle(w http.ResponseWriter, r *http.Request, p sabuhp.Params) {
	var clientId = r.Header.Get(ClientIdentificationHeader)
	clientIdCount := len(clientId)
	if clientIdCount == 0 {
		var cerr = nerror.New("Request does not have the client identification header")
		w.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			w,
			cerr, "Failed to decode request into message",
			http.StatusInternalServerError,
		); err != nil {
			njson.Log(sse.logger).New().
				Message("failed to send message into transport").
				String("error", nerror.WrapOnly(cerr).Error()).
				End()
		}

		njson.Log(sse.logger).New().
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
			njson.Log(sse.logger).New().
				Message("failed to send message into transport").
				String("error", nerror.WrapOnly(cerr).Error()).
				End()
		}

		njson.Log(sse.logger).New().
			Message("failed to send message on transport").
			String("error", nerror.WrapOnly(cerr).Error()).
			End()
		return
	}

	sse.ssl.RLock()
	var existingSocket, hasSocket = sse.sockets[clientId]
	sse.ssl.RUnlock()

	if !hasSocket {
		var socket = NewSSESocket(
			clientId,
			sse.ctx,
			r,
			w,
			sse.logger,
			sse.manager,
			sse.optionalHeaders,
		)
		if startSocketErr := socket.Start(); startSocketErr != nil {
			w.WriteHeader(http.StatusInternalServerError)
			if err := utils.CreateError(
				w,
				startSocketErr,
				"Failed to decode request into message",
				http.StatusInternalServerError,
			); err != nil {
				njson.Log(sse.logger).New().
					Message("failed to send message into transport").
					String("error", nerror.WrapOnly(startSocketErr).Error()).
					End()
			}

			njson.Log(sse.logger).New().
				Message("failed to send message on transport").
				String("error", nerror.WrapOnly(startSocketErr).Error()).
				End()
			return
		}

		sse.ssl.Lock()
		sse.sockets[clientId] = socket
		sse.ssl.Unlock()

		sse.manager.ManageSocketOpened(socket)

		socket.Wait()

		sse.manager.ManageSocketClosed(socket)
	}

	var buffer bytes.Buffer
	if _, terr := io.Copy(&buffer, r.Body); terr != nil {
		w.WriteHeader(http.StatusBadRequest)
		if err := utils.CreateError(
			w,
			terr,
			"Failed to read request body",
			http.StatusBadRequest,
		); err != nil {
			njson.Log(sse.logger).New().
				Message("failed to read request body").
				String("error", nerror.WrapOnly(terr).Error()).
				End()
		}

		njson.Log(sse.logger).New().
			Message("failed to read request body").
			String("error", nerror.WrapOnly(terr).Error()).
			End()
		return
	}

	if deliveryErr := existingSocket.SendRead(buffer.Bytes(), 0); deliveryErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		if err := utils.CreateError(
			w,
			deliveryErr,
			"Failed to send request body",
			http.StatusInternalServerError,
		); err != nil {
			njson.Log(sse.logger).New().
				Message("failed to write delivery error to response").
				String("error", nerror.WrapOnly(deliveryErr).Error()).
				End()
		}

		njson.Log(sse.logger).New().
			Message("failed to send deliver request body").
			String("error", nerror.WrapOnly(deliveryErr).Error()).
			End()
		return
	}
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
	manager    *pubsub.Manager
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
	manager *pubsub.Manager,
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
		xid:      nxid.New(),
		headers:  optionalHeaders,
		canceler: newCanceler,
		sentMsgs: make(chan []byte),
		rcvMsgs:  make(chan []byte),
	}
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

func (se *SSESocket) Send(bytes []byte, duration time.Duration) error {
	atomic.AddInt64(&se.sent, 1)
	return nil
}

func (se *SSESocket) SendRead(bytes []byte, duration time.Duration) error {
	atomic.AddInt64(&se.received, 1)
	return nil
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

	// Set the headers related to event streaming.
	se.res.Header().Set("Content-Type", "text/event-stream")
	se.res.Header().Set("Cache-Control", "no-cache")
	se.res.Header().Set("Connection", "keep-alive")
	se.res.Header().Set("Transfer-Encoding", "chunked")
	se.res.Header().Set("Access-Control-Allow-Origin", "*")

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

doLoop:
	for {
		select {
		case <-requestContext.Done():
			break doLoop
		case <-se.ctx.Done():
			break doLoop
		case msg := <-se.rcvMsgs:
			njson.Log(se.logger).New().
				Message("received new data from client").
				Bytes("data", msg).
				End()

			if handleErr := se.manager.HandleSocketMessage(msg, se); handleErr != nil {
				njson.Log(se.logger).New().
					Message("failed handle socket message").
					String("error", nerror.WrapOnly(handleErr).Error()).
					End()
			}
		}
	}
}

func (se *SSESocket) manageWrites(flusher http.Flusher) {
	defer se.waiter.Done()

	var requestContext = se.req.Context()
	var builder strings.Builder

doLoop:
	for {
		select {
		case <-requestContext.Done():
			break doLoop
		case <-se.ctx.Done():
			break doLoop
		case msg := <-se.sentMsgs:
			builder.Reset()
			builder.WriteString(SSEStreamHeader)
			builder.WriteString("\n")
			builder.WriteString("data: ")
			builder.Write(msg)
			builder.WriteString("\n\n")

			njson.Log(se.logger).New().
				Info().
				Message("sending new data into writer").
				String("data", builder.String()).
				End()

			if _, writeErr := se.res.Write(nunsafe.String2Bytes(builder.String())); writeErr != nil {
				njson.Log(se.logger).New().
					Error().
					Message("failed to write data to http response writer").
					String("error", nerror.WrapOnly(writeErr).Error()).
					End()
			}

			// flush content into response writer.
			flusher.Flush()
		}
	}
}
