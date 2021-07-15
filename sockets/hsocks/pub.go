package hsocks

import (
	"context"
	"github.com/ewe-studios/sabuhp/sabu"
	"net"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/ewe-studios/sabuhp/httpub"

	"github.com/influx6/npkg/njson"

	"github.com/ewe-studios/sabuhp/utils"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg/nerror"
)

const (
	ClientIdentificationHeader = "X-SSE-Client-Id"
)

var _ sabu.Handler = (*HttpServlet)(nil)

func ManagedHttpServlet(
	ctx context.Context,
	logger sabu.Logger,
	decoder sabu.HttpDecoder,
	encoder sabu.HttpEncoder,
	optionalHeaders sabu.HeaderModifications,
	bus sabu.MessageBus,
) *HttpServlet {
	return &HttpServlet{
		bus:       bus,
		ctx:       ctx,
		logger:    logger,
		encoder:   encoder,
		decoder:   decoder,
		headerMod: optionalHeaders,
		streams:   sabu.NewSocketServers(),
	}
}

type HttpServlet struct {
	logger    sabu.Logger
	decoder   sabu.HttpDecoder
	encoder   sabu.HttpEncoder
	headerMod sabu.HeaderModifications
	ctx       context.Context
	streams   *sabu.SocketServers
	bus       sabu.MessageBus
}

func (htp *HttpServlet) Bus(bus sabu.MessageBus) {
	htp.bus = bus
}

func (htp *HttpServlet) Stream(server sabu.SocketService) {
	htp.streams.Stream(server)
}

// ServeHTTP implements the http.Handler interface.
//
// It collects all values from http.Request.ParseForm() as params map
// and calls them with HttpServlet.Handle.
func (htp *HttpServlet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.ToLower(r.Method) == "head" {
		w.Header().Add("X-Service-Name", "SabuHP HTTP")
		w.WriteHeader(httpub.StatusNoContent)
		return
	}

	if parseErr := r.ParseForm(); parseErr != nil {
		njson.Log(htp.logger).New().
			Message("failed to parse forms, might be non form request").
			String("error", nerror.WrapOnly(parseErr).Error()).
			End()
		return
	}

	var param = sabu.Params{}
	for key := range r.Form {
		param.Set(key, r.Form.Get(key))
	}

	htp.Handle(w, r, param)
}

func (htp *HttpServlet) Handle(w http.ResponseWriter, r *http.Request, p sabu.Params) {
	htp.HandleMessage(w, r, p, "", nil)
}

func (htp *HttpServlet) HandleWithResponder(w http.ResponseWriter, r *http.Request, p sabu.Params, handler sabu.SocketMessageHandler) {
	htp.HandleMessage(w, r, p, "", handler)
}

// HandleMessage implements necessary logic to handle an incoming request and response life cycle.
func (htp *HttpServlet) HandleMessage(
	w http.ResponseWriter,
	r *http.Request,
	p sabu.Params,
	asEvent string,
	handler sabu.SocketMessageHandler,
) {
	var clientId = r.Header.Get(ClientIdentificationHeader)

	var stack = njson.Log(htp.logger)

	stack.New().
		LInfo().
		Message("creating new http socket for request").
		String("client_id", clientId).
		End()

	var socket = NewServletSocket(
		htp.ctx,
		clientId,
		htp.streams,
		r,
		w,
		p,
		htp.logger,
		htp.decoder,
		htp.encoder,
		htp.headerMod,
		asEvent,
		handler,
	)

	stack.New().
		LInfo().
		Message("starting http socket").
		String("client_id", clientId).
		End()

	htp.streams.SocketOpened(socket)

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
				String("error", nerror.WrapOnly(err).Error()).
				End()
		}

		stack.New().
			LError().
			Message("failed to send start http socket properly").
			String("error", nerror.WrapOnly(startSocketErr).Error()).
			End()

		htp.streams.SocketClosed(socket)
		return
	}

	stack.New().
		LInfo().
		Message("started http socket").
		String("client_id", clientId).
		End()

	stack.New().
		LInfo().
		Message("await socket closure").
		String("client_id", clientId).
		End()

	socket.Wait()

	if flushErr := socket.flush(); flushErr != nil {
		w.WriteHeader(http.StatusInternalServerError)

		stack.New().
			LError().
			Message("failed to flush message to transport").
			String("error", nerror.WrapOnly(flushErr).Error()).
			End()

		htp.streams.SocketClosed(socket)
		return
	}

	htp.streams.SocketClosed(socket)

	stack.New().
		LInfo().
		Message("socket http closed by connection").
		String("client_id", clientId).
		End()

	stack.New().
		LInfo().
		Message("delivered message to http socket").
		String("client_id", clientId).
		End()
}

var _ sabu.Socket = (*ServletSocket)(nil)

type ServletSocket struct {
	clientId   string
	xid        nxid.ID
	asEvent    string
	streams    *sabu.SocketServers
	logger     sabu.Logger
	req        *http.Request
	res        http.ResponseWriter
	ctx        context.Context
	params     sabu.Params
	canceler   context.CancelFunc
	decoder    sabu.HttpDecoder
	encoder    sabu.HttpEncoder
	headers    sabu.HeaderModifications
	handler    sabu.SocketMessageHandler
	remoteAddr net.Addr
	localAddr  net.Addr
	sent       int64
	handled    int64
	received   int64
}

func NewServletSocket(
	ctx context.Context,
	clientId string,
	streams *sabu.SocketServers,
	r *http.Request,
	w http.ResponseWriter,
	params sabu.Params,
	logger sabu.Logger,
	decoder sabu.HttpDecoder,
	encoder sabu.HttpEncoder,
	headerMod sabu.HeaderModifications,
	asEvent string,
	handler sabu.SocketMessageHandler,
) *ServletSocket {
	var newCtx, newCanceler = context.WithCancel(ctx)
	return &ServletSocket{
		req:      r,
		res:      w,
		streams:  streams,
		asEvent:  asEvent,
		logger:   logger,
		clientId: clientId,
		ctx:      newCtx,
		decoder:  decoder,
		encoder:  encoder,
		params:   params,
		handler:  handler,
		xid:      nxid.New(),
		headers:  headerMod,
		canceler: newCanceler,
		remoteAddr: &httpAddr{
			network: "tcp",
			addr:    r.RemoteAddr,
		},
		localAddr: &httpAddr{
			network: "tcp",
			addr:    r.Host,
		},
	}
}

type httpAddr struct {
	network string
	addr    string
}

func (se httpAddr) String() string {
	return se.addr
}

func (se httpAddr) Network() string {
	return se.network
}

func (se *ServletSocket) ID() nxid.ID {
	return se.xid
}

func (se *ServletSocket) Stat() sabu.SocketStat {
	var stat sabu.SocketStat
	stat.Id = se.xid.String()
	stat.Addr = se.localAddr
	stat.RemoteAddr = se.remoteAddr
	stat.Sent = atomic.LoadInt64(&se.sent)
	stat.Handled = atomic.LoadInt64(&se.handled)
	stat.Received = atomic.LoadInt64(&se.received)
	return stat
}

func (se *ServletSocket) RemoteAddr() net.Addr {
	return se.remoteAddr
}

func (se *ServletSocket) LocalAddr() net.Addr {
	return se.localAddr
}

func (se *ServletSocket) Send(msgs ...sabu.Message) {
	for _, msg := range msgs {
		var encodeErr = se.encoder.Encode(se.res, msg)
		if msg.Future != nil {
			if encodeErr != nil {
				msg.Future.WithError(encodeErr)
				continue
			}
			msg.Future.WithValue(nil)
		}
	}
	se.Stop()
}

func (se *ServletSocket) Wait() {
	select {
	case <-se.req.Context().Done():
		return
	case <-se.ctx.Done():
		return
	}
}

func (se *ServletSocket) Stop() {
	se.canceler()
}

func (se *ServletSocket) flush() error {
	se.res.WriteHeader(http.StatusOK)
	if se.headers != nil {
		se.headers(se.res.Header())
	}
	return nil
}

func (se *ServletSocket) Conn() sabu.Conn {
	return se.req
}

func (se *ServletSocket) Listen(handler sabu.SocketMessageHandler) {
	se.handler = handler
}

func (se *ServletSocket) Start() error {
	if se.handler == nil {
		return nerror.New("failed to start servlet socket")
	}

	var stack = njson.Log(se.logger)

	var decodedMessage, decodedErr = se.decoder.Decode(se.req, se.params)
	if decodedErr != nil {
		var statusCode int
		if nerror.IsAny(decodedErr, sabu.BodyToLargeErr) {
			statusCode = http.StatusRequestEntityTooLarge
		} else {
			statusCode = http.StatusBadRequest
		}

		se.res.WriteHeader(statusCode)
		if err := utils.CreateError(
			se.res,
			decodedErr,
			"Failed to read request body",
			statusCode,
		); err != nil {
			stack.New().
				LError().
				Message("failed to read request body").
				String("error", nerror.WrapOnly(err).Error()).
				End()
			return err
		}

		stack.New().
			LError().
			Message("failed to read request body").
			String("error", nerror.WrapOnly(decodedErr).Error()).
			End()

		return decodedErr
	}

	// if we have being scoped to specific event name, use that.
	if se.asEvent != "" {
		decodedMessage.Topic = sabu.T(se.asEvent)
	}

	// overridingHandler overrides sending message to the manager
	// by using provided TransportResponse to handle message.
	var handleErr = se.handler(decodedMessage, se)
	if handleErr != nil {
		stack.New().
			Message("failed handle socket message").
			String("error", nerror.WrapOnly(handleErr).Error()).
			End()

		if mh, ok := handleErr.(sabu.MessageErr); ok {
			se.res.WriteHeader(mh.StatusCode())
		}

		return handleErr
	}

	stack.New().
		LInfo().
		Message("scheduling flush of data to client socket").
		End()

	return nil
}
