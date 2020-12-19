package hsocks

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influx6/sabuhp/transport/httpub"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/sabuhp/utils"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp/managers"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/sabuhp"
)

const (
	ClientIdentificationHeader = "X-SSE-Client-Id"
)

var _ sabuhp.Handler = (*HttpServlet)(nil)

func ManagedHttpServlet(
	ctx context.Context,
	logger sabuhp.Logger,
	transposer sabuhp.Transposer,
	manager *managers.Manager,
	optionalHeaders sabuhp.HeaderModifications,
) *HttpServlet {
	return &HttpServlet{
		ctx:             ctx,
		logger:          logger,
		manager:         manager,
		transposer:      transposer,
		optionalHeaders: optionalHeaders,
	}
}

type HttpServlet struct {
	logger          sabuhp.Logger
	transposer      sabuhp.Transposer
	optionalHeaders sabuhp.HeaderModifications
	ctx             context.Context
	manager         *managers.Manager
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

	var param = sabuhp.Params{}
	for key := range r.Form {
		param.Set(key, r.Form.Get(key))
	}

	htp.Handle(w, r, param)
}

func (htp *HttpServlet) Handle(w http.ResponseWriter, r *http.Request, p sabuhp.Params) {
	htp.HandleMessage(w, r, p, "", nil)
}

func (htp *HttpServlet) HandleWithResponder(w http.ResponseWriter, r *http.Request, p sabuhp.Params, responder sabuhp.TransportResponse) {
	htp.HandleMessage(w, r, p, "", responder)
}

// HandleMessage implements necessary logic to handle an incoming request and response life cycle.
//
func (htp *HttpServlet) HandleMessage(
	w http.ResponseWriter,
	r *http.Request,
	p sabuhp.Params,
	asEvent string,
	overrideResponder sabuhp.TransportResponse,
) {
	var clientId = r.Header.Get(ClientIdentificationHeader)

	var stack = njson.Log(htp.logger)
	defer njson.ReleaseLogStack(stack)

	stack.New().
		LInfo().
		Message("creating new http socket for request").
		String("client_id", clientId).
		End()

	var socket = NewServletSocket(
		clientId,
		htp.ctx,
		r,
		w,
		p,
		htp.logger,
		htp.transposer,
		htp.manager,
		htp.optionalHeaders,
		overrideResponder,
		asEvent,
	)

	stack.New().
		LInfo().
		Message("starting http socket").
		String("client_id", clientId).
		End()

	htp.manager.ManageSocketOpened(socket)

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
		return
	}

	htp.manager.ManageSocketClosed(socket)

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

var _ sabuhp.Socket = (*ServletSocket)(nil)

type ServletSocket struct {
	clientId          string
	xid               nxid.ID
	asEvent           string
	logger            sabuhp.Logger
	req               *http.Request
	res               http.ResponseWriter
	ctx               context.Context
	params            sabuhp.Params
	canceler          context.CancelFunc
	codec             sabuhp.Codec
	manager           *managers.Manager
	transposer        sabuhp.Transposer
	headers           sabuhp.HeaderModifications
	overridingHandler sabuhp.TransportResponse
	remoteAddr        net.Addr
	localAddr         net.Addr

	sent     int64
	handled  int64
	received int64
}

func NewServletSocket(
	clientId string,
	ctx context.Context,
	r *http.Request,
	w http.ResponseWriter,
	params sabuhp.Params,
	logger sabuhp.Logger,
	transposer sabuhp.Transposer,
	manager *managers.Manager,
	optionalHeaders sabuhp.HeaderModifications,
	overridingHandler sabuhp.TransportResponse,
	asEvent string,
) *ServletSocket {
	var newCtx, newCanceler = context.WithCancel(ctx)
	return &ServletSocket{
		req:               r,
		res:               w,
		asEvent:           asEvent,
		logger:            logger,
		clientId:          clientId,
		ctx:               newCtx,
		transposer:        transposer,
		manager:           manager,
		params:            params,
		codec:             manager.Codec(),
		xid:               nxid.New(),
		headers:           optionalHeaders,
		overridingHandler: overridingHandler,
		canceler:          newCanceler,
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

func (se *ServletSocket) Stat() sabuhp.SocketStat {
	var stat sabuhp.SocketStat
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

// SendWriter implements the necessary method to send data across the writer to the
// underline response object.
func (se *ServletSocket) SendWriter(msgWriter io.WriterTo, timeout time.Duration) sabuhp.ErrorWaiter {
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
	default:
		// add new data into the write buffer
		_, _ = socketWriter.WriteTo(se.res)
	}

	se.canceler()
	return socketWriter
}

func (se *ServletSocket) Send(msg []byte, timeout time.Duration) error {
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case <-se.req.Context().Done():
		return nerror.New("not receiving anymore messages")
	case <-timeoutChan: // nil channel will be ignored
		return nerror.New("message delivery timeout")
	case <-se.ctx.Done():
		return nerror.New("not receiving anymore messages")
	default:
		// add new data into the write buffer
		if _, err := se.res.Write(msg); err != nil {
			return nerror.WrapOnly(err)
		}
	}

	// close write channel
	se.canceler()
	return nil
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

func (se *ServletSocket) Conn() sabuhp.Conn {
	return se.req
}

func (se *ServletSocket) Listen(_ string, _ sabuhp.TransportResponse) sabuhp.Channel {
	return &sabuhp.ErrChannel{
		Error: nerror.New("not supported"),
	}
}

func (se *ServletSocket) SendToOne(msg *sabuhp.Message, ts time.Duration) error {
	var encoded, encodedErr = se.codec.Encode(msg)
	if encodedErr != nil {
		return nerror.WrapOnly(encodedErr)
	}

	if sendErr := se.Send(encoded, ts); sendErr != nil {
		return nerror.WrapOnly(sendErr)
	}
	return nil
}

func (se *ServletSocket) SendToAll(msg *sabuhp.Message, ts time.Duration) error {
	return se.SendToOne(msg, ts)
}

func (se *ServletSocket) Start() error {
	var stack = njson.Log(se.logger)
	defer njson.ReleaseLogStack(stack)

	var decodedMessage, decodedErr = se.transposer.Transpose(se.req, se.params)
	if decodedErr != nil {
		se.res.WriteHeader(http.StatusBadRequest)
		if err := utils.CreateError(
			se.res,
			decodedErr,
			"Failed to read request body",
			http.StatusBadRequest,
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

	decodedMessage.LocalPayload = sabuhp.HttpResponseWriterAsPayload(se.res)

	// if we have being scoped to specific event name, use that.
	if se.asEvent != "" {
		decodedMessage.Topic = se.asEvent
	}

	atomic.AddInt64(&se.received, 1)
	stack.New().
		LInfo().
		Message("received new data from client").
		Object("message", decodedMessage).
		End()

	decodedMessage.OverridingTransport = se

	// overridingHandler overrides sending message to the manager
	// by using provided TransportResponse to handle message.
	var handleErr error
	if se.overridingHandler != nil {
		handleErr = se.overridingHandler.Handle(decodedMessage, se)
	} else {
		handleErr = se.manager.HandleSocketMessage(decodedMessage, se)
	}

	if handleErr != nil {
		stack.New().
			Message("failed handle socket message").
			String("error", nerror.WrapOnly(handleErr).Error()).
			End()
		return handleErr
	}

	stack.New().
		LInfo().
		Message("scheduling flush of data to client socket").
		End()

	return nil
}
