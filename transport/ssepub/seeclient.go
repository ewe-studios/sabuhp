package ssepub

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/sabuhp/utils"
)

var (
	newLine         = "\n"
	spaceBytes      = []byte(" ")
	dataHeaderBytes = []byte("data:")
)

type MessageHandler func(message []byte, socket *SSEClient) error

type SSEHub struct {
	maxRetries int
	retryFunc  sabuhp.RetryFunc
	ctx        context.Context
	client     *http.Client
	logging    sabuhp.Logger
}

func NewSSEHub(
	ctx context.Context,
	maxRetries int,
	client *http.Client,
	logging sabuhp.Logger,
	retryFn sabuhp.RetryFunc,
) *SSEHub {
	if client.CheckRedirect == nil {
		client.CheckRedirect = utils.CheckRedirect
	}
	return &SSEHub{ctx: ctx, maxRetries: maxRetries, client: client, retryFunc: retryFn, logging: logging}
}

func (se *SSEHub) Delete(
	handler MessageHandler,
	id nxid.ID,
	route string,
	lastEventIds ...string,
) (*SSEClient, error) {
	return se.For(handler, id, "Delete", route, nil, lastEventIds...)
}

func (se *SSEHub) Patch(
	handler MessageHandler,
	id nxid.ID,
	route string,
	body io.Reader,
	lastEventIds ...string,
) (*SSEClient, error) {
	return se.For(handler, id, "PATCH", route, body, lastEventIds...)
}

func (se *SSEHub) Post(
	handler MessageHandler,
	id nxid.ID,
	route string,
	body io.Reader,
	lastEventIds ...string,
) (*SSEClient, error) {
	return se.For(handler, id, "POST", route, body, lastEventIds...)
}

func (se *SSEHub) Put(
	handler MessageHandler,
	id nxid.ID,
	route string,
	body io.Reader,
	lastEventIds ...string,
) (*SSEClient, error) {
	return se.For(handler, id, "PUT", route, body, lastEventIds...)
}

func (se *SSEHub) Get(handler MessageHandler, id nxid.ID, route string, lastEventIds ...string) (*SSEClient, error) {
	return se.For(handler, id, "GET", route, nil, lastEventIds...)
}

func (se *SSEHub) For(
	handler MessageHandler,
	id nxid.ID,
	method string,
	route string,
	body io.Reader,
	lastEventIds ...string,
) (*SSEClient, error) {
	var header = http.Header{}
	header.Set(ClientIdentificationHeader, id.String())
	header.Set("Cache-Control", "no-cache")
	header.Set("Accept", "text/event-stream")
	if len(lastEventIds) > 0 {
		header.Set(LastEventIdListHeader, strings.Join(lastEventIds, ";"))
	}

	var req, response, err = utils.DoRequest(se.ctx, se.client, method, route, body, header)
	if err != nil {
		return nil, nerror.WrapOnly(err)
	}

	return NewSSEClient(id, se.maxRetries, handler, req, response, se.retryFunc, se.logging, se.client), nil
}

type SSEClient struct {
	id         nxid.ID
	maxRetries int
	logger     sabuhp.Logger
	retryFunc  sabuhp.RetryFunc
	handler    MessageHandler // ensure to copy the bytes if your use will span multiple goroutines
	ctx        context.Context
	canceler   context.CancelFunc
	client     *http.Client
	request    *http.Request
	response   *http.Response
	lastId     nxid.ID
	retry      time.Duration
	waiter     sync.WaitGroup
}

func NewSSEClient(
	id nxid.ID,
	maxRetries int,
	handler MessageHandler,
	req *http.Request,
	res *http.Response,
	retryFn sabuhp.RetryFunc,
	logger sabuhp.Logger,
	reqClient *http.Client,
) *SSEClient {
	if req.Context() == nil {
		panic("Request is required to have a context.Context attached")
	}

	var newCtx, canceler = context.WithCancel(req.Context())
	var client = &SSEClient{
		id:         id,
		maxRetries: maxRetries,
		logger:     logger,
		client:     reqClient,
		retryFunc:  retryFn,
		handler:    handler,
		canceler:   canceler,
		ctx:        newCtx,
		request:    req,
		response:   res,
		retry:      0,
	}

	client.waiter.Add(1)
	go client.run()
	return client
}

// Wait blocks till client and it's managing goroutine closes.
func (sc *SSEClient) Wait() {
	sc.waiter.Wait()
}

func (sc *SSEClient) Send(msg []byte, timeout time.Duration) ([]byte, error) {
	var header = http.Header{}
	header.Set("Cache-Control", "no-cache")
	header.Set(ClientIdentificationHeader, sc.id.String())
	if !sc.lastId.IsNil() {
		header.Set(LastEventIdListHeader, sc.lastId.String())
	}

	var ctx = sc.ctx
	var canceler context.CancelFunc
	if timeout > 0 {
		ctx, canceler = context.WithTimeout(sc.ctx, timeout)
	} else {
		canceler = func() {
		}
	}

	defer canceler()

	var req, response, err = utils.DoRequest(
		ctx,
		sc.client,
		sc.request.Method,
		sc.request.URL.String(),
		bytes.NewReader(msg),
		header,
	)
	if err != nil {
		njson.Log(sc.logger).New().
			Error().
			Message("failed to send request request").
			String("error", nerror.WrapOnly(err).Error()).
			End()
		return nil, nerror.WrapOnly(err)
	}

	njson.Log(sc.logger).New().
		Info().
		Message("sent SSE http request").
		Bytes("msg", msg).
		String("url", req.URL.String()).
		String("method", req.Method).
		String("remote_addr", req.RemoteAddr).
		String("response_status", response.Status).
		Int("response_status_code", response.StatusCode).
		End()

	var responseBody = bytes.NewBuffer(make([]byte, 0, 128))
	if _, readErr := io.Copy(responseBody, response.Body); readErr != nil {
		njson.Log(sc.logger).New().
			Error().
			Message("failed to read response").
			Bytes("msg", msg).
			String("url", req.URL.String()).
			String("method", req.Method).
			String("remote_addr", req.RemoteAddr).
			String("response_status", response.Status).
			Int("response_status_code", response.StatusCode).
			String("error", nerror.WrapOnly(readErr).Error()).
			End()
	}

	if closeErr := response.Body.Close(); closeErr != nil {
		njson.Log(sc.logger).New().
			Error().
			Message("failed to close response body").
			String("error", nerror.WrapOnly(err).Error()).
			End()
	}

	return responseBody.Bytes(), nil
}

// Close closes client's request and response cycle
// and waits till managing goroutine is closed.
func (sc *SSEClient) Close() error {
	sc.canceler()
	sc.waiter.Wait()
	return nil
}

func (sc *SSEClient) run() {
	var normalized = utils.NewNormalisedReader(sc.response.Body)
	var reader = bufio.NewReader(normalized)

	var decoding = false
	var data bytes.Buffer
doLoop:
	for {
		select {
		case <-sc.ctx.Done():
			sc.waiter.Done()
			_ = sc.response.Body.Close()
			return
		default:
			// do nothing.
		}

		var line, lineErr = reader.ReadString('\n')
		if lineErr != nil {
			njson.Log(sc.logger).New().
				Error().
				Message("failed to read more data").
				String("error", nerror.WrapOnly(lineErr).Error()).
				End()
			break doLoop
		}

		if line == "" {
			continue doLoop
		}

		// if we see only a new line then this is the end of
		// an event data section.
		if line == "\n" && decoding {
			decoding = false

			// if we have data, then decode and
			// deliver to handler.
			if data.Len() != 0 {
				njson.Log(sc.logger).New().
					Info().
					Message("received complete data").
					String("data", data.String()).
					End()

				var dataLine = bytes.TrimPrefix(data.Bytes(), dataHeaderBytes)
				dataLine = bytes.TrimPrefix(dataLine, spaceBytes)
				if handleErr := sc.handler(dataLine, sc); handleErr != nil {
					njson.Log(sc.logger).New().
						Error().
						Message("failed to handle message").
						String("error", nerror.WrapOnly(handleErr).Error()).
						End()
				}
			}

			continue doLoop
		}

		if line == "\n" && !decoding {
			continue doLoop
		}

		var stripLine = strings.TrimSpace(line)
		if stripLine == SSEStreamHeader {
			decoding = true
			data.Reset()
			continue
		}

		line = strings.TrimSuffix(line, newLine)
		line = strings.TrimPrefix(line, newLine)
		data.WriteString(line)
	}

	sc.reconnect()
}

func (sc *SSEClient) reconnect() {
	select {
	case <-sc.ctx.Done():
		sc.waiter.Done()
		return
	default:
	}
	var header = http.Header{}
	header.Set("Cache-Control", "no-cache")
	header.Set("Accept", "text/event-stream")
	header.Set(ClientIdentificationHeader, sc.id.String())
	if !sc.lastId.IsNil() {
		header.Set(LastEventIdListHeader, sc.lastId.String())
	}

	var retryCount int
	for {
		var delay = sc.retryFunc(retryCount)
		<-time.After(delay)

		var req, response, err = utils.DoRequest(
			sc.ctx,
			sc.client,
			sc.request.Method,
			sc.request.URL.String(),
			nil,
			header,
		)
		if err != nil && retryCount < sc.maxRetries {
			retryCount++
			continue
		}
		if err != nil && retryCount >= sc.maxRetries {
			njson.Log(sc.logger).New().
				Error().
				Message("failed to create request").
				String("error", nerror.WrapOnly(err).Error()).
				End()
			return
		}

		sc.request = req
		sc.response = response
		go sc.run()
		return
	}

}
