package ssepub

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"

	"github.com/ewe-studios/sabuhp"

	"github.com/influx6/npkg/nerror"

	"github.com/ewe-studios/sabuhp/utils"
)

var (
	newLine         = "\n"
	spaceBytes      = []byte(" ")
	dataHeaderBytes = []byte("data:")
)

type MessageHandler func(message sabuhp.Message, socket *SSEClient) error

type SSEHub struct {
	maxRetries int
	codec      sabuhp.Codec
	retryFunc  sabuhp.RetryFunc
	ctx        context.Context
	client     *http.Client
	logging    sabuhp.Logger
}

func NewSSEHub(
	ctx context.Context,
	maxRetries int,
	client *http.Client,
	codec sabuhp.Codec,
	logging sabuhp.Logger,
	retryFn sabuhp.RetryFunc,
) *SSEHub {
	if client.CheckRedirect == nil {
		client.CheckRedirect = utils.CheckRedirect
	}
	return &SSEHub{
		ctx:        ctx,
		codec:      codec,
		maxRetries: maxRetries,
		client:     client,
		retryFunc:  retryFn,
		logging:    logging,
	}
}

func (se *SSEHub) Get(handler MessageHandler, id nxid.ID, route string, lastEventIds ...string) (*SSEClient, error) {
	return se.For(handler, id, "GET", route, lastEventIds...)
}

func (se *SSEHub) For(
	handler MessageHandler,
	id nxid.ID,
	method string,
	route string,
	lastEventIds ...string,
) (*SSEClient, error) {
	var header = http.Header{}
	header.Set(ClientIdentificationHeader, id.String())
	header.Set("Cache-Control", "no-cache")
	header.Set("Accept", "text/event-stream")
	if len(lastEventIds) > 0 {
		header.Set(LastEventIdListHeader, strings.Join(lastEventIds, ";"))
	}

	var req, response, err = utils.DoRequest(se.ctx, se.client, method, route, nil, header)
	if err != nil {
		return nil, nerror.WrapOnly(err)
	}

	return NewSSEClient(
		id,
		se.maxRetries,
		handler,
		req,
		response,
		se.retryFunc,
		se.codec,
		se.logging,
		se.client,
	), nil
}

type SSEClient struct {
	id         nxid.ID
	maxRetries int
	logger     sabuhp.Logger
	retryFunc  sabuhp.RetryFunc
	codec      sabuhp.Codec
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
	codec sabuhp.Codec,
	logger sabuhp.Logger,
	reqClient *http.Client,
) *SSEClient {
	if req.Context() == nil {
		panic("Request is required to have a context.Context attached")
	}

	var newCtx, canceler = context.WithCancel(req.Context())
	var client = &SSEClient{
		id:         id,
		codec:      codec,
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

func (sc *SSEClient) Send(method string, msg sabuhp.Message, timeout time.Duration) error {
	var header = http.Header{}
	for k, v := range msg.Headers {
		header[k] = v
	}

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

	var payloadBytes, payloadErr = sc.codec.Encode(msg)
	if payloadErr != nil {
		return nerror.WrapOnly(payloadErr)
	}

	var req, response, err = utils.DoRequest(
		ctx,
		sc.client,
		method,
		sc.request.URL.String(),
		bytes.NewReader(payloadBytes),
		header,
	)
	if err != nil {
		njson.Log(sc.logger).New().
			LError().
			Message("failed to send request request").
			String("error", nerror.WrapOnly(err).Error()).
			End()
		return nerror.WrapOnly(err)
	}

	_ = response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nerror.New("failed to request [Status Code: %d]", response.StatusCode)
	}

	njson.Log(sc.logger).New().
		LInfo().
		Message("sent SSE http request").
		Object("msg", msg).
		String("url", req.URL.String()).
		String("method", req.Method).
		String("remote_addr", req.RemoteAddr).
		String("response_status", response.Status).
		Int("response_status_code", response.StatusCode).
		End()

	return nil
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

	var contentType string
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
				LError().
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
					LInfo().
					Message("received complete data").
					String("data", data.String()).
					End()

				var dataLine = bytes.TrimPrefix(data.Bytes(), dataHeaderBytes)
				dataLine = bytes.TrimPrefix(dataLine, spaceBytes)

				var messageErr error
				var message sabuhp.Message
				if contentType == sabuhp.MessageContentType {
					message, messageErr = sc.codec.Decode(dataLine)
					if messageErr != nil {
						var wrappedErr = nerror.WrapOnly(messageErr)
						njson.Log(sc.logger).New().
							LError().
							Message("failed to handle message").
							Error("error", wrappedErr).
							End()
						continue doLoop
					}
					if len(message.Path) == 0 {
						message.Path = sc.request.URL.Path
					}
				} else {
					var payload = make([]byte, len(dataLine))
					_ = copy(payload, dataLine)

					message = sabuhp.Message{
						Topic:       sabuhp.T(sc.request.URL.Path),
						Id:          nxid.New(),
						Path:        sc.request.URL.Path,
						ContentType: contentType,
						Query:       url.Values{},
						Form:        url.Values{},
						Headers:     sabuhp.Header{},
						Cookies:     nil,
						Bytes:       payload,
						Metadata:    map[string]string{},
						Params:      map[string]string{},
					}
				}

				if handleErr := sc.handler(message, sc); handleErr != nil {
					var wrappedErr = nerror.WrapOnly(handleErr)
					njson.Log(sc.logger).New().
						LError().
						Message("failed to handle message").
						Error("error", wrappedErr).
						End()
				}
			}

			continue doLoop
		}

		if line == "\n" && !decoding {
			continue doLoop
		}

		var stripLine = strings.TrimSpace(line)
		if strings.HasPrefix(stripLine, eventHeader) {
			contentType = strings.TrimSpace(strings.TrimPrefix(stripLine, eventHeader))
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
	header.Set("Connection", "keep-alive")
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
				LError().
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
