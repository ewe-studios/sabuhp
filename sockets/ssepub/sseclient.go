package ssepub

import (
	"bufio"
	"bytes"
	"context"
	"github.com/ewe-studios/sabuhp/sabu"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg/nerror"

	"github.com/ewe-studios/sabuhp/utils"
)

var (
	newLine         = "\n"
	spaceBytes      = []byte(" ")
	dataHeaderBytes = []byte("data:")
)

type MessageHandler func(message sabu.Message, socket *SSEClient) error

type SSEClient struct {
	id         nxid.ID
	maxRetries int
	method     string
	logger     sabu.Logger
	retryFunc  sabu.RetryFunc
	codec      sabu.Codec
	handler    MessageHandler // ensure to copy the bytes if your use will span multiple goroutines
	ctx        context.Context
	canceler   context.CancelFunc
	client     sabu.HttpClient
	request    *http.Request
	response   *http.Response
	lastId     nxid.ID
	retry      time.Duration
	waiter     sync.WaitGroup
}
func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}


func NewSSEClient3(
	ctx context.Context,
	route string,
	method string,
	handler MessageHandler,
	codec sabu.Codec,
	logger sabu.Logger,
) (*SSEClient, error) {
	return NewSSEClient(
		ctx,
		nxid.New(),
		5,
		route,
		method,
		handler,
		linearBackOff,
		codec,
		logger,
		utils.CreateDefaultHttpClient(),
	)
}

func NewSSEClient2(
	ctx context.Context,
	route string,
	method string,
	handler MessageHandler,
	codec sabu.Codec,
	logger sabu.Logger,
	reqClient sabu.HttpClient,
) (*SSEClient, error) {
	return NewSSEClient(
		ctx,
		nxid.New(),
		5,
		route,
		method,
		handler,
		linearBackOff,
		codec,
		logger,
		reqClient,
	)
}

func NewSSEClient(
	ctx context.Context,
	id nxid.ID,
	maxRetries int,
	route string,
	method string,
	handler MessageHandler,
	retryFn sabu.RetryFunc,
	codec sabu.Codec,
	logger sabu.Logger,
	reqClient sabu.HttpClient,
) (*SSEClient, error) {
	var header = http.Header{}
	header.Set(ClientIdentificationHeader, id.String())
	header.Set("Cache-Control", "no-cache")
	header.Set("Accept", "text/event-stream")

	var req, response, err = utils.DoRequest(ctx, reqClient, method, route, nil, header)
	if err != nil {
		return nil, nerror.WrapOnly(err)
	}

	return NewSSEClientWithRequestResponse(
		ctx,
		id,
		maxRetries,
		method,
		handler,
		req,
		response,
		retryFn,
		codec,
		logger,
		reqClient,
	), nil
}

func NewSSEClientWithRequestResponse(
	ctx context.Context,
	id nxid.ID,
	maxRetries int,
	method string,
	handler MessageHandler,
	req *http.Request,
	res *http.Response,
	retryFn sabu.RetryFunc,
	codec sabu.Codec,
	logger sabu.Logger,
	reqClient sabu.HttpClient,
) *SSEClient {
	if req.Context() == nil {
		panic("Request is required to have a context.Context attached")
	}

	var newCtx, canceler = context.WithCancel(ctx)
	var client = &SSEClient{
		id:         id,
		method:     method,
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

func (sc *SSEClient) Send(msgs ...sabu.Message) {
	for _, msg := range msgs {
		if err := sc.SendAsMethod(sc.method, msg); err != nil {
			if msg.Future != nil {
				msg.Future.WithError(err)
			}
		}
	}
}

func (sc *SSEClient) SendAsMethod(method string, msg sabu.Message) error {
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
	if msg.Within > 0 {
		ctx, canceler = context.WithTimeout(sc.ctx, msg.Within)
	} else {
		canceler = func() {}
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

func (sc *SSEClient) Start() {
	// do nothing
}

func (sc *SSEClient) ID() nxid.ID {
	return sc.id
}

func (sc *SSEClient) Stop() {
	_ = sc.Close()
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
				var message sabu.Message
				if contentType == sabu.MessageContentType {
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

					message = sabu.Message{
						Topic:       sabu.T(sc.request.URL.Path),
						Id:          nxid.New(),
						Path:        sc.request.URL.Path,
						ContentType: contentType,
						Query:       url.Values{},
						Form:        url.Values{},
						Headers:     sabu.Header{},
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
