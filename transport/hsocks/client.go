package hsocks

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/sabuhp"

	"github.com/influx6/npkg/nerror"

	"github.com/influx6/sabuhp/utils"
)

type MessageHandler func(message []byte, socket *SendClient) error

type HttpHub struct {
	maxRetries int
	codec      sabuhp.Codec
	retryFunc  sabuhp.RetryFunc
	ctx        context.Context
	client     *http.Client
	logging    sabuhp.Logger
}

func NewHub(
	ctx context.Context,
	maxRetries int,
	codec sabuhp.Codec,
	client *http.Client,
	logging sabuhp.Logger,
	retryFn sabuhp.RetryFunc,
) *HttpHub {
	if client.CheckRedirect == nil {
		client.CheckRedirect = utils.CheckRedirect
	}
	return &HttpHub{ctx: ctx, codec: codec, maxRetries: maxRetries, client: client, retryFunc: retryFn, logging: logging}
}

func (se *HttpHub) For(
	id nxid.ID,
	route string,
) (*SendClient, error) {
	return NewClient(se.ctx, id, route, se.maxRetries, se.codec, se.retryFunc, se.logging, se.client), nil
}

type SendClient struct {
	id         nxid.ID
	maxRetries int
	route      string
	codec      sabuhp.Codec
	logger     sabuhp.Logger
	retryFunc  sabuhp.RetryFunc
	ctx        context.Context
	client     *http.Client
	lastId     nxid.ID
	retry      time.Duration
}

func NewClient(
	ctx context.Context,
	id nxid.ID,
	route string,
	maxRetries int,
	codec sabuhp.Codec,
	retryFn sabuhp.RetryFunc,
	logger sabuhp.Logger,
	reqClient *http.Client,
) *SendClient {
	var client = &SendClient{
		id:         id,
		route:      route,
		codec:      codec,
		maxRetries: maxRetries,
		logger:     logger,
		client:     reqClient,
		retryFunc:  retryFn,
		ctx:        ctx,
		retry:      0,
	}

	return client
}

func (sc *SendClient) Send(method string, msg *sabuhp.Message, timeout time.Duration) (*sabuhp.Message, error) {
	var header = sabuhp.Header{}
	for k, v := range msg.Headers {
		header[k] = v
	}
	header.Set(ClientIdentificationHeader, sc.id.String())
	header.Set("Content-Type", sabuhp.MessageContentType)

	var ctx = sc.ctx
	var canceler context.CancelFunc
	if timeout > 0 {
		ctx, canceler = context.WithTimeout(sc.ctx, timeout)
	} else {
		canceler = func() {}
	}

	defer canceler()

	var payload, payloadErr = sc.codec.Encode(msg)
	if payloadErr != nil {
		return nil, nerror.WrapOnly(payloadErr)
	}

	var req, response, err = sc.try(method, header, bytes.NewBuffer(payload), ctx)
	if err != nil {
		return nil, nerror.WrapOnly(err)
	}

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, nerror.New("failed to request [Status Code: %d]", response.StatusCode)
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

	var responseBody = bytes.NewBuffer(make([]byte, 0, 512))
	if _, readErr := io.Copy(responseBody, response.Body); readErr != nil {
		var wrappedErr = nerror.WrapOnly(readErr)
		njson.Log(sc.logger).New().
			LError().
			Message("failed to read response").
			Object("msg", msg).
			String("url", req.URL.String()).
			String("method", req.Method).
			String("remote_addr", req.RemoteAddr).
			String("response_status", response.Status).
			Int("response_status_code", response.StatusCode).
			Error("error", wrappedErr).
			End()
	}

	if closeErr := response.Body.Close(); closeErr != nil {
		njson.Log(sc.logger).New().
			LError().
			Message("failed to close response body").
			String("error", nerror.WrapOnly(err).Error()).
			End()
	}

	var contentType = response.Header.Get("Content-Type")
	var contentTypeLower = strings.ToLower(contentType)
	if !strings.Contains(contentTypeLower, sabuhp.MessageContentType) {
		return &sabuhp.Message{
			Topic:    req.URL.Path,
			ID:       nxid.New(),
			Delivery: sabuhp.SendToAll,
			MessageMeta: sabuhp.MessageMeta{
				ContentType:     contentType,
				Query:           req.URL.Query(),
				Form:            req.Form,
				Headers:         sabuhp.Header(response.Header.Clone()),
				Cookies:         sabuhp.ReadCookies(sabuhp.Header(response.Header), ""),
				MultipartReader: nil,
			},
			Payload:             responseBody.Bytes(),
			Metadata:            map[string]string{},
			Params:              map[string]string{},
			LocalPayload:        nil,
			OverridingTransport: nil,
		}, nil
	}

	var responseMessage, responseMsgErr = sc.codec.Decode(responseBody.Bytes())
	if responseMsgErr != nil {
		return nil, nerror.WrapOnly(responseMsgErr)
	}

	return responseMessage, nil
}

func (sc *SendClient) try(method string, header sabuhp.Header, body io.Reader, ctx context.Context) (*http.Request, *http.Response, error) {
	header.Set(ClientIdentificationHeader, sc.id.String())

	var retryCount int
	for {
		var delay = sc.retryFunc(retryCount)
		<-time.After(delay)

		var req, response, err = utils.DoRequest(
			ctx,
			sc.client,
			method,
			sc.route,
			body,
			http.Header(header),
		)
		if err != nil && retryCount < sc.maxRetries && method == "GET" {
			retryCount++
			continue
		}
		if err != nil && retryCount >= sc.maxRetries {
			njson.Log(sc.logger).New().
				LError().
				Message("failed to create request").
				String("error", nerror.WrapOnly(err).Error()).
				End()
		}

		return req, response, err
	}

}
