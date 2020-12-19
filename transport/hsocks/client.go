package hsocks

import (
	"bytes"
	"context"
	"io"
	"net/http"
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
	retryFunc  sabuhp.RetryFunc
	ctx        context.Context
	client     *http.Client
	logging    sabuhp.Logger
}

func NewHub(
	ctx context.Context,
	maxRetries int,
	client *http.Client,
	logging sabuhp.Logger,
	retryFn sabuhp.RetryFunc,
) *HttpHub {
	if client.CheckRedirect == nil {
		client.CheckRedirect = utils.CheckRedirect
	}
	return &HttpHub{ctx: ctx, maxRetries: maxRetries, client: client, retryFunc: retryFn, logging: logging}
}

func (se *HttpHub) For(
	id nxid.ID,
	route string,
) (*SendClient, error) {
	return NewClient(id, route, se.maxRetries, se.ctx, se.retryFunc, se.logging, se.client), nil
}

type SendClient struct {
	id         nxid.ID
	maxRetries int
	route      string
	logger     sabuhp.Logger
	retryFunc  sabuhp.RetryFunc
	ctx        context.Context
	client     *http.Client
	lastId     nxid.ID
	retry      time.Duration
}

func NewClient(
	id nxid.ID,
	route string,
	maxRetries int,
	ctx context.Context,
	retryFn sabuhp.RetryFunc,
	logger sabuhp.Logger,
	reqClient *http.Client,
) *SendClient {
	var client = &SendClient{
		id:         id,
		route:      route,
		maxRetries: maxRetries,
		logger:     logger,
		client:     reqClient,
		retryFunc:  retryFn,
		ctx:        ctx,
		retry:      0,
	}

	return client
}

func (sc *SendClient) Send(method string, msg []byte, timeout time.Duration) ([]byte, error) {

	var header = http.Header{}
	header.Set(ClientIdentificationHeader, sc.id.String())

	var ctx = sc.ctx
	var canceler context.CancelFunc
	if timeout > 0 {
		ctx, canceler = context.WithTimeout(sc.ctx, timeout)
	} else {
		canceler = func() {}
	}

	defer canceler()

	var req, response, err = sc.try(method, bytes.NewBuffer(msg), ctx)
	if err != nil {
		return nil, nerror.WrapOnly(err)
	}

	njson.Log(sc.logger).New().
		LInfo().
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
			LError().
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
			LError().
			Message("failed to close response body").
			String("error", nerror.WrapOnly(err).Error()).
			End()
	}

	return responseBody.Bytes(), nil
}

func (sc *SendClient) try(method string, body io.Reader, ctx context.Context) (*http.Request, *http.Response, error) {
	var header = http.Header{}
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
			header,
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
