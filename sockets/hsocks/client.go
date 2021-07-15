package hsocks

import (
	"bytes"
	"context"
	"github.com/ewe-studios/sabuhp/sabu"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/influx6/npkg/njson"

	"github.com/influx6/npkg/nxid"

	"github.com/influx6/npkg/nerror"

	"github.com/ewe-studios/sabuhp/utils"
)

type SendClient struct {
	maxRetries    int
	id            nxid.ID
	Route         string
	RequestMethod string
	codec         sabu.Codec
	logger        sabu.Logger
	retryFunc     sabu.RetryFunc
	ctx           context.Context
	client        sabu.HttpClient
	lastId        nxid.ID
	retry         time.Duration
}

func linearBackOff(i int) time.Duration {
	return time.Duration(i) * (10 * time.Millisecond)
}

func CreateClient(
	ctx context.Context,
	addr string,
	method string,
	codec sabu.Codec,
	logger sabu.Logger,
	client sabu.HttpClient,
) *SendClient {
	return NewClient(ctx, nxid.New(), addr, 5, method, codec, linearBackOff, logger, client)
}

func CreateClient2(
	ctx context.Context,
	addr string,
	method string,
	codec sabu.Codec,
	logger sabu.Logger,
) *SendClient {
	return NewClient(ctx, nxid.New(), addr, 5, method, codec, linearBackOff, logger, utils.CreateDefaultHttpClient())
}

func NewClient(
	ctx context.Context,
	id nxid.ID,
	route string,
	maxRetries int,
	method string,
	codec sabu.Codec,
	retryFn sabu.RetryFunc,
	logger sabu.Logger,
	reqClient sabu.HttpClient,
) *SendClient {
	var client = &SendClient{
		id:            id,
		Route:         route,
		codec:         codec,
		maxRetries:    maxRetries,
		logger:        logger,
		RequestMethod: method,
		client:        reqClient,
		retryFunc:     retryFn,
		ctx:           ctx,
		retry:         0,
	}
	return client
}

func (sc *SendClient) CloneForMethod(method string) *SendClient {
	var scClone = *sc
	scClone.RequestMethod = method
	return &scClone
}

func (sc *SendClient) ID() nxid.ID {
	return sc.id
}

func (sc *SendClient) Close() {
	// do nothing
}

func (sc *SendClient) Stop() {
	// do nothing
}

func (sc *SendClient) Start() {
	// do nothing
}

func (sc *SendClient) Send(msgs ...sabu.Message) {
	for _, msg := range msgs {
		if err := sc.SendMessageAsMethod(sc.RequestMethod, msg); err != nil {
			if msg.Future != nil {
				msg.Future.WithError(err)
			}
		}
	}
}

func (sc *SendClient) SendMessageAsMethod(method string, msg sabu.Message) error {
	var ft = msg.Future

	var timeout = msg.Within
	var header = sabu.Header{}
	for k, v := range msg.Headers {
		header[k] = v
	}
	header.Set(ClientIdentificationHeader, sc.id.String())
	header.Set("Content-Type", sabu.MessageContentType)

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
		return nerror.WrapOnly(payloadErr)
	}

	var req, response, err = sc.try(method, header, bytes.NewBuffer(payload), ctx)
	if err != nil {
		return err
	}

	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nerror.New("failed to request [Status Code: %d]", response.StatusCode)
	}

	// if no future is attached then dont read response, but close and end here
	if ft == nil {
		return response.Body.Close()
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
		return wrappedErr
	}

	if closeErr := response.Body.Close(); closeErr != nil {
		njson.Log(sc.logger).New().
			LError().
			Message("failed to close response body").
			String("error", nerror.WrapOnly(err).Error()).
			End()

		return nerror.WrapOnly(closeErr)
	}

	var contentType = response.Header.Get("Content-Type")
	var contentTypeLower = strings.ToLower(contentType)
	if !strings.Contains(contentTypeLower, sabu.MessageContentType) {
		ft.WithValue(sabu.Message{
			Topic:       sabu.T(req.URL.Path),
			Id:          nxid.New(),
			ContentType: contentType,
			Path:        req.URL.Path,
			Query:       req.URL.Query(),
			Form:        req.Form,
			Headers:     sabu.Header(response.Header.Clone()),
			Cookies:     sabu.ReadCookies(sabu.Header(response.Header), ""),
			Bytes:       responseBody.Bytes(),
			Metadata:    map[string]string{},
			Params:      map[string]string{},
		})
		return nil
	}

	var responseMessage, responseMsgErr = sc.codec.Decode(responseBody.Bytes())
	if responseMsgErr != nil {
		return nerror.WrapOnly(responseMsgErr)
	}

	if len(responseMessage.Path) == 0 {
		responseMessage.Path = req.URL.Path
	}

	ft.WithValue(responseMessage)
	return nil
}

func (sc *SendClient) try(method string, header sabu.Header, body io.Reader, ctx context.Context) (*http.Request, *http.Response, error) {
	header.Set(ClientIdentificationHeader, sc.id.String())

	var retryCount int
	for {
		var delay = sc.retryFunc(retryCount)
		<-time.After(delay)

		var req, response, err = utils.DoRequest(
			ctx,
			sc.client,
			method,
			sc.Route,
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
