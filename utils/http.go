package utils

import (
	"context"
	"github.com/ewe-studios/sabuhp"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/influx6/npkg"
	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/njson"
)

func CreateError(w io.Writer, err error, message string, code int) error {
	var content = njson.JSONB(func(event npkg.Encoder) {
		event.ObjectFor("error", func(encoder npkg.ObjectEncoder) {
			encoder.Int("code", code)
			encoder.String("message", message)
			encoder.String("details", err.Error())
		})
	})
	if _, err := content.WriteTo(w); err != nil {
		return nerror.WrapOnly(err)
	}
	return nil
}

func CheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return nerror.New("stopped after 10 redirects")
	}
	for k, vv := range via[0].Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	return nil
}

func CreateDefaultHttpClient() *http.Client {
	var client http.Client
	client.Timeout = time.Second * 3
	client.CheckRedirect = CheckRedirect
	return &client
}


// DoRequest makes a request for giving parameters.
func DoRequest(
	ctx context.Context,
	client sabuhp.HttpClient,
	method string,
	route string,
	body io.Reader,
	headers http.Header,
) (*http.Request, *http.Response, error) {
	var initialReq, reqErr = http.NewRequest(method, route, body)
	if reqErr != nil {
		return nil, nil, nerror.WrapOnly(reqErr)
	}

	var req = initialReq.WithContext(ctx)

	for key, params := range headers {
		for _, param := range params {
			req.Header.Set(key, param)
		}
	}

	var response, resErr = client.Do(req)
	if resErr != nil {
		return nil, nil, nerror.WrapOnly(resErr)
	}

	if response.StatusCode < 200 || response.StatusCode > 299 {
		defer func() {
			_ = response.Body.Close()
		}()

		message, _ := ioutil.ReadAll(response.Body)
		var requestErr = RequestErr{
			Code:    response.StatusCode,
			Message: string(message),
		}
		return nil, nil, nerror.WrapOnly(&requestErr)
	}
	return req, response, nil
}
