package utils

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"

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

// Go's http package doesn't copy headers across when it encounters
// redirects so we need to do that manually.
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

// DoRequest makes a request for giving parameters.
func DoRequest(
	ctx context.Context,
	client *http.Client,
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
		defer response.Body.Close()

		message, _ := ioutil.ReadAll(response.Body)
		var requestErr = RequestErr{
			Code:    response.StatusCode,
			Message: string(message),
		}
		return nil, nil, nerror.WrapOnly(&requestErr)
	}
	return req, response, nil
}
