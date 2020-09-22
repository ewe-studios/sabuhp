package mux

import (
	"net/http"

	"github.com/influx6/sabuhp"
)

func FromRequest(req *http.Request) *Request {
	return &Request{
		Request: &sabuhp.Request{
			URL:     req.URL,
			TLS:     req.TLS != nil,
			Method:  req.Method,
			Headers: req.Header,
			Body:    req.Body,
			Cookies: req.Cookies(),
		},
	}
}
