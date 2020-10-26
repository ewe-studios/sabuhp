package sabuhp

import (
	"io"
	"net/http"
	"net/url"
)

func HTTPRequestToRequest(req *http.Request) *Request {
	var headers = Header(req.Header)
	return &Request{
		Host:          req.Host,
		Form:          req.Form,
		PostForm:      req.PostForm,
		MultipartForm: req.MultipartForm,
		Proto:         req.Proto,
		IP:            req.RemoteAddr,
		URL:           req.URL,
		TLS:           req.TLS != nil,
		Method:        req.Method,
		Headers:       headers,
		Body:          req.Body,
		Req:           req,
		Cookies:       ReadCookies(headers, ""),
	}
}

func NewRequest(addr string, method string, body io.ReadCloser) (*Request, error) {
	var reqURL, reqErr = url.Parse(addr)
	if reqErr != nil {
		return nil, reqErr
	}

	return &Request{
		Headers: Header{},
		Host:    reqURL.Host,
		URL:     reqURL,
		Method:  method,
		Body:    body,
	}, nil
}
