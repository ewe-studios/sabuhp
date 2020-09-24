package sabuhp

import (
	"net/http"
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
		Cookies:       ReadCookies(headers, ""),
	}
}
