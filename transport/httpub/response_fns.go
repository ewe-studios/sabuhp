package httpub

// IsWebSocket returns true/false if the giving reqest is a websocket connection.
func IsWebSocket(r *Request) bool {
	upgrade := r.Headers.Get(HeaderUpgrade)
	return upgrade == "websocket" || upgrade == "Websocket"
}

// Scheme attempts to return the exact url scheme of the request.
func Scheme(r *Request) string {
	if scheme := r.Headers.Get(HeaderXForwardedProto); scheme != "" {
		return scheme
	}
	if scheme := r.Headers.Get(HeaderXForwardedProtocol); scheme != "" {
		return scheme
	}
	if ssl := r.Headers.Get(HeaderXForwardedSsl); ssl == "on" {
		return "https"
	}
	if scheme := r.Headers.Get(HeaderXUrlScheme); scheme != "" {
		return scheme
	}
	return "http"
}

//// RealIP attempts to return the ip of the giving request.
//func RealIP(r *Request) string {
//	ra := r.RemoteAddr
//	if ip := r.Headers.Get(HeaderXForwardedFor); ip != "" {
//		ra = strings.Split(ip, ", ")[0]
//	} else if ip := c.request.Header.Get(HeaderXRealIP); ip != "" {
//		ra = ip
//	} else {
//		ra, _, _ = net.SplitHostPort(ra)
//	}
//	return ra
//}
//
//// Path returns the request path associated with the context.
//func (c *Ctx) Path() string {
//	if c.path == "" && c.request != nil {
//		c.path = c.request.URL.Path
//	}
//
//	return c.path
//}
//
//// QueryParam finds the giving value for the giving name in the querie set.
//func (c *Ctx) QueryParam(name string) string {
//	if c.query == nil {
//		c.query = c.request.URL.Query()
//	}
//
//	return c.query.Get(name)
//}
//
//// QueryParams returns the context url.Values object.
//func (c *Ctx) QueryParams() url.Values {
//	if c.query == nil {
//		c.query = c.request.URL.Query()
//	}
//	return c.query
//}
//
//// QueryString returns the raw query portion of the request path.
//func (c *Ctx) QueryString() string {
//	return c.request.URL.RawQuery
//}
//
//// Form returns the url.Values of the giving request.
//func (c *Ctx) Form() url.Values {
//	return c.request.Form
//}
//
//// FormValue returns the value of the giving item from the form fields.
//func (c *Ctx) FormValue(name string) string {
//	return c.request.FormValue(name)
//}
//
//// FormParams returns a url.Values which contains the parse form values for
//// multipart or wwww-urlencoded forms.
//func (c *Ctx) FormParams() (url.Values, error) {
//	if strings.HasPrefix(c.request.Header.Get(HeaderContentType), MIMEMultipartForm) {
//		if err := c.request.ParseMultipartForm(c.multipartFormSize); err != nil {
//			return nil, err
//		}
//	} else {
//		if err := c.request.ParseForm(); err != nil {
//			return nil, err
//		}
//	}
//	return c.request.Form, nil
//}
//
//// FormFile returns the giving FileHeader for the giving name.
//func (c *Ctx) FormFile(name string) (*multipart.FileHeader, error) {
//	_, fh, err := c.request.FormFile(name)
//	return fh, err
//}
//
//// MultipartForm returns the multipart form of the giving request if its a multipart
//// request.
//func (c *Ctx) MultipartForm() (*multipart.Form, error) {
//	err := c.request.ParseMultipartForm(defaultMemory)
//	return c.request.MultipartForm, err
//}
//
//// Cookie returns the associated cookie by the giving name.
//func (c *Ctx) Cookie(name string) (*http.Cookie, error) {
//	return c.request.Cookie(name)
//}
//
//// SetCookie sets the cookie into the response object.
//func (c *Ctx) SetCookie(cookie *http.Cookie) {
//	http.SetCookie(c.response, cookie)
//}
//
//// Cookies returns the associated cookies slice of the http request.
//func (c *Ctx) Cookies() []*http.Cookie {
//	return c.request.Cookies()
//}
//
//// ErrNoRenderInitiated defines the error returned when a renderer is not set
//// but Ctx.Render() is called.
//var ErrNoRenderInitiated = errors.New("Renderer was not set or is uninitiated")
//
//// Render renders the giving string with data binding using the provided Render
//// of the context.
//func (c *Ctx) Render(code int, tmpl string, data interface{}) (err error) {
//	if c.render == nil {
//		return ErrNoRenderInitiated
//	}
//
//	buf := new(bytes.Buffer)
//
//	if err = c.render.Render(buf, tmpl, data); err != nil {
//		return
//	}
//
//	return c.HTMLBlob(code, buf.Bytes())
//}
//
//// Template renders provided template.Template object into the response object.
//func (c *Ctx) Template(code int, tmpl *template.Template, data interface{}) error {
//	c.Status(code)
//	return tmpl.Funcs(TextContextFunctions(c)).Execute(c.response, data)
//}
//
//// HTMLTemplate renders provided template.Template object into the response object.
//func (c *Ctx) HTMLTemplate(code int, tmpl *htemplate.Template, data interface{}) error {
//	c.Status(code)
//	return tmpl.Funcs(HTMLContextFunctions(c)).Execute(c.response, data)
//}
//
//// HTML renders giving html into response.
//func (c *Ctx) HTML(code int, html string) (err error) {
//	return c.HTMLBlob(code, []byte(html))
//}
//
//// HTMLBlob renders giving html into response.
//func (c *Ctx) HTMLBlob(code int, b []byte) (err error) {
//	return c.Blob(code, MIMETextHTMLCharsetUTF8, b)
//}
//
//// Error renders giving error response into response.
//func (c *Ctx) Error(statusCode int, errorCode string, message string, err error) error {
//	c.response.Header().Set(HeaderContentType, MIMEApplicationJSONCharsetUTF8)
//	return JSONError(c.Response(), statusCode, errorCode, message, err)
//}
//
//// String renders giving string into response.
//func (c *Ctx) String(code int, s string) (err error) {
//	return c.Blob(code, MIMETextPlainCharsetUTF8, []byte(s))
//}
//
//// NJSON renders njson object as json response.
//func (c *Ctx) NJSON(code int, data *njson.JSON) (err error) {
//	c.response.Header().Set(HeaderContentType, MIMEApplicationJSON)
//	c.response.WriteHeader(code)
//	_, err = data.WriteTo(c.response)
//	return
//}
//
//// JSON renders giving json data into response.
//func (c *Ctx) JSON(code int, i interface{}) (err error) {
//	_, pretty := c.QueryParams()["pretty"]
//	if pretty {
//		return c.JSONPretty(code, i, "  ")
//	}
//	b, err := json.Marshal(i)
//	if err != nil {
//		return
//	}
//	return c.JSONBlob(code, b)
//}
//
//// JSONPretty renders giving json data as indented into response.
//func (c *Ctx) JSONPretty(code int, i interface{}, indent string) (err error) {
//	b, err := json.MarshalIndent(i, "", indent)
//	if err != nil {
//		return
//	}
//	return c.JSONBlob(code, b)
//}
//
//// JSONBlob renders giving json data into response with proper mime type.
//func (c *Ctx) JSONBlob(code int, b []byte) (err error) {
//	return c.Blob(code, MIMEApplicationJSONCharsetUTF8, b)
//}
//
//// JSONP renders giving jsonp as response with proper mime type.
//func (c *Ctx) JSONP(code int, callback string, i interface{}) (err error) {
//	b, err := json.Marshal(i)
//	if err != nil {
//		return
//	}
//	return c.JSONPBlob(code, callback, b)
//}
//
//// JSONPBlob renders giving jsonp as response with proper mime type.
//func (c *Ctx) JSONPBlob(code int, callback string, b []byte) (err error) {
//	c.response.Header().Set(HeaderContentType, MIMEApplicationJavaScriptCharsetUTF8)
//	c.response.WriteHeader(code)
//	if _, err = c.response.Write([]byte(callback + "(")); err != nil {
//		return
//	}
//	if _, err = c.response.Write(b); err != nil {
//		return
//	}
//	_, err = c.response.Write([]byte(");"))
//	return
//}
//
//// XML renders giving xml as response with proper mime type.
//func (c *Ctx) XML(code int, i interface{}) (err error) {
//	_, pretty := c.QueryParams()["pretty"]
//	if pretty {
//		return c.XMLPretty(code, i, "  ")
//	}
//	b, err := xml.Marshal(i)
//	if err != nil {
//		return
//	}
//	return c.XMLBlob(code, b)
//}
//
//// XMLPretty renders giving xml as indent as response with proper mime type.
//func (c *Ctx) XMLPretty(code int, i interface{}, indent string) (err error) {
//	b, err := xml.MarshalIndent(i, "", indent)
//	if err != nil {
//		return
//	}
//	return c.XMLBlob(code, b)
//}
//
//// XMLBlob renders giving xml as response with proper mime type.
//func (c *Ctx) XMLBlob(code int, b []byte) (err error) {
//	c.response.Header().Set(HeaderContentType, MIMEApplicationXMLCharsetUTF8)
//	c.response.WriteHeader(code)
//	if _, err = c.response.Write([]byte(xml.Header)); err != nil {
//		return
//	}
//	_, err = c.response.Write(b)
//	return
//}
//
//// Blob write giving byte slice as response with proper mime type.
//func (c *Ctx) Blob(code int, contentType string, b []byte) (err error) {
//	c.response.Header().Set(HeaderContentType, contentType)
//	c.response.WriteHeader(code)
//	_, err = c.response.Write(b)
//	return
//}
//
//// Stream copies giving io.Readers content into response.
//func (c *Ctx) Stream(code int, contentType string, r io.Reader) (err error) {
//	c.response.Header().Set(HeaderContentType, contentType)
//	c.response.WriteHeader(code)
//	_, err = io.Copy(c.response, r)
//	return
//}
//
//// File streams file content into response.
//func (c *Ctx) File(file string) (err error) {
//	f, err := os.Open(file)
//	if err != nil {
//		return err
//	}
//
//	defer f.Close()
//
//	fi, _ := f.Stat()
//	if fi.IsDir() {
//		file = filepath.Join(file, "index.html")
//		f, err = os.Open(file)
//		if err != nil {
//			return
//		}
//
//		defer f.Close()
//		if fi, err = f.Stat(); err != nil {
//			return
//		}
//	}
//
//	http.ServeContent(c.Response(), c.Request(), fi.Name(), fi.ModTime(), f)
//	return
//}
//
//// Attachment attempts to attach giving file details.
//func (c *Ctx) Attachment(file, name string) (err error) {
//	return c.contentDisposition(file, name, "attachment")
//}
//
//// Inline attempts to inline file content.
//func (c *Ctx) Inline(file, name string) (err error) {
//	return c.contentDisposition(file, name, "inline")
//}
//
//func (c *Ctx) contentDisposition(file, name, dispositionType string) (err error) {
//	c.response.Header().Set(HeaderContentDisposition, fmt.Sprintf("%s; filename=%s", dispositionType, name))
//	c.File(file)
//	return
//}
