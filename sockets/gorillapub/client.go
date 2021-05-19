package gorillapub

//import (
//	"context"
//	"fmt"
//	"net"
//	"net/http"
//	"time"
//
//	"github.com/influx6/npkg/nerror"
//
//	"github.com/ewe-studios/websocket"
//)
//
//type DialerEndpoint struct {
//	Addr    string
//	Headers http.Header
//	Dialer  *websocket.Dialer
//}
//
//func DefaultEndpoint(addr string, dialTimeout time.Duration) *DialerEndpoint {
//	return &DialerEndpoint{
//		Addr:    addr,
//		Headers: http.Header{},
//		Dialer: &websocket.Dialer{
//			NetDial: func(network, addr string) (net.Conn, error) {
//				fmt.Printf("Connecting to %q -> %q\n", network, addr)
//				return net.DialTimeout(network, addr, dialTimeout)
//			},
//			NetDialContext:    nil,
//			Proxy:             http.ProxyFromEnvironment,
//			TLSClientConfig:   nil,
//			HandshakeTimeout:  45 * time.Second,
//			ReadBufferSize:    1024 * 4,
//			WriteBufferSize:   1024 * 4,
//			WriteBufferPool:   nil,
//			Subprotocols:      nil,
//			EnableCompression: false,
//			Jar:               nil,
//		},
//	}
//}
//
//func (de DialerEndpoint) Dial(ctx context.Context) (*websocket.Conn, *http.Response, error) {
//	var ws, wsres, wserr = de.Dialer.DialContext(ctx, de.Addr, de.Headers)
//	if wserr != nil {
//		return nil, nil, nerror.WrapOnly(wserr)
//	}
//	return ws, wsres, nil
//}
