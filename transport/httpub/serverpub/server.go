package serverpub

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/influx6/npkg/nerror"

	netutils "github.com/influx6/npkg/nnet"
	"golang.org/x/crypto/acme/autocert"
)

const (
	shutdownDuration = time.Second * 30
)

var (
	// ErrUnhealthy is returned when a server is considered unhealthy.
	ErrUnhealthy = errors.New("Service is unhealthy")
)

// HealthPinger exposes what we expect to provide us a Health check for a server.
type HealthPinger interface {
	Ping() error
}

type HttpHealthIndicator struct {
	healthy uint32
}

// Ping implements the HealthPinger interface.
func (h *HttpHealthIndicator) Ping() error {
	if atomic.LoadUint32(&h.healthy) == 1 {
		return ErrUnhealthy
	}
	return nil
}

func (h *HttpHealthIndicator) SetUnhealthy() {
	atomic.StoreUint32(&h.healthy, 1)
}

func (h *HttpHealthIndicator) SetHealthy() {
	atomic.StoreUint32(&h.healthy, 0)
}

// Server implements a http server wrapper.
type Server struct {
	Http2           bool
	ShutdownTimeout time.Duration
	Handler         http.Handler
	ReadyFunc       func()
	Health          *HttpHealthIndicator
	TLSConfig       *tls.Config
	Man             *autocert.Manager

	waiter   sync.WaitGroup
	closer   chan struct{}
	server   *http.Server
	listener net.Listener
}

// NewServer returns a new server which uses http instead of https.
func NewServer(handler http.Handler, shutdown ...time.Duration) *Server {
	var shutdownDur = shutdownDuration
	if len(shutdown) != 0 {
		shutdownDur = shutdown[0]
	}

	var health HttpHealthIndicator
	var server Server
	server.Http2 = false
	server.Health = &health
	server.Handler = handler
	server.ShutdownTimeout = shutdownDur
	return &server
}

// NewServerWithTLS returns a new server which uses the provided tlsconfig for https connections.
func NewServerWithTLS(http2 bool, tconfig *tls.Config, handler http.Handler, shutdown ...time.Duration) *Server {
	var shutdownDur = shutdownDuration
	if len(shutdown) != 0 {
		shutdownDur = shutdown[0]
	}

	var health HttpHealthIndicator
	var server Server
	server.Http2 = http2
	server.Health = &health
	server.Handler = handler
	server.TLSConfig = tconfig
	server.ShutdownTimeout = shutdownDur
	return &server
}

// NewServerWithCertMan returns a new server which uses the provided autocert certificate
// manager to provide http certificate.
func NewServerWithCertMan(http2 bool, man *autocert.Manager, handler http.Handler, shutdown ...time.Duration) *Server {
	var shutdownDur = shutdownDuration
	if len(shutdown) != 0 {
		shutdownDur = shutdown[0]
	}

	var health HttpHealthIndicator
	var server Server
	server.Man = man
	server.Http2 = http2
	server.Health = &health
	server.Handler = handler
	server.ShutdownTimeout = shutdownDur
	return &server
}

// Listen creates new http listen for giving addr and returns any error
// that occurs in attempt to starting the server.
func (s *Server) Listen(ctx context.Context, addr string) error {
	s.closer = make(chan struct{})

	var tlsConfig = s.TLSConfig
	if s.Http2 && tlsConfig == nil && s.Man == nil {
		tlsConfig = &tls.Config{
			GetCertificate: s.Man.GetCertificate,
		}
	}

	if s.Http2 {
		tlsConfig.NextProtos = append(tlsConfig.NextProtos, "h2")
	}

	var listener, err = netutils.MakeListener("tcp", addr, tlsConfig)
	if err != nil {
		return err
	}

	var server = &http.Server{
		Addr:           addr,
		Handler:        s.Handler,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		MaxHeaderBytes: 1 << 20,
		TLSConfig:      tlsConfig,
	}

	s.Health.SetHealthy()

	var errs = make(chan error, 1)
	s.waiter.Add(1)
	go func() {
		defer s.waiter.Done()
		if s.ReadyFunc != nil {
			s.ReadyFunc()
		}
		if err := server.Serve(netutils.NewKeepAliveListener(listener)); err != nil {
			s.Health.SetUnhealthy()
			errs <- err
		}
	}()

	var signals = make(chan os.Signal, 1)
	signal.Notify(signals,
		syscall.SIGTERM,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	s.waiter.Add(1)
	go func() {
		defer s.waiter.Done()
		select {
		case <-ctx.Done():
			// server was called to close.
			s.gracefulShutdown(server)
		case <-s.closer:
			// server was closed intentionally.
			s.gracefulShutdown(server)
		case <-signals:
			// server received signal to close entirely.
			s.gracefulShutdown(server)
		}
	}()

	return <-errs
}

func (s *Server) gracefulShutdown(server *http.Server) {
	s.Health.SetUnhealthy()
	time.Sleep(20 * time.Second)
	var ctx, cancel = context.WithTimeout(context.Background(), s.ShutdownTimeout)
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Close server returned error: %+q", nerror.WrapOnly(err))
	}
	cancel()
}

// Close closes giving server
func (s *Server) Close() {
	select {
	case <-s.closer:
		return
	default:
		close(s.closer)
	}
}

// Wait blocks till server is closed.
func (s *Server) Wait(after ...func()) {
	s.waiter.Wait()
	for _, cb := range after {
		cb()
	}
}

// TLSManager returns the autocert.Manager associated with the giving server
// for its tls certificates.
func (s *Server) TLSManager() *autocert.Manager {
	return s.Man
}
