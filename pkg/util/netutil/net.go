// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package netutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
)

// ListenAndServeGRPC creates a listener and serves the specified grpc Server
// on it, closing the listener when signaled by the stopper.
func ListenAndServeGRPC(
	stopper *stop.Stopper, server *grpc.Server, addr net.Addr,
) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return ln, err
	}

	ctx := context.TODO()

	stopper.AddCloser(stop.CloserFn(server.Stop))
	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		FatalIfUnexpected(ln.Close())
	}
	if err := stopper.RunAsyncTask(ctx, "listen-quiesce", waitQuiesce); err != nil {
		waitQuiesce(ctx)
		return nil, err
	}

	if err := stopper.RunAsyncTask(ctx, "serve", func(context.Context) {
		FatalIfUnexpected(server.Serve(ln))
	}); err != nil {
		return nil, err
	}
	return ln, nil
}

var httpLogger = log.NewStdLogger(severity.ERROR, "net/http")

// HTTPServer is a thin wrapper around http.Server. See MakeHTTPServer for more detail.
type HTTPServer struct {
	*http.Server
}

// MakeHTTPServer constructs a http.Server that tracks active connections,
// closing them when signaled by stopper.
func MakeHTTPServer(
	ctx context.Context, stopper *stop.Stopper, tlsConfig *tls.Config, handler http.Handler,
) HTTPServer {
	var mu syncutil.Mutex
	activeConns := make(map[net.Conn]struct{})
	server := HTTPServer{
		Server: &http.Server{
			Handler:   handler,
			TLSConfig: tlsConfig,
			ConnState: func(conn net.Conn, state http.ConnState) {
				mu.Lock()
				defer mu.Unlock()
				switch state {
				case http.StateNew:
					activeConns[conn] = struct{}{}
				case http.StateClosed:
					delete(activeConns, conn)
				}
			},
			ErrorLog: httpLogger,
		},
	}

	// net/http.(*Server).Serve/http2.ConfigureServer are not thread safe with
	// respect to net/http.(*Server).TLSConfig, so we call it synchronously here.
	if err := http2.ConfigureServer(server.Server, nil); err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()

		mu.Lock()
		defer mu.Unlock()
		for conn := range activeConns {
			conn.Close()
		}
	}
	if err := stopper.RunAsyncTask(ctx, "http2-wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(ctx)
	}

	return server
}

// MakeTCPServer constructs a connection server that tracks active connections,
// closing them when signaled by stopper.
func MakeTCPServer(ctx context.Context, stopper *stop.Stopper) *TCPServer {
	server := &TCPServer{
		stopper:     stopper,
		activeConns: make(map[net.Conn]struct{}),
	}

	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()

		server.mu.Lock()
		defer server.mu.Unlock()
		for conn := range server.activeConns {
			conn.Close()
		}
	}
	if err := stopper.RunAsyncTask(ctx, "tcp-wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(ctx)
	}
	return server
}

// TCPServer is wrapper around a map of active connections.
type TCPServer struct {
	mu          syncutil.Mutex
	stopper     *stop.Stopper
	activeConns map[net.Conn]struct{}
}

func (s *TCPServer) addConn(n net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeConns[n] = struct{}{}
}

func (s *TCPServer) rmConn(n net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activeConns, n)
}

// ServeWith accepts connections on ln and serves them using serveConn.
func (s *TCPServer) ServeWith(
	ctx context.Context, l net.Listener, serveConn func(context.Context, net.Conn),
) error {
	// Inspired by net/http.(*Server).Serve
	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		rw, e := l.Accept()
		if e != nil {
			//lint:ignore SA1019 see discussion at https://github.com/cockroachdb/cockroach/pull/84590#issuecomment-1192709976
			if ne := (net.Error)(nil); errors.As(e, &ne) && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				httpLogger.Printf("http: Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0
		err := s.stopper.RunAsyncTask(ctx, "tcp-serve", func(ctx context.Context) {
			defer func() {
				_ = rw.Close()
			}()
			s.addConn(rw)
			defer s.rmConn(rw)
			serveConn(ctx, rw)
		})
		if err != nil {
			err = errors.CombineErrors(err, rw.Close())
			return err
		}
	}
}

// IsClosedConnection returns true if err is a non-temporary net.Error or is
// cmux.ErrListenerClosed, grpc.ErrServerStopped, io.EOF, or net.ErrClosed.
func IsClosedConnection(err error) bool {
	if netError := net.Error(nil); errors.As(err, &netError) {
		//lint:ignore SA1019 see discussion at https://github.com/cockroachdb/cockroach/pull/84590#issuecomment-1192709976
		return !netError.Temporary()
	}
	return errors.IsAny(err, cmux.ErrListenerClosed, grpc.ErrServerStopped, io.EOF, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection")
}

// FatalIfUnexpected calls Log.Fatal(err) unless err is nil, or an error that
// comes from the net package indicating that the listener was closed or from
// the Stopper indicating quiescence.
func FatalIfUnexpected(err error) {
	if err != nil && !IsClosedConnection(err) && !errors.Is(err, stop.ErrUnavailable) {
		log.Fatalf(context.TODO(), "%+v", err)
	}
}

// InitialHeartbeatFailedError indicates that while attempting a GRPC
// connection to a node, we aren't successful and have never seen a
// heartbeat over that connection before.
type InitialHeartbeatFailedError struct {
	WrappedErr error
}

var _ error = (*InitialHeartbeatFailedError)(nil)
var _ fmt.Formatter = (*InitialHeartbeatFailedError)(nil)
var _ errors.SafeFormatter = (*InitialHeartbeatFailedError)(nil)

// Error implements error.
func (e *InitialHeartbeatFailedError) Error() string { return fmt.Sprintf("%v", e) }

// Cause implements causer.
func (e *InitialHeartbeatFailedError) Cause() error { return e.WrappedErr }

// Format formats the error.
func (e *InitialHeartbeatFailedError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// SafeFormatError implements errors.SafeFormatter.
func (e *InitialHeartbeatFailedError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("initial connection heartbeat failed")
	return e.WrappedErr
}

// NewInitialHeartBeatFailedError creates a new InitialHeartbeatFailedError.
func NewInitialHeartBeatFailedError(cause error) *InitialHeartbeatFailedError {
	return &InitialHeartbeatFailedError{
		WrappedErr: cause,
	}
}
