// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// Server is a thin wrapper around http.Server. See MakeServer for more detail.
type Server struct {
	*http.Server
}

// MakeServer constructs a Server that tracks active connections,
// closing them when signaled by stopper.
//
// It can serve two different purposes simultaneously:
//
// - to serve as actual HTTP server, using the .Serve(net.Listener) method.
// - to serve as plain TCP server, using the .ServeWith(...) method.
//
// The latter is used e.g. to accept SQL client connections.
//
// When the HTTP facility is not used, the Go HTTP server object is
// still used internally to maintain/register the connections via the
// ConnState() method, for convenience.
func MakeServer(stopper *stop.Stopper, tlsConfig *tls.Config, handler http.Handler) Server {
	var mu syncutil.Mutex
	activeConns := make(map[net.Conn]struct{})
	server := Server{
		Server: &http.Server{
			Handler:   handler,
			TLSConfig: tlsConfig,
			ConnState: func(conn net.Conn, state http.ConnState) {
				mu.Lock()
				switch state {
				case http.StateNew:
					activeConns[conn] = struct{}{}
				case http.StateClosed:
					delete(activeConns, conn)
				}
				mu.Unlock()
			},
			ErrorLog: httpLogger,
		},
	}

	ctx := context.TODO()

	// net/http.(*Server).Serve/http2.ConfigureServer are not thread safe with
	// respect to net/http.(*Server).TLSConfig, so we call it synchronously here.
	if err := http2.ConfigureServer(server.Server, nil); err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	waitQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()

		mu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		mu.Unlock()
	}
	if err := stopper.RunAsyncTask(ctx, "http2-wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(ctx)
	}

	return server
}

// ServeWith accepts connections on ln and serves them using serveConn.
func (s *Server) ServeWith(
	ctx context.Context, stopper *stop.Stopper, l net.Listener, serveConn func(net.Conn),
) error {
	// Inspired by net/http.(*Server).Serve
	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		rw, e := l.Accept()
		if e != nil {
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
		go func() {
			defer stopper.Recover(ctx)
			s.Server.ConnState(rw, http.StateNew) // before Serve can return
			serveConn(rw)
			s.Server.ConnState(rw, http.StateClosed)
		}()
	}
}

// IsClosedConnection returns true if err is cmux.ErrListenerClosed,
// grpc.ErrServerStopped, io.EOF, or the net package's errClosed.
func IsClosedConnection(err error) bool {
	return errors.IsAny(err, cmux.ErrListenerClosed, grpc.ErrServerStopped, io.EOF) ||
		strings.Contains(err.Error(), "use of closed network connection")
}

// FatalIfUnexpected calls Log.Fatal(err) unless err is nil,
// cmux.ErrListenerClosed, or the net package's errClosed.
func FatalIfUnexpected(err error) {
	if err != nil && !IsClosedConnection(err) {
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
var _ errors.Formatter = (*InitialHeartbeatFailedError)(nil)

// Error implements error.
func (e *InitialHeartbeatFailedError) Error() string { return fmt.Sprintf("%v", e) }

// Cause implements causer.
func (e *InitialHeartbeatFailedError) Cause() error { return e.WrappedErr }

// Format implements fmt.Formatter.
func (e *InitialHeartbeatFailedError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.FormatError.
func (e *InitialHeartbeatFailedError) FormatError(p errors.Printer) error {
	p.Print("initial connection heartbeat failed")
	return e.WrappedErr
}

// NewInitialHeartBeatFailedError creates a new InitialHeartbeatFailedError.
func NewInitialHeartBeatFailedError(cause error) *InitialHeartbeatFailedError {
	return &InitialHeartbeatFailedError{
		WrappedErr: cause,
	}
}
