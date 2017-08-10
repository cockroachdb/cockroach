// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package netutil

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ListenAndServeGRPC creates a listener and serves the specified grpc Server
// on it, closing the listener when signalled by the stopper.
func ListenAndServeGRPC(
	stopper *stop.Stopper, server *grpc.Server, addr net.Addr,
) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return ln, err
	}

	ctx := context.TODO()

	stopper.RunWorker(ctx, func(context.Context) {
		<-stopper.ShouldQuiesce()
		FatalIfUnexpected(ln.Close())
		<-stopper.ShouldStop()
		server.Stop()
	})

	stopper.RunWorker(ctx, func(context.Context) {
		FatalIfUnexpected(server.Serve(ln))
	})
	return ln, nil
}

var httpLogger = log.NewStdLogger(log.Severity_ERROR)

// Server is a thin wrapper around http.Server. See MakeServer for more detail.
type Server struct {
	*http.Server
}

// MakeServer constructs a Server that tracks active connections, closing them
// when signalled by stopper.
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
		log.Fatal(ctx, err)
	}

	stopper.RunWorker(ctx, func(context.Context) {
		<-stopper.ShouldStop()

		mu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		mu.Unlock()
	})

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
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
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
	return err == cmux.ErrListenerClosed ||
		err == grpc.ErrServerStopped ||
		err == io.EOF ||
		strings.Contains(err.Error(), "use of closed network connection")
}

// FatalIfUnexpected calls Log.Fatal(err) unless err is nil,
// cmux.ErrListenerClosed, or the net package's errClosed.
func FatalIfUnexpected(err error) {
	if err != nil && !IsClosedConnection(err) {
		log.Fatal(context.TODO(), err)
	}
}

// ErrIsGRPCUnavailable checks whether an error is the GRPC error for
// "unavailable" nodes. This can be used to check if an RPC network error is
// guaranteed to mean that the server did not receive the request.
// All connection errors except for an unavailable node (this is GRPC's
// fail-fast error), may mean that the request succeeded on the remote server,
// but we were unable to receive the reply.
//
// The Unavailable code is used by GRPC to indicate that a request fails fast
// and is not sent, so we can be sure there is no ambiguity on these errors.
// Note that these are common if a node is down.
// See https://github.com/grpc/grpc-go/blob/52f6504dc290bd928a8139ba94e3ab32ed9a6273/call.go#L182
// See https://github.com/grpc/grpc-go/blob/52f6504dc290bd928a8139ba94e3ab32ed9a6273/stream.go#L158
//
// Returns false is err is nil.
func ErrIsGRPCUnavailable(err error) bool {
	return grpc.Code(err) == codes.Unavailable
}
