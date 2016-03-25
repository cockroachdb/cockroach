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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/http2"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// ListenAndServeGRPC creates a listener and serves the specified grpc Server
// on it, closing the listener when signalled by the stopper.
func ListenAndServeGRPC(stopper *stop.Stopper, server *grpc.Server,
	addr net.Addr) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return ln, err
	}

	stopper.RunWorker(func() {
		<-stopper.ShouldDrain()
		server.Stop()
	})

	stopper.RunWorker(func() {
		FatalIfUnexpected(server.Serve(ln))
	})
	return ln, nil
}

// ServeHandler serves the handler on the listener and returns a function that
// serves an additional listener using a function that takes a connection. The
// returned function can be called multiple times.
func ServeHandler(
	stopper *stop.Stopper, handler http.Handler, ln net.Listener, tlsConfig *tls.Config,
) func(net.Listener, func(net.Conn)) error {
	var mu sync.Mutex
	activeConns := make(map[net.Conn]struct{})

	logger := log.NewStdLogger(log.ErrorLog)
	httpServer := http.Server{
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
		ErrorLog: logger,
	}

	// net/http.(*Server).Serve/http2.ConfigureServer are not thread safe with
	// respect to net/http.(*Server).TLSConfig, so we call it synchronously here.
	if err := http2.ConfigureServer(&httpServer, nil); err != nil {
		log.Fatal(err)
	}

	stopper.RunWorker(func() {
		FatalIfUnexpected(httpServer.Serve(ln))

		<-stopper.ShouldStop()
		mu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		mu.Unlock()
	})

	logFn := logger.Printf
	return func(l net.Listener, serveConn func(net.Conn)) error {
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
					logFn("http: Accept error: %v; retrying in %v", e, tempDelay)
					time.Sleep(tempDelay)
					continue
				}
				return e
			}
			tempDelay = 0
			go func() {
				httpServer.ConnState(rw, http.StateNew) // before Serve can return
				serveConn(rw)
				httpServer.ConnState(rw, http.StateClosed)
			}()
		}
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
		log.Fatal(err)
	}
}
