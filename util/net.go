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
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tamird/cmux"
	"golang.org/x/net/http2"

	"github.com/cockroachdb/cockroach/util/stop"
)

// ListenAndServe creates a listener and serves handler on it, closing
// the listener when signalled by the stopper.
func ListenAndServe(stopper *stop.Stopper, handler http.Handler, addr net.Addr, tlsConfig *tls.Config) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err == nil {
		stopper.RunWorker(func() {
			<-stopper.ShouldDrain()
			// Some unit tests manually close `ln`, so it may already be closed
			// when we get here.
			if err := ln.Close(); err != nil && !IsClosedConnection(err) {
				log.Fatal(err)
			}
		})

		if tlsConfig != nil {
			ServeHandler(stopper, handler, tls.NewListener(ln, tlsConfig), tlsConfig)
		} else {
			m := cmux.New(ln)
			h2L := m.Match(cmux.HTTP2())
			anyL := m.Match(cmux.Any())

			var h2 http2.Server

			serveConnOpts := &http2.ServeConnOpts{
				Handler: handler,
			}

			serveH2 := func(conn net.Conn) {
				h2.ServeConn(conn, serveConnOpts)
			}

			serveConn := ServeHandler(stopper, handler, anyL, tlsConfig)

			stopper.RunWorker(func() {
				if err := serveConn(h2L, serveH2); err != nil && !IsClosedConnection(err) {
					log.Fatal(err)
				}
			})

			stopper.RunWorker(func() {
				if err := m.Serve(); err != nil && !IsClosedConnection(err) {
					log.Fatal(err)
				}
			})
		}
	}
	return ln, err
}

// ServeHandler serves the handler on the listener.
func ServeHandler(stopper *stop.Stopper, handler http.Handler, ln net.Listener, tlsConfig *tls.Config) func(net.Listener, func(net.Conn)) error {
	var mu sync.Mutex
	activeConns := make(map[net.Conn]struct{})

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
	}

	// net/http.(*Server).Serve/http2.ConfigureServer are not thread safe with
	// respect to net/http.(*Server).TLSConfig, so we call it synchronously here.
	if err := http2.ConfigureServer(&httpServer, nil); err != nil {
		log.Fatal(err)
	}

	stopper.RunWorker(func() {
		if err := httpServer.Serve(ln); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}

		<-stopper.ShouldStop()
		mu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		mu.Unlock()
	})

	logFn := log.Printf
	if httpServer.ErrorLog != nil {
		logFn = httpServer.ErrorLog.Printf
	}

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

// IsClosedConnection returns true if err is the net package's errClosed.
func IsClosedConnection(err error) bool {
	return err == cmux.ErrListenerClosed || strings.Contains(err.Error(), "use of closed network connection")
}
