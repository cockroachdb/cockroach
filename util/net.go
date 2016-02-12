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

	"golang.org/x/net/http2"

	"github.com/cockroachdb/cockroach/util/stop"
)

// Listen delegates to `net.Listen` and, if tlsConfig is not nil, to `tls.NewListener`.
// The returned listener's Addr() method will return an address with the hostname unresovled,
// which means it can be used to initiate TLS connections.
func Listen(addr net.Addr, tlsConfig *tls.Config) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err == nil && tlsConfig != nil {
		ln = tls.NewListener(ln, tlsConfig)
	}

	return ln, err
}

// ListenAndServe creates a listener and serves handler on it, closing
// the listener when signalled by the stopper.
func ListenAndServe(stopper *stop.Stopper, handler http.Handler, addr net.Addr, tlsConfig *tls.Config) (net.Listener, error) {
	ln, err := Listen(addr, tlsConfig)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	activeConns := make(map[net.Conn]struct{})

	httpServer := http.Server{
		TLSConfig: tlsConfig,
		Handler:   handler,
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
	if err := http2.ConfigureServer(&httpServer, nil); err != nil {
		return nil, err
	}

	stopper.RunWorker(func() {
		if err := httpServer.Serve(ln); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}

		mu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		mu.Unlock()
	})

	stopper.RunWorker(func() {
		<-stopper.ShouldStop()
		// Some unit tests manually close `ln`, so it may already be closed
		// when we get here.
		if err := ln.Close(); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}
	})

	return ln, nil
}

// IsClosedConnection returns true if err is the net package's errClosed.
func IsClosedConnection(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
