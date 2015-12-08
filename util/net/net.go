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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package net

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/util/stop"
)

// ListenAndServe creates a listener and serves handler on it, closing
// the listener when signalled by the stopper.
func ListenAndServe(stopper *stop.Stopper, handler http.Handler, addr net.Addr, config *tls.Config) (ln net.Listener, err error) {
	ln, err = net.Listen(addr.Network(), addr.String())
	if err != nil {
		return
	}
	if config != nil {
		ln = tls.NewListener(ln, config)
	}

	stopper.RunWorker(func() {
		var mu sync.Mutex
		activeConns := make(map[net.Conn]struct{})

		httpServer := http.Server{
			Handler: handler,
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
		// `ln` might already be closed.
		if err := ln.Close(); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}
	})

	return
}

// IsClosedConnection returns true if err is the net package's errClosed.
func IsClosedConnection(err error) bool {
	return strings.HasSuffix(err.Error(), "use of closed network connection")
}
