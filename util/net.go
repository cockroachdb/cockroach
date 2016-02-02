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
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/util/stop"
)

var _ net.Listener = listener{}

type listener struct {
	addr net.Addr
	net.Listener
}

func (ln listener) Addr() net.Addr {
	return ln.addr
}

// updatedAddr returns our "official" address based on the address we asked for
// (oldAddr) and the address we successfully bound to (newAddr). It's kind of
// hacky, but necessary to make TLS work.
func updatedAddr(oldAddr, newAddr net.Addr) (net.Addr, error) {
	oldAddrStr := oldAddr.String()
	newAddrStr := newAddr.String()

	switch network := oldAddr.Network(); network {
	case "tcp", "tcp4", "tcp6":
		// After binding, it's possible that our host and/or port will be
		// different from what we requested. If the hostname is different, we
		// want to keep the original one since it's more likely to match our
		// TLS certificate. But if the port is different, it should be because
		// we asked for ":0" and got an arbitrary unused port; that needs to be
		// reflected in our addr.
		host, oldPort, err := net.SplitHostPort(EnsureHostPort(oldAddrStr))
		if err != nil {
			return nil, fmt.Errorf("unable to parse original addr '%s': %v", oldAddrStr, err)
		}
		_, newPort, err := net.SplitHostPort(newAddrStr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse new addr '%s': %v", newAddrStr, err)
		}

		if newPort != oldPort && oldPort != "0" {
			return nil, fmt.Errorf("asked for port %s, got %s", oldPort, newPort)
		}

		return MakeUnresolvedAddr(network, net.JoinHostPort(host, newPort)), nil

	case "unix":
		if oldAddrStr != newAddrStr {
			return nil, fmt.Errorf("asked for unix addr %s, got %s", oldAddr, newAddr)
		}
		return newAddr, nil

	default:
		return nil, fmt.Errorf("unexpected network type: %s", network)
	}
}

func listen(addr net.Addr, config *tls.Config) (net.Listener, error) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return nil, err
	}
	newAddr, err := updatedAddr(addr, ln.Addr())
	if err != nil {
		return nil, err
	}

	if config != nil {
		ln = tls.NewListener(ln, config)
	}

	return listener{newAddr, ln}, nil
}

// ListenAndServeGRPC creates a listener and serves server on it, closing
// the listener when signalled by the stopper.
func ListenAndServeGRPC(stopper *stop.Stopper, server *grpc.Server, addr net.Addr, config *tls.Config) (net.Listener, error) {
	ln, err := listen(addr, config)
	if err != nil {
		return nil, err
	}

	stopper.RunWorker(func() {
		if err := server.Serve(ln); err != nil && !IsClosedConnection(err) {
			log.Fatal(err)
		}
	})

	stopper.RunWorker(func() {
		<-stopper.ShouldStop()
		if err := ln.Close(); err != nil {
			log.Fatal(err)
		}
	})

	return ln, nil
}

// ListenAndServe creates a listener and serves handler on it, closing
// the listener when signalled by the stopper.
func ListenAndServe(stopper *stop.Stopper, handler http.Handler, addr net.Addr, config *tls.Config) (net.Listener, error) {
	ln, err := listen(addr, config)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	activeConns := make(map[net.Conn]struct{})

	httpServer := http.Server{
		TLSConfig: config,
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
	return strings.HasSuffix(err.Error(), "use of closed network connection")
}

func GRPCHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}
