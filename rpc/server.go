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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/cockroachdb/cockroach/rpc/codec"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Server is a Cockroach-specific RPC server with an embedded go RPC
// server struct. By default it handles a simple heartbeat protocol
// to measure link health. It also supports close callbacks.
//
// TODO(spencer): heartbeat protocol should also measure link latency.
type Server struct {
	*rpc.Server              // Embedded RPC server instance
	listener    net.Listener // Server listener
	handler     http.Handler

	context *Context

	mu             sync.RWMutex          // Mutex protects the fields below
	addr           net.Addr              // Server address; may change if picking unused port
	closed         bool                  // Set upon invocation of Close()
	closeCallbacks []func(conn net.Conn) // Slice of callbacks to invoke on conn close
}

// NewServer creates a new instance of Server.
func NewServer(addr net.Addr, context *Context) *Server {
	s := &Server{
		Server:  rpc.NewServer(),
		context: context,
		addr:    addr,
	}
	heartbeat := &HeartbeatService{
		clock:              context.localClock,
		remoteClockMonitor: context.RemoteClocks,
	}
	if err := s.RegisterName("Heartbeat", heartbeat); err != nil {
		log.Fatalf("unable to register heartbeat service with RPC server: %s", err)
	}
	return s
}

// AddCloseCallback adds a callback to the closeCallbacks slice to
// be invoked when a connection is closed.
func (s *Server) AddCloseCallback(cb func(conn net.Conn)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCallbacks = append(s.closeCallbacks, cb)
}

// Can connect to RPC service using HTTP CONNECT to rpcPath.
var connected = "200 Connected to Go RPC"

// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != rpc.DefaultRPCPath {
		if s.handler != nil {
			s.handler.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	// Note: this code was adapted from net/rpc.Server.ServeHTTP.
	if r.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Infof("rpc hijacking %s: %s", r.RemoteAddr, err)
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.serveConn(conn)
}

// Listen listens on the configured address but does not start
// accepting connections until Serve is called.
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ln, err := tlsListen(s.addr.Network(), s.addr.String(), s.context.tlsConfig)
	if err != nil {
		return err
	}
	s.listener = ln

	addr, err := updatedAddr(s.addr, ln.Addr())
	if err != nil {
		s.Close()
		return err
	}
	s.addr = addr

	return nil
}

// Serve accepts and services connections on the already started
// listener.
func (s *Server) Serve(handler http.Handler) {
	s.handler = handler
	go http.Serve(s.listener, s)
}

// Start runs the RPC server. After this method returns, the socket
// will have been bound. Use Server.Addr() to ascertain server address.
func (s *Server) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.Serve(s)
	return nil
}

// updatedAddr returns our "official" address based on the address we asked for
// (oldAddr) and the address we successfully bound to (newAddr). It's kind of
// hacky, but necessary to make TLS work.
func updatedAddr(oldAddr, newAddr net.Addr) (net.Addr, error) {
	switch oldAddr.Network() {
	case "tcp", "tcp4", "tcp6":
		// After binding, it's possible that our host and/or port will be
		// different from what we requested. If the hostname is different, we
		// want to keep the original one since it's more likely to match our
		// TLS certificate. But if the port is different, it should be because
		// we asked for ":0" and got an arbitrary unused port; that needs to be
		// reflected in our addr.
		host, oldPort, err := net.SplitHostPort(util.EnsureHost(oldAddr.String()))
		if err != nil {
			return nil, fmt.Errorf("unable to parse original addr '%s': %v",
				oldAddr.String(), err)
		}
		_, newPort, err := net.SplitHostPort(newAddr.String())
		if err != nil {
			return nil, fmt.Errorf("unable to parse new addr '%s': %v",
				newAddr.String(), err)
		}

		if newPort != oldPort && oldPort != "0" {
			log.Warningf("asked for port %s, got %s", oldPort, newPort)
		}

		return util.MakeRawAddr("tcp", net.JoinHostPort(host, newPort)), nil

	case "unix":
		if oldAddr.String() != newAddr.String() {
			return nil, fmt.Errorf("asked for unix addr %s, got %s", oldAddr, newAddr)
		}
		return newAddr, nil

	default:
		return nil, fmt.Errorf("unexpected network type: %s", oldAddr.Network())
	}
}

// Addr returns the server's network address.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// Close closes the listener.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	// If the server didn't start properly, it might not have a listener.
	if s.listener != nil {
		s.listener.Close()
	}
}

// serveConn synchronously serves a single connection. When the
// connection is closed, close callbacks are invoked.
func (s *Server) serveConn(conn net.Conn) {
	s.ServeCodec(codec.NewServerCodec(conn))
	s.mu.Lock()
	if s.closeCallbacks != nil {
		for _, cb := range s.closeCallbacks {
			cb(conn)
		}
	}
	s.mu.Unlock()
	conn.Close()
}
