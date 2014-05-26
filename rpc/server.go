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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"net"
	"net/rpc"
	"sync"

	"github.com/golang/glog"
)

// Server is a Cockroach-specific RPC server with an embedded go RPC
// server struct.
type Server struct {
	*rpc.Server               // Embedded RPC server instance
	listener    net.Listener  // Server listener
	listening   chan struct{} // signaled when the server is listening

	mu             sync.RWMutex          // Mutex protects the fields below
	addr           net.Addr              // Server address; may change if picking unused port
	closed         bool                  // Set upon invocation of Close()
	closeCallbacks []func(conn net.Conn) // Slice of callbacks to invoke on conn close
}

// NewServer creates a new instance of Server.
func NewServer(addr net.Addr) *Server {
	s := &Server{
		Server:         rpc.NewServer(),
		addr:           addr,
		listening:      make(chan struct{}, 1),
		closeCallbacks: make([]func(conn net.Conn), 0, 1),
	}
	heartbeat := &HeartbeatService{}
	s.RegisterName("Heartbeat", heartbeat)
	return s
}

// AddCloseCallback adds a callback to the closeCallbacks slice to
// be invoked when a connection is closed.
func (s *Server) AddCloseCallback(cb func(conn net.Conn)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCallbacks = append(s.closeCallbacks, cb)
}

// ListenAndServe begins listening and serving. This method should be
// invoked by goroutine as it will loop serving incoming client and
// not return until Close() is invoked.
func (s *Server) ListenAndServe() {
	ln, err := net.Listen(s.addr.Network(), s.addr.String())
	if err != nil {
		glog.Fatalf("unable to start gossip node: %s", err)
	}
	s.listening <- struct{}{} // signal that we are now listening.
	s.listener = ln

	s.mu.Lock()
	s.addr = ln.Addr()
	s.mu.Unlock()

	// Start serving gossip protocol in a loop until listener is closed.
	glog.Infof("serving on %+v...", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.closed {
				glog.Fatalf("gossip server terminated: %s", err)
			}
			break
		}
		// Serve connection to completion in a goroutine.
		go s.serveConn(conn)
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
	s.listener.Close()
}

// serveConn synchronously serves a single connection. When the
// connection is closed, the client address is removed from the
// incoming set.
func (s *Server) serveConn(conn net.Conn) {
	s.ServeConn(conn)
	s.mu.Lock()
	for _, cb := range s.closeCallbacks {
		cb(conn)
	}
	s.mu.Unlock()
	conn.Close()
}
