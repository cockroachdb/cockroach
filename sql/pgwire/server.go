// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package pgwire

import (
	"bytes"
	"net"
	"sync"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	versionSSL = []byte{0x12, 0x34, 0x56, 0x79}
	version30  = []byte{0x00, 0x03, 0x00, 0x00}
)

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	context  *Context
	listener net.Listener
	mu       sync.Mutex // Mutex protects the fields below
	conns    map[net.Conn]struct{}
	closing  bool
}

// MakeServer creates a Server.
func MakeServer(context *Context) *Server {
	return &Server{
		context: context,
		conns:   make(map[net.Conn]struct{}),
	}
}

// Start a server on the given address.
func (s *Server) Start(addr net.Addr) error {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return err
	}
	s.listener = ln

	s.context.Stopper.RunWorker(func() {
		s.serve(ln)
	})

	s.context.Stopper.RunWorker(func() {
		<-s.context.Stopper.ShouldStop()
		s.close()
	})
	return nil
}

// Addr returns this Server's address.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// serve connections on this listener until it is closed.
func (s *Server) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if !s.isClosing() {
				log.Error(err)
			}
			return
		}

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
				conn.Close()
			}()

			if err := s.serveConn(conn); err != nil {
				if !s.isClosing() {
					log.Error(err)
				}
			}
		}()
	}
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// close this server, and all client connections.
func (s *Server) close() {
	s.listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
	for conn := range s.conns {
		conn.Close()
	}
}

// serveConn serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) serveConn(conn net.Conn) error {
	var buf readBuffer
	if err := buf.readUntypedMsg(conn); err != nil {
		return err
	}
	version := buf.msg[:4]
	rest := buf.msg[4:]
	if bytes.Compare(version, versionSSL) == 0 {
		if len(rest) > 0 {
			return util.Errorf("unexpected data after SSL request")
		}
		panic("TODO(bdarnell): ssl mode")
	} else if bytes.Compare(version, version30) == 0 {
		v3conn, err := newV3Conn(conn, rest, s.context.Executor)
		if err != nil {
			return err
		}
		return v3conn.serve()
	}
	return util.Errorf("unknown protocol version %q", version)
}
