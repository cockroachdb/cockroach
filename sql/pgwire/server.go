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
// permissions and limitations under the License.
//
// Author: Ben Darnell

package pgwire

import (
	"crypto/tls"
	"io"
	"net"
	"sync"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
)

// ErrSSLRequired is returned when a client attempts to connect to a
// secure server in cleartext.
const ErrSSLRequired = "cleartext connections are not permitted"

const (
	version30  = 196608
	versionSSL = 80877103
)

var (
	sslSupported   = []byte{'S'}
	sslUnsupported = []byte{'N'}
)

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	context  *Context
	listener net.Listener
	mu       sync.Mutex // Mutex protects the fields below
	conns    map[net.Conn]struct{}
	closing  bool
	registry *metric.Registry
	metrics  *serverMetrics
}

type serverMetrics struct {
	bytesInCount  *metric.Counter
	bytesOutCount *metric.Counter
}

// NewServer creates a Server.
func NewServer(context *Context) *Server {
	// Create a registry to hold pgwire stats.
	reg := metric.NewRegistry()
	metrics := &serverMetrics{
		bytesInCount:  reg.Counter("bytesin"),
		bytesOutCount: reg.Counter("bytesout"),
	}

	return &Server{
		context:  context,
		conns:    make(map[net.Conn]struct{}),
		metrics:  metrics,
		registry: reg,
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
	log.Infof("starting postgres server at %s", ln.Addr())
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
				if err != io.EOF && !s.isClosing() {
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
	n, err := buf.readUntypedMsg(conn)
	if err != nil {
		return err
	}
	s.metrics.bytesInCount.Inc(int64(n))
	version, err := buf.getInt32()
	if err != nil {
		return err
	}
	errSSLRequired := false
	if version == versionSSL {
		if len(buf.msg) > 0 {
			return util.Errorf("unexpected data after SSLRequest: %q", buf.msg)
		}

		if s.context.Insecure {
			if _, err := conn.Write(sslUnsupported); err != nil {
				return err
			}
		} else {
			if _, err := conn.Write(sslSupported); err != nil {
				return err
			}
			tlsConfig, err := s.context.GetServerTLSConfig()
			if err != nil {
				return err
			}
			conn = tls.Server(conn, tlsConfig)
		}

		n, err := buf.readUntypedMsg(conn)
		if err != nil {
			return err
		}
		s.metrics.bytesInCount.Inc(int64(n))
		version, err = buf.getInt32()
		if err != nil {
			return err
		}
	} else if !s.context.Insecure {
		errSSLRequired = true
	}

	if version == version30 {
		v3conn := makeV3Conn(conn, s.context.Executor, s.metrics)
		// This is better than always flushing on error.
		defer func() {
			if err := v3conn.wr.Flush(); err != nil {
				log.Error(err)
			}
		}()
		if errSSLRequired {
			return v3conn.sendError(ErrSSLRequired)
		}
		if err := v3conn.parseOptions(buf.msg); err != nil {
			return v3conn.sendError(err.Error())
		}
		if tlsConn, ok := conn.(*tls.Conn); ok {
			tlsState := tlsConn.ConnectionState()
			authenticationHook, err := security.UserAuthHook(s.context.Insecure, &tlsState)
			if err != nil {
				return v3conn.sendError(err.Error())
			}
			return v3conn.serve(authenticationHook)
		}
		return v3conn.serve(nil)
	}

	return util.Errorf("unknown protocol version %d", version)
}

// Registry returns a registry with the metrics tracked by this server, which can be used to
// access its stats or be added to another registry.
func (s *Server) Registry() *metric.Registry {
	return s.registry
}
