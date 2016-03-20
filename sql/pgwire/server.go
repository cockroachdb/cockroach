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

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util"
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
	context  *base.Context
	executor *sql.Executor

	registry *metric.Registry
	metrics  *serverMetrics
}

type serverMetrics struct {
	bytesInCount  *metric.Counter
	bytesOutCount *metric.Counter
	conns         *metric.Counter
}

// MakeServer creates a Server, adding network stats to the given Registry.
func MakeServer(context *base.Context, executor *sql.Executor, reg *metric.Registry) Server {
	return Server{
		context:  context,
		executor: executor,
		registry: reg,
		metrics: &serverMetrics{
			conns:         reg.Counter("conns"),
			bytesInCount:  reg.Counter("bytesin"),
			bytesOutCount: reg.Counter("bytesout"),
		},
	}
}

// Match returns true if rd appears to be a Postgres connection.
func Match(rd io.Reader) bool {
	var buf readBuffer
	_, err := buf.readUntypedMsg(rd)
	if err != nil {
		return false
	}
	version, err := buf.getInt32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL
}

// ServeConn serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) ServeConn(conn net.Conn) error {
	s.metrics.conns.Inc(1)
	defer s.metrics.conns.Dec(1)

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
		sessionArgs, argsErr := parseOptions(buf.msg)
		// We make a connection regardless of argsErr. If there was an error parsing
		// the args, the connection will only be used to send a report of that
		// error.
		v3conn := makeV3Conn(conn, s.executor, s.metrics, sessionArgs)
		defer v3conn.finish()
		if argsErr != nil {
			return v3conn.sendInternalError(err.Error())
		}
		if errSSLRequired {
			return v3conn.sendInternalError(ErrSSLRequired)
		}
		if tlsConn, ok := conn.(*tls.Conn); ok {
			tlsState := tlsConn.ConnectionState()
			authenticationHook, err := security.UserAuthHook(s.context.Insecure, &tlsState)
			if err != nil {
				return v3conn.sendInternalError(err.Error())
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
