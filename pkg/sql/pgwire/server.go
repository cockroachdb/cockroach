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
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

const (
	// ErrSSLRequired is returned when a client attempts to connect to a
	// secure server in cleartext.
	ErrSSLRequired = "cleartext connections are not permitted"

	// ErrDraining is returned when a client attempts to connect to a server
	// which is not accepting client connections.
	ErrDraining = "server is not accepting clients"
)

// Fully-qualified names for metrics.
var (
	MetaConns    = metric.Metadata{Name: "sql.conns"}
	MetaBytesIn  = metric.Metadata{Name: "sql.bytesin"}
	MetaBytesOut = metric.Metadata{Name: "sql.bytesout"}
)

const (
	version30  = 196608
	versionSSL = 80877103
)

const drainMaxWait = 10 * time.Second

var (
	sslSupported   = []byte{'S'}
	sslUnsupported = []byte{'N'}
)

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	AmbientCtx log.AmbientContext
	cfg        *base.Config
	executor   *sql.Executor

	metrics ServerMetrics

	mu struct {
		syncutil.Mutex
		draining bool
	}

	sqlMemoryPool mon.MemoryMonitor
	connMonitor   mon.MemoryMonitor
}

// ServerMetrics is the set of metrics for the pgwire server.
type ServerMetrics struct {
	BytesInCount   *metric.Counter
	BytesOutCount  *metric.Counter
	Conns          *metric.Counter
	ConnMemMetrics sql.MemoryMetrics
	SQLMemMetrics  sql.MemoryMetrics
}

func makeServerMetrics() ServerMetrics {
	return ServerMetrics{
		Conns:          metric.NewCounter(MetaConns),
		BytesInCount:   metric.NewCounter(MetaBytesIn),
		BytesOutCount:  metric.NewCounter(MetaBytesOut),
		ConnMemMetrics: sql.MakeMemMetrics("conns"),
		SQLMemMetrics:  sql.MakeMemMetrics("client"),
	}
}

// MakeServer creates a Server.
func MakeServer(
	ambientCtx log.AmbientContext, cfg *base.Config, executor *sql.Executor, maxSQLMem int64,
) *Server {
	server := &Server{
		AmbientCtx: ambientCtx,
		cfg:        cfg,
		executor:   executor,
		metrics:    makeServerMetrics(),
	}
	server.sqlMemoryPool.Start("sql-mon",
		server.metrics.SQLMemMetrics.CurBytesCount,
		server.metrics.SQLMemMetrics.MaxBytesHist,
		nil, maxSQLMem, 1)
	server.connMonitor.Start("conn-mon",
		server.metrics.ConnMemMetrics.CurBytesCount,
		server.metrics.ConnMemMetrics.MaxBytesHist,
		&server.sqlMemoryPool, 0, -1)
	return server
}

// Match returns true if rd appears to be a Postgres connection.
func Match(rd io.Reader) bool {
	var buf readBuffer
	_, err := buf.readUntypedMsg(rd)
	if err != nil {
		return false
	}
	version, err := buf.getUint32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL
}

// IsDraining returns true if the server is not currently accepting
// connections.
func (s *Server) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.draining
}

// Metrics returns the metrics struct.
func (s *Server) Metrics() *ServerMetrics {
	return &s.metrics
}

// SetDraining (when called with 'true') prevents new connections from being
// served and waits a reasonable amount of time for open connections to
// terminate. If an error is returned, the server remains in draining state,
// though open connections may continue to exist.
// When called with 'false', switches back to the normal mode of operation in
// which connections are accepted.
func (s *Server) SetDraining(drain bool) error {
	s.mu.Lock()
	s.mu.draining = drain
	s.mu.Unlock()
	if !drain {
		return nil
	}
	return util.RetryForDuration(drainMaxWait, func() error {
		if c := s.metrics.Conns.Count(); c != 0 {
			// TODO(tschottdorf): Do more plumbing to actively disrupt
			// connections; see #6283. There isn't much of a point until
			// we know what load-balanced clients like to see (#6295).
			return fmt.Errorf("timed out waiting for %d open connections to drain", c)
		}
		return nil
	})
}

// ServeConn serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	var draining bool
	{
		s.mu.Lock()
		draining = s.mu.draining
		s.mu.Unlock()
	}

	// If the Server is draining, we will use the connection only to send an
	// error, so we don't count it in the stats. This makes sense since
	// DrainClient() waits for that number to drop to zero,
	// so we don't want it to oscillate unnecessarily.
	if !draining {
		s.metrics.Conns.Inc(1)
		defer s.metrics.Conns.Dec(1)
	}

	var buf readBuffer
	n, err := buf.readUntypedMsg(conn)
	if err != nil {
		return err
	}
	s.metrics.BytesInCount.Inc(int64(n))
	version, err := buf.getUint32()
	if err != nil {
		return err
	}
	errSSLRequired := false
	if version == versionSSL {
		if len(buf.msg) > 0 {
			return errors.Errorf("unexpected data after SSLRequest: %q", buf.msg)
		}

		if s.cfg.Insecure {
			if _, err := conn.Write(sslUnsupported); err != nil {
				return err
			}
		} else {
			if _, err := conn.Write(sslSupported); err != nil {
				return err
			}
			tlsConfig, err := s.cfg.GetServerTLSConfig()
			if err != nil {
				return err
			}
			conn = tls.Server(conn, tlsConfig)
		}

		n, err := buf.readUntypedMsg(conn)
		if err != nil {
			return err
		}
		s.metrics.BytesInCount.Inc(int64(n))
		version, err = buf.getUint32()
		if err != nil {
			return err
		}
	} else if !s.cfg.Insecure {
		errSSLRequired = true
	}

	if version == version30 {
		// We make a connection before anything. If there is an error
		// parsing the connection arguments, the connection will only be
		// used to send a report of that error.
		v3conn := makeV3Conn(ctx, conn, &s.metrics, &s.sqlMemoryPool, &s.connMonitor)
		defer v3conn.finish(ctx)

		if v3conn.sessionArgs, err = parseOptions(buf.msg); err != nil {
			return v3conn.sendInternalError(err.Error())
		}
		if errSSLRequired {
			return v3conn.sendInternalError(ErrSSLRequired)
		}
		if draining {
			// TODO(tschottdorf): Likely not handled gracefully by clients.
			// See #6295.
			return v3conn.sendInternalError(ErrDraining)
		}

		if err := v3conn.handleAuthentication(ctx, s.cfg.Insecure); err != nil {
			return v3conn.sendInternalError(err.Error())
		}
		return v3conn.serve(ctx, s.executor)
	}

	return errors.Errorf("unknown protocol version %d", version)
}
