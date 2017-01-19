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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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

const (
	// drainMaxWait is the amount of time a draining server gives to sessions
	// with ongoing transactions to finish work before cancellation.
	drainMaxWait = 10 * time.Second
	// cancelMaxWait is the amount of time a draining server gives to sessions
	// to react to cancellation and return before a forceful shutdown.
	cancelMaxWait = 1 * time.Second
)

// baseSQLMemoryBudget is the amount of memory pre-allocated in each connection.
var baseSQLMemoryBudget = envutil.EnvOrDefaultInt64("COCKROACH_BASE_SQL_MEMORY_BUDGET",
	int64(2.1*float64(mon.DefaultPoolAllocationSize)))

// connReservationBatchSize determines for how many connections memory
// is pre-reserved at once.
var connReservationBatchSize = 5

var (
	sslSupported   = []byte{'S'}
	sslUnsupported = []byte{'N'}
)

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	ambientCtx log.AmbientContext
	cfg        *base.Config
	executor   *sql.Executor

	metrics ServerMetrics

	mu struct {
		syncutil.Mutex
		// All session contexts are derived from this cancellable context which
		// in turn is derived from the server's ambient context. This context
		// will be reset when the server exits draining mode, hence the need to
		// keep around the ambient context to derive a new context from.
		ctx           context.Context
		cancel        context.CancelFunc
		connDoneChans map[chan struct{}]struct{}
		draining      bool
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

	internalMemMetrics *sql.MemoryMetrics
}

func makeServerMetrics(internalMemMetrics *sql.MemoryMetrics) ServerMetrics {
	return ServerMetrics{
		Conns:              metric.NewCounter(MetaConns),
		BytesInCount:       metric.NewCounter(MetaBytesIn),
		BytesOutCount:      metric.NewCounter(MetaBytesOut),
		ConnMemMetrics:     sql.MakeMemMetrics("conns"),
		SQLMemMetrics:      sql.MakeMemMetrics("client"),
		internalMemMetrics: internalMemMetrics,
	}
}

// noteworthySQLMemoryUsageBytes is the minimum size tracked by the
// client SQL pool before the pool start explicitly logging overall
// usage growth in the log.
var noteworthySQLMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SQL_MEMORY_USAGE", 100*1024*1024)

// noteworthyConnMemoryUsageBytes is the minimum size tracked by the
// connection monitor before the monitor start explicitly logging overall
// usage growth in the log.
var noteworthyConnMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_CONN_MEMORY_USAGE", 2*1024*1024)

// MakeServer creates a Server.
func MakeServer(
	ambientCtx log.AmbientContext,
	cfg *base.Config,
	executor *sql.Executor,
	internalMemMetrics *sql.MemoryMetrics,
	maxSQLMem int64,
) *Server {
	server := &Server{
		ambientCtx: ambientCtx,
		cfg:        cfg,
		executor:   executor,
		metrics:    makeServerMetrics(internalMemMetrics),
	}
	server.sqlMemoryPool = mon.MakeMonitor("sql",
		server.metrics.SQLMemMetrics.CurBytesCount,
		server.metrics.SQLMemMetrics.MaxBytesHist,
		0, noteworthySQLMemoryUsageBytes)
	server.sqlMemoryPool.Start(context.Background(), nil, mon.MakeStandaloneBudget(maxSQLMem))

	server.connMonitor = mon.MakeMonitor("conn",
		server.metrics.ConnMemMetrics.CurBytesCount,
		server.metrics.ConnMemMetrics.MaxBytesHist,
		int64(connReservationBatchSize)*baseSQLMemoryBudget, noteworthyConnMemoryUsageBytes)
	server.connMonitor.Start(context.Background(), &server.sqlMemoryPool, mon.BoundAccount{})

	server.mu.Lock()
	server.resetStateLocked()
	server.mu.Unlock()

	return server
}

// s.mu must be locked.
func (s *Server) resetStateLocked() {
	s.mu.ctx, s.mu.cancel = context.WithCancel(s.ambientCtx.AnnotateCtx(context.Background()))
	s.mu.connDoneChans = make(map[chan struct{}]struct{})
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

// Ctx returns the current cancellable context.
func (s *Server) Ctx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.ctx
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
	return s.SetDrainingImpl(drain, drainMaxWait, cancelMaxWait)
}

// SetDrainingImpl is exported for testing purposes only.
func (s *Server) SetDrainingImpl(
	drain bool, drainWait time.Duration, cancelWait time.Duration,
) error {
	// This anonymous function returns a slice of channels to listen to for the
	// completion of the server's connections if the server is entering draining
	// mode.
	connDoneChans := func() map[chan struct{}]struct{} {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.mu.draining == drain {
			return nil
		}
		s.mu.draining = drain
		if !drain {
			// If the server is returning to an operational state, the context that
			// all new sessions will be derived from must be reset as well as the
			// map of connection done channels.
			s.resetStateLocked()
			return nil
		}

		// Copy the state of the connection done channel map within the critical
		// section.
		connDoneChans := make(map[chan struct{}]struct{})
		for done := range s.mu.connDoneChans {
			connDoneChans[done] = struct{}{}
		}
		return connDoneChans
	}()
	if connDoneChans == nil {
		return nil
	}

	// Spin off a goroutine that waits for all connections to signal that they
	// are done and reports it on allConnsDone. The main goroutine signals this
	// goroutine to stop work through quitWaitingForConns.
	allConnsDone := make(chan struct{})
	quitWaitingForConns := make(chan struct{})
	defer close(quitWaitingForConns)
	go func() {
		defer close(allConnsDone)
		for done := range connDoneChans {
			select {
			case <-done:
			case <-quitWaitingForConns:
				return
			}
		}
	}()

	// Wait for all connections to finish up to drainWait.
	select {
	case <-time.After(drainWait):
	case <-allConnsDone:
	}

	// Cancel the parent contexts of any sessions. Any context derived from a
	// cancelled context will also be cancelled.
	s.mu.cancel()

	select {
	case <-time.After(cancelWait):
		return errors.Errorf("some sessions did not respond to cancellation within %s", cancelWait)
	case <-allConnsDone:
	}
	return nil
}

// ServeConn serves a single connection, driving the handshake process
// and delegating to the appropriate connection type.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	var draining bool
	{
		s.mu.Lock()
		draining = s.mu.draining
		if !draining {
			done := make(chan struct{})
			s.mu.connDoneChans[done] = struct{}{}
			defer func() {
				close(done)
				s.mu.Lock()
				delete(s.mu.connDoneChans, done)
				s.mu.Unlock()
			}()
		}
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
		v3conn := makeV3Conn(conn, &s.metrics, &s.sqlMemoryPool, s.executor)
		defer v3conn.finish(ctx)

		if v3conn.sessionArgs, err = parseOptions(buf.msg); err != nil {
			return v3conn.sendInternalError(err.Error())
		}

		if errSSLRequired {
			return v3conn.sendInternalError(ErrSSLRequired)
		}
		if draining {
			return v3conn.sendError(newAdminShutdownErr(errors.New(ErrDraining)))
		}

		v3conn.sessionArgs.User = parser.Name(v3conn.sessionArgs.User).Normalize()
		if err := v3conn.handleAuthentication(ctx, s.cfg.Insecure); err != nil {
			return v3conn.sendInternalError(err.Error())
		}

		// Reserve some memory for this connection using the server's
		// monitor. This reduces pressure on the shared pool because the
		// server monitor allocates in chunks from the shared pool and
		// these chunks should be larger than baseSQLMemoryBudget.
		//
		// We only reserve memory to the connection monitor after
		// authentication has completed successfully, so as to prevent a DoS
		// attack: many open-but-unauthenticated connections that exhaust
		// the memory available to connections already open.
		acc := s.connMonitor.MakeBoundAccount(ctx)
		if err := acc.Grow(baseSQLMemoryBudget); err != nil {
			return errors.Errorf("unable to pre-allocate %d bytes for this connection: %v",
				baseSQLMemoryBudget, err)
		}

		err := v3conn.serve(ctx, s.IsDraining, acc)
		// If the error that closed the connection is related to an
		// administrative shutdown, relay that information to the client.
		if code, ok := pgerror.PGCode(err); ok && code == pgerror.CodeAdminShutdownError {
			return v3conn.sendError(err)
		}
		return err
	}

	return errors.Errorf("unknown protocol version %d", version)
}
