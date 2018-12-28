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

package pgwire

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// ATTENTION: After changing this value in a unit test, you probably want to
// open a new connection pool since the connections in the existing one are not
// affected.
//
// TODO(andrei): This setting is under "sql.defaults", but there's no way to
// control the setting on a per-connection basis. We should introduce a
// corresponding session variable.
var connResultsBufferSize = settings.RegisterByteSizeSetting(
	"sql.defaults.results_buffer.size",
	"size of the buffer that accumulates results for a statement or a batch "+
		"of statements before they are sent to the client. Note that auto-retries "+
		"generally only happen while no results have been delivered to the client, so "+
		"reducing this size can increase the number of retriable errors a client "+
		"receives. On the other hand, increasing the buffer size can increase the "+
		"delay until the client receives the first result row. "+
		"Updating the setting only affects new connections. "+
		"Setting to 0 disables any buffering.",
	16<<10, // 16 KiB
)

const (
	// ErrSSLRequired is returned when a client attempts to connect to a
	// secure server in cleartext.
	ErrSSLRequired = "node is running secure mode, SSL connection required"

	// ErrDraining is returned when a client attempts to connect to a server
	// which is not accepting client connections.
	ErrDraining = "server is not accepting clients"
)

// Fully-qualified names for metrics.
var (
	MetaConns = metric.Metadata{
		Name:        "sql.conns",
		Help:        "Number of active sql connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaBytesIn = metric.Metadata{
		Name:        "sql.bytesin",
		Help:        "Number of sql bytes received",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaBytesOut = metric.Metadata{
		Name:        "sql.bytesout",
		Help:        "Number of sql bytes sent",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
)

const (
	version30     = 196608
	versionSSL    = 80877103
	versionCancel = 80877102
)

// cancelMaxWait is the amount of time a draining server gives to sessions to
// react to cancellation and return before a forceful shutdown.
const cancelMaxWait = 1 * time.Second

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

// cancelChanMap keeps track of channels that are closed after the associated
// cancellation function has been called and the cancellation has taken place.
type cancelChanMap map[chan struct{}]context.CancelFunc

// Server implements the server side of the PostgreSQL wire protocol.
type Server struct {
	AmbientCtx log.AmbientContext
	cfg        *base.Config
	SQLServer  *sql.Server
	execCfg    *sql.ExecutorConfig

	metrics ServerMetrics

	mu struct {
		syncutil.Mutex
		// connCancelMap entries represent connections started when the server
		// was not draining. Each value is a function that can be called to
		// cancel the associated connection. The corresponding key is a channel
		// that is closed when the connection is done.
		connCancelMap cancelChanMap
		draining      bool
	}

	auth struct {
		syncutil.RWMutex
		conf *hba.Conf
	}

	sqlMemoryPool mon.BytesMonitor
	connMonitor   mon.BytesMonitor

	stopper *stop.Stopper
}

// ServerMetrics is the set of metrics for the pgwire server.
type ServerMetrics struct {
	BytesInCount   *metric.Counter
	BytesOutCount  *metric.Counter
	Conns          *metric.Gauge
	ConnMemMetrics sql.MemoryMetrics
	SQLMemMetrics  sql.MemoryMetrics
}

func makeServerMetrics(
	sqlMemMetrics sql.MemoryMetrics, histogramWindow time.Duration,
) ServerMetrics {
	return ServerMetrics{
		BytesInCount:   metric.NewCounter(MetaBytesIn),
		BytesOutCount:  metric.NewCounter(MetaBytesOut),
		Conns:          metric.NewGauge(MetaConns),
		ConnMemMetrics: sql.MakeMemMetrics("conns", histogramWindow),
		SQLMemMetrics:  sqlMemMetrics,
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
//
// Start() needs to be called on the Server so it begins processing.
func MakeServer(
	ambientCtx log.AmbientContext,
	cfg *base.Config,
	st *cluster.Settings,
	sqlMemMetrics sql.MemoryMetrics,
	parentMemoryMonitor *mon.BytesMonitor,
	histogramWindow time.Duration,
	executorConfig *sql.ExecutorConfig,
) *Server {
	server := &Server{
		AmbientCtx: ambientCtx,
		cfg:        cfg,
		execCfg:    executorConfig,
		metrics:    makeServerMetrics(sqlMemMetrics, histogramWindow),
	}
	server.sqlMemoryPool = mon.MakeMonitor("sql",
		mon.MemoryResource,
		server.metrics.SQLMemMetrics.CurBytesCount,
		server.metrics.SQLMemMetrics.MaxBytesHist,
		0, noteworthySQLMemoryUsageBytes, st)
	server.sqlMemoryPool.Start(context.Background(), parentMemoryMonitor, mon.BoundAccount{})
	server.SQLServer = sql.NewServer(executorConfig, &server.sqlMemoryPool)

	server.connMonitor = mon.MakeMonitor("conn",
		mon.MemoryResource,
		server.metrics.ConnMemMetrics.CurBytesCount,
		server.metrics.ConnMemMetrics.MaxBytesHist,
		int64(connReservationBatchSize)*baseSQLMemoryBudget, noteworthyConnMemoryUsageBytes, st)
	server.connMonitor.Start(context.Background(), &server.sqlMemoryPool, mon.BoundAccount{})

	server.mu.Lock()
	server.mu.connCancelMap = make(cancelChanMap)
	server.mu.Unlock()

	connAuthConf.SetOnChange(&st.SV, func() {
		val := connAuthConf.Get(&st.SV)
		server.auth.Lock()
		defer server.auth.Unlock()
		if val == "" {
			server.auth.conf = nil
			return
		}
		conf, err := hba.Parse(val)
		if err != nil {
			log.Warningf(ambientCtx.AnnotateCtx(context.Background()), "invalid %s: %v", serverHBAConfSetting, err)
			conf = nil
		}
		server.auth.conf = conf
	})

	return server
}

// Match returns true if rd appears to be a Postgres connection.
func Match(rd io.Reader) bool {
	var buf pgwirebase.ReadBuffer
	_, err := buf.ReadUntypedMsg(rd)
	if err != nil {
		return false
	}
	version, err := buf.GetUint32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL || version == versionCancel
}

// Start makes the Server ready for serving connections.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	s.stopper = stopper
	s.SQLServer.Start(ctx, stopper)
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

// StatementCounters returns the Server's StatementCounters.
func (s *Server) StatementCounters() *sql.StatementCounters {
	return &s.SQLServer.StatementCounters
}

// EngineMetrics returns the Server's EngineMetrics.
func (s *Server) EngineMetrics() *sql.EngineMetrics {
	return &s.SQLServer.EngineMetrics
}

// Drain prevents new connections from being served and waits for drainWait for
// open connections to terminate before canceling them.
// An error will be returned when connections that have been canceled have not
// responded to this cancellation and closed themselves in time. The server
// will remain in draining state, though open connections may continue to
// exist.
// The RFC on drain modes has more information regarding the specifics of
// what will happen to connections in different states:
// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160425_drain_modes.md
func (s *Server) Drain(drainWait time.Duration) error {
	return s.drainImpl(drainWait, cancelMaxWait)
}

// Undrain switches the server back to the normal mode of operation in which
// connections are accepted.
func (s *Server) Undrain() {
	s.mu.Lock()
	s.setDrainingLocked(false)
	s.mu.Unlock()
}

// setDrainingLocked sets the server's draining state and returns whether the
// state changed (i.e. drain != s.mu.draining). s.mu must be locked.
func (s *Server) setDrainingLocked(drain bool) bool {
	if s.mu.draining == drain {
		return false
	}
	s.mu.draining = drain
	return true
}

func (s *Server) drainImpl(drainWait time.Duration, cancelWait time.Duration) error {
	// This anonymous function returns a copy of s.mu.connCancelMap if there are
	// any active connections to cancel. We will only attempt to cancel
	// connections that were active at the moment the draining switch happened.
	// It is enough to do this because:
	// 1) If no new connections are added to the original map all connections
	// will be canceled.
	// 2) If new connections are added to the original map, it follows that they
	// were added when s.mu.draining = false, thus not requiring cancellation.
	// These connections are not our responsibility and will be handled when the
	// server starts draining again.
	connCancelMap := func() cancelChanMap {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.setDrainingLocked(true) {
			// We are already draining.
			return nil
		}
		connCancelMap := make(cancelChanMap)
		for done, cancel := range s.mu.connCancelMap {
			connCancelMap[done] = cancel
		}
		return connCancelMap
	}()
	if len(connCancelMap) == 0 {
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
		for done := range connCancelMap {
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

	// Cancel the contexts of all sessions if the server is still in draining
	// mode.
	if stop := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.mu.draining {
			return true
		}
		for _, cancel := range connCancelMap {
			// There is a possibility that different calls to SetDraining have
			// overlapping connCancelMaps, but context.CancelFunc calls are
			// idempotent.
			cancel()
		}
		return false
	}(); stop {
		return nil
	}

	select {
	case <-time.After(cancelWait):
		return errors.Errorf("some sessions did not respond to cancellation within %s", cancelWait)
	case <-allConnsDone:
	}
	return nil
}

// ServeConn serves a single connection, driving the handshake process and
// delegating to the appropriate connection type.
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	s.mu.Lock()
	draining := s.mu.draining
	if !draining {
		var cancel context.CancelFunc
		ctx, cancel = contextutil.WithCancel(ctx)
		done := make(chan struct{})
		s.mu.connCancelMap[done] = cancel
		defer func() {
			cancel()
			close(done)
			s.mu.Lock()
			delete(s.mu.connCancelMap, done)
			s.mu.Unlock()
		}()
	}
	s.mu.Unlock()

	// If the Server is draining, we will use the connection only to send an
	// error, so we don't count it in the stats. This makes sense since
	// DrainClient() waits for that number to drop to zero,
	// so we don't want it to oscillate unnecessarily.
	if !draining {
		s.metrics.Conns.Inc(1)
		defer s.metrics.Conns.Dec(1)
	}

	var buf pgwirebase.ReadBuffer
	n, err := buf.ReadUntypedMsg(conn)
	if err != nil {
		return err
	}
	s.metrics.BytesInCount.Inc(int64(n))
	version, err := buf.GetUint32()
	if err != nil {
		return err
	}
	errSSLRequired := false
	if version == versionSSL {
		if len(buf.Msg) > 0 {
			return errors.Errorf("unexpected data after SSLRequest: %q", buf.Msg)
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

		n, err := buf.ReadUntypedMsg(conn)
		if err != nil {
			return err
		}
		s.metrics.BytesInCount.Inc(int64(n))
		version, err = buf.GetUint32()
		if err != nil {
			return err
		}
	} else if !s.cfg.Insecure {
		errSSLRequired = true
	}

	sendErr := func(err error) error {
		msgBuilder := newWriteBuffer(s.metrics.BytesOutCount)
		_ /* err */ = writeErr(err, msgBuilder, conn)
		_ = conn.Close()
		return err
	}

	if version != version30 {
		if version == versionCancel {
			telemetry.Count("pgwire.unimplemented.cancel_request")
			_ = conn.Close()
			return nil
		}
		return sendErr(fmt.Errorf("unknown protocol version %d", version))
	}
	if errSSLRequired {
		return sendErr(pgerror.NewError(pgerror.CodeProtocolViolationError, ErrSSLRequired))
	}
	if draining {
		return sendErr(newAdminShutdownErr(errors.New(ErrDraining)))
	}

	var sArgs sql.SessionArgs
	if sArgs, err = parseOptions(ctx, buf.Msg); err != nil {
		return sendErr(err)
	}
	sArgs.User = tree.Name(sArgs.User).Normalize()

	// Reserve some memory for this connection using the server's monitor. This
	// reduces pressure on the shared pool because the server monitor allocates in
	// chunks from the shared pool and these chunks should be larger than
	// baseSQLMemoryBudget.
	reserved := s.connMonitor.MakeBoundAccount()
	if err := reserved.Grow(ctx, baseSQLMemoryBudget); err != nil {
		return errors.Errorf("unable to pre-allocate %d bytes for this connection: %v",
			baseSQLMemoryBudget, err)
	}

	s.auth.RLock()
	auth := s.auth.conf
	s.auth.RUnlock()

	return serveConn(
		ctx, conn, sArgs,
		connResultsBufferSize.Get(&s.execCfg.Settings.SV),
		&s.metrics, reserved, s.SQLServer,
		s.IsDraining, s.execCfg.InternalExecutor, s.stopper, s.cfg.Insecure, auth)
}

func parseOptions(ctx context.Context, data []byte) (sql.SessionArgs, error) {
	args := sql.SessionArgs{SessionDefaults: make(map[string]string)}
	buf := pgwirebase.ReadBuffer{Msg: data}
	for {
		key, err := buf.GetString()
		if err != nil {
			return sql.SessionArgs{}, pgerror.NewErrorf(pgerror.CodeProtocolViolationError,
				"error reading option key: %s", err)
		}
		if len(key) == 0 {
			break
		}
		value, err := buf.GetString()
		if err != nil {
			return sql.SessionArgs{}, pgerror.NewErrorf(pgerror.CodeProtocolViolationError,
				"error reading option value: %s", err)
		}
		key = strings.ToLower(key)
		switch key {
		case "user":
			args.User = value
		default:
			exists, configurable := sql.IsSessionVariableConfigurable(key)
			if exists && configurable {
				args.SessionDefaults[key] = value
			} else {
				if !exists {
					if _, ok := sql.UnsupportedVars[key]; ok {
						telemetry.Count("unimplemented.pgwire.parameter." + key)
					}
					log.Warningf(ctx, "unknown configuration parameter: %q", key)
				} else {
					return sql.SessionArgs{}, pgerror.NewErrorf(pgerror.CodeCantChangeRuntimeParamError,
						"parameter %q cannot be changed", key)
				}
			}
		}
	}

	if _, ok := args.SessionDefaults["database"]; !ok {
		// CockroachDB-specific behavior: if no database is specified,
		// default to "defaultdb". In PostgreSQL this would be "postgres".
		args.SessionDefaults["database"] = sessiondata.DefaultDatabaseName
	}

	return args, nil
}

func newAdminShutdownErr(err error) error {
	return pgerror.NewErrorf(pgerror.CodeAdminShutdownError, err.Error())
}
