// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ATTENTION: After changing this value in a unit test, you probably want to
// open a new connection pool since the connections in the existing one are not
// affected.
//
// The "results_buffer_size" connection parameter can be used to override this
// default for an individual connection.
var connResultsBufferSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.defaults.results_buffer.size",
	"default size of the buffer that accumulates results for a statement or a batch "+
		"of statements before they are sent to the client. This can be overridden on "+
		"an individual connection with the 'results_buffer_size' parameter. Note that auto-retries "+
		"generally only happen while no results have been delivered to the client, so "+
		"reducing this size can increase the number of retriable errors a client "+
		"receives. On the other hand, increasing the buffer size can increase the "+
		"delay until the client receives the first result row. "+
		"Updating the setting only affects new connections. "+
		"Setting to 0 disables any buffering.",
	16<<10, // 16 KiB
).WithPublic()

var logConnAuth = settings.RegisterBoolSetting(
	settings.TenantWritable,
	sql.ConnAuditingClusterSettingName,
	"if set, log SQL client connect and disconnect events (note: may hinder performance on loaded nodes)",
	false).WithPublic()

var logSessionAuth = settings.RegisterBoolSetting(
	settings.TenantWritable,
	sql.AuthAuditingClusterSettingName,
	"if set, log SQL session login/disconnection events (note: may hinder performance on loaded nodes)",
	false).WithPublic()

// TODO(alyshan): This setting is enforcing max number of connections with superusers not being affected by
// the limit. However, admin users connections are counted towards the max count. So we should either update the
// description to say "the maximum number of connections per gateway ... Superusers are not affected by this limit"
// or stop counting superuser connections towards the max count.
var maxNumNonAdminConnections = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.max_connections_per_gateway",
	"the maximum number of non-superuser SQL connections per gateway allowed at a given time "+
		"(note: this will only limit future connection attempts and will not affect already established connections). "+
		"Negative values result in unlimited number of connections. Superusers are not affected by this limit.",
	-1, // Postgres defaults to 100, but we default to -1 to match our previous behavior of unlimited.
).WithPublic()

// Note(alyshan): This setting is not public. It is intended to be used by Cockroach Cloud to limit
// connections to serverless clusters while still being able to connect from the Cockroach Cloud control plane.
// This setting may be extended one day to include an arbitrary list of users to exclude from connection limiting.
// This setting may be removed one day.
var maxNumNonRootConnections = settings.RegisterIntSetting(
	settings.TenantWritable,
	"server.cockroach_cloud.max_client_connections_per_gateway",
	"this setting is intended to be used by Cockroach Cloud for limiting connections to serverless clusters. "+
		"The maximum number of SQL connections per gateway allowed at a given time "+
		"(note: this will only limit future connection attempts and will not affect already established connections). "+
		"Negative values result in unlimited number of connections. Cockroach Cloud internal users (including root user) "+
		"are not affected by this limit.",
	-1,
)

// maxNumNonRootConnectionsReason is used to supplement the error message for connections that denied due to
// server.cockroach_cloud.max_client_connections_per_gateway.
// Note(alyshan): This setting is not public. It is intended to be used by Cockroach Cloud when limiting
// connections to serverless clusters.
// This setting may be removed one day.
var maxNumNonRootConnectionsReason = settings.RegisterStringSetting(
	settings.TenantWritable,
	"server.cockroach_cloud.max_client_connections_per_gateway_reason",
	"a reason to provide in the error message for connections that are denied due to "+
		"server.cockroach_cloud.max_client_connections_per_gateway",
	"",
)

const (
	// ErrSSLRequired is returned when a client attempts to connect to a
	// secure server in cleartext.
	ErrSSLRequired = "node is running secure mode, SSL connection required"

	// ErrDrainingNewConn is returned when a client attempts to connect to a server
	// which is not accepting client connections.
	ErrDrainingNewConn = "server is not accepting clients, try another node"
	// ErrDrainingExistingConn is returned when a connection is shut down because
	// the server is draining.
	ErrDrainingExistingConn = "server is shutting down"
)

// Fully-qualified names for metrics.
var (
	MetaConns = metric.Metadata{
		Name:        "sql.conns",
		Help:        "Number of open SQL connections",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaNewConns = metric.Metadata{
		Name:        "sql.new_conns",
		Help:        "Counter of the number of SQL connections created",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaBytesIn = metric.Metadata{
		Name:        "sql.bytesin",
		Help:        "Number of SQL bytes received",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaBytesOut = metric.Metadata{
		Name:        "sql.bytesout",
		Help:        "Number of SQL bytes sent",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaConnLatency = metric.Metadata{
		Name:        "sql.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	MetaConnFailures = metric.Metadata{
		Name:        "sql.conn.failures",
		Help:        "Number of SQL connection failures",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaPGWireCancelTotal = metric.Metadata{
		Name:        "sql.pgwire_cancel.total",
		Help:        "Counter of the number of pgwire query cancel requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	MetaPGWireCancelIgnored = metric.Metadata{
		Name:        "sql.pgwire_cancel.ignored",
		Help:        "Counter of the number of pgwire query cancel requests that were ignored due to rate limiting",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	MetaPGWireCancelSuccessful = metric.Metadata{
		Name:        "sql.pgwire_cancel.successful",
		Help:        "Counter of the number of pgwire query cancel requests that were successful",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

const (
	// The below constants can occur during the first message a client
	// sends to the server. There are two categories: protocol version and
	// request code. The protocol version is (major version number << 16)
	// + minor version number. Request codes are (1234 << 16) + 5678 + N,
	// where N started at 0 and is increased by 1 for every new request
	// code added, which happens rarely during major or minor Postgres
	// releases.
	//
	// See: https://www.postgresql.org/docs/current/protocol-message-formats.html

	version30     = 196608   // (3 << 16) + 0
	versionCancel = 80877102 // (1234 << 16) + 5678
	versionSSL    = 80877103 // (1234 << 16) + 5679
	versionGSSENC = 80877104 // (1234 << 16) + 5680
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

// Server implements the server side of the PostgreSQL wire protocol for one
// specific tenant (i.e. its configuration is specific to one tenant).
type Server struct {
	AmbientCtx log.AmbientContext
	cfg        *base.Config
	SQLServer  *sql.Server
	execCfg    *sql.ExecutorConfig

	tenantMetrics tenantSpecificMetrics

	mu struct {
		syncutil.Mutex
		// connCancelMap entries represent connections started when the server
		// was not draining. Each value is a function that can be called to
		// cancel the associated connection. The corresponding key is a channel
		// that is closed when the connection is done.
		connCancelMap cancelChanMap
		// draining is set to true when the server starts draining the SQL layer.
		// When set to true, remaining SQL connections will be closed.
		// After the timeout set by server.shutdown.query_wait,
		// all connections will be closed regardless any queries in flight.
		draining bool
		// rejectNewConnections is set true when the server does not accept new
		// SQL connections, e.g. when the draining process enters the phase whose
		// duration is specified by the server.shutdown.connection_wait.
		rejectNewConnections bool
	}

	auth struct {
		syncutil.RWMutex
		conf        *hba.Conf
		identityMap *identmap.Conf
	}

	// sqlMemoryPool is the parent memory pool for all SQL memory allocations
	// for this tenant, including SQL query execution, etc.
	sqlMemoryPool *mon.BytesMonitor

	// tenantSpecificConnMonitor is the pool where the memory usage for the
	// initial connection overhead is accounted for.
	tenantSpecificConnMonitor *mon.BytesMonitor

	// testing{Conn,Auth}LogEnabled is used in unit tests in this
	// package to force-enable conn/auth logging without dancing around
	// the asynchronicity of cluster settings.
	testingConnLogEnabled int32
	testingAuthLogEnabled int32
}

// tenantSpecificMetrics is the set of metrics for a pgwire server
// bound to a specific tenant.
type tenantSpecificMetrics struct {
	BytesInCount                *metric.Counter
	BytesOutCount               *metric.Counter
	Conns                       *metric.Gauge
	NewConns                    *metric.Counter
	ConnLatency                 metric.IHistogram
	ConnFailures                *metric.Counter
	PGWireCancelTotalCount      *metric.Counter
	PGWireCancelIgnoredCount    *metric.Counter
	PGWireCancelSuccessfulCount *metric.Counter
	ConnMemMetrics              sql.BaseMemoryMetrics
	SQLMemMetrics               sql.MemoryMetrics
}

func makeTenantSpecificMetrics(
	sqlMemMetrics sql.MemoryMetrics, histogramWindow time.Duration,
) tenantSpecificMetrics {
	return tenantSpecificMetrics{
		BytesInCount:  metric.NewCounter(MetaBytesIn),
		BytesOutCount: metric.NewCounter(MetaBytesOut),
		Conns:         metric.NewGauge(MetaConns),
		NewConns:      metric.NewCounter(MetaNewConns),
		ConnLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePreferHdrLatency,
			Metadata: MetaConnLatency,
			Duration: histogramWindow,
			Buckets:  metric.IOLatencyBuckets,
		}),
		ConnFailures:                metric.NewCounter(MetaConnFailures),
		PGWireCancelTotalCount:      metric.NewCounter(MetaPGWireCancelTotal),
		PGWireCancelIgnoredCount:    metric.NewCounter(MetaPGWireCancelIgnored),
		PGWireCancelSuccessfulCount: metric.NewCounter(MetaPGWireCancelSuccessful),
		ConnMemMetrics:              sql.MakeBaseMemMetrics("conns", histogramWindow),
		SQLMemMetrics:               sqlMemMetrics,
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
	ctx := ambientCtx.AnnotateCtx(context.Background())
	server := &Server{
		AmbientCtx: ambientCtx,
		cfg:        cfg,
		execCfg:    executorConfig,

		tenantMetrics: makeTenantSpecificMetrics(sqlMemMetrics, histogramWindow),
	}
	server.sqlMemoryPool = mon.NewMonitor("sql",
		mon.MemoryResource,
		// Note that we don't report metrics on this monitor. The reason for this is
		// that we report metrics on the sum of all the child monitors of this pool.
		// This monitor is the "main sql" monitor. It's a child of the root memory
		// monitor. Its children are the sql monitors for each new connection. The
		// sum of those children, plus the extra memory in the "conn" monitor below,
		// is more than enough metrics information about the monitors.
		nil, /* curCount */
		nil, /* maxHist */
		0, noteworthySQLMemoryUsageBytes, st)
	server.sqlMemoryPool.StartNoReserved(ctx, parentMemoryMonitor)
	server.SQLServer = sql.NewServer(executorConfig, server.sqlMemoryPool)

	server.tenantSpecificConnMonitor = mon.NewMonitor("conn",
		mon.MemoryResource,
		server.tenantMetrics.ConnMemMetrics.CurBytesCount,
		server.tenantMetrics.ConnMemMetrics.MaxBytesHist,
		int64(connReservationBatchSize)*baseSQLMemoryBudget, noteworthyConnMemoryUsageBytes, st)
	server.tenantSpecificConnMonitor.StartNoReserved(ctx, server.sqlMemoryPool)

	server.mu.Lock()
	server.mu.connCancelMap = make(cancelChanMap)
	server.mu.Unlock()

	connAuthConf.SetOnChange(&st.SV, func(ctx context.Context) {
		loadLocalHBAConfigUponRemoteSettingChange(ctx, server, st)
	})
	ConnIdentityMapConf.SetOnChange(&st.SV, func(ctx context.Context) {
		loadLocalIdentityMapUponRemoteSettingChange(ctx, server, st)
	})

	return server
}

// BytesOut returns the total number of bytes transmitted from this server.
func (s *Server) BytesOut() uint64 {
	return uint64(s.tenantMetrics.BytesOutCount.Count())
}

// Match returns true if rd appears to be a Postgres connection.
func Match(rd io.Reader) bool {
	buf := pgwirebase.MakeReadBuffer()
	_, err := buf.ReadUntypedMsg(rd)
	if err != nil {
		return false
	}
	version, err := buf.GetUint32()
	if err != nil {
		return false
	}
	return version == version30 || version == versionSSL || version == versionCancel || version == versionGSSENC
}

// Start makes the Server ready for serving connections.
func (s *Server) Start(ctx context.Context, stopper *stop.Stopper) {
	s.SQLServer.Start(ctx, stopper)
}

// IsDraining returns true if the server is not currently accepting
// connections.
func (s *Server) IsDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.draining
}

// Metrics returns the set of metrics structs.
func (s *Server) Metrics() []interface{} {
	return []interface{}{
		&s.tenantMetrics,
		&s.SQLServer.Metrics.StartedStatementCounters,
		&s.SQLServer.Metrics.ExecutedStatementCounters,
		&s.SQLServer.Metrics.EngineMetrics,
		&s.SQLServer.Metrics.GuardrailMetrics,
		&s.SQLServer.InternalMetrics.StartedStatementCounters,
		&s.SQLServer.InternalMetrics.ExecutedStatementCounters,
		&s.SQLServer.InternalMetrics.EngineMetrics,
		&s.SQLServer.InternalMetrics.GuardrailMetrics,
		&s.SQLServer.ServerMetrics.StatsMetrics,
		&s.SQLServer.ServerMetrics.ContentionSubsystemMetrics,
		&s.SQLServer.ServerMetrics.InsightsMetrics,
	}
}

// Drain prevents new connections from being served and waits the duration of
// queryWait for open connections to terminate before canceling them.
// An error will be returned when connections that have been canceled have not
// responded to this cancellation and closed themselves in time. The server
// will remain in draining state, though open connections may continue to
// exist.
// The RFC on drain modes has more information regarding the specifics of
// what will happen to connections in different states:
// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160425_drain_modes.md
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (s *Server) Drain(
	ctx context.Context,
	queryWait time.Duration,
	reporter func(int, redact.SafeString),
	stopper *stop.Stopper,
) error {
	return s.drainImpl(ctx, queryWait, cancelMaxWait, reporter, stopper)
}

// Undrain switches the server back to the normal mode of operation in which
// connections are accepted.
func (s *Server) Undrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setRejectNewConnectionsLocked(false)
	s.setDrainingLocked(false)
}

// setDrainingLocked sets the server's draining state and returns whether the
// state changed (i.e. drain != s.mu.draining). s.mu must be locked when
// setDrainingLocked is called.
func (s *Server) setDrainingLocked(drain bool) bool {
	if s.mu.draining == drain {
		return false
	}
	s.mu.draining = drain
	return true
}

// setRejectNewConnectionsLocked sets the server's rejectNewConnections state.
// s.mu must be locked when setRejectNewConnectionsLocked is called.
func (s *Server) setRejectNewConnectionsLocked(rej bool) {
	s.mu.rejectNewConnections = rej
}

// GetConnCancelMapLen returns the length of connCancelMap of the server.
// This is a helper function when the server waits the SQL connections to be
// closed. During this period, the server listens to the status of all
// connections, and early exits this draining phase if there remains no active
// SQL connections.
func (s *Server) GetConnCancelMapLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mu.connCancelMap)
}

// WaitForSQLConnsToClose waits for the client to close all SQL connections for the
// duration of connectionWait.
// With this phase, the node starts rejecting SQL connections, and as
// soon as all existing SQL connections are closed, the server early exits this
// draining phase.
func (s *Server) WaitForSQLConnsToClose(
	ctx context.Context, connectionWait time.Duration, stopper *stop.Stopper,
) error {
	// If we're already draining the SQL connections, we don't need to wait again.
	if s.IsDraining() {
		return nil
	}

	s.mu.Lock()
	s.setRejectNewConnectionsLocked(true)
	s.mu.Unlock()

	if connectionWait == 0 {
		return nil
	}

	log.Ops.Info(ctx, "waiting for clients to close existing SQL connections")

	timer := time.NewTimer(connectionWait)
	defer timer.Stop()

	_, allConnsDone, quitWaitingForConns := s.waitConnsDone()
	defer close(quitWaitingForConns)

	select {
	// Connection wait times out.
	case <-time.After(connectionWait):
		log.Ops.Warningf(ctx,
			"%d connections remain after waiting %s; proceeding to drain SQL connections",
			s.GetConnCancelMapLen(),
			connectionWait,
		)
	case <-allConnsDone:
	case <-ctx.Done():
		return ctx.Err()
	case <-stopper.ShouldQuiesce():
		return context.Canceled
	}

	return nil
}

// waitConnsDone returns a copy of s.mu.connCancelMap, and a channel that
// will be closed once all sql connections are closed, or the server quits
// waiting for connections, whichever earlier.
func (s *Server) waitConnsDone() (cancelChanMap, chan struct{}, chan struct{}) {
	connCancelMap := func() cancelChanMap {
		s.mu.Lock()
		defer s.mu.Unlock()
		connCancelMap := make(cancelChanMap, len(s.mu.connCancelMap))
		for done, cancel := range s.mu.connCancelMap {
			connCancelMap[done] = cancel
		}
		return connCancelMap
	}()

	allConnsDone := make(chan struct{}, 1)

	quitWaitingForConns := make(chan struct{}, 1)

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

	return connCancelMap, allConnsDone, quitWaitingForConns
}

// drainImpl drains the SQL clients.
//
// The queryWait duration is used to wait on clients to
// self-disconnect after their session has been canceled. The
// cancelWait is used to wait after the queryWait timer has expired
// and there are still clients connected, and their context.Context is
// canceled.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (s *Server) drainImpl(
	ctx context.Context,
	queryWait time.Duration,
	cancelWait time.Duration,
	reporter func(int, redact.SafeString),
	stopper *stop.Stopper,
) error {

	s.mu.Lock()
	if !s.setDrainingLocked(true) {
		// We are already draining.
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	// If there is no open SQL connections to drain, just return.
	if s.GetConnCancelMapLen() == 0 {
		return nil
	}

	log.Ops.Info(ctx, "starting draining SQL connections")

	// Spin off a goroutine that waits for all connections to signal that they
	// are done and reports it on allConnsDone. The main goroutine signals this
	// goroutine to stop work through quitWaitingForConns.

	// This s.waitConnsDone function returns a copy of s.mu.connCancelMap if there
	// are any active connections to cancel. We will only attempt to cancel
	// connections that were active at the moment the draining switch happened.
	// It is enough to do this because:
	// 1) If no new connections are added to the original map all connections
	// will be canceled.
	// 2) If new connections are added to the original map, it follows that they
	// were added when s.mu.draining = false, thus not requiring cancellation.
	// These connections are not our responsibility and will be handled when the
	// server starts draining again.
	connCancelMap, allConnsDone, quitWaitingForConns := s.waitConnsDone()
	defer close(quitWaitingForConns)

	if reporter != nil {
		// Report progress to the Drain RPC.
		reporter(len(connCancelMap), "SQL clients")
	}

	// Wait for connections to finish up their queries for the duration of queryWait.
	select {
	case <-time.After(queryWait):
		log.Ops.Warningf(ctx, "canceling all sessions after waiting %s", queryWait)
	case <-allConnsDone:
	case <-ctx.Done():
		return ctx.Err()
	case <-stopper.ShouldQuiesce():
		return context.Canceled
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

// SocketType indicates the connection type. This is an optimization to
// prevent a comparison against conn.LocalAddr().Network().
type SocketType int

const (
	SocketUnknown SocketType = iota
	// SocketTCP is used for TCP sockets. The standard.
	SocketTCP
	// SocketUnix is used for unix datagram sockets.
	SocketUnix
	// SocketInternalLoopback is used for internal connections running over our
	// loopback listener.
	SocketInternalLoopback
)

func (s SocketType) asConnType() (hba.ConnType, error) {
	switch s {
	case SocketTCP:
		return hba.ConnHostNoSSL, nil
	case SocketUnix:
		return hba.ConnLocal, nil
	case SocketInternalLoopback:
		return hba.ConnInternalLoopback, nil
	default:
		return 0, errors.AssertionFailedf("unimplemented socket type: %v", errors.Safe(s))
	}
}

func (s *Server) connLogEnabled() bool {
	return atomic.LoadInt32(&s.testingConnLogEnabled) != 0 || logConnAuth.Get(&s.execCfg.Settings.SV)
}

// TestingEnableConnLogging is exported for use in tests.
func (s *Server) TestingEnableConnLogging() {
	atomic.StoreInt32(&s.testingConnLogEnabled, 1)
}

// TestingEnableAuthLogging is exported for use in tests.
func (s *Server) TestingEnableAuthLogging() {
	atomic.StoreInt32(&s.testingAuthLogEnabled, 1)
}

// ServeConn serves a single connection, driving the handshake process and
// delegating to the appropriate connection type.
//
// The socketType argument is an optimization to avoid a string
// compare on conn.LocalAddr().Network(). When the socket type is
// unix datagram (local filesystem), SSL negotiation is disabled
// even when the server is running securely with certificates.
// This has the effect of forcing password auth, also in a way
// compatible with postgres.
//
// An error is returned if the initial handshake of the connection fails.
func (s *Server) ServeConn(
	ctx context.Context, conn net.Conn, preServeStatus PreServeStatus,
) (err error) {
	if preServeStatus.State != PreServeReady {
		return errors.AssertionFailedf("programming error: cannot call ServeConn with state %v (did you mean HandleCancel?)", preServeStatus.State)
	}

	defer func() {
		if err != nil {
			s.tenantMetrics.ConnFailures.Inc(1)
		}
	}()

	ctx, rejectNewConnections, onCloseFn := s.registerConn(ctx)
	defer onCloseFn()

	connDetails := eventpb.CommonConnectionDetails{
		InstanceID:    int32(s.execCfg.NodeInfo.NodeID.SQLInstanceID()),
		Network:       conn.RemoteAddr().Network(),
		RemoteAddress: conn.RemoteAddr().String(),
	}

	// Some bookkeeping, for security-minded administrators.
	// This registers the connection to the authentication log.
	connStart := timeutil.Now()
	if s.connLogEnabled() {
		ev := &eventpb.ClientConnectionStart{
			CommonEventDetails:      logpb.CommonEventDetails{Timestamp: connStart.UnixNano()},
			CommonConnectionDetails: connDetails,
		}
		log.StructuredEvent(ctx, ev)
	}
	defer func() {
		// The duration of the session is logged at the end so that the
		// reader of the log file can know how much to look back in time
		// to find when the connection was opened. This is important
		// because the log files may have been rotated since.
		if s.connLogEnabled() {
			endTime := timeutil.Now()
			ev := &eventpb.ClientConnectionEnd{
				CommonEventDetails:      logpb.CommonEventDetails{Timestamp: endTime.UnixNano()},
				CommonConnectionDetails: connDetails,
				Duration:                endTime.Sub(connStart).Nanoseconds(),
			}
			log.StructuredEvent(ctx, ev)
		}
	}()

	st := s.execCfg.Settings
	// If the server is shutting down, terminate the connection early.
	if rejectNewConnections {
		log.Ops.Info(ctx, "rejecting new connection while server is draining")
		return s.sendErr(ctx, st, conn, newAdminShutdownErr(ErrDrainingNewConn))
	}

	sArgs, err := finalizeClientParameters(ctx, preServeStatus.clientParameters, &st.SV)
	if err != nil {
		preServeStatus.Reserved.Close(ctx)
		return s.sendErr(ctx, st, conn, err)
	}

	// Transfer the memory account into this tenant.
	tenantReserved, err := s.tenantSpecificConnMonitor.TransferAccount(ctx, &preServeStatus.Reserved)
	if err != nil {
		preServeStatus.Reserved.Close(ctx)
		return s.sendErr(ctx, st, conn, err)
	}

	// Populate the client address field in the context tags and the
	// shared struct for structured logging.
	// Only now do we know the remote client address for sure (it may have
	// been overridden by a status parameter).
	connDetails.RemoteAddress = sArgs.RemoteAddr.String()
	sp := tracing.SpanFromContext(ctx)
	ctx = logtags.AddTag(ctx, "client", log.SafeOperational(connDetails.RemoteAddress))
	sp.SetTag("conn_type", attribute.StringValue(preServeStatus.ConnType.String()))
	sp.SetTag("client", attribute.StringValue(connDetails.RemoteAddress))

	// If a test is hooking in some authentication option, load it.
	var testingAuthHook func(context.Context) error
	if k := s.execCfg.PGWireTestingKnobs; k != nil {
		testingAuthHook = k.AuthHook
	}

	hbaConf, identMap := s.GetAuthenticationConfiguration()

	// Defer the rest of the processing to the connection handler.
	// This includes authentication.
	s.serveConn(
		ctx, conn, sArgs,
		&tenantReserved,
		connStart,
		authOptions{
			connType:        preServeStatus.ConnType,
			connDetails:     connDetails,
			insecure:        s.cfg.Insecure,
			auth:            hbaConf,
			identMap:        identMap,
			testingAuthHook: testingAuthHook,
		},
	)
	return nil
}

// readCancelKeyAndCloseConn retrieves the "backend data" key that identifies
// a cancellable query, then closes the connection.
func readCancelKeyAndCloseConn(
	ctx context.Context, conn net.Conn, buf *pgwirebase.ReadBuffer,
) (ok bool, cancelKey pgwirecancel.BackendKeyData) {
	telemetry.Inc(sqltelemetry.CancelRequestCounter)
	backendKeyDataBits, err := buf.GetUint64()
	// The connection that issued the cancel is not a SQL session -- it's an
	// entirely new connection that's created just to send the cancel. We close
	// the connection as soon as possible after reading the data, since there
	// is nothing to send back to the client.
	_ = conn.Close()
	// The client is also unwilling to read an error payload, so we just log it locally.
	if err != nil {
		log.Sessions.Warningf(ctx, "%v", errors.Wrap(err, "reading cancel key from client"))
		return false, 0
	}
	return true, pgwirecancel.BackendKeyData(backendKeyDataBits)
}

// HandleCancel handles a pgwire query cancellation request. Note that the
// request is unauthenticated. To mitigate the security risk (i.e., a
// malicious actor spamming this endpoint with random data to try to cancel
// a query), the logic is rate-limited by a semaphore. Refer to the comments
// around the following:
//
// - (*server/statusServer).cancelSemaphore
// - (*server/statusServer).CancelRequestByKey
// - (*server/serverController).sqlMux
func (s *Server) HandleCancel(ctx context.Context, cancelKey pgwirecancel.BackendKeyData) {
	s.tenantMetrics.PGWireCancelTotalCount.Inc(1)

	resp, err := func() (*serverpb.CancelQueryByKeyResponse, error) {
		// The request is forwarded to the appropriate node.
		req := &serverpb.CancelQueryByKeyRequest{
			SQLInstanceID:  cancelKey.GetSQLInstanceID(),
			CancelQueryKey: cancelKey,
		}
		resp, err := s.execCfg.SQLStatusServer.CancelQueryByKey(ctx, req)
		if resp != nil && len(resp.Error) > 0 {
			err = errors.Newf("error from CancelQueryByKeyResponse: %s", resp.Error)
		}
		return resp, err
	}()

	if resp != nil && resp.Canceled {
		s.tenantMetrics.PGWireCancelSuccessfulCount.Inc(1)
	} else if err != nil {
		if respStatus := status.Convert(err); respStatus.Code() == codes.ResourceExhausted {
			s.tenantMetrics.PGWireCancelIgnoredCount.Inc(1)
			log.Sessions.Warningf(ctx, "unexpected while handling pgwire cancellation request: %v", err)
		}
	}
}

// finalizeClientParameters "fills in" the session arguments with
// any tenant-specific defaults.
func finalizeClientParameters(
	ctx context.Context, cp tenantIndependentClientParameters, tenantSV *settings.Values,
) (sql.SessionArgs, error) {
	// Inject the result buffer size if not defined by client.
	if !cp.foundBufferSize && tenantSV != nil {
		cp.ConnResultsBufferSize = connResultsBufferSize.Get(tenantSV)
	}

	return cp.SessionArgs, nil
}

// registerConn registers the incoming connection to the map of active connections,
// which can be canceled by a concurrent server drain. It also returns a boolean
// variable rejectConn, which shows if the server is rejecting new SQL
// connections.
//
// The onCloseFn() callback must be called at the end of the
// connection by the caller.
func (s *Server) registerConn(
	ctx context.Context,
) (newCtx context.Context, rejectNewConnections bool, onCloseFn func()) {
	onCloseFn = func() {}
	newCtx = ctx
	s.mu.Lock()
	rejectNewConnections = s.mu.rejectNewConnections
	if !rejectNewConnections {
		var cancel context.CancelFunc
		newCtx, cancel = contextutil.WithCancel(ctx)
		done := make(chan struct{})
		s.mu.connCancelMap[done] = cancel
		onCloseFn = func() {
			cancel()
			close(done)
			s.mu.Lock()
			delete(s.mu.connCancelMap, done)
			s.mu.Unlock()
		}
	}
	s.mu.Unlock()

	// If the server is rejecting new SQL connections, we will use the connection
	// only to send an error, so we don't count it in the stats. This makes sense
	// since DrainClient() waits for that number to drop to zero,
	// so we don't want it to oscillate unnecessarily.
	if !rejectNewConnections {
		s.tenantMetrics.NewConns.Inc(1)
		s.tenantMetrics.Conns.Inc(1)
		prevOnCloseFn := onCloseFn
		onCloseFn = func() { prevOnCloseFn(); s.tenantMetrics.Conns.Dec(1) }
	}
	return
}

// sendErr sends errors to the client during the connection startup
// sequence. Later error sends during/after authentication are handled
// in conn.go.
func (s *Server) sendErr(
	ctx context.Context, st *cluster.Settings, conn net.Conn, err error,
) error {
	w := errWriter{
		sv:         &st.SV,
		msgBuilder: newWriteBuffer(s.tenantMetrics.BytesOutCount),
	}
	// We could, but do not, report server-side network errors while
	// trying to send the client error. This is because clients that
	// receive error payload are highly correlated with clients
	// disconnecting abruptly.
	_ /* err */ = w.writeErr(ctx, err, conn)
	_ = conn.Close()
	return err
}

func newAdminShutdownErr(msg string) error {
	return pgerror.New(pgcode.AdminShutdown, msg)
}
