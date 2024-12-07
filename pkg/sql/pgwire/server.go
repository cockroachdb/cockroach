// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/ctxlog"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	io_prometheus_client "github.com/prometheus/client_model/go"
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
	settings.ApplicationLevel,
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
	512<<10, // 512 KiB
	settings.WithPublic)

var logConnAuth = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	sql.ConnAuditingClusterSettingName,
	"if set, log SQL client connect and disconnect events to the SESSIONS log channel "+
		"(note: may hinder performance on loaded nodes)",
	false,
	settings.WithPublic)

var logVerboseSessionAuth = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	sql.AuthAuditingClusterSettingName,
	"if set, log verbose SQL session authentication events to the SESSIONS log channel "+
		"(note: may hinder performance on loaded nodes). "+
		"Session start and end events are always logged regardless of this setting; "+
		"disable the SESSIONS log channel to suppress them.",
	false,
	settings.WithPublic)

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
		Help:        "Number of SQL connections created",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaConnsWaitingToHash = metric.Metadata{
		Name:        "sql.conns_waiting_to_hash",
		Help:        "Number of SQL connection attempts that are being throttled in order to limit password hashing concurrency",
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
		Help:        "Number of pgwire query cancel requests",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	MetaPGWireCancelIgnored = metric.Metadata{
		Name:        "sql.pgwire_cancel.ignored",
		Help:        "Number of pgwire query cancel requests that were ignored due to rate limiting",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	MetaPGWireCancelSuccessful = metric.Metadata{
		Name:        "sql.pgwire_cancel.successful",
		Help:        "Number of pgwire query cancel requests that were successful",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
	MetaPGWirePipelineCount = metric.Metadata{
		Name:        "sql.pgwire.pipeline.count",
		Help:        "Number of pgwire commands received by the server that have not yet begun processing",
		Measurement: "Commands",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	AuthJWTConnLatency = metric.Metadata{
		Name:        "auth.jwt.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection using JWT Token",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	AuthCertConnLatency = metric.Metadata{
		Name:        "auth.cert.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection using certificate",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	AuthPassConnLatency = metric.Metadata{
		Name:        "auth.password.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection using password",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	AuthLDAPConnLatency = metric.Metadata{
		Name:        "auth.ldap.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection using LDAP",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	AuthGSSConnLatency = metric.Metadata{
		Name:        "auth.gss.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection using GSS",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	AuthScramConnLatency = metric.Metadata{
		Name:        "auth.scram.conn.latency",
		Help:        "Latency to establish and authenticate a SQL connection using SCRAM",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
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

	tenantMetrics *tenantSpecificMetrics

	destinationMetrics destinationAggMetrics

	mu struct {
		syncutil.Mutex
		// connCancelMap entries represent connections started when the server
		// was not draining. Each value is a function that can be called to
		// cancel the associated connection. The corresponding key is a channel
		// that is closed when the connection is done.
		connCancelMap cancelChanMap
		// draining is set to true when the server starts draining the SQL layer.
		// When set to true, remaining SQL connections will be closed.
		// After the timeout set by server.shutdown.transactions.timeout,
		// all connections will be closed regardless any txns in flight.
		draining bool
		// drainCh is closed when draining is set to true. If Undrain is called,
		// this channel is reset.
		drainCh chan struct{}
		// rejectNewConnections is set true when the server does not accept new
		// SQL connections, e.g. when the draining process enters the phase whose
		// duration is specified by the server.shutdown.connections.timeout.
		rejectNewConnections bool

		// destinations tracks the metrics for each destination.
		destinations map[string]*destinationMetrics
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
	testingConnLogEnabled atomic.Bool
	testingAuthLogEnabled atomic.Bool
}

// destMetrics returns the destination metrics for the given connection given
// the current cidr mapping. For performance we cache a metrics object on the
// connection. but we need to check if it is still valid. If its invalid, we
// call lookup and update the cache.
func (s *Server) destMetrics(ctx context.Context, c *conn) *destinationMetrics {
	dm := c.curDestMetrics.Load()
	if dm != nil && !dm.invalid.Load() {
		return dm
	}
	dm = s.lookup(ctx, c.conn)
	c.curDestMetrics.Store(dm)
	return dm
}

type destinationAggMetrics struct {
	BytesInCount  *aggmetric.AggCounter
	BytesOutCount *aggmetric.AggCounter
}

// destinationMetrics is the set of metrics to a specific destination. A
// destination can be defined based on a CIDR block. The invalid flag is used to
// protect against CIDR mapping changes. When the CIDR mapping changes, the
// destination metrics are invalidated and a new struct is created.
type destinationMetrics struct {
	// NB: The transition from valid to invalid is one way.
	invalid atomic.Bool
	// The counters should never be accessed directly, only through the
	// functions.
	BytesInCount  *aggmetric.Counter
	BytesOutCount *aggmetric.Counter
}

// tenantSpecificMetrics is the set of metrics for a pgwire server
// bound to a specific tenant.
type tenantSpecificMetrics struct {
	Conns                       *metric.Gauge
	NewConns                    *metric.Counter
	ConnsWaitingToHash          *metric.Gauge
	PGWirePipelineCount         *metric.Gauge
	ConnLatency                 metric.IHistogram
	ConnFailures                *metric.Counter
	PGWireCancelTotalCount      *metric.Counter
	PGWireCancelIgnoredCount    *metric.Counter
	PGWireCancelSuccessfulCount *metric.Counter
	ConnMemMetrics              sql.BaseMemoryMetrics
	SQLMemMetrics               sql.MemoryMetrics
	AuthJWTConnLatency          metric.IHistogram
	AuthCertConnLatency         metric.IHistogram
	AuthPassConnLatency         metric.IHistogram
	AuthLDAPConnLatency         metric.IHistogram
	AuthGSSConnLatency          metric.IHistogram
	AuthScramConnLatency        metric.IHistogram
}

func newTenantSpecificMetrics(
	sqlMemMetrics sql.MemoryMetrics, histogramWindow time.Duration,
) *tenantSpecificMetrics {
	return &tenantSpecificMetrics{
		Conns:               metric.NewGauge(MetaConns),
		NewConns:            metric.NewCounter(MetaNewConns),
		ConnsWaitingToHash:  metric.NewGauge(MetaConnsWaitingToHash),
		PGWirePipelineCount: metric.NewGauge(MetaPGWirePipelineCount),
		ConnLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     MetaConnLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		ConnFailures:                metric.NewCounter(MetaConnFailures),
		PGWireCancelTotalCount:      metric.NewCounter(MetaPGWireCancelTotal),
		PGWireCancelIgnoredCount:    metric.NewCounter(MetaPGWireCancelIgnored),
		PGWireCancelSuccessfulCount: metric.NewCounter(MetaPGWireCancelSuccessful),
		ConnMemMetrics:              sql.MakeBaseMemMetrics("conns", histogramWindow),
		SQLMemMetrics:               sqlMemMetrics,
		AuthJWTConnLatency: metric.NewHistogram(
			getHistogramOptionsForIOLatency(AuthJWTConnLatency, histogramWindow)),
		AuthCertConnLatency: metric.NewHistogram(
			getHistogramOptionsForIOLatency(AuthCertConnLatency, histogramWindow)),
		AuthPassConnLatency: metric.NewHistogram(
			getHistogramOptionsForIOLatency(AuthPassConnLatency, histogramWindow)),
		AuthLDAPConnLatency: metric.NewHistogram(
			getHistogramOptionsForIOLatency(AuthLDAPConnLatency, histogramWindow)),
		AuthGSSConnLatency: metric.NewHistogram(
			getHistogramOptionsForIOLatency(AuthGSSConnLatency, histogramWindow)),
		AuthScramConnLatency: metric.NewHistogram(
			getHistogramOptionsForIOLatency(AuthScramConnLatency, histogramWindow)),
	}
}

func getHistogramOptionsForIOLatency(
	metadata metric.Metadata, histogramWindow time.Duration,
) metric.HistogramOptions {
	return metric.HistogramOptions{
		Mode:         metric.HistogramModePreferHdrLatency,
		Metadata:     metadata,
		Duration:     histogramWindow,
		BucketConfig: metric.IOLatencyBuckets,
	}
}

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

		tenantMetrics: newTenantSpecificMetrics(sqlMemMetrics, histogramWindow),
		destinationMetrics: destinationAggMetrics{
			BytesInCount:  aggmetric.NewCounter(MetaBytesIn, "remote"),
			BytesOutCount: aggmetric.NewCounter(MetaBytesOut, "remote"),
		},
	}
	server.sqlMemoryPool = mon.NewMonitor(mon.Options{
		Name: mon.MakeMonitorName("sql"),
		// Note that we don't report metrics on this monitor. The reason for this is
		// that we report metrics on the sum of all the child monitors of this pool.
		// This monitor is the "main sql" monitor. It's a child of the root memory
		// monitor. Its children are the sql monitors for each new connection. The
		// sum of those children, plus the extra memory in the "conn" monitor below,
		// is more than enough metrics information about the monitors.
		CurCount:   nil,
		MaxHist:    nil,
		Settings:   st,
		LongLiving: true,
	})
	server.sqlMemoryPool.StartNoReserved(ctx, parentMemoryMonitor)
	server.SQLServer = sql.NewServer(executorConfig, server.sqlMemoryPool)

	server.tenantSpecificConnMonitor = mon.NewMonitor(mon.Options{
		Name:       mon.MakeMonitorName("conn"),
		CurCount:   server.tenantMetrics.ConnMemMetrics.CurBytesCount,
		MaxHist:    server.tenantMetrics.ConnMemMetrics.MaxBytesHist,
		Increment:  int64(connReservationBatchSize) * baseSQLMemoryBudget,
		Settings:   st,
		LongLiving: true,
	})
	server.tenantSpecificConnMonitor.StartNoReserved(ctx, server.sqlMemoryPool)

	server.mu.Lock()
	server.mu.connCancelMap = make(cancelChanMap)
	server.mu.drainCh = make(chan struct{})
	server.mu.destinations = make(map[string]*destinationMetrics)
	server.mu.Unlock()
	executorConfig.CidrLookup.SetOnChange(server.onCidrChange)

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
	return uint64(s.destinationMetrics.BytesOutCount.Count())
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
		s.tenantMetrics,
		s.destinationMetrics,
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

// DrainCh returns a channel that can be watched to detect when the server
// enters the draining state.
func (s *Server) DrainCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.drainCh
}

// setDrainingLocked sets the server's draining state and returns whether the
// state changed (i.e. drain != s.mu.draining). s.mu must be locked when
// setDrainingLocked is called.
func (s *Server) setDrainingLocked(drain bool) bool {
	if s.mu.draining == drain {
		return false
	}
	s.mu.draining = drain
	if drain {
		close(s.mu.drainCh)
	} else {
		s.mu.drainCh = make(chan struct{})
	}
	return true
}

// setDraining sets the server's draining state and returns whether the
// state changed (i.e. drain != s.mu.draining).
func (s *Server) setDraining(drain bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setDrainingLocked(drain)
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

	connectionTimeoutEvent := &eventpb.NodeShutdownConnectionTimeout{
		CommonNodeEventDetails: eventpb.CommonNodeEventDetails{
			NodeID: int32(s.execCfg.NodeInfo.NodeID.SQLInstanceID()),
		},
		Detail:        redact.SafeString("draining SQL queries after waiting for server.shutdown.connections.timeout"),
		TimeoutMillis: uint32(connectionWait.Milliseconds()),
	}

	if connectionWait == 0 {
		numOpenConns := s.GetConnCancelMapLen()
		if numOpenConns > 0 {
			connectionTimeoutEvent.ConnectionsRemaining = uint32(numOpenConns)
			log.StructuredEvent(ctx, severity.WARNING, connectionTimeoutEvent)
		}
		return nil
	}

	log.Ops.Infof(ctx, "waiting for clients to close %d existing SQL connections", s.GetConnCancelMapLen())

	timer := time.NewTimer(connectionWait)
	defer timer.Stop()

	_, allConnsDone, quitWaitingForConns := s.waitConnsDone()
	defer close(quitWaitingForConns)

	select {
	// Connection wait times out.
	case <-time.After(connectionWait):
		connectionTimeoutEvent.ConnectionsRemaining = uint32(s.GetConnCancelMapLen())
		log.StructuredEvent(ctx, severity.WARNING, connectionTimeoutEvent)
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

	if !s.setDraining(true) {
		// We are already draining.
		return nil
	}

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
		transactionTimeoutEvent := &eventpb.NodeShutdownTransactionTimeout{
			CommonNodeEventDetails: eventpb.CommonNodeEventDetails{
				NodeID: int32(s.execCfg.NodeInfo.NodeID.SQLInstanceID()),
			},
			Detail:               redact.SafeString("forcibly closing SQL connections after waiting for server.shutdown.transactions.timeout"),
			ConnectionsRemaining: uint32(s.GetConnCancelMapLen()),
			TimeoutMillis:        uint32(queryWait.Milliseconds()),
		}
		log.StructuredEvent(ctx, severity.WARNING, transactionTimeoutEvent)
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
	return s.testingConnLogEnabled.Load() || logConnAuth.Get(&s.execCfg.Settings.SV)
}

// TestingEnableConnLogging is exported for use in tests.
func (s *Server) TestingEnableConnLogging() {
	s.testingConnLogEnabled.Store(true)
}

// TestingEnableAuthLogging is exported for use in tests.
func (s *Server) TestingEnableAuthLogging() {
	s.testingAuthLogEnabled.Store(true)
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

	sessionID := s.execCfg.GenerateID()
	connDetails := eventpb.CommonConnectionDetails{
		InstanceID:    int32(s.execCfg.NodeInfo.NodeID.SQLInstanceID()),
		Network:       conn.RemoteAddr().Network(),
		RemoteAddress: conn.RemoteAddr().String(),
		SessionID:     sessionID.String(),
	}

	// Some bookkeeping, for security-minded administrators.
	// This registers the connection to the authentication log.
	connStart := timeutil.Now()
	if s.connLogEnabled() {
		ev := &eventpb.ClientConnectionStart{
			CommonEventDetails:      logpb.CommonEventDetails{Timestamp: connStart.UnixNano()},
			CommonConnectionDetails: connDetails,
		}
		log.StructuredEvent(ctx, severity.INFO, ev)
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
			log.StructuredEvent(ctx, severity.INFO, ev)
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
		return s.sendErr(ctx, st, conn, err)
	}

	// Transfer the memory account into this tenant.
	tenantReserved, err := s.tenantSpecificConnMonitor.TransferAccount(ctx, preServeStatus.Reserved)
	if err != nil {
		return s.sendErr(ctx, st, conn, err)
	}

	// Populate the client address field in the context tags and the
	// shared struct for structured logging.
	// Only now do we know the remote client address for sure (it may have
	// been overridden by a status parameter).
	connDetails.RemoteAddress = sArgs.RemoteAddr.String()
	sp := tracing.SpanFromContext(ctx)
	ctx = logtags.AddTag(ctx, "client", log.SafeOperational(connDetails.RemoteAddress))
	ctx = logtags.AddTag(ctx, preServeStatus.ConnType.String(), nil)
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
	if log.V(2) {
		log.Infof(ctx, "new connection with options: %+v", sArgs)
	}

	ctx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	c := s.newConn(
		ctx,
		cancelConn,
		conn,
		sArgs,
		connStart,
	)

	// Do the reading of commands from the network.
	s.serveImpl(
		ctx,
		c,
		&tenantReserved,
		authOptions{
			connType:        preServeStatus.ConnType,
			connDetails:     connDetails,
			insecure:        s.cfg.Insecure,
			auth:            hbaConf,
			identMap:        identMap,
			testingAuthHook: testingAuthHook,
		},
		sessionID,
	)
	return nil
}

// onCidrChange is called when the cluster setting for the CIDR lookup table
// changes. We invalidate all existing metric mappings as it is possible that
// they changed. On each connection, the next call to lookup will recompute the
// current mapping for that connection.
func (s *Server) onCidrChange(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dest := range s.mu.destinations {
		dest.invalid.Store(true)
	}
}

// lookup returns the destination metrics for a given connection.
func (s *Server) lookup(ctx context.Context, netConn net.Conn) *destinationMetrics {
	ip := net.IPv4zero
	if addr, ok := netConn.RemoteAddr().(*net.TCPAddr); ok {
		ip = addr.IP
	}
	destination := s.execCfg.CidrLookup.LookupIP(ip)
	s.mu.Lock()
	defer s.mu.Unlock()
	if ret, ok := s.mu.destinations[destination]; ok {
		// If there is an existing invalid entry, create a new valid entry. All
		// the existing connections will still reference the old entry, but will
		// get updated when they next attempt to use it. Note that we can't call
		// AddChild again for the same destination name.
		if ret.invalid.Load() {
			ret = &destinationMetrics{
				BytesInCount:  ret.BytesInCount,
				BytesOutCount: ret.BytesOutCount,
			}
		}
		return ret
	}
	ret := &destinationMetrics{
		BytesInCount:  s.destinationMetrics.BytesInCount.AddChild(destination),
		BytesOutCount: s.destinationMetrics.BytesOutCount.AddChild(destination),
	}
	s.mu.destinations[destination] = ret
	return ret
}

func (s *Server) newConn(
	ctx context.Context,
	cancelConn context.CancelFunc,
	netConn net.Conn,
	sArgs sql.SessionArgs,
	connStart time.Time,
) *conn {
	sv := &s.execCfg.Settings.SV
	c := &conn{
		conn:                  netConn,
		cancelConn:            cancelConn,
		sessionArgs:           sArgs,
		metrics:               s.tenantMetrics,
		startTime:             connStart,
		rd:                    *bufio.NewReader(netConn),
		sv:                    sv,
		readBuf:               pgwirebase.MakeReadBuffer(pgwirebase.ReadBufferOptionWithClusterSettings(sv)),
		alwaysLogAuthActivity: s.testingAuthLogEnabled.Load(),
	}
	c.destMetrics = func() *destinationMetrics { return s.destMetrics(ctx, c) }
	c.stmtBuf.Init()
	c.stmtBuf.PipelineCount = s.tenantMetrics.PGWirePipelineCount
	c.res.released = true
	c.writerState.fi.buf = &c.writerState.buf
	c.writerState.fi.lastFlushed = -1
	c.msgBuilder.init(func(i int64) { c.destMetrics().BytesOutCount.Inc(i) })
	c.errWriter.sv = sv
	c.errWriter.msgBuilder = &c.msgBuilder
	return c
}

// maxRepeatedErrorCount is the number of times an error can be received
// while reading from the network connection before the server decides to give
// up and abort the connection.
const maxRepeatedErrorCount = 1 << 15

// serveImpl continuously reads from the network connection and pushes execution
// instructions into a sql.StmtBuf, from where they'll be processed by a command
// "processor" goroutine (a connExecutor).
// The method returns when the pgwire termination message is received, when
// network communication fails, when the server is draining or when ctx is
// canceled (which also happens when draining (but not from the get-go), and
// when the processor encounters a fatal error).
//
// serveImpl closes the stmtBuf before returning, which is a signal to the
// processor goroutine to exit.
//
// sqlServer is used to create the command processor. As a special facility for
// tests, sqlServer can be nil, in which case the command processor and the
// write-side of the connection will not be created.
//
// Internally, a connExecutor will be created to execute commands. Commands read
// from the network are buffered in a stmtBuf which is consumed by the
// connExecutor. The connExecutor produces results which are buffered and
// sometimes synchronously flushed to the network.
//
// The reader goroutine (this one) outlives the connExecutor's goroutine (the
// "processor goroutine").
// However, they can both signal each other to stop. Here's how the different
// cases work:
// 1) The reader receives a ClientMsgTerminate protocol packet: the reader
// closes the stmtBuf and also cancels the command processing context. These
// actions will prompt the command processor to finish.
// 2) The reader gets a read error from the network connection: like above, the
// reader closes the command processor.
// 3) The reader's context is canceled (happens when the server is draining but
// the connection was busy and hasn't quit yet): the reader notices the canceled
// context and, like above, closes the processor.
// 4) The processor encounters an error. This error can come from various fatal
// conditions encountered internally by the processor, or from a network
// communication error encountered while flushing results to the network.
// The processor will cancel the reader's context and terminate.
// Note that query processing errors are different; they don't cause the
// termination of the connection.
//
// Draining notes:
//
// The reader notices that the server is draining by watching the channel
// returned by Server.DrainCh. At that point, the reader delegates the
// responsibility of closing the connection to the statement processor: it will
// push a DrainRequest to the stmtBuf which signals the processor to quit ASAP.
// The processor will quit immediately upon seeing that command if it's not
// currently in a transaction. If it is in a transaction, it will wait until the
// first time a Sync command is processed outside of a transaction - the logic
// being that we want to stop when we're both outside transactions and outside
// batches. If the processor does not process the DrainRequest quickly enough
// (based on the server.shutdown.transactions.timeout setting), the server
// will cancel the context, which will close the connection and make the
// reader goroutine exit.
func (s *Server) serveImpl(
	ctx context.Context,
	c *conn,
	reserved *mon.BoundAccount,
	authOpt authOptions,
	sessionID clusterunique.ID,
) {
	if c.sessionArgs.User.IsRootUser() || c.sessionArgs.User.IsNodeUser() {
		ctx = logtags.AddTag(ctx, "user", redact.Safe(c.sessionArgs.User))
	} else {
		ctx = logtags.AddTag(ctx, "user", c.sessionArgs.User)
	}
	tracing.SpanFromContext(ctx).SetTag("user", attribute.StringValue(c.sessionArgs.User.Normalized()))

	sqlServer := s.SQLServer
	inTestWithoutSQL := sqlServer == nil

	go func() {
		select {
		case <-ctx.Done():
			// If the context was canceled, it's time to stop reading. Either a
			// higher-level server or the command processor have canceled us.
		case <-s.DrainCh():
			// If the server is draining, we'll let the processor know by pushing a
			// DrainRequest. This will make the processor quit whenever it finds a
			// good time (i.e., outside of a transaction). The context will be
			// cancelled by the server after all processors have quit or after the
			// server.shutdown.transactions.timeout duration.
			_ /* err */ = c.stmtBuf.Push(ctx, sql.DrainRequest{})
			<-ctx.Done()
		}
		// If possible, we try to only close the read side of the connection. This will cause the
		// ReadTypedMsg call in the reader goroutine to return an error, which will
		// cause the reader to exit and signal the processor to quit also, and still
		// be able to write an error message to the client. If we're unable to only
		// close the read side, we fallback to setting a read deadline that will
		// make all reads timeout.
		var tcpConn *net.TCPConn
		switch c := c.conn.(type) {
		case *net.TCPConn:
			tcpConn = c
		case *tls.Conn:
			underConn := c.NetConn()
			tcpConn, _ = underConn.(*net.TCPConn)
		}
		if tcpConn == nil {
			_ = c.conn.SetReadDeadline(timeutil.Now())
		} else {
			_ = tcpConn.CloseRead()
		}
	}()

	// NOTE: We're going to write a few messages to the connection in this method,
	// for the handshake. After that, all writes are done async, in the
	// startWriter() goroutine.

	// We'll build an authPipe to communicate with the authentication process.
	systemIdentity := c.sessionArgs.SystemIdentity
	if systemIdentity == "" {
		systemIdentity = c.sessionArgs.User.Normalized()
	}
	logVerboseAuthn := !inTestWithoutSQL && c.verboseAuthLogEnabled()
	authPipe := newAuthPipe(c, logVerboseAuthn, authOpt, systemIdentity)

	if !inTestWithoutSQL {
		defer func() {
			endTime := timeutil.Now()
			authPipe.LogSessionEnd(ctx, endTime)
		}()
	}

	// procWg waits for the command processor to return.
	var procWg sync.WaitGroup

	// We need a value for the unqualified int size here, but it is controlled
	// by a session variable, and this layer doesn't have access to the session
	// data. The callback below is called whenever default_int_size changes.
	// It happens in a different goroutine, so it has to be changed atomically.
	var atomicUnqualifiedIntSize = new(int32)
	onDefaultIntSizeChange := func(newSize int32) {
		atomic.StoreInt32(atomicUnqualifiedIntSize, newSize)
	}

	if !inTestWithoutSQL {
		// Spawn the command processing goroutine, which also handles connection
		// authentication). It will notify us when it's done through procWg, and
		// we'll also interact with the authentication process through authPipe.
		procWg.Add(1)
		go func() {
			// Inform the connection goroutine.
			defer procWg.Done()
			c.processCommands(
				ctx,
				authOpt,
				authPipe,
				sqlServer,
				reserved,
				onDefaultIntSizeChange,
				sessionID,
			)
		}()
	} else {
		// sqlServer == nil means we are in a local test. In this case
		// we only need the minimum to make pgx happy.
		defer reserved.Clear(ctx)
		var err error
		for param, value := range testingStatusReportParams {
			err = c.bufferParamStatus(param, value)
			if err != nil {
				break
			}
		}
		if err != nil {
			return
		}
		// Simulate auth succeeding.
		authPipe.AuthOK(ctx)

		if err := c.bufferInitialReadyForQuery(0 /* queryCancelKey */); err != nil {
			return
		}
		// We don't have a CmdPos to pass in, since we haven't received any commands
		// yet, so we just use the initial lastFlushed value.
		if err := c.Flush(c.writerState.fi.lastFlushed); err != nil {
			return
		}
	}

	var terminateSeen bool
	var authDone, ignoreUntilSync bool
	var repeatedErrorCount int
	for {
		breakLoop, isSimpleQuery, err := func() (bool, bool, error) {
			typ, n, err := c.readBuf.ReadTypedMsg(&c.rd)
			s.destMetrics(ctx, c).BytesInCount.Inc(int64(n))
			if err == nil {
				if knobs := s.execCfg.PGWireTestingKnobs; knobs != nil {
					if afterReadMsgTestingKnob := knobs.AfterReadMsgTestingKnob; afterReadMsgTestingKnob != nil {
						err = afterReadMsgTestingKnob(ctx)
					}
				}
			}
			isSimpleQuery := typ == pgwirebase.ClientMsgSimpleQuery
			if err != nil {
				if pgwirebase.IsMessageTooBigError(err) {
					log.VInfof(ctx, 1, "pgwire: found big error message; attempting to slurp bytes and return error: %s", err)

					// Slurp the remaining bytes.
					slurpN, slurpErr := c.readBuf.SlurpBytes(&c.rd, pgwirebase.GetMessageTooBigSize(err))
					s.destMetrics(ctx, c).BytesInCount.Inc(int64(slurpN))
					if slurpErr != nil {
						return false, isSimpleQuery, errors.Wrap(slurpErr, "pgwire: error slurping remaining bytes")
					}
				}

				// Notify connExecutor of the error so it can send it to the client.
				if err := c.stmtBuf.Push(ctx, sql.SendError{Err: err}); err != nil {
					return false, isSimpleQuery, errors.New("pgwire: error writing error message to the client")
				}

				// If this is a simple query, we have to send the sync message back as
				// well.
				if isSimpleQuery {
					if err := c.stmtBuf.Push(ctx, sql.Sync{
						// CRDB is implicitly generating this Sync during the simple
						// protocol.
						ExplicitFromClient: false,
					}); err != nil {
						return false, isSimpleQuery, errors.New("pgwire: error writing sync to the client while handling error")
					}
				}

				// We need to continue processing here for pgwire clients to be able to
				// successfully read the error message off pgwire.
				//
				// If break here, we terminate the connection. The client will instead see that
				// we terminated the connection prematurely (as opposed to seeing a ClientMsgTerminate
				// packet) and instead return a broken pipe or io.EOF error message.
				return false, isSimpleQuery, errors.Wrap(err, "pgwire: error reading input")
			}
			timeReceived := crtime.NowMono()
			log.VEventf(ctx, 2, "pgwire: processing %s", typ)

			if ignoreUntilSync {
				if typ != pgwirebase.ClientMsgSync {
					log.VInfof(ctx, 1, "pgwire: skipping non-sync message after encountering error")
					return false, isSimpleQuery, nil
				}
				ignoreUntilSync = false
			}

			if !authDone {
				if typ == pgwirebase.ClientMsgPassword {
					var pwd []byte
					if pwd, err = c.readBuf.GetBytes(n - 4); err != nil {
						return false, isSimpleQuery, err
					}
					// Pass the data to the authenticator. This hopefully causes it to finish
					// authentication in the background and give us an intSizer when we loop
					// around.
					if err = authPipe.sendPwdData(pwd); err != nil {
						return false, isSimpleQuery, err
					}
					return false, isSimpleQuery, nil
				}
				// Wait for the auth result.
				if err = authPipe.authResult(); err != nil {
					// The error has already been sent to the client.
					return true, isSimpleQuery, nil //nolint:returnerrcheck
				}
				authDone = true
			}

			switch typ {
			case pgwirebase.ClientMsgPassword:
				// This messages are only acceptable during the auth phase, handled above.
				err = pgwirebase.NewProtocolViolationErrorf("unexpected authentication data")
				return true, isSimpleQuery, c.writeErr(ctx, err, &c.writerState.buf)
			case pgwirebase.ClientMsgSimpleQuery:
				if err = c.handleSimpleQuery(
					ctx, &c.readBuf, timeReceived, parser.NakedIntTypeFromDefaultIntSize(atomic.LoadInt32(atomicUnqualifiedIntSize)),
				); err != nil {
					return false, isSimpleQuery, err
				}
				return false, isSimpleQuery, c.stmtBuf.Push(ctx, sql.Sync{
					// CRDB is implicitly generating this Sync during the simple
					// protocol.
					ExplicitFromClient: false,
				})

			case pgwirebase.ClientMsgExecute:
				if err := c.prohibitUnderReplicationMode(ctx); err != nil {
					return false, isSimpleQuery, err
				}
				// To support the 1PC txn fast path, we peek at the next command to
				// see if it is a Sync. This is because in the extended protocol, an
				// implicit transaction cannot commit until the Sync is seen. If there's
				// an error while peeking (for example, there are no bytes in the
				// buffer), the error is ignored since it will be handled on the next
				// loop iteration.
				followedBySync := false
				if nextMsgType, err := c.rd.Peek(1); err == nil &&
					pgwirebase.ClientMessageType(nextMsgType[0]) == pgwirebase.ClientMsgSync {
					followedBySync = true
				}
				return false, isSimpleQuery, c.handleExecute(ctx, timeReceived, followedBySync)

			case pgwirebase.ClientMsgParse:
				if err := c.prohibitUnderReplicationMode(ctx); err != nil {
					return false, isSimpleQuery, err
				}
				return false, isSimpleQuery, c.handleParse(ctx, parser.NakedIntTypeFromDefaultIntSize(atomic.LoadInt32(atomicUnqualifiedIntSize)))

			case pgwirebase.ClientMsgDescribe:
				if err := c.prohibitUnderReplicationMode(ctx); err != nil {
					return false, isSimpleQuery, err
				}
				return false, isSimpleQuery, c.handleDescribe(ctx)

			case pgwirebase.ClientMsgBind:
				if err := c.prohibitUnderReplicationMode(ctx); err != nil {
					return false, isSimpleQuery, err
				}
				return false, isSimpleQuery, c.handleBind(ctx)

			case pgwirebase.ClientMsgClose:
				if err := c.prohibitUnderReplicationMode(ctx); err != nil {
					return false, isSimpleQuery, err
				}
				return false, isSimpleQuery, c.handleClose(ctx)

			case pgwirebase.ClientMsgTerminate:
				terminateSeen = true
				return true, isSimpleQuery, nil

			case pgwirebase.ClientMsgSync:
				// We're starting a batch here. If the client continues using the extended
				// protocol and encounters an error, everything until the next sync
				// message has to be skipped. See:
				// https://www.postgresql.org/docs/current/10/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
				return false, isSimpleQuery, c.stmtBuf.Push(ctx, sql.Sync{
					// The client explicitly sent this Sync as part of the extended
					// protocol.
					ExplicitFromClient: true,
				})

			case pgwirebase.ClientMsgFlush:
				return false, isSimpleQuery, c.handleFlush(ctx)

			case pgwirebase.ClientMsgCopyData, pgwirebase.ClientMsgCopyDone, pgwirebase.ClientMsgCopyFail:
				// We're supposed to ignore these messages, per the protocol spec. This
				// state will happen when an error occurs on the server-side during a copy
				// operation: the server will send an error and a ready message back to
				// the client, and must then ignore further copy messages. See:
				// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
				return false, isSimpleQuery, nil
			default:
				return false, isSimpleQuery, c.stmtBuf.Push(
					ctx,
					sql.SendError{Err: pgwirebase.NewUnrecognizedMsgTypeErr(typ)},
				)
			}
		}()
		if err != nil {
			log.VEventf(ctx, 1, "pgwire: error processing message: %s", err)
			if !isSimpleQuery {
				// In the extended protocol, after seeing an error, we ignore all
				// messages until receiving a sync.
				ignoreUntilSync = true
			}
			repeatedErrorCount++
			// If we can't read data because of any one of the following conditions,
			// then we should break:
			// 1. the connection was closed.
			// 2. the context was canceled (e.g. during authentication).
			// 3. we reached an arbitrary threshold of repeated errors.
			if netutil.IsClosedConnection(err) ||
				errors.Is(err, context.Canceled) ||
				repeatedErrorCount > maxRepeatedErrorCount {
				break
			}
		} else {
			repeatedErrorCount = 0
		}
		if breakLoop {
			break
		}
	}

	// We're done reading data from the client, so make the communication
	// goroutine stop. Depending on what that goroutine is currently doing (or
	// blocked on), we cancel and close all the possible channels to make sure we
	// tickle it in the right way.

	// Signal command processing to stop. It might be the case that the processor
	// canceled our context and that's how we got here; in that case, this will
	// be a no-op.
	c.stmtBuf.Close()
	// Cancel the processor's context.
	c.cancelConn()
	// In case the authenticator is blocked on waiting for data from the client,
	// tell it that there's no more data coming. This is a no-op if authentication
	// was completed already.
	authPipe.noMorePwdData()

	// Wait for the processor goroutine to finish, if it hasn't already.
	procWg.Wait()

	if terminateSeen {
		return
	}
	// If we're draining, let the client know by piling on an AdminShutdownError
	// and flushing the buffer.
	if s.IsDraining() {
		// The error here is also sent with pgcode.AdminShutdown, to indicate that
		// the connection is being closed. Clients are expected to be able to handle
		// this even when not waiting for a query result. See the discussion at
		// https://github.com/cockroachdb/cockroach/issues/22630.
		// NOTE: If a query is canceled due to draining, the conn_executor already
		// will have sent a QueryCanceled error as a response to the query.
		log.Ops.Info(ctx, "closing existing connection while server is draining")
		_ /* err */ = c.writeErr(ctx, newAdminShutdownErr(ErrDrainingExistingConn), &c.writerState.buf)
		_ /* n */, _ /* err */ = c.writerState.buf.WriteTo(c.conn)
	}
}

// From https://github.com/postgres/postgres/blob/28b5726561841556dc3e00ffe26b01a8107ee654/src/backend/tcop/postgres.c#L4891-L4891
func (c *conn) prohibitUnderReplicationMode(ctx context.Context) error {
	if c.sessionArgs.ReplicationMode == sessiondatapb.ReplicationMode_REPLICATION_MODE_DISABLED {
		return nil
	}
	pgErr := pgerror.New(
		pgcode.ProtocolViolation,
		"extended query protocol not supported in a replication connection",
	)
	if err := c.stmtBuf.Push(ctx, sql.SendError{
		Err: pgErr,
	}); err != nil {
		return err
	}
	// return the same error so that ignoreUntilSync is hit.
	return pgErr
}

// readCancelKey retrieves the "backend data" key that identifies
// a cancellable query, then closes the connection.
func readCancelKey(
	ctx context.Context, buf *pgwirebase.ReadBuffer,
) (ok bool, cancelKey pgwirecancel.BackendKeyData) {
	telemetry.Inc(sqltelemetry.CancelRequestCounter)
	backendKeyDataBits, err := buf.GetUint64()
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
		newCtx, cancel = ctxlog.WithCancel(ctx)
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
		sv: &st.SV,
		msgBuilder: newWriteBuffer(func(n int64) {
			s.lookup(ctx, conn).BytesOutCount.Inc(n)
		}),
	}
	// We could, but do not, report server-side network errors while
	// trying to send the client error. This is because clients that
	// receive error payload are highly correlated with clients
	// disconnecting abruptly.
	_ /* err */ = w.writeErr(ctx, err, conn)
	return err
}

func newAdminShutdownErr(msg string) error {
	return pgerror.New(pgcode.AdminShutdown, msg)
}
