// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirecancel"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// Fully-qualified names for metrics.
var (
	MetaPreServeNewConns = metric.Metadata{
		Name:        "sql.pre_serve.new_conns",
		Help:        "Number of SQL connections created prior to routing the connection to the target SQL server",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaPreServeBytesIn = metric.Metadata{
		Name:        "sql.pre_serve.bytesin",
		Help:        "Number of SQL bytes received prior to routing the connection to the target SQL server",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaPreServeBytesOut = metric.Metadata{
		Name:        "sql.pre_serve.bytesout",
		Help:        "Number of SQL bytes sent prior to routing the connection to the target SQL server",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaPreServeConnFailures = metric.Metadata{
		Name:        "sql.pre_serve.conn.failures",
		Help:        "Number of SQL connection failures prior to routing the connection to the target SQL server",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaPreServeMaxBytes = metric.Metadata{
		Name:        "sql.pre_serve.mem.max",
		Help:        "Memory usage for SQL connections prior to routing the connection to the target SQL server",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	MetaPreServeCurBytes = metric.Metadata{
		Name:        "sql.pre_serve.mem.cur",
		Help:        "Current memory usage for SQL connections prior to routing the connection to the target SQL server",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
)

// PreServeConnHandler implements the early initialization of an incoming
// SQL connection, before it is routed to a specific tenant. It is
// tenant-independent, and thus cannot rely on tenant-specific connection
// or state.
type PreServeConnHandler struct {
	cfg                      *base.Config
	st                       *cluster.Settings
	tenantIndependentMetrics tenantIndependentMetrics

	getTLSConfig func() (*tls.Config, error)

	// tenantIndependentConnMonitor is the pool where the
	// memory usage for the initial connection overhead
	// is accounted for. After the connection is attributed
	// to a specific tenant, the account for the initial
	// connection overhead is transferred to the per-tenant
	// monitor.
	tenantIndependentConnMonitor *mon.BytesMonitor

	// trustClientProvidedRemoteAddr indicates whether the server should honor
	// a `crdb:remote_addr` status parameter provided by the client during
	// session authentication. This status parameter can be set by SQL proxies
	// to feed the "real" client address, where otherwise the CockroachDB SQL
	// server would only see the address of the proxy.
	//
	// This setting is security-sensitive and should not be enabled
	// without a SQL proxy that carefully scrubs any client-provided
	// `crdb:remote_addr` field. In particular, this setting should never
	// be set when there is no SQL proxy at all. Otherwise, a malicious
	// client could use this field to pretend being from another address
	// than its own and defeat the HBA rules.
	//
	// TODO(knz,ben): It would be good to have something more specific
	// than a boolean, i.e. to accept the provided address only from
	// certain peer IPs, or with certain certificates. (could it be a
	// special hba.conf directive?)
	trustClientProvidedRemoteAddr atomic.Bool

	// acceptSystemIdentityOption determines whether the system_identity
	// option will be read from the client. This is used in tests.
	acceptSystemIdentityOption atomic.Bool

	// acceptTenantName determines whether this pre-serve handler will
	// interpret a tenant name specification in the connection
	// parameters.
	acceptTenantName bool
}

// NewPreServeConnHandler creates a PreServeConnHandler.
// sv refers to the setting values "outside" of the current tenant - i.e. from the storage cluster.
func NewPreServeConnHandler(
	ambientCtx log.AmbientContext,
	cfg *base.Config,
	st *cluster.Settings,
	getTLSConfig func() (*tls.Config, error),
	histogramWindow time.Duration,
	parentMemoryMonitor *mon.BytesMonitor,
	acceptTenantName bool,
) *PreServeConnHandler {
	ctx := ambientCtx.AnnotateCtx(context.Background())
	metrics := makeTenantIndependentMetrics(histogramWindow)
	s := PreServeConnHandler{
		cfg:                      cfg,
		st:                       st,
		acceptTenantName:         acceptTenantName,
		tenantIndependentMetrics: metrics,
		getTLSConfig:             getTLSConfig,

		tenantIndependentConnMonitor: mon.NewMonitor(mon.Options{
			Name:       mon.MakeMonitorName("pre-conn"),
			CurCount:   metrics.PreServeCurBytes,
			MaxHist:    metrics.PreServeMaxBytes,
			Increment:  int64(connReservationBatchSize) * baseSQLMemoryBudget,
			Settings:   st,
			LongLiving: true,
		}),
	}
	s.tenantIndependentConnMonitor.StartNoReserved(ctx, parentMemoryMonitor)

	// TODO(knz,ben): Use a cluster setting for this.
	s.trustClientProvidedRemoteAddr.Store(trustClientProvidedRemoteAddrOverride)

	return &s
}

// AnnotateCtxForIncomingConn annotates the provided context with a
// tag that reports the peer's address. In the common case, the
// context is annotated with a "client" tag. When the server is
// configured to recognize client-specified remote addresses, it is
// annotated with a "peer" tag and the "client" tag is added later
// when the session is set up.
func (s *PreServeConnHandler) AnnotateCtxForIncomingConn(
	ctx context.Context, conn net.Conn,
) context.Context {
	tag := "client"
	if s.trustClientProvidedRemoteAddr.Load() {
		tag = "peer"
	}
	return logtags.AddTag(ctx, tag, conn.RemoteAddr().String())
}

// tenantIndependentMetrics is the set of metrics for the
// pre-serve part of connection handling, before the connection
// is routed to a specific tenant.
type tenantIndependentMetrics struct {
	PreServeBytesInCount  *metric.Counter
	PreServeBytesOutCount *metric.Counter
	PreServeConnFailures  *metric.Counter
	PreServeNewConns      *metric.Counter
	PreServeMaxBytes      metric.IHistogram
	PreServeCurBytes      *metric.Gauge
}

func makeTenantIndependentMetrics(histogramWindow time.Duration) tenantIndependentMetrics {
	return tenantIndependentMetrics{
		PreServeBytesInCount:  metric.NewCounter(MetaPreServeBytesIn),
		PreServeBytesOutCount: metric.NewCounter(MetaPreServeBytesOut),
		PreServeNewConns:      metric.NewCounter(MetaPreServeNewConns),
		PreServeConnFailures:  metric.NewCounter(MetaPreServeConnFailures),
		PreServeMaxBytes: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     MetaPreServeMaxBytes,
			Duration:     histogramWindow,
			BucketConfig: metric.MemoryUsage64MBBuckets,
			Mode:         metric.HistogramModePrometheus,
		}),
		PreServeCurBytes: metric.NewGauge(MetaPreServeCurBytes),
	}
}

// Metrics returns the set of metrics structs.
func (s *PreServeConnHandler) Metrics() (res []interface{}) {
	return []interface{}{&s.tenantIndependentMetrics}
}

// SendRoutingError informs the client that they selected an invalid
// cluster and closes the connection.
func (s *PreServeConnHandler) SendRoutingError(
	ctx context.Context, conn net.Conn, tenantName roachpb.TenantName,
) {
	err := errors.WithHint(
		pgerror.Newf(pgcode.ConnectionException,
			"service unavailable for target tenant (%v)", tenantName),
		`Double check your "-ccluster=" connection option or your "cluster:" database name prefix.`)

	_ = s.sendErr(ctx, s.st, conn, err)
}

// sendErr sends errors to the client during the connection startup
// sequence. Later error sends during/after authentication are handled
// in conn.go.
func (s *PreServeConnHandler) sendErr(
	ctx context.Context, st *cluster.Settings, conn net.Conn, err error,
) error {
	w := errWriter{
		sv:         &st.SV,
		msgBuilder: newWriteBuffer(s.tenantIndependentMetrics.PreServeBytesOutCount.Inc),
	}
	// We could, but do not, report server-side network errors while
	// trying to send the client error. This is because clients that
	// receive error payload are highly correlated with clients
	// disconnecting abruptly.
	_ /* err */ = w.writeErr(ctx, err, conn)
	return err
}

// tenantIndependentClientParameters encapsulates the session
// parameters provided to the client, prior to any tenant-specific
// configuration adjustements.
type tenantIndependentClientParameters struct {
	sql.SessionArgs
	foundBufferSize bool
	tenantName      string
}

// PreServeState describes the state of a connection after PrepareConn,
// before a specific tenant has been selected.
type PreServeState int8

const (
	// PreServeReady indicates the connection was set up successfully
	// and can serve SQL traffic.
	PreServeReady PreServeState = iota
	// PreServeCancel indicates that the client has sent a cancel
	// request. No further traffic is expected and the net.Conn
	// has been closed already.
	PreServeCancel
	// PreServeError indicates that an error was encountered during
	// PrepareConn. No further traffic is expected. The caller is responsible
	// for closing the net.Conn.
	PreServeError
)

// PreServeStatus encapsulates the result of PrepareConn, before
// a specific tenant has been selected.
type PreServeStatus struct {
	// State is the state of the connection. See the values
	// defined above.
	State PreServeState

	// ConnType is the type of incoming connection.
	ConnType hba.ConnType

	// CancelKey is the data sufficient to serve a cancel request.
	// Defined only if State == PreServeCancel.
	CancelKey pgwirecancel.BackendKeyData

	// Reserved is a memory account of the memory overhead for the
	// connection. Defined only if State == PreServeReady.
	Reserved *mon.BoundAccount

	// clientParameters is the set of client-provided status parameters.
	clientParameters tenantIndependentClientParameters
}

// GetTenantName retrieves the selected tenant name.
func (st PreServeStatus) GetTenantName() string {
	return st.clientParameters.tenantName
}

// ReleaseMemory releases memory reserved for the "pre-serve" phase of a
// connection.
func (st PreServeStatus) ReleaseMemory(ctx context.Context) {
	if st.State == PreServeReady {
		st.Reserved.Clear(ctx)
	}
}

// PreServe serves a single connection, up to and including the
// point status parameters are read from the client (which happens
// pre-authentication). This logic is tenant-independent. Once the
// status parameters are known, the connection can be routed to a
// particular tenant.
func (s *PreServeConnHandler) PreServe(
	ctx context.Context, origConn net.Conn, socketType SocketType,
) (conn net.Conn, st PreServeStatus, err error) {
	defer func() {
		if err != nil {
			s.tenantIndependentMetrics.PreServeConnFailures.Inc(1)
		}
	}()
	s.tenantIndependentMetrics.PreServeNewConns.Inc(1)

	conn = origConn
	st.State = PreServeError // Will be overridden below as needed.

	// In any case, first check the command in the start-up message.
	//
	// We're assuming that a client is not willing/able to receive error
	// packets before we drain that message.
	version, buf, err := s.readVersion(conn)
	if err != nil {
		return conn, st, err
	}

	switch version {
	case versionCancel:
		// The cancel message is rather peculiar: it is sent without
		// authentication, always over an unencrypted channel.
		if ok, key := readCancelKey(ctx, &buf); ok {
			return conn, PreServeStatus{
				State:     PreServeCancel,
				CancelKey: key,
			}, nil
		} else {
			return conn, st, errors.New("cannot read cancel key")
		}

	case versionGSSENC:
		// This is a request for an unsupported feature: GSS encryption.
		// https://github.com/cockroachdb/cockroach/issues/52184
		//
		// Ensure the right SQLSTATE is sent to the SQL client.
		err := pgerror.New(pgcode.ProtocolViolation, "GSS encryption is not yet supported")
		// Annotate a telemetry key. These objects
		// are treated specially by sendErr: they increase a
		// telemetry counter to indicate an attempt was made
		// to use this feature.
		err = errors.WithTelemetry(err, "#52184")
		return conn, st, s.sendErr(ctx, s.st, conn, err)
	}

	// Compute the initial connType.
	st.ConnType, err = socketType.asConnType()
	if err != nil {
		return conn, st, err
	}

	// If the client requests SSL, upgrade the connection to use TLS.
	var clientErr error
	conn, st.ConnType, version, clientErr, err = s.maybeUpgradeToSecureConn(ctx, conn, st.ConnType, version, &buf)
	if err != nil {
		return conn, st, err
	}
	if clientErr != nil {
		return conn, st, s.sendErr(ctx, s.st, conn, clientErr)
	}

	// What does the client want to do?
	switch version {
	case version30:
		// Normal SQL connection. Proceed normally below.

	case versionCancel:
		// The PostgreSQL protocol definition says that cancel payloads
		// must be sent *prior to upgrading the connection to use TLS*.
		// Yet, we've found clients in the wild that send the cancel
		// after the TLS handshake, for example at
		// https://github.com/cockroachlabs/support/issues/600.
		if ok, key := readCancelKey(ctx, &buf); ok {
			return conn, PreServeStatus{
				State:     PreServeCancel,
				CancelKey: key,
			}, nil
		} else {
			return conn, st, errors.New("cannot read cancel key")
		}

	default:
		// We don't know this protocol.
		err := pgerror.Newf(pgcode.ProtocolViolation, "unknown protocol version %d", version)
		err = errors.WithTelemetry(err, fmt.Sprintf("protocol-version-%d", version))
		return conn, st, s.sendErr(ctx, s.st, conn, err)
	}

	// Reserve some memory for this connection using the server's monitor. This
	// reduces pressure on the shared pool because the server monitor allocates in
	// chunks from the shared pool and these chunks should be larger than
	// baseSQLMemoryBudget.
	connBoundAccount := s.tenantIndependentConnMonitor.MakeBoundAccount()
	st.Reserved = &connBoundAccount
	if err := st.Reserved.Grow(ctx, baseSQLMemoryBudget); err != nil {
		return conn, st, errors.Wrapf(err, "unable to pre-allocate %d bytes for this connection",
			baseSQLMemoryBudget)
	}

	// Load the client-provided session parameters.
	st.clientParameters, err = parseClientProvidedSessionParameters(
		ctx, &buf, conn.RemoteAddr(), s.trustClientProvidedRemoteAddr.Load(), s.acceptTenantName, s.acceptSystemIdentityOption.Load())
	if err != nil {
		st.Reserved.Clear(ctx)
		return conn, st, s.sendErr(ctx, s.st, conn, err)
	}
	st.clientParameters.IsSSL = st.ConnType == hba.ConnHostSSL

	st.State = PreServeReady
	return conn, st, nil
}

// maybeUpgradeToSecureConn upgrades the connection to TLS/SSL if
// requested by the client, and available in the server configuration.
func (s *PreServeConnHandler) maybeUpgradeToSecureConn(
	ctx context.Context,
	conn net.Conn,
	connType hba.ConnType,
	version uint32,
	buf *pgwirebase.ReadBuffer,
) (newConn net.Conn, newConnType hba.ConnType, newVersion uint32, clientErr, serverErr error) {
	// By default, this is a no-op.
	newConn = conn
	newConnType = connType
	newVersion = version
	var n int // byte counts

	if version != versionSSL {
		// The client did not require a SSL connection.

		// Insecure mode: nothing to say, nothing to do.
		// TODO(knz): Remove this condition - see
		// https://github.com/cockroachdb/cockroach/issues/53404
		if s.cfg.Insecure {
			return
		}

		// Secure mode: disallow if TCP and the user did not opt into
		// non-TLS SQL conns.
		if !s.cfg.AcceptSQLWithoutTLS && connType != hba.ConnLocal && connType != hba.ConnInternalLoopback {
			clientErr = pgerror.New(pgcode.ProtocolViolation, ErrSSLRequired)
		}
		return
	}

	if connType == hba.ConnLocal || connType == hba.ConnInternalLoopback {
		// No existing PostgreSQL driver ever tries to activate TLS over a unix
		// socket. Similarly, internal loopback connections don't use TLS. But in
		// case someone, sometime, somewhere, makes that mistake, let them know that
		// we don't want it.
		clientErr = pgerror.New(pgcode.ProtocolViolation,
			"cannot use SSL/TLS over local connections")
		return
	}

	// Protocol sanity check.
	if len(buf.Msg) > 0 {
		serverErr = errors.Errorf("unexpected data after SSLRequest: %q", buf.Msg)
		return
	}

	// The client has requested SSL. We're going to try and upgrade the
	// connection to use TLS/SSL.

	// Do we have a TLS configuration?
	tlsConfig, serverErr := s.getTLSConfig()
	if serverErr != nil {
		return
	}

	if tlsConfig == nil {
		// We don't have a TLS configuration available, so we can't honor
		// the client's request.
		n, serverErr = conn.Write(sslUnsupported)
		if serverErr != nil {
			return
		}
	} else {
		// We have a TLS configuration. Upgrade the connection.
		n, serverErr = conn.Write(sslSupported)
		if serverErr != nil {
			return
		}
		newConn = tls.Server(conn, tlsConfig)
		newConnType = hba.ConnHostSSL
	}
	s.tenantIndependentMetrics.PreServeBytesOutCount.Inc(int64(n))

	// Finally, re-read the version/command from the client.
	newVersion, *buf, serverErr = s.readVersion(newConn)
	return
}

// readVersion reads the start-up message, then returns the version
// code (first uint32 in message) and the buffer containing the rest
// of the payload.
func (s *PreServeConnHandler) readVersion(
	conn io.Reader,
) (version uint32, buf pgwirebase.ReadBuffer, err error) {
	var n int
	buf = pgwirebase.MakeReadBuffer(
		pgwirebase.ReadBufferOptionWithClusterSettings(&s.st.SV),
	)
	n, err = buf.ReadUntypedMsg(conn)
	if err != nil {
		return
	}
	version, err = buf.GetUint32()
	if err != nil {
		return
	}
	s.tenantIndependentMetrics.PreServeBytesInCount.Inc(int64(n))
	return
}
