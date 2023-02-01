// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// metrics contains pointers to the metrics for monitoring proxy operations.
type metrics struct {
	BackendDisconnectCount *metric.Counter
	IdleDisconnectCount    *metric.Counter
	BackendDownCount       *metric.Counter
	ClientDisconnectCount  *metric.Counter
	CurConnCount           *metric.Gauge
	RoutingErrCount        *metric.Counter
	RefusedConnCount       *metric.Counter
	SuccessfulConnCount    *metric.Counter
	ConnectionLatency      metric.IHistogram
	AuthFailedCount        *metric.Counter
	ExpiredClientConnCount *metric.Counter

	DialTenantLatency metric.IHistogram
	DialTenantRetries *metric.Counter

	ConnMigrationSuccessCount                *metric.Counter
	ConnMigrationErrorFatalCount             *metric.Counter
	ConnMigrationErrorRecoverableCount       *metric.Counter
	ConnMigrationAttemptedCount              *metric.Counter
	ConnMigrationAttemptedLatency            metric.IHistogram
	ConnMigrationTransferResponseMessageSize metric.IHistogram

	QueryCancelReceivedPGWire *metric.Counter
	QueryCancelReceivedHTTP   *metric.Counter
	QueryCancelForwarded      *metric.Counter
	QueryCancelIgnored        *metric.Counter
	QueryCancelSuccessful     *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (metrics) MetricStruct() {}

var _ metric.Struct = metrics{}

const (
	// maxExpectedTransferResponseMessageSize corresponds to maximum expected
	// response message size for the SHOW TRANSFER STATE query. We choose 16MB
	// here to match the defaultMaxReadBufferSize used for ingesting SQL
	// statements in the SQL server (see pkg/sql/pgwire/pgwirebase/encoding.go).
	//
	// This will be used to tune sql.session_transfer.max_session_size.
	maxExpectedTransferResponseMessageSize = 1 << 24 // 16MB
)

var (
	metaCurConnCount = metric.Metadata{
		Name:        "proxy.sql.conns",
		Help:        "Number of connections being proxied",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaConnectionLatency = metric.Metadata{
		Name:        "proxy.sql.connection_latency",
		Unit:        metric.Unit_NANOSECONDS,
		Help:        "Latency histogram for connecting and authenticating to a tenant cluster.",
		Measurement: "Latency",
	}
	metaRoutingErrCount = metric.Metadata{
		Name:        "proxy.err.routing",
		Help:        "Number of errors encountered when attempting to route clients",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaBackendDownCount = metric.Metadata{
		Name:        "proxy.err.backend_down",
		Help:        "Number of errors encountered when connecting to backend servers",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaBackendDisconnectCount = metric.Metadata{
		Name:        "proxy.err.backend_disconnect",
		Help:        "Number of disconnects initiated by proxied backends",
		Measurement: "Disconnects",
		Unit:        metric.Unit_COUNT,
	}
	metaIdleDisconnectCount = metric.Metadata{
		Name:        "proxy.err.idle_disconnect",
		Help:        "Number of disconnects due to idle timeout",
		Measurement: "Idle Disconnects",
		Unit:        metric.Unit_COUNT,
	}
	metaClientDisconnectCount = metric.Metadata{
		Name:        "proxy.err.client_disconnect",
		Help:        "Number of disconnects initiated by clients",
		Measurement: "Client Disconnects",
		Unit:        metric.Unit_COUNT,
	}
	metaRefusedConnCount = metric.Metadata{
		Name:        "proxy.err.refused_conn",
		Help:        "Number of refused connections initiated by a given IP",
		Measurement: "Refused",
		Unit:        metric.Unit_COUNT,
	}
	metaSuccessfulConnCount = metric.Metadata{
		Name:        "proxy.sql.successful_conns",
		Help:        "Number of successful connections that were/are being proxied",
		Measurement: "Successful Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaAuthFailedCount = metric.Metadata{
		Name:        "proxy.sql.authentication_failures",
		Help:        "Number of authentication failures",
		Measurement: "Authentication Failures",
		Unit:        metric.Unit_COUNT,
	}
	metaExpiredClientConnCount = metric.Metadata{
		Name:        "proxy.sql.expired_client_conns",
		Help:        "Number of expired client connections",
		Measurement: "Expired Client Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaDialTenantLatency = metric.Metadata{
		Name:        "proxy.dial_tenant.latency",
		Unit:        metric.Unit_NANOSECONDS,
		Help:        "Latency histogram for establishing a tcp connection to a tenant cluster.",
		Measurement: "Latency",
	}
	metaDialTenantRetries = metric.Metadata{
		Name:        "proxy.dial_tenant.retries",
		Unit:        metric.Unit_COUNT,
		Help:        "Number of retries dialing a tenant cluster.",
		Measurement: "Retries",
	}
	// Connection migration metrics.
	//
	// attempted = success + error_fatal + error_recoverable
	metaConnMigrationSuccessCount = metric.Metadata{
		Name:        "proxy.conn_migration.success",
		Help:        "Number of successful connection migrations",
		Measurement: "Connection Migrations",
		Unit:        metric.Unit_COUNT,
	}
	metaConnMigrationErrorFatalCount = metric.Metadata{
		// When connection migrations errored out, connections will be closed.
		Name:        "proxy.conn_migration.error_fatal",
		Help:        "Number of failed connection migrations which resulted in terminations",
		Measurement: "Connection Migrations",
		Unit:        metric.Unit_COUNT,
	}
	metaConnMigrationErrorRecoverableCount = metric.Metadata{
		// Connections are recoverable, so they won't be closed.
		Name:        "proxy.conn_migration.error_recoverable",
		Help:        "Number of failed connection migrations that were recoverable",
		Measurement: "Connection Migrations",
		Unit:        metric.Unit_COUNT,
	}
	metaConnMigrationAttemptedCount = metric.Metadata{
		Name:        "proxy.conn_migration.attempted",
		Help:        "Number of attempted connection migrations",
		Measurement: "Connection Migrations",
		Unit:        metric.Unit_COUNT,
	}
	metaConnMigrationAttemptedLatency = metric.Metadata{
		Name:        "proxy.conn_migration.attempted.latency",
		Help:        "Latency histogram for attempted connection migrations",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaConnMigrationTransferResponseMessageSize = metric.Metadata{
		Name:        "proxy.conn_migration.transfer_response.message_size",
		Help:        "Message size for the SHOW TRANSFER STATE response",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaQueryCancelReceivedPGWire = metric.Metadata{
		Name:        "proxy.query_cancel.received.pgwire",
		Help:        "Number of query cancel requests this proxy received over pgwire",
		Measurement: "Query Cancel Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaQueryCancelReceivedHTTP = metric.Metadata{
		Name:        "proxy.query_cancel.received.http",
		Help:        "Number of query cancel requests this proxy received over HTTP",
		Measurement: "Query Cancel Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaQueryCancelIgnored = metric.Metadata{
		Name:        "proxy.query_cancel.ignored",
		Help:        "Number of query cancel requests this proxy ignored",
		Measurement: "Query Cancel Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaQueryCancelForwarded = metric.Metadata{
		Name:        "proxy.query_cancel.forwarded",
		Help:        "Number of query cancel requests this proxy forwarded to another proxy",
		Measurement: "Query Cancel Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaQueryCancelSuccessful = metric.Metadata{
		Name:        "proxy.query_cancel.successful",
		Help:        "Number of query cancel requests this proxy forwarded to the tenant",
		Measurement: "Query Cancel Requests",
		Unit:        metric.Unit_COUNT,
	}
)

// makeProxyMetrics instantiates the metrics holder for proxy monitoring.
func makeProxyMetrics() metrics {

	return metrics{
		BackendDisconnectCount: metric.NewCounter(metaBackendDisconnectCount),
		IdleDisconnectCount:    metric.NewCounter(metaIdleDisconnectCount),
		BackendDownCount:       metric.NewCounter(metaBackendDownCount),
		ClientDisconnectCount:  metric.NewCounter(metaClientDisconnectCount),
		CurConnCount:           metric.NewGauge(metaCurConnCount),
		RoutingErrCount:        metric.NewCounter(metaRoutingErrCount),
		RefusedConnCount:       metric.NewCounter(metaRefusedConnCount),
		SuccessfulConnCount:    metric.NewCounter(metaSuccessfulConnCount),
		ConnectionLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePreferHdrLatency,
			Metadata: metaConnMigrationAttemptedCount,
			Duration: base.DefaultHistogramWindowInterval(),
			Buckets:  metric.NetworkLatencyBuckets,
		}),
		AuthFailedCount:        metric.NewCounter(metaAuthFailedCount),
		ExpiredClientConnCount: metric.NewCounter(metaExpiredClientConnCount),
		// Connector metrics.
		DialTenantLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePreferHdrLatency,
			Metadata: metaDialTenantLatency,
			Duration: base.DefaultHistogramWindowInterval(),
			Buckets:  metric.NetworkLatencyBuckets},
		),
		DialTenantRetries: metric.NewCounter(metaDialTenantRetries),
		// Connection migration metrics.
		ConnMigrationSuccessCount:          metric.NewCounter(metaConnMigrationSuccessCount),
		ConnMigrationErrorFatalCount:       metric.NewCounter(metaConnMigrationErrorFatalCount),
		ConnMigrationErrorRecoverableCount: metric.NewCounter(metaConnMigrationErrorRecoverableCount),
		ConnMigrationAttemptedCount:        metric.NewCounter(metaConnMigrationAttemptedCount),
		ConnMigrationAttemptedLatency: metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePreferHdrLatency,
			Metadata: metaConnMigrationAttemptedLatency,
			Duration: base.DefaultHistogramWindowInterval(),
			Buckets:  metric.NetworkLatencyBuckets,
		}),
		ConnMigrationTransferResponseMessageSize: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metaConnMigrationTransferResponseMessageSize,
			Duration: base.DefaultHistogramWindowInterval(),
			Buckets:  metric.DataSize16MBBuckets,
			MaxVal:   maxExpectedTransferResponseMessageSize,
			SigFigs:  1,
		}),
		QueryCancelReceivedPGWire: metric.NewCounter(metaQueryCancelReceivedPGWire),
		QueryCancelReceivedHTTP:   metric.NewCounter(metaQueryCancelReceivedHTTP),
		QueryCancelIgnored:        metric.NewCounter(metaQueryCancelIgnored),
		QueryCancelForwarded:      metric.NewCounter(metaQueryCancelForwarded),
		QueryCancelSuccessful:     metric.NewCounter(metaQueryCancelSuccessful),
	}
}

// updateForError updates the metrics relevant for the type of the error
// message.
func (metrics *metrics) updateForError(err error) {
	if err == nil {
		return
	}
	switch getErrorCode(err) {
	case codeExpiredClientConnection:
		metrics.ExpiredClientConnCount.Inc(1)
	case codeBackendDisconnected, codeBackendReadFailed, codeBackendWriteFailed:
		metrics.BackendDisconnectCount.Inc(1)
	case codeClientDisconnected, codeClientWriteFailed, codeClientReadFailed:
		metrics.ClientDisconnectCount.Inc(1)
	case codeProxyRefusedConnection:
		metrics.RefusedConnCount.Inc(1)
		metrics.BackendDownCount.Inc(1)
	case codeParamsRoutingFailed, codeUnavailable:
		metrics.RoutingErrCount.Inc(1)
		metrics.BackendDownCount.Inc(1)
	case codeBackendDown:
		metrics.BackendDownCount.Inc(1)
	case codeAuthFailed:
		metrics.AuthFailedCount.Inc(1)
	}
}
