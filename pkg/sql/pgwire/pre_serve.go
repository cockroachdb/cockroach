// Copyright 2022 The Cockroach Authors.
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
	"net"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Fully-qualified names for metrics.
var (
	MetaPreServeNewConns = metric.Metadata{
		Name:        "sql.pre_serve.new_conns",
		Help:        "Counter of the number of SQL connections created prior to routine the connection a specific tenant",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	MetaPreServeBytesIn = metric.Metadata{
		Name:        "sql.pre_serve.bytesin",
		Help:        "Number of SQL bytes received prior to routing the connection to a specific tenant",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaPreServeBytesOut = metric.Metadata{
		Name:        "sql.pre_serve.bytesout",
		Help:        "Number of SQL bytes sent prior to routing the connection to a specific tenant",
		Measurement: "SQL Bytes",
		Unit:        metric.Unit_BYTES,
	}
	MetaPreServeConnFailures = metric.Metadata{
		Name:        "sql.pre_serve.conn.failures",
		Help:        "Number of SQL connection failures prior to routing the connection to a specific tenant",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
)

// PreServeConnHandler implements the early initialization of an incoming
// SQL connection, before it is routed to a specific tenant. It is
// tenant-independent, and thus cannot rely on tenant-specific connection
// or state.
type PreServeConnHandler struct {
	errWriter                errWriter
	tenantIndependentMetrics tenantIndependentMetrics
}

// MakePreServeConnHandler creates a PreServeConnHandler.
// sv refers to the setting values "outside" of the current tenant - i.e. from the storage cluster.
func MakePreServeConnHandler(sv *settings.Values) PreServeConnHandler {
	metrics := makeTenantIndependentMetrics()
	return PreServeConnHandler{
		errWriter: errWriter{
			sv:         sv,
			msgBuilder: newWriteBuffer(metrics.PreServeBytesOutCount),
		},
		tenantIndependentMetrics: metrics,
	}
}

// tenantIndependentMetrics is the set of metrics for the
// pre-serve part of connection handling, before the connection
// is routed to a specific tenant.
type tenantIndependentMetrics struct {
	PreServeBytesInCount  *metric.Counter
	PreServeBytesOutCount *metric.Counter
	PreServeConnFailures  *metric.Counter
	PreServeNewConns      *metric.Counter
}

func makeTenantIndependentMetrics() tenantIndependentMetrics {
	return tenantIndependentMetrics{
		PreServeBytesInCount:  metric.NewCounter(MetaPreServeBytesIn),
		PreServeBytesOutCount: metric.NewCounter(MetaPreServeBytesOut),
		PreServeNewConns:      metric.NewCounter(MetaPreServeNewConns),
		PreServeConnFailures:  metric.NewCounter(MetaPreServeConnFailures),
	}
}

// Metrics returns the set of metrics structs.
func (s *PreServeConnHandler) Metrics() (res []interface{}) {
	return []interface{}{&s.tenantIndependentMetrics}
}

// sendErr sends errors to the client during the connection startup
// sequence. Later error sends during/after authentication are handled
// in conn.go.
func (s *PreServeConnHandler) sendPreServeErr(ctx context.Context, conn net.Conn, err error) error {
	// We could, but do not, report server-side network errors while
	// trying to send the client error. This is because clients that
	// receive error payload are highly correlated with clients
	// disconnecting abruptly.
	_ /* err */ = s.errWriter.writeErr(ctx, err, conn)
	_ = conn.Close()
	return err
}

// PreServe serves a single connection, up to and including the
// point status parameters are read from the client (which happens
// pre-authentication).  This logic is tenant-independent. Once the
// status parameters are known, the connection can be routed to a
// particular tenant.
func (s *PreServeConnHandler) PreServe(
	ctx context.Context, conn net.Conn, socketType SocketType,
) (err error) {
	defer func() {
		if err != nil {
			s.tenantIndependentMetrics.PreServeConnFailures.Inc(1)
		}
	}()
	s.tenantIndependentMetrics.PreServeNewConns.Inc(1)

	return nil
}
