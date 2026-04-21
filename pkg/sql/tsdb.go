// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import "context"

// TimeSeriesQuerier exposes a narrow surface of the TSDB to the SQL layer.
// It is implemented in pkg/ts and wired into ExecutorConfig at server
// startup. Defining the interface here keeps pkg/sql free of an import on
// pkg/ts (which would create a cycle, since pkg/ts already depends on
// pkg/sql via SQL infrastructure for tests and admin RPCs).
//
// The interface is intentionally minimal: a single per-source point
// query. Aggregation across sources and time-axis downsampling are left
// to the SQL caller (e.g. the crdb_internal.tsdb virtual table uses
// GROUP BY and the tsround() builtin to implement downsampling).
type TimeSeriesQuerier interface {
	// QueryTimeSeries returns one TimeSeriesRow per (timestamp, source)
	// pair for the requested metric over the requested time range. Rows
	// are returned in (source, timestamp) order. The returned slice may
	// be empty when no data is available; the absence of data is not an
	// error.
	QueryTimeSeries(ctx context.Context, query TimeSeriesQuery) ([]TimeSeriesRow, error)
}

// TimeSeriesQuery describes a single TSDB query. All time fields use
// nanoseconds since the Unix epoch.
type TimeSeriesQuery struct {
	// MetricName is the fully-qualified TSDB metric (e.g.
	// "cr.node.sql.select.count"). Required.
	MetricName string
	// StartNanos is the inclusive lower bound of the query window.
	StartNanos int64
	// EndNanos is the inclusive upper bound of the query window.
	EndNanos int64
}

// TimeSeriesRow is one datapoint for a single source.
type TimeSeriesRow struct {
	TimestampNanos int64
	Value          float64
	// Source is the raw TSDB source identifier — typically the node ID
	// rendered as a decimal string for cr.node.* metrics.
	Source string
}
