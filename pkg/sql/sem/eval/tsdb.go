// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// ErrTooManyTimeSeriesRows is returned (wrapped) when a query would,
// or did, exceed TimeSeriesQuery.MaxRows. Detect with errors.Is.
var ErrTooManyTimeSeriesRows = errors.New("time series query result exceeds row cap")

// TimeSeriesQuerier exposes TSDB to the SQL layer.
type TimeSeriesQuerier interface {
	// QueryTimeSeries returns one TimeSeriesRow per datapoint produced
	// by the underlying TSDB. The Source field is populated only when
	// the adapter dispatches a per-source batch (Sources listed without
	// SourceAggregator); it is empty otherwise. Callers that require a
	// specific row ordering must ORDER BY explicitly.
	QueryTimeSeries(ctx context.Context, query TimeSeriesQuery) ([]TimeSeriesRow, error)
}

// TimeSeriesQuery describes a TSDB query with optional downsampling,
// derivative, source filter, and cross-source aggregation. All time
// fields use nanoseconds since the Unix epoch.
type TimeSeriesQuery struct {
	// MetricName is the fully-qualified TSDB metric (e.g.
	// "cr.node.sql.select.count"). Required.
	MetricName string
	// StartNanos is the lower bound of the query window. Best-effort
	// inclusive: the underlying TSDB samples on a fixed resolution and
	// rounds to the enclosing bucket; the implementation may also pull
	// this bound earlier to keep the window inside the queryable range.
	StartNanos int64
	// EndNanos is the upper bound of the query window. Implementations
	// clamp this down to wall-clock now; a window entirely in the
	// future returns zero rows.
	EndNanos int64
	// Downsampler is the per-bucket aggregation function. Zero value
	// uses the TSDB default (AVG). Meaningful only when SampleNanos > 0.
	Downsampler Aggregator
	// SampleNanos is the downsampling bucket size in nanoseconds. Must
	// be a positive multiple of 10s (validated upstream), or 0 for
	// native resolution.
	SampleNanos int64
	// Derivative converts datapoints to rate-of-change. Zero value
	// uses the TSDB default (NONE).
	Derivative Derivative
	// SourceAggregator is the cross-source aggregator applied per
	// bucket. Zero value uses the TSDB default (SUM). Ignored on the
	// per-source dispatch path; see pkg/ts/sql_adapter.go.
	SourceAggregator Aggregator
	// Sources restricts the query to a subset of sources. Nil or empty
	// means all sources. The slice is captured by reference: the
	// implementation may pass it through to a tspb.Query without
	// copying, so callers must not mutate it after the call.
	Sources []string
	// MaxRows, when > 0, causes the implementation to refuse a query
	// that returns more than this many rows. Trips return an error
	// wrapping ErrTooManyTimeSeriesRows.
	MaxRows int64
}

// Validate checks that q's fields are mutually consistent, returning
// a pgerror with code InvalidParameterValue on a violation.
//
// The SQL-side parser already enforces these invariants. Validate
// defends against a Go caller constructing a literal
// TimeSeriesQuery{} that the TSDB would silently no-op.
func (q *TimeSeriesQuery) Validate() error {
	if q.MetricName == "" {
		return pgerror.New(pgcode.InvalidParameterValue,
			"TimeSeriesQuery requires a non-empty MetricName")
	}
	if q.Downsampler != AggregatorDefault && q.SampleNanos == 0 {
		return pgerror.New(pgcode.InvalidParameterValue,
			"TimeSeriesQuery: Downsampler is set but SampleNanos is 0; "+
				"the downsampler would silently no-op in the TSDB")
	}
	return nil
}

// TimeSeriesRow is one datapoint for a single source.
type TimeSeriesRow struct {
	TimestampNanos int64
	Value          float64
	// Source is the raw TSDB source identifier — typically the node ID
	// rendered as a decimal string for cr.node.* metrics.
	Source string
}

// Aggregator names a TSDB aggregation function. It mirrors
// tspb.TimeSeriesQueryAggregator but adds AggregatorDefault as the
// zero value, so a literal TimeSeriesQuery{} uses the TSDB
// proto's defaults; the adapter translates it into "field unset" on
// tspb.Query.
type Aggregator int

const (
	AggregatorDefault Aggregator = iota
	AggregatorAvg
	AggregatorSum
	AggregatorMax
	AggregatorMin
	AggregatorFirst
	AggregatorLast
	AggregatorVariance
)

// Derivative names a TSDB rate-of-change function. Mirrors
// tspb.TimeSeriesQueryDerivative; DerivativeDefault is the zero
// value — see Aggregator.
type Derivative int

const (
	DerivativeDefault Derivative = iota
	DerivativeNone
	DerivativeFirstOrder
	DerivativeNonNegativeFirstOrder
)
