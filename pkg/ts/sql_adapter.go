// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// queryFn is the subset of the TSDB server Query API that SQLAdapter
// depends on. Both *Server (system tenant) and *TenantServer (secondary
// tenants) satisfy it.
type queryFn func(ctx context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error)

// SQLAdapter bridges the SQL layer to the TSDB. It implements
// eval.TimeSeriesQuerier on top of a Query function so that pkg/sql
// can query historical metrics without taking a direct dependency on
// pkg/ts. See QueryTimeSeries for the dispatch rules.
type SQLAdapter struct {
	query queryFn
	// Defaults to timeutil.Now;
	now func() time.Time
}

var _ eval.TimeSeriesQuerier = (*SQLAdapter)(nil)

// NewSQLAdapter constructs an adapter that delegates to the supplied
// system-tenant TSDB server.
func NewSQLAdapter(server *Server) *SQLAdapter {
	return &SQLAdapter{query: server.Query, now: timeutil.Now}
}

// NewTenantSQLAdapter constructs an adapter that delegates to a
// secondary-tenant TSDB server. Tenant queries are scoped by the
// underlying TenantServer to that tenant's metrics only.
func NewTenantSQLAdapter(server *TenantServer) *SQLAdapter {
	return &SQLAdapter{query: server.Query, now: timeutil.Now}
}

// clampBounds mutates q to keep the query window inside the
// resolution's queryable range.
//
//  1. Clamp EndNanos to wall-clock now.
//  2. If StartNanos > EndNanos (future-only window), the caller
//     short-circuits.
//  3. Clamp StartNanos to at most nowNanos - sampleNanos. The TSDB
//     rejects queries whose StartNanos lies inside the most recent
//     sample bucket.
func (a *SQLAdapter) clampBounds(q *eval.TimeSeriesQuery) {
	now := a.now
	if now == nil {
		now = timeutil.Now
	}
	nowNanos := now().UnixNano()
	sampleNanos := q.SampleNanos
	if sampleNanos == 0 {
		sampleNanos = Resolution10s.SampleDuration()
	}
	if q.EndNanos > nowNanos {
		q.EndNanos = nowNanos
	}
	if q.StartNanos > q.EndNanos {
		return
	}
	if floor := nowNanos - sampleNanos; q.StartNanos > floor {
		q.StartNanos = floor
	}
}

func countDatapoints(resp *tspb.TimeSeriesQueryResponse) int64 {
	var total int64
	for i := range resp.Results {
		total += int64(len(resp.Results[i].Datapoints))
	}
	return total
}

func markTooManyRows(err error) error {
	return errors.Mark(err, eval.ErrTooManyTimeSeriesRows)
}

// QueryTimeSeries implements eval.TimeSeriesQuerier. Dispatch:
//
//   - If q.Sources is non-empty AND q.SourceAggregator is the zero
//     value, issue one tspb.Query per listed source ("per-source
//     batch"). The result preserves per-source breakdown.
//   - Otherwise, issue a single tspb.Query that lets the TSDB
//     pipeline collapse sources via source_aggregator. Each row's
//     Source field is empty; the SQL layer renders NULL.
func (a *SQLAdapter) QueryTimeSeries(
	ctx context.Context, q eval.TimeSeriesQuery,
) ([]eval.TimeSeriesRow, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	a.clampBounds(&q)
	if q.StartNanos > q.EndNanos {
		return nil, nil
	}
	if len(q.Sources) > 0 && q.SourceAggregator == eval.AggregatorDefault {
		return a.queryPerSource(ctx, q)
	}
	return a.queryAggregate(ctx, q)
}

// queryAggregate dispatches a single tspb.Query that lets the TSDB
// pipeline collapse sources via source_aggregator. The returned rows
// have an empty Source field.
func (a *SQLAdapter) queryAggregate(
	ctx context.Context, q eval.TimeSeriesQuery,
) ([]eval.TimeSeriesRow, error) {
	tsQuery := tspb.Query{
		Name:    q.MetricName,
		Sources: q.Sources,
	}
	dsVal, dsHasValue, err := aggregatorToTSPB(q.Downsampler)
	if err != nil {
		return nil, err
	}
	if dsHasValue {
		tsQuery.Downsampler = dsVal.Enum()
	}
	saVal, saHasValue, err := aggregatorToTSPB(q.SourceAggregator)
	if err != nil {
		return nil, err
	}
	if saHasValue {
		tsQuery.SourceAggregator = saVal.Enum()
	}
	derivVal, derivHasValue, err := derivativeToTSPB(q.Derivative)
	if err != nil {
		return nil, err
	}
	if derivHasValue {
		tsQuery.Derivative = derivVal.Enum()
	}
	resp, err := a.query(ctx, &tspb.TimeSeriesQueryRequest{
		StartNanos:  q.StartNanos,
		EndNanos:    q.EndNanos,
		SampleNanos: q.SampleNanos,
		Queries:     []tspb.Query{tsQuery},
	})
	if err != nil {
		return nil, err
	}
	total := countDatapoints(resp)
	if q.MaxRows > 0 && total > q.MaxRows {
		return nil, markTooManyRows(errors.Newf(
			"returned %d rows exceeds row cap of %d", total, q.MaxRows))
	}
	rows := make([]eval.TimeSeriesRow, 0, total)
	for i := range resp.Results {
		for _, dp := range resp.Results[i].Datapoints {
			rows = append(rows, eval.TimeSeriesRow{
				TimestampNanos: dp.TimestampNanos,
				Value:          dp.Value,
			})
		}
	}
	return rows, nil
}

// queryPerSource dispatches one tspb.Query per listed source so the
// result preserves per-source breakdown. No discovery call: the
// user-provided source list drives dispatch.
func (a *SQLAdapter) queryPerSource(
	ctx context.Context, q eval.TimeSeriesQuery,
) ([]eval.TimeSeriesRow, error) {
	// Pre-check from an upper bound (sources × buckets) so an obvious
	// blowup never reaches the TSDB.
	if q.MaxRows > 0 {
		upper := upperBoundRows(q, len(q.Sources))
		if upper > q.MaxRows {
			return nil, markTooManyRows(errors.Newf(
				"upper-bound estimate of %d rows (%d sources × %d buckets) exceeds row cap of %d",
				upper, len(q.Sources), bucketsInWindow(q), q.MaxRows))
		}
	}
	dsVal, dsHasValue, err := aggregatorToTSPB(q.Downsampler)
	if err != nil {
		return nil, err
	}
	derivVal, derivHasValue, err := derivativeToTSPB(q.Derivative)
	if err != nil {
		return nil, err
	}
	queries := make([]tspb.Query, len(q.Sources))
	for i, src := range q.Sources {
		queries[i] = tspb.Query{
			Name:    q.MetricName,
			Sources: []string{src},
		}
		if dsHasValue {
			queries[i].Downsampler = dsVal.Enum()
		}
		if derivHasValue {
			queries[i].Derivative = derivVal.Enum()
		}
		// SourceAggregator left unset: with one source per query, it
		// would be a no-op.
	}
	resp, err := a.query(ctx, &tspb.TimeSeriesQueryRequest{
		StartNanos:  q.StartNanos,
		EndNanos:    q.EndNanos,
		SampleNanos: q.SampleNanos,
		Queries:     queries,
	})
	if err != nil {
		return nil, err
	}
	total := countDatapoints(resp)
	rows := make([]eval.TimeSeriesRow, 0, total)
	for i, result := range resp.Results {
		src := q.Sources[i]
		for _, dp := range result.Datapoints {
			rows = append(rows, eval.TimeSeriesRow{
				TimestampNanos: dp.TimestampNanos,
				Value:          dp.Value,
				Source:         src,
			})
		}
	}
	return rows, nil
}

// bucketsInWindow returns the number of sample buckets in the query
// window, rounding partial trailing buckets up.
func bucketsInWindow(q eval.TimeSeriesQuery) int64 {
	bucketNanos := q.SampleNanos
	if bucketNanos == 0 {
		bucketNanos = Resolution10s.SampleDuration()
	}
	windowNanos := q.EndNanos - q.StartNanos
	return (windowNanos + bucketNanos - 1) / bucketNanos
}

// upperBoundRows returns the maximum number of rows the per-source
// path could produce. Not every (source, bucket) pair has data, so
// over-rejection is the safe direction.
func upperBoundRows(q eval.TimeSeriesQuery, sourceCount int) int64 {
	return int64(sourceCount) * bucketsInWindow(q)
}

// aggregatorToTSPB translates an eval.Aggregator into its tspb
// counterpart. hasValue=false means the field should be omitted from
// tspb.Query so the proto-level default applies (AVG for downsampler,
// SUM for source_aggregator). An unknown enum returns an assertion
// error rather than silently defaulting, so a missing case here or a
// cast-from-int caller surfaces loudly.
func aggregatorToTSPB(
	a eval.Aggregator,
) (val tspb.TimeSeriesQueryAggregator, hasValue bool, err error) {
	switch a {
	case eval.AggregatorDefault:
		return 0, false, nil
	case eval.AggregatorAvg:
		return tspb.TimeSeriesQueryAggregator_AVG, true, nil
	case eval.AggregatorSum:
		return tspb.TimeSeriesQueryAggregator_SUM, true, nil
	case eval.AggregatorMax:
		return tspb.TimeSeriesQueryAggregator_MAX, true, nil
	case eval.AggregatorMin:
		return tspb.TimeSeriesQueryAggregator_MIN, true, nil
	case eval.AggregatorFirst:
		return tspb.TimeSeriesQueryAggregator_FIRST, true, nil
	case eval.AggregatorLast:
		return tspb.TimeSeriesQueryAggregator_LAST, true, nil
	case eval.AggregatorVariance:
		return tspb.TimeSeriesQueryAggregator_VARIANCE, true, nil
	default:
		return 0, false, errors.AssertionFailedf("unknown eval.Aggregator %d", int(a))
	}
}

// derivativeToTSPB translates an eval.Derivative into its tspb
// counterpart. See aggregatorToTSPB.
func derivativeToTSPB(
	d eval.Derivative,
) (val tspb.TimeSeriesQueryDerivative, hasValue bool, err error) {
	switch d {
	case eval.DerivativeDefault:
		return 0, false, nil
	case eval.DerivativeNone:
		return tspb.TimeSeriesQueryDerivative_NONE, true, nil
	case eval.DerivativeFirstOrder:
		return tspb.TimeSeriesQueryDerivative_DERIVATIVE, true, nil
	case eval.DerivativeNonNegativeFirstOrder:
		return tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE, true, nil
	default:
		return 0, false, errors.AssertionFailedf("unknown eval.Derivative %d", int(d))
	}
}
