// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// queryFn is the subset of the TSDB server Query API that SQLAdapter
// depends on. Both *Server (system tenant) and *TenantServer (secondary
// tenants) satisfy it.
type queryFn func(ctx context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error)

// SQLAdapter bridges the SQL layer to the TSDB. It implements
// sql.TimeSeriesQuerier on top of a Query function so that pkg/sql can
// query historical metrics without taking a direct dependency on pkg/ts.
//
// Each SQL-level query expands into two server queries:
//
//  1. A discovery query that runs the metric across all sources at a
//     coarse sample resolution. Its only purpose is to enumerate the
//     sources that reported data in the requested window — the
//     aggregated datapoints are discarded.
//  2. A per-source query batch that requests one tspb.Query per source
//     (with Sources restricted to that single source). This bypasses
//     source aggregation and yields raw per-source datapoints in a
//     single Query call.
//
// This avoids a per-source RPC round-trip while keeping the per-source
// data isolated. Sources are returned in sorted order, and within each
// source the TSDB returns datapoints chronologically — together
// satisfying the (source, timestamp) ordering contract documented on
// sql.TimeSeriesQuerier.
type SQLAdapter struct {
	query queryFn
}

var _ sql.TimeSeriesQuerier = (*SQLAdapter)(nil)

// NewSQLAdapter constructs an adapter that delegates to the supplied
// system-tenant TSDB server.
func NewSQLAdapter(server *Server) *SQLAdapter {
	return &SQLAdapter{query: server.Query}
}

// NewTenantSQLAdapter constructs an adapter that delegates to a
// secondary-tenant TSDB server. Tenant queries are scoped by the
// underlying TenantServer to that tenant's metrics only.
func NewTenantSQLAdapter(server *TenantServer) *SQLAdapter {
	return &SQLAdapter{query: server.Query}
}

// QueryTimeSeries implements sql.TimeSeriesQuerier.
func (a *SQLAdapter) QueryTimeSeries(
	ctx context.Context, q sql.TimeSeriesQuery,
) ([]sql.TimeSeriesRow, error) {
	sources, err := a.discoverSources(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, nil
	}

	queries := make([]tspb.Query, len(sources))
	for i, src := range sources {
		queries[i] = tspb.Query{
			Name:    q.MetricName,
			Sources: []string{src},
		}
	}
	resp, err := a.query(ctx, &tspb.TimeSeriesQueryRequest{
		StartNanos: q.StartNanos,
		EndNanos:   q.EndNanos,
		Queries:    queries,
	})
	if err != nil {
		return nil, err
	}

	// Pre-size from the per-source datapoint counts so we allocate
	// once for the typical case.
	var total int
	for i := range resp.Results {
		total += len(resp.Results[i].Datapoints)
	}
	rows := make([]sql.TimeSeriesRow, 0, total)
	for i, result := range resp.Results {
		src := sources[i]
		for _, dp := range result.Datapoints {
			rows = append(rows, sql.TimeSeriesRow{
				TimestampNanos: dp.TimestampNanos,
				Value:          dp.Value,
				Source:         src,
			})
		}
	}
	return rows, nil
}

// discoverSources runs an aggregated query across all sources to learn
// which sources reported data in the requested window. The returned
// slice is sorted to give the caller a stable iteration order.
func (a *SQLAdapter) discoverSources(ctx context.Context, q sql.TimeSeriesQuery) ([]string, error) {
	resp, err := a.query(ctx, &tspb.TimeSeriesQueryRequest{
		StartNanos: q.StartNanos,
		EndNanos:   q.EndNanos,
		Queries:    []tspb.Query{{Name: q.MetricName}},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Results) == 0 {
		return nil, nil
	}
	sources := append([]string(nil), resp.Results[0].Sources...)
	sort.Strings(sources)
	return sources, nil
}
