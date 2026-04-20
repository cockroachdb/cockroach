// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// SourceLister provides the set of source IDs available for node-level and
// store-level metrics. The queryable uses these to issue per-source TSDB
// queries, returning individual Prometheus series per source.
type SourceLister interface {
	// NodeSources returns source IDs for node-level metrics (e.g. ["1", "2"]).
	NodeSources() []string
	// StoreSources returns source IDs for store-level metrics (e.g. ["1", "2"]).
	StoreSources() []string
}

// TSDBQueryable implements storage.Queryable, bridging the Prometheus PromQL
// engine to CockroachDB's internal time series database. Each PromQL query
// creates a Querier that translates label matchers into TSDB queries.
type TSDBQueryable struct {
	server  tspb.TimeSeriesServer
	catalog *MetricCatalog
	sources SourceLister
}

var _ storage.Queryable = (*TSDBQueryable)(nil)

// NewTSDBQueryable creates a new TSDBQueryable.
func NewTSDBQueryable(
	server tspb.TimeSeriesServer, catalog *MetricCatalog, sources SourceLister,
) *TSDBQueryable {
	return &TSDBQueryable{
		server:  server,
		catalog: catalog,
		sources: sources,
	}
}

// Querier returns a new Querier scoped to the given time range. Timestamps
// are in milliseconds (Prometheus convention).
func (q *TSDBQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &tsdbQuerier{
		ctx:     ctx,
		mint:    mint,
		maxt:    maxt,
		server:  q.server,
		catalog: q.catalog,
		sources: q.sources,
	}, nil
}

// tsdbQuerier implements storage.Querier, translating Prometheus label
// matchers into TSDB queries and converting the results into Prometheus
// series.
type tsdbQuerier struct {
	ctx     context.Context
	mint    int64 // milliseconds
	maxt    int64 // milliseconds
	server  tspb.TimeSeriesServer
	catalog *MetricCatalog
	sources SourceLister
}

var _ storage.Querier = (*tsdbQuerier)(nil)

// Select translates Prometheus label matchers into TSDB queries. It:
//  1. Extracts the __name__ matcher to find the TSDB metric name(s).
//  2. Extracts node_id/store_id matchers to filter sources.
//  3. Issues one TSDB query per source to get per-source series.
//  4. Wraps results as a storage.SeriesSet.
//
// The PromQL engine handles all aggregation, rate computation, and function
// evaluation on top of the raw series returned here.
func (q *tsdbQuerier) Select(
	sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher,
) storage.SeriesSet {
	// Find matching metric names.
	promNames := q.matchingMetricNames(matchers)
	if len(promNames) == 0 {
		return storage.EmptySeriesSet()
	}

	// Extract source filters from matchers.
	sourceFilter := q.extractSourceFilter(matchers)

	// Determine sample period from hints.
	sampleNanos := int64(10e9) // default 10s
	if hints != nil && hints.Step > 0 {
		stepNanos := hints.Step * 1e6
		// Round up to nearest multiple of 10s.
		if stepNanos > sampleNanos {
			sampleNanos = (stepNanos / int64(10e9)) * int64(10e9)
			if sampleNanos == 0 {
				sampleNanos = int64(10e9)
			}
		}
	}

	var allSeries []storage.Series

	for _, promName := range promNames {
		tsdbName, ok := q.catalog.TSDBName(promName)
		if !ok {
			continue
		}
		instType := InstanceType(tsdbName)
		sources := q.sourcesForMetric(instType, sourceFilter)

		for _, source := range sources {
			series, err := q.querySource(
				promName, tsdbName, instType, source, sampleNanos,
			)
			if err != nil {
				return errSeriesSet{err: err}
			}
			if series != nil {
				allSeries = append(allSeries, series)
			}
		}
	}

	return newSeriesSet(allSeries)
}

// querySource issues a single TSDB query for one metric and one source,
// returning a tsdbSeries or nil if no data exists.
func (q *tsdbQuerier) querySource(
	promName, tsdbName, instType, source string, sampleNanos int64,
) (*tsdbSeries, error) {
	req := &tspb.TimeSeriesQueryRequest{
		StartNanos:  q.mint * 1e6, // ms -> ns
		EndNanos:    q.maxt * 1e6,
		SampleNanos: sampleNanos,
		Queries: []tspb.Query{
			{
				Name:    tsdbName,
				Sources: []string{source},
			},
		},
	}

	resp, err := q.server.Query(q.ctx, req)
	if err != nil {
		return nil, fmt.Errorf("tsdb query for %s source %s: %w", tsdbName, source, err)
	}

	if len(resp.Results) == 0 || len(resp.Results[0].Datapoints) == 0 {
		return nil, nil
	}

	return buildSeries(promName, instType, source, resp.Results[0].Datapoints), nil
}

// matchingMetricNames returns all Prometheus metric names that match the
// __name__ matcher(s) in the provided matchers list. If no __name__ matcher
// is present, all metric names are returned.
func (q *tsdbQuerier) matchingMetricNames(matchers []*labels.Matcher) []string {
	for _, m := range matchers {
		if m.Name != labels.MetricName {
			continue
		}
		switch m.Type {
		case labels.MatchEqual:
			if _, ok := q.catalog.TSDBName(m.Value); ok {
				return []string{m.Value}
			}
			return nil
		case labels.MatchRegexp, labels.MatchNotEqual, labels.MatchNotRegexp:
			return q.catalog.MatchPromNames(m.Matches)
		}
	}
	// No __name__ matcher: return all names (unusual but valid PromQL).
	return q.catalog.AllPromNames()
}

// sourceFilter holds extracted source constraints from label matchers.
type sourceFilter struct {
	nodeIDs  []string // non-nil means filter to these node IDs
	storeIDs []string // non-nil means filter to these store IDs
}

// extractSourceFilter examines matchers for node_id and store_id labels
// and returns any explicit source constraints.
func (q *tsdbQuerier) extractSourceFilter(matchers []*labels.Matcher) sourceFilter {
	var sf sourceFilter
	for _, m := range matchers {
		switch m.Name {
		case nodeIDLabel:
			if m.Type == labels.MatchEqual {
				sf.nodeIDs = []string{m.Value}
			}
		case storeIDLabel:
			if m.Type == labels.MatchEqual {
				sf.storeIDs = []string{m.Value}
			}
		}
	}
	return sf
}

// sourcesForMetric returns the list of source IDs to query for a metric of the
// given instance type, respecting any source filters.
func (q *tsdbQuerier) sourcesForMetric(instType string, sf sourceFilter) []string {
	switch instType {
	case "store":
		if sf.storeIDs != nil {
			return sf.storeIDs
		}
		sources := q.sources.StoreSources()
		sort.Strings(sources)
		return sources
	case "cluster":
		// Cluster metrics typically have a single source "0" or "1".
		return []string{"1"}
	default: // "node"
		if sf.nodeIDs != nil {
			return sf.nodeIDs
		}
		sources := q.sources.NodeSources()
		sort.Strings(sources)
		return sources
	}
}

// LabelValues returns all known values for a label name.
func (q *tsdbQuerier) LabelValues(
	name string, matchers ...*labels.Matcher,
) ([]string, storage.Warnings, error) {
	switch name {
	case labels.MetricName:
		return q.catalog.AllPromNames(), nil, nil
	case instanceTypeLabel:
		return []string{"cluster", "node", "store"}, nil, nil
	case nodeIDLabel:
		sources := q.sources.NodeSources()
		sort.Strings(sources)
		return sources, nil, nil
	case storeIDLabel:
		sources := q.sources.StoreSources()
		sort.Strings(sources)
		return sources, nil, nil
	default:
		return nil, nil, nil
	}
}

// LabelNames returns all known label names.
func (q *tsdbQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return []string{
		labels.MetricName,
		instanceTypeLabel,
		nodeIDLabel,
		storeIDLabel,
	}, nil, nil
}

// Close is a no-op.
func (q *tsdbQuerier) Close() error {
	return nil
}
