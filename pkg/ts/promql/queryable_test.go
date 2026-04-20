// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// mockTSDBServer implements tspb.TimeSeriesServer for testing.
type mockTSDBServer struct {
	tspb.TimeSeriesServer
	queryFn func(context.Context, *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error)
	calls   []*tspb.TimeSeriesQueryRequest
}

func (m *mockTSDBServer) Query(
	ctx context.Context, req *tspb.TimeSeriesQueryRequest,
) (*tspb.TimeSeriesQueryResponse, error) {
	m.calls = append(m.calls, req)
	if m.queryFn != nil {
		return m.queryFn(ctx, req)
	}
	return &tspb.TimeSeriesQueryResponse{}, nil
}

// staticSourceLister is a simple SourceLister for testing.
type staticSourceLister struct {
	nodes  []string
	stores []string
}

func (s *staticSourceLister) NodeSources() []string  { return s.nodes }
func (s *staticSourceLister) StoreSources() []string { return s.stores }

// testQueryable builds a TSDBQueryable with a small catalog and the given mock.
func testQueryable(mock *mockTSDBServer) (*TSDBQueryable, *MetricCatalog) {
	catalog := NewMetricCatalog(
		map[string]string{
			"sql.conn.count":  "cr.node.sql.conn.count",
			"sql.query.count": "cr.node.sql.query.count",
			"livebytes":       "cr.store.livebytes",
			"rebalancing":     "cr.cluster.rebalancing",
		},
		nil, /* allMetadata */
	)
	sources := &staticSourceLister{
		nodes:  []string{"1", "2", "3"},
		stores: []string{"1", "2"},
	}
	q := NewTSDBQueryable(mock, catalog, sources)
	return q, catalog
}

// withData returns a queryFn that returns the given datapoints for every query.
func withData(
	dps ...tspb.TimeSeriesDatapoint,
) func(context.Context, *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
	return func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
		return &tspb.TimeSeriesQueryResponse{
			Results: []tspb.TimeSeriesQueryResponse_Result{
				{Datapoints: dps},
			},
		}, nil
	}
}

func TestQuerierSelect_MatchEqual(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 42))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000) // 0-60s in ms
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "sql_conn_count"),
	)

	// Iterate through all results.
	var count int
	for ss.Next() {
		count++
		lbls := ss.At().Labels()
		require.Equal(t, "sql_conn_count", lbls.Get(labels.MetricName))
	}
	require.NoError(t, ss.Err())
	// One series per node source (3 nodes).
	require.Equal(t, 3, count)

	// Verify TSDB requests had the correct metric name.
	require.Len(t, mock.calls, 3)
	for _, call := range mock.calls {
		require.Equal(t, "cr.node.sql.conn.count", call.Queries[0].Name)
	}
}

func TestQuerierSelect_MatchRegexp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	// Match all sql_ metrics.
	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "sql_.*"),
	)

	metricNames := map[string]bool{}
	for ss.Next() {
		metricNames[ss.At().Labels().Get(labels.MetricName)] = true
	}
	require.NoError(t, ss.Err())
	require.True(t, metricNames["sql_conn_count"])
	require.True(t, metricNames["sql_query_count"])
	require.False(t, metricNames["livebytes"])
}

func TestQuerierSelect_NoNameMatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	// No __name__ matcher — should query all metrics.
	ss := querier.Select(false, nil)

	metricNames := map[string]bool{}
	for ss.Next() {
		metricNames[ss.At().Labels().Get(labels.MetricName)] = true
	}
	require.NoError(t, ss.Err())
	require.True(t, len(metricNames) >= 4, "expected at least 4 metrics, got %d", len(metricNames))
}

func TestQuerierSelect_UnknownMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "nonexistent"),
	)
	require.False(t, ss.Next())
	require.NoError(t, ss.Err())
	// No TSDB queries should have been issued.
	require.Empty(t, mock.calls)
}

func TestQuerierSelect_SourceFiltering_NodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "sql_conn_count"),
		labels.MustNewMatcher(labels.MatchEqual, nodeIDLabel, "2"),
	)

	var count int
	for ss.Next() {
		require.Equal(t, "2", ss.At().Labels().Get(nodeIDLabel))
		count++
	}
	require.NoError(t, ss.Err())
	require.Equal(t, 1, count, "should query only one source")
	require.Len(t, mock.calls, 1)
	require.Equal(t, []string{"2"}, mock.calls[0].Queries[0].Sources)
}

func TestQuerierSelect_SourceFiltering_StoreID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "livebytes"),
		labels.MustNewMatcher(labels.MatchEqual, storeIDLabel, "1"),
	)

	var count int
	for ss.Next() {
		require.Equal(t, "1", ss.At().Labels().Get(storeIDLabel))
		count++
	}
	require.NoError(t, ss.Err())
	require.Equal(t, 1, count)
	require.Len(t, mock.calls, 1)
}

// TestQuerierSelect_SourceFiltering_RegexIgnored documents that regex matchers
// on node_id/store_id are silently ignored. When fixed, this test should verify
// that only the matched sources are queried.
func TestQuerierSelect_SourceFiltering_RegexIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	// BUG: regex matcher on node_id is ignored — all 3 nodes are queried.
	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "sql_conn_count"),
		labels.MustNewMatcher(labels.MatchRegexp, nodeIDLabel, "1|2"),
	)

	var count int
	for ss.Next() {
		count++
	}
	require.NoError(t, ss.Err())
	// Current (broken) behavior: all 3 sources queried, not just 2.
	require.Equal(t, 3, count, "BUG: regex matcher on node_id is ignored, all sources queried")
}

func TestQuerierSelect_ClusterMetricSource(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "rebalancing"),
	)

	var count int
	for ss.Next() {
		count++
	}
	require.NoError(t, ss.Err())
	// Cluster metrics are hardcoded to source "1".
	require.Equal(t, 1, count)
	require.Len(t, mock.calls, 1)
	require.Equal(t, []string{"1"}, mock.calls[0].Queries[0].Sources)
}

// TestSamplePeriodComputation documents the sample period truncation behavior.
// The comment in queryable.go says "round up to nearest multiple of 10s" but
// the code truncates. E.g., 15s step → 10s sample period, not 20s.
func TestSamplePeriodComputation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name                string
		stepMs              int64
		expectedSampleNanos int64
	}{
		{name: "no hint", stepMs: 0, expectedSampleNanos: int64(10e9)},
		{name: "5s below default", stepMs: 5_000, expectedSampleNanos: int64(10e9)},
		{name: "10s exact", stepMs: 10_000, expectedSampleNanos: int64(10e9)},
		// BUG: 15s should round up to 20s, but truncates to 10s.
		{name: "15s truncates to 10s", stepMs: 15_000, expectedSampleNanos: int64(10e9)},
		{name: "20s exact", stepMs: 20_000, expectedSampleNanos: int64(20e9)},
		// BUG: 25s should round up to 30s, but truncates to 20s.
		{name: "25s truncates to 20s", stepMs: 25_000, expectedSampleNanos: int64(20e9)},
		{name: "30s exact", stepMs: 30_000, expectedSampleNanos: int64(30e9)},
		{name: "60s exact", stepMs: 60_000, expectedSampleNanos: int64(60e9)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockTSDBServer{queryFn: withData(dp(10e9, 1))}
			q, _ := testQueryable(mock)

			querier, err := q.Querier(context.Background(), 0, 60_000)
			require.NoError(t, err)

			var hints *storage.SelectHints
			if tc.stepMs > 0 {
				hints = &storage.SelectHints{Step: tc.stepMs}
			}

			ss := querier.Select(
				false, hints,
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "rebalancing"),
			)
			for ss.Next() {
			}
			require.NoError(t, ss.Err())

			require.NotEmpty(t, mock.calls, "expected at least one TSDB call")
			require.Equal(t, tc.expectedSampleNanos, mock.calls[0].SampleNanos,
				"step %dms → sampleNanos", tc.stepMs)
		})
	}
}

func TestExtractSourceFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)
	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)
	tq := querier.(*tsdbQuerier)

	tests := []struct {
		name             string
		matchers         []*labels.Matcher
		expectedNodeIDs  []string
		expectedStoreIDs []string
	}{
		{
			name:            "node_id equal",
			matchers:        []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, nodeIDLabel, "1")},
			expectedNodeIDs: []string{"1"},
		},
		{
			name:             "store_id equal",
			matchers:         []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, storeIDLabel, "5")},
			expectedStoreIDs: []string{"5"},
		},
		{
			name: "both node and store",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, nodeIDLabel, "1"),
				labels.MustNewMatcher(labels.MatchEqual, storeIDLabel, "5"),
			},
			expectedNodeIDs:  []string{"1"},
			expectedStoreIDs: []string{"5"},
		},
		{
			// BUG: regex matchers are ignored.
			name:     "regex ignored",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, nodeIDLabel, "1|2")},
		},
		{
			// BUG: not-equal matchers are ignored.
			name:     "not-equal ignored",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, nodeIDLabel, "3")},
		},
		{
			name: "no source matchers",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sf := tq.extractSourceFilter(tc.matchers)
			require.Equal(t, tc.expectedNodeIDs, sf.nodeIDs)
			require.Equal(t, tc.expectedStoreIDs, sf.storeIDs)
		})
	}
}

func TestSourcesForMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)
	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)
	tq := querier.(*tsdbQuerier)

	t.Run("node no filter", func(t *testing.T) {
		sources := tq.sourcesForMetric("node", sourceFilter{})
		require.Equal(t, []string{"1", "2", "3"}, sources)
		require.True(t, sort.StringsAreSorted(sources))
	})

	t.Run("node with filter", func(t *testing.T) {
		sources := tq.sourcesForMetric("node", sourceFilter{nodeIDs: []string{"2"}})
		require.Equal(t, []string{"2"}, sources)
	})

	t.Run("store no filter", func(t *testing.T) {
		sources := tq.sourcesForMetric("store", sourceFilter{})
		require.Equal(t, []string{"1", "2"}, sources)
		require.True(t, sort.StringsAreSorted(sources))
	})

	t.Run("store with filter", func(t *testing.T) {
		sources := tq.sourcesForMetric("store", sourceFilter{storeIDs: []string{"1"}})
		require.Equal(t, []string{"1"}, sources)
	})

	t.Run("cluster always returns 1", func(t *testing.T) {
		sources := tq.sourcesForMetric("cluster", sourceFilter{})
		require.Equal(t, []string{"1"}, sources)
	})
}

func TestMatchingMetricNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)
	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)
	tq := querier.(*tsdbQuerier)

	t.Run("MatchEqual existing", func(t *testing.T) {
		names := tq.matchingMetricNames([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "sql_conn_count"),
		})
		require.Equal(t, []string{"sql_conn_count"}, names)
	})

	t.Run("MatchEqual nonexistent", func(t *testing.T) {
		names := tq.matchingMetricNames([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "nonexistent"),
		})
		require.Nil(t, names)
	})

	t.Run("MatchRegexp", func(t *testing.T) {
		names := tq.matchingMetricNames([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "sql_.*"),
		})
		require.NotEmpty(t, names)
		for _, n := range names {
			require.Regexp(t, `^sql_`, n)
		}
	})

	t.Run("MatchNotEqual", func(t *testing.T) {
		names := tq.matchingMetricNames([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, "livebytes"),
		})
		require.NotEmpty(t, names)
		require.NotContains(t, names, "livebytes")
	})

	t.Run("no __name__ matcher", func(t *testing.T) {
		names := tq.matchingMetricNames([]*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "other_label", "val"),
		})
		// Should return all metric names.
		require.True(t, len(names) >= 4)
	})
}

func TestQuerierSelect_TSDBError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{
		queryFn: func(context.Context, *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "sql_conn_count"),
	)
	require.False(t, ss.Next())
	require.Error(t, ss.Err())
	require.Contains(t, ss.Err().Error(), "connection refused")
}

func TestQuerierSelect_EmptyResults(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TSDB returns empty response (no results at all).
	mock := &mockTSDBServer{
		queryFn: func(context.Context, *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			return &tspb.TimeSeriesQueryResponse{}, nil
		},
	}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	ss := querier.Select(
		false, nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "sql_conn_count"),
	)
	// Should be empty but not error.
	require.False(t, ss.Next())
	require.NoError(t, ss.Err())
}

func TestQuerierLabelValues(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	t.Run("__name__", func(t *testing.T) {
		vals, warnings, err := querier.LabelValues(labels.MetricName)
		require.NoError(t, err)
		require.Nil(t, warnings)
		require.Contains(t, vals, "sql_conn_count")
		require.Contains(t, vals, "livebytes")
	})

	t.Run("instance_type", func(t *testing.T) {
		vals, _, err := querier.LabelValues(instanceTypeLabel)
		require.NoError(t, err)
		require.Equal(t, []string{"cluster", "node", "store"}, vals)
	})

	t.Run("node_id", func(t *testing.T) {
		vals, _, err := querier.LabelValues(nodeIDLabel)
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3"}, vals)
	})

	t.Run("store_id", func(t *testing.T) {
		vals, _, err := querier.LabelValues(storeIDLabel)
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2"}, vals)
	})

	t.Run("unknown label", func(t *testing.T) {
		vals, _, err := querier.LabelValues("unknown")
		require.NoError(t, err)
		require.Nil(t, vals)
	})
}

func TestQuerierLabelNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)

	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)

	names, warnings, err := querier.LabelNames()
	require.NoError(t, err)
	require.Nil(t, warnings)
	require.Equal(t, []string{
		labels.MetricName,
		instanceTypeLabel,
		nodeIDLabel,
		storeIDLabel,
	}, names)
}

func TestQuerierClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mock := &mockTSDBServer{}
	q, _ := testQueryable(mock)
	querier, err := q.Querier(context.Background(), 0, 60_000)
	require.NoError(t, err)
	require.NoError(t, querier.Close())
}
