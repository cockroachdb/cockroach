// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSQLAdapter exercises SQLAdapter against a stubbed queryFn so we
// can assert the two-phase (discover sources, then per-source batch)
// dispatch and the (source, timestamp) ordering of the returned rows
// without standing up a full TSDB. These behaviours are part of the
// sql.TimeSeriesQuerier contract that pkg/sql/crdb_internal.tsdb relies
// on.
func TestSQLAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const metric = "cr.node.sql.select.count"

	t.Run("two-phase dispatch and ordering", func(t *testing.T) {
		var calls []*tspb.TimeSeriesQueryRequest
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls = append(calls, req)
			// Discovery query: a single Query with no Sources. Return
			// the sources in non-sorted order to verify the adapter
			// sorts them before issuing the per-source batch.
			if len(req.Queries) == 1 && len(req.Queries[0].Sources) == 0 {
				return &tspb.TimeSeriesQueryResponse{
					Results: []tspb.TimeSeriesQueryResponse_Result{{
						Query: tspb.Query{Sources: []string{"3", "1", "2"}},
					}},
				}, nil
			}
			// Per-source batch: one Query per source, in sorted order.
			results := make([]tspb.TimeSeriesQueryResponse_Result, len(req.Queries))
			for i, q := range req.Queries {
				require.Len(t, q.Sources, 1,
					"per-source query must restrict to a single source")
				src := q.Sources[0]
				// Emit two datapoints per source so we can assert the
				// per-source datapoint order is preserved.
				results[i] = tspb.TimeSeriesQueryResponse_Result{
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: 100, Value: float64(100 + len(src))},
						{TimestampNanos: 200, Value: float64(200 + len(src))},
					},
				}
			}
			return &tspb.TimeSeriesQueryResponse{Results: results}, nil
		}

		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), sql.TimeSeriesQuery{
			MetricName: metric,
			StartNanos: 0,
			EndNanos:   1000,
		})
		require.NoError(t, err)

		// Two requests: discovery, then a single per-source batch.
		require.Len(t, calls, 2)
		require.Len(t, calls[0].Queries, 1)
		require.Empty(t, calls[0].Queries[0].Sources)
		require.Len(t, calls[1].Queries, 3)

		// The per-source batch must be ordered by source, matching the
		// ordering the adapter returns to its caller.
		gotSources := make([]string, 0, len(calls[1].Queries))
		for _, q := range calls[1].Queries {
			gotSources = append(gotSources, q.Sources[0])
		}
		require.Equal(t, []string{"1", "2", "3"}, gotSources)
		require.True(t, sort.StringsAreSorted(gotSources))

		// Six rows, ordered by (source, timestamp).
		require.Len(t, rows, 6)
		expected := []sql.TimeSeriesRow{
			{Source: "1", TimestampNanos: 100, Value: 101},
			{Source: "1", TimestampNanos: 200, Value: 201},
			{Source: "2", TimestampNanos: 100, Value: 101},
			{Source: "2", TimestampNanos: 200, Value: 201},
			{Source: "3", TimestampNanos: 100, Value: 101},
			{Source: "3", TimestampNanos: 200, Value: 201},
		}
		require.Equal(t, expected, rows)
	})

	t.Run("no sources skips per-source batch", func(t *testing.T) {
		var calls int
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls++
			// Empty Results means no sources reported in the window.
			return &tspb.TimeSeriesQueryResponse{}, nil
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), sql.TimeSeriesQuery{
			MetricName: metric,
		})
		require.NoError(t, err)
		require.Empty(t, rows)
		require.Equal(t, 1, calls,
			"adapter should not issue the per-source batch when discovery returned no sources")
	})

	// A failed discovery query must surface the error to the caller and
	// must not trigger the per-source batch.
	t.Run("discovery error propagates", func(t *testing.T) {
		discoveryErr := errors.New("boom from discovery")
		var calls int
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls++
			return nil, discoveryErr
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), sql.TimeSeriesQuery{
			MetricName: metric,
		})
		require.Nil(t, rows)
		require.True(t, errors.Is(err, discoveryErr),
			"caller should see the underlying discovery error")
		require.Equal(t, 1, calls,
			"adapter must not issue the per-source batch when discovery fails")
	})

	// A failed per-source batch must surface the error to the caller
	// rather than silently returning a partial result set.
	t.Run("per-source batch error propagates", func(t *testing.T) {
		batchErr := errors.New("boom from batch")
		var calls int
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls++
			if len(req.Queries) == 1 && len(req.Queries[0].Sources) == 0 {
				return &tspb.TimeSeriesQueryResponse{
					Results: []tspb.TimeSeriesQueryResponse_Result{{
						Query: tspb.Query{Sources: []string{"1", "2"}},
					}},
				}, nil
			}
			return nil, batchErr
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), sql.TimeSeriesQuery{
			MetricName: metric,
		})
		require.Nil(t, rows)
		require.True(t, errors.Is(err, batchErr),
			"caller should see the underlying batch error")
		require.Equal(t, 2, calls,
			"adapter should attempt the per-source batch exactly once")
	})

	// The adapter must propagate every source string verbatim, including
	// non-numeric sources (e.g. "<nodeID>-<storeID>"). Filtering of those
	// sources is the SQL caller's responsibility.
	t.Run("preserves all source strings verbatim", func(t *testing.T) {
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			if len(req.Queries) == 1 && len(req.Queries[0].Sources) == 0 {
				return &tspb.TimeSeriesQueryResponse{
					Results: []tspb.TimeSeriesQueryResponse_Result{{
						Query: tspb.Query{Sources: []string{"3", "abc", "1-2", "1"}},
					}},
				}, nil
			}
			results := make([]tspb.TimeSeriesQueryResponse_Result, len(req.Queries))
			for i, q := range req.Queries {
				results[i] = tspb.TimeSeriesQueryResponse_Result{
					Datapoints: []tspb.TimeSeriesDatapoint{{TimestampNanos: 100, Value: 1}},
				}
				_ = q
			}
			return &tspb.TimeSeriesQueryResponse{Results: results}, nil
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), sql.TimeSeriesQuery{
			MetricName: metric,
		})
		require.NoError(t, err)

		gotSources := make([]string, 0, len(rows))
		for _, r := range rows {
			gotSources = append(gotSources, r.Source)
		}
		// Sources are sorted lexicographically; non-numeric ones survive.
		require.Equal(t, []string{"1", "1-2", "3", "abc"}, gotSources)
	})

	// Concurrent QueryTimeSeries calls must not race on the adapter or
	// produce inconsistent results. This is a smoke test, not exhaustive
	// — run the package under -race to catch the failure mode of
	// interest.
	t.Run("concurrent calls are safe", func(t *testing.T) {
		var queryCount int64
		var mu sync.Mutex
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			atomic.AddInt64(&queryCount, 1)
			mu.Lock()
			defer mu.Unlock()
			if len(req.Queries) == 1 && len(req.Queries[0].Sources) == 0 {
				return &tspb.TimeSeriesQueryResponse{
					Results: []tspb.TimeSeriesQueryResponse_Result{{
						Query: tspb.Query{Sources: []string{"1", "2"}},
					}},
				}, nil
			}
			results := make([]tspb.TimeSeriesQueryResponse_Result, len(req.Queries))
			for i := range req.Queries {
				results[i] = tspb.TimeSeriesQueryResponse_Result{
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: 100, Value: 1},
						{TimestampNanos: 200, Value: 2},
					},
				}
			}
			return &tspb.TimeSeriesQueryResponse{Results: results}, nil
		}
		a := &SQLAdapter{query: stub}

		const goroutines = 8
		var wg sync.WaitGroup
		errCh := make(chan error, goroutines)
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rows, err := a.QueryTimeSeries(context.Background(), sql.TimeSeriesQuery{
					MetricName: metric,
				})
				if err != nil {
					errCh <- err
					return
				}
				if len(rows) != 4 {
					errCh <- errors.Newf("expected 4 rows, got %d", len(rows))
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}
		// Two stub calls per goroutine (discovery + batch).
		require.Equal(t, int64(goroutines*2), atomic.LoadInt64(&queryCount))
	})
}
