// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestAggregatorToTSPB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cases := []struct {
		name      string
		in        eval.Aggregator
		want      tspb.TimeSeriesQueryAggregator
		hasValue  bool
		expectErr bool
	}{
		{name: "default-unset", in: eval.AggregatorDefault, hasValue: false},
		{name: "avg", in: eval.AggregatorAvg, want: tspb.TimeSeriesQueryAggregator_AVG, hasValue: true},
		{name: "sum", in: eval.AggregatorSum, want: tspb.TimeSeriesQueryAggregator_SUM, hasValue: true},
		{name: "max", in: eval.AggregatorMax, want: tspb.TimeSeriesQueryAggregator_MAX, hasValue: true},
		{name: "min", in: eval.AggregatorMin, want: tspb.TimeSeriesQueryAggregator_MIN, hasValue: true},
		{name: "first", in: eval.AggregatorFirst, want: tspb.TimeSeriesQueryAggregator_FIRST, hasValue: true},
		{name: "last", in: eval.AggregatorLast, want: tspb.TimeSeriesQueryAggregator_LAST, hasValue: true},
		{name: "variance", in: eval.AggregatorVariance, want: tspb.TimeSeriesQueryAggregator_VARIANCE, hasValue: true},
		{name: "unknown-rejected", in: eval.Aggregator(999), expectErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, hasValue, err := aggregatorToTSPB(tc.in)
			if tc.expectErr {
				require.Error(t, err)
				require.True(t, errors.HasAssertionFailure(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.hasValue, hasValue)
			if hasValue {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestDerivativeToTSPB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cases := []struct {
		name      string
		in        eval.Derivative
		want      tspb.TimeSeriesQueryDerivative
		hasValue  bool
		expectErr bool
	}{
		{name: "default-unset", in: eval.DerivativeDefault, hasValue: false},
		{name: "none", in: eval.DerivativeNone, want: tspb.TimeSeriesQueryDerivative_NONE, hasValue: true},
		{name: "first-order", in: eval.DerivativeFirstOrder, want: tspb.TimeSeriesQueryDerivative_DERIVATIVE, hasValue: true},
		{name: "non-negative", in: eval.DerivativeNonNegativeFirstOrder, want: tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE, hasValue: true},
		{name: "unknown-rejected", in: eval.Derivative(999), expectErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, hasValue, err := derivativeToTSPB(tc.in)
			if tc.expectErr {
				require.Error(t, err)
				require.True(t, errors.HasAssertionFailure(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.hasValue, hasValue)
			if hasValue {
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestUpperBoundRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const tenSec = int64(10 * time.Second)
	const oneMin = int64(60 * time.Second)
	const oneHr = int64(60 * time.Minute)
	cases := []struct {
		name        string
		startNanos  int64
		endNanos    int64
		sampleNanos int64
		sourceCount int
		want        int64
	}{
		{"native-1-source-1hr", 0, oneHr, 0, 1, 360}, // 3600s / 10s
		{"native-3-sources-1hr", 0, oneHr, 0, 3, 3 * 360},
		{"1m-1-source-1hr", 0, oneHr, oneMin, 1, 60},                 // 60min / 1min
		{"1m-10-sources-1day", 0, 24 * oneHr, oneMin, 10, 10 * 1440}, // 1440min/day
		{"window-not-multiple-of-bucket-rounds-up", 0, 15 * int64(time.Second), tenSec, 1, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := eval.TimeSeriesQuery{
				StartNanos:  tc.startNanos,
				EndNanos:    tc.endNanos,
				SampleNanos: tc.sampleNanos,
			}
			got := upperBoundRows(q, tc.sourceCount)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestQueryTimeSeries_NoSources_SingleQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var seen *tspb.TimeSeriesQueryRequest
	adapter := &SQLAdapter{
		query: func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			seen = req
			return &tspb.TimeSeriesQueryResponse{
				Results: []tspb.TimeSeriesQueryResponse_Result{{
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: 1_000_000_000, Value: 42},
					},
				}},
			}, nil
		},
		now: func() time.Time { return timeutil.Unix(1000, 0) },
	}
	rows, err := adapter.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
		MetricName: "cr.node.foo",
		StartNanos: 0,
		EndNanos:   500_000_000_000,
	})
	require.NoError(t, err)
	require.Len(t, seen.Queries, 1)
	require.Empty(t, seen.Queries[0].Sources)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1_000_000_000), rows[0].TimestampNanos)
	require.Equal(t, "", rows[0].Source)
}

func TestQueryTimeSeries_NoSources_PropagatesDownsamplerAndDerivative(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var seen *tspb.TimeSeriesQueryRequest
	adapter := &SQLAdapter{
		query: func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			seen = req
			return &tspb.TimeSeriesQueryResponse{}, nil
		},
		now: func() time.Time { return timeutil.Unix(1000, 0) },
	}
	_, err := adapter.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
		MetricName:  "cr.node.foo",
		StartNanos:  0,
		EndNanos:    500_000_000_000,
		Downsampler: eval.AggregatorMax,
		SampleNanos: int64(time.Minute),
		Derivative:  eval.DerivativeNonNegativeFirstOrder,
	})
	require.NoError(t, err)
	require.Len(t, seen.Queries, 1)
	require.NotNil(t, seen.Queries[0].Downsampler)
	require.Equal(t, tspb.TimeSeriesQueryAggregator_MAX, *seen.Queries[0].Downsampler)
	require.NotNil(t, seen.Queries[0].Derivative)
	require.Equal(t, tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE, *seen.Queries[0].Derivative)
	require.Equal(t, int64(time.Minute), seen.SampleNanos)
}

func TestQueryTimeSeries_SourcesWithAggregator_CollapsedResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var seen *tspb.TimeSeriesQueryRequest
	adapter := &SQLAdapter{
		query: func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			seen = req
			return &tspb.TimeSeriesQueryResponse{
				Results: []tspb.TimeSeriesQueryResponse_Result{{
					Datapoints: []tspb.TimeSeriesDatapoint{{TimestampNanos: 0, Value: 99}},
				}},
			}, nil
		},
		now: func() time.Time { return timeutil.Unix(1000, 0) },
	}
	rows, err := adapter.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
		MetricName:       "cr.node.cpu",
		StartNanos:       0,
		EndNanos:         500_000_000_000,
		Sources:          []string{"1", "2", "3"},
		SourceAggregator: eval.AggregatorMax,
	})
	require.NoError(t, err)
	require.Len(t, seen.Queries, 1) // single query, NOT per-source
	require.Equal(t, []string{"1", "2", "3"}, seen.Queries[0].Sources)
	require.NotNil(t, seen.Queries[0].SourceAggregator)
	require.Equal(t, tspb.TimeSeriesQueryAggregator_MAX, *seen.Queries[0].SourceAggregator)
	require.Equal(t, "", rows[0].Source) // empty even though Sources was set
}

func TestQueryTimeSeries_PerSourceBreakdown_PreCheckTrips(t *testing.T) {
	defer leaktest.AfterTest(t)()
	adapter := &SQLAdapter{
		query: func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			t.Fatal("query should not be invoked when pre-check trips")
			return nil, nil
		},
		now: func() time.Time { return timeutil.Unix(1000, 0) },
	}
	_, err := adapter.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
		MetricName: "cr.node.foo",
		StartNanos: 0,
		EndNanos:   int64(time.Hour),
		Sources:    []string{"1", "2"},
		MaxRows:    10, // 2 sources × 360 buckets = 720 > 10
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, eval.ErrTooManyTimeSeriesRows))
}

func TestQueryTimeSeries_NoSources_NoPreCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// No source-count multiplier on this path; pre-check is skipped.
	called := false
	adapter := &SQLAdapter{
		query: func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			called = true
			return &tspb.TimeSeriesQueryResponse{}, nil
		},
		now: func() time.Time { return timeutil.Unix(1000, 0) },
	}
	_, err := adapter.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
		MetricName: "cr.node.foo",
		StartNanos: 0,
		EndNanos:   int64(time.Hour),
		MaxRows:    10,
	})
	require.NoError(t, err)
	require.True(t, called, "no-sources path skips pre-check; query should be invoked")
}

// TestSQLAdapter exercises SQLAdapter against a stubbed queryFn.
func TestSQLAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const metric = "cr.node.sql.select.count"

	t.Run("per-source breakdown: dispatch and ordering", func(t *testing.T) {
		var calls []*tspb.TimeSeriesQueryRequest
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls = append(calls, req)
			results := make([]tspb.TimeSeriesQueryResponse_Result, len(req.Queries))
			for i, q := range req.Queries {
				require.Len(t, q.Sources, 1,
					"per-source query must restrict to a single source")
				src := q.Sources[0]
				// Two datapoints per source so we can assert order.
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
		rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
			StartNanos: 0,
			EndNanos:   1000,
			Sources:    []string{"3", "1", "2"},
		})
		require.NoError(t, err)

		require.Len(t, calls, 1)
		require.Len(t, calls[0].Queries, 3)

		// User-provided source order is preserved.
		gotSources := make([]string, 0, len(calls[0].Queries))
		for _, q := range calls[0].Queries {
			gotSources = append(gotSources, q.Sources[0])
		}
		require.Equal(t, []string{"3", "1", "2"}, gotSources)

		require.Len(t, rows, 6)
		expected := []eval.TimeSeriesRow{
			{Source: "3", TimestampNanos: 100, Value: 101},
			{Source: "3", TimestampNanos: 200, Value: 201},
			{Source: "1", TimestampNanos: 100, Value: 101},
			{Source: "1", TimestampNanos: 200, Value: 201},
			{Source: "2", TimestampNanos: 100, Value: 101},
			{Source: "2", TimestampNanos: 200, Value: 201},
		}
		require.Equal(t, expected, rows)
	})

	t.Run("no sources: empty results", func(t *testing.T) {
		var calls int
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls++
			return &tspb.TimeSeriesQueryResponse{}, nil
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
		})
		require.NoError(t, err)
		require.Empty(t, rows)
		require.Equal(t, 1, calls, "no-sources path: single query call")
	})

	t.Run("no sources: error propagates", func(t *testing.T) {
		queryErr := errors.New("boom from query")
		var calls int
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls++
			return nil, queryErr
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
		})
		require.Nil(t, rows)
		require.True(t, errors.Is(err, queryErr))
		require.Equal(t, 1, calls)
	})

	t.Run("per-source breakdown: error propagates", func(t *testing.T) {
		batchErr := errors.New("boom from batch")
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			return nil, batchErr
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
			Sources:    []string{"1", "2"},
		})
		require.Nil(t, rows)
		require.True(t, errors.Is(err, batchErr))
	})

	// Pins the resp.Results[i] ↔ request.Queries[i] alignment the
	// adapter relies on, plus verbatim source propagation for
	// non-numeric sources (e.g. "<nodeID>-<storeID>"). The stub
	// embeds the source identity in the datapoint value so a
	// misalignment shows up as a wrong (source, value) pair.
	t.Run("per-source breakdown: preserves all source strings verbatim", func(t *testing.T) {
		sourceValue := func(src string) float64 {
			var v float64
			for i := 0; i < len(src); i++ {
				v = v*256 + float64(src[i])
			}
			return v
		}
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			results := make([]tspb.TimeSeriesQueryResponse_Result, len(req.Queries))
			for i, q := range req.Queries {
				require.Len(t, q.Sources, 1,
					"per-source query must restrict to a single source")
				results[i] = tspb.TimeSeriesQueryResponse_Result{
					Datapoints: []tspb.TimeSeriesDatapoint{{
						TimestampNanos: 100,
						Value:          sourceValue(q.Sources[0]),
					}},
				}
			}
			return &tspb.TimeSeriesQueryResponse{Results: results}, nil
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
			Sources:    []string{"3", "abc", "1-2", "1"},
		})
		require.NoError(t, err)

		gotSources := make([]string, 0, len(rows))
		for _, r := range rows {
			gotSources = append(gotSources, r.Source)
		}
		require.Equal(t, []string{"3", "abc", "1-2", "1"}, gotSources)
		for _, r := range rows {
			require.Equal(t, sourceValue(r.Source), r.Value,
				"row for source %q has wrong value; sources/results misaligned?", r.Source)
		}
	})

	// Smoke test for adapter concurrency safety; run under -race.
	t.Run("concurrent calls are safe", func(t *testing.T) {
		var queryCount int64
		var mu syncutil.Mutex
		stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			atomic.AddInt64(&queryCount, 1)
			mu.Lock()
			defer mu.Unlock()
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
				rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
					MetricName: metric,
					Sources:    []string{"1", "2"},
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
		// One stub call per goroutine.
		require.Equal(t, int64(goroutines), atomic.LoadInt64(&queryCount))
	})

	t.Run("per-source breakdown: MaxRows pre-check fires", func(t *testing.T) {
		var calls int
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			calls++
			t.Fatal("stub should not be called when pre-check trips")
			return nil, nil
		}
		a := &SQLAdapter{query: stub}
		// 3 sources × ceil(1h / 10s) = 3 × 360 = 1080 rows upper bound,
		// well above MaxRows = 100. Pre-check trips.
		_, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
			StartNanos: 0,
			EndNanos:   int64(time.Hour),
			Sources:    []string{"1", "2", "3"},
			MaxRows:    100,
		})
		require.True(t, errors.Is(err, eval.ErrTooManyTimeSeriesRows),
			"pre-check error must wrap eval.ErrTooManyTimeSeriesRows; got %v", err)
		require.Contains(t, err.Error(), "upper-bound estimate")
		require.Equal(t, 0, calls, "pre-check must prevent the TSDB call")
	})

	t.Run("MaxRows zero disables the cap", func(t *testing.T) {
		stub := func(_ context.Context, _ *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			return &tspb.TimeSeriesQueryResponse{
				Results: []tspb.TimeSeriesQueryResponse_Result{{
					Datapoints: []tspb.TimeSeriesDatapoint{
						{TimestampNanos: 0, Value: 1},
					},
				}},
			}, nil
		}
		a := &SQLAdapter{query: stub}
		rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
			MetricName: metric,
			StartNanos: 0,
			EndNanos:   int64(time.Hour),
			Sources:    []string{"1"},
			MaxRows:    0,
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
	})

	// Pins the time-clamping contract: EndNanos is clamped down to
	// now; a fully-future window short-circuits without a TSDB call;
	// StartNanos is clamped to nowNanos - sampleNanos so the TSDB
	// does not reject with "cannot query time series in the future".
	t.Run("time clamping", func(t *testing.T) {
		now := time.Unix(1_000_000, 0)

		for _, tc := range []struct {
			name              string
			startNanos        int64
			endNanos          int64
			sampleNanos       int64
			expectedDispatch  bool
			expectedSeenStart int64
			expectedSeenEnd   int64
		}{
			{
				name:              "end exceeding now is clamped to now",
				startNanos:        0,
				endNanos:          now.UnixNano() + int64(time.Hour),
				expectedDispatch:  true,
				expectedSeenStart: 0,
				expectedSeenEnd:   now.UnixNano(),
			},
			{
				name:             "future-only window short-circuits without dispatching",
				startNanos:       now.UnixNano() + int64(time.Hour),
				endNanos:         now.UnixNano() + 2*int64(time.Hour),
				expectedDispatch: false,
			},
			{
				name:              "fully-past window is left unmodified",
				startNanos:        100,
				endNanos:          200,
				expectedDispatch:  true,
				expectedSeenStart: 100,
				expectedSeenEnd:   200,
			},
			{
				// Start 5s before now is inside the 10s bucket the
				// TSDB rejects; clamp pulls it to now - 10s.
				name:              "native resolution: start inside most recent bucket is clamped to nowNanos - 10s",
				startNanos:        now.UnixNano() - int64(5*time.Second),
				endNanos:          now.UnixNano(),
				expectedDispatch:  true,
				expectedSeenStart: now.UnixNano() - Resolution10s.SampleDuration(),
				expectedSeenEnd:   now.UnixNano(),
			},
			{
				// Regression for hard-coded 10s headroom: a 1m
				// downsample requires a 60s floor, not 10s.
				name:              "interval=1m: start inside most recent bucket is clamped to nowNanos - 1m",
				startNanos:        now.UnixNano() - int64(30*time.Second),
				endNanos:          now.UnixNano(),
				sampleNanos:       int64(time.Minute),
				expectedDispatch:  true,
				expectedSeenStart: now.UnixNano() - int64(time.Minute),
				expectedSeenEnd:   now.UnixNano(),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var calls int
				var seenStart, seenEnd int64
				stub := func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
					calls++
					seenStart, seenEnd = req.StartNanos, req.EndNanos
					return &tspb.TimeSeriesQueryResponse{}, nil
				}
				a := &SQLAdapter{query: stub, now: func() time.Time { return now }}
				_, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
					MetricName:  metric,
					StartNanos:  tc.startNanos,
					EndNanos:    tc.endNanos,
					SampleNanos: tc.sampleNanos,
				})
				require.NoError(t, err)
				if !tc.expectedDispatch {
					require.Equal(t, 0, calls,
						"adapter must not dispatch when the window lies entirely past now")
					return
				}
				require.Equal(t, 1, calls,
					"adapter dispatched exactly one tspb.Query (no source filter)")
				require.Equal(t, tc.expectedSeenStart, seenStart, "StartNanos clamp")
				require.Equal(t, tc.expectedSeenEnd, seenEnd, "EndNanos clamp")
			})
		}
	})
}

// stubTenantConnector embeds kvtenant.Connector so the stub satisfies
// the type without implementing every method; only Query is
// exercised. Unimplemented methods nil-deref loudly if hit.
type stubTenantConnector struct {
	kvtenant.Connector
	queryFn func(context.Context, *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error)
}

func (s *stubTenantConnector) Query(
	ctx context.Context, req *tspb.TimeSeriesQueryRequest,
) (*tspb.TimeSeriesQueryResponse, error) {
	return s.queryFn(ctx, req)
}

// TestNewTenantSQLAdapter pins the secondary-tenant wiring:
// QueryTimeSeries must dispatch into TenantServer.Query (and from
// there into the supplied connector). The SQLAdapter unit tests use
// a struct literal with a stubbed query function, so they don't
// cover the bound-method-value path the production constructor uses.
func TestNewTenantSQLAdapter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const metricName = "cr.node.sql.select.count"
	tenantID := roachpb.MustMakeTenantID(123)

	// Route through the per-source path so we exercise the
	// per-source dispatch.
	var queries []*tspb.TimeSeriesQueryRequest
	stub := &stubTenantConnector{
		queryFn: func(_ context.Context, req *tspb.TimeSeriesQueryRequest) (*tspb.TimeSeriesQueryResponse, error) {
			queries = append(queries, req)
			results := make([]tspb.TimeSeriesQueryResponse_Result, len(req.Queries))
			for i := range req.Queries {
				results[i] = tspb.TimeSeriesQueryResponse_Result{
					Datapoints: []tspb.TimeSeriesDatapoint{{TimestampNanos: 100, Value: 1}},
				}
			}
			return &tspb.TimeSeriesQueryResponse{Results: results}, nil
		},
	}

	// Empty registry: tenant-tagging is server.go's contract, not
	// the adapter's.
	server := &TenantServer{
		tenantID:       tenantID,
		tenantConnect:  stub,
		tenantRegistry: metric.NewRegistry(),
	}

	a := NewTenantSQLAdapter(server)
	require.NotNil(t, a, "NewTenantSQLAdapter must not return nil")

	now := time.Unix(1_000_000, 0)
	a.now = func() time.Time { return now }
	rows, err := a.QueryTimeSeries(context.Background(), eval.TimeSeriesQuery{
		MetricName: metricName,
		StartNanos: 0,
		EndNanos:   now.UnixNano(),
		Sources:    []string{"1-123"}, // per-source batch
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "1-123", rows[0].Source)

	// Confirms the wrapped TenantServer.Query is what the adapter
	// calls, not some unrelated bound method.
	require.Len(t, queries, 1)
	require.Len(t, queries[0].Queries, 1)
	require.Equal(t, []string{"1-123"}, queries[0].Queries[0].Sources,
		"per-source batch must be keyed by the user-supplied source")
}
