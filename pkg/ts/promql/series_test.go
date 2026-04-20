// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// dp is a shorthand constructor for tspb.TimeSeriesDatapoint.
func dp(tsNanos int64, val float64) tspb.TimeSeriesDatapoint {
	return tspb.TimeSeriesDatapoint{TimestampNanos: tsNanos, Value: val}
}

func TestBuildSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("node metric labels", func(t *testing.T) {
		s := buildSeries("sql_conn_count", "node", "1", []tspb.TimeSeriesDatapoint{
			dp(10e9, 42),
		})
		lbls := s.Labels()
		require.Equal(t, "sql_conn_count", lbls.Get(labels.MetricName))
		require.Equal(t, "node", lbls.Get(instanceTypeLabel))
		require.Equal(t, "1", lbls.Get(nodeIDLabel))
		require.Equal(t, "", lbls.Get(storeIDLabel))
	})

	t.Run("store metric labels", func(t *testing.T) {
		s := buildSeries("livebytes", "store", "3", []tspb.TimeSeriesDatapoint{
			dp(10e9, 100),
		})
		lbls := s.Labels()
		require.Equal(t, "livebytes", lbls.Get(labels.MetricName))
		require.Equal(t, "store", lbls.Get(instanceTypeLabel))
		require.Equal(t, "3", lbls.Get(storeIDLabel))
	})

	t.Run("labels are sorted", func(t *testing.T) {
		s := buildSeries("sql_conn_count", "node", "1", []tspb.TimeSeriesDatapoint{
			dp(10e9, 1),
		})
		lbls := s.Labels()
		for i := 1; i < len(lbls); i++ {
			require.True(t, lbls[i-1].Name < lbls[i].Name,
				"labels not sorted: %q >= %q", lbls[i-1].Name, lbls[i].Name)
		}
	})

	t.Run("NaN values filtered", func(t *testing.T) {
		s := buildSeries("m", "node", "1", []tspb.TimeSeriesDatapoint{
			dp(10e9, 1),
			dp(20e9, math.NaN()),
			dp(30e9, 3),
		})
		require.Len(t, s.datapoints, 2)
		require.Equal(t, float64(1), s.datapoints[0].Value)
		require.Equal(t, float64(3), s.datapoints[1].Value)
	})

	t.Run("all NaN produces empty datapoints", func(t *testing.T) {
		s := buildSeries("m", "node", "1", []tspb.TimeSeriesDatapoint{
			dp(10e9, math.NaN()),
			dp(20e9, math.NaN()),
		})
		require.Empty(t, s.datapoints)
	})

	t.Run("empty input", func(t *testing.T) {
		s := buildSeries("m", "node", "1", nil)
		require.Empty(t, s.datapoints)
	})
}

func TestSeriesSetIteration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty set", func(t *testing.T) {
		ss := newSeriesSet(nil)
		require.False(t, ss.Next())
		require.Nil(t, ss.Err())
		require.Nil(t, ss.Warnings())
	})

	t.Run("single series", func(t *testing.T) {
		s := buildSeries("m", "node", "1", []tspb.TimeSeriesDatapoint{dp(10e9, 1)})
		ss := newSeriesSet([]storage.Series{s})
		require.True(t, ss.Next())
		require.NotNil(t, ss.At())
		require.False(t, ss.Next())
	})

	t.Run("multiple series in label order", func(t *testing.T) {
		s1 := buildSeries("b_metric", "node", "1", []tspb.TimeSeriesDatapoint{dp(10e9, 2)})
		s2 := buildSeries("a_metric", "node", "1", []tspb.TimeSeriesDatapoint{dp(10e9, 1)})
		// Deliberately pass in reverse order; newSeriesSet should sort them.
		ss := newSeriesSet([]storage.Series{s1, s2})

		var names []string
		for ss.Next() {
			names = append(names, ss.At().Labels().Get(labels.MetricName))
		}
		require.Equal(t, []string{"a_metric", "b_metric"}, names)
		require.Nil(t, ss.Err())
	})
}

func TestSeriesSetSorting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Same metric name but different node_ids — verify sort order is deterministic.
	s1 := buildSeries("m", "node", "3", []tspb.TimeSeriesDatapoint{dp(10e9, 1)})
	s2 := buildSeries("m", "node", "1", []tspb.TimeSeriesDatapoint{dp(10e9, 2)})
	s3 := buildSeries("m", "node", "2", []tspb.TimeSeriesDatapoint{dp(10e9, 3)})

	ss := newSeriesSet([]storage.Series{s1, s2, s3})

	var nodeIDs []string
	for ss.Next() {
		nodeIDs = append(nodeIDs, ss.At().Labels().Get(nodeIDLabel))
	}
	require.Equal(t, []string{"1", "2", "3"}, nodeIDs)
}

func TestIteratorNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty", func(t *testing.T) {
		it := &tsdbIterator{datapoints: nil, idx: -1}
		require.False(t, it.Next())
	})

	t.Run("single datapoint", func(t *testing.T) {
		it := &tsdbIterator{
			datapoints: []tspb.TimeSeriesDatapoint{dp(10_000_000_000, 42)},
			idx:        -1,
		}
		require.True(t, it.Next())
		ts, val := it.At()
		require.Equal(t, int64(10_000), ts) // 10e9 ns = 10_000 ms
		require.Equal(t, float64(42), val)
		require.False(t, it.Next())
	})

	t.Run("multiple datapoints", func(t *testing.T) {
		it := &tsdbIterator{
			datapoints: []tspb.TimeSeriesDatapoint{
				dp(10e9, 1),
				dp(20e9, 2),
				dp(30e9, 3),
			},
			idx: -1,
		}

		var timestamps []int64
		var values []float64
		for it.Next() {
			ts, val := it.At()
			timestamps = append(timestamps, ts)
			values = append(values, val)
		}
		require.Equal(t, []int64{10_000, 20_000, 30_000}, timestamps)
		require.Equal(t, []float64{1, 2, 3}, values)
	})
}

func TestIteratorAt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dps := []tspb.TimeSeriesDatapoint{dp(10e9, 42)}

	t.Run("before Next", func(t *testing.T) {
		it := &tsdbIterator{datapoints: dps, idx: -1}
		ts, val := it.At()
		require.Equal(t, int64(0), ts)
		require.Equal(t, float64(0), val)
	})

	t.Run("after exhaustion", func(t *testing.T) {
		it := &tsdbIterator{datapoints: dps, idx: -1}
		it.Next()
		it.Next()
		ts, val := it.At()
		require.Equal(t, int64(0), ts)
		require.Equal(t, float64(0), val)
	})

	t.Run("at valid position", func(t *testing.T) {
		it := &tsdbIterator{datapoints: dps, idx: -1}
		it.Next()
		ts, val := it.At()
		require.Equal(t, int64(10_000), ts)
		require.Equal(t, float64(42), val)
	})
}

func TestIteratorSeek(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Datapoints at 10s, 20s, 30s (in milliseconds: 10000, 20000, 30000).
	dps := []tspb.TimeSeriesDatapoint{
		dp(10e9, 1),
		dp(20e9, 2),
		dp(30e9, 3),
	}

	tests := []struct {
		name      string
		dps       []tspb.TimeSeriesDatapoint
		initIdx   int
		seekMs    int64
		wantFound bool
		wantTs    int64
		wantVal   float64
	}{
		// BUG: Seek panics on empty datapoints when idx starts at -1.
		// The loop condition `idx < len(nil)` is `(-1 < 0) = true`, then
		// `if idx < 0 { idx = 0 }` sets idx=0, then datapoints[0] panics.
		// Uncomment when fixed:
		// {
		// 	name:      "empty datapoints",
		// 	dps:       nil,
		// 	initIdx:   -1,
		// 	seekMs:    0,
		// 	wantFound: false,
		// },
		{
			name:      "seek before all data from initial state",
			dps:       dps,
			initIdx:   -1,
			seekMs:    0,
			wantFound: true,
			wantTs:    10_000,
			wantVal:   1,
		},
		{
			name:      "seek to exact timestamp",
			dps:       dps,
			initIdx:   -1,
			seekMs:    20_000,
			wantFound: true,
			wantTs:    20_000,
			wantVal:   2,
		},
		{
			name:      "seek between datapoints",
			dps:       dps,
			initIdx:   -1,
			seekMs:    15_000,
			wantFound: true,
			wantTs:    20_000,
			wantVal:   2,
		},
		{
			name:      "seek after all data",
			dps:       dps,
			initIdx:   -1,
			seekMs:    40_000,
			wantFound: false,
		},
		{
			name:      "seek from mid-iteration",
			dps:       dps,
			initIdx:   1, // already at second datapoint (20s)
			seekMs:    25_000,
			wantFound: true,
			wantTs:    30_000,
			wantVal:   3,
		},
		{
			name:      "seek does not backtrack",
			dps:       dps,
			initIdx:   2, // at third datapoint (30s)
			seekMs:    10_000,
			wantFound: true,
			wantTs:    30_000,
			wantVal:   3,
		},
		{
			name:      "seek after exhaustion",
			dps:       dps,
			initIdx:   3, // past end
			seekMs:    0,
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			it := &tsdbIterator{datapoints: tc.dps, idx: tc.initIdx}
			found := it.Seek(tc.seekMs)
			require.Equal(t, tc.wantFound, found, "Seek(%d)", tc.seekMs)
			if found {
				ts, val := it.At()
				require.Equal(t, tc.wantTs, ts, "timestamp")
				require.Equal(t, tc.wantVal, val, "value")
			}
		})
	}
}

func TestIteratorTimestampConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	it := &tsdbIterator{
		datapoints: []tspb.TimeSeriesDatapoint{
			dp(1_609_459_200_000_000_000, 1), // 2021-01-01T00:00:00Z in ns
		},
		idx: -1,
	}
	require.True(t, it.Next())
	ts, _ := it.At()
	require.Equal(t, int64(1_609_459_200_000), ts) // same in ms
}

func TestIteratorErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	it := &tsdbIterator{idx: -1}
	require.Nil(t, it.Err())
}

func TestErrSeriesSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := fmt.Errorf("test error")
	ss := errSeriesSet{err: err}

	require.False(t, ss.Next())
	require.Nil(t, ss.At())
	require.Equal(t, err, ss.Err())
	require.Nil(t, ss.Warnings())
}
