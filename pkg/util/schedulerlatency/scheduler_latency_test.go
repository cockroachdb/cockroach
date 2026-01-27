// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulerlatency

import (
	"context"
	"fmt"
	"math"
	"runtime/metrics"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSchedulerLatencySampler is an integration test for the scheduler latency
// sampler -- it verifies that scheduling latencies are measured, registered
// callbacks are invoked, and that the prometheus metrics emitted are non-empty.
func TestSchedulerLatencySampler(t *testing.T) {
	skip.UnderStress(t)
	skip.UnderShort(t)

	ctx, cancel := context.WithCancel(context.Background())

	st := cluster.MakeTestingClusterSettings()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	const n = 500 // start 500 goroutines that run until ctx cancellation
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()

			a := 1
			for x := 0; x >= 0; x++ {
				a = a*13 + x
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}(i)
	}

	mu := testListener{}

	reg := metric.NewRegistry()
	require.NoError(t, StartSampler(ctx, st, stopper, reg, 10*time.Second, &mu))
	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()
		if mu.p99.Nanoseconds() == 0 {
			return fmt.Errorf("expected non-zero p99 scheduling latency")
		}

		var err error
		reg.Each(func(name string, mtr interface{}) {
			wh := mtr.(metric.WindowedHistogram)
			windowSnapshot := wh.WindowedSnapshot()
			avg := windowSnapshot.Mean()
			if math.IsNaN(avg) || math.IsInf(avg, +1) || math.IsInf(avg, -1) {
				avg = 0
			}

			if windowSnapshot.ValueAtQuantile(99) == 0 || avg == 0 {
				err = fmt.Errorf("expected non-zero p99 scheduling latency metrics")
			}
		})
		return err
	})

	if mu.err != nil {
		t.Fatal(mu.err)
	}
}

type testListener struct {
	syncutil.Mutex
	err error
	p99 time.Duration
}

func (l *testListener) SchedulerLatency(p99 time.Duration, period time.Duration) {
	l.Lock()
	defer l.Unlock()
	if samplePeriod.Default() != period {
		l.err = errors.CombineErrors(l.err, errors.Newf("mismatch: expected %v, got %v", samplePeriod.Default(), period))
	}
	l.p99 = p99
}

func TestComputeSchedulerPercentile(t *testing.T) {
	{
		//	  ▲
		//	8 │               ┌───┐
		//	7 │           ┌───┤   │
		//	6 │           │   │   ├───┐
		//	5 │       ┌───┤   │   │   ├───┐       ┌───┐
		//	4 │       │   │   │   │   │   ├───┐   │   │
		//	3 │   ┌───┤   │   │   │   │   │   ├───┤   │
		//	2 │   │   │   │   │   │   │   │   │   │   │
		//	1 ├───┤   │   │   │   │   │   │   │   │   │
		//	  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴─────────────▶
		//	     10  20  30  40  50  60  70  80  90  100 110 120 130
		hist := metrics.Float64Histogram{
			Counts:  []uint64{1, 3, 5, 7, 8, 6, 5, 4, 3, 5, 0, 0, 0},
			Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130},
		}

		require.InDelta(t, 100.0, percentile(&hist, 1.00), 0.001)  // pmax
		require.InDelta(t, 0.0, percentile(&hist, 0.00), 0.001)    // pmin
		require.InDelta(t, 49.375, percentile(&hist, 0.50), 0.001) // p50
		require.InDelta(t, 70.625, percentile(&hist, 0.75), 0.001) // p75
		require.InDelta(t, 90.600, percentile(&hist, 0.90), 0.001) // p90
		require.InDelta(t, 99.060, percentile(&hist, 0.99), 0.001) // p99
	}

	{
		hist := metrics.Float64Histogram{
			Counts:  []uint64{100, 50},
			Buckets: []float64{math.Inf(-1), 10, math.Inf(+1)},
		}
		require.Equal(t, 10.0, percentile(&hist, 1.00)) // pmax
		require.Equal(t, 10.0, percentile(&hist, 0.00)) // pmin
		require.Equal(t, 10.0, percentile(&hist, 0.50)) // p50
		require.Equal(t, 10.0, percentile(&hist, 0.75)) // p75
		require.Equal(t, 10.0, percentile(&hist, 0.90)) // p90
		require.Equal(t, 10.0, percentile(&hist, 0.99)) // p99
	}

	{
		hist := metrics.Float64Histogram{
			Counts:  []uint64{100},
			Buckets: []float64{math.Inf(-1), math.Inf(+1)},
		}
		require.Equal(t, 00.0, percentile(&hist, 1.00)) // pmax
		require.Equal(t, 00.0, percentile(&hist, 0.00)) // pmin
		require.Equal(t, 00.0, percentile(&hist, 0.50)) // p50
	}
}

func TestComputeSchedulerPercentileAgainstPrometheus(t *testing.T) {
	{
		//	  ▲
		//	8 │               ┌───┐
		//	7 │           ┌───┤   │
		//	6 │           │   │   ├───┐
		//	5 │       ┌───┤   │   │   ├───┐       ┌───┐
		//	4 │       │   │   │   │   │   ├───┐   │   │
		//	3 │   ┌───┤   │   │   │   │   │   ├───┤   │
		//	2 │   │   │   │   │   │   │   │   │   │   │
		//	1 ├───┤   │   │   │   │   │   │   │   │   │
		//	  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴─────────────▶
		//	     10  20  30  40  50  60  70  80  90  100 110 120 130
		hist := metrics.Float64Histogram{
			Counts:  []uint64{1, 3, 5, 7, 8, 6, 5, 4, 3, 5, 0, 0, 0},
			Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130},
		}

		// Compare values against metric.Histogram (prometheus-based implementation)
		promhist := metric.NewHistogram(metric.HistogramOptions{
			Mode:     metric.HistogramModePrometheus,
			Metadata: metric.Metadata{},
			Duration: time.Hour,
			Buckets:  hist.Buckets,
		})
		for i := 0; i < len(hist.Counts); i++ {
			for j := 0; j < int(hist.Counts[i]); j++ {
				// Since the scheduler buckets are non-inclusive of Upper Bound and prometheus
				// buckets are inclusive, we use i+1 below to create an identical histogram in
				// prometheus.
				promhist.RecordValue(int64(hist.Buckets[i+1]))
			}
		}

		histWindow := promhist.WindowedSnapshot()
		require.InDelta(t, histWindow.ValueAtQuantile(100), percentile(&hist, 1.00), 1) // pmax
		require.InDelta(t, histWindow.ValueAtQuantile(0), percentile(&hist, 0.00), 1)   // pmin
		require.InDelta(t, histWindow.ValueAtQuantile(50), percentile(&hist, 0.50), 1)  // p50
		require.InDelta(t, histWindow.ValueAtQuantile(75), percentile(&hist, 0.75), 1)  // p75
		require.InDelta(t, histWindow.ValueAtQuantile(90), percentile(&hist, 0.90), 1)  // p90
		require.InDelta(t, histWindow.ValueAtQuantile(99), percentile(&hist, 0.99), 1)  // p99
	}
}

func TestSubtractHistograms(t *testing.T) {
	//	  ▲
	//	8 │               ┌───┐
	//	7 │           ┌───┤   │
	//	6 │           │   │   ├───┐
	//	5 │       ┌───┤   │   │   ├───┐       ┌───┐
	//	4 │       │   │   │   │   │   ├───┐   │   │
	//	3 │   ┌───┤   │   │   │   │   │   ├───┤   │
	//	2 │   │   │   │   │   │   │   │   │   │   │
	//	1 ├───┤   │   │   │   │   │   │   │   │   │
	//	  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴─▶
	//	     10  20  30  40  50  60  70  80  90  100
	a := metrics.Float64Histogram{
		Counts:  []uint64{1, 3, 5, 7, 8, 6, 5, 4, 3, 5},
		Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
	}

	//	  ▲
	//	  │
	//	9 │
	//	8 │
	//	7 │
	//	6 │
	//	5 │           ┌───┐       ┌───┐
	//	4 │           │   │   ┌───┤   │
	//	3 │           │   ├───┤   │   ├───┐   ┌───┐
	//	2 │   ┌───┬───┤   │   │   │   │   ├───┤   │
	//	1 │   │   │   │   │   │   │   │   │   │   │
	//	  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴─▶
	//	   10  20  30  40  50  60  70  80  90  100
	b := metrics.Float64Histogram{
		Counts:  []uint64{0, 2, 2, 5, 3, 4, 5, 3, 2, 3},
		Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
	}

	c := sub(&a, &b)
	require.Equal(t, []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, c.Buckets)
	for i := range c.Counts {
		require.Equal(t, a.Counts[i]-b.Counts[i], c.Counts[i])
	}
}

func TestCloneHistogram(t *testing.T) {
	hist := metrics.Float64Histogram{
		Counts:  []uint64{9, 7, 6, 5, 4, 2, 0, 1, 2, 5},
		Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
	}
	cloned := clone(&hist)
	require.Equal(t, cloned.Counts, hist.Counts)
	require.Equal(t, cloned.Buckets, hist.Buckets)
}

// TestRuntimeHistogramCumulativeVsWindowed verifies that runtimeHistogram
// correctly maintains both cumulative and windowed snapshots. Regression test
// for #116690 (windowed).
//
// This simulates how Go runtime scheduler latency metrics work in practice:
//
//	Every 100ms (samplePeriod):
//	  - Sample Go runtime histogram (cumulative since process start)
//	  - Compute delta from previous sample
//	  - Accumulate delta into schedulerLatencyAccumulator
//
//	Every 10s (statsInterval):
//	  - Pass accumulated observations to rh.update()
//	  - Clear accumulator
//
// Example timeline:
//
//	Time 0-10s:  Accumulate ~100 samples of 100ms deltas → 10 total observations
//	Time 10-20s: Accumulate ~100 samples of 100ms deltas → 6 total observations
//
// runtimeHistogram.update() receives these accumulated deltas and must:
//   - Accumulate into cumulativeCounts for Prometheus export (10 → 16)
//   - Replace windowedCounts with latest delta for TSDB percentiles (10 → 6)
func TestRuntimeHistogramCumulativeVsWindowed(t *testing.T) {
	buckets := []float64{0, 10, 20, 30, 40, 50}
	rh := newRuntimeHistogram(metric.Metadata{Name: "test.histogram"}, buckets)
	rh.mult = 1.0

	// Simulate first statsInterval (e.g., 0s-10s): 10 accumulated observations.
	// In production, this is the sum of ~100 deltas (one per 100ms sample).
	rh.update(&metrics.Float64Histogram{
		Counts:  []uint64{0, 5, 3, 2, 0}, // 10 total observations
		Buckets: buckets,
	})

	cumSnap1 := rh.CumulativeSnapshot()
	winSnap1 := rh.WindowedSnapshot()
	cumCount1, cumSum1 := cumSnap1.Total()
	winCount1, winSum1 := winSnap1.Total()

	// After first update, cumulative and windowed should be the same.
	require.Equal(t, int64(10), cumCount1)
	require.Equal(t, int64(10), winCount1)
	require.Equal(t, cumSum1, winSum1)

	// Simulate second statsInterval (e.g., 10s-20s): 6 accumulated observations.
	rh.update(&metrics.Float64Histogram{
		Counts:  []uint64{0, 1, 2, 3, 0}, // 6 total observations
		Buckets: buckets,
	})

	cumSnap2 := rh.CumulativeSnapshot()
	winSnap2 := rh.WindowedSnapshot()
	cumCount2, cumSum2 := cumSnap2.Total()
	winCount2, winSum2 := winSnap2.Total()

	// Cumulative must accumulate: 10 + 6 = 16 (not reset to 6). This is what
	// Prometheus scrapes via /_status/vars for rate() calculations.
	require.Equal(t, int64(16), cumCount2)
	require.GreaterOrEqual(t, cumCount2, cumCount1)
	require.Greater(t, cumSum2, cumSum1)

	// Windowed should be just this window: 6 (not cumulative 16). This is used
	// for TSDB percentile calculations (p50, p99, etc.).
	require.Equal(t, int64(6), winCount2)
	require.Less(t, winSum2, cumSum2)

	// Verify ToPrometheusMetric returns cumulative values (for Prometheus
	// scraping).
	promMetric := rh.ToPrometheusMetric().GetHistogram()
	require.Equal(t, uint64(16), promMetric.GetSampleCount())
	require.Equal(t, cumSum2, promMetric.GetSampleSum())
}

// BenchmarkSampleSchedulerLatencies measures the overhead of sampling scheduler
// latencies.
//
//	goos: linux
//	goarch: amd64
//	cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
//	BenchmarkSampleSchedulerLatency
//	BenchmarkSampleSchedulerLatency-24       6465466              2798 ns/op
func BenchmarkSampleSchedulerLatencies(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sample()
	}
}

// BenchmarkComputeSchedulerP99Latency measures the overhead of computing p99
// scheduling latencies.
//
//	goos: linux
//	goarch: amd64
//	cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
//	BenchmarkComputeSchedulerP99Latency
//	BenchmarkComputeSchedulerP99Latency-24           2090049              2841 ns/op
func BenchmarkComputeSchedulerP99Latency(b *testing.B) {
	s := sample()
	for i := 0; i < b.N; i++ {
		percentile(s, 0.99)
	}
}

// BenchmarkCloneLatencyHistogram measures how long it takes to clone scheduling
// latency histogram obtained from the Go runtime.
//
//	goos: linux
//	goarch: amd64
//	cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
//	BenchmarkCloneLatencyHistogram
//	BenchmarkCloneLatencyHistogram-23        1405375              4256 ns/op
func BenchmarkCloneLatencyHistogram(b *testing.B) {
	s := sample()
	for i := 0; i < b.N; i++ {
		clone(s)
	}
}

// BenchmarkSubtractLatencyHistograms measures how long it takes to subtract a
// histogram from another.
//
//	goos: linux
//	goarch: amd64
//	cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
//	BenchmarkSubtractLatencyHistograms
//	BenchmarkSubtractLatencyHistograms-24            1205223              5060 ns/op
func BenchmarkSubtractLatencyHistograms(b *testing.B) {
	a, z := sample(), sample()
	for i := 0; i < b.N; i++ {
		sub(a, z)
	}
}
