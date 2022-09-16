// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestSchedulerLatencyCallbacks(t *testing.T) {
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

	mu := struct {
		syncutil.Mutex
		p99 time.Duration
	}{}
	slcbID := RegisterCallback(func(p99 time.Duration, period time.Duration) {
		require.Equal(t, samplePeriod.Default(), period)
		mu.Lock()
		defer mu.Unlock()
		mu.p99 = p99
	})
	defer UnregisterCallback(slcbID)

	require.NoError(t, StartSampler(ctx, st, stopper))
	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()
		if mu.p99.Nanoseconds() == 0 {
			return fmt.Errorf("expected non-zero p99 scheduling latency")
		}
		return nil
	})
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

		require.Equal(t, 95.0, percentile(&hist, 1.00)) // pmax
		require.Equal(t, 05.0, percentile(&hist, 0.00)) // pmin
		require.Equal(t, 45.0, percentile(&hist, 0.50)) // p50
		require.Equal(t, 75.0, percentile(&hist, 0.75)) // p75
		require.Equal(t, 95.0, percentile(&hist, 0.90)) // p90
		require.Equal(t, 95.0, percentile(&hist, 0.99)) // p99
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
