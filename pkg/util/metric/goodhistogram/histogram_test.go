// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goodhistogram

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPickSchema(t *testing.T) {
	tests := []struct {
		desiredError float64
		wantSchema   int32
	}{
		{0.35, 0},  // 33.3% error for schema 0
		{0.10, 2},  // 8.6% error for schema 2
		{0.05, 3},  // 4.3% error for schema 3
		{0.03, 4},  // 2.17% error for schema 4
		{0.02, 5},  // schema 4 is 2.17% > 2%, so need schema 5 (1.08%)
		{0.015, 5}, // 1.08% for schema 5
		{0.005, 7}, // schema 6 is 0.54% > 0.5%, so need schema 7 (0.27%)
		{0.003, 7}, // 0.27% for schema 7
		{0.002, 8}, // 0.14% for schema 8
		{0.001, 8}, // still schema 8 (finest available)
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("error=%.3f", tt.desiredError), func(t *testing.T) {
			got := pickSchema(tt.desiredError)
			require.Equal(t, tt.wantSchema, got)
			// Verify the actual error is at or below desired, unless we're at
			// schema 8 (the finest available) and the desired error is below
			// what's achievable.
			actualErr := schemaRelativeError(got)
			if got < 8 {
				require.LessOrEqualf(t, actualErr, tt.desiredError,
					"schema %d error %.6f exceeds desired %.6f", got, actualErr, tt.desiredError)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cfg := NewConfig(1e4, 1e16, 0.05)
		require.Equal(t, int32(3), cfg.Schema) // 4.3% error
		require.Greater(t, cfg.NumBuckets, 0)
		require.Equal(t, cfg.NumBuckets+1, len(cfg.Boundaries))
		require.Equal(t, 1e4, cfg.Boundaries[0])
		require.Equal(t, 1e16, cfg.Boundaries[cfg.NumBuckets])

		// Boundaries should be monotonically increasing.
		for i := 1; i < len(cfg.Boundaries); i++ {
			require.Greaterf(t, cfg.Boundaries[i], cfg.Boundaries[i-1],
				"boundary[%d]=%v not > boundary[%d]=%v", i, cfg.Boundaries[i], i-1, cfg.Boundaries[i-1])
		}
	})

	t.Run("panics on invalid input", func(t *testing.T) {
		require.Panics(t, func() { NewConfig(0, 100, 0.05) })
		require.Panics(t, func() { NewConfig(-1, 100, 0.05) })
		require.Panics(t, func() { NewConfig(100, 50, 0.05) })
		require.Panics(t, func() { NewConfig(1, 100, 0) })
	})
}

func TestRecordAndSnapshot(t *testing.T) {
	h := New(1e3, 1e9, 0.05)

	// Record a range of values.
	values := []int64{1000, 5000, 10000, 100000, 1000000, 10000000, 100000000, 999999999}
	var expectedSum int64
	for _, v := range values {
		h.Record(v)
		expectedSum += v
	}

	snap := h.Snapshot()
	require.Equal(t, uint64(len(values)), snap.TotalCount)
	require.Equal(t, expectedSum, snap.TotalSum)

	// All values should be in some bucket.
	var totalInBuckets uint64
	for _, c := range snap.Counts {
		totalInBuckets += c
	}
	require.Equal(t, uint64(len(values)), totalInBuckets)
	require.Equal(t, uint64(0), snap.ZeroCount)
	require.Equal(t, uint64(0), snap.Underflow)
	require.Equal(t, uint64(0), snap.Overflow)
}

func TestRecordBoundaryValues(t *testing.T) {
	h := New(100, 10000, 0.05)

	// Record value exactly at lo.
	h.Record(100)
	// Record value exactly at hi.
	h.Record(10000)

	snap := h.Snapshot()
	require.Equal(t, uint64(2), snap.TotalCount)
	require.Equal(t, uint64(0), snap.Underflow)
	require.Equal(t, uint64(0), snap.Overflow)
}

func TestRecordOutOfRange(t *testing.T) {
	h := New(100, 10000, 0.05)

	// Below range.
	h.Record(50)
	// Above range.
	h.Record(20000)
	// Zero.
	h.Record(0)
	// Negative.
	h.Record(-5)

	snap := h.Snapshot()
	require.Equal(t, uint64(4), snap.TotalCount)
	require.Equal(t, uint64(1), snap.Underflow)
	require.Equal(t, uint64(1), snap.Overflow)
	require.Equal(t, uint64(2), snap.ZeroCount) // 0 and -5
}

func TestRecordAccuracy(t *testing.T) {
	// Verify that each recorded value lands in a bucket whose boundaries
	// contain the value (within floating-point tolerance).
	h := New(1, 1e6, 0.05)
	cfg := h.Config()

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		// Log-uniform distribution over [1, 1e6].
		v := int64(math.Exp(rng.Float64() * math.Log(1e6)))
		if v < 1 {
			v = 1
		}
		h.Record(v)
	}

	snap := h.Snapshot()
	// For each non-zero bucket, verify the bucket boundaries make sense
	// relative to the schema's error bound.
	gamma := math.Pow(2, math.Pow(2, float64(-cfg.Schema)))
	for i, c := range snap.Counts {
		if c == 0 {
			continue
		}
		lo := cfg.Boundaries[i]
		hi := cfg.Boundaries[i+1]
		ratio := hi / lo
		// The ratio should be approximately gamma (within floating point).
		// Allow up to 2x gamma for edge buckets that may be clamped.
		require.LessOrEqualf(t, ratio, gamma*1.01,
			"bucket %d [%.2f, %.2f] ratio %.4f exceeds gamma %.4f", i, lo, hi, ratio, gamma)
	}
}

func TestConcurrentRecord(t *testing.T) {
	h := New(1, 1e6, 0.05)
	const goroutines = 8
	const recordsPerGoroutine = 10000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < recordsPerGoroutine; i++ {
				v := rng.Int63n(1e6) + 1
				h.Record(v)
			}
		}(int64(g))
	}
	wg.Wait()

	snap := h.Snapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), snap.TotalCount)
}

func TestQuantileUniform(t *testing.T) {
	h := New(1, 1000, 0.05)
	// Record values 1..1000.
	for i := int64(1); i <= 1000; i++ {
		h.Record(i)
	}

	snap := h.Snapshot()
	cfg := h.Config()
	gamma := math.Pow(2, math.Pow(2, float64(-cfg.Schema)))
	maxRelError := (gamma - 1) / (gamma + 1)

	// Test quantiles.
	for _, q := range []float64{50, 75, 90, 95, 99} {
		got := snap.ValueAtQuantile(q)
		expected := q / 100.0 * 1000.0
		relErr := math.Abs(got-expected) / expected
		// Allow a generous bound: the histogram error plus some interpolation error.
		require.LessOrEqualf(t, relErr, maxRelError*3,
			"q%.0f: got=%.1f expected=%.1f relErr=%.4f", q, got, expected, relErr)
	}
}

func TestQuantileExponential(t *testing.T) {
	h := New(1, 1e6, 0.05)
	rng := rand.New(rand.NewSource(42))
	n := 100000
	// Exponential distribution with rate=1, values in nanoseconds.
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		v := rng.ExpFloat64() * 10000 // mean = 10000
		if v < 1 {
			v = 1
		}
		if v > 1e6 {
			v = 1e6
		}
		values[i] = v
		h.Record(int64(v))
	}

	snap := h.Snapshot()
	// Verify median is in a reasonable range.
	median := snap.ValueAtQuantile(50)
	// Median of Exp(1/10000) ≈ 10000 * ln(2) ≈ 6931
	require.InDelta(t, 6931, median, 2000, "median=%.1f", median)
}

func TestQuantileEdgeCases(t *testing.T) {
	t.Run("empty histogram", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		snap := h.Snapshot()
		require.Equal(t, 0.0, snap.ValueAtQuantile(50))
	})

	t.Run("single value", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		h.Record(500)
		snap := h.Snapshot()
		got := snap.ValueAtQuantile(50)
		// Should be somewhere in the bucket containing 500.
		require.InDelta(t, 500, got, 100)
	})

	t.Run("q0 and q100", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.GreaterOrEqual(t, snap.ValueAtQuantile(0), 1.0)
		require.LessOrEqual(t, snap.ValueAtQuantile(100), 1000.0)
	})

	t.Run("all zeros", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		for i := 0; i < 100; i++ {
			h.Record(0)
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(100), snap.ZeroCount)
		require.Equal(t, uint64(100), snap.TotalCount)
		// All observations are zeros (below lo), so every quantile
		// should clamp to lo.
		require.Equal(t, 1.0, snap.ValueAtQuantile(50))
		require.Equal(t, 1.0, snap.ValueAtQuantile(99))
	})

	t.Run("all underflow", func(t *testing.T) {
		h := New(100, 10000, 0.05)
		for i := int64(1); i <= 50; i++ {
			h.Record(i) // all below lo=100
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(50), snap.Underflow)
		require.Equal(t, uint64(50), snap.TotalCount)
		// Every quantile should clamp to lo.
		require.Equal(t, 100.0, snap.ValueAtQuantile(50))
		require.Equal(t, 100.0, snap.ValueAtQuantile(99))
	})

	t.Run("all overflow", func(t *testing.T) {
		h := New(1, 100, 0.05)
		for i := int64(200); i <= 300; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(101), snap.Overflow)
		require.Equal(t, uint64(101), snap.TotalCount)
		// Every quantile should clamp to hi.
		require.Equal(t, 100.0, snap.ValueAtQuantile(50))
		require.Equal(t, 100.0, snap.ValueAtQuantile(99))
	})

	t.Run("underflow with in-range values", func(t *testing.T) {
		h := New(100, 10000, 0.05)
		// 80 underflow values, 20 in-range values.
		for i := int64(1); i <= 80; i++ {
			h.Record(i) // underflow
		}
		for i := int64(500); i <= 519; i++ {
			h.Record(i) // in range
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(80), snap.Underflow)
		require.Equal(t, uint64(100), snap.TotalCount)

		// p50 is at rank 50. With 80 underflow values, rank 50
		// falls in the underflow region — should clamp to lo.
		require.Equal(t, 100.0, snap.ValueAtQuantile(50))

		// p90 is at rank 90. Past the 80 underflow values, it falls
		// in the in-range buckets containing 500–519.
		p90 := snap.ValueAtQuantile(90)
		require.Greater(t, p90, 100.0, "p90 should be in the in-range region")
		require.InDelta(t, 510, p90, 50, "p90 should be near the in-range values")
	})

	t.Run("overflow with in-range values", func(t *testing.T) {
		h := New(1, 100, 0.05)
		// 50 in-range values, 50 overflow values.
		for i := int64(1); i <= 50; i++ {
			h.Record(i) // in range
		}
		for i := int64(200); i <= 249; i++ {
			h.Record(i) // overflow
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(50), snap.Overflow)
		require.Equal(t, uint64(100), snap.TotalCount)

		// p50 falls in the in-range buckets.
		p50 := snap.ValueAtQuantile(50)
		require.Greater(t, p50, 0.0)
		require.LessOrEqual(t, p50, 100.0)

		// p99 should be in the overflow region — clamps to hi.
		require.Equal(t, 100.0, snap.ValueAtQuantile(99))
	})

	t.Run("mixed zeros underflow overflow and in-range", func(t *testing.T) {
		h := New(100, 10000, 0.05)
		// 10 zeros, 10 underflow, 60 in-range, 20 overflow.
		for i := 0; i < 10; i++ {
			h.Record(0)
		}
		for i := int64(1); i <= 10; i++ {
			h.Record(i)
		}
		for i := int64(500); i <= 559; i++ {
			h.Record(i)
		}
		for i := int64(20000); i <= 20019; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(10), snap.ZeroCount)
		require.Equal(t, uint64(10), snap.Underflow)
		require.Equal(t, uint64(20), snap.Overflow)
		require.Equal(t, uint64(100), snap.TotalCount)

		// p10 is at rank 10, which falls at the boundary between
		// zeros and underflow — should clamp to lo.
		require.Equal(t, 100.0, snap.ValueAtQuantile(10))

		// p50 is at rank 50. With 20 sub-lo values (zeros +
		// underflow), rank 50 falls in the in-range buckets.
		p50 := snap.ValueAtQuantile(50)
		require.Greater(t, p50, 100.0)
		require.Less(t, p50, 10000.0)

		// p95 is at rank 95. With 80 non-overflow values, rank 95
		// falls in the overflow region — should clamp to hi.
		require.Equal(t, 10000.0, snap.ValueAtQuantile(95))
	})
}

func TestMeanAndTotal(t *testing.T) {
	h := New(1, 1000, 0.05)
	h.Record(100)
	h.Record(200)
	h.Record(300)

	snap := h.Snapshot()
	require.InDelta(t, 200.0, snap.Mean(), 0.001)

	count, sum := snap.Total()
	require.Equal(t, int64(3), count)
	require.InDelta(t, 600.0, sum, 0.001)
}

// decodeDeltaCounts decodes a Prometheus native histogram's delta-encoded
// positive bucket counts, returning the absolute count for each bucket
// across all spans. This is the inverse of the encoding in export.go.
func decodeDeltaCounts(
	spans []*prometheusgo.BucketSpan, deltas []int64,
) (keys []int, counts []uint64) {
	var prevCount int64
	deltaIdx := 0
	for _, sp := range spans {
		key := int(sp.GetOffset())
		if len(keys) > 0 {
			// Subsequent spans: offset is relative to end of previous span.
			key += keys[len(keys)-1] + 1
		}
		for j := 0; j < int(sp.GetLength()); j++ {
			prevCount += deltas[deltaIdx]
			deltaIdx++
			keys = append(keys, key)
			counts = append(counts, uint64(prevCount))
			key++
		}
	}
	return keys, counts
}

func TestPrometheusExport(t *testing.T) {
	t.Run("sample count and sum", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()
		require.Equal(t, uint64(100), ph.GetSampleCount())
		require.Equal(t, float64(5050), ph.GetSampleSum())
	})

	t.Run("schema matches config", func(t *testing.T) {
		for _, errBound := range []float64{0.10, 0.05, 0.02, 0.005} {
			h := New(1, 1000, errBound)
			h.Record(500)
			snap := h.Snapshot()
			ph := snap.ToPrometheusHistogram()
			require.Equal(t, h.Config().Schema, ph.GetSchema(),
				"errBound=%f", errBound)
		}
	})

	t.Run("zero bucket", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		h.Record(0)
		h.Record(0)
		h.Record(-5)
		h.Record(500)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.Equal(t, uint64(3), ph.GetZeroCount())
		require.Equal(t, math.SmallestNonzeroFloat64, ph.GetZeroThreshold())
		require.Equal(t, uint64(4), ph.GetSampleCount())
	})

	t.Run("conventional buckets are cumulative", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.NotEmpty(t, ph.Bucket)
		var prev uint64
		for i, b := range ph.Bucket {
			require.GreaterOrEqualf(t, b.GetCumulativeCount(), prev,
				"bucket[%d] cumulative count %d < previous %d",
				i, b.GetCumulativeCount(), prev)
			prev = b.GetCumulativeCount()
		}
		// Last bucket must contain all observations.
		require.Equal(t, uint64(100),
			ph.Bucket[len(ph.Bucket)-1].GetCumulativeCount())
	})

	t.Run("conventional bucket upper bounds are monotonic", func(t *testing.T) {
		h := New(1, 1e6, 0.05)
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		var prevUB float64
		for i, b := range ph.Bucket {
			require.Greaterf(t, b.GetUpperBound(), prevUB,
				"bucket[%d] upper bound %.4f not > previous %.4f",
				i, b.GetUpperBound(), prevUB)
			prevUB = b.GetUpperBound()
		}
	})

	t.Run("native delta decoding reconstructs counts", func(t *testing.T) {
		h := New(1, 1e6, 0.05)
		rng := rand.New(rand.NewSource(42))
		const n = 10000
		for i := 0; i < n; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(n), total,
			"sum of decoded native bucket counts must equal total observations")
	})

	t.Run("native spans have non-zero lengths", func(t *testing.T) {
		h := New(1, 1e6, 0.05)
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		for i, sp := range ph.PositiveSpan {
			require.Greaterf(t, sp.GetLength(), uint32(0),
				"span[%d] has zero length", i)
		}
	})

	t.Run("native bucket keys are monotonically increasing", func(t *testing.T) {
		h := New(1, 1e6, 0.05)
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 5000; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		keys, _ := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		for i := 1; i < len(keys); i++ {
			require.Greaterf(t, keys[i], keys[i-1],
				"key[%d]=%d not > key[%d]=%d", i, keys[i], i-1, keys[i-1])
		}
	})

	t.Run("sparse encoding with widely separated values", func(t *testing.T) {
		h := New(1, 1e6, 0.05)
		for i := 0; i < 100; i++ {
			h.Record(10)
			h.Record(100000)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// Should have at least 2 spans for the two clusters of values.
		require.GreaterOrEqual(t, len(ph.PositiveSpan), 2,
			"expected at least 2 spans for widely separated values")

		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(200), total)
	})

	t.Run("single observation", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		h.Record(42)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.Equal(t, uint64(1), ph.GetSampleCount())
		require.Equal(t, float64(42), ph.GetSampleSum())
		require.Len(t, ph.PositiveSpan, 1)
		require.Equal(t, uint32(1), ph.PositiveSpan[0].GetLength())
		require.Equal(t, []int64{1}, ph.PositiveDelta)
	})

	t.Run("empty histogram", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.Equal(t, uint64(0), ph.GetSampleCount())
		require.Equal(t, float64(0), ph.GetSampleSum())
		require.Empty(t, ph.PositiveSpan)
		require.Empty(t, ph.PositiveDelta)
	})

	t.Run("conventional and native counts agree", func(t *testing.T) {
		h := New(1, 1e6, 0.05)
		rng := rand.New(rand.NewSource(99))
		const n = 5000
		for i := 0; i < n; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// Total from conventional buckets.
		convTotal := ph.Bucket[len(ph.Bucket)-1].GetCumulativeCount()

		// Total from native buckets.
		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var nativeTotal uint64
		for _, c := range counts {
			nativeTotal += c
		}

		require.Equal(t, convTotal, nativeTotal,
			"conventional and native bucket totals must agree")
		require.Equal(t, uint64(n), convTotal)
	})

	t.Run("native keys align with prometheus schema", func(t *testing.T) {
		// Verify that the bucket keys produced by our export match what
		// Prometheus would compute for the same values via promBucketKey,
		// allowing off-by-one for values that land on a straddling entry
		// in the lookup table (where we round up to the next sub-bucket).
		h := New(1, 1e6, 0.05)
		schema := h.Config().Schema

		// Record specific values and verify their keys.
		testValues := []int64{1, 2, 4, 8, 16, 100, 1000, 10000, 100000, 999999}
		for _, v := range testValues {
			h.Record(v)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		keys, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)

		// Build a set of expected keys from promBucketKey, including
		// key+1 for each value to account for the lookup table's
		// round-up behavior at straddling boundaries.
		expectedKeys := make(map[int]bool)
		for _, v := range testValues {
			k := promBucketKey(float64(v), schema)
			expectedKeys[k] = true
			expectedKeys[k+1] = true
		}

		// Every key with count > 0 in the export should be in the expected set.
		for i, k := range keys {
			if counts[i] > 0 {
				require.Truef(t, expectedKeys[k],
					"exported key %d (count=%d) not in expected set %v",
					k, counts[i], expectedKeys)
			}
		}
	})

	t.Run("downsampled export has correct schema", func(t *testing.T) {
		h := New(1, 1e6, 0.01) // schema 6
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ds := snap.Downsample(100)
		ph := ds.ToPrometheusHistogram()

		require.Equal(t, ds.Config.Schema, ph.GetSchema())
		require.Less(t, ph.GetSchema(), snap.Config.Schema)
		require.Equal(t, uint64(1000), ph.GetSampleCount())

		// Decoded counts should still sum to total.
		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(1000), total)
	})

	t.Run("negative deltas are valid", func(t *testing.T) {
		// When bucket counts decrease (e.g., a tall bucket followed by a short
		// one), deltas should be negative. Verify this doesn't break decoding.
		h := New(1, 1000, 0.05)
		// Put 1000 observations in one bucket, then 1 in an adjacent one.
		for i := 0; i < 1000; i++ {
			h.Record(100)
		}
		h.Record(200)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// There should be at least one negative delta.
		hasNegative := false
		for _, d := range ph.PositiveDelta {
			if d < 0 {
				hasNegative = true
				break
			}
		}
		require.True(t, hasNegative,
			"expected negative deltas when bucket counts decrease")

		// Decoding should still produce correct total.
		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(1001), total)
	})
}

func TestDownsample(t *testing.T) {
	t.Run("no-op when under limit", func(t *testing.T) {
		h := New(1, 1000, 0.05)
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ds := snap.Downsample(snap.Config.NumBuckets + 10)
		require.Equal(t, snap.Config.NumBuckets, ds.Config.NumBuckets)
		require.Equal(t, snap.TotalCount, ds.TotalCount)
	})

	t.Run("preserves total count and sum", func(t *testing.T) {
		h := New(1, 1e6, 0.01) // schema 6, lots of buckets
		for i := int64(1); i <= 10000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.Greater(t, snap.Config.NumBuckets, 100,
			"need enough buckets to make downsampling meaningful")

		ds := snap.Downsample(100)
		require.LessOrEqual(t, ds.Config.NumBuckets, 100)
		require.Equal(t, snap.TotalCount, ds.TotalCount)
		require.Equal(t, snap.TotalSum, ds.TotalSum)
		require.Equal(t, snap.ZeroCount, ds.ZeroCount)

		// Sum of downsampled bucket counts should equal sum of original.
		var origSum, dsSum uint64
		for _, c := range snap.Counts {
			origSum += c
		}
		for _, c := range ds.Counts {
			dsSum += c
		}
		require.Equal(t, origSum, dsSum)
	})

	t.Run("schema is reduced", func(t *testing.T) {
		h := New(1, 1e6, 0.01) // schema 6
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ds := snap.Downsample(100)
		require.Less(t, ds.Config.Schema, snap.Config.Schema)
	})

	t.Run("boundaries are monotonic", func(t *testing.T) {
		h := New(1, 1e6, 0.01)
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ds := snap.Downsample(50)
		for i := 1; i < len(ds.Config.Boundaries); i++ {
			require.Greaterf(t, ds.Config.Boundaries[i], ds.Config.Boundaries[i-1],
				"boundary[%d] not > boundary[%d]", i, i-1)
		}
	})

	t.Run("quantile accuracy degrades gracefully", func(t *testing.T) {
		h := New(1, 1e6, 0.01) // schema 6, ~0.5% error
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 100000; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ds := snap.Downsample(100)

		// Downsampled quantiles should still be reasonable, just less precise.
		origP50 := snap.ValueAtQuantile(50)
		dsP50 := ds.ValueAtQuantile(50)
		relErr := math.Abs(dsP50-origP50) / origP50
		require.Less(t, relErr, 0.2,
			"downsampled p50=%.1f vs original p50=%.1f, relErr=%.3f", dsP50, origP50, relErr)
	})

	t.Run("export produces valid native histogram", func(t *testing.T) {
		h := New(1, 1e6, 0.01)
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ds := snap.Downsample(100)
		ph := ds.ToPrometheusHistogram()

		require.Equal(t, ds.Config.Schema, ph.GetSchema())
		require.Equal(t, uint64(1000), ph.GetSampleCount())
		require.NotEmpty(t, ph.PositiveSpan)
	})
}

func TestWindowedHistogramBasic(t *testing.T) {
	wh := NewWindowed(1, 1000, 0.05)

	// Record values.
	for i := int64(1); i <= 100; i++ {
		wh.Record(i)
	}

	// Cumulative snapshot should have all values.
	cumSnap := wh.CumulativeSnapshot()
	require.Equal(t, uint64(100), cumSnap.TotalCount)

	// Windowed snapshot should have all values (no rotation yet, cur only).
	winSnap := wh.WindowedSnapshot()
	require.Equal(t, uint64(100), winSnap.TotalCount)

	// Without rotation, windowed snapshot is unchanged.
	winSnap2 := wh.WindowedSnapshot()
	require.Equal(t, uint64(100), winSnap2.TotalCount)

	// After rotation, prev=100 values, cur=0. Merged = 100.
	wh.Rotate()
	winSnap3 := wh.WindowedSnapshot()
	require.Equal(t, uint64(100), winSnap3.TotalCount)

	// After a second rotation with no new records, prev=0, cur=0.
	wh.Rotate()
	winSnap4 := wh.WindowedSnapshot()
	require.Equal(t, uint64(0), winSnap4.TotalCount)

	// Cumulative should still have everything.
	cumSnap2 := wh.CumulativeSnapshot()
	require.Equal(t, uint64(100), cumSnap2.TotalCount)
}

func TestWindowedHistogramMultipleRotations(t *testing.T) {
	wh := NewWindowed(1, 1000, 0.05)

	// Window 1: record 50 values, then rotate.
	for i := int64(1); i <= 50; i++ {
		wh.Record(i)
	}
	wh.Rotate()

	// Window 2: record 30 more values. Snapshot = prev(50) + cur(30) = 80.
	for i := int64(51); i <= 80; i++ {
		wh.Record(i)
	}
	snap1 := wh.WindowedSnapshot()
	require.Equal(t, uint64(80), snap1.TotalCount)

	wh.Rotate()

	// Window 3: record 20 more. Snapshot = prev(30) + cur(20) = 50.
	for i := int64(81); i <= 100; i++ {
		wh.Record(i)
	}
	snap2 := wh.WindowedSnapshot()
	require.Equal(t, uint64(50), snap2.TotalCount)

	// Cumulative should have all 100.
	cumSnap := wh.CumulativeSnapshot()
	require.Equal(t, uint64(100), cumSnap.TotalCount)
}

func TestWindowedHistogramQuantiles(t *testing.T) {
	wh := NewWindowed(1, 1000, 0.05)

	// Record a uniform distribution in the first window.
	for i := int64(1); i <= 1000; i++ {
		wh.Record(i)
	}

	// Windowed snapshot should support quantile computation.
	snap := wh.WindowedSnapshot()
	p50 := snap.ValueAtQuantile(50)
	require.InDelta(t, 500, p50, 50, "p50=%.1f", p50)

	p99 := snap.ValueAtQuantile(99)
	require.InDelta(t, 990, p99, 50, "p99=%.1f", p99)
}

func TestWindowedHistogramConcurrent(t *testing.T) {
	wh := NewWindowed(1, 1e6, 0.05)
	const goroutines = 8
	const recordsPerGoroutine = 10000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < recordsPerGoroutine; i++ {
				v := rng.Int63n(1e6) + 1
				wh.Record(v)
			}
		}(int64(g))
	}
	wg.Wait()

	// Cumulative should have all observations.
	cumSnap := wh.CumulativeSnapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), cumSnap.TotalCount)

	// Windowed snapshot should also have all (no rotation yet, cur only).
	winSnap := wh.WindowedSnapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), winSnap.TotalCount)

	// After rotation, prev has all values, cur is empty. Merged = all.
	wh.Rotate()
	winSnap2 := wh.WindowedSnapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), winSnap2.TotalCount)

	// After second rotation with no new records, both windows are empty.
	wh.Rotate()
	winSnap3 := wh.WindowedSnapshot()
	require.Equal(t, uint64(0), winSnap3.TotalCount)
}
