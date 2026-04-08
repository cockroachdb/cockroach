// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base2histogram

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketIndex(t *testing.T) {
	// Values 0-3 should get exact 1:1 mapping.
	for i := uint64(0); i < GroupSize; i++ {
		require.Equal(t, int(i), BucketIndex(i), "exact mapping for v=%d", i)
	}

	// Values 4-7 should be in the first group (4 sub-buckets).
	require.Equal(t, GroupSize+0, BucketIndex(4)) // group=0, offset=0
	require.Equal(t, GroupSize+1, BucketIndex(5)) // group=0, offset=1
	require.Equal(t, GroupSize+2, BucketIndex(6)) // group=0, offset=2
	require.Equal(t, GroupSize+3, BucketIndex(7)) // group=0, offset=3

	// Values 8-15 should be in the second group (2 values per bucket).
	require.Equal(t, GroupSize+4, BucketIndex(8))
	require.Equal(t, GroupSize+4, BucketIndex(9)) // same bucket as 8
	require.Equal(t, GroupSize+5, BucketIndex(10))
	require.Equal(t, GroupSize+5, BucketIndex(11)) // same bucket as 10
	require.Equal(t, GroupSize+7, BucketIndex(14))
	require.Equal(t, GroupSize+7, BucketIndex(15)) // same bucket as 14

	// Values 16-31: 4 values per bucket.
	require.Equal(t, GroupSize+8, BucketIndex(16))
	require.Equal(t, GroupSize+8, BucketIndex(19)) // same bucket
	require.Equal(t, GroupSize+9, BucketIndex(20))
}

func TestBucketIndexMonotonic(t *testing.T) {
	// Bucket indices should be monotonically non-decreasing.
	prev := BucketIndex(0)
	for v := uint64(1); v < 100000; v++ {
		idx := BucketIndex(v)
		require.GreaterOrEqualf(t, idx, prev,
			"BucketIndex(%d)=%d < BucketIndex(%d)=%d", v, idx, v-1, prev)
		prev = idx
	}
}

func TestBucketMinMaxValues(t *testing.T) {
	// For each bucket that covers values within int64 range, verify that
	// BucketMinValue and BucketMaxValue are consistent with BucketIndex.
	for idx := 0; idx < NumBuckets; idx++ {
		lo := BucketMinValue(idx)
		hi := BucketMaxValue(idx)

		// Skip buckets whose min value overflows int64 range — these
		// are unreachable with int64 inputs.
		if lo > 1<<63-1 {
			continue
		}

		require.LessOrEqual(t, lo, hi, "bucket %d: min=%d > max=%d", idx, lo, hi)
		require.Equal(t, idx, BucketIndex(lo), "bucket %d: BucketIndex(min=%d)", idx, lo)
		if hi <= 1<<63-1 {
			require.Equal(t, idx, BucketIndex(hi), "bucket %d: BucketIndex(max=%d)", idx, hi)
		}

		// The value just above max should be in the next bucket (if not the last).
		if idx < NumBuckets-1 && hi < 1<<63-1 {
			require.Equal(t, idx+1, BucketIndex(hi+1),
				"bucket %d: BucketIndex(max+1=%d)", idx, hi+1)
		}
	}
}

func TestBucketCount(t *testing.T) {
	require.Equal(t, 252, NumBuckets)
	require.Equal(t, 4, GroupSize)
	require.Equal(t, 16, NumRegions)
}

func TestRecordAndSnapshot(t *testing.T) {
	h := New()
	values := []int64{1000, 5000, 10000, 100000, 1000000}
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

	// Region sums should equal total count.
	var totalInRegions uint64
	for _, r := range snap.RegionSums {
		totalInRegions += r
	}
	require.Equal(t, uint64(len(values)), totalInRegions)
}

func TestRecordNegativeAndZero(t *testing.T) {
	h := New()
	h.Record(0)
	h.Record(-5)
	h.Record(-100)

	snap := h.Snapshot()
	require.Equal(t, uint64(3), snap.TotalCount)
	// All should be in bucket 0 (value 0).
	require.Equal(t, uint64(3), snap.Counts[0])
}

func TestConcurrentRecord(t *testing.T) {
	h := New()
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

	// Region sums should also be consistent.
	var regionTotal uint64
	for _, r := range snap.RegionSums {
		regionTotal += r
	}
	require.Equal(t, snap.TotalCount, regionTotal)
}

func TestQuantileUniform(t *testing.T) {
	h := New()
	for i := int64(1); i <= 1000; i++ {
		h.Record(i)
	}

	snap := h.Snapshot()
	for _, q := range []float64{50, 75, 90, 95, 99} {
		got := snap.ValueAtQuantile(q)
		expected := q / 100.0 * 1000.0
		relErr := math.Abs(got-expected) / expected
		// base2histogram has ~12.5% worst-case error at WIDTH=3 due to
		// linear sub-bucket spacing, so use a generous bound.
		require.LessOrEqualf(t, relErr, 0.20,
			"q%.0f: got=%.1f expected=%.1f relErr=%.4f", q, got, expected, relErr)
	}
}

func TestQuantileEdgeCases(t *testing.T) {
	t.Run("empty histogram", func(t *testing.T) {
		h := New()
		snap := h.Snapshot()
		require.Equal(t, 0.0, snap.ValueAtQuantile(50))
	})

	t.Run("single value", func(t *testing.T) {
		h := New()
		h.Record(500)
		snap := h.Snapshot()
		got := snap.ValueAtQuantile(50)
		// Value 500 falls in a bucket covering [448, 511]. The interpolated
		// estimate should be somewhere in that range.
		lo := float64(BucketMinValue(BucketIndex(500)))
		hi := float64(BucketMaxValue(BucketIndex(500))) + 1
		require.GreaterOrEqual(t, got, lo)
		require.LessOrEqual(t, got, hi)
	})
}

func TestMeanAndTotal(t *testing.T) {
	h := New()
	h.Record(100)
	h.Record(200)
	h.Record(300)

	snap := h.Snapshot()
	require.InDelta(t, 200.0, snap.Mean(), 0.001)

	count, sum := snap.Total()
	require.Equal(t, int64(3), count)
	require.InDelta(t, 600.0, sum, 0.001)
}

func TestRegionSumsConsistency(t *testing.T) {
	h := New()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		h.Record(rng.Int63n(1e9) + 1)
	}

	snap := h.Snapshot()
	// Verify each region sum equals the sum of its constituent buckets.
	for r := 0; r < NumRegions; r++ {
		var bucketSum uint64
		start := r * RegionSize
		end := start + RegionSize
		if end > NumBuckets {
			end = NumBuckets
		}
		for i := start; i < end; i++ {
			bucketSum += snap.Counts[i]
		}
		require.Equal(t, bucketSum, snap.RegionSums[r],
			"region %d: bucketSum=%d != regionSum=%d", r, bucketSum, snap.RegionSums[r])
	}
}
