// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goodhistogram

import (
	"math"
	"math/rand"
	"testing"
)

// TestBucketMappingConsistency compares the Record() path (which uses
// the SubBucketLookup table for O(1) bucket resolution) against the
// promBucketKey path (which uses math.Frexp + sort.SearchFloat64s).
//
// The lookup table uses 8 bits of mantissa, so ~4 of 256 entries per
// schema may straddle a sub-bucket boundary and resolve to the next
// bucket. This test verifies that:
//  1. Mismatches are bounded by the expected straddling rate (<2%).
//  2. All mismatches differ by exactly 1 bucket (never more).
//  3. The resulting quantile estimates remain accurate despite mismatches.
func TestBucketMappingConsistency(t *testing.T) {
	cfg := NewConfig(500, 60e9, 0.10) // schema 2

	// recordBucketIndex matches Record() exactly: lookup table only.
	recordBucketIndex := func(v int64) int {
		fv := float64(v)
		bits := math.Float64bits(fv)
		exp := int(bits>>52) - 1022
		sub := int(cfg.SubBucketLookup[(bits>>subBucketLookupShift)&0xFF])
		key := sub + (exp-1)*cfg.NumSubBuckets
		idx := key - cfg.MinKey
		if idx < 0 {
			idx = 0
		} else if idx >= cfg.NumBuckets {
			idx = cfg.NumBuckets - 1
		}
		return idx
	}

	// promBucketIndex uses promBucketKey (the reference implementation).
	promBucketIndex := func(v int64) int {
		fv := float64(v)
		key := promBucketKey(fv, cfg.Schema)
		idx := key - cfg.MinKey
		if idx < 0 {
			idx = 0
		} else if idx >= cfg.NumBuckets {
			idx = cfg.NumBuckets - 1
		}
		return idx
	}

	// Test random values across the range. Allow a small mismatch rate
	// due to lookup table straddling, but require that all mismatches
	// differ by at most 1 bucket.
	rng := rand.New(rand.NewSource(42))
	logLo := math.Log(500)
	logHi := math.Log(60e9)
	const numSamples = 1000000
	mismatches := 0
	for i := 0; i < numSamples; i++ {
		v := int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
		if v <= 0 {
			continue
		}
		refIdx := promBucketIndex(v)
		recIdx := recordBucketIndex(v)
		if refIdx != recIdx {
			diff := recIdx - refIdx
			if diff < -1 || diff > 1 {
				t.Errorf("v=%d: ref=%d rec=%d (diff=%d exceeds 1)", v, refIdx, recIdx, diff)
			}
			mismatches++
		}
	}
	mismatchRate := float64(mismatches) / float64(numSamples) * 100
	t.Logf("Mismatches: %d / %d (%.2f%%)", mismatches, numSamples, mismatchRate)
	if mismatchRate > 2.0 {
		t.Errorf("Mismatch rate %.2f%% exceeds 2%% threshold", mismatchRate)
	}

	// Verify that quantile estimates are close despite straddling.
	t.Run("full-histogram-comparison", func(t *testing.T) {
		h := New(500, 60e9, 0.10)

		rng := rand.New(rand.NewSource(99))
		values := make([]int64, 100000)
		for i := range values {
			values[i] = int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
			if values[i] <= 0 {
				values[i] = 1
			}
		}
		for _, v := range values {
			h.Record(v)
		}
		newSnap := h.Snapshot()

		// Build counts using the reference (promBucketKey) path.
		refCounts := make([]uint64, cfg.NumBuckets)
		for _, v := range values {
			idx := promBucketIndex(v)
			refCounts[idx]++
		}

		// Compare quantile estimates between the two bucket distributions.
		// They should be very close since mismatches shift values by at
		// most one bucket width.
		refSnap := Snapshot{
			Config:     &cfg,
			Counts:     refCounts,
			TotalCount: newSnap.TotalCount,
			TotalSum:   newSnap.TotalSum,
		}
		for _, q := range []float64{50, 75, 90, 95, 99, 99.9} {
			refQ := refSnap.ValueAtQuantile(q)
			recQ := newSnap.ValueAtQuantile(q)
			if refQ == 0 {
				continue
			}
			relErr := math.Abs(recQ-refQ) / refQ
			if relErr > 0.05 {
				t.Errorf("p%.1f: ref=%.6f rec=%.6f relErr=%.4f (>5%%)", q, refQ, recQ, relErr)
			}
		}
	})

	// Show the exponent calculation difference for edge cases.
	t.Run("exponent-comparison", func(t *testing.T) {
		edgeCases := []int64{1, 2, 3, 4, 7, 8, 15, 16, 1023, 1024, 2047, 2048}
		for _, v := range edgeCases {
			fv := float64(v)
			_, frexpExp := math.Frexp(fv)
			bits := math.Float64bits(fv)
			bitsExp := int(bits>>52) - 1022
			if frexpExp != bitsExp {
				t.Logf("v=%d: frexp_exp=%d bits_exp=%d (DIFFER)", v, frexpExp, bitsExp)
			} else {
				t.Logf("v=%d: frexp_exp=%d bits_exp=%d (match)", v, frexpExp, bitsExp)
			}
		}
	})

	// Show sub-bucket comparison for values near boundaries.
	t.Run("sub-bucket-comparison", func(t *testing.T) {
		bounds := cfg.SubBucketBounds
		for i := 0; i < len(bounds); i++ {
			// Generate a value right at a sub-bucket boundary.
			boundary := bounds[i] * 2 // undo the /2 from Frexp normalization
			// Test values just below, at, and just above.
			for _, offset := range []float64{-1e-10, 0, 1e-10} {
				testVal := boundary + offset
				bits := math.Float64bits(testVal)
				lookupIdx := int(cfg.SubBucketLookup[(bits>>subBucketLookupShift)&0xFF])
				oldKey := promBucketKey(testVal, cfg.Schema)
				newKey := lookupIdx + (int(bits>>52)-1022-1)*cfg.NumSubBuckets

				if oldKey != newKey && i < 5 {
					t.Logf("boundary[%d]=%.15f offset=%+e: oldKey=%d newKey=%d (DIFFER)",
						i, boundary, offset, oldKey, newKey)
				}
			}
		}
	})
}
