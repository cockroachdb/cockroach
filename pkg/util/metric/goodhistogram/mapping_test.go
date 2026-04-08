// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goodhistogram

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

// TestBucketMappingConsistency compares the old recording path
// (promBucketKey via math.Frexp + sort.SearchFloat64s) against the new
// recording path (math.Float64bits + SubBucketLookup table) to check
// whether they produce identical bucket indices for the same values.
func TestBucketMappingConsistency(t *testing.T) {
	cfg := NewConfig(500, 60e9, 0.10) // schema 2

	// The new recording path, matching Record() exactly:
	newBucketIndex := func(v int64) int {
		fv := float64(v)
		bits := math.Float64bits(fv)
		exp := int(bits>>52) - 1022
		sub := int(cfg.SubBucketLookup[(bits>>subBucketLookupShift)&0xFF])
		if sub == cfg.NumSubBuckets {
			frac, frexpExp := math.Frexp(fv)
			sub = sort.SearchFloat64s(cfg.SubBucketBounds, frac)
			exp = frexpExp
		}
		key := sub + (exp-1)*cfg.NumSubBuckets
		idx := key - cfg.MinKey
		if idx < 0 {
			idx = 0
		} else if idx >= cfg.NumBuckets {
			idx = cfg.NumBuckets - 1
		}
		return idx
	}

	// The old recording path:
	oldBucketIndex := func(v int64) int {
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

	// Test specific values.
	specific := []int64{
		500, 501, 999, 1000, 1001, 1023, 1024, 1025,
		2047, 2048, 2049, 4096, 8192, 10000, 100000,
		1000000, 10000000, 100000000, 1000000000,
		30000000000, 59999999999, 60000000000,
	}

	mismatches := 0
	for _, v := range specific {
		oldIdx := oldBucketIndex(v)
		newIdx := newBucketIndex(v)
		if oldIdx != newIdx {
			t.Errorf("v=%d: old=%d new=%d (diff=%d)", v, oldIdx, newIdx, newIdx-oldIdx)
			mismatches++
		}
	}

	// Test random values across the range.
	rng := rand.New(rand.NewSource(42))
	logLo := math.Log(500)
	logHi := math.Log(60e9)
	for i := 0; i < 1000000; i++ {
		v := int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
		if v <= 0 {
			continue
		}
		oldIdx := oldBucketIndex(v)
		newIdx := newBucketIndex(v)
		if oldIdx != newIdx {
			if mismatches < 20 {
				t.Errorf("v=%d: old=%d new=%d (diff=%d)", v, oldIdx, newIdx, newIdx-oldIdx)
			}
			mismatches++
		}
	}

	if mismatches > 0 {
		t.Errorf("Total mismatches: %d", mismatches)
	} else {
		t.Logf("All bucket mappings match between old and new paths")
	}

	// Also test: do the Counts arrays differ when recording the same data?
	t.Run("full-histogram-comparison", func(t *testing.T) {
		h := New(500, 60e9, 0.10)

		// Record using the new path (Record method).
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

		// Build counts using the old path manually.
		oldCounts := make([]uint64, cfg.NumBuckets)
		for _, v := range values {
			idx := oldBucketIndex(v)
			oldCounts[idx]++
		}

		diffs := 0
		for i := range oldCounts {
			if oldCounts[i] != newSnap.Counts[i] {
				if diffs < 10 {
					lo := cfg.Boundaries[i]
					hi := cfg.Boundaries[min(i+1, len(cfg.Boundaries)-1)]
					t.Errorf("bucket %d [%.1f, %.1f]: old=%d new=%d",
						i, lo, hi, oldCounts[i], newSnap.Counts[i])
				}
				diffs++
			}
		}
		if diffs > 0 {
			t.Errorf("Total bucket count differences: %d / %d", diffs, cfg.NumBuckets)
		} else {
			t.Logf("All %d bucket counts match exactly", cfg.NumBuckets)
		}

		// If counts match, check that quantiles also match.
		if diffs == 0 {
			oldSnap := Snapshot{
				Config:     &cfg,
				Counts:     oldCounts,
				TotalCount: newSnap.TotalCount,
				TotalSum:   newSnap.TotalSum,
			}
			for _, q := range []float64{50, 75, 90, 95, 99, 99.9} {
				oldQ := oldSnap.ValueAtQuantile(q)
				newQ := newSnap.ValueAtQuantile(q)
				if oldQ != newQ {
					t.Errorf("p%.1f: old=%.6f new=%.6f", q, oldQ, newQ)
				}
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
				searchIdx := promBucketKey(testVal, cfg.Schema) - promBucketKey(testVal, cfg.Schema) // normalize
				// Actually compute both sub-bucket indices directly.
				frac, _ := math.Frexp(testVal)
				_ = frac
				oldKey := promBucketKey(testVal, cfg.Schema)
				newKey := lookupIdx + (int(bits>>52)-1022-1)*cfg.NumSubBuckets

				if oldKey != newKey && i < 5 {
					t.Logf("boundary[%d]=%.15f offset=%+e: oldKey=%d newKey=%d (DIFFER)",
						i, boundary, offset, oldKey, newKey)
				}
				_ = searchIdx
			}
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	_ = fmt.Sprintf
}
