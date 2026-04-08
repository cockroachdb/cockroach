// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base2histogram

import "math"

// ValueAtQuantile returns the estimated value at the given quantile q ∈ [0, 100]
// using slope-clamped trapezoidal interpolation.
//
// This is the key innovation of base2histogram over simpler midpoint-return
// approaches. The density within a bucket is modeled as a linear function
// whose slope is derived from neighboring buckets. The slope is clamped to
// prevent the density from going negative at either bucket boundary.
//
// The method uses region sums to accelerate finding the target bucket:
// first scan the ~16 region sums to find the right 16-bucket window, then
// scan individual buckets within that window.
func (s *Snapshot) ValueAtQuantile(q float64) float64 {
	if s.TotalCount == 0 {
		return 0
	}
	rank := (q / 100.0) * float64(s.TotalCount)
	if rank <= 0 {
		// Find first non-zero bucket.
		for i, c := range s.Counts {
			if c > 0 {
				return float64(BucketMinValue(i))
			}
		}
		return 0
	}
	if rank >= float64(s.TotalCount) {
		// Find last non-zero bucket.
		for i := NumBuckets - 1; i >= 0; i-- {
			if s.Counts[i] > 0 {
				return float64(BucketMaxValue(i))
			}
		}
		return 0
	}

	// Step 1: Use region sums to find the region containing the target rank.
	var cumCount float64
	regionIdx := 0
	for regionIdx < NumRegions {
		rc := float64(s.RegionSums[regionIdx])
		if cumCount+rc >= rank {
			break
		}
		cumCount += rc
		regionIdx++
	}

	// Step 2: Scan individual buckets within the region.
	startBucket := regionIdx * RegionSize
	endBucket := startBucket + RegionSize
	if endBucket > NumBuckets {
		endBucket = NumBuckets
	}

	bucketIdx := startBucket
	for bucketIdx < endBucket {
		bc := float64(s.Counts[bucketIdx])
		if cumCount+bc >= rank {
			break
		}
		cumCount += bc
		bucketIdx++
	}
	if bucketIdx >= NumBuckets {
		bucketIdx = NumBuckets - 1
	}

	// Step 3: Interpolate within the target bucket using slope-clamped
	// trapezoidal estimation.
	localRank := rank - cumCount
	return s.interpolate(bucketIdx, localRank)
}

// interpolate estimates the value within bucket idx for the given local rank
// (fractional count within the bucket) using slope-clamped trapezoidal
// interpolation.
func (s *Snapshot) interpolate(idx int, localRank float64) float64 {
	c := float64(s.Counts[idx])
	if c == 0 {
		return float64(BucketMinValue(idx))
	}

	lo := float64(BucketMinValue(idx))
	hi := float64(BucketMaxValue(idx)) + 1 // exclusive upper bound
	w := hi - lo

	// For single-width buckets (exact mapping region), return the exact value.
	if w <= 1 {
		return lo
	}

	// Compute average density in this bucket.
	d := c / w

	// Compute density slope from neighboring buckets.
	var dLeft, dRight float64
	var midLeft, midRight float64

	if idx > 0 && s.Counts[idx-1] > 0 {
		wLeft := float64(BucketWidth(idx - 1))
		dLeft = float64(s.Counts[idx-1]) / wLeft
		midLeft = BucketMidpoint(idx - 1)
	} else {
		dLeft = d
		midLeft = lo - w/2
	}

	if idx < NumBuckets-1 && s.Counts[idx+1] > 0 {
		wRight := float64(BucketWidth(idx + 1))
		dRight = float64(s.Counts[idx+1]) / wRight
		midRight = BucketMidpoint(idx + 1)
	} else {
		dRight = d
		midRight = hi + w/2
	}

	// Compute density slope.
	var k float64
	span := midRight - midLeft
	if span > 0 {
		k = (dRight - dLeft) / span
	}

	// Clamp slope so density stays non-negative across the bucket.
	// The density model is: f(x) = a + k*x, where a = d - k*w/2.
	// For f(0) >= 0: a >= 0 => k <= 2*d/w
	// For f(w) >= 0: a + k*w >= 0 => k >= -2*d/w (when d > 0)
	if d > 0 {
		maxK := 2 * d / w
		if k > maxK {
			k = maxK
		}
		if k < -maxK {
			k = -maxK
		}
	} else {
		k = 0
	}

	// Density model: f(x) = a + k*x, where x is offset from lo.
	// a = d - k*w/2 (so that integral over [0, w] = d*w = c... approximately)
	a := d - k*w/2

	// Solve CDF: a*x + k*x^2/2 = localRank
	// Rewrite as: (k/2)*x^2 + a*x - localRank = 0
	if math.Abs(k) < 1e-15 {
		// Near-zero slope: uniform density.
		x := localRank / d
		return lo + clampFloat(x, 0, w)
	}

	qa := k / 2
	qb := a
	qc := -localRank
	disc := qb*qb - 4*qa*qc
	if disc < 0 {
		// Numerical issue; fall back to uniform.
		return lo + clampFloat(localRank/d, 0, w)
	}

	// Numerically stable quadratic formula.
	var x float64
	if qb >= 0 {
		x = (2 * qc) / (-qb - math.Sqrt(disc))
	} else {
		x = (-qb + math.Sqrt(disc)) / (2 * qa)
	}

	return lo + clampFloat(x, 0, w)
}

// Mean returns the mean of all recorded values.
func (s *Snapshot) Mean() float64 {
	if s.TotalCount == 0 {
		return 0
	}
	return float64(s.TotalSum) / float64(s.TotalCount)
}

// Total returns the total count and sum.
func (s *Snapshot) Total() (int64, float64) {
	return int64(s.TotalCount), float64(s.TotalSum)
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
