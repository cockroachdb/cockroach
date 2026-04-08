// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goodhistogram

import "math"

// ValueAtQuantile returns the estimated value at the given quantile q ∈ [0, 100]
// using trapezoidal interpolation.
//
// Unlike the standard linear (uniform-density) interpolation used by Prometheus,
// trapezoidal interpolation estimates the probability density at each bucket
// boundary by averaging the densities of adjacent buckets. Within each bucket
// the density is modeled as varying linearly from the left boundary density to
// the right boundary density — a trapezoid. The CDF within a bucket is then the
// integral of this linear density, which is a quadratic. The quantile value is
// found by solving this quadratic.
//
// This produces more accurate estimates when the true distribution is not
// uniform within buckets (i.e., nearly always), especially near bucket
// boundaries where the density changes.
func (s *Snapshot) ValueAtQuantile(q float64) float64 {
	if s.TotalCount == 0 {
		return 0
	}
	// Target rank (fractional, 0-based) across all observations,
	// including underflow and overflow.
	rank := (q / 100.0) * float64(s.TotalCount)
	if rank <= 0 {
		return s.Config.Boundaries[0]
	}
	if rank >= float64(s.TotalCount) {
		return s.Config.Boundaries[len(s.Config.Boundaries)-1]
	}

	// Underflow and zero observations form an implicit region below lo.
	// If the quantile falls within this region, clamp to lo.
	belowLo := float64(s.ZeroCount + s.Underflow)
	if rank <= belowLo {
		return s.Config.Lo
	}
	// Adjust rank to be relative to the in-range buckets.
	rank -= belowLo

	// Count of in-range observations (buckets only, excluding
	// underflow, overflow, and zeros).
	var inRangeCount uint64
	for _, c := range s.Counts {
		inRangeCount += c
	}

	// If the quantile falls beyond all in-range observations, it's
	// in the overflow region. Clamp to hi, matching Prometheus's
	// behavior of clamping to the last explicit bucket boundary.
	if rank > float64(inRangeCount) {
		return s.Config.Hi
	}

	n := len(s.Counts)

	// Step 1: Compute average density in each bucket.
	// density[i] = count[i] / width[i]
	avgDensity := make([]float64, n)
	for i := 0; i < n; i++ {
		w := s.Config.Boundaries[i+1] - s.Config.Boundaries[i]
		if w > 0 && s.Counts[i] > 0 {
			avgDensity[i] = float64(s.Counts[i]) / w
		}
	}

	// Step 2: Estimate density at each boundary by averaging neighbors.
	// boundaryDensity has length n+1 (one per boundary).
	boundaryDensity := make([]float64, n+1)
	for i := 0; i <= n; i++ {
		switch {
		case i == 0:
			boundaryDensity[i] = avgDensity[0]
		case i == n:
			boundaryDensity[i] = avgDensity[n-1]
		default:
			boundaryDensity[i] = (avgDensity[i-1] + avgDensity[i]) / 2.0
		}
	}

	// Step 3: Walk buckets to find which one contains the target rank,
	// then solve the trapezoidal CDF within that bucket.
	var cumCount float64
	for i := 0; i < n; i++ {
		fc := float64(s.Counts[i])
		if cumCount+fc >= rank {
			localRank := rank - cumCount
			lo := s.Config.Boundaries[i]
			hi := s.Config.Boundaries[i+1]
			w := hi - lo
			if w <= 0 || fc == 0 {
				return lo
			}

			dL := boundaryDensity[i]
			dR := boundaryDensity[i+1]

			return trapezoidalSolve(lo, w, fc, dL, dR, localRank)
		}
		cumCount += fc
	}
	// Should not reach here, but clamp to upper bound.
	return s.Config.Boundaries[n]
}

// Mean returns the mean of all recorded values. Returns NaN if no
// observations have been recorded, matching Prometheus histogram behavior.
func (s *Snapshot) Mean() float64 {
	if s.TotalCount == 0 {
		return math.NaN()
	}
	return float64(s.TotalSum) / float64(s.TotalCount)
}

// Total returns the total count and sum.
func (s *Snapshot) Total() (int64, float64) {
	return int64(s.TotalCount), float64(s.TotalSum)
}

// trapezoidalSolve finds the value x ∈ [lo, lo+w] such that the area under a
// linear density function from lo to x equals localRank.
//
// The density is modeled as f(t) = dL + (dR - dL) * (t - lo) / w, where dL and
// dR are the densities at the left and right bucket boundaries. However, the
// raw boundary densities may not integrate to exactly the bucket count over
// [lo, lo+w], so we scale them: the area of the trapezoid w*(dL+dR)/2 should
// equal the bucket count fc.
//
// The CDF within the bucket is:
//
//	F(x) = a*x + b*x^2  (with x measured from lo)
//
// where a = scaled dL, b = (scaled dR - scaled dL) / (2*w).
// We solve a*x + b*x^2 = localRank via the quadratic formula.
func trapezoidalSolve(lo, w, fc, dL, dR, localRank float64) float64 {
	// Scale boundary densities so the trapezoid area matches the count.
	rawArea := w * (dL + dR) / 2.0
	if rawArea <= 0 {
		// Both boundary densities are zero — fall back to linear interpolation.
		return lo + w*(localRank/fc)
	}
	scale := fc / rawArea
	sL := dL * scale // scaled left density
	sR := dR * scale // scaled right density

	// Degenerate case: uniform density (sL ≈ sR). Linear interpolation.
	if math.Abs(sR-sL) < 1e-12*(sL+sR) {
		return lo + w*(localRank/fc)
	}

	// Quadratic: sL*x + ((sR-sL)/(2*w))*x^2 = localRank
	// Rewrite as: ((sR-sL)/(2*w))*x^2 + sL*x - localRank = 0
	a := (sR - sL) / (2.0 * w)
	b := sL
	c := -localRank
	disc := b*b - 4*a*c
	if disc < 0 {
		// Numerical issue; fall back to linear.
		return lo + w*(localRank/fc)
	}
	// Use the numerically stable quadratic formula.
	var x float64
	if b >= 0 {
		x = (2 * c) / (-b - math.Sqrt(disc))
	} else {
		x = (-b + math.Sqrt(disc)) / (2 * a)
	}

	// Clamp to bucket range.
	if x < 0 {
		x = 0
	} else if x > w {
		x = w
	}
	return lo + x
}
