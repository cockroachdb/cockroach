// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"math"
	"sort"
)

// Sample is a collection of possibly weighted data points.
type Sample struct {
	// Xs is the slice of sample values.
	Xs []float64

	// Weights[i] is the weight of sample Xs[i].  If Weights is
	// nil, all Xs have weight 1.  Weights must have the same
	// length of Xs and all values must be non-negative.
	Weights []float64

	// Sorted indicates that Xs is sorted in ascending order.
	Sorted bool
}

// Bounds returns the minimum and maximum values of xs.
func Bounds(xs []float64) (min float64, max float64) {
	if len(xs) == 0 {
		return math.NaN(), math.NaN()
	}
	min, max = xs[0], xs[0]
	for _, x := range xs {
		if x < min {
			min = x
		}
		if x > max {
			max = x
		}
	}
	return
}

// Bounds returns the minimum and maximum values of the Sample.
//
// If the Sample is weighted, this ignores samples with zero weight.
//
// This is constant time if s.Sorted and there are no zero-weighted
// values.
func (s Sample) Bounds() (min float64, max float64) {
	if len(s.Xs) == 0 || (!s.Sorted && s.Weights == nil) {
		return Bounds(s.Xs)
	}

	if s.Sorted {
		if s.Weights == nil {
			return s.Xs[0], s.Xs[len(s.Xs)-1]
		}
		min, max = math.NaN(), math.NaN()
		for i, w := range s.Weights {
			if w != 0 {
				min = s.Xs[i]
				break
			}
		}
		if math.IsNaN(min) {
			return
		}
		for i := range s.Weights {
			if s.Weights[len(s.Weights)-i-1] != 0 {
				max = s.Xs[len(s.Weights)-i-1]
				break
			}
		}
	} else {
		min, max = math.Inf(1), math.Inf(-1)
		for i, x := range s.Xs {
			w := s.Weights[i]
			if x < min && w != 0 {
				min = x
			}
			if x > max && w != 0 {
				max = x
			}
		}
		if math.IsInf(min, 0) {
			min, max = math.NaN(), math.NaN()
		}
	}
	return
}

// vecSum returns the sum of xs.
func vecSum(xs []float64) float64 {
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum
}

// Sum returns the (possibly weighted) sum of the Sample.
func (s Sample) Sum() float64 {
	if s.Weights == nil {
		return vecSum(s.Xs)
	}
	sum := 0.0
	for i, x := range s.Xs {
		sum += x * s.Weights[i]
	}
	return sum
}

// Weight returns the total weight of the Sasmple.
func (s Sample) Weight() float64 {
	if s.Weights == nil {
		return float64(len(s.Xs))
	}
	return vecSum(s.Weights)
}

// Mean returns the arithmetic mean of xs.
func Mean(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	m := 0.0
	for i, x := range xs {
		m += (x - m) / float64(i+1)
	}
	return m
}

// Mean returns the arithmetic mean of the Sample.
func (s Sample) Mean() float64 {
	if len(s.Xs) == 0 || s.Weights == nil {
		return Mean(s.Xs)
	}

	m, wsum := 0.0, 0.0
	for i, x := range s.Xs {
		// Use weighted incremental mean:
		//   m_i = (1 - w_i/wsum_i) * m_(i-1) + (w_i/wsum_i) * x_i
		//       = m_(i-1) + (x_i - m_(i-1)) * (w_i/wsum_i)
		w := s.Weights[i]
		wsum += w
		m += (x - m) * w / wsum
	}
	return m
}

// GeoMean returns the geometric mean of xs. xs must be positive.
func GeoMean(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	m := 0.0
	for i, x := range xs {
		if x <= 0 {
			return math.NaN()
		}
		lx := math.Log(x)
		m += (lx - m) / float64(i+1)
	}
	return math.Exp(m)
}

// GeoMean returns the geometric mean of the Sample. All samples
// values must be positive.
func (s Sample) GeoMean() float64 {
	if len(s.Xs) == 0 || s.Weights == nil {
		return GeoMean(s.Xs)
	}

	m, wsum := 0.0, 0.0
	for i, x := range s.Xs {
		w := s.Weights[i]
		wsum += w
		lx := math.Log(x)
		m += (lx - m) * w / wsum
	}
	return math.Exp(m)
}

// Variance returns the sample variance of xs.
func Variance(xs []float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	} else if len(xs) <= 1 {
		return 0
	}

	// Based on Wikipedia's presentation of Welford 1962
	// (http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm).
	// This is more numerically stable than the standard two-pass
	// formula and not prone to massive cancellation.
	mean, M2 := 0.0, 0.0
	for n, x := range xs {
		delta := x - mean
		mean += delta / float64(n+1)
		M2 += delta * (x - mean)
	}
	return M2 / float64(len(xs)-1)
}

func (s Sample) Variance() float64 {
	if len(s.Xs) == 0 || s.Weights == nil {
		return Variance(s.Xs)
	}
	// TODO(austin)
	panic("Weighted Variance not implemented")
}

// StdDev returns the sample standard deviation of xs.
func StdDev(xs []float64) float64 {
	return math.Sqrt(Variance(xs))
}

// StdDev returns the sample standard deviation of the Sample.
func (s Sample) StdDev() float64 {
	if len(s.Xs) == 0 || s.Weights == nil {
		return StdDev(s.Xs)
	}
	// TODO(austin)
	panic("Weighted StdDev not implemented")
}

// Percentile returns the pctileth value from the Sample. This uses
// interpolation method R8 from Hyndman and Fan (1996).
//
// pctile will be capped to the range [0, 1]. If len(xs) == 0 or all
// weights are 0, returns NaN.
//
// Percentile(0.5) is the median. Percentile(0.25) and
// Percentile(0.75) are the first and third quartiles, respectively.
//
// This is constant time if s.Sorted and s.Weights == nil.
func (s Sample) Percentile(pctile float64) float64 {
	if len(s.Xs) == 0 {
		return math.NaN()
	} else if pctile <= 0 {
		min, _ := s.Bounds()
		return min
	} else if pctile >= 1 {
		_, max := s.Bounds()
		return max
	}

	if !s.Sorted {
		// TODO(austin) Use select algorithm instead
		s = *s.Copy().Sort()
	}

	if s.Weights == nil {
		N := float64(len(s.Xs))
		//n := pctile * (N + 1) // R6
		n := 1/3.0 + pctile*(N+1/3.0) // R8
		kf, frac := math.Modf(n)
		k := int(kf)
		if k <= 0 {
			return s.Xs[0]
		} else if k >= len(s.Xs) {
			return s.Xs[len(s.Xs)-1]
		}
		return s.Xs[k-1] + frac*(s.Xs[k]-s.Xs[k-1])
	} else {
		// TODO(austin): Implement interpolation

		target := s.Weight() * pctile

		// TODO(austin) If we had cumulative weights, we could
		// do this in log time.
		for i, weight := range s.Weights {
			target -= weight
			if target < 0 {
				return s.Xs[i]
			}
		}
		return s.Xs[len(s.Xs)-1]
	}
}

// IQR returns the interquartile range of the Sample.
//
// This is constant time if s.Sorted and s.Weights == nil.
func (s Sample) IQR() float64 {
	if !s.Sorted {
		s = *s.Copy().Sort()
	}
	return s.Percentile(0.75) - s.Percentile(0.25)
}

type sampleSorter struct {
	xs      []float64
	weights []float64
}

func (p *sampleSorter) Len() int {
	return len(p.xs)
}

func (p *sampleSorter) Less(i, j int) bool {
	return p.xs[i] < p.xs[j]
}

func (p *sampleSorter) Swap(i, j int) {
	p.xs[i], p.xs[j] = p.xs[j], p.xs[i]
	p.weights[i], p.weights[j] = p.weights[j], p.weights[i]
}

// Sort sorts the samples in place in s and returns s.
//
// A sorted sample improves the performance of some algorithms.
func (s *Sample) Sort() *Sample {
	if s.Sorted || sort.Float64sAreSorted(s.Xs) {
		// All set
	} else if s.Weights == nil {
		sort.Float64s(s.Xs)
	} else {
		sort.Sort(&sampleSorter{s.Xs, s.Weights})
	}
	s.Sorted = true
	return s
}

// Copy returns a copy of the Sample.
//
// The returned Sample shares no data with the original, so they can
// be modified (for example, sorted) independently.
func (s Sample) Copy() *Sample {
	xs := make([]float64, len(s.Xs))
	copy(xs, s.Xs)

	weights := []float64(nil)
	if s.Weights != nil {
		weights = make([]float64, len(s.Weights))
		copy(weights, s.Weights)
	}

	return &Sample{xs, weights, s.Sorted}
}
