// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// PercentileEstimator stores a uniform random sample of a data stream in order
// to estimate percentiles (e.g. p50, p90, p99). It is thread-safe.
type PercentileEstimator struct {
	mu struct {
		syncutil.Mutex

		// reservoir records a uniform random sample of incoming data points.
		reservoir []float64
		// capacity is the maximum size of the reservoir.
		capacity int
		// count records the total number of observations seen.
		count int
	}
}

// NewPercentileEstimator initializes a new estimator with a fixed capacity.
func NewPercentileEstimator(capacity int) *PercentileEstimator {
	estimator := &PercentileEstimator{}
	estimator.mu.reservoir = make([]float64, 0, capacity)
	estimator.mu.capacity = capacity
	return estimator
}

// Add records a new observation in the reservoir (with uniform probability).
func (rs *PercentileEstimator) Add(value float64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.mu.count++
	if len(rs.mu.reservoir) < rs.mu.capacity {
		// Reservoir not yet full, just append.
		rs.mu.reservoir = append(rs.mu.reservoir, value)
	} else {
		// Reservoir full, replace an element at random.
		r := rand.Intn(rs.mu.count)
		if r < rs.mu.capacity {
			rs.mu.reservoir[r] = value
		}
	}
}

// Estimate returns the given percentile (approximate) from the reservoir.
// "percentile" must be between 0 and 1, inclusive.
func (rs *PercentileEstimator) Estimate(percentile float64) float64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(rs.mu.reservoir) == 0 {
		return 0
	}

	// Sort the reservoir and return the requested percentile value.
	size := min(rs.mu.count, rs.mu.capacity)
	sort.Float64s(rs.mu.reservoir[:size])

	idx := int(percentile * float64(size))
	if idx >= size {
		idx = size - 1
	}
	return rs.mu.reservoir[idx]
}
