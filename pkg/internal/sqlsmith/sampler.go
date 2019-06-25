// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// NewWeightedSampler creates a WeightedSampler that produces indexes
// corresponding to positions in weights. They are returned at the relative
// frequency of the values of weights. For example, if weights is {1, 3},
// reading 4 values from Next would return `0` one time and `1` three times
// in some random order. All weights must be >= 1.
func NewWeightedSampler(weights []int, seed int64) *WeightedSampler {
	sum := 0
	for _, w := range weights {
		if w < 1 {
			panic("expected weight >= 1")
		}
		sum += w
	}
	if sum == 0 {
		panic("expected weights")
	}
	samples := make([]int, sum)
	pos := 0
	for wi, w := range weights {
		for count := 0; count < w; count++ {
			samples[pos] = wi
			pos++
		}
	}
	return &WeightedSampler{
		rnd:     rand.New(rand.NewSource(seed)),
		samples: samples,
	}
}

// WeightedSampler is a weighted sampler.
type WeightedSampler struct {
	mu      syncutil.Mutex
	rnd     *rand.Rand
	samples []int
}

// Next returns the next weighted sample index.
func (w *WeightedSampler) Next() int {
	w.mu.Lock()
	v := w.samples[w.rnd.Intn(len(w.samples))]
	w.mu.Unlock()
	return v
}
