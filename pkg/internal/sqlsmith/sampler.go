// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
