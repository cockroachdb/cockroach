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

	"github.com/cheekybits/genny/generic"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Element is the generic type.
type Element generic.Type

// ElementWeight is the generic weight type.
type ElementWeight struct {
	weight int
	elem   Element
}

// NewElementWeightedSampler creates a ElementSampler that produces
// Elements. They are returned at the relative frequency of the values of
// weights. All weights must be >= 1.
func NewWeightedElementSampler(weights []ElementWeight, seed int64) *ElementSampler {
	sum := 0
	for _, w := range weights {
		if w.weight < 1 {
			panic("expected weight >= 1")
		}
		sum += w.weight
	}
	if sum == 0 {
		panic("expected weights")
	}
	samples := make([]Element, sum)
	pos := 0
	for _, w := range weights {
		for count := 0; count < w.weight; count++ {
			samples[pos] = w.elem
			pos++
		}
	}
	return &ElementSampler{
		rnd:     rand.New(rand.NewSource(seed)),
		samples: samples,
	}
}

// ElementSampler is a weighted Element sampler.
type ElementSampler struct {
	mu      syncutil.Mutex
	rnd     *rand.Rand
	samples []Element
}

// Next returns the next weighted sample.
func (w *ElementSampler) Next() Element {
	w.mu.Lock()
	v := w.samples[w.rnd.Intn(len(w.samples))]
	w.mu.Unlock()
	return v
}
