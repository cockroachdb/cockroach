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

type element generic.Type

// elementWeight is the generic weight type.
type elementWeight struct {
	weight int
	elem   element
}

// newWeightedelementSampler creates a elementSampler that produces
// elements. They are returned at the relative frequency of the values of
// weights. All weights must be >= 1.
func newWeightedelementSampler(weights []elementWeight, seed int64) *elementSampler {
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
	samples := make([]element, sum)
	pos := 0
	for _, w := range weights {
		for count := 0; count < w.weight; count++ {
			samples[pos] = w.elem
			pos++
		}
	}
	return &elementSampler{
		rnd:     rand.New(rand.NewSource(seed)),
		samples: samples,
	}
}

type elementSampler struct {
	mu      syncutil.Mutex
	rnd     *rand.Rand
	samples []element
}

func (w *elementSampler) Next() element {
	w.mu.Lock()
	v := w.samples[w.rnd.Intn(len(w.samples))]
	w.mu.Unlock()
	return v
}
