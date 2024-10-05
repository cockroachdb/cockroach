// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ycsb

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/exp/rand"
)

// UniformGenerator is a random number generator that generates draws from a
// uniform distribution.
type UniformGenerator struct {
	iMin uint64
	mu   struct {
		syncutil.Mutex
		r    *rand.Rand
		iMax uint64
	}
}

// NewUniformGenerator constructs a new UniformGenerator with the given parameters.
// It returns an error if the parameters are outside the accepted range.
func NewUniformGenerator(rng *rand.Rand, iMin, iMax uint64) (*UniformGenerator, error) {

	z := UniformGenerator{}
	z.iMin = iMin
	z.mu.r = rng
	z.mu.iMax = iMax

	return &z, nil
}

// IncrementIMax increments iMax by count.
func (z *UniformGenerator) IncrementIMax(count uint64) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.mu.iMax += count
	return nil
}

// Uint64 returns a random Uint64 between iMin and iMax, drawn from a uniform
// distribution.
func (z *UniformGenerator) Uint64() uint64 {
	z.mu.Lock()
	defer z.mu.Unlock()
	return (uint64)(z.mu.r.Int63n((int64)(z.mu.iMax-z.iMin+1))) + z.iMin
}
