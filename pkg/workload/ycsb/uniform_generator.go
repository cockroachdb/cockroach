// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package ycsb

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"math/rand"
)

// UniformGenerator is a random number generator that generates draws from a
// uniform distribution.
type UniformGenerator struct {
	mu struct {
		syncutil.Mutex
		r        *rand.Rand
		sequence uint64
	}
}

// NewUniformGenerator constructs a new UniformGenerator with the given parameters.
// It returns an error if the parameters are outside the accepted range.
func NewUniformGenerator(rng *rand.Rand, minInsertRow uint64) (*UniformGenerator, error) {

	z := UniformGenerator{}
	z.mu.r = rng
	z.mu.sequence = minInsertRow

	return &z, nil
}

// IMaxHead returns the current value of IMaxHead, without incrementing.
func (z *UniformGenerator) IMaxHead() uint64 {
	z.mu.Lock()
	max := z.mu.sequence
	z.mu.Unlock()
	return max
}

// IncrementIMax increments the sequence number.
func (z *UniformGenerator) IncrementIMax() error {
	z.mu.Lock()
	z.mu.sequence++
	z.mu.Unlock()
	return nil
}

// Uint64 returns a random Uint64 between min and sequence, drawn from a uniform
// distribution.
func (z *UniformGenerator) Uint64() uint64 {
	z.mu.Lock()
	result := rand.Uint64() % z.mu.sequence
	z.mu.Unlock()
	return result
}
