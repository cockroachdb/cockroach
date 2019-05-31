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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package ycsb

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SkewedLatestGenerator is a random number generator that generates numbers in
// the range [iMin, iMax], but skews it towards iMax using a zipfian
// distribution.
type SkewedLatestGenerator struct {
	mu struct {
		syncutil.Mutex
		iMax    uint64
		zipfGen *ZipfGenerator
	}
}

// NewSkewedLatestGenerator constructs a new SkewedLatestGenerator with the
// given parameters. It returns an error if the parameters are outside the
// accepted range.
func NewSkewedLatestGenerator(
	rng *rand.Rand, iMin, iMax uint64, theta float64, verbose bool,
) (*SkewedLatestGenerator, error) {

	z := SkewedLatestGenerator{}
	z.mu.iMax = iMax
	zipfGen, err := NewZipfGenerator(rng, 0, iMax-iMin, theta, verbose)
	if err != nil {
		return nil, err
	}
	z.mu.zipfGen = zipfGen

	return &z, nil
}

// IncrementIMax increments iMax by count.
func (z *SkewedLatestGenerator) IncrementIMax(count uint64) error {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.mu.iMax += count
	return z.mu.zipfGen.IncrementIMax(count)
}

// Uint64 returns a random Uint64 between iMin and iMax, where keys near iMax
// are most likely to be drawn.
func (z *SkewedLatestGenerator) Uint64() uint64 {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.mu.iMax - z.mu.zipfGen.Uint64()
}
