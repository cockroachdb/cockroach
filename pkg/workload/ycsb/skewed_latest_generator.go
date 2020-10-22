// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ycsb

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/exp/rand"
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
