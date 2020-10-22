// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// ZipfGenerator implements the Incrementing Zipfian Random Number Generator from
// [1]: "Quickly Generating Billion-Record Synthetic Databases"
// by Gray, Sundaresan, Englert, Baclawski, and Weinberger, SIGMOD 1994.

package ycsb

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/rand"
)

const (
	// See https://github.com/brianfrankcooper/YCSB/blob/f886c1e7988f8f4965cb88a1fe2f6bad2c61b56d/core/src/main/java/com/yahoo/ycsb/generator/ScrambledZipfianGenerator.java#L33-L35
	defaultIMax  = 10000000000
	defaultTheta = 0.99
	defaultZetaN = 26.46902820178302
)

// ZipfGenerator is a random number generator that generates draws from a Zipf
// distribution. Unlike rand.Zipf, this generator supports incrementing the
// imax parameter without performing an expensive recomputation of the
// underlying hidden parameters, which is a pattern used in [1] for efficiently
// generating large volumes of Zipf-distributed records for synthetic data.
// Second, rand.Zipf only supports theta <= 1, we suppose all values of theta.
type ZipfGenerator struct {
	// The underlying RNG
	zipfGenMu ZipfGeneratorMu
	// supplied values
	theta float64
	iMin  uint64
	// internally computed values
	alpha, zeta2, halfPowTheta float64
	verbose                    bool
}

// ZipfGeneratorMu holds variables which must be globally synced.
type ZipfGeneratorMu struct {
	mu    syncutil.Mutex
	r     *rand.Rand
	iMax  uint64
	eta   float64
	zetaN float64
}

// NewZipfGenerator constructs a new ZipfGenerator with the given parameters.
// It returns an error if the parameters are outside the accepted range.
func NewZipfGenerator(
	rng *rand.Rand, iMin, iMax uint64, theta float64, verbose bool,
) (*ZipfGenerator, error) {
	if iMin > iMax {
		return nil, errors.Errorf("iMin %d > iMax %d", iMin, iMax)
	}
	if theta < 0.0 || theta == 1.0 {
		return nil, errors.Errorf("0 < theta, and theta != 1")
	}

	z := ZipfGenerator{
		iMin: iMin,
		zipfGenMu: ZipfGeneratorMu{
			r:    rng,
			iMax: iMax,
		},
		theta:   theta,
		verbose: verbose,
	}
	z.zipfGenMu.mu.Lock()
	defer z.zipfGenMu.mu.Unlock()

	// Compute hidden parameters
	zeta2, err := computeZetaFromScratch(2, theta)
	if err != nil {
		return nil, errors.Errorf("Could not compute zeta(2,theta): %s", err)
	}
	var zetaN float64
	zetaN, err = computeZetaFromScratch(iMax+1-iMin, theta)
	if err != nil {
		return nil, errors.Errorf("Could not compute zeta(%d,theta): %s", iMax, err)
	}
	z.alpha = 1.0 / (1.0 - theta)
	z.zipfGenMu.eta = (1 - math.Pow(2.0/float64(z.zipfGenMu.iMax+1-z.iMin), 1.0-theta)) / (1.0 - zeta2/zetaN)
	z.zipfGenMu.zetaN = zetaN
	z.zeta2 = zeta2
	z.halfPowTheta = 1.0 + math.Pow(0.5, z.theta)
	return &z, nil
}

// computeZetaIncrementally recomputes zeta(iMax, theta), assuming that
// sum = zeta(oldIMax, theta). It returns zeta(iMax, theta), computed incrementally.
func computeZetaIncrementally(oldIMax, iMax uint64, theta float64, sum float64) (float64, error) {
	if iMax < oldIMax {
		return 0, errors.Errorf("Can't increment iMax backwards!")
	}
	for i := oldIMax + 1; i <= iMax; i++ {
		sum += 1.0 / math.Pow(float64(i), theta)
	}
	return sum, nil
}

// The function zeta computes the value
// zeta(n, theta) = (1/1)^theta + (1/2)^theta + (1/3)^theta + ... + (1/n)^theta
func computeZetaFromScratch(n uint64, theta float64) (float64, error) {
	if n == defaultIMax && theta == defaultTheta {
		// Precomputed value, borrowed from ScrambledZipfianGenerator.java. (This is
		// quite slow to calculate from scratch due to the large n value.)
		return defaultZetaN, nil
	}
	zeta, err := computeZetaIncrementally(0, n, theta, 0.0)
	if err != nil {
		return zeta, errors.Errorf("could not compute zeta: %s", err)
	}
	return zeta, nil
}

// Uint64 draws a new value between iMin and iMax, with probabilities
// according to the Zipf distribution.
func (z *ZipfGenerator) Uint64() uint64 {
	z.zipfGenMu.mu.Lock()
	u := z.zipfGenMu.r.Float64()
	uz := u * z.zipfGenMu.zetaN
	var result uint64
	if uz < 1.0 {
		result = z.iMin
	} else if uz < z.halfPowTheta {
		result = z.iMin + 1
	} else {
		spread := float64(z.zipfGenMu.iMax + 1 - z.iMin)
		result = z.iMin + uint64(int64(spread*math.Pow(z.zipfGenMu.eta*u-z.zipfGenMu.eta+1.0, z.alpha)))
	}
	if z.verbose {
		fmt.Printf("Uint64[%d, %d] -> %d\n", z.iMin, z.zipfGenMu.iMax, result)
	}
	z.zipfGenMu.mu.Unlock()
	return result
}

// IncrementIMax increments iMax by count and recomputes the internal values
// that depend on it. It throws an error if the recomputation failed.
func (z *ZipfGenerator) IncrementIMax(count uint64) error {
	z.zipfGenMu.mu.Lock()
	zetaN, err := computeZetaIncrementally(
		z.zipfGenMu.iMax+1-z.iMin, z.zipfGenMu.iMax+count+1-z.iMin, z.theta, z.zipfGenMu.zetaN)
	if err != nil {
		z.zipfGenMu.mu.Unlock()
		return errors.Errorf("Could not incrementally compute zeta: %s", err)
	}
	z.zipfGenMu.iMax += count
	eta := (1 - math.Pow(2.0/float64(z.zipfGenMu.iMax+1-z.iMin), 1.0-z.theta)) / (1.0 - z.zeta2/zetaN)
	z.zipfGenMu.eta = eta
	z.zipfGenMu.zetaN = zetaN
	z.zipfGenMu.mu.Unlock()
	return nil
}
