// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"math/rand"
	"time"
)

// Jitter returns a duration that is randomly adjusted by a percentage of the original interval.
// The returned duration will be in the range of [interval*(1-percentage), interval*(1+percentage)].
// It uses the default random source from the math/rand package.
func Jitter(interval time.Duration, percentage float64) time.Duration {
	return JitterWithRand(interval, percentage, rand.Float64)
}

// JitterWithRand returns a duration that is randomly adjusted by a percentage of the original interval
// using the provided random float generator function.
// Parameters:
//   - interval: The base time duration
//   - percentage: The maximum percentage of variation (must be between 0 and 1)
//   - randFloat: A function that returns a random float64 in the range [0.0, 1.0)
//
// The returned duration will be in the range of [interval*(1-percentage), interval*(1+percentage)].
// Panics if percentage is not in the range [0, 1].
func JitterWithRand(
	interval time.Duration, percentage float64, randFloat func() float64,
) time.Duration {
	if percentage < 0 || percentage > 1 {
		panic("Jitter percentage out of range")
	}
	zJitter := 1 - percentage + 2*percentage*randFloat()
	return time.Duration(float64(interval) * zJitter)
}
