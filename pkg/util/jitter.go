package util

import (
	"math/rand"
	"time"
)

// Jitter returns a jitter adjusted duration using fraction.
// The returned duration is a random duration between
// (1-jitter)*d and (1+jitter)*d
func Jitter(d time.Duration, fraction float64) time.Duration {
	jitterFraction := 1 + (2*rand.Float64()-1)*fraction
	return time.Duration(float64(d) * jitterFraction)
}
