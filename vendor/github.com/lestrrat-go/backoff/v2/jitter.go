package backoff

import (
	"math/rand"
	"time"
)

type jitter interface {
	apply(interval float64) float64
}

func newJitter(jitterFactor float64, rng Random) jitter {
	if jitterFactor <= 0 || jitterFactor >= 1 {
		return newNopJitter()
	}
	return newRandomJitter(jitterFactor, rng)
}

type nopJitter struct{}

func newNopJitter() *nopJitter {
	return &nopJitter{}
}

func (j *nopJitter) apply(interval float64) float64 {
	return interval
}

type randomJitter struct {
	jitterFactor float64
	rng          Random
}

func newRandomJitter(jitterFactor float64, rng Random) *randomJitter {
	if rng == nil {
		// if we have a jitter factor, and no RNG is provided, create one.
		// This is definitely not "secure", but well, if you care enough,
		// you would provide one
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	return &randomJitter{
		jitterFactor: jitterFactor,
		rng:          rng,
	}
}

func (j *randomJitter) apply(interval float64) float64 {
	jitterDelta := interval * j.jitterFactor
	jitterMin := interval - jitterDelta
	jitterMax := interval + jitterDelta

	// Get a random value from the range [minInterval, maxInterval].
	// The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
	// we want a 33% chance for selecting either 1, 2 or 3.
	//
	// see also: https://github.com/cenkalti/backoff/blob/c2975ffa541a1caeca5f76c396cb8c3e7b3bb5f8/exponential.go#L154-L157
	return jitterMin + j.rng.Float64()*(jitterMax-jitterMin+1)
}
