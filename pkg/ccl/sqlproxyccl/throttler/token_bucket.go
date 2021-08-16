// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import "time"

// BucketPolicy controls the behavior of a token bucket.
type BucketPolicy struct {
	// The maximum number of tokens in the bucket.
	Capacity int
	// Controls how often a token is added to the bucket. A fill period
	// of one second would add a token to the bucket every second.
	//
	// Setting FillPeriod to zero disables the token bucket and the bucket
	// will act as if it contains an infinite number of tokens.
	FillPeriod time.Duration
}

// tokenBucket is not thread safe.
type tokenBucket struct {
	// The policy defining the bucket's behavior.
	policy BucketPolicy
	// The most recent time a token was added to the bucket.
	filledAt time.Time
	// The number of tokens contained within the bucket.
	tokens uint
}

func newBucket(now time.Time, policy BucketPolicy) *tokenBucket {
	return &tokenBucket{
		policy:   policy,
		filledAt: now,
		tokens:   uint(policy.Capacity),
	}
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// Add tokens to the bucket. The number of tokens added depends on the time
// passed since the last call to fill.
func (b *tokenBucket) fill(now time.Time) {
	if b.policy.FillPeriod == 0 {
		return
	}
	duration := now.Sub(b.filledAt)
	if duration < 1 {
		/* if the clock rolled back, reset the filledAt to now */
		b.filledAt = now
		return
	}

	tokensToRefill := duration / b.policy.FillPeriod
	// Compute filledAt instead of setting filledAt := now in order to
	// keep the remainder.
	b.filledAt = b.filledAt.Add(tokensToRefill * b.policy.FillPeriod)
	b.tokens = min(b.tokens+uint(tokensToRefill), uint(b.policy.Capacity))
}

// Attempt to remove a token from the bucket. Returns true if there
// was a token available.
func (b *tokenBucket) tryConsume() bool {
	if b.policy.FillPeriod == 0 {
		return true
	}
	if b.tokens == 0 {
		return false
	}
	b.tokens--
	return true
}

// Return a token to the bucket. Called if the token is no longer needed.
func (b *tokenBucket) returnToken() {
	b.tokens = min(b.tokens+1, uint(b.policy.Capacity))
}
