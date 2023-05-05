// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Tokens are abstract units (usually units of work).
type Tokens float64

// TokensPerSecond is the rate of token replenishment.
type TokensPerSecond float64

// TokenBucket implements the basic accounting for a token bucket.
//
// A token bucket has a rate of replenishment and a burst limit. Tokens are
// replenished over time, up to the burst limit.
//
// The token bucket keeps track of the current amount and updates it as time
// passes. The bucket can go into debt (i.e. negative current amount).
type TokenBucket struct {
	rate       TokensPerSecond
	burst      Tokens
	timeSource timeutil.TimeSource

	current     Tokens
	lastUpdated time.Time

	exhaustedStart  time.Time
	exhaustedMicros int64
}

// Init the token bucket.
func (tb *TokenBucket) Init(rate TokensPerSecond, burst Tokens, timeSource timeutil.TimeSource) {
	*tb = TokenBucket{
		rate:        rate,
		burst:       burst,
		timeSource:  timeSource,
		current:     burst,
		lastUpdated: timeSource.Now(),
	}
}

// Update moves the time forward, accounting for the replenishment since the
// last update.
func (tb *TokenBucket) Update() {
	now := tb.timeSource.Now()
	if since := now.Sub(tb.lastUpdated); since > 0 {
		tb.current += Tokens(float64(tb.rate) * since.Seconds())

		if tb.current > tb.burst {
			tb.current = tb.burst
		}
		tb.lastUpdated = now
		tb.updateExhaustedMicros()
	}
}

// UpdateConfig updates the rate and burst limits. The change in burst will be
// applied to the current token quantity. For example, if the RateLimiter
// currently had 5 available tokens and the burst is updated from 10 to 20, the
// amount will increase to 15. Similarly, if the burst is decreased by 10, the
// current quota will decrease accordingly, potentially putting the limiter into
// debt.
func (tb *TokenBucket) UpdateConfig(rate TokensPerSecond, burst Tokens) {
	tb.Update()

	burstDelta := burst - tb.burst
	tb.rate = rate
	tb.burst = burst

	tb.current += burstDelta
	tb.updateExhaustedMicros()
}

// Reset resets the current tokens to whatever the burst is.
func (tb *TokenBucket) Reset() {
	tb.current = tb.burst
	tb.updateExhaustedMicros()
}

// Adjust returns tokens to the bucket (positive delta) or accounts for a debt
// of tokens (negative delta).
func (tb *TokenBucket) Adjust(delta Tokens) {
	tb.Update()
	tb.current += delta
	if tb.current > tb.burst {
		tb.current = tb.burst
	}
	tb.updateExhaustedMicros()
}

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the request should be retried.
func (tb *TokenBucket) TryToFulfill(amount Tokens) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.Update()

	// Deal with the case where the request is larger than the burst size. In
	// this case we'll allow the acquisition to complete if and when the current
	// value is equal to the burst. If the acquisition succeeds, it will put the
	// limiter into debt.
	want := amount
	if want > tb.burst {
		want = tb.burst
	}
	if delta := want - tb.current; delta > 0 {
		// Compute the time it will take to get to the needed capacity.
		timeDelta := time.Duration((float64(delta) * float64(time.Second)) / float64(tb.rate))
		if timeDelta < time.Nanosecond {
			timeDelta = time.Nanosecond
		}
		return false, timeDelta
	}

	tb.current -= amount
	tb.updateExhaustedMicros()
	return true, 0
}

// Exhausted returns the cumulative duration over which this token bucket was
// exhausted. Exported only for metrics.
func (tb *TokenBucket) Exhausted() time.Duration {
	var ongoingExhaustionMicros int64
	if !tb.exhaustedStart.IsZero() {
		ongoingExhaustionMicros = tb.timeSource.Now().Sub(tb.exhaustedStart).Microseconds()
	}
	return time.Duration(tb.exhaustedMicros+ongoingExhaustionMicros) * time.Microsecond
}

// Available returns the currently available tokens (can be -ve). Exported only
// for metrics.
func (tb *TokenBucket) Available() Tokens {
	return tb.current
}

func (tb *TokenBucket) updateExhaustedMicros() {
	now := tb.timeSource.Now()
	if tb.current <= 0 && tb.exhaustedStart.IsZero() {
		tb.exhaustedStart = now
	}
	if tb.current > 0 && !tb.exhaustedStart.IsZero() {
		tb.exhaustedMicros += now.Sub(tb.exhaustedStart).Microseconds()
		tb.exhaustedStart = time.Time{}
	}
}

// TestingInternalParameters returns the refill rate (configured), burst tokens
// (configured), and number of available tokens where available <= burst. It's
// used in tests.
func (tb *TokenBucket) TestingInternalParameters() (rate TokensPerSecond, burst, available Tokens) {
	tb.Update()
	return tb.rate, tb.burst, tb.current
}
