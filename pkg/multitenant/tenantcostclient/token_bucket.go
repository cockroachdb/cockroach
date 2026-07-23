// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import (
	"fmt"
	"time"
)

// tokenBucket implements a token bucket. It is a more specialized form of
// quotapool.TokenBucket. The main differences are:
//   - it does not currently support a burst limit;
//   - it has special debt handling.
//
// -- Debt handling --
//
// The token bucket is designed to handle arbitrary removal of tokens to account
// for usage that cannot be throttled (e.g. read/transferred bytes, CPU usage).
// This can bring the token bucket into debt.
//
// The simplest handling of debt is to immediately subtract it from available
// tokens, and then blocking all operations that require tokens in the meantime.
// However, this is undesirable because the accounting can happen at arbitrary
// intervals which can lead to the workload periodically experiencing starvation
// (e.g. CPU usage might be accounted for only once per second, which can lead
// to all requests being blocked for the beginning part of each second).
//
// Instead, we apply any un-throttled debt D over a time period T, starting from
// the time the last debt was incurred. We do this by splitting the refill rate
// into D/T and using only what's left for replenishing tokens.
//
// This rate is recalculated every time we incur debt. So in essence, every time
// we incur debt, we put it together with any other waiting debt and then plan
// to subtract it from available tokens over time T (we can think of this as
// "refinancing" the existing waiting debt).
//
// This behavior is somewhat arbitrary because the rate at which we apply
// waiting debt depends on how frequently we incur additional debt. To see how
// much it can vary, imagine that at time t=0 we incur some debt D(0) and
// consider the two extremes:
//
//	A. We start with debt D(0), and we never recalculate the rate (no
//	   "refinancing"). We apply debt at constant rate D(0) / T and all debt is
//	   paid at time T.
//
//	B. We start with debt D(0), and we recalculate the rate ("refinance")
//	   continuously (or, more intuitively, every nanosecond).  The
//	   instantaneous rate is:
//	     D'(t) = - D(t) / T
//	   The debt formula is:
//	     D(t) = D(0) * e^(-t/T)
//	   We apply 63% of the debt in time T; 86% in 2T; and 95% in 3T.
//
// The difference between these two extremes is reasonable - we apply between
// 63% and 100% of the debt in time T, depending on the usage pattern.
//
// Design note: ideally we would always simulate B. However, under this model it
// is hard to compute the time until a request can go through (i.e. time until
// we accumulate a certain amount of tokens): it involves calculating the
// intersection of a line with an exponential, which cannot be solved
// algebraically (it requires slower numerical methods).
//
// tokenBucket's methods are *not* thread-safe, and rely on higher-level
// synchronization.
type tokenBucket struct {
	// -- Dynamic fields --
	// Protected by the AbstractPool's lock. All changes should happen either
	// inside a Request.Acquire() method or under AbstractPool.Update().

	// limit is the maximum number of tokens that can be present in the bucket.
	// Tokens beyond this limit are discarded. If limit = 0, then no limit is
	// enforced.
	limit float64
	// rate that tokens fill the bucket, in tokens/s.
	rate float64
	// available is the number of currently available tokens in the bucket. This
	// can be negative if waiting debt has been subtracted from it.
	available float64

	// waitingDebt is debt that will be subtracted from the available tkoens over
	// time, in order to smooth its impact.
	waitingDebt float64

	// waitingDebtRate is the rate at which "waitingDebt" is subtracted from
	// available tokens; cannot exceed the fill rate.
	waitingDebtRate float64

	lastUpdated time.Time
}

// debtApplySecs is the target number of seconds that it should take to subtract
// waiting debt from the available tokens.
const debtApplySecs = 2

func (tb *tokenBucket) Init(now time.Time) {
	*tb = tokenBucket{lastUpdated: now}
}

// update accounts for the passing of time.
func (tb *tokenBucket) update(now time.Time) {
	since := now.Sub(tb.lastUpdated)
	if since <= 0 {
		return
	}
	tb.lastUpdated = now
	sinceSeconds := since.Seconds()
	tb.available += tb.rate * sinceSeconds

	// Subtract some portion of waiting debt, if there is any.
	if tb.waitingDebt != 0 {
		debt := tb.waitingDebtRate * sinceSeconds
		if debt > tb.waitingDebt {
			debt = tb.waitingDebt
		}
		tb.waitingDebt -= debt
		tb.available -= debt
	}

	tb.clampToLimit()
}

func (tb *tokenBucket) clampToLimit() {
	if tb.limit != 0 && tb.available > tb.limit {
		tb.available = tb.limit
	}
}

func (tb *tokenBucket) calculateDebtRate() {
	tb.waitingDebtRate = tb.waitingDebt / debtApplySecs
	if tb.waitingDebtRate > tb.rate {
		// There is waiting debt that cannot be applied within debtApplySecs;
		// immediately subtract it from available tokens (which might cause that
		// to become negative).
		debt := (tb.waitingDebtRate - tb.rate) * debtApplySecs
		tb.waitingDebt -= debt
		tb.available -= debt
		tb.waitingDebtRate = tb.rate
	}
}

// RemoveTokens decreases the amount of tokens currently available. If there are
// not enough tokens, this causes the token bucket to go into debt. Debt will be
// subtracted from the available tokens over the next few seconds in order to
// smooth its impact.
func (tb *tokenBucket) RemoveTokens(now time.Time, amount float64) {
	tb.update(now)
	if tb.available >= amount {
		tb.available -= amount
	} else {
		if tb.available > 0 {
			amount -= tb.available
			tb.available = 0
		}
		tb.waitingDebt += amount
	}
	tb.calculateDebtRate()
}

// tokenBucketReconfigureArgs is used to update the token bucket's
// configuration.
type tokenBucketReconfigureArgs struct {
	// NewTokens is the number of tokens that should be added to the token bucket.
	NewTokens float64

	// NewRate is the new token fill rate for the bucket.
	NewRate float64

	// MaxTokens is the maximum number of tokens that can be present in the
	// bucket. Tokens beyond this limit are discarded. If MaxTokens = 0, then
	// no limit is enforced.
	MaxTokens float64
}

// Reconfigure changes the rate, optionally adjusts the available tokens and
// configures the next notification.
func (tb *tokenBucket) Reconfigure(now time.Time, args tokenBucketReconfigureArgs) {
	tb.update(now)
	tb.rate = args.NewRate
	tb.limit = args.MaxTokens
	tb.addTokens(args.NewTokens)
}

// addTokens increases the number of tokens currently available.
func (tb *tokenBucket) addTokens(amount float64) {
	if tb.waitingDebt > 0 {
		if tb.waitingDebt >= amount {
			tb.waitingDebt -= amount
		} else {
			tb.available += amount - tb.waitingDebt
			tb.waitingDebt = 0
		}
	} else {
		tb.available += amount
	}
	tb.calculateDebtRate()
	tb.clampToLimit()
}

// maxTryAgainAfterSeconds is the maximum value that can be returned from
// TryToFulfill.
const maxTryAgainAfterSeconds = 1000

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the request should be retried.
func (tb *tokenBucket) TryToFulfill(
	now time.Time, amount float64,
) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.update(now)

	// Fast path.
	if amount <= tb.available {
		tb.available -= amount
		return true, 0
	}

	// Handle edge case where amount > limit, since no amount of waiting will ever
	// allow the request to be fulfilled. Go into debt to fulfill the request.
	// NOTE: This case is never expected to happen, but we handle it anyway just
	// to be sure.
	if tb.limit != 0 && amount > tb.limit && tb.available >= tb.limit {
		tb.RemoveTokens(now, amount)
		return true, 0
	}

	needed := amount - tb.available

	// Compute the time it will take to refill to the needed amount.
	var timeSeconds float64

	if tb.waitingDebt == 0 {
		timeSeconds = needed / tb.rate
	} else {
		remainingRate := tb.rate - tb.waitingDebtRate
		// There are two cases:
		//
		//  1. We accumulate enough tokens from the remainingRate before applying
		//     the entire waiting debt.
		//
		//  2. We apply all waiting debt before accumulating enough tokens from
		//     the remainingRate.
		//
		// The time to accumulate the needed tokens while applying waiting debt is:
		//   needed / remainingRate
		// The time to apply the waiting debt is:
		//   waitingDebt / waitingDebtRate
		//
		// We are in case 1 if
		//   needed / remainingRate <= debt / waitingDebtRate
		// or equivalently:
		//   needed * waitingDebtRate <= debt * remainingRate
		if needed*tb.waitingDebtRate <= tb.waitingDebt*remainingRate {
			// Case 1.
			timeSeconds = needed / remainingRate
		} else {
			// Case 2.
			debtSeconds := tb.waitingDebt / tb.waitingDebtRate
			timeSeconds = debtSeconds + (needed-debtSeconds*remainingRate)/tb.rate
		}
	}

	// Cap the number of seconds to avoid overflow; we want to tolerate even a
	// rate of 0 (in which case we are really waiting for a token adjustment).
	if timeSeconds > maxTryAgainAfterSeconds {
		return false, maxTryAgainAfterSeconds * time.Second
	}

	timeDelta := time.Duration(timeSeconds * float64(time.Second))
	if timeDelta < time.Nanosecond {
		timeDelta = time.Nanosecond
	}
	return false, timeDelta
}

// AvailableTokens returns the current number of tokens that can be immediately
// used. This can be negative if we accumulated debt.
func (tb *tokenBucket) AvailableTokens(now time.Time) float64 {
	tb.update(now)
	return tb.available - tb.waitingDebt
}

func (tb *tokenBucket) String(now time.Time) string {
	tb.update(now)
	s := fmt.Sprintf("%.2f tokens filling @ %.2f tokens/s", tb.available, tb.rate)
	if tb.limit > 0 {
		s += fmt.Sprintf(" (limited to %.2f tokens)", tb.limit)
	}
	if tb.waitingDebt > 0 {
		s += fmt.Sprintf(" (%.2f waiting debt @ %.2f tokens/s)", tb.waitingDebt, tb.waitingDebtRate)
	}
	return s
}
