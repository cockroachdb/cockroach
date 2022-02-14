// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

// tokenBucket implements a token bucket. It is a more specialized form of
// quotapool.TokenBucket. The main differences are:
//  - it does not currently support a burst limit;
//  - it implements a "low tokens" notification mechanism;
//  - it has special debt handling.
//
// -- Notification mechanism --
//
// Notifications can be configured to fire when the number of available RUs dips
// below a certain value (or when a request blocks). Notifications are delivered
// via a non-blocking send to a given channel.
//
// -- Debt handling --
//
// The token bucket is designed to handle arbitrary removal of tokens to account
// for usage that cannot be throttled (e.g. read/transferred bytes, CPU usage).
// This can bring the token bucket into debt.
//
// The simplest handling of debt is to always pay the debt first, blocking all
// operations that require tokens in the mean time. However, this is undesirable
// because the accounting can happen at arbitrary intervals which can lead to
// the workload periodically experiencing starvation (e.g. CPU usage might be
// accounted for only once per second, which can lead to all requests being
// blocked for the beginning part of each second).
//
// Instead, we aim to pay all outstanding debt D within time T from the time the
// last debt was incurred. We do this by splitting the refill rate into D/T and
// using only what's left for replenishing tokens.
//
// This rate is recalculated every time we incur debt. So in essence, every time
// we incur debt, we put it together with all existing debt and plan to pay it
// within time T (we can think of this as "refinancing" the existing debt).
//
// This behavior is somewhat arbitrary because the rate at which we pay debt
// depends on how frequently we incur additional debt. To see how much it can
// vary, imagine that at time t=0 we incur some debt D(0) and consider the two
// extremes:
//
//   A. We start with debt D(0), and we never recalculate the rate (no
//      "refinancing"). We pay debt at constant rate D(0) / T and all debt is
//      paid at time T.
//
//   B. We start with debt D(0), and we recalculate the rate ("refinance")
//      continuously (or, more intuitively, every nanosecond).  The
//      instantaneous rate is:
//        D'(t) = - D(t) / T
//      The debt formula is:
//        D(t) = D(0) * e^(-t/T)
//      We pay 63% of the debt in time T; 86% in 2T; and 95% in 3T.
//
// The difference between these two extremes is reasonable - we pay between 63%
// and 100% of the debt in time T, depending on the usage pattern.
//
// Design note: ideally we would always simulate B. However, under this model it
// is hard to compute the time until a request can go through (i.e. time until
// we accumulate a certain amount of tokens): it involves calculating the
// intersection of a line with an exponential, which cannot be solved
// algebraically (it requires slower numerical methods).
type tokenBucket struct {
	// -- Static fields --

	// Once the available RUs are below the notifyThreshold or a request cannot
	// be immediately fulfilled, we do a (non-blocking) send on this channel.
	notifyCh chan struct{}

	// -- Dynamic fields --
	// Protected by the AbstractPool's lock. All changes should happen either
	// inside a Request.Acquire() method or under AbstractPool.Update().

	// See notifyCh. A threshold of zero disables notifications.
	notifyThreshold tenantcostmodel.RU

	// Refill rate, in RU/s.
	rate tenantcostmodel.RU
	// Currently available RUs. Can not be negative.
	available tenantcostmodel.RU

	// Debt incurred that we will try to pay over time. See debtHalfLife.
	debt tenantcostmodel.RU

	// Rate at which we pay off debt; cannot exceed the refill rate.
	debtRate tenantcostmodel.RU

	lastUpdated time.Time
}

// We try to repay debt over the next 2 seconds.
const debtRepaymentSecs = 2

func (tb *tokenBucket) Init(
	now time.Time, notifyCh chan struct{}, rate, available tenantcostmodel.RU,
) {
	*tb = tokenBucket{
		notifyCh:        notifyCh,
		notifyThreshold: 0,
		rate:            rate,
		available:       available,
		lastUpdated:     now,
	}
}

// update accounts for the passing of time.
func (tb *tokenBucket) update(now time.Time) {
	since := now.Sub(tb.lastUpdated)
	if since <= 0 {
		return
	}
	tb.lastUpdated = now
	sinceSeconds := since.Seconds()
	refilled := tb.rate * tenantcostmodel.RU(sinceSeconds)

	if tb.debt == 0 {
		// Fast path: no debt.
		tb.available += refilled
		return
	}

	debtPaid := tb.debtRate * tenantcostmodel.RU(sinceSeconds)
	if tb.debt >= debtPaid {
		tb.debt -= debtPaid
	} else {
		debtPaid = tb.debt
		tb.debt = 0
	}
	tb.available += refilled - debtPaid
}

// notify tries to send a non-blocking notification on notifyCh and disables
// further notifications (until the next Reconfigure or StartNotification).
func (tb *tokenBucket) notify() {
	tb.notifyThreshold = 0
	select {
	case tb.notifyCh <- struct{}{}:
	default:
	}
}

// maybeNotify checks if it's time to send the notification and if so, performs
// the notification.
func (tb *tokenBucket) maybeNotify(now time.Time) {
	if tb.notifyThreshold > 0 && tb.available < tb.notifyThreshold {
		tb.notify()
	}
}

func (tb *tokenBucket) calculateDebtRate() {
	tb.debtRate = tb.debt / debtRepaymentSecs
	if tb.debtRate > tb.rate {
		tb.debtRate = tb.rate
	}
}

// RemoveTokens decreases the amount of tokens currently available.
//
// If there are not enough tokens, this causes the token bucket to go into debt.
// Debt will be attempted to be repaid over the next few seconds.
func (tb *tokenBucket) RemoveTokens(now time.Time, amount tenantcostmodel.RU) {
	tb.update(now)
	if tb.available >= amount {
		tb.available -= amount
	} else {
		tb.debt += amount - tb.available
		tb.available = 0
		tb.calculateDebtRate()
	}
	tb.maybeNotify(now)
}

// AddTokens increases the amount of tokens currently available.
func (tb *tokenBucket) AddTokens(now time.Time, amount tenantcostmodel.RU) {
	tb.update(now)
	if amount >= 0 {
		tb.addTokens(amount)
	}
	tb.maybeNotify(now)
}

func (tb *tokenBucket) addTokens(amount tenantcostmodel.RU) {
	if tb.debt > 0 {
		if tb.debt >= amount {
			tb.debt -= amount
		} else {
			tb.available += amount - tb.debt
			tb.debt = 0
		}
		tb.calculateDebtRate()
	} else {
		tb.available += amount
	}
}

type tokenBucketReconfigureArgs struct {
	NewTokens tenantcostmodel.RU

	NewRate tenantcostmodel.RU

	NotifyThreshold tenantcostmodel.RU
}

// Reconfigure changes the rate, optionally adjusts the available tokens and
// configures the next notification.
func (tb *tokenBucket) Reconfigure(now time.Time, args tokenBucketReconfigureArgs) {
	tb.update(now)
	// If we already produced a notification that wasn't processed, drain it. This
	// not racy as long as this code does not run in parallel with the code that
	// receives from notifyCh.
	select {
	case <-tb.notifyCh:
	default:
	}
	tb.rate = args.NewRate
	tb.notifyThreshold = args.NotifyThreshold
	if args.NewTokens > 0 {
		tb.addTokens(args.NewTokens)
	}
	tb.maybeNotify(now)
}

// SetupNotification enables the notification at the given threshold.
func (tb *tokenBucket) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	tb.update(now)
	tb.notifyThreshold = threshold
}

const maxTryAgainAfterSeconds = 1000

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the request should be retried.
func (tb *tokenBucket) TryToFulfill(
	now time.Time, amount tenantcostmodel.RU,
) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.update(now)

	if amount <= tb.available {
		tb.available -= amount
		tb.maybeNotify(now)
		return true, 0
	}

	// We have run out of available tokens; notify if we haven't already. This is
	// possible if we have more than the notifyThreshold available.
	if tb.notifyThreshold > 0 {
		tb.notify()
	}

	needed := amount - tb.available

	// Compute the time it will take to refill to the needed amount.
	var timeSeconds float64

	if tb.debt == 0 {
		timeSeconds = float64(needed / tb.rate)
	} else {
		remainingRate := tb.rate - tb.debtRate
		// There are two cases:
		//
		//  1. We accumulate enough tokens from the remainingRate before paying off
		//     the entire debt.
		//
		//  2. We pay off all debt before accumulating enough tokens from the
		//     remainingRate.
		//
		// The time to accumulate the needed tokens while paying debt is:
		//   needed / remainingRate
		// The time to pay off the debt is:
		//   debt / debtRate
		//
		// We are in case 1 if
		//   needed / remainingRate <= debt / debtRate
		// or equivalently:
		//   needed * debtRate <= debt * remainingRate
		if needed*tb.debtRate <= tb.debt*remainingRate {
			// Case 1.
			timeSeconds = float64(needed / remainingRate)
		} else {
			// Case 2.
			debtPaySeconds := tb.debt / tb.debtRate
			timeSeconds = float64(debtPaySeconds + (needed-debtPaySeconds*remainingRate)/tb.rate)
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

// AvailableTokens returns the current number of available RUs. This can be
// negative if we accumulated debt.
func (tb *tokenBucket) AvailableTokens(now time.Time) tenantcostmodel.RU {
	tb.update(now)
	return tb.available - tb.debt
}
