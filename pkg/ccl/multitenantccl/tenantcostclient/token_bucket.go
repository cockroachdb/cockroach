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
//  - it does not currently support a burst limit
//  - it implements a "low tokens" notification mechanism.
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
	// Currently available RUs. Can be negative (indicating debt).
	available tenantcostmodel.RU

	lastUpdated time.Time
}

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
	if since := now.Sub(tb.lastUpdated); since > 0 {
		tb.available += tb.rate * tenantcostmodel.RU(since.Seconds())
		tb.lastUpdated = now
	}
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

// AdjustTokens changes the amount of tokens currently available, either
// increasing or decreasing them. The amount can become negative (indicating
// debt).
func (tb *tokenBucket) AdjustTokens(now time.Time, delta tenantcostmodel.RU) {
	tb.update(now)
	tb.available += delta
	tb.maybeNotify(now)
}

type tokenBucketReconfigureArgs struct {
	TokenAdjustment tenantcostmodel.RU

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
	tb.available += args.TokenAdjustment
	tb.rate = args.NewRate
	tb.notifyThreshold = args.NotifyThreshold
	tb.maybeNotify(now)
}

// SetupNotification enables the notification at the given threshold.
func (tb *tokenBucket) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	tb.update(now)
	tb.notifyThreshold = threshold
}

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

	// Compute the time it will take to get to the needed capacity.
	timeSeconds := float64((amount - tb.available) / tb.rate)
	// Cap the number of seconds to avoid overflow; we want to tolerate even a
	// rate of 0 (in which case we are really waiting for a token adjustment).
	if timeSeconds > 1000 {
		timeSeconds = 1000
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
	return tb.available
}
