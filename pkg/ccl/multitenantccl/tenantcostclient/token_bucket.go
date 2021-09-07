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

// tokenBucket implements a token bucket. It is a more specialized
// form of quotapool.TokenBucket.
type tokenBucket struct {
	// -- Static fields --

	// When the available amount goes below the notifyThreshold and the time is
	// after notifyStartTime, we do a (non-blocking) send on the channel.
	notifyCh chan<- struct{}

	// -- Dynamic fields --
	// Protected by the AbstractPool's lock. All changes should happen either
	// inside a Request.Acquire() method or under AbstractPool.Update().

	// When the available RUs go below this value, we do a non-blocking send on
	// notifyCh (after which the threshold is reset to zero). A threshold of zero
	// disables notifications.
	notifyThreshold tenantcostmodel.RU
	notifyStartTime time.Time

	// Refill rate, in RU/s.
	rate tenantcostmodel.RU
	// Currently available RUs. Can be negative (indicating debt).
	available tenantcostmodel.RU

	lastUpdated time.Time
}

func (tb *tokenBucket) Init(
	now time.Time, notifyCh chan<- struct{}, rate, available tenantcostmodel.RU,
) {
	*tb = tokenBucket{
		notifyCh:        notifyCh,
		notifyThreshold: 0,
		notifyStartTime: time.Time{},
		rate:            rate,
		available:       available,
		lastUpdated:     now,
	}
}

// Update accounts for the passing of time. It is automatically called by other
// methods, but can also be called directly to potentially trigger a
// notification.
func (tb *tokenBucket) Update(now time.Time) {
	if since := now.Sub(tb.lastUpdated); since > 0 {
		tb.available += tb.rate * tenantcostmodel.RU(since.Seconds())
		tb.lastUpdated = now
		// The number of tokens didn't go down, but it's possible we just passed the
		// notifyStartTime.
		tb.maybeNotify(now)
	}
}

// notify tries to send a non-blocking notification on notifyCh and disables
// further notifications (until the next Reconfigure).
func (tb *tokenBucket) notify() {
	tb.notifyThreshold = 0
	tb.notifyStartTime = time.Time{}
	select {
	case tb.notifyCh <- struct{}{}:
	default:
	}
}

// maybeNotify checks if it's time to send the notification and if so, performs
// the notification.
func (tb *tokenBucket) maybeNotify(now time.Time) {
	if tb.notifyThreshold > 0 && tb.available < tb.notifyThreshold && !now.Before(tb.notifyStartTime) {
		tb.notify()
	}
}

// AdjustTokens changes the amount of tokens currently available, either
// increasing or decreasing them. The amount can become negative (indicating
// debt).
func (tb *tokenBucket) AdjustTokens(now time.Time, delta tenantcostmodel.RU) {
	tb.Update(now)
	tb.available += delta
	tb.maybeNotify(now)
}

type tokenBucketReconfigureArgs struct {
	TokenAdjustment tenantcostmodel.RU

	NewRate tenantcostmodel.RU

	NotifyThreshold tenantcostmodel.RU
	NotifyStartTime time.Time
}

// Reconfigure changes the rate, optionally adjusts the available tokens and
// configures the next notification.
func (tb *tokenBucket) Reconfigure(now time.Time, args tokenBucketReconfigureArgs) {
	tb.Update(now)
	tb.available += args.TokenAdjustment
	tb.rate = args.NewRate
	tb.notifyThreshold = args.NotifyThreshold
	tb.notifyStartTime = args.NotifyStartTime
	tb.maybeNotify(now)
}

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the request should be retried.
func (tb *tokenBucket) TryToFulfill(
	now time.Time, amount tenantcostmodel.RU,
) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.Update(now)

	if amount <= tb.available {
		tb.available -= amount
		tb.maybeNotify(now)
		return true, 0
	}

	// We have run out of available tokens; notify if we haven't already. This is
	// necessary if the amount is larger than the notify threshold.
	if tb.notifyThreshold > 0 && !now.Before(tb.notifyStartTime) {
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
	tb.Update(now)
	return tb.available
}
