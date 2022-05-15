// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

// maxTryAgainAfterSeconds is the maximum value that can be returned from
// TryToFulfill.
const maxTryAgainAfterSeconds = 1000

// tokenBucket implements a token bucket. It is a more specialized form of
// quotapool.TokenBucket. The main differences are:
//  - it does not currently support a burst limit;
//  - it implements a "low tokens" notification mechanism;
//  - its methods take explicit "now" parameters;
//  - it fulfills every request, as long as the token bucket is not in debt.
//
// -- Notification mechanism --
//
// Notifications can be configured to fire when the number of available RUs dips
// below a certain value. Notifications are delivered via a non-blocking send to
// a given channel.
//
// -- Fulfillment --
//
// The token bucket is designed to work with the tenant-side controller, which
// is called both when a batch request is received and when a batch response is
// received. In the general case, the number of tokens needed by an operation is
// not known until the response is received. However, the controller would like
// to block further requests if the token bucket is empty. Therefore, the
// controller always acquires 0 tokens from the bucket for every batch request,
// and only acquires >0 tokens once it receives a batch response (since it knows
// the cost at that point). To ensure that even 0 token requests will block, the
// token bucket will continue to fulfill until it has gone into debt, at which
// point it will begin to refuse even 0 token acquisition attempts.
//
// tokenBucket's methods are *not* thread-safe, and rely on higher-level
// synchronization.
type tokenBucket struct {
	// -- Immutable fields set at initialization --

	// notifyCh gets a (non-blocking) send when available RUs falls below the
	// notifyThreshold.
	notifyCh chan struct{}

	// -- Dynamic fields --
	// Protected by the AbstractPool's lock. All changes should happen either
	// inside a Request.Acquire() method or under AbstractPool.Update().

	// notifyThreshold is the RU level below which a notification should be sent
	// to notifyCh. A threshold <= 0 disables notifications.
	notifyThreshold tenantcostmodel.RU

	// rate that tokens fill the bucket, in RU/s.
	rate tenantcostmodel.RU
	// current is the number of currently available RUs in the bucket.
	current tenantcostmodel.RU
	// lastUpdated is the last time that the token bucket's rate was used to
	// calculate the fill level.
	lastUpdated time.Time
}

// tokenBucketReconfigureArgs is used to update the token bucket's
// configuration.
type tokenBucketReconfigureArgs struct {
	// NewTokens is the number of tokens that should be added to the token bucket.
	NewTokens tenantcostmodel.RU

	// NewRate is the new token fill rate for the bucket.
	NewRate tenantcostmodel.RU

	// NotifyThreshold is the token bucket level below which a low RU notification
	// will be sent.
	NotifyThreshold tenantcostmodel.RU
}

func (tb *tokenBucket) Init(now time.Time, notifyCh chan struct{}) {
	*tb = tokenBucket{
		notifyCh:    notifyCh,
		lastUpdated: now,
	}
}

// AvailableTokens returns the current number of available RUs in the bucket.
// This can be negative if we accumulated debt.
func (tb *tokenBucket) AvailableTokens(now time.Time) tenantcostmodel.RU {
	tb.update(now)
	return tb.current
}

// RemoveTokens decreases the amount of tokens currently available. If there are
// not enough tokens, this causes the token bucket to go into debt.
func (tb *tokenBucket) RemoveTokens(now time.Time, amount tenantcostmodel.RU) {
	tb.update(now)
	tb.current -= amount
	tb.maybeNotify(now)
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
	tb.current += args.NewTokens
	tb.notifyThreshold = args.NotifyThreshold
	tb.maybeNotify(now)
}

// SetupNotification enables the notification at the given threshold. It is used
// when only the threshold needs to be set, with no adjustment to available
// tokens or rate needed (use Reconfigure for that).
func (tb *tokenBucket) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	tb.update(now)
	tb.notifyThreshold = threshold
	tb.maybeNotify(now)
}

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the operation should be retried.
func (tb *tokenBucket) TryToFulfill(
	now time.Time, amount tenantcostmodel.RU,
) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.update(now)

	// If token bucket is in debt, then calculate tryAgainAfter.
	if tb.current < 0 {
		// Calculate the number of seconds needed to get out of debt.
		delaySeconds := float64(-tb.current / tb.rate)

		// Cap the number of seconds to avoid overflow; we want to tolerate
		// even a fill rate of 0 (in which case we are really waiting for a token
		// adjustment).
		if delaySeconds > maxTryAgainAfterSeconds {
			delaySeconds = maxTryAgainAfterSeconds
		}

		tryAgainAfter = time.Duration(delaySeconds * float64(time.Second))
		if tryAgainAfter < time.Nanosecond {
			tryAgainAfter = time.Nanosecond
		}
		return false, tryAgainAfter
	}

	tb.current -= amount
	tb.maybeNotify(now)
	return true, 0
}

func (tb *tokenBucket) String() string {
	return fmt.Sprintf("%.2f RU filling @ %.2f RU/s", tb.current, tb.rate)
}

// update accounts for the passing of time.
func (tb *tokenBucket) update(now time.Time) {
	since := now.Sub(tb.lastUpdated)
	if since <= 0 {
		return
	}
	tb.lastUpdated = now
	sinceSeconds := since.Seconds()
	tb.current += tb.rate * tenantcostmodel.RU(sinceSeconds)
}

// notify tries to send a non-blocking notification on notifyCh and disables
// further notifications (until the next Reconfigure or SetupNotification).
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
	if tb.notifyThreshold > 0 && tb.current < tb.notifyThreshold {
		tb.notify()
	}
}
