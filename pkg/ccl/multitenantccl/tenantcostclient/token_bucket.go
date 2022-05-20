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
//  - its methods take explicit "now" parameters;
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
// the cost at that point).
//
// tokenBucket's methods are *not* thread-safe, and rely on higher-level
// synchronization.
//
// TODO(andyk): Replace this with quotapool.TokenBucket. Doing that requires
// fixing an overflow bug in TryToFulfill when rate=0. We probably also want to
// change the other TokenBucket's methods to take "now" parameters. These kinds
// of changes are unnecessarily risky to backport, so keep the two token buckets
// separate for now.
type tokenBucket struct {
	// -- Dynamic fields --
	// Protected by the AbstractPool's lock. All changes should happen either
	// inside a Request.Acquire() method or under AbstractPool.Update().

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
}

func (tb *tokenBucket) Init(now time.Time) {
	*tb = tokenBucket{lastUpdated: now}
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
}

// Reconfigure changes the rate and optionally adjusts the available tokens.
func (tb *tokenBucket) Reconfigure(now time.Time, cfg tokenBucketReconfigureArgs) {
	tb.update(now)
	tb.rate = cfg.NewRate
	tb.current += cfg.NewTokens
}

// TryToFulfill either removes the given amount if is available, or returns a
// time after which the operation should be retried.
func (tb *tokenBucket) TryToFulfill(
	now time.Time, amount tenantcostmodel.RU,
) (fulfilled bool, tryAgainAfter time.Duration) {
	tb.update(now)

	shortfall := amount - tb.current
	if shortfall <= 0 {
		tb.current -= amount
		return true, 0
	}

	// Compute the number of seconds it will take to make up the shortfall.
	delaySeconds := float64(shortfall / tb.rate)

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

func (tb *tokenBucket) String(now time.Time) string {
	tb.update(now)
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
