// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package tenanttokenbucket implements the tenant token bucket server-side
// algorithm described in the distributed token bucket RFC. It has minimal
// dependencies and is meant to be testable on its own.
package tenanttokenbucket

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// State of the distributed token bucket.
type State struct {
	// RUBurstLimit is the burst limit in RUs.
	RUBurstLimit float64
	// RURefillRate is the refill rate in RUs/second.
	RURefillRate float64

	// RUCurrent is the available (burst) RUs.
	RUCurrent float64

	// CurrentShareSum is the sum of the last reported share value for
	// each active SQL pod for the tenant.
	CurrentShareSum float64
}

// Request processes a request for more tokens and updates the State
// accordingly.
func (s *State) Request(
	req *roachpb.TokenBucketRequest, now time.Time,
) roachpb.TokenBucketResponse {
	var res roachpb.TokenBucketResponse
	// TODO(radu): fill in response.
	return res
}

// Reconfigure updates the settings for the token bucket.
//
// Arguments:
//
//  - availableRU is the amount of Request Units that the tenant can consume at
//    will. Also known as "burst RUs".
//
//  - refillRate is the amount of Request Units per second that the tenant
//    receives.
//
//  - maxBurstRU is the maximum amount of Request Units that can be accumulated
//    from the refill rate, or 0 if there is no limit.
//
//  - asOf is a timestamp; the reconfiguration request is assumed to be based on
//    the consumption at that time. This timestamp is used to compensate for any
//    refill that would have happened in the meantime.
//
//  - asOfConsumedRequestUnits is the total number of consumed RUs based on
//    which the reconfiguration values were calculated (i.e. at the asOf time).
//    It is used to adjust availableRU with the consumption that happened in the
//    meantime.
//
//  - now is the current time.
//
//  - currentConsumedRequestUnits is the current total number of consumed RUs.
//
func (s *State) Reconfigure(
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
	now time.Time,
	currentConsumedRequestUnits float64,
) {
	// TODO(radu): adjust available RUs according to asOf and asOfConsumedUnits
	// and add tests.
	s.RUCurrent = availableRU
	s.RURefillRate = refillRate
	s.RUBurstLimit = maxBurstRU
}
