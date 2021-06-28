// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This package implements the tenant token bucket server-side algorithm
// described in the distributed token bucket RFC. It has minimal
// dependencies and is meant to be testable on its own.
package tenanttokenbucket

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// State of the distributed token bucket.
type State struct {
	// Burst limit in RUs.
	RUBurstLimit float64
	// Refill rate in RUs/second.
	RURefillRate float64

	// Available (burst) RUs.
	RUCurrent float64

	// Sum of the last reported share value for each tenant.
	CurrentShareSum float64
}

// Request processes a request for more tokens and updates the State
// accordingly.
func (s *State) Request(
	req *roachpb.TokenBucketRequest, now time.Time,
) roachpb.TokenBucketResponse {
	var res roachpb.TokenBucketResponse
	res.OperationTimestamp = now
	// TODO(radu): for now we always grant tokens.
	res.GrantedTokens = req.RequestedTokens
	res.TrickleTime = 0
	res.MaxBurstTokens = 0
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
