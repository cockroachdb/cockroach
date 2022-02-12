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
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// State of the distributed token bucket.
type State struct {
	// RUBurstLimit is the burst limit in RUs.
	// TODO(radu): this is ignored for now.
	RUBurstLimit float64

	// RURefillRate is the refill rate in RUs/second.
	RURefillRate float64

	// RUCurrent is the available (burst) RUs.
	RUCurrent float64

	// CurrentShareSum is the sum of the last reported share value for
	// each active SQL pod for the tenant.
	CurrentShareSum float64
}

// fallbackRateTimeFrame is a time frame used to calculate a fallback rate.
//
// The fallback rate is used when the tenant can't get a TokenBucket request
// through. It is calculated so that all available (burst) RUs are used through
// this time period. We assume that this time frame is enough to react to and
// fix an infrastructure problem.
const fallbackRateTimeFrame = time.Hour

// Update accounts for passing of time, replenishing tokens according to the
// rate.
func (s *State) Update(since time.Duration) {
	if since > 0 {
		s.RUCurrent += s.RURefillRate * since.Seconds()
	}
}

// Request processes a request for more tokens and updates the State
// accordingly.
func (s *State) Request(
	ctx context.Context, req *roachpb.TokenBucketRequest,
) roachpb.TokenBucketResponse {
	var res roachpb.TokenBucketResponse

	// Calculate the fallback rate.
	res.FallbackRate = s.RURefillRate
	if s.RUCurrent > 0 {
		res.FallbackRate += s.RUCurrent / fallbackRateTimeFrame.Seconds()
	}
	if log.V(1) {
		log.Infof(ctx, "token bucket request (tenant=%d requested=%g current=%g)", req.TenantID, req.RequestedRU, s.RUCurrent)
	}

	needed := req.RequestedRU
	if needed <= 0 {
		return res
	}

	if s.RUCurrent >= needed {
		s.RUCurrent -= needed
		res.GrantedRU = needed
		if log.V(1) {
			log.Infof(ctx, "request granted (tenant=%d remaining=%g)", req.TenantID, s.RUCurrent)
		}
		return res
	}

	var grantedTokens float64

	if s.RUCurrent > 0 {
		grantedTokens = s.RUCurrent
		needed -= s.RUCurrent
	}

	availableRate := s.RURefillRate
	if debt := -s.RUCurrent; debt > 0 {
		// We pre-distribute tokens over the next TargetRefillPeriod; any debt over
		// that is a systematic error we need to account for.
		debt -= req.TargetRequestPeriod.Seconds() * s.RURefillRate
		if debt > 0 {
			// Say that we want to pay the debt over the next RefillPeriod (but use at
			// most 95% of the rate for the debt).
			// TODO(radu): make this configurable?
			debtRate := debt / req.TargetRequestPeriod.Seconds()
			availableRate -= debtRate
			availableRate = math.Max(availableRate, 0.05*s.RURefillRate)
		}
	}
	// TODO(radu): support multiple instances by giving out only a share of the rate.
	// Without this, all instances will get roughly equal rates even if they have
	// different levels of load (in addition, we are heavily relying on the debt
	// mechanism above).
	allowedRate := availableRate
	duration := time.Duration(float64(time.Second) * (needed / allowedRate))
	if duration <= req.TargetRequestPeriod {
		grantedTokens += needed
	} else {
		// We don't want to plan ahead for more than the target period; give out
		// fewer tokens.
		duration = req.TargetRequestPeriod
		grantedTokens += allowedRate * duration.Seconds()
	}
	s.RUCurrent -= grantedTokens
	res.GrantedRU = grantedTokens
	res.TrickleDuration = duration
	if log.V(1) {
		log.Infof(ctx, "request granted over time (tenant=%d granted=%g trickle=%s)", req.TenantID, res.GrantedRU, res.TrickleDuration)
	}
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
	ctx context.Context,
	tenantID roachpb.TenantID,
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
	log.Infof(
		ctx, "token bucket for tenant %s reconfigured: available=%g refill-rate=%g burst-limit=%g",
		tenantID.String(), s.RUCurrent, s.RURefillRate, s.RUBurstLimit,
	)
}
