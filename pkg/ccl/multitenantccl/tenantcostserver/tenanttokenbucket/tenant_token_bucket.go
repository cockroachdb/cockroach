// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tenanttokenbucket implements the tenant token bucket server-side
// algorithm described in the distributed token bucket RFC. It has minimal
// dependencies and is meant to be testable on its own.
package tenanttokenbucket

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// movingAvgFactor is the weight applied to a new "sample" of the current number
// of RUs in the bucket (with one sample per Request call).
const movingAvgFactor = 0.25

// debtLimitMultiple is a multiple of the default debt limit (number of tokens
// refilled in target period). This is used to allow grants that temporarily
// exceed the default debt limit in the case where multiple tenant instances are
// competing and RUCurrentAvg has not yet stabilized at a level that provides
// the right grant size to each of them.
const debtLimitMultiple = 1.5

// State of the distributed token bucket.
type State struct {
	// RUBurstLimit is the burst limit in RUs.
	RUBurstLimit float64

	// RURefillRate is the refill rate in RUs/second.
	RURefillRate float64

	// RUCurrent is the available (burst) RUs.
	RUCurrent float64

	// RUCurrentAvg is the average number of (burst) RUs in the bucket when
	// requests arrive. This is used to smooth out the size of trickle grants
	// made to competing tenant instances.
	// NOTE: This field is serialized as the current_share_sum column in the
	// system.tenant_usage table. That column is unused and reusing it avoids
	// a system table schema change.
	RUCurrentAvg float64
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
	s.clampToLimit()
}

// Request processes a request for more tokens and updates the State
// accordingly.
func (s *State) Request(
	ctx context.Context, req *kvpb.TokenBucketRequest,
) kvpb.TokenBucketResponse {
	var res kvpb.TokenBucketResponse

	// Compute the exponential moving average (EMA) of the number of RUs in the
	// bucket when Request is called.
	s.RUCurrentAvg = movingAvgFactor*s.RUCurrent + (1-movingAvgFactor)*s.RUCurrentAvg

	// Calculate the fallback rate.
	res.FallbackRate = s.RURefillRate
	if s.RUCurrent > 0 {
		res.FallbackRate += s.RUCurrent / fallbackRateTimeFrame.Seconds()
	}
	if log.V(1) {
		log.Infof(ctx, "token bucket request (tenant=%d requested=%g current=%g, avg=%g)",
			req.TenantID, req.RequestedRU, s.RUCurrent, s.RUCurrentAvg)
	}

	needed := req.RequestedRU
	if needed > s.RUCurrent && s.RURefillRate == 0 {
		// No way to refill tokens, so don't allow taking on debt.
		needed = s.RUCurrent
	}
	if needed <= 0 {
		// Grant zero tokens.
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

	var granted float64

	// There are not enough tokens in the bucket, so take on debt and return a
	// trickle grant that needs to be consumed over the target request period.
	if s.RUCurrent > 0 {
		// Consume remaining available tokens.
		granted = s.RUCurrent
		needed -= s.RUCurrent
	}

	// The trickle grant algorithm tries to ensure that the rate of token grants
	// does not exceed the token bucket refill rate. This is challenging when
	// there are multiple tenant instances requesting tokens at constantly
	// shifting intervals and rates. The solution is to ensure a *statistical*
	// guarantee rather than a *hard* guarantee. Over time, the *average* rate of
	// token consumption should not exceed the token bucket refill rate, even if
	// at any given time, it may temporarily exceed it.
	//
	// The way this is accomplished is quite simple. We track the average number
	// of request units that are present in the bucket when Request is called
	// (RUCurrentAvg). This is typically a negative number, as the bucket will be
	// in debt when trickle grants are made. The grant increases the average debt
	// to the maximum allowed level - the number of tokens that would refill the
	// bucket during the target request period.
	//
	// As an example, say that RUCurrentAvg starts at 0 and the refill rate is
	// 1000 RU/s. Tenant instance #1 requests 1000 RU/s with a target request
	// period of 10 seconds. It is granted the entire 1000 RU/s rate with a 10-
	// second trickle. Four seconds later, instance #2 also requests 1000 RU/s.
	// The bucket has -6000 RUs available, and RUCurrentAvg is updated to the
	// EMA of -6000 * 0.2 = -1200. Instance #2 is therefore granted 880 RU/s.
	// This temporarily exceeds the token bucket refill rate. However, over time
	// the EMA will converge towards -5000 and each instance will get 500 RU/s.
	refill := req.TargetRequestPeriod.Seconds() * s.RURefillRate
	available := refill

	if debtAvg := -s.RUCurrentAvg; debtAvg > 0 {
		// Clamp available RUs to average.
		available -= debtAvg
	}

	if debt := -s.RUCurrent; debt > 0 {
		// Don't allow debt to exceed max limit.
		available = math.Min(available, refill*debtLimitMultiple-debt)
	}

	needed = math.Min(needed, math.Max(available, 0))

	// Compute the grant and adjust the current RU balance.
	granted += needed
	s.RUCurrent -= granted
	res.GrantedRU = granted
	res.TrickleDuration = req.TargetRequestPeriod
	if log.V(1) {
		log.Infof(ctx, "request granted over time (tenant=%d granted=%g trickle=%s)",
			req.TenantID, res.GrantedRU, res.TrickleDuration)
	}
	return res
}

// Reconfigure updates the settings for the token bucket.
//
// Arguments:
//
//   - availableRU is the amount of Request Units that the tenant can consume at
//     will. Also known as "burst RUs". If this is -1 (or any negative number),
//     the bucket's available tokens are not updated.
//
//   - refillRate is the amount of Request Units per second that the tenant
//     receives. If this is 0, the bucket does not refill on its own.
//
//   - maxBurstRU is the maximum amount of Request Units that can be accumulated
//     from the refill rate, or 0 if there is no limit.
//
//   - asOf is a timestamp; the reconfiguration request is assumed to be based on
//     the consumption at that time. This timestamp is used to compensate for any
//     refill that would have happened in the meantime.
//
//   - asOfConsumedRequestUnits is the total number of consumed RUs based on
//     which the reconfiguration values were calculated (i.e. at the asOf time).
//     It is used to adjust availableRU with the consumption that happened in the
//     meantime.
//
//   - now is the current time.
//
//   - currentConsumedRequestUnits is the current total number of consumed RUs.
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
	if availableRU >= 0 {
		s.RUCurrent = availableRU
	}
	s.RURefillRate = refillRate
	s.RUBurstLimit = maxBurstRU
	s.clampToLimit()
	log.Infof(
		ctx, "token bucket for tenant %s reconfigured: available=%g refill-rate=%g burst-limit=%g",
		tenantID.String(), s.RUCurrent, s.RURefillRate, s.RUBurstLimit,
	)
}

// clampToLimit limits current RUs in the bucket to the burst limit.
func (s *State) clampToLimit() {
	if s.RUBurstLimit > 0 && s.RUCurrent > s.RUBurstLimit {
		s.RUCurrent = s.RUBurstLimit
	}
}
