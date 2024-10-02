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
// of tokens in the bucket (with one sample per Request call).
const movingAvgFactor = 0.25

// debtLimitMultiple is a multiple of the default debt limit (number of tokens
// refilled in target period). This is used to allow grants that temporarily
// exceed the default debt limit in the case where multiple tenant instances are
// competing and TokenCurrentAvg has not yet stabilized at a level that provides
// the right grant size to each of them.
const debtLimitMultiple = 1.5

// State of the distributed token bucket.
type State struct {
	// TokenBurstLimit is the burst limit in tokens.
	TokenBurstLimit float64

	// TokenRefillRate is the refill rate in tokens/second.
	TokenRefillRate float64

	// TokenCurrent is the available (burst) tokens.
	TokenCurrent float64

	// TokenCurrentAvg is the average number of (burst) tokens in the bucket when
	// requests arrive. This is used to smooth out the size of trickle grants
	// made to competing tenant instances.
	// NOTE: This field is serialized as the current_share_sum column in the
	// system.tenant_usage table. That column is unused and reusing it avoids
	// a system table schema change.
	TokenCurrentAvg float64
}

// fallbackRateTimeFrame is a time frame used to calculate a fallback rate.
//
// The fallback rate is used when the tenant can't get a TokenBucket request
// through. It is calculated so that all available (burst) tokens are used
// through this time period. We assume that this time frame is enough to react
// to and fix an infrastructure problem.
const fallbackRateTimeFrame = time.Hour

// Update accounts for passing of time, replenishing tokens according to the
// rate.
func (s *State) Update(since time.Duration) {
	if since > 0 {
		s.TokenCurrent += s.TokenRefillRate * since.Seconds()
	}
	s.clampToLimit()
}

// Request processes a request for more tokens and updates the State
// accordingly.
func (s *State) Request(
	ctx context.Context, req *kvpb.TokenBucketRequest,
) kvpb.TokenBucketResponse {
	var res kvpb.TokenBucketResponse

	// Compute the exponential moving average (EMA) of the number of tokens in
	// the bucket when Request is called.
	s.TokenCurrentAvg = movingAvgFactor*s.TokenCurrent + (1-movingAvgFactor)*s.TokenCurrentAvg

	// Calculate the fallback rate.
	res.FallbackRate = s.TokenRefillRate
	if s.TokenCurrent > 0 {
		res.FallbackRate += s.TokenCurrent / fallbackRateTimeFrame.Seconds()
	}
	if log.V(1) {
		log.Infof(ctx, "token bucket request (tenant=%d requested=%g current=%g, avg=%g)",
			req.TenantID, req.RequestedTokens, s.TokenCurrent, s.TokenCurrentAvg)
	}

	needed := req.RequestedTokens
	if needed > s.TokenCurrent && s.TokenRefillRate == 0 {
		// No way to refill tokens, so don't allow taking on debt.
		needed = s.TokenCurrent
	}
	if needed <= 0 {
		// Grant zero tokens.
		return res
	}

	if s.TokenCurrent >= needed {
		s.TokenCurrent -= needed
		res.GrantedTokens = needed
		if log.V(1) {
			log.Infof(ctx, "request granted (tenant=%d remaining=%g)", req.TenantID, s.TokenCurrent)
		}
		return res
	}

	var granted float64

	// There are not enough tokens in the bucket, so take on debt and return a
	// trickle grant that needs to be consumed over the target request period.
	if s.TokenCurrent > 0 {
		// Consume remaining available tokens.
		granted = s.TokenCurrent
		needed -= s.TokenCurrent
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
	// of tokens that are present in the bucket when Request is called
	// (TokenCurrentAvg). This is typically a negative number, as the bucket will
	// be in debt when trickle grants are made. The grant increases the average
	// debt to the maximum allowed level - the number of tokens that would refill
	// the bucket during the target request period.
	//
	// As an example, say that TokenCurrentAvg starts at 0 and the refill rate is
	// 1000 tokens/s. Tenant instance #1 requests 1000 tokens/s with a target
	// request period of 10 seconds. It is granted the entire 1000 tokens/s rate
	// with a 10-second trickle. Four seconds later, instance #2 also requests
	// 1000 tokens/s. The bucket has -6000 tokens available, and TokenCurrentAvg
	// is updated to the EMA of -6000 * 0.2 = -1200. Instance #2 is therefore
	// granted 880 tokens/s. This temporarily exceeds the token bucket refill
	// rate. However, over time the EMA will converge towards -5000 and each
	// instance will get 500 tokens/s.
	refill := req.TargetRequestPeriod.Seconds() * s.TokenRefillRate
	available := refill

	if debtAvg := -s.TokenCurrentAvg; debtAvg > 0 {
		// Clamp available tokens to average.
		available -= debtAvg
	}

	if debt := -s.TokenCurrent; debt > 0 {
		// Don't allow debt to exceed max limit.
		available = math.Min(available, refill*debtLimitMultiple-debt)
	}

	needed = math.Min(needed, math.Max(available, 0))

	// Compute the grant and adjust the current token balance.
	granted += needed
	s.TokenCurrent -= granted
	res.GrantedTokens = granted
	res.TrickleDuration = req.TargetRequestPeriod
	if log.V(1) {
		log.Infof(ctx, "request granted over time (tenant=%d granted=%g trickle=%s)",
			req.TenantID, res.GrantedTokens, res.TrickleDuration)
	}
	return res
}

// Reconfigure updates the settings for the token bucket.
//
// Arguments:
//
//   - availableTokens is the number of tokens that the tenant can consume at
//     will. Also known as "burst tokens". If this is -1 (or any negative
//     number), the bucket's available tokens are not updated.
//
//   - refillRate is the amount of tokens per second that the tenant receives.
//     If this is 0, the bucket does not refill on its own.
//
//   - maxBurstTokens is the maximum number of tokens that can be accumulated
//     from the refill rate, or 0 if there is no limit.
func (s *State) Reconfigure(
	ctx context.Context,
	tenantID roachpb.TenantID,
	availableTokens float64,
	refillRate float64,
	maxBurstTokens float64,
) {
	// TODO(radu): adjust available tokens according to asOf and asOfConsumedUnits
	// and add tests.
	if availableTokens >= 0 {
		s.TokenCurrent = availableTokens
	}
	s.TokenRefillRate = refillRate
	s.TokenBurstLimit = maxBurstTokens
	s.clampToLimit()
	log.Infof(
		ctx, "token bucket for tenant %s reconfigured: available=%g refill-rate=%g burst-limit=%g",
		tenantID.String(), s.TokenCurrent, s.TokenRefillRate, s.TokenBurstLimit,
	)
}

// clampToLimit limits current tokens in the bucket to the burst limit.
func (s *State) clampToLimit() {
	if s.TokenBurstLimit > 0 && s.TokenCurrent > s.TokenBurstLimit {
		s.TokenCurrent = s.TokenBurstLimit
	}
}
