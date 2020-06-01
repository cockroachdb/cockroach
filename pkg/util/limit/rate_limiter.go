// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package limit

import (
	"context"

	"golang.org/x/time/rate"
)

// burstFactor is used to set the burst on the underlying Limiter. Unfortunately
// burst is a required parameter, so we set it as a function of the limit to
// ease the cognitive burden on the user of this function.
const burstFactor int = 5

// LimiterBurstDisabled is used to solve a complication in rate.Limiter.
// The rate.Limiter requires a burst parameter and if the throttled value
// exceeds the burst it just fails. This not always the desired behavior,
// sometimes we want the limiter to apply the throttle and not enforce any
// hard limits on an arbitrarily large value. This feature is particularly
//useful in Cockroach, when we want to throttle on the KV pair, the size
// of which is not strictly enforced.
type LimiterBurstDisabled struct {
	// Avoid embedding, as most methods on the limiter take the parameter
	// burst into consideration.
	limiter *rate.Limiter
}

// NewLimiter returns a new LimiterBurstDisabled that allows events up to rate r.
func NewLimiter(r rate.Limit) *LimiterBurstDisabled {
	return &LimiterBurstDisabled{
		limiter: rate.NewLimiter(r, int(r)*burstFactor),
	}
}

// WaitN blocks until lim permits n events to happen.
//
// This function will now only return an error if the Context is cancelled and
// should never in practice hit the burst check in the underlying limiter.
func (lim *LimiterBurstDisabled) WaitN(ctx context.Context, n int) error {
	for n > 0 {
		cur := n
		if cur > lim.limiter.Burst() {
			cur = lim.limiter.Burst()
		}
		if err := lim.limiter.WaitN(ctx, cur); err != nil {
			return err
		}
		n -= cur
	}
	return nil
}
