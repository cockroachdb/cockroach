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
	"math/big"

	"golang.org/x/time/rate"
)

// maxInt is the maximum int allowed by the architecture running this code i.e.
// it could be int32 or int64, unfortunately golang does not have a built in
// field for this.
const maxInt = int(^uint(0) >> 1)

// LimiterBurstDisabled is used to solve a complication in rate.Limiter.
// The rate.Limiter requires a burst parameter and if the throttled value
// exceeds the burst it just fails. This not always the desired behavior,
// sometimes we want the limiter to apply the throttle and not enforce any
// hard limits on an arbitrarily large value. This feature is particularly
// useful in Cockroach, when we want to throttle on the KV pair, the size
// of which is not strictly enforced.
type LimiterBurstDisabled struct {
	// Avoid embedding, as most methods on the limiter take the parameter
	// burst into consideration.
	limiter *rate.Limiter
}

// NewLimiter returns a new LimiterBurstDisabled that allows events up to rate r.
func NewLimiter(r rate.Limit) *LimiterBurstDisabled {
	// Unfortunately we can't disable the burst parameter on the
	// limiter, so we have to provide some value to it. To remove the cognitive
	// burden from the user, we set this value to be equal to the limit.
	// Semantically the choice of burst parameter does not matter, since
	// we will loop the limiter until all the tokens have been consumed. However
	// we want to minimize the number of loops for performance, which is why
	// setting the burst parameter to the limit is a good trade off.
	var burst, _ = big.NewFloat(float64(r)).Int64()
	if burst > int64(maxInt) {
		burst = int64(maxInt)
	}
	return &LimiterBurstDisabled{
		limiter: rate.NewLimiter(r, int(burst)),
	}
}

// WaitN blocks until lim permits n events to happen.
//
// This function will now only return an error if the Context is canceled and
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
