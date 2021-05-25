// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import (
	"context"
	"sync"
	"time"
)

// Limit defines a rate in terms of quota per second.
type Limit float64

// RateLimiter implements a token-bucket style rate limiter.
// It has the added feature that quota acquired from the pool can be returned
// in the case that they end up not getting used.
type RateLimiter struct {
	qp *AbstractPool
}

// NewRateLimiter defines a new RateLimiter. The limiter is implemented as a
// token bucket which has a maximum capacity of burst. If a request attempts to
// acquire more than burst, it will block until the bucket is full and then
// put the token bucket in debt.
func NewRateLimiter(name string, rate Limit, burst int64, options ...Option) *RateLimiter {
	rl := &RateLimiter{}
	tb := &TokenBucket{}
	rl.qp = New(name, tb, options...)
	tb.Init(TokensPerSecond(rate), Tokens(burst), rl.qp.timeSource)
	return rl
}

// Acquire acquires n quota from the RateLimiter. This acquired quota may be
// released back into the token bucket or it may be consumed.
func (rl *RateLimiter) Acquire(ctx context.Context, n int64) (*RateAlloc, error) {
	if err := rl.WaitN(ctx, n); err != nil {
		return nil, err
	}
	return (*RateAlloc)(rl.newRateAlloc(n)), nil
}

// WaitN acquires n quota from the RateLimiter. This acquisition cannot be
// released.
func (rl *RateLimiter) WaitN(ctx context.Context, n int64) error {
	if n == 0 {
		// Special case 0 acquisition.
		return nil
	}
	r := rl.newRateRequest(n)
	defer rl.putRateRequest(r)
	if err := rl.qp.Acquire(ctx, r); err != nil {
		return err
	}
	return nil
}

// AdmitN acquire n quota from the RateLimiter if it succeeds. It will return
// false and not block if there is currently insufficient quota or the pool is
// closed.
func (rl *RateLimiter) AdmitN(n int64) bool {
	r := rl.newRateRequest(n)
	defer rl.putRateRequest(r)
	return rl.qp.Acquire(context.Background(), (*rateRequestNoWait)(r)) == nil
}

// UpdateLimit updates the rate and burst limits. The change in burst will
// be applied to the current quantity of quota. For example, if the RateLimiter
// currently had a quota of 5 available with a burst of 10 and the burst is
// update to 20, the quota will increase to 15. Similarly, if the burst is
// decreased by 10, the current quota will decrease accordingly, potentially
// putting the limiter into debt.
func (rl *RateLimiter) UpdateLimit(rate Limit, burst int64) {
	rl.qp.Update(func(res Resource) (shouldNotify bool) {
		tb := res.(*TokenBucket)
		tb.UpdateConfig(TokensPerSecond(rate), Tokens(burst))
		return true
	})
}

// RateAlloc is an allocated quantity of quota which can be released back into
// the token-bucket RateLimiter.
type RateAlloc struct {
	alloc int64
	rl    *RateLimiter
}

// Return returns the RateAlloc to the RateLimiter. It is not safe to call any
// methods on the RateAlloc after this call.
func (ra *RateAlloc) Return() {
	ra.rl.qp.Update(func(res Resource) (shouldNotify bool) {
		tb := res.(*TokenBucket)
		tb.Adjust(Tokens(ra.alloc))
		return true
	})
	ra.rl.putRateAlloc((*rateAlloc)(ra))
}

// Consume destroys the RateAlloc. It is not safe to call any methods on the
// RateAlloc after this call.
func (ra *RateAlloc) Consume() {
	ra.rl.putRateAlloc((*rateAlloc)(ra))
}

// rateAlloc is the internal implementation of Resource used by the RateLimiter.
type rateAlloc RateAlloc

type rateRequest struct {
	want int64
}

var rateRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(rateRequest) },
}

// newRateRequest allocates a rateRequest from the sync.Pool.
// It should be returned with putRateRequest.
func (rl *RateLimiter) newRateRequest(v int64) *rateRequest {
	r := rateRequestSyncPool.Get().(*rateRequest)
	*r = rateRequest{want: v}
	return r
}

func (rl *RateLimiter) putRateRequest(r *rateRequest) {
	*r = rateRequest{}
	rateRequestSyncPool.Put(r)
}

func (i *rateRequest) Acquire(
	ctx context.Context, res Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	tb := res.(*TokenBucket)
	return tb.TryToFulfill(Tokens(i.want))
}

func (i *rateRequest) ShouldWait() bool {
	return true
}

var rateAllocSyncPool = sync.Pool{
	New: func() interface{} { return new(rateAlloc) },
}

func (rl *RateLimiter) newRateAlloc(v int64) *rateAlloc {
	a := rateAllocSyncPool.Get().(*rateAlloc)
	*a = rateAlloc{alloc: v, rl: rl}
	return a
}

func (rl *RateLimiter) putRateAlloc(a *rateAlloc) {
	*a = rateAlloc{}
	rateAllocSyncPool.Put(a)
}

// rateRequestNoWait is like a rate request but will not block waiting for
// quota.
type rateRequestNoWait rateRequest

func (r *rateRequestNoWait) Acquire(
	ctx context.Context, resource Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	return (*rateRequest)(r).Acquire(ctx, resource)
}

func (r *rateRequestNoWait) ShouldWait() bool {
	return false
}

var _ Request = (*rateRequestNoWait)(nil)
