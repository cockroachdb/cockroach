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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Limit defines a rate in terms of quota per second.
type Limit float64

// RateLimiter implements a token-bucket style rate limiter.
// It has the added feature that quota acquired from the pool can be returned
// in the case that they end up not getting used.
type RateLimiter struct {
	qp *QuotaPool

	// TODO(ajwerner): synchronization around changing limits.
	burst     int64
	rateLimit Limit
}

// NewRateLimiter defines a new RateLimiter.
func NewRateLimiter(name string, limit Limit, burst int64, options ...Option) *RateLimiter {
	rl := &RateLimiter{rateLimit: limit, burst: burst}
	bucket := rateBucket{
		p:           rl,
		cur:         float64(burst),
		lastUpdated: timeutil.Now(),
	}
	rl.qp = New(name, &bucket, options...)
	bucket.lastUpdated = rl.qp.timeSource.Now()
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
	return r.err
}

// rateBucket is the implementation of Resource which remains in the quotapool
// for a RateLimiter.
type rateBucket struct {
	p           *RateLimiter
	cur         float64
	lastUpdated time.Time
}

var _ Resource = (*rateBucket)(nil)

func (i *rateBucket) Merge(other Resource) bool {
	o := other.(*rateAlloc)
	i.cur += float64(o.alloc)
	o.rl.putRateAlloc(o)

	if i.cur > float64(i.p.burst) {
		i.cur = float64(i.p.burst)
	}
	return true
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
	ra.rl.qp.Add((*rateAlloc)(ra))
}

// Consume destroys the RateAlloc. It is not safe to call any methods on the
// RateAlloc after this call.
func (ra *RateAlloc) Consume() {
	ra.rl.putRateAlloc((*rateAlloc)(ra))
}

// rateAlloc is the internal implementation of Resource used by the RateLimiter.
type rateAlloc RateAlloc

var _ Resource = (*rateAlloc)(nil)

func (i *rateAlloc) Merge(other Resource) bool {
	panic("merge not implemented on rateAlloc")
}

type rateRequest struct {
	want int64
	err  error
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
) (fulfilled bool, unused Resource, tryAgainAfter time.Duration) {
	r := res.(*rateBucket)
	now := r.p.qp.timeSource.Now()
	// Deal with the case where the allocation is larger than the burst size.
	if i.want > r.p.burst {
		i.err = ErrNotEnoughQuota
		return true, res, 0
	}

	// TODO(ajwerner): Consider instituting a minimum update frequency to avoid
	// spinning too fast on timers for tons of tiny allocations at a fast rate.
	if since := now.Sub(r.lastUpdated); since > 0 {
		r.cur += float64(r.p.rateLimit) * since.Seconds()
		if r.cur > float64(r.p.burst) {
			r.cur = float64(r.p.burst)
		}
		r.lastUpdated = now
	}
	if i.want > int64(r.cur) {
		// Compute the time it will take for r.cur to get to the needed capacity.
		delta := float64(i.want) - r.cur
		seconds := delta / float64(r.p.rateLimit)
		timeDelta := time.Duration(seconds * float64(time.Second))
		return false, nil, timeDelta
	}
	r.cur -= float64(i.want)
	return true, r, 0
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
