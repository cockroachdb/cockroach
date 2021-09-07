// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// limiter is used to rate-limit KV requests according to a local token bucket.
//
// The Wait() method is called when a KV request requires RUs. The other methods
// are used to adjust/reconfigure/replenish the local token bucket.
type limiter struct {
	timeSource timeutil.TimeSource
	tb         tokenBucket
	qp         *quotapool.AbstractPool

	// Total (rounded) RU needed for all currently waiting requests (or requests
	// that are in the process of being fulfilled).
	waitingRU int64
}

// Initial settings for the local token bucket. They are used only until the
// first TokenBucket request returns. We allow immediate use of the initial RUs
// (we essentially borrow them and pay them back in the first TokenBucket
// request). The intention is to avoid any throttling during start-up in normal
// circumstances.
const initialRUs = 10000
const initialRate = 100

func (l *limiter) Init(timeSource timeutil.TimeSource, notifyChan chan<- struct{}) {
	*l = limiter{
		timeSource: timeSource,
	}

	l.tb.Init(timeSource.Now(), notifyChan, initialRate, initialRUs)

	onWaitStartFn := func(ctx context.Context, poolName string, r quotapool.Request) {
		atomic.AddInt64(&l.waitingRU, r.(*waitRequest).neededCeil())
	}
	onWaitFinishFn := func(
		ctx context.Context, poolName string, r quotapool.Request, start time.Time,
	) {
		atomic.AddInt64(&l.waitingRU, -r.(*waitRequest).neededCeil())
	}
	l.qp = quotapool.New(
		"tenant-side-limiter", l,
		quotapool.WithTimeSource(timeSource),
		quotapool.OnWaitStart(onWaitStartFn),
		quotapool.OnWaitFinish(onWaitFinishFn),
	)
}

// Wait removes the needed RUs from the bucket, waiting as necessary until it is
// possible.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU) error {
	r := newWaitRequest(needed)
	defer putWaitRequest(r)

	return l.qp.Acquire(ctx, r)
}

// AdjustTokens adds or removes tokens from the bucket. Tokens are added when we
// receive more tokens from the host cluster. Tokens are removed when
// consumption has occurred without Wait(): accounting for CPU usage and the
// number of read bytes.
func (l *limiter) AdjustTokens(now time.Time, delta tenantcostmodel.RU) {
	if delta == 0 {
		return
	}
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		l.tb.AdjustTokens(now, delta)
		// We notify the head of the queue if we added RUs, in which case that
		// request might be allowed to go through earlier.
		return delta > 0
	})
}

// Reconfigure is used to call tokenBucket.Reconfigure under the pool's lock.
func (l *limiter) Reconfigure(now time.Time, args tokenBucketReconfigureArgs) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.tb.Reconfigure(now, args)
		// Notify the head of the queue; the new configuration might allow that
		// request to go through earlier.
		return true
	})
}

// AvailableTokens returns the current number of available RUs. This can be
// negative if we accumulated debt or we have waiting requests.
func (l *limiter) AvailableTokens(now time.Time) tenantcostmodel.RU {
	var result tenantcostmodel.RU
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = l.tb.AvailableTokens(now)
		return false
	})
	// Subtract the RUs for currently waiting requests.
	result -= tenantcostmodel.RU(atomic.LoadInt64(&l.waitingRU))
	return result
}

// Update accounts for the passing of time; used to potentially trigger
// a notification. See tokenBucket.Update.
func (l *limiter) Update(now time.Time) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.tb.Update(now)
		// Nothing changed.
		return false
	})
}

// waitRequest is used to wait for adequate resources in the tokenBucket.
type waitRequest struct {
	needed tenantcostmodel.RU
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(needed tenantcostmodel.RU) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{needed: needed}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

// neededCeil returns the amount of needed RUs, rounded up to an integer.
func (req *waitRequest) neededCeil() int64 {
	return int64(math.Ceil(float64(req.needed)))
}

// Acquire is part of quotapool.Request.
func (req *waitRequest) Acquire(
	ctx context.Context, res quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	l := res.(*limiter)
	now := l.timeSource.Now()
	return l.tb.TryToFulfill(now, req.needed)
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
