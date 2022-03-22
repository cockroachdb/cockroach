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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// limiter is used to rate-limit KV requests according to a local token bucket.
//
// The Wait() method is called when a KV request requires RUs. The other methods
// are used to adjust/reconfigure/replenish the local token bucket.
type limiter struct {
	timeSource timeutil.TimeSource
	testInstr  TestInstrumentation
	tb         tokenBucket
	qp         *quotapool.AbstractPool

	// Total (rounded) RU needed for all currently waiting requests (or requests
	// that are in the process of being fulfilled).
	// Only accessed using atomics.
	waitingRU int64
}

// Initial settings for the local token bucket. They are used only until the
// first TokenBucket request returns. We allow immediate use of the initial RUs
// (we essentially borrow them and pay them back in the first TokenBucket
// request). The intention is to avoid any throttling during start-up in normal
// circumstances.
const initialRUs = 10000
const initialRate = 100

func (l *limiter) Init(
	timeSource timeutil.TimeSource, testInstr TestInstrumentation, notifyChan chan struct{},
) {
	*l = limiter{
		timeSource: timeSource,
		testInstr:  testInstr,
	}

	l.tb.Init(timeSource.Now(), notifyChan, initialRate, initialRUs)

	onWaitStartFn := func(ctx context.Context, poolName string, r quotapool.Request) {
		req := r.(*waitRequest)
		// Account for the RUs, unless we already did in waitRequest.Acquire.
		// This is necessary because Acquire is only called for the head of the
		// queue.
		if !req.waitingRUAccounted {
			req.waitingRUAccounted = true
			atomic.AddInt64(&l.waitingRU, req.neededCeil())
			if l.testInstr != nil {
				l.testInstr.Event(l.timeSource.Now(), WaitingRUAccountedInCallback)
			}
		}
	}

	onWaitFinishFn := func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
		// Log a trace event for requests that waited for a long time.
		if waitDuration := timeSource.Since(start); waitDuration > time.Second {
			log.VEventf(ctx, 1, "request waited for RUs for %s", waitDuration.String())
		}
	}

	l.qp = quotapool.New(
		"tenant-side-limiter", l,
		quotapool.WithTimeSource(timeSource),
		quotapool.OnWaitStart(onWaitStartFn),
		quotapool.OnWaitFinish(onWaitFinishFn),
	)
}

func (l *limiter) Close() {
	l.qp.Close("shutting down")
}

// Wait removes the needed RUs from the bucket, waiting as necessary until it is
// possible.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU) error {
	r := newWaitRequest(needed)
	defer putWaitRequest(r)

	return l.qp.Acquire(ctx, r)
}

// RemoveTokens removes tokens from the bucket.
//
// Tokens are removed when consumption has occurred without Wait(), like
// accounting for CPU usage or for the number of read bytes.
func (l *limiter) RemoveTokens(now time.Time, delta tenantcostmodel.RU) {
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		l.tb.RemoveTokens(now, delta)
		// Don't notify the head of the queue; this change can only delay the time
		// it can go through.
		return false
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

// SetupNotification is used to call tokenBucket.SetupNotification under the
// pool's lock.
func (l *limiter) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.tb.SetupNotification(now, threshold)
		// We return true so that if there is a request waiting, TryToFulfill gets
		// called again which may produce a notification.
		return true
	})
}

// waitRequest is used to wait for adequate resources in the tokenBucket.
type waitRequest struct {
	needed tenantcostmodel.RU

	// waitingRUAccounted is true if we counted the needed RUs as "waiting RU".
	// This flag is necessary because we want to account f
	waitingRUAccounted bool
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
	fulfilled, tryAgainAfter = l.tb.TryToFulfill(now, req.needed)

	if !fulfilled {
		// We want to account for the waiting RU here (under the quotapool lock)
		// rather than in the OnWaitStart callback. This is to ensure that the
		// waiting RUs for the head of the queue are reliably reflected in the next
		// call to AvailableTokens(). If it is not reflected, a low RU notification
		// might effectively be ignored because it looks like we have enough RUs.
		//
		// The waitingRUAccounted flag ensures we don't count the same request
		// multiple times.
		if !req.waitingRUAccounted {
			req.waitingRUAccounted = true
			atomic.AddInt64(&l.waitingRU, req.neededCeil())
		}
	} else {
		if req.waitingRUAccounted {
			req.waitingRUAccounted = false
			atomic.AddInt64(&l.waitingRU, -req.neededCeil())
		}
	}
	return fulfilled, tryAgainAfter
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
