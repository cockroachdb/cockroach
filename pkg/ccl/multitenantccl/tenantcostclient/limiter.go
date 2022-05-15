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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// limiter is used to rate-limit KV requests according to a local token bucket.
//
// The Wait() method is called when a KV batch request or batch response
// arrives. If the token bucket is in debt, then Wait will block until the debt
// is cleared. The tenant-side controller will always call Wait with needed
// tokens = 0 for batch requests, since it does not yet know the cost at that
// time. The cost becomes available once the batch response has been recieved,
// and is passed to Wait at that time.
//
// Besides RU usage from calls to the KV layer, the controller tracks CPU and
// egress network usage. These costs are updated every second and passed to the
// limiter's OnTick method. Adding these costs in a single chunk to the token
// bucket would be the simplest approach. However, doing this all at once can
// cause a jerky "stop-start" pattern, where all batch requests/responses get
// stopped at the beginning of each second until the debt is cleared, and then
// flow freely until the next second, where they are stopped again.
//
// To ensure more even flow, the limiter introduces the concept of "extra RU".
// On each tick, the extra RU accumulated (e.g. from CPU and Egress) is stored
// separately and then divided amongst the read/write operations that are
// expected to arrive over the next tick. The number of operations expected to
// arrive is based on the exponential moving average of the historical rate of
// operation arrival. For example, if there are 100 extra RUs and if an average
// of 20 operations per second have arrived in the past, then 5 RUs are added to
// each operation that arrives during the next tick.
//
// limiter's methods are thread-safe.
type limiter struct {
	// Access to these fields is protected by the mutex.
	mu struct {
		sync.Mutex
		extraUsage   tenantcostmodel.RU
		opsCount     float64
		opsPerTick   float64
		extraRUPerOp tenantcostmodel.RU
	}

	// Access to these fields is protected by the quota pool lock. Only access
	// them in the scope of the abstract pool's Update method or within the
	// wait request's Acquire method.
	qp struct {
		*quotapool.AbstractPool
		tb tokenBucket
	}
}

// movingAvgOpsPerSecFactor is the weight applied to new average ops/sec
// samples. The lower this is, the "smoother" the moving average will be.
const movingAvgOpsPerSecFactor = 0.2

func (l *limiter) Init(timeSource timeutil.TimeSource, notifyChan chan struct{}) {
	*l = limiter{}

	onWaitFinishFn := func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
		// Log a trace event for requests that waited for a long time.
		if waitDuration := timeSource.Since(start); waitDuration > time.Second {
			log.VEventf(ctx, 1, "request waited for RUs for %s", waitDuration.String())
		}
	}

	l.qp.AbstractPool = quotapool.New(
		"tenant-side-limiter", l,
		quotapool.WithTimeSource(timeSource),
		quotapool.OnWaitFinish(onWaitFinishFn),
	)

	l.qp.tb.Init(timeSource.Now(), notifyChan)
}

func (l *limiter) Close() {
	l.qp.Close("shutting down")
}

// AvailableRU returns the current number of available RUs. This can be negative
// if the token bucket is in debt.
func (l *limiter) AvailableRU(now time.Time) tenantcostmodel.RU {
	var result tenantcostmodel.RU
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = l.qp.tb.AvailableTokens(now)
		return false
	})
	return result
}

// Wait acquires the requested number of RUs from the bucket as long as the
// bucket is not in debt. If in debt, then Wait will block until the bucket is
// no longer in debt, either due to its refill rate or due to more tokens being
// added to the bucket. Wait is called before making a read/write batch request
// and before returning a read/write batch response.
//
// See the comment for the limiter struct for details on extraRU handling.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU) error {
	if needed > 0 {
		// Combine the needed RUs with some portion of any outstanding extraRU.
		needed += l.amortizeExtraRU()
	}

	r := newWaitRequest(quotapool.Tokens(needed))
	defer putWaitRequest(r)

	return l.qp.Acquire(ctx, r)
}

// RemoveRU removes tokens from the bucket immediately, potentially putting it
// into debt.
func (l *limiter) RemoveRU(now time.Time, amount tenantcostmodel.RU) {
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		l.qp.tb.RemoveTokens(now, amount)

		// Don't notify the head of the queue; this change can only delay the time
		// it can go through.
		return false
	})
}

// Reconfigure is used to call tokenBucket.Reconfigure under the pool's lock.
func (l *limiter) Reconfigure(now time.Time, cfg tokenBucketReconfigureArgs) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.qp.tb.Reconfigure(now, cfg)

		// Notify the head of the queue; the new configuration might allow a
		// request to go through earlier.
		return true
	})
}

// SetupNotification is used to call tokenBucket.SetupNotification under the
// pool's lock.
func (l *limiter) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.qp.tb.SetupNotification(now, threshold)
		return false
	})
}

// OnTick is called by the tenant-side controller on each ticker callback (~once
// per second). It passes the amount of extra RU usage that has accumulated
// during the last tick, such as CPU and Egress usage. Based on the average
// number of read/write operations per tick, OnTick will determine how to
// amortize the extra RU usage across the operations expected in the next tick.
func (l *limiter) OnTick(now time.Time, extraUsage tenantcostmodel.RU) {
	// Access locked fields within protected function. Don't attempt to update
	// the quota pool in this function to reduce risk of deadlocks caused by
	// different lock acquisition orders.
	removeRU := func() (removeRU tenantcostmodel.RU) {
		l.mu.Lock()
		defer l.mu.Unlock()

		if l.mu.opsCount == 0 {
			// During this last tick, there have been no operations to associate
			// with extra RU usage. Remove all previous usage directly from the
			// token bucket without attempting to distribute it across operations.
			removeRU = l.mu.extraUsage
			l.mu.extraUsage = 0
		}

		// Add extra usage during the last tick.
		l.mu.extraUsage += extraUsage

		// Distribute the extra usage across the estimated number of operations
		// occurring during the next tick.
		if l.mu.opsPerTick == 0 {
			l.mu.opsPerTick = l.mu.opsCount
		} else {
			// Update the exponential moving average of operations per tick.
			l.mu.opsPerTick = movingAvgOpsPerSecFactor*l.mu.opsCount +
				(1-movingAvgOpsPerSecFactor)*l.mu.opsPerTick
		}
		l.mu.opsCount = 0

		// Compute how any extra RUs will be divided amongst the expected
		// number of operations in the next tick.
		opsPerTick := l.mu.opsPerTick
		if opsPerTick < 1 {
			// Add all extra RUs to the next operation.
			opsPerTick = 1
		}
		l.mu.extraRUPerOp = l.mu.extraUsage / tenantcostmodel.RU(opsPerTick)

		return removeRU
	}()

	l.RemoveRU(now, removeRU)
}

// amortizeExtraRU returns the portion of extraRU that should be combined with
// the RUs needed by a read/write operation. The extraRU is amortized across the
// number of operations expected to arrive in the current tick.
func (l *limiter) amortizeExtraRU() tenantcostmodel.RU {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.mu.opsCount++

	extraRU := l.mu.extraRUPerOp
	if extraRU > l.mu.extraUsage {
		extraRU = l.mu.extraUsage
	}

	l.mu.extraUsage -= extraRU
	return extraRU
}

// extraUsage is an unexported function used by a unit test.
func (l *limiter) extraUsage() tenantcostmodel.RU {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.mu.extraUsage
}

func (l *limiter) String() string {
	l.mu.Lock()
	avg := l.mu.opsPerTick
	extraUsage := l.mu.extraUsage
	l.mu.Unlock()

	var s string
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		s = fmt.Sprintf("%s (avg %.2f ops/sec, %.2f extra RU)", l.qp.tb.String(), avg, extraUsage)
		return false
	})
	return s
}

// waitRequest is used to wait for adequate resources in the tokenBucket.
type waitRequest struct {
	needed quotapool.Tokens
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// It should be returned with putWaitRequest.
func newWaitRequest(needed quotapool.Tokens) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{needed: needed}
	return r
}

func putWaitRequest(r *waitRequest) {
	*r = waitRequest{}
	waitRequestSyncPool.Put(r)
}

// Acquire is part of quotapool.Request. It is called by the quota pool under
// the scope of its lock.
func (req *waitRequest) Acquire(
	ctx context.Context, res quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	l := res.(*limiter)
	now := l.qp.TimeSource().Now()
	return l.qp.tb.TryToFulfill(now, tenantcostmodel.RU(req.needed))
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
