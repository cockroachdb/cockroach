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
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// movingAvgKVRUFactor is the weight applied to a new sample of KV RU usage
// during a tick, as part of calculating the exponential moving average.
const movingAvgKVRUPerTickFactor = 0.2

// limiter is used to rate-limit KV and external I/O operations according to a
// local token bucket.
//
// Operations call the Wait() method in order to block until there are
// sufficient tokens in the bucket. Since operation cost is not known until it
// has completed (e.g. because cost should be zero if operation errors), Wait is
// only called with a non-zero cost after completion. However, the KV operations
// also call Wait before starting, with a zero cost. This prevents new
// operations from proceeding if the token bucket has fallen into debt.
//
// Besides RU usage from KV and external I/O calls, the controller tracks CPU
// and egress network usage (i.e. pgwire egress) in the SQL layer. These costs
// are updated every second and passed to the limiter's OnTick method. Adding
// these costs in a single chunk to the token bucket would be the simplest
// approach. However, doing this all at once can cause a jerky "stop-start"
// pattern, where all operations get stopped at the beginning of each second
// until the SQL RU debt is cleared, and will then flow freely until the next
// second, where they are stopped again.
//
// To ensure more even flow, the limiter introduces the concept of amortized SQL
// RUs. On each tick, the accumulated SQL RU (e.g. from CPU and Egress) is
// stored separately and then divided amongst any KV read/write operations that
// arrive over the next tick. SQL RU is added to each KV operation based on its
// size (in RUs) as compared to the average number of RUs consumed in total per
// tick. For example, if 100 SQL RUs are stored, and if on average 1100 total
// RUs are consumed per tick, then 50 SQL RUs would be added to a 500 KV RU
// request, and 75 SQL RUs would be added to a 750 KV RU request.
//
// Note that SQL RUs are not amortized across external I/O operations. These
// operations are reported in big chunks (16MB by default), and so if anything,
// they should be amortized.
// TODO(andyk): Consider whether to smooth external I/O flow better. However,
// that would likely mean acquiring the quota pool lock more frequently.
//
// limiter's methods are thread-safe.
type limiter struct {
	// notifyCh gets a (non-blocking) send when available RUs falls below the
	// notifyThreshold.
	notifyCh chan struct{}

	// Access to these fields is protected by the quota pool lock. Only access
	// them in the scope of the abstract pool's Update method or within the
	// wait request's Acquire method.
	qp struct {
		*quotapool.AbstractPool

		// tb is the underlying local token bucket controlled by the quota pool.
		tb tokenBucket

		// notifyThreshold is the RU level below which a notification should be
		// sent to notifyCh. A threshold <= 0 disables notifications.
		notifyThreshold tenantcostmodel.RU

		// waitingRU is the total (rounded) RU needed for all currently waiting
		// requests (or requests that are in the process of being fulfilled).
		waitingRU tenantcostmodel.RU

		// usedSQLRU is true if any amount of SQL RUs were amortized across one
		// or more KV read/write operations during the current tick. This is used
		// to detect the case where no KV operations arrive during a tick.
		usedSQLRU bool

		// sqlRU is the number of RUs used in the SQL pod that are waiting to be
		// amortized across future KV operations.
		sqlRU tenantcostmodel.RU

		// avgKVRUPerTick is an exponentially-weighted average of the KV RU
		// consumed per tick. This is used to calculate sqlRUPerKVRU.
		avgKVRUPerTick tenantcostmodel.RU

		// sqlRUPerKVRU is the number of SQL RUs that will be added for each KV
		// operation that is requested during the current tick. For example, if
		// sqlRUPerKVRU is 0.20, and a 500 RU KV request arrives, then 100 SQL
		// RUs will be added to the request, for a total of 600 RUs.
		sqlRUPerKVRU tenantcostmodel.RU
	}
}

func (l *limiter) Init(timeSource timeutil.TimeSource, notifyCh chan struct{}) {
	*l = limiter{notifyCh: notifyCh}

	onWaitFinishFn := func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
		// Remove any waiting RUs that were not fulfilled by Acquire due to
		// context cancellation.
		l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
			l.maybeRemoveWaitingRULocked(r.(*waitRequest))
			return false
		})

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

	l.qp.tb.Init(timeSource.Now())
}

func (l *limiter) Close() {
	l.qp.Close("shutting down")
}

// AvailableRU returns the current number of available RUs. This can be negative
// if the token bucket is in debt, or we have pending SQL RU or waiting
// requests.
func (l *limiter) AvailableRU(now time.Time) tenantcostmodel.RU {
	var result tenantcostmodel.RU
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = l.availableRULocked(now)
		return false
	})
	return result
}

// Wait blocks until the token bucket has the needed number of tokens available.
// If amortize is true, then a portion of any waiting SQL RU will be added to
// the the request.
// Wait is called once a KV or External I/O operation has completed and is ready
// to be accounted for. It's also called before a KV operation starts, in order
// to block if the token bucket is in debt.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU, amortize bool) error {
	r := newWaitRequest(needed, amortize)
	defer putWaitRequest(r)
	return l.qp.Acquire(ctx, r)
}

// RemoveRU removes tokens from the bucket immediately, potentially putting it
// into debt.
func (l *limiter) RemoveRU(now time.Time, amount tenantcostmodel.RU) {
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		l.qp.tb.RemoveTokens(now, amount)
		l.maybeNotifyLocked(now)

		// Don't notify the head of the queue; this change can only delay the time
		// it can go through.
		return false
	})
}

// limiterReconfigureArgs is used to update the limiter's configuration.
type limiterReconfigureArgs struct {
	// NewTokens is the number of tokens that should be added to the token bucket.
	NewTokens tenantcostmodel.RU

	// NewRate is the new token fill rate for the bucket.
	NewRate tenantcostmodel.RU

	// NotifyThreshold is the AvailableRU level below which a low RU notification
	// will be sent.
	NotifyThreshold tenantcostmodel.RU
}

// Reconfigure is used to call tokenBucket.Reconfigure under the pool's lock.
func (l *limiter) Reconfigure(now time.Time, cfg limiterReconfigureArgs) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		// If we already produced a notification that wasn't processed, drain it.
		// This not racy as long as this code does not run in parallel with the
		// code that receives from notifyCh.
		select {
		case <-l.notifyCh:
		default:
		}
		l.qp.notifyThreshold = cfg.NotifyThreshold

		l.qp.tb.Reconfigure(now, tokenBucketReconfigureArgs{
			NewTokens: cfg.NewTokens,
			NewRate:   cfg.NewRate,
		})
		l.maybeNotifyLocked(now)

		// Notify the head of the queue; the new configuration might allow a
		// request to go through earlier.
		return true
	})
}

// SetupNotification is used to call tokenBucket.SetupNotification under the
// pool's lock.
func (l *limiter) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.qp.notifyThreshold = threshold
		l.maybeNotifyLocked(now)
		return false
	})
}

// OnTick is called by the tenant-side controller on each ticker callback (~once
// per second). It passes the amount of SQL and KV RU usage that has accumulated
// during the last tick.
func (l *limiter) OnTick(now time.Time, sqlRU, kvRU tenantcostmodel.RU) {
	l.qp.Update(func(res quotapool.Resource) (shouldNotify bool) {
		if !l.qp.usedSQLRU {
			// During this last tick, there have been no KV responses to associate
			// with SQL RU usage. Remove all previous SQL usage directly from the
			// token bucket without attempting to distribute it across operations.
			l.qp.tb.RemoveTokens(now, l.qp.sqlRU)
			l.qp.sqlRU = 0
		}
		l.qp.usedSQLRU = false

		// Add new SQL usage accumulated during the last tick.
		l.qp.sqlRU += sqlRU

		// Calculate the updated moving average of KV RUs consumed per tick.
		if l.qp.avgKVRUPerTick == 0 {
			l.qp.avgKVRUPerTick = kvRU
		} else {
			l.qp.avgKVRUPerTick *= (1 - movingAvgKVRUPerTickFactor)
			l.qp.avgKVRUPerTick += kvRU * movingAvgKVRUPerTickFactor
		}

		// For each RU needed by a KV operation, how many SQL RUs should be added to
		// that operation? Use the moving average to make an estimate of that ratio.
		if l.qp.avgKVRUPerTick > 1 {
			l.qp.sqlRUPerKVRU = l.qp.sqlRU / l.qp.avgKVRUPerTick
		} else {
			// Add entire debt to the first KV operation that occurs.
			l.qp.sqlRUPerKVRU = tenantcostmodel.RU(math.Inf(1))
		}

		// Check if the changes require a notification.
		l.maybeNotifyLocked(now)

		// OnTick only removes tokens from the bucket, so no need to notify head
		// of queue.
		return false
	})
}

func (l *limiter) String(now time.Time) string {
	var s string
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		s = fmt.Sprintf("%s (%.2f SQL RU, %.2f waiting RU, %.2f SQL/KV ratio)",
			l.qp.tb.String(now), l.qp.sqlRU, l.qp.waitingRU, l.qp.sqlRUPerKVRU)
		return false
	})
	return s
}

// amortizeSQLRU returns the portion of sqlRU that should be combined with the
// RUs needed by a read/write operation. The sqlRU is amortized across the
// operations expected to arrive in the current tick, and is proportional to
// their size.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) amortizeSQLRU(needed tenantcostmodel.RU) tenantcostmodel.RU {
	l.qp.usedSQLRU = true

	sqlRU := needed * l.qp.sqlRUPerKVRU
	if sqlRU > l.qp.sqlRU {
		sqlRU = l.qp.sqlRU
	}

	l.qp.sqlRU -= sqlRU
	return sqlRU
}

// availableRULocked returns the current number of available RUs. This can be
// negative if we have accumulated debt or we have waiting requests.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) availableRULocked(now time.Time) tenantcostmodel.RU {
	available := l.qp.tb.AvailableTokens(now)

	// Subtract accumulated SQL RU and waiting RU debts.
	available -= l.qp.sqlRU
	available -= l.qp.waitingRU
	return available
}

// notify tries to send a non-blocking notification on lowRUNotify and disables
// further notifications (until the next Reconfigure or SetupNotification).
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) notifyLocked() {
	l.qp.notifyThreshold = 0
	select {
	case l.notifyCh <- struct{}{}:
	default:
	}
}

// maybeNotify checks if it's time to send the notification and if so, performs
// the notification.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeNotifyLocked(now time.Time) {
	if l.qp.notifyThreshold > 0 {
		available := l.availableRULocked(now)
		if available < l.qp.notifyThreshold {
			l.notifyLocked()
		}
	}
}

// maybeAddWaitingRULocked increases "waitingRU" by the total SQL and KV RU
// needed by the given request. It can be called multiple times, but should only
// increase waitingRU the first time.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeAddWaitingRULocked(req *waitRequest) {
	if !req.isWaitingRU {
		l.qp.waitingRU += (req.kvRU + req.sqlRU)
		req.isWaitingRU = true
	}
}

// maybeRemoveWaitingRULocked subtracts any "waitingRU" reserved by
// addWaitingRULocked. It is called either in Acquire or in the OnWaitFinish
// callback, depending on whether the request completes successfully or is
// canceled, respectively.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeRemoveWaitingRULocked(req *waitRequest) {
	if req.isWaitingRU {
		l.qp.waitingRU -= (req.kvRU + req.sqlRU)
		req.isWaitingRU = false
	}
}

// waitRequest is used to wait for adequate resources in the tokenBucket.
type waitRequest struct {
	// kvRU is the RUs required by the KV read/write operation.
	kvRU tenantcostmodel.RU

	// sqlRU is the additional RUs that are added to this request in order to
	// amortize SQL layer usage (e.g. CPU and Egress).
	sqlRU tenantcostmodel.RU

	// isWaitingRU is true if this request's SQL and KV RUs are included in the
	// limiter's waitingRU total. This tracks whether that RU needs to be
	// subtracted from waitingRU once the request is complete.
	isWaitingRU bool
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool. If amortizeSQLRU
// is true, then Acquire will add amortized SQL RUs to this request.
// The waitRequest should be returned with putWaitRequest.
func newWaitRequest(kvRU tenantcostmodel.RU, amortizeSQLRU bool) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{kvRU: kvRU}
	if amortizeSQLRU {
		// -1 tells Acquire to add amortized SQL RUs.
		r.sqlRU = -1
	}
	return r
}

// putWaitRequest returns the waitRequest to the sync.Pool.
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

	// Compute any SQL RUs that will be added to this request, if it has not yet
	// been done.
	if req.sqlRU == -1 {
		req.sqlRU = l.amortizeSQLRU(req.kvRU)
	}

	fulfilled, tryAgainAfter = l.qp.tb.TryToFulfill(now, req.kvRU+req.sqlRU)
	if !fulfilled {
		// This request will now be blocked (or is already blocked if it was
		// already waiting) in the quota pool's queue. However, it still needs to
		// be reliably reflected in the next call to AvailableRUs(). If it is not
		// reflected, a low RU notification might effectively be ignored because
		// it looks like we have enough RUs.
		l.maybeAddWaitingRULocked(req)
	} else {
		// If this request was part of waitingRU, eagerly subtract it now that it's
		// complete. Don't wait until OnWaitFinish, since there is a small window
		// between now and then where waitingRU will be incorrect.
		l.maybeRemoveWaitingRULocked(req)
	}

	// Check if new token bucket level is low enough to trigger notification.
	l.maybeNotifyLocked(now)

	return fulfilled, tryAgainAfter
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
