// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
// are updated every second and removed from the bucket via the RemoveRU method.
//
// limiter's methods are thread-safe.
type limiter struct {
	// metrics manages the set of metrics that are tracked by the cost client.
	metrics *metrics

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
	}
}

func (l *limiter) Init(metrics *metrics, timeSource timeutil.TimeSource, notifyCh chan struct{}) {
	*l = limiter{metrics: metrics, notifyCh: notifyCh}

	onWaitStartFn := func(ctx context.Context, poolName string, r quotapool.Request) {
		// Add to the waiting RU total if this request has not already been added
		// to it in Acquire.
		l.metrics.CurrentBlocked.Inc(1)
		l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
			l.maybeAddWaitingRULocked(r.(*waitRequest))
			return false
		})
	}

	onWaitFinishFn := func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
		// Remove any waiting RUs that were not fulfilled by Acquire due to
		// context cancellation.
		l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
			l.maybeRemoveWaitingRULocked(r.(*waitRequest))
			return false
		})
		l.metrics.CurrentBlocked.Dec(1)

		// Log a trace event for requests that waited for a long time.
		if waitDuration := timeSource.Since(start); waitDuration > time.Second {
			log.VEventf(ctx, 1, "request waited for RUs for %s", waitDuration.String())
		}
	}

	l.qp.AbstractPool = quotapool.New(
		"tenant-side-limiter", l,
		quotapool.WithTimeSource(timeSource),
		quotapool.OnWaitStart(onWaitStartFn),
		quotapool.OnWaitFinish(onWaitFinishFn),
	)

	l.qp.tb.Init(timeSource.Now())
}

func (l *limiter) Close() {
	l.qp.Close("shutting down")
}

// AvailableRU returns the current number of available RUs. This can be negative
// if the token bucket is in debt, or we have pending waiting requests.
func (l *limiter) AvailableRU(now time.Time) tenantcostmodel.RU {
	var result tenantcostmodel.RU
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = l.availableRULocked(now)
		return false
	})
	return result
}

// Wait blocks until the token bucket has the needed number of tokens available.
// Wait is called once a KV or External I/O operation has completed and is ready
// to be accounted for. It's also called before a KV operation starts, in order
// to block if the token bucket is in debt.
func (l *limiter) Wait(ctx context.Context, needed tenantcostmodel.RU) error {
	r := newWaitRequest(needed)
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

	// MaxTokens is the maximum number of tokens that can be present in the
	// bucket. Tokens beyond this limit are discarded. If MaxTokens = 0, then
	// no limit is enforced.
	MaxTokens tenantcostmodel.RU

	// NotifyThreshold is the AvailableRU level below which a low RU notification
	// will be sent.
	NotifyThreshold tenantcostmodel.RU
}

// Reconfigure is used to call tokenBucket.Reconfigure under the pool's lock.
func (l *limiter) Reconfigure(now time.Time, cfg limiterReconfigureArgs) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		// If we already produced a notification that wasn't processed, drain it.
		select {
		case <-l.notifyCh:
		default:
		}
		l.qp.notifyThreshold = cfg.NotifyThreshold

		l.qp.tb.Reconfigure(now, tokenBucketReconfigureArgs{
			NewTokens: cfg.NewTokens,
			NewRate:   cfg.NewRate,
			MaxTokens: cfg.MaxTokens,
		})
		l.maybeNotifyLocked(now)

		// Notify the head of the queue; the new configuration might allow a
		// request to go through earlier.
		return true
	})
}

// SetupNotification sets a new low RU notification threshold and triggers the
// notification if available RUs are lower than the new threshold.
func (l *limiter) SetupNotification(now time.Time, threshold tenantcostmodel.RU) {
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		l.qp.notifyThreshold = threshold
		l.maybeNotifyLocked(now)
		return false
	})
}

func (l *limiter) String(now time.Time) string {
	var s string
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		s = l.qp.tb.String(now)
		if l.qp.waitingRU > 0 {
			s += fmt.Sprintf(" (%.2f waiting RU)", l.qp.waitingRU)
		}
		return false
	})
	return s
}

// availableRULocked returns the current number of available RUs. This can be
// negative if we have accumulated debt or we have waiting requests.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) availableRULocked(now time.Time) tenantcostmodel.RU {
	available := l.qp.tb.AvailableTokens(now)

	// Subtract any waiting RU.
	available -= l.qp.waitingRU
	return available
}

// notifyLocked sends a non-blocking notification on lowRUNotify (unless there
// is already a notification pending in the channel), and disables further
// notifications (until the next Reconfigure or SetupNotification).
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) notifyLocked() {
	l.qp.notifyThreshold = 0
	select {
	case l.notifyCh <- struct{}{}:
	default:
	}
}

// maybeNotifyLocked checks if it's time to send the notification and if so,
// performs the notification.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeNotifyLocked(now time.Time) {
	if l.qp.notifyThreshold > 0 {
		available := l.availableRULocked(now)
		if available < l.qp.notifyThreshold {
			l.notifyLocked()
		}
	}
}

// maybeAddWaitingRULocked increases "waitingRU" by the total RUs needed by the
// given request. It can be called multiple times, but should only increase
// waitingRU the first time.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeAddWaitingRULocked(req *waitRequest) {
	if !req.waitingRUAccounted {
		l.qp.waitingRU += req.needed
		req.waitingRUAccounted = true
	}
}

// maybeRemoveWaitingRULocked subtracts any "waitingRU" reserved by
// addWaitingRULocked. It is called either in Acquire or in the OnWaitFinish
// callback, depending on whether the request completes successfully or is
// canceled, respectively.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeRemoveWaitingRULocked(req *waitRequest) {
	if req.waitingRUAccounted {
		l.qp.waitingRU -= req.needed
		req.waitingRUAccounted = false
	}
}

// waitRequest is used to wait for adequate resources in the tokenBucket for a
// KV or external I/O operation.
type waitRequest struct {
	// needed is the RUs required by the operation.
	needed tenantcostmodel.RU

	// waitingRUAccounted is true if we counted the needed RUs as "waiting RU".
	// This is used to determine whether the RU needs to be subtracted from
	// waitingRU once the request is complete.
	waitingRUAccounted bool
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// The waitRequest should be returned with putWaitRequest.
func newWaitRequest(needed tenantcostmodel.RU) *waitRequest {
	r := waitRequestSyncPool.Get().(*waitRequest)
	*r = waitRequest{needed: needed}
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

	fulfilled, tryAgainAfter = l.qp.tb.TryToFulfill(now, req.needed)
	if !fulfilled {
		// This request will now be blocked (or is already blocked if it was
		// already waiting) in the quota pool's queue. However, it still needs to
		// be reliably reflected in the next call to AvailableRUs(). If it is not
		// reflected, a low RU notification might effectively be ignored because
		// it looks like we have enough RUs. Don't wait until OnWaitStart to call
		// this, since there is a small unlocked window between now and then where
		// waitingRU will be incorrect.
		l.maybeAddWaitingRULocked(req)
	} else {
		// If this request was part of waitingRU, eagerly subtract it now that it's
		// complete. Don't wait until OnWaitFinish to call this, since there is a
		// small unlocked window between now and then where waitingRU will be
		// incorrect.
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
