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
// limiter's methods are thread-safe.
type limiter struct {
	// metrics manages the set of metrics that are tracked by the cost client.
	metrics *metrics

	// notifyCh gets a (non-blocking) send when available tokens falls below the
	// notifyThreshold.
	notifyCh chan struct{}

	// Access to these fields is protected by the quota pool lock. Only access
	// them in the scope of the abstract pool's Update method or within the
	// wait request's Acquire method.
	qp struct {
		*quotapool.AbstractPool

		// tb is the underlying local token bucket controlled by the quota pool.
		tb tokenBucket

		// notifyThreshold is the available token level below which a notification
		// should be sent to notifyCh. A threshold <= 0 disables notifications.
		notifyThreshold float64

		// waitingTokens is the total (rounded) tokens needed for all currently
		// waiting requests (or requests that are in the process of being
		// fulfilled).
		waitingTokens float64
	}
}

func (l *limiter) Init(metrics *metrics, timeSource timeutil.TimeSource, notifyCh chan struct{}) {
	*l = limiter{metrics: metrics, notifyCh: notifyCh}

	onWaitStartFn := func(ctx context.Context, poolName string, r quotapool.Request) {
		// Add to the waiting tokens total if this request has not already been
		// added to it in Acquire.
		l.metrics.CurrentBlocked.Inc(1)
		l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
			l.maybeAddWaitingTokensLocked(r.(*waitRequest))
			return false
		})
	}

	onWaitFinishFn := func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
		// Remove any waiting tokens that were not fulfilled by Acquire due to
		// context cancellation.
		l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
			l.maybeRemoveWaitingTokensLocked(r.(*waitRequest))
			return false
		})
		l.metrics.CurrentBlocked.Dec(1)

		// Log a trace event for requests that waited for a long time.
		if waitDuration := timeSource.Since(start); waitDuration > time.Second {
			log.VEventf(ctx, 1, "request waited for tokens for %s", waitDuration.String())
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

// AvailableTokens returns the current number of available tokens. This can be
// negative if the token bucket is in debt, or we have pending waiting requests.
func (l *limiter) AvailableTokens(now time.Time) float64 {
	var result float64
	l.qp.Update(func(quotapool.Resource) (shouldNotify bool) {
		result = l.availableTokensLocked(now)
		return false
	})
	return result
}

// Wait blocks until the token bucket has the needed number of tokens available.
// Wait is called once a KV or External I/O operation has completed and is ready
// to be accounted for. It's also called before a KV operation starts, in order
// to block if the token bucket is in debt.
func (l *limiter) Wait(ctx context.Context, needed float64) error {
	r := newWaitRequest(needed)
	defer putWaitRequest(r)
	return l.qp.Acquire(ctx, r)
}

// RemoveTokens removes tokens from the bucket immediately, potentially putting
// it into debt.
func (l *limiter) RemoveTokens(now time.Time, amount float64) {
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
	NewTokens float64

	// NewRate is the new token fill rate for the bucket.
	NewRate float64

	// MaxTokens is the maximum number of tokens that can be present in the
	// bucket. Tokens beyond this limit are discarded. If MaxTokens = 0, then
	// no limit is enforced.
	MaxTokens float64

	// NotifyThreshold is the AvailableTokens level below which a low tokens
	// notification will be sent.
	NotifyThreshold float64
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

// SetupNotification sets a new low tokens notification threshold and triggers
// the notification if available tokens are lower than the new threshold.
func (l *limiter) SetupNotification(now time.Time, threshold float64) {
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
		if l.qp.waitingTokens > 0 {
			s += fmt.Sprintf(" (%.2f waiting tokens)", l.qp.waitingTokens)
		}
		return false
	})
	return s
}

// availableTokensLocked returns the current number of available tokens. This
// can be negative if we have accumulated debt or we have waiting requests.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) availableTokensLocked(now time.Time) float64 {
	available := l.qp.tb.AvailableTokens(now)

	// Subtract any waiting tokens.
	available -= l.qp.waitingTokens
	return available
}

// notifyLocked sends a non-blocking notification on lowTokensNotify (unless
// there is already a notification pending in the channel), and disables further
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
		available := l.availableTokensLocked(now)
		if available < l.qp.notifyThreshold {
			l.notifyLocked()
		}
	}
}

// maybeAddWaitingTokensLocked increases "waitingTokens" by the total tokens
// needed by the given request. It can be called multiple times, but should
// only increase waitingTokens the first time.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeAddWaitingTokensLocked(req *waitRequest) {
	if !req.waitingTokensAccounted {
		l.qp.waitingTokens += req.needed
		req.waitingTokensAccounted = true
	}
}

// maybeRemoveWaitingTokensLocked subtracts any "waitingTokens" reserved by
// addWaitingTokensLocked. It is called either in Acquire or in the OnWaitFinish
// callback, depending on whether the request completes successfully or is
// canceled, respectively.
// NOTE: This must be called under the scope of the quota pool lock.
func (l *limiter) maybeRemoveWaitingTokensLocked(req *waitRequest) {
	if req.waitingTokensAccounted {
		l.qp.waitingTokens -= req.needed
		req.waitingTokensAccounted = false
	}
}

// waitRequest is used to wait for adequate resources in the tokenBucket for a
// KV or external I/O operation.
type waitRequest struct {
	// needed is the number of tokens required by the operation.
	needed float64

	// waitingTokensAccounted is true if we counted the needed tokens as
	// "waiting tokens". This is used to determine whether the tokens need to be
	// subtracted from waitingTokens once the request is complete.
	waitingTokensAccounted bool
}

var _ quotapool.Request = (*waitRequest)(nil)

var waitRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(waitRequest) },
}

// newWaitRequest allocates a waitRequest from the sync.Pool.
// The waitRequest should be returned with putWaitRequest.
func newWaitRequest(needed float64) *waitRequest {
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
		// be reliably reflected in the next call to AvailableTokens(). If it is
		// not reflected, a low tokens notification might effectively be ignored
		// because it looks like we have enough tokens. Don't wait until
		// OnWaitStart to call this, since there is a small unlocked window between
		// now and then where waitingTokens will be incorrect.
		l.maybeAddWaitingTokensLocked(req)
	} else {
		// If this request was part of waitingTokens, eagerly subtract it now that
		// it's complete. Don't wait until OnWaitFinish to call this, since there
		// is a small unlocked window between now and then where waitingTokens will
		// be incorrect.
		l.maybeRemoveWaitingTokensLocked(req)
	}

	// Check if new token bucket level is low enough to trigger notification.
	l.maybeNotifyLocked(now)

	return fulfilled, tryAgainAfter
}

// ShouldWait is part of quotapool.Request.
func (req *waitRequest) ShouldWait() bool {
	return true
}
