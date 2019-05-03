// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package quotapool provides an abstract implementation of a
package quotapool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Resource is an abstract interface that represents a quantity which has been
// taken from the QuotaPool.
type Resource interface {
	Merge(other Resource)
}

// Request is an interface used to acquire quota from the pool.
type Request interface {

	// Acquire decides whether a Request can be fulfilled by a given quantity of
	// Resource.
	// If it is not fulfilled it must not modify or retain the passed	alloc.
	// If it is fulfilled, it should return any portion of the Alloc it does
	// not intend to use.
	Acquire(context.Context, Resource) (fulfilled bool, extra Resource)
}

// ErrClosed is returned from Acquire after Close has been called.
type ErrClosed struct {
	poolName string
	reason   string
}

func (ec *ErrClosed) Error() string {
	return fmt.Sprintf("%s pool closed: %s", ec.poolName, ec.reason)
}

// QuotaPool is an abstract implementation of a pool that stores some unit of
// quota. The basic idea is that it allows requests to acquire a quantity of
// abstract Alloc from the pool while playing nice with concepts with context
// cancelation. Alloc is some unit which can be merged together via a Pool and
// can be acquired by a request.
type QuotaPool struct {
	config

	// We use a channel to 'park' our quota value for easier composition with
	// context cancellation and quotaPool closing (see quotaPool.acquire).
	//
	// Alloc additions push a value into the channel whereas the acquisition
	// first in line waits on the channel itself.
	quota chan Resource

	// Ongoing acquisitions listen on quotaPool.done which is closed when the quota
	// pool is closed (see QuotaPool.Close).
	done chan struct{}

	// chanSyncPool is used to pool allocations of the channels used to notify
	// goroutines waiting in Acquire.
	chanSyncPool sync.Pool

	// closeErr is populated with a non-nil error when Close is called.
	closeErr *ErrClosed

	mu struct {
		syncutil.Mutex

		// We service quota acquisitions in a first come, first serve basis. This
		// is done in order to prevent starvations of large acquisitions by a
		// continuous stream of smaller ones. Acquisitions 'register' themselves
		// for a notification that indicates they're now first in line. This is
		// done by appending to the queue the channel they will then wait
		// on. If a goroutine no longer needs to be notified, i.e. their
		// acquisition context has been canceled, the goroutine is responsible for
		// blocking subsequent notifications to the channel by filling up the
		// channel buffer.
		queue []chan struct{}

		// closed is set to true when the quota pool is closed (see
		// QuotaPool.Close).
		closed bool
	}
}

// New returns a new quota pool initialized with a given quota. The quota
// is capped at this amount, meaning that callers may return more quota than they
// acquired without ever making more than the quota capacity available.
func New(name string, initialResource Resource, options ...Option) *QuotaPool {
	qp := &QuotaPool{
		quota: make(chan Resource, 1),
		done:  make(chan struct{}),
		chanSyncPool: sync.Pool{
			New: func() interface{} { return make(chan struct{}, 1) },
		},
	}
	for _, o := range options {
		o.apply(&qp.config)
	}
	qp.quota <- initialResource
	return qp
}

// Add adds the provided Alloc back to the pool. The value will be merged with
// the existing resources in the QuotaPool if there are any.
//
// Safe for concurrent use.
func (qp *QuotaPool) Add(v Resource) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	qp.addLocked(v)
}

// addLocked is like add, but it requires that qp.Lock is held.
func (qp *QuotaPool) addLocked(r Resource) {
	select {
	case q := <-qp.quota:
		r.Merge(q)
	default:
	}
	qp.quota <- r
}

// Acquire acquires the specified amount of quota from the pool. On success,
// nil is returned and the caller must call add(v) or otherwise arrange for the
// quota to be returned to the pool. If 'v' is greater than the total capacity
// of the pool, we instead try to acquire quota equal to the maximum capacity.
//
// Safe for concurrent use.
func (qp *QuotaPool) Acquire(ctx context.Context, r Request) (err error) {
	notifyCh := qp.chanSyncPool.Get().(chan struct{})
	qp.mu.Lock()
	var closeErr error
	if qp.mu.closed {
		closeErr = qp.closeErr
	} else {
		qp.mu.queue = append(qp.mu.queue, notifyCh)
		// If we're first in line, we notify ourself immediately.
		if len(qp.mu.queue) == 1 {
			notifyCh <- struct{}{}
		}
	}
	qp.mu.Unlock()
	if closeErr != nil {
		return closeErr
	}
	// Set up the infrastructure to report slow requests.
	var slowTimer *timeutil.Timer
	var slowTimerC <-chan time.Time
	var start time.Time
	if qp.onSlowAcquisition != nil {
		slowTimer = timeutil.NewTimer()
		slowTimerC = slowTimer.C
		defer slowTimer.Stop()
		start = timeutil.Now()
		// Intentionally reset only once, for we care more about the select duration in
		// goroutine profiles than periodic logging.
		slowTimer.Reset(base.SlowRequestThreshold)
	}

	for {
		select {
		case <-slowTimerC:
			slowTimer.Read = true
			defer logSlowAcquire(ctx, qp.name, r, start)()
			continue
		case <-ctx.Done():
			qp.mu.Lock()
			// We no longer need to be notified but we need to be careful and check
			// whether or not we're first in queue. If so, we need to notify the
			// next acquisition goroutine and clean up the waiting queue while
			// doing so.
			//
			// Else we simply 'unregister' ourselves from the queue by filling
			// up the channel buffer. This is what is checked when a goroutine
			// wishes to notify the next in line.
			if qp.mu.queue[0] == notifyCh {
				// NB: Notifying the channel before moving it to the head of the
				// queue is safe because the queue itself is guarded by a lock.
				// Goroutines are not a risk of getting notified and finding
				// out they're not first in line.
				qp.notifyNextLocked()
			} else {
				notifyCh <- struct{}{}
			}

			qp.mu.Unlock()
			return ctx.Err()
		case <-qp.done:
			// We don't need to 'unregister' ourselves as in the case when the
			// context is canceled. In fact, we want others waiters to only
			// receive on qp.done and signaling them would work against that.
			return qp.closeErr // always non-nil when qp.done is closed
		case <-notifyCh:
			qp.chanSyncPool.Put(notifyCh)
		}
		break
	}

	// We're first in line to receive quota, we keep accumulating quota until
	// we've acquired enough or determine we no longer need the acquisition.
	// If we have acquired the quota needed or our context gets canceled,
	// we're sure to remove ourselves from the queue and notify the goroutine
	// next in line (if any).
	var alloc, extra Resource
	var fulfilled bool
	for !fulfilled {
		select {
		case <-slowTimerC:
			slowTimer.Read = true
			defer logSlowAcquire(ctx, qp.name, r, start)()
		case <-ctx.Done():
			qp.mu.Lock()
			if alloc != nil {
				qp.addLocked(alloc)
			}
			qp.notifyNextLocked()
			qp.mu.Unlock()
			return ctx.Err()
		case <-qp.done:
			// We don't need to release quota back as all ongoing and
			// subsequent acquisitions will succeed immediately.
			return qp.closeErr // always non-nil when qp.done is closed
		case more := <-qp.quota:
			if alloc == nil {
				alloc = more
			} else {
				alloc.Merge(more)
			}
			fulfilled, extra = r.Acquire(ctx, alloc)
		}
	}
	qp.mu.Lock()
	if extra != nil {
		qp.addLocked(extra)
	}
	qp.notifyNextLocked()
	qp.mu.Unlock()
	return nil
}

// notifyNextLocked notifies the waiting acquisition goroutine next in line (if
// any). It requires that qp.Lock is held.
func (qp *QuotaPool) notifyNextLocked() {
	// We're at the head of the queue. We traverse until we find a goroutine
	// waiting to be notified, notify the goroutine and truncate our queue so
	// to ensure the said goroutine is at the head of the queue. Normally the
	// next lined up waiter is the one waiting for notification, but if others
	// behind us have also gotten their context canceled, they
	// will leave behind waiters that we would skip below.
	//
	// If we determine there are no goroutines waiting, we simply truncate the
	// queue to reflect this.
	qp.mu.queue = qp.mu.queue[1:]
	for _, ch := range qp.mu.queue {
		select {
		case ch <- struct{}{}:
		default:
			select {
			case <-ch:
			default:
			}
			qp.chanSyncPool.Put(ch)
			qp.mu.queue = qp.mu.queue[1:]
			continue
		}
		break
	}
}

// ApproximateQuota will report approximately the amount of quota available
// in the pool to f. It is accurate only if there are no ongoing acquisition
// goroutines. If there are, the passed value can be nil or maybe contain a
// value up to 'v' less than actual available quota where 'v' is the value the
// acquisition goroutine first in line is attempting to acquire.
func (qp *QuotaPool) ApproximateQuota(f func(Resource)) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	select {
	case q := <-qp.quota:
		f(q)
		qp.quota <- q
	default:
		f(nil)
	}
}

// Close signals to all ongoing and subsequent acquisitions that they are
// free to return to their callers. They will receive an *ErrClosed which
// contains this reason.
//
// Safe for concurrent use.
func (qp *QuotaPool) Close(reason string) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if qp.closeErr == nil {
		qp.mu.closed = true
		qp.closeErr = &ErrClosed{
			poolName: qp.name,
			reason:   reason,
		}
		close(qp.done)
	}
}
