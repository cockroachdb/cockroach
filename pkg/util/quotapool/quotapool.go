// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package quotapool provides an abstract implementation of a pool of resources
// to be distributed among concurrent clients.
//
// The library also offers a concrete implementation of such a quota pool for
// single-dimension integer quota. This IntPool acts like a weighted semaphore
// that additionally offers FIFO ordering for serving requests.
package quotapool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TODO(ajwerner): provide option to limit the maximum queue size.
// TODO(ajwerner): provide mechanism to collect metrics.

// Resource is an interface that represents a quantity which is being
// pooled and allocated. It is any quantity that can be subdivided and
// combined.
//
// This library does not provide any concrete implementations of Resource but
// internally the *IntAlloc is used as a resource.
type Resource interface {

	// Merge combines other into the current resource.
	// After a Resource (other) is passed to Merge, the QuotaPool will never use
	// that Resource again. This behavior allows clients to pool instances of
	// Resources by creating Resource during Acquisition and destroying them in
	// Merge.
	Merge(other Resource)
}

// Request is an interface used to acquire quota from the pool.
// Request is responsible for subdividing a resource into the portion which is
// retained when the Request is fulfilled and the remainder.
type Request interface {

	// Acquire decides whether a Request can be fulfilled by a given quantity of
	// Resource.
	//
	// If it is not fulfilled it must not modify or retain the passed alloc.
	// If it is fulfilled, it should return any portion of the Alloc it does
	// not intend to use.
	//
	// It is up to the implementer to decide if it makes sense to return a
	// zero-valued, non-nil Resource or nil as unused when acquiring all of the
	// passed Resource. If nil is returned and there is a notion of acquiring a
	// zero-valued Resource unit from the pool then those acquisitions may need to
	// wait until the pool is non-empty before proceeding. Those zero-valued
	// acquisitions will still need to wait to be at the front of the queue. It
	// may make sense for implementers to special case zero-valued acquisitions
	// entirely as IntPool does.
	Acquire(context.Context, Resource) (fulfilled bool, unused Resource)
}

// ErrClosed is returned from Acquire after Close has been called.
type ErrClosed struct {
	poolName string
	reason   string
}

// Error implements error.
func (ec *ErrClosed) Error() string {
	return fmt.Sprintf("%s pool closed: %s", ec.poolName, ec.reason)
}

// QuotaPool is an abstract implementation of a pool that stores some unit of
// Resource. The basic idea is that it allows requests to acquire a quantity of
// Resource from the pool in FIFO order in a way that interacts well with
// context cancelation.
type QuotaPool struct {
	config

	// chanSyncPool is used to pool allocations of the channels used to notify
	// goroutines waiting in Acquire.
	chanSyncPool sync.Pool

	// We use a channel to 'park' our quota value for easier composition with
	// context cancellation and quotaPool closing (see quotaPool.Acquire).
	//
	// Resource additions push a value into the channel whereas the acquisition
	// first in line waits on the channel itself.
	quota chan Resource

	// Ongoing acquisitions listen on done which is closed when the quota
	// pool is closed (see QuotaPool.Close).
	done chan struct{}

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
		q notifyQueue

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
	initializeNotifyQueue(&qp.mu.q)
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

func (qp *QuotaPool) addLocked(r Resource) {
	select {
	case q := <-qp.quota:
		r.Merge(q)
	default:
	}
	qp.quota <- r
}

// Acquire attempts to fulfill the Request with Resource from the qp.
// Requests are serviced in a FIFO order; only a single request is ever
// being offered resources at a time. A Request will be offered the pool's
// current quantity of Resource until it is fulfilled or its context is
// canceled.
//
// Safe for concurrent use.
func (qp *QuotaPool) Acquire(ctx context.Context, r Request) (err error) {
	notifyCh := qp.chanSyncPool.Get().(chan struct{})
	qp.mu.Lock()
	var closeErr error
	if qp.mu.closed {
		closeErr = qp.closeErr
	} else {
		qp.mu.q.enqueue(notifyCh)
		// If we're first in line, we notify ourself immediately.
		if qp.mu.q.len == 1 {
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
		start = timeutil.Now()
		slowTimer = timeutil.NewTimer()
		defer slowTimer.Stop()
		// Intentionally reset only once, for we care more about the select duration in
		// goroutine profiles than periodic logging.
		slowTimer.Reset(qp.slowAcquisitionThreshold)
		slowTimerC = slowTimer.C
	}
	for {
		select {
		case <-slowTimerC:
			slowTimer.Read = true
			defer qp.onSlowAcquisition(ctx, qp.name, r, start)()
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
			if qp.mu.q.peek() == notifyCh {
				// It is possible that we were notified before we grabbed the mutex but
				// after the context cancellation in which case we must clear notifyCh
				// so that qp.notifyNextLocked() can put it back into the pool.
				select {
				case <-notifyCh:
				default:
				}
				qp.notifyNextLocked()
			} else {
				// NB: Notifying the channel before moving it to the head of the
				// queue is safe because the queue itself is guarded by a lock.
				// Goroutines are not a risk of getting notified and finding
				// out they're not first in line.
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

		}
		break
	}

	// We're first in line to receive quota, we keep accumulating quota until
	// we've acquired enough or determine we no longer need the acquisition.
	// If we have acquired the quota needed or our context gets canceled,
	// we're sure to remove ourselves from the queue and notify the goroutine
	// next in line (if any).
	var alloc, unused Resource
	var fulfilled bool
	for !fulfilled {
		select {
		case <-slowTimerC:
			slowTimer.Read = true
			defer qp.onSlowAcquisition(ctx, qp.name, r, start)()
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
			fulfilled, unused = r.Acquire(ctx, alloc)
		}
	}
	qp.mu.Lock()
	if unused != nil {
		qp.addLocked(unused)
	}
	qp.notifyNextLocked()
	qp.mu.Unlock()
	return nil
}

// notifyNextLocked notifies the waiting acquisition goroutine next in line (if
// any). It requires that qp.mu.Mutex is held.
func (qp *QuotaPool) notifyNextLocked() {
	// We're at the head of the queue. In all cases we ensure that the channel
	// at the head of the queue has already been notified.
	qp.chanSyncPool.Put(qp.mu.q.dequeue())
	// We traverse until we find a goroutine waiting to be notified, notify the
	// goroutine and truncate our queue so to ensure the said goroutine is at the
	// head of the queue. Normally the next lined up waiter is the one waiting for
	// notification, but if others behind us have also gotten their context
	// canceled, they will leave behind waiters that we would skip below.
	//
	// If we determine there are no goroutines waiting, we simply truncate the
	// queue to reflect this.
	for ch := qp.mu.q.peek(); ch != nil; ch = qp.mu.q.peek() {
		select {
		case ch <- struct{}{}:
		default:
			// When requests are canceled by their context, they ensure that they
			// cannot be notified by sending a message on their own queued notify
			// channel. The default case here deals with canceled queued channels
			// by clearing the channel before putting it back in the pool and then
			// shifting the queue.
			<-ch
			qp.chanSyncPool.Put(qp.mu.q.dequeue())
			continue
		}
		break
	}
}

// ApproximateQuota will report approximately the amount of quota available
// in the pool to f. It is accurate only if there are no ongoing acquisition
// goroutines. If there are, the passed value can be nil if there is an ongoing
// acquisition or if non-nil may contain a value up to 'v' less than actual
// available quota where 'v' is the value the acquisition goroutine first in
// line is attempting to acquire. The provided Resource must not be mutated.
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
