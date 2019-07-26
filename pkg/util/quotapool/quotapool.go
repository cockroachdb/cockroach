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

	// name is used for logging purposes and is passed to functions used to report
	// acquistions or slow acqusitions.
	name string

	// Ongoing acquisitions listen on done which is closed when the quota
	// pool is closed (see QuotaPool.Close).
	done chan struct{}

	// closeErr is populated with a non-nil error when Close is called.
	closeErr *ErrClosed

	mu struct {
		syncutil.Mutex

		// quota stores the current quantity of quota available in the pool.
		quota Resource

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

		// numCanceled is the number of members of q which have been canceled.
		// It is used to determine the current number of active waiters in the queue
		// which is q.len() less this value.
		numCanceled int

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
		name: name,
		done: make(chan struct{}),
	}
	for _, o := range options {
		o.apply(&qp.config)
	}
	qp.mu.quota = initialResource
	initializeNotifyQueue(&qp.mu.q)
	return qp
}

// ApproximateQuota will report approximately the amount of quota available
// in the pool to f. The provided Resource must not be mutated.
func (qp *QuotaPool) ApproximateQuota(f func(Resource)) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	f(qp.mu.quota)
}

// Len returns the current length of the queue for this QuotaPool.
func (qp *QuotaPool) Len() int {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	return int(qp.mu.q.len) - qp.mu.numCanceled
}

// Close signals to all ongoing and subsequent acquisitions that they are
// free to return to their callers. They will receive an *ErrClosed which
// contains this reason.
//
// Safe for concurrent use.
func (qp *QuotaPool) Close(reason string) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if qp.mu.closed {
		return
	}
	qp.mu.closed = true
	qp.closeErr = &ErrClosed{
		poolName: qp.name,
		reason:   reason,
	}
	close(qp.done)
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
	if qp.mu.quota != nil {
		r.Merge(qp.mu.quota)
	}
	qp.mu.quota = r
	// Notify the head of the queue if there is one waiting.
	if n := qp.mu.q.peek(); n != nil && n.c != nil {
		select {
		case n.c <- struct{}{}:
		default:
		}
	}
}

// chanSyncPool is used to pool allocations of the channels used to notify
// goroutines waiting in Acquire.
var chanSyncPool = sync.Pool{
	New: func() interface{} { return make(chan struct{}, 1) },
}

// Acquire attempts to fulfill the Request with Resource from the qp.
// Requests are serviced in a FIFO order; only a single request is ever
// being offered resources at a time. A Request will be offered the pool's
// current quantity of Resource until it is fulfilled or its context is
// canceled.
//
// Safe for concurrent use.
func (qp *QuotaPool) Acquire(ctx context.Context, r Request) (err error) {
	// Set up onAcquisition if we have one.
	start := timeutil.Now()
	if qp.config.onAcquisition != nil {
		defer func() {
			if err == nil {
				qp.config.onAcquisition(ctx, qp.name, r, start)
			}
		}()
	}
	// Attempt to acquire quota on the fast path.
	fulfilled, n, err := qp.acquireFastPath(ctx, r)
	if fulfilled || err != nil {
		return err
	}
	// Set up the infrastructure to report slow requests.
	var slowTimer *timeutil.Timer
	var slowTimerC <-chan time.Time
	if qp.onSlowAcquisition != nil {
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
			slowTimerC = nil
			defer qp.onSlowAcquisition(ctx, qp.name, r, start)()
			continue
		case <-n.c:
			if fulfilled := qp.tryAcquireOnNotify(ctx, r, n); fulfilled {
				return nil
			}
		case <-ctx.Done():
			qp.cleanupOnCancel(n)
			return ctx.Err()
		case <-qp.done:
			// We don't need to 'unregister' ourselves as in the case when the
			// context is canceled. In fact, we want others waiters to only
			// receive on qp.done and signaling them would work against that.
			return qp.closeErr // always non-nil when qp.done is closed
		}
	}
}

// acquireFastPath attempts to acquire quota if nobody is waiting and returns a
// notifyee if the request is not immediately fulfilled.
func (qp *QuotaPool) acquireFastPath(
	ctx context.Context, r Request,
) (fulfilled bool, _ *notifyee, _ error) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if qp.mu.closed {
		return false, nil, qp.closeErr
	}
	if qp.mu.q.len == 0 {
		if fulfilled, unused := r.Acquire(ctx, qp.mu.quota); fulfilled {
			qp.mu.quota = unused
			return true, nil, nil
		}
	}
	c := chanSyncPool.Get().(chan struct{})
	return false, qp.mu.q.enqueue(c), nil
}

func (qp *QuotaPool) tryAcquireOnNotify(
	ctx context.Context, r Request, n *notifyee,
) (fulfilled bool) {
	// Release the notify channel back into the sync pool if we're fulfilled.
	// Capture nc's value because it's not safe to avoid a race accessing n after
	// it has been released back to the notifyQueue.
	defer func(nc chan struct{}) {
		if fulfilled {
			chanSyncPool.Put(nc)
		}
	}(n.c)

	qp.mu.Lock()
	defer qp.mu.Unlock()
	// Make sure nobody already notified us again between the last receive and grabbing
	// the mutex.
	if len(n.c) > 0 {
		<-n.c
	}
	var unused Resource
	if fulfilled, unused = r.Acquire(ctx, qp.mu.quota); fulfilled {
		n.c = nil
		qp.mu.quota = unused
		qp.notifyNextLocked()
	}
	return fulfilled
}

func (qp *QuotaPool) cleanupOnCancel(n *notifyee) {
	// No matter what, we're going to want to put our notify channel back in to
	// the sync pool. Note that this defer call evaluates n.c here and is not
	// affected by later code that sets n.c to nil.
	defer chanSyncPool.Put(n.c)

	qp.mu.Lock()
	defer qp.mu.Unlock()

	// It we're not the head, prevent ourselves from being notified and move
	// along.
	if n != qp.mu.q.peek() {
		n.c = nil
		qp.mu.numCanceled++
		return
	}

	// If we're the head, make sure nobody already notified us before we notify the
	// next waiting notifyee.
	if len(n.c) > 0 {
		<-n.c
	}
	qp.notifyNextLocked()
}

// notifyNextLocked notifies the waiting acquisition goroutine next in line (if
// any). It requires that qp.mu.Mutex is held.
func (qp *QuotaPool) notifyNextLocked() {
	// Pop ourselves off the front of the queue.
	qp.mu.q.dequeue()
	// We traverse until we find a goroutine waiting to be notified, notify the
	// goroutine and truncate our queue to ensure the said goroutine is at the
	// head of the queue. Normally the next lined up waiter is the one waiting for
	// notification, but if others behind us have also gotten their context
	// canceled, they will leave behind notifyees with nil channels that we skip
	// below.
	//
	// If we determine there are no goroutines waiting, we simply truncate the
	// queue to reflect this.
	for n := qp.mu.q.peek(); n != nil; n = qp.mu.q.peek() {
		if n.c == nil {
			qp.mu.numCanceled--
			qp.mu.q.dequeue()
			continue
		}
		n.c <- struct{}{}
		break
	}
}
