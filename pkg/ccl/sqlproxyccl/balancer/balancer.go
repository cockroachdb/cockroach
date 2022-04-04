// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"container/list"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ErrNoAvailablePods is an error that indicates that no pods are available
// for selection.
var ErrNoAvailablePods = errors.New("no available pods")

// Balancer handles load balancing of SQL connections within the proxy.
// All methods on the Balancer instance are thread-safe.
type Balancer struct {
	// mu synchronizes access to fields in the struct.
	mu struct {
		syncutil.Mutex

		// rng corresponds to the random number generator instance which will
		// be used for load balancing.
		rng *rand.Rand
	}
}

// NewBalancer constructs a new Balancer instance that is responsible for
// load balancing SQL connections within the proxy.
//
// TODO(jaylim-crl): Update Balancer to take in a ConnTracker object.
func NewBalancer() *Balancer {
	b := &Balancer{}
	b.mu.rng, _ = randutil.NewPseudoRand()
	return b
}

// SelectTenantPod selects a tenant pod from the given list based on a weighted
// CPU load algorithm. It is expected that all pods within the list belongs to
// the same tenant. If no pods are available, this returns ErrNoAvailablePods.
func (b *Balancer) SelectTenantPod(pods []*tenant.Pod) (*tenant.Pod, error) {
	pod := selectTenantPod(b.randFloat32(), pods)
	if pod == nil {
		return nil, ErrNoAvailablePods
	}
	return pod, nil
}

// randFloat32 generates a random float32 within the bounds [0, 1) and is
// thread-safe.
func (b *Balancer) randFloat32() float32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.rng.Float32()
}

// rebalanceRequest corresponds to a rebalance request. For now, this only
// indicates where the connection should be transferred to through dst.
type rebalanceRequest struct {
	createdAt time.Time
	conn      ConnectionHandle
	dst       string
}

// rebalancerQueue represents the balancer's internal queue which is used for
// rebalancing requests.
type rebalancerQueue struct {
	mu       syncutil.Mutex
	cond     sync.Cond
	queue    *list.List
	elements map[ConnectionHandle]*list.Element
	closed   bool
}

// newRebalancerQueue returns a new instance of rebalancerQueue.
func newRebalancerQueue() *rebalancerQueue {
	q := &rebalancerQueue{
		queue:    list.New(),
		elements: make(map[ConnectionHandle]*list.Element),
	}
	q.cond.L = &q.mu
	return q
}

// isClosed returns true if the balancer queue is closed, or false otherwise.
func (q *rebalancerQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// close closes the balancer queue, and wakes up all goroutines blocked on
// dequeue.
func (q *rebalancerQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// enqueue puts the rebalance request into the queue. If a request for the
// connection already exists, the newer of the two will be used. If the queue
// has already been closed, this is a no-op.
//
// NOTE: req cannot be nil as that is used as a sentinel return value for
// dequeue to denote that the queue has been closed.
func (q *rebalancerQueue) enqueue(req *rebalanceRequest) {
	// req cannot be nil. See note above.
	if req == nil {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	e, ok := q.elements[req.conn]
	if ok {
		// Use the newer request of the two.
		if e.Value.(*rebalanceRequest).createdAt.Before(req.createdAt) {
			e.Value = req
		}
	} else {
		e = q.queue.PushBack(req)
		q.elements[req.conn] = e
	}
	q.cond.Broadcast()
}

// dequeue removes a request at the front of the queue, and returns that. If the
// queue has no items, dequeue will block until the queue is non-empty. If the
// queue has already been closed, this returns nil.
func (q *rebalancerQueue) dequeue() *rebalanceRequest {
	q.mu.Lock()
	defer q.mu.Unlock()

	var e *list.Element
	for {
		e = q.queue.Front()
		if e != nil {
			break
		}
		if q.closed {
			return nil
		}
		q.cond.Wait()
	}
	req := q.queue.Remove(e).(*rebalanceRequest)
	delete(q.elements, req.conn)
	return req
}
