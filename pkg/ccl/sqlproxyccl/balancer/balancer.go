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
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ErrNoAvailablePods is an error that indicates that no pods are available
// for selection.
var ErrNoAvailablePods = errors.New("no available pods")

// defaultMaxConcurrentRebalances represents the maximum number of concurrent
// rebalance requests that are being processed. This effectively limits the
// number of concurrent transfers per proxy.
const defaultMaxConcurrentRebalances = 100

// maxTransferAttempts represents the maximum number of transfer attempts per
// rebalance requests when the previous attempts failed (possibly due to an
// unsafe transfer point). Note that each transfer attempt currently has a
// timeout of 15 seconds, so retrying up to 3 times may hold onto processSem
// up to 45 seconds for each rebalance request.
//
// TODO(jaylim-crl): Reduce transfer timeout to 5 seconds.
const maxTransferAttempts = 3

// balancerOptions controls the behavior of the balancer component.
type balancerOptions struct {
	maxConcurrentRebalances int
}

// BalancerOption defines an option that can be passed to NewBalancer in order
// to control its behavior.
type BalancerOption func(opts *balancerOptions)

// MaxConcurrentRebalances defines the maximum number of concurrent rebalance
// operations for the balancer. This defaults to defaultMaxConcurrentRebalances.
func MaxConcurrentRebalances(max int) func(opts *balancerOptions) {
	return func(opts *balancerOptions) {
		opts.maxConcurrentRebalances = max
	}
}

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

	// stopper is used to start async tasks (e.g. transfer requests) within the
	// balancer.
	stopper *stop.Stopper

	// queue represents the balancer queue. All transfer requests should be
	// enqueued to this queue instead of calling the transfer API directly.
	queue *balancerQueue

	// processSem is used to limit the number of concurrent rebalance requests
	// that are being processed.
	processSem chan struct{}

	// testingKnobs are knobs used for testing.
	testingKnobs struct {
		beforeProcessQueueItem func()
		afterProcessQueueItem  func()
	}
}

// NewBalancer constructs a new Balancer instance that is responsible for
// load balancing SQL connections within the proxy.
//
// TODO(jaylim-crl): Update Balancer to take in a ConnTracker object.
func NewBalancer(
	ctx context.Context, stopper *stop.Stopper, opts ...BalancerOption,
) (*Balancer, error) {
	options := &balancerOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.maxConcurrentRebalances == 0 {
		options.maxConcurrentRebalances = defaultMaxConcurrentRebalances
	}

	b := &Balancer{
		stopper:    stopper,
		queue:      newBalancerQueue(),
		processSem: make(chan struct{}, options.maxConcurrentRebalances),
	}
	b.mu.rng, _ = randutil.NewPseudoRand()

	if err := b.stopper.RunAsyncTask(ctx, "processQueue", b.processQueue); err != nil {
		return nil, err
	}

	if err := b.stopper.RunAsyncTask(ctx, "processQueue-closer", func(ctx context.Context) {
		<-b.stopper.ShouldQuiesce()
		b.queue.close()
	}); err != nil {
		return nil, err
	}

	return b, nil
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

// processQueue runs on a background goroutine, and invokes TransferConnection
// for each rebalance request.
func (b *Balancer) processQueue(ctx context.Context) {
	// acquire blocks until ctx has been cancelled, or it has acquired a
	// process semaphore. This returns true if the acquire was successful, and
	// false otherwise.
	acquire := func() bool {
		select {
		case b.processSem <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	// release releases the process semaphore.
	release := func() {
		<-b.processSem
	}
	// processOneReq processors a request from the balancer queue. If the queue
	// is empty, this blocks.
	processOneReq := func() {
		if !acquire() {
			return
		}

		req := b.queue.dequeue()

		// Queue has been closed. Releasing the semaphore here isn't necessary,
		// but we'll do that for consistency.
		if req == nil {
			release()
			return
		}

		// TODO(jaylim-crl): implement enhancements:
		//   1. Add metrics to track the number of active transfers.
		//   2. Rate limit the number of transfers per connection (e.g. once
		//      every 5 minutes). This ensures that the connection isn't
		//      ping-ponged between pods within a short interval. However, for
		//      draining ones, we may want to move right away (or after 60 secs),
		//      even if the connection was recently transferred to the draining
		//      pod.
		if err := b.stopper.RunAsyncTask(ctx, "processQueue-item", func(ctx context.Context) {
			defer release()

			if b.testingKnobs.beforeProcessQueueItem != nil {
				b.testingKnobs.beforeProcessQueueItem()
			}

			// Each request is retried up to maxTransferAttempts.
			for i := 0; i < maxTransferAttempts && ctx.Err() == nil; i++ {
				// TODO(jaylim-crl): Once the TransferConnection API accepts a
				// destination, we could update this code, and pass along dst.
				err := req.conn.TransferConnection( /* req.dst */ )
				if err == nil ||
					err == context.Canceled ||
					req.dst == req.conn.ServerRemoteAddr() {
					break
				}

				// Retry again if the connection hasn't been closed or
				// transferred to the destination.
				time.Sleep(250 * time.Millisecond)
			}

			if b.testingKnobs.afterProcessQueueItem != nil {
				b.testingKnobs.afterProcessQueueItem()
			}
		}); err != nil {
			// We should not hit this case, but if we did, log and abandon the
			// transfer.
			log.Errorf(ctx, "could not run async task for processQueue-item: %v", err.Error())
		}
	}
	for ctx.Err() == nil && !b.queue.isClosed() {
		processOneReq()
	}
}

// rebalanceRequest corresponds to a rebalance request. For now, this only
// indicates where the connection should be transferred to through dst.
type rebalanceRequest struct {
	createdAt time.Time
	conn      ConnectionHandle
	dst       string
}

// balancerQueue represents the balancer's internal queue which is used for
// rebalancing requests. All methods on the queue are thread-safe.
type balancerQueue struct {
	mu       syncutil.Mutex
	cond     sync.Cond
	queue    *list.List
	elements map[ConnectionHandle]*list.Element
	closed   bool
}

// newBalancerQueue returns a new instance of balancerQueue.
func newBalancerQueue() *balancerQueue {
	q := &balancerQueue{
		queue:    list.New(),
		elements: make(map[ConnectionHandle]*list.Element),
	}
	q.cond.L = &q.mu
	return q
}

// isClosed returns true if the balancer queue is closed, or false otherwise.
func (q *balancerQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// close closes the balancer queue, and wakes up all goroutines blocked on
// dequeue.
func (q *balancerQueue) close() {
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
func (q *balancerQueue) enqueue(req *rebalanceRequest) {
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
func (q *balancerQueue) dequeue() *rebalanceRequest {
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
