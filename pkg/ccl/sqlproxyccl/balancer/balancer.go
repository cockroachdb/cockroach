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
	"math"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
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

// Option defines an option that can be passed to NewBalancer in order to
// control its behavior.
type Option func(opts *balancerOptions)

// MaxConcurrentRebalances defines the maximum number of concurrent rebalance
// operations for the balancer. This defaults to defaultMaxConcurrentRebalances.
func MaxConcurrentRebalances(max int) Option {
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

	// queue represents the rebalancer queue. All transfer requests should be
	// enqueued to this queue instead of calling the transfer API directly.
	queue *rebalancerQueue

	// processSem is used to limit the number of concurrent rebalance requests
	// that are being processed.
	processSem semaphore.Semaphore
}

// NewBalancer constructs a new Balancer instance that is responsible for
// load balancing SQL connections within the proxy.
//
// TODO(jaylim-crl): Update Balancer to take in a ConnTracker object.
func NewBalancer(ctx context.Context, stopper *stop.Stopper, opts ...Option) (*Balancer, error) {
	// Handle options.
	options := &balancerOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.maxConcurrentRebalances == 0 {
		options.maxConcurrentRebalances = defaultMaxConcurrentRebalances
	}

	// Ensure that ctx gets cancelled on stopper's quiescing.
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	q, err := newRebalancerQueue(ctx)
	if err != nil {
		return nil, err
	}

	b := &Balancer{
		stopper:    stopper,
		queue:      q,
		processSem: semaphore.New(options.maxConcurrentRebalances),
	}
	b.mu.rng, _ = randutil.NewPseudoRand()

	if err := b.stopper.RunAsyncTask(ctx, "processQueue", b.processQueue); err != nil {
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
	// processOneReq processors a request from the balancer queue. If the queue
	// is empty, this blocks. This returns true if processing should continue,
	// or false otherwise.
	processOneReq := func() (canContinue bool) {
		if err := b.processSem.Acquire(ctx, 1); err != nil {
			log.Errorf(ctx, "could not acquire processSem: %v", err.Error())
			return false
		}

		req, err := b.queue.dequeue(ctx)
		if err != nil {
			// Context is cancelled.
			log.Errorf(ctx, "could not dequeue from rebalancer queue: %v", err.Error())
			return false
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
			defer b.processSem.Release(1)

			// Each request is retried up to maxTransferAttempts.
			for i := 0; i < maxTransferAttempts && ctx.Err() == nil; i++ {
				// TODO(jaylim-crl): Once the TransferConnection API accepts a
				// destination, we could update this code, and pass along dst.
				err := req.conn.TransferConnection( /* req.dst */ )
				if err == nil || errors.Is(err, context.Canceled) ||
					req.dst == req.conn.ServerRemoteAddr() {
					break
				}

				// Retry again if the connection hasn't been closed or
				// transferred to the destination.
				time.Sleep(250 * time.Millisecond)
			}
		}); err != nil {
			// We should not hit this case, but if we did, log and abandon the
			// transfer.
			log.Errorf(ctx, "could not run async task for processQueue-item: %v", err.Error())
		}
		return true
	}
	for ctx.Err() == nil && processOneReq() {
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
type rebalancerQueue struct {
	mu       syncutil.Mutex
	sem      semaphore.Semaphore
	queue    *list.List
	elements map[ConnectionHandle]*list.Element
}

// newRebalancerQueue returns a new instance of rebalancerQueue.
func newRebalancerQueue(ctx context.Context) (*rebalancerQueue, error) {
	q := &rebalancerQueue{
		sem:      semaphore.New(math.MaxInt32),
		queue:    list.New(),
		elements: make(map[ConnectionHandle]*list.Element),
	}
	// sem represents the number of items in the queue, so we'll acquire
	// everything to denote an empty queue.
	if err := q.sem.Acquire(ctx, math.MaxInt32); err != nil {
		return nil, err
	}
	return q, nil
}

// enqueue puts the rebalance request into the queue. If a request for the
// connection already exists, the newer of the two will be used. This returns
// nil if the operation succeeded.
//
// NOTE: req should not be nil.
func (q *rebalancerQueue) enqueue(req *rebalanceRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()

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
	q.sem.Release(1)
}

// dequeue removes a request at the front of the queue, and returns that. If the
// queue has no items, dequeue will block until the queue is non-empty.
//
// NOTE: It is unsafe to continue using the queue if dequeue returns an error.
func (q *rebalancerQueue) dequeue(ctx context.Context) (*rebalanceRequest, error) {
	// Block until there is an item in the queue. There is a possibility where
	// Acquire returns an error AND obtains the semaphore. It is unsafe to
	// continue using the queue when that happens.
	//
	// It is deliberate to block on acquiring the semaphore before obtaining
	// the mu lock. We need that lock to enqueue items.
	if err := q.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	e := q.queue.Front()
	if e == nil {
		// The queue cannot be empty here.
		return nil, errors.AssertionFailedf("unexpected empty queue")
	}

	req := q.queue.Remove(e).(*rebalanceRequest)
	delete(q.elements, req.conn)
	return req, nil
}
