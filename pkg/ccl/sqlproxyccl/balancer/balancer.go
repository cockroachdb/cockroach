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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// ErrNoAvailablePods is an error that indicates that no pods are available
// for selection.
var ErrNoAvailablePods = errors.New("no available pods")

const (
	// rebalanceInterval is the period which we attempt to perform a rebalance
	// operation for connections across all tenants within the proxy.
	rebalanceInterval = 30 * time.Second

	// minDrainPeriod is the amount of time that a SQL pod needs to be in the
	// DRAINING state before the proxy starts moving connections away from it.
	minDrainPeriod = 1 * time.Minute

	// defaultMaxConcurrentRebalances represents the maximum number of
	// concurrent rebalance requests that are being processed. This effectively
	// limits the number of concurrent transfers per proxy.
	defaultMaxConcurrentRebalances = 100

	// maxTransferAttempts represents the maximum number of transfer attempts
	// per rebalance request when the previous attempts failed (possibly due to
	// an unsafe transfer point). Note that each transfer attempt currently has
	// a timeout of 15 seconds, so retrying up to 3 times may hold onto
	// processSem up to 45 seconds for each rebalance request.
	//
	// TODO(jaylim-crl): Reduce transfer timeout to 5 seconds.
	maxTransferAttempts = 3
)

// balancerOptions controls the behavior of the balancer component.
type balancerOptions struct {
	maxConcurrentRebalances int
	noRebalanceLoop         bool
	timeSource              timeutil.TimeSource
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

// NoRebalanceLoop disables the rebalance loop within the balancer. Note that
// this only disables automated rebalancing of connections. Events that invoke
// rebalance directly will still work.
func NoRebalanceLoop() Option {
	return func(opts *balancerOptions) {
		opts.noRebalanceLoop = true
	}
}

// TimeSource defines the time source's behavior for the balancer. This defaults
// to timeutil.DefaultTimeSource.
func TimeSource(ts timeutil.TimeSource) Option {
	return func(opts *balancerOptions) {
		opts.timeSource = ts
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

	// metrics contains various counters reflecting the balancer operations.
	metrics *Metrics

	// directoryCache corresponds to the tenant directory cache, which will be
	// used to lookup IP addresses of SQL pods for tenants.
	directoryCache tenant.DirectoryCache

	// connTracker is used to track connections within the proxy.
	connTracker *ConnTracker

	// queue represents the rebalancer queue. All transfer requests should be
	// enqueued to this queue instead of calling the transfer API directly.
	queue *rebalancerQueue

	// processSem is used to limit the number of concurrent rebalance requests
	// that are being processed.
	processSem semaphore.Semaphore

	// timeSource is the source of the time. By default, this will be set to
	// timeutil.DefaultTimeSource. Override with the TimeSource() option when
	// calling NewBalancer.
	timeSource timeutil.TimeSource
}

// NewBalancer constructs a new Balancer instance that is responsible for
// load balancing SQL connections within the proxy.
func NewBalancer(
	ctx context.Context,
	stopper *stop.Stopper,
	metrics *Metrics,
	directoryCache tenant.DirectoryCache,
	connTracker *ConnTracker,
	opts ...Option,
) (*Balancer, error) {
	// Handle options.
	options := &balancerOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.maxConcurrentRebalances == 0 {
		options.maxConcurrentRebalances = defaultMaxConcurrentRebalances
	}
	if options.timeSource == nil {
		options.timeSource = timeutil.DefaultTimeSource{}
	}

	// Ensure that ctx gets cancelled on stopper's quiescing.
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	q, err := newRebalancerQueue(ctx, metrics)
	if err != nil {
		return nil, err
	}

	b := &Balancer{
		stopper:        stopper,
		metrics:        metrics,
		connTracker:    connTracker,
		directoryCache: directoryCache,
		queue:          q,
		processSem:     semaphore.New(options.maxConcurrentRebalances),
		timeSource:     options.timeSource,
	}
	b.mu.rng, _ = randutil.NewPseudoRand()

	// Run queue processor to handle rebalance requests.
	if err := b.stopper.RunAsyncTask(ctx, "processQueue", b.processQueue); err != nil {
		return nil, err
	}

	if !options.noRebalanceLoop {
		// Start rebalance loop to continuously rebalance connections.
		if err := b.stopper.RunAsyncTask(ctx, "rebalanceLoop", b.rebalanceLoop); err != nil {
			return nil, err
		}
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
		//   1. Rate limit the number of transfers per connection (e.g. once
		//      every 5 minutes). This ensures that the connection isn't
		//      ping-ponged between pods within a short interval. However, for
		//      draining ones, we may want to move right away (or after 60 secs),
		//      even if the connection was recently transferred to the draining
		//      pod.
		if err := b.stopper.RunAsyncTask(ctx, "processQueue-item", func(ctx context.Context) {
			defer b.processSem.Release(1)

			b.metrics.processRebalanceStart()
			defer b.metrics.processRebalanceFinish()

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

// rebalanceLoop runs on a background goroutine to continuously rebalance
// connections, once every rebalanceInterval.
func (b *Balancer) rebalanceLoop(ctx context.Context) {
	timer := b.timeSource.NewTimer()
	defer timer.Stop()
	for {
		timer.Reset(rebalanceInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.Ch():
			timer.MarkRead()
			b.rebalance(ctx)
		}
	}
}

// rebalance attempts to rebalance connections for all tenants within the proxy.
//
// TODO(jaylim-crl): Update this to support rebalancing a single tenant. That
// way, the pod watcher could call this to rebalance a single tenant. We may
// also want to rate limit the number of rebalances per tenant for requests
// coming from the pod watcher.
func (b *Balancer) rebalance(ctx context.Context) {
	// GetTenantIDs ensures that tenants will have at least one connection.
	tenantIDs := b.connTracker.GetTenantIDs()

	for _, tenantID := range tenantIDs {
		tenantPods, err := b.directoryCache.TryLookupTenantPods(ctx, tenantID)
		if err != nil {
			// This case shouldn't really occur unless there's a bug in the
			// directory server (e.g. deleted pod events, but the pod is still
			// alive).
			log.Errorf(ctx, "could not lookup pods for tenant %s: %v", tenantID, err.Error())
			continue
		}

		// Build a podMap so we could easily retrieve the pod by address.
		podMap := make(map[string]*tenant.Pod)
		hasRunningPod := false
		for _, pod := range tenantPods {
			podMap[pod.Addr] = pod

			if pod.State == tenant.RUNNING {
				hasRunningPod = true
			}
		}

		// Only attempt to rebalance if we have a RUNNING pod. In theory, this
		// case would happen if we're scaling down from 1 to 0, which in that
		// case, we can't transfer connections anywhere. Practically, we will
		// never scale a tenant from 1 to 0 if there are still active
		// connections, so this case should not occur.
		if !hasRunningPod {
			continue
		}

		connMap := b.connTracker.GetConnsMap(tenantID)
		for addr, podConns := range connMap {
			pod, ok := podMap[addr]
			if !ok {
				// We have a connection to the pod, but the pod is not in the
				// directory cache. This race case happens if the connection
				// was transferred by a different goroutine to this new pod
				// right after we fetch the list of pods from the directory
				// cache above. Ignore here, and this connection will be handled
				// on the next rebalance loop.
				continue
			}

			// Transfer all connections in DRAINING pods.
			//
			// TODO(jaylim-crl): Consider extracting this logic for the DRAINING
			// case into a separate function once we add the rebalancing logic.
			if pod.State != tenant.DRAINING {
				continue
			}

			// Only move connections for pods which have been draining for
			// at least 1 minute. When load is fluctuating, the pod may
			// transition back and forth between the DRAINING and RUNNING
			// states. This check prevents us from moving connections around
			// when that happens.
			drainingFor := b.timeSource.Now().Sub(pod.StateTimestamp)
			if drainingFor < minDrainPeriod {
				continue
			}

			for _, c := range podConns {
				// TODO(jaylim-crl): We currently transfer without a dest, which
				// will result in using the weighted CPU algorithm to assign
				// pods. Once we start using a leastconns algorithm, we can make
				// better decisions here, and specify a destination.
				b.queue.enqueue(&rebalanceRequest{
					createdAt: b.timeSource.Now(),
					conn:      c,
				})
			}
		}
	}
}

// rebalanceRequest corresponds to a rebalance request. For now, this only
// indicates where the connection should be transferred to through dst.
//
// TODO(jaylim-crl): Consider adding src, and evaluating that before invoking
// the transfer operation to handle the case where a transfer was in progress
// when a new request was added to the queue.
type rebalanceRequest struct {
	createdAt time.Time
	conn      ConnectionHandle
	dst       string
}

// balancerQueue represents the balancer's internal queue which is used for
// rebalancing requests. All methods on the queue are thread-safe.
type rebalancerQueue struct {
	mu       syncutil.Mutex
	metrics  *Metrics
	sem      semaphore.Semaphore
	queue    *list.List
	elements map[ConnectionHandle]*list.Element
}

// newRebalancerQueue returns a new instance of rebalancerQueue.
func newRebalancerQueue(ctx context.Context, metrics *Metrics) (*rebalancerQueue, error) {
	q := &rebalancerQueue{
		metrics:  metrics,
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
// connection already exists, the newer of the two will be used.
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
		return
	}

	e = q.queue.PushBack(req)
	q.elements[req.conn] = e
	q.metrics.rebalanceReqQueued.Inc(1)
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
	q.metrics.rebalanceReqQueued.Dec(1)
	return req, nil
}
