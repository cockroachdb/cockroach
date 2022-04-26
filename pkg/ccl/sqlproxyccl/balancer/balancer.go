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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// rebalancePercentDeviation defines the percentage threshold that the
	// current number of assignments can deviate away from the mean. Having a
	// 15% "deadzone" reduces frequent transfers especially when load is
	// fluctuating.
	//
	// For example, if the percent deviation is 0.15, and mean is 10, the
	// number of assignments for every pod has to be between [8, 12] to be
	// considered balanced.
	//
	// NOTE: This must be between 0 and 1 inclusive.
	rebalancePercentDeviation = 0.15

	// rebalanceRate defines the rate of rebalancing assignments across SQL
	// pods. This rate applies to both RUNNING and DRAINING pods. For example,
	// consider the case where the rate is 0.50; if we have decided that we need
	// to move 15 assignments away from a particular pod, only 7 pods will be
	// moved at a time.
	//
	// NOTE: This must be between 0 and 1 inclusive. 0 means no rebalancing
	// will occur.
	rebalanceRate = 0.50

	// defaultMaxConcurrentRebalances represents the maximum number of
	// concurrent rebalance requests that are being processed. This effectively
	// limits the number of concurrent transfers per proxy.
	defaultMaxConcurrentRebalances = 100

	// maxTransferAttempts represents the maximum number of transfer attempts
	// per rebalance request when the previous attempts failed (possibly due to
	// an unsafe transfer point). Note that each transfer attempt currently has
	// a timeout of 15 seconds, so retrying up to 3 times may hold onto
	// processSem up to 45 seconds for each rebalance request.
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
	// stopper is used to start async tasks (e.g. transfer requests) within the
	// balancer.
	stopper *stop.Stopper

	// metrics contains various counters reflecting the balancer operations.
	metrics *Metrics

	// directoryCache corresponds to the tenant directory cache, which will be
	// used to lookup IP addresses of SQL pods for tenants.
	directoryCache tenant.DirectoryCache

	// connTracker is used to track connections within the proxy.
	//
	// TODO(jaylim-crl): Rename connTracker to tracker.
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
		directoryCache: directoryCache,
		queue:          q,
		processSem:     semaphore.New(options.maxConcurrentRebalances),
		timeSource:     options.timeSource,
	}
	b.connTracker, err = NewConnTracker(ctx, b.stopper, b.timeSource)
	if err != nil {
		return nil, err
	}

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
//
// TODO(jaylim-crl): Rename this to SelectSQLServer(requester, tenantID, clusterName)
// which returns a ServerAssignment.
func (b *Balancer) SelectTenantPod(pods []*tenant.Pod) (*tenant.Pod, error) {
	// The second case should not happen if the directory is returning the
	// right data. Check it regardless or else roachpb.MakeTenantID will panic
	// on a zero TenantID.
	if len(pods) == 0 || pods[0].TenantID == 0 {
		return nil, ErrNoAvailablePods
	}
	tenantID := roachpb.MakeTenantID(pods[0].TenantID)
	tenantEntry := b.connTracker.getEntry(tenantID, true /* allowCreate */)
	pod := selectTenantPod(pods, tenantEntry)
	if pod == nil {
		return nil, ErrNoAvailablePods
	}
	return pod, nil
}

// GetTracker returns the tracker associated with the balancer.
//
// TODO(jaylim-crl): Remove GetTracker entirely once SelectTenantPod returns
// a ServerAssignment instead of a pod.
func (b *Balancer) GetTracker() *ConnTracker {
	return b.connTracker
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
				err := req.conn.TransferConnection()
				if err == nil || errors.Is(err, context.Canceled) {
					break
				}

				// Retry again if the connection hasn't been closed.
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
	// getTenantIDs ensures that tenants will have at least one connection.
	tenantIDs := b.connTracker.getTenantIDs()

	for _, tenantID := range tenantIDs {
		tenantPods, err := b.directoryCache.TryLookupTenantPods(ctx, tenantID)
		if err != nil {
			// This case shouldn't really occur unless there's a bug in the
			// directory server (e.g. deleted pod events, but the pod is still
			// alive).
			log.Errorf(ctx, "could not lookup pods for tenant %s: %v", tenantID, err.Error())
			continue
		}

		// Construct a map so we could easily retrieve the pod by address.
		podMap := make(map[string]*tenant.Pod)
		var hasRunningPod bool
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

		activeList, idleList := b.connTracker.listAssignments(tenantID)
		b.rebalancePartition(podMap, activeList)
		b.rebalancePartition(podMap, idleList)
	}
}

// rebalancePartition rebalances the given assignments partition.
func (b *Balancer) rebalancePartition(
	pods map[string]*tenant.Pod, assignments []*ServerAssignment,
) {
	// Nothing to do here.
	if len(pods) == 0 || len(assignments) == 0 {
		return
	}

	// Transfer assignments away if the partition is in an imbalanced state.
	toMove := collectRunningPodAssignments(pods, assignments, rebalancePercentDeviation)
	b.enqueueRebalanceRequests(toMove)

	// Move all assignments away from DRAINING pods if and only if the pods have
	// been draining for at least minDrainPeriod.
	toMove = collectDrainingPodAssignments(pods, assignments, b.timeSource)
	b.enqueueRebalanceRequests(toMove)
}

// enqueueRebalanceRequests enqueues the first N server assignments for a
// transfer operation based on the defined rebalance rate. For example, if
// there are 10 server assignments in the input list, and rebalance rate is 0.4,
// only the first four server assignments will be enqueued for a transfer.
func (b *Balancer) enqueueRebalanceRequests(list []*ServerAssignment) {
	toMoveCount := int(math.Ceil(float64(len(list)) * float64(rebalanceRate)))
	for i := 0; i < toMoveCount; i++ {
		b.queue.enqueue(&rebalanceRequest{
			createdAt: b.timeSource.Now(),
			conn:      list[i].Owner(),
		})
	}
}

// collectRunningPodAssignments returns a set of ServerAssignments that have to
// be moved because the partition is in an imbalanced state. Only assignments to
// RUNNING pods will be accounted for.
//
// NOTE: pods should not be nil, and percentDeviation must be between [0, 1].
func collectRunningPodAssignments(
	pods map[string]*tenant.Pod, partition []*ServerAssignment, percentDeviation float64,
) []*ServerAssignment {
	// Construct a distribution map of server assignments.
	numAssignments := 0
	distribution := make(map[string][]*ServerAssignment)
	for _, a := range partition {
		pod, ok := pods[a.Addr()]
		if !ok || pod.State != tenant.RUNNING {
			// We have a connection to the pod, but the pod is not in the
			// directory cache. This race case happens if the connection was
			// transferred by a different goroutine to this new pod right after
			// we fetch the list of pods from the directory cache. Ignore here,
			// and this connection will be handled on the next rebalance loop.
			continue
		}
		distribution[a.Addr()] = append(distribution[a.Addr()], a)
		numAssignments++
	}

	// Ensure that all RUNNING pods have an entry in distribution. Doing that
	// allows us to account for new or underutilized pods.
	for _, pod := range pods {
		if pod.State != tenant.RUNNING {
			continue
		}
		if _, ok := distribution[pod.Addr]; !ok {
			distribution[pod.Addr] = []*ServerAssignment{}
		}
	}

	// No pods or assignments to work with.
	if len(distribution) == 0 || numAssignments == 0 {
		return nil
	}

	// Calculate average number of assignments, and lower/upper bounds based
	// on the rebalance percent deviation. We want to ensure that the number
	// of assignments on each pod is within [lowerBound, upperBound]. If all
	// of the pods are within that interval, the partition is considered to be
	// balanced.
	//
	// Note that lowerBound cannot be 0, or else the addition of a new pod with
	// no connections may still result in a balanced state.
	avgAssignments := float64(numAssignments) / float64(len(distribution))
	lowerBound := int(math.Max(1, math.Floor(avgAssignments*(1-percentDeviation))))
	upperBound := int(math.Ceil(avgAssignments * (1 + percentDeviation)))

	// Construct a set of assignments that we want to move, and the algorithm to
	// do so would be as follows:
	//     1. Compute the number of assignments that we need to move. This would
	//        be X = MAX(n, m), where:
	//          n = total number of assignments that exceed the upper bound
	//          m = total number of assignments that fall short of lower bound
	//
	//     2. First pass on distribution: collect assignments that exceed the
	//        upper bound. Update distribution and X to reflect the remaining
	//        assignments accordingly.
	//
	//     3. Second pass on distribution: greedily collect as many assignments
	//        up to X without violating the average. We could theoretically
	//        minimize the deviation from the mean by collecting from pods
	//        starting with the ones with the largest number of assignments,
	//        but this would require a sort.
	//
	// The implementation below is an optimization of the algorithm described
	// above, where steps 1 and 2 are combined. We will also start simple by
	// omitting the sort in (3).

	// Steps 1 and 2.
	missingCount := 0
	var toMove []*ServerAssignment
	for addr, d := range distribution {
		missingCount += int(math.Max(float64(lowerBound-len(d)), 0.0))

		// Move everything that exceed the upper bound.
		excess := len(d) - upperBound
		if excess > 0 {
			toMove = append(toMove, d[:excess]...)
			distribution[addr] = d[excess:]
			missingCount -= excess
		}
	}

	// Step 3.
	for addr, d := range distribution {
		if missingCount <= 0 {
			break
		}
		extra := len(d) - int(avgAssignments)
		if extra <= 0 || len(d) <= 1 {
			// Check length in second condition here to ensure that we don't
			// remove connections resulting in 0 assignments to that pod.
			continue
		}
		excess := int(math.Min(float64(extra), float64(missingCount)))
		missingCount -= excess
		toMove = append(toMove, d[:excess]...)
		distribution[addr] = d[excess:]
	}

	return toMove
}

// collectDrainingPodAssignments returns a set of ServerAssignments that have to
// be moved because the pods that they are in have been draining for at least
// minDrainPeriod.
//
// NOTE: pods and timeSource should not be nil.
func collectDrainingPodAssignments(
	pods map[string]*tenant.Pod, partition []*ServerAssignment, timeSource timeutil.TimeSource,
) []*ServerAssignment {
	var collected []*ServerAssignment
	for _, a := range partition {
		pod, ok := pods[a.Addr()]
		if !ok || pod.State != tenant.DRAINING {
			// We have a connection to the pod, but the pod is not in the
			// directory cache. This race case happens if the connection was
			// transferred by a different goroutine to this new pod right after
			// we fetch the list of pods from the directory cache. Ignore here,
			// and this connection will be handled on the next rebalance loop.
			continue
		}

		// Only move connections for pods which have been draining for at least
		// 1 minute. When load is fluctuating, the pod may transition back and
		// forth between the DRAINING and RUNNING states. This check prevents us
		// from moving connections around when that happens.
		drainingFor := timeSource.Now().Sub(pod.StateTimestamp)
		if drainingFor < minDrainPeriod {
			continue
		}
		collected = append(collected, a)
	}
	return collected
}

// rebalanceRequest corresponds to a rebalance request.
type rebalanceRequest struct {
	createdAt time.Time
	conn      ConnectionHandle
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

	// Test environments may create rebalanceRequests with nil owners.
	if req.conn == nil {
		return
	}

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
