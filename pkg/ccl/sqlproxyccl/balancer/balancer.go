// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import (
	"container/list"
	"context"
	"math"
	"math/rand"
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

	// defaultRebalanceDelay is the minimum amount of time that must elapse
	// between rebalance operations. This was deliberately chosen to be half of
	// rebalanceInterval, and is mainly used to rate limit effects due to events
	// from the pod watcher.
	defaultRebalanceDelay = 15 * time.Second

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

	// defaultRebalanceRate defines the rate of rebalancing assignments across
	// SQL pods. This rate applies to both RUNNING and DRAINING pods. For
	// example, consider the case where the rate is 0.50; if we have decided
	// that we need to move 15 assignments away from a particular pod, only 7
	// pods will be moved at a time.
	//
	// NOTE: This must be between 0 and 1 inclusive. 0 means no rebalancing
	// will occur.
	defaultRebalanceRate = 0.50

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
	rebalanceRate           float32
	rebalanceDelay          time.Duration
	disableRebalancing      bool
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

// RebalanceRate defines the rate of rebalancing across pods. This must be
// between 0 and 1 inclusive. 0 means no rebalancing will occur.
func RebalanceRate(rate float32) Option {
	return func(opts *balancerOptions) {
		opts.rebalanceRate = rate
	}
}

// RebalanceDelay specifies the minimum amount of time that must elapse between
// attempts to rebalance a given tenant. This delay has the effect of throttling
// RebalanceTenant calls to avoid constantly moving connections around.
//
// RebalanceDelay defaults to defaultRebalanceDelay. Use -1 to never throttle.
func RebalanceDelay(delay time.Duration) Option {
	return func(opts *balancerOptions) {
		opts.rebalanceDelay = delay
	}
}

// DisableRebalancing disables all rebalancing operations within the balancer.
// Unlike NoRebalanceLoop that only disables automated rebalancing, this also
// causes RebalanceTenant to no-op if called by the pod watcher. Using this
// option implicitly disables the rebalance loop as well.
func DisableRebalancing() Option {
	return func(opts *balancerOptions) {
		opts.disableRebalancing = true
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

	// rebalanceRate represents the rate of rebalancing connections.
	rebalanceRate float32

	// rebalanceDelay is the minimum amount of time that must elapse between
	// attempts to rebalance a given tenant. Defaults to defaultRebalanceDelay.
	rebalanceDelay time.Duration

	// disableRebalancing is used to indicate that all rebalancing options will
	// be disabled.
	disableRebalancing bool

	// lastRebalance is the last time the tenants are rebalanced. This is used
	// to rate limit the number of rebalances per tenant. Synchronization is
	// needed since rebalance operations can be triggered by the rebalance loop,
	// or the pod watcher.
	lastRebalance struct {
		syncutil.Mutex
		tenants map[roachpb.TenantID]time.Time
	}
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
	options := &balancerOptions{
		maxConcurrentRebalances: defaultMaxConcurrentRebalances,
		timeSource:              timeutil.DefaultTimeSource{},
		rebalanceRate:           defaultRebalanceRate,
		rebalanceDelay:          defaultRebalanceDelay,
	}
	for _, opt := range opts {
		opt(options)
	}
	// Since we want to disable all rebalancing operations, no point having the
	// rebalance loop running.
	if options.disableRebalancing {
		options.noRebalanceLoop = true
	}

	// Ensure that ctx gets cancelled on stopper's quiescing.
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	q, err := newRebalancerQueue(ctx, metrics)
	if err != nil {
		return nil, err
	}

	b := &Balancer{
		stopper:            stopper,
		metrics:            metrics,
		directoryCache:     directoryCache,
		queue:              q,
		processSem:         semaphore.New(options.maxConcurrentRebalances),
		timeSource:         options.timeSource,
		rebalanceRate:      options.rebalanceRate,
		rebalanceDelay:     options.rebalanceDelay,
		disableRebalancing: options.disableRebalancing,
	}
	b.lastRebalance.tenants = make(map[roachpb.TenantID]time.Time)

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

// RebalanceTenant rebalances connections to the given tenant. If no RUNNING
// pod exists for the given tenant, or the tenant has been recently rebalanced,
// this is a no-op.
func (b *Balancer) RebalanceTenant(ctx context.Context, tenantID roachpb.TenantID) {
	// If rebalancing is disabled, or tenant was rebalanced recently, then
	// RebalanceTenant is a no-op.
	if b.disableRebalancing || !b.canRebalanceTenant(tenantID) {
		return
	}

	tenantPods, err := b.directoryCache.TryLookupTenantPods(ctx, tenantID)
	if err != nil {
		log.Errorf(ctx, "could not rebalance tenant %s: %v", tenantID, err.Error())
		return
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
		return
	}

	activeList, idleList := b.connTracker.listAssignments(tenantID)
	b.rebalancePartition(podMap, activeList)
	b.rebalancePartition(podMap, idleList)
}

// SelectTenantPod selects a tenant pod from the given list based on a weighted
// CPU load algorithm. It is expected that all pods within the list belongs to
// the same tenant. If no pods are available, this returns ErrNoAvailablePods.
//
// TODO(jaylim-crl): Rename this to SelectSQLServer(requester, tenantID, clusterName)
// which returns a ServerAssignment.
func (b *Balancer) SelectTenantPod(pods []*tenant.Pod) (*tenant.Pod, error) {
	// The second case should not happen if the directory is returning the
	// right data. Check it regardless or else roachpb.MustMakeTenantID will panic
	// on a zero TenantID.
	if len(pods) == 0 || pods[0].TenantID == 0 {
		return nil, ErrNoAvailablePods
	}
	tenantID := roachpb.MustMakeTenantID(pods[0].TenantID)
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

// canRebalanceTenant returns true if it has been at least `rebalanceDelay`
// since the last time the given tenant was rebalanced, or false otherwise.
func (b *Balancer) canRebalanceTenant(tenantID roachpb.TenantID) bool {
	b.lastRebalance.Lock()
	defer b.lastRebalance.Unlock()

	now := b.timeSource.Now()
	if now.Sub(b.lastRebalance.tenants[tenantID]) < b.rebalanceDelay {
		return false
	}
	b.lastRebalance.tenants[tenantID] = now
	return true
}

// rebalance attempts to rebalance connections for all tenants within the proxy.
func (b *Balancer) rebalance(ctx context.Context) {
	// getTenantIDs ensures that tenants will have at least one connection.
	tenantIDs := b.connTracker.getTenantIDs()
	for _, tenantID := range tenantIDs {
		b.RebalanceTenant(ctx, tenantID)
	}
}

// rebalancePartition rebalances the given assignments partition.
func (b *Balancer) rebalancePartition(
	pods map[string]*tenant.Pod, assignments []*ServerAssignment,
) {
	// Nothing to do here if there are no assignments, or only one pod.
	if len(pods) <= 1 || len(assignments) == 0 {
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

// enqueueRebalanceRequests enqueues N random server assignments for a transfer
// operation based on the defined rebalance rate. For example, if there are 10
// server assignments in the input list, and rebalance rate is 0.4, four server
// assignments will be selected at random, and enqueued for a transfer.
//
// NOTE: Elements in the list may be shuffled around once this method returns.
func (b *Balancer) enqueueRebalanceRequests(list []*ServerAssignment) {
	toMoveCount := int(math.Ceil(float64(len(list)) * float64(b.rebalanceRate)))
	partition, _ := partitionNRandom(list, toMoveCount)
	for _, a := range partition {
		b.queue.enqueue(&rebalanceRequest{
			createdAt: b.timeSource.Now(),
			conn:      a.Owner(),
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
	// Construct a map indexed by addresses of pods.
	podAssignments := make(map[string][]*ServerAssignment)

	// Ensure that all RUNNING pods have an entry in podAssignments. Doing that
	// allows us to account for new or underutilized pods.
	for _, pod := range pods {
		if pod.State == tenant.RUNNING {
			podAssignments[pod.Addr] = nil
		}
	}
	numAssignments := 0
	for _, a := range partition {
		// If the assignment's address was not found in podAssignments, this
		// means that we have a connection to the pod, but the pod is not in the
		// directory cache. This race case happens if the connection was
		// transferred by a different goroutine to this new pod right after we
		// fetch the list of pods from the directory cache. Ignore here, and
		// this connection will be handled on the next rebalance loop.
		if _, ok := podAssignments[a.Addr()]; ok {
			numAssignments++
			podAssignments[a.Addr()] = append(podAssignments[a.Addr()], a)
		}
	}

	// No pods or assignments to work with.
	if len(podAssignments) == 0 || numAssignments == 0 {
		return nil
	}

	// Calculate average number of assignments, and lower/upper bounds based
	// on the rebalance percent deviation. We want to ensure that the number
	// of assignments on each pod is within [lowerBound, upperBound]. If all
	// of the pods are within that interval, the partition is considered to be
	// balanced.
	//
	// Note that bounds cannot be 0, or else the addition of a new pod with no
	// connections may still result in a balanced state.
	avgAssignments := float64(numAssignments) / float64(len(podAssignments))
	lowerBound := int(math.Max(1, math.Floor(avgAssignments*(1-percentDeviation))))
	upperBound := int(math.Max(1, math.Ceil(avgAssignments*(1+percentDeviation))))

	// Normalize average to fit between [lowerBound, upperBound]. This implies
	// that average must be at least 1, and we don't end up moving assignments
	// away resulting in 0 assignments to a pod.
	avgAssignments = float64(lowerBound+upperBound) / 2.0

	// Construct a set of assignments that we want to move, and the algorithm to
	// do so would be as follows:
	//
	//   1. Compute the number of assignments that we need to move. This would
	//      be X = MAX(n, m), where:
	//        n = total number of assignments that exceed the upper bound
	//        m = total number of assignments that fall short of lower bound
	//
	//   2. First pass on podAssignments: collect assignments that exceed the
	//      upper bound. Update podAssignments and X to reflect the remaining
	//      assignments accordingly.
	//
	//   3. Second pass on podAssignments: greedily collect as many assignments
	//      up to X without violating the average.
	//
	// The implementation below is an optimization of the algorithm described
	// above, where steps 1 and 2 are combined. We also randomize the collection
	// process to ensure that there are no biases.

	// Steps 1 and 2.
	missingCount := 0
	var mustMove, eligibleToMove []*ServerAssignment
	var random []*ServerAssignment
	for addr, d := range podAssignments {
		missingCount += int(math.Max(float64(lowerBound-len(d)), 0.0))

		// Move everything that exceed the upper bound.
		excess := len(d) - upperBound
		if excess > 0 {
			random, d = partitionNRandom(d, excess)
			mustMove = append(mustMove, random...)
			missingCount -= excess
		}

		// The remaining ones that exceed the average are eligible for a move.
		// Within each pod, choose `excess` assignments randomly.
		excess = len(d) - int(avgAssignments)
		if excess > 0 {
			random, d = partitionNRandom(d, excess)
			eligibleToMove = append(eligibleToMove, random...)
		}

		podAssignments[addr] = d
	}

	// Step 3.
	// Across all pods, choose `missingCount` assignments randomly.
	if missingCount > 0 {
		random, _ = partitionNRandom(eligibleToMove, missingCount)
		mustMove = append(mustMove, random...)
	}

	return mustMove
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

// partitionNRandom partitions the input slice into two, with the first being
// n random elements, and the second being the remaining elements.
// - If n <= 0, (nil, nil) will be returned.
// - If n <= len(src), (src, nil) will be returned.
//
// NOTE: Elements in src may be shuffled around when this function returns, so
// this does not allocate extra memory. It is guaranteed that elements that are
// chosen will be the last n items of src.
var partitionNRandom = func(
	src []*ServerAssignment, n int,
) (chosen []*ServerAssignment, rest []*ServerAssignment) {
	if n <= 0 {
		return nil, nil
	}
	if n >= len(src) {
		return src, nil
	}
	restLen := len(src)
	for i := 0; i < n; i++ {
		idx := rand.Intn(restLen)
		src[idx], src[restLen-1] = src[restLen-1], src[idx]
		restLen--
	}
	return src[restLen:], src[:restLen]
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
	q.metrics.RebalanceReqQueued.Inc(1)
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
	q.metrics.RebalanceReqQueued.Dec(1)
	return req, nil
}
