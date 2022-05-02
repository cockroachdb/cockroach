// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package balancer

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBalancer_SelectTenantPod(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	b, err := NewBalancer(
		ctx,
		stopper,
		nil, /* metrics */
		nil, /* directoryCache */
		NoRebalanceLoop(),
	)
	require.NoError(t, err)

	t.Run("no pods", func(t *testing.T) {
		pod, err := b.SelectTenantPod([]*tenant.Pod{})
		require.EqualError(t, err, ErrNoAvailablePods.Error())
		require.Nil(t, pod)
	})

	t.Run("few pods", func(t *testing.T) {
		pod, err := b.SelectTenantPod([]*tenant.Pod{
			{TenantID: 10, Addr: "1"},
			{TenantID: 10, Addr: "2"}},
		)
		require.NoError(t, err)
		require.Contains(t, []string{"1", "2"}, pod.Addr)
	})
}

func TestRebalancer_processQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	b, err := NewBalancer(
		ctx,
		stopper,
		NewMetrics(),
		nil, /* directoryCache */
		MaxConcurrentRebalances(1),
		NoRebalanceLoop(),
	)
	require.NoError(t, err)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	// syncReq is used to wait until the test has completed processing the
	// items that are of concern.
	syncCh := make(chan struct{})
	syncReq := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn: &testConnHandle{
			onTransferConnection: func() error {
				syncCh <- struct{}{}
				return nil
			},
		},
	}

	// assertNoRunningRequests asserts that the rebalance loop isn't processing
	// any request.
	assertNoRunningRequests := func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			runningReq := b.metrics.rebalanceReqRunning.Value()
			if runningReq != 0 {
				return errors.Newf("expected no running requests, but got %d", runningReq)
			}
			return nil
		})
	}

	t.Run("retries_up_to_maxTransferAttempts", func(t *testing.T) {
		count := 0
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn: &testConnHandle{
				onTransferConnection: func() error {
					count++
					require.Equal(t, int64(1), b.metrics.rebalanceReqRunning.Value())
					return errors.New("cannot transfer")
				},
			},
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		b.queue.enqueue(syncReq)
		<-syncCh

		assertNoRunningRequests(t)

		// Ensure that we only retried up to 3 times.
		require.Equal(t, 3, count)
	})

	t.Run("conn_was_transferred", func(t *testing.T) {
		count := 0
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn: &testConnHandle{
				onTransferConnection: func() error {
					count++
					require.Equal(t, int64(1), b.metrics.rebalanceReqRunning.Value())
					return nil
				},
			},
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		b.queue.enqueue(syncReq)
		<-syncCh

		assertNoRunningRequests(t)

		// We should only retry once.
		require.Equal(t, 1, count)
	})

	t.Run("conn_was_closed", func(t *testing.T) {
		count := 0
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn: &testConnHandle{
				onTransferConnection: func() error {
					count++
					require.Equal(t, int64(1), b.metrics.rebalanceReqRunning.Value())
					return context.Canceled
				},
			},
		}
		b.queue.enqueue(req)

		// Wait until the item has been processed.
		b.queue.enqueue(syncReq)
		<-syncCh

		assertNoRunningRequests(t)

		// We should only retry once.
		require.Equal(t, 1, count)
	})

	t.Run("limit_concurrent_rebalances", func(t *testing.T) {
		// Temporarily override metrics so we could test total counts
		// independent of other tests.
		defer func(oldMetrics *Metrics) { b.metrics = oldMetrics }(b.metrics)
		b.metrics = NewMetrics()

		const reqCount = 100

		// Allow up to 2 concurrent rebalances.
		b.processSem.SetLimit(2)

		// wg is used to wait until all transfers have completed.
		var wg sync.WaitGroup
		wg.Add(reqCount)

		// waitCh is used to wait until all items have fully been enqueued.
		waitCh := make(chan struct{})

		var count int32
		for i := 0; i < reqCount; i++ {
			req := &rebalanceRequest{
				createdAt: timeSource.Now(),
				conn: &testConnHandle{
					onTransferConnection: func() error {
						// Block until all requests are enqueued.
						<-waitCh

						defer func() {
							newCount := atomic.AddInt32(&count, -1)
							require.True(t, newCount >= 0)
							wg.Done()
						}()

						// Count should not exceed the maximum number of
						// concurrent rebalances defined.
						newCount := atomic.AddInt32(&count, 1)
						require.True(t, newCount <= 2)
						require.True(t, b.metrics.rebalanceReqRunning.Value() <= 2)
						return nil
					},
				},
			}
			b.queue.enqueue(req)
		}

		// Close the channel to unblock.
		close(waitCh)

		// Wait until all transfers have completed.
		wg.Wait()

		// We should only transfer once for every connection.
		require.Equal(t, int32(0), count)
		require.Equal(t, int64(reqCount), b.metrics.rebalanceReqTotal.Count())
	})
}

// This test only tests that the rebalance method was invoked during the
// rebalance interval, and does a basic high-level assertion. Tests for the
// actual logic was extracted into its own test below because testing them with
// the manual timer is difficult to get it right (and often flaky).
func TestRebalancer_rebalanceLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	metrics := NewMetrics()
	directoryCache := newTestDirectoryCache()

	b, err := NewBalancer(
		ctx,
		stopper,
		metrics,
		directoryCache,
		MaxConcurrentRebalances(1),
		TimeSource(timeSource),
	)
	require.NoError(t, err)

	pods := []*tenant.Pod{
		{TenantID: 30, Addr: "127.0.0.30:80", State: tenant.DRAINING},
		{TenantID: 30, Addr: "127.0.0.30:81", State: tenant.RUNNING},
	}
	for _, pod := range pods {
		require.True(t, directoryCache.upsertPod(pod))
	}

	// Manually assign a pod to the tracker in the balancer.
	h := &testConnHandle{
		onTransferConnection: func() error {
			return nil
		},
	}
	b.connTracker.registerAssignment(roachpb.MakeTenantID(30), &ServerAssignment{
		addr:  pods[0].Addr,
		owner: h,
	})

	// Wait until rebalance queue gets processed.
	runs := 0
	testutils.SucceedsSoon(t, func() error {
		runs++

		timeSource.Advance(rebalanceInterval)

		count := h.transferConnectionCount()
		if count >= 3 && runs >= count {
			return nil
		}
		return errors.Newf("insufficient runs, expected >= 3, but got %d", count)
	})
}

func TestRebalancer_rebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	metrics := NewMetrics()
	directoryCache := newTestDirectoryCache()

	b, err := NewBalancer(
		ctx,
		stopper,
		metrics,
		directoryCache,
		NoRebalanceLoop(),
		TimeSource(timeSource),
	)
	require.NoError(t, err)

	// Set up the following tenants:
	// - tenant-10: no pods
	// - tenant-20: one draining pod (no running pods)
	// - tenant-30: two draining pods (one with < 1m), one running pod
	// - tenant-40: one draining pod, one running pod
	// - tenant-50: one running pod
	recentlyDrainedPod := &tenant.Pod{
		TenantID: 30,
		Addr:     "127.0.0.30:81",
		State:    tenant.DRAINING,
	}
	pods := []*tenant.Pod{
		{TenantID: 20, Addr: "127.0.0.20:80", State: tenant.DRAINING},
		{TenantID: 30, Addr: "127.0.0.30:80", State: tenant.DRAINING},
		recentlyDrainedPod, // tenant-30, 127.0.0.30:81
		{TenantID: 30, Addr: "127.0.0.30:82", State: tenant.RUNNING},
		{TenantID: 40, Addr: "127.0.0.40:80", State: tenant.DRAINING},
		{TenantID: 40, Addr: "127.0.0.40:81", State: tenant.RUNNING},
		{TenantID: 50, Addr: "127.0.0.50:80", State: tenant.RUNNING},
	}

	// reset recreates the directory cache.
	reset := func(t *testing.T) {
		t.Helper()

		directoryCache = newTestDirectoryCache()
		b.directoryCache = directoryCache

		// Set it such that when the rebalance occurs, the pod goes into the
		// DRAINING state.
		recentlyDrainedPod.StateTimestamp = timeSource.Now().Add(rebalanceInterval)
		for _, pod := range pods {
			require.True(t, directoryCache.upsertPod(pod))
		}
	}

	// makeHandle returns a handle that doesn't panic when TransferConnection
	// is called.
	makeHandle := func() *testConnHandle {
		return &testConnHandle{
			onTransferConnection: func() error {
				return nil
			},
		}
	}

	for _, tc := range []struct {
		name           string
		handlesFn      func(t *testing.T) []ConnectionHandle
		expectedCounts []int
	}{
		{
			// This case should not occur unless there's a bug in the directory
			// server, where the connection is still alive, but the pod has
			// been removed from the cache.
			name: "no pods",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				tenant10 := roachpb.MakeTenantID(10)

				// Use a random IP since tenant-10 doesn't have a pod, and it
				// does not matter.
				handle := makeHandle()
				sa := NewServerAssignment(tenant10, b.connTracker, handle, "foobarip")
				handle.onClose = sa.Close
				return []ConnectionHandle{handle}
			},
			expectedCounts: []int{0},
		},
		{
			// Only draining pods for tenant-20. We shouldn't transfer because
			// there's nothing to transfer to.
			name: "no running pods",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				tenant20 := roachpb.MakeTenantID(20)

				handle := makeHandle()
				sa := NewServerAssignment(tenant20, b.connTracker, handle, pods[0].Addr)
				handle.onClose = sa.Close
				return []ConnectionHandle{handle}
			},
			expectedCounts: []int{0},
		},
		{
			// If the connection has been closed, we shouldn't bother initiating
			// a transfer. Use tenant-30's DRAINING pod here.
			name: "connection closed",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				tenant30 := roachpb.MakeTenantID(30)
				cancelledCtx, cancel := context.WithCancel(context.Background())
				cancel()

				handle := makeHandle()
				handle.ctx = cancelledCtx
				sa := NewServerAssignment(tenant30, b.connTracker, handle, pods[1].Addr)
				handle.onClose = sa.Close
				return []ConnectionHandle{handle}
			},
			expectedCounts: []int{0},
		},
		{
			// Use tenant-30's recently drained pod. We shouldn't transfer
			// because minDrainPeriod hasn't elapsed.
			name: "recently drained pod",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				tenant30 := roachpb.MakeTenantID(30)

				handle := makeHandle()
				sa := NewServerAssignment(tenant30, b.connTracker, handle, recentlyDrainedPod.Addr)
				handle.onClose = sa.Close
				return []ConnectionHandle{handle}
			},
			expectedCounts: []int{0},
		},
		{
			name: "multiple connections",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				conns := []*tenant.Pod{
					// Connection on tenant with single draining pod. Should
					// not transfer because nothing to transfer to.
					pods[0],
					// Connections to draining pod (>= 1m).
					pods[1],
					pods[1],
					// Connections to recently drained pod.
					recentlyDrainedPod,
					recentlyDrainedPod,
					// Connection to running pod. Nothing happens.
					pods[3],
					// Connections to draining pod (>= 1m).
					pods[4],
					// Connections to running pods. Nothing happens.
					pods[5],
					pods[6],
				}
				var handles []ConnectionHandle
				for _, c := range conns {
					handle := makeHandle()
					sa := NewServerAssignment(
						roachpb.MakeTenantID(c.TenantID),
						b.connTracker,
						handle,
						c.Addr,
					)
					handle.onClose = sa.Close
					handles = append(handles, handle)
				}
				return handles
			},
			expectedCounts: []int{0, 1, 1, 0, 0, 0, 1, 0, 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			handles := tc.handlesFn(t)

			// Attempt the rebalance.
			b.rebalance(ctx)

			// Wait until rebalance queue gets processed.
			testutils.SucceedsSoon(t, func() error {
				var counts []int
				for _, h := range handles {
					counts = append(counts, h.(*testConnHandle).transferConnectionCount())
				}
				if !reflect.DeepEqual(tc.expectedCounts, counts) {
					return errors.Newf("require %v, but got %v", tc.expectedCounts, counts)
				}
				return nil
			})

			// Clean up the handles.
			for _, h := range handles {
				h.Close()
			}
		})
	}
}

func TestRebalancerQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q, err := newRebalancerQueue(ctx, NewMetrics())
	require.NoError(t, err)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	// Create rebalance requests for the same connection handle.
	conn1 := &testConnHandle{}
	req1 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn1,
	}
	timeSource.Advance(5 * time.Second)
	req2 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn1,
	}
	timeSource.Advance(5 * time.Second)
	req3 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn1,
	}

	// Enqueue in a specific order. req3 overrides req1; req2 is a no-op.
	q.enqueue(req1)
	require.Equal(t, int64(1), q.metrics.rebalanceReqQueued.Value())
	q.enqueue(req3)
	require.Equal(t, int64(1), q.metrics.rebalanceReqQueued.Value())
	q.enqueue(req2)
	require.Len(t, q.elements, 1)
	require.Equal(t, 1, q.queue.Len())
	require.Equal(t, int64(1), q.metrics.rebalanceReqQueued.Value())

	// Create another request.
	conn2 := &testConnHandle{}
	req4 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn2,
	}
	q.enqueue(req4)
	require.Equal(t, int64(2), q.metrics.rebalanceReqQueued.Value())
	require.Len(t, q.elements, 2)
	require.Equal(t, 2, q.queue.Len())

	// Dequeue the items.
	item, err := q.dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, req3, item)
	require.Equal(t, int64(1), q.metrics.rebalanceReqQueued.Value())
	item, err = q.dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, req4, item)
	require.Equal(t, int64(0), q.metrics.rebalanceReqQueued.Value())
	require.Empty(t, q.elements)
	require.Equal(t, 0, q.queue.Len())

	// Cancel the context. Dequeue should return immediately with an error.
	cancel()
	req4, err = q.dequeue(ctx)
	require.EqualError(t, err, context.Canceled.Error())
	require.Nil(t, req4)
	require.Equal(t, int64(0), q.metrics.rebalanceReqQueued.Value())
}

// TestRebalancerQueueBlocking tests the blocking behavior when invoking
// dequeue.
func TestRebalancerQueueBlocking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q, err := newRebalancerQueue(ctx, NewMetrics())
	require.NoError(t, err)

	reqCh := make(chan *rebalanceRequest, 10)
	go func() {
		for {
			req, err := q.dequeue(ctx)
			if err != nil {
				break
			}
			reqCh <- req
		}
	}()

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	const reqCount = 100
	for i := 0; i < reqCount; i++ {
		req := &rebalanceRequest{
			createdAt: timeSource.Now(),
			conn:      &testConnHandle{id: i},
		}
		q.enqueue(req)
		timeSource.Advance(1 * time.Second)
	}

	for i := 0; i < reqCount; i++ {
		req := <-reqCh
		require.Equal(t, i, (req.conn.(*testConnHandle)).id)
	}
}

// testDirectoryCache is a test implementation of the tenant directory cache.
// This only overrides TryLookupTenantPods. Other methods will panic.
type testDirectoryCache struct {
	tenant.DirectoryCache

	mu struct {
		syncutil.Mutex
		// When updating pods through the tests, we should copy on write to
		// prevent races with callers because the slice is returned directly.
		pods map[roachpb.TenantID][]*tenant.Pod
	}
}

var _ tenant.DirectoryCache = &testDirectoryCache{}

// newTestDirectoryCache returns a new instance of the test directory cache.
func newTestDirectoryCache() *testDirectoryCache {
	dc := &testDirectoryCache{}
	dc.mu.pods = make(map[roachpb.TenantID][]*tenant.Pod)
	return dc
}

// TryLookupTenantPods implements the tenant.DirectoryCache interface.
func (r *testDirectoryCache) TryLookupTenantPods(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]*tenant.Pod, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	p, ok := r.mu.pods[tenantID]
	if !ok {
		return nil, errors.New("no pods")
	}
	return p, nil
}

// upsertPod inserts the given pod into the tenant's list of pods. If it is
// already present, then upsertPod updates the list and returns false.
func (r *testDirectoryCache) upsertPod(pod *tenant.Pod) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	tenantID := roachpb.MakeTenantID(pod.TenantID)
	pods := r.mu.pods[tenantID]
	for i, existing := range pods {
		if existing.Addr == pod.Addr {
			r.mu.pods[tenantID] = make([]*tenant.Pod, len(pods))
			copy(r.mu.pods[tenantID], pods)
			r.mu.pods[tenantID][i] = pod
			return false
		}
	}
	r.mu.pods[tenantID] = append(r.mu.pods[tenantID], pod)
	return true
}
