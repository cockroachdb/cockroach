// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBalancer_SelectTenantPod(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

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
	testutilsccl.ServerlessOnly(t)

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
			runningReq := b.metrics.RebalanceReqRunning.Value()
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
					require.Equal(t, int64(1), b.metrics.RebalanceReqRunning.Value())
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
					require.Equal(t, int64(1), b.metrics.RebalanceReqRunning.Value())
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
					require.Equal(t, int64(1), b.metrics.RebalanceReqRunning.Value())
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
						require.True(t, b.metrics.RebalanceReqRunning.Value() <= 2)
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
		require.Equal(t, int64(reqCount), b.metrics.RebalanceReqTotal.Count())
	})
}

// This test only tests that the rebalance method was invoked during the
// rebalance interval, and does a basic high-level assertion. Tests for the
// actual logic was extracted into its own test below because testing them with
// the manual timer is difficult to get it right (and often flaky).
func TestRebalancer_rebalanceLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

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

	tenantID := roachpb.MustMakeTenantID(30)
	drainingPod := &tenant.Pod{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:80", State: tenant.DRAINING}
	require.True(t, directoryCache.upsertPod(drainingPod))
	runningPods := []*tenant.Pod{
		{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:81", State: tenant.RUNNING},
		{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:82", State: tenant.RUNNING},
	}
	for _, pod := range runningPods {
		require.True(t, directoryCache.upsertPod(pod))
	}

	// Create new server assignments.
	// - 1 to drainingPod
	// - 9 to runningPods[1]
	//
	// We expect the connection to drainingPod to be moved away because the pod
	// is draining. At the same time, 4 connections should be moved away from
	// runningPods[1] with mean interval of [4, 5].
	var mu syncutil.Mutex
	var assignments []*ServerAssignment
	var makeTestConnHandle func(idx int) *testConnHandle
	makeTestConnHandle = func(idx int) *testConnHandle {
		var handle *testConnHandle
		handle = &testConnHandle{
			onTransferConnection: func() error {
				mu.Lock()
				defer mu.Unlock()

				// Already moved earlier.
				if assignments[idx].Owner() != handle {
					return nil
				}
				assignments[idx].Close()

				pod := selectTenantPod(runningPods, b.connTracker.getEntry(tenantID, false))
				require.NotNil(t, pod)
				assignments[idx] = NewServerAssignment(
					tenantID, b.connTracker, makeTestConnHandle(idx), pod.Addr,
				)
				return nil
			},
		}
		return handle
	}
	assignments = append(
		assignments,
		NewServerAssignment(tenantID, b.connTracker, makeTestConnHandle(0), drainingPod.Addr),
	)
	for i := 1; i < 10; i++ {
		assignments = append(
			assignments,
			NewServerAssignment(tenantID, b.connTracker, makeTestConnHandle(i), runningPods[1].Addr),
		)
	}

	// Wait until the rebalance queue gets processed.
	testutils.SucceedsSoon(t, func() error {
		timeSource.Advance(rebalanceInterval)

		activeList, _ := b.connTracker.listAssignments(tenantID)
		distribution := make(map[string]int)
		total := 0
		for _, sa := range activeList {
			distribution[sa.Addr()]++
			total++
		}
		for _, val := range distribution {
			if val > 6 || val < 4 {
				return errors.Newf("expected count to be between [4, 6]")
			}
		}
		if total != 10 {
			return errors.Newf("should have 10 assignments, but got %d", total)
		}
		return nil
	})
}

func TestRebalancer_rebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

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
		RebalanceDelay(-1),
	)
	require.NoError(t, err)

	// Set up the following tenants:
	// - tenant-10: no pods
	// - tenant-20: one draining pod (no running pods)
	// - tenant-30: two draining pods (one with < 1m), one running pod
	// - tenant-40: one draining pod, one running pod
	// - tenant-50: one running pod
	// - tenant-60: three running pods
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
		{TenantID: 60, Addr: "127.0.0.60:80", State: tenant.RUNNING},
		{TenantID: 60, Addr: "127.0.0.60:81", State: tenant.RUNNING},
		{TenantID: 60, Addr: "127.0.0.60:82", State: tenant.RUNNING},
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

	for _, tc := range []struct {
		name                    string
		handlesFn               func(t *testing.T) []ConnectionHandle
		preRebalanceFn          func(t *testing.T)
		expectedCounts          []int
		expectedCountsMatcherFn func(handles []ConnectionHandle) error
	}{
		{
			// This case should not occur unless there's a bug in the directory
			// server, where the connection is still alive, but the pod has
			// been removed from the cache.
			name: "no pods",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				tenant10 := roachpb.MustMakeTenantID(10)

				// Use a random IP since tenant-10 doesn't have a pod, and it
				// does not matter.
				handle := makeTestHandle()
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
				tenant20 := roachpb.MustMakeTenantID(20)

				handle := makeTestHandle()
				sa := NewServerAssignment(tenant20, b.connTracker, handle, pods[0].Addr)
				handle.onClose = sa.Close
				return []ConnectionHandle{handle}
			},
			expectedCounts: []int{0},
		},
		{
			// Use tenant-30's recently drained pod. We shouldn't transfer
			// because minDrainPeriod hasn't elapsed.
			name: "draining/recently drained pod",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				tenant30 := roachpb.MustMakeTenantID(30)

				activeHandle := makeTestHandle()
				sa := NewServerAssignment(tenant30, b.connTracker, activeHandle, recentlyDrainedPod.Addr)
				activeHandle.onClose = sa.Close

				idleHandle := makeTestHandle()
				sa = NewServerAssignment(tenant30, b.connTracker, idleHandle, recentlyDrainedPod.Addr)
				idleHandle.onClose = sa.Close
				idleHandle.setIdle(true)

				// Refresh partitions, and validate idle connection.
				e30 := b.connTracker.getEntry(tenant30, false)
				e30.refreshPartitions()
				_, idleList30 := e30.listAssignments()
				require.Len(t, idleList30, 1)

				return []ConnectionHandle{activeHandle, idleHandle}
			},
			expectedCounts: []int{0, 0},
		},
		{
			name: "draining/multiple connections",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				conns := []*tenant.Pod{
					// Active connections
					// ------------------
					// Connection on tenant with single draining pod. Should
					// not transfer because nothing to transfer to.
					pods[0],
					// Connection to draining pod (>= 1m).
					pods[1],
					// Connections to recently drained pod.
					recentlyDrainedPod,
					recentlyDrainedPod,
					// Connection to running pod. Nothing happens.
					pods[3],
					// Connection to draining pod (>= 1m).
					pods[4],
					// Connections to running pods. Nothing happens.
					pods[5],
					pods[6],
					// Idle connections
					// ----------------
					// Connection to draining pod (>= 1m).
					pods[1],
					// Connection to draining pod (>= 1m).
					pods[4],
				}
				var handles []ConnectionHandle
				for _, c := range conns {
					handle := makeTestHandle()
					sa := NewServerAssignment(
						roachpb.MustMakeTenantID(c.TenantID),
						b.connTracker,
						handle,
						c.Addr,
					)
					handle.onClose = sa.Close
					handles = append(handles, handle)
				}
				// The last two are idle connections.
				handles[len(handles)-1].(*testConnHandle).setIdle(true)
				handles[len(handles)-2].(*testConnHandle).setIdle(true)

				// Refresh partitions, and validate idle connections.
				e30 := b.connTracker.getEntry(roachpb.MustMakeTenantID(30), false)
				e40 := b.connTracker.getEntry(roachpb.MustMakeTenantID(40), false)
				e30.refreshPartitions()
				e40.refreshPartitions()
				_, idleList30 := e30.listAssignments()
				require.Len(t, idleList30, 1)
				_, idleList40 := e40.listAssignments()
				require.Len(t, idleList40, 1)

				return handles
			},
			expectedCounts: []int{0, 1, 0, 0, 0, 1, 0, 0, 1, 1},
		},
		{
			name: "running/multiple connections",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				// Create 100 active connections: 53->pod[7], 0->pod[8], 47->pod[9].
				var conns []*tenant.Pod
				for i := 0; i < 53; i++ {
					conns = append(conns, pods[7])
				}
				for i := 0; i < 47; i++ {
					conns = append(conns, pods[9])
				}
				// Add another 30 idle connections: 20->pod[7], 1->pod[8], 9->pod[9].
				for i := 0; i < 20; i++ {
					conns = append(conns, pods[7])
				}
				conns = append(conns, pods[8])
				for i := 0; i < 9; i++ {
					conns = append(conns, pods[9])
				}
				var handles []ConnectionHandle
				for _, c := range conns {
					handle := makeTestHandle()
					sa := NewServerAssignment(
						roachpb.MustMakeTenantID(c.TenantID),
						b.connTracker,
						handle,
						c.Addr,
					)
					handle.onClose = sa.Close
					handles = append(handles, handle)
				}
				for i := 0; i < 30; i++ {
					handles[len(handles)-i-1].(*testConnHandle).setIdle(true)
				}

				// Refresh partitions, and validate idle connection.
				e60 := b.connTracker.getEntry(roachpb.MustMakeTenantID(60), false)
				e60.refreshPartitions()
				_, idleList60 := e60.listAssignments()
				require.Len(t, idleList60, 30)

				return handles
			},
			expectedCountsMatcherFn: func(handles []ConnectionHandle) error {
				// Active connections
				// ------------------
				// Average = 33.33, mean interval with 15% deadzone is [28, 39].
				// Expect 53-39=14 from pod[7] and 47-39=8 from pod[9]. Since
				// missingCount = 28 > (14+8), 28 connections should be moved.
				// Taking a 50% rebalancing rate into account, we have 14 in
				// total. Since we cannot guarantee ordering, so we will just
				// count here. The actual logic is already unit tested in
				// TestCollectRunningPodAssignments.
				count := 0
				for i := 0; i < 100; i++ {
					count += handles[i].(*testConnHandle).transferConnectionCount()
				}
				if count != 14 {
					return errors.Newf("require 14, but got %v", count)
				}
				// Idle connections
				// ----------------
				// Average = 10, mean interval with 15% deadzone is [8, 12].
				// Exceed 8 from pod[7] and short 7 from pod[8]. Taking the
				// greater of the two, we will transfer 8 connections in total.
				// Half that to account for rebalancing rate.
				count = 0
				for i := 0; i < 30; i++ {
					count += handles[i+100].(*testConnHandle).transferConnectionCount()
				}
				if count != 4 {
					return errors.Newf("require 4, but got %v", count)
				}
				return nil
			},
		},
		{
			name: "both active and idle connections",
			handlesFn: func(t *testing.T) []ConnectionHandle {
				conns := []*tenant.Pod{
					// Active connections
					// ------------------
					// Connection to draining pod (>= 1m).
					pods[1],
					// Connections to running pods. Move 2 away. With rebalance
					// rate of 50%, move 1.
					pods[7],
					pods[7],
					pods[7],
					// Idle connections
					// ----------------
					// Connection to draining pod (>= 1m).
					pods[4],
					// Connections to running pods. Move 1 away. Rebalance rate
					// does not apply.
					pods[8],
					pods[8],
				}
				var handles []ConnectionHandle
				for _, c := range conns {
					handle := makeTestHandle()
					sa := NewServerAssignment(
						roachpb.MustMakeTenantID(c.TenantID),
						b.connTracker,
						handle,
						c.Addr,
					)
					handle.onClose = sa.Close
					handles = append(handles, handle)
				}
				// The last three are idle connections.
				handles[len(handles)-1].(*testConnHandle).setIdle(true)
				handles[len(handles)-2].(*testConnHandle).setIdle(true)
				handles[len(handles)-3].(*testConnHandle).setIdle(true)

				// Refresh partitions, and validate idle connections.
				e40 := b.connTracker.getEntry(roachpb.MustMakeTenantID(40), false)
				e60 := b.connTracker.getEntry(roachpb.MustMakeTenantID(60), false)
				e40.refreshPartitions()
				e60.refreshPartitions()
				_, idleList40 := e40.listAssignments()
				require.Len(t, idleList40, 1)
				_, idleList60 := e60.listAssignments()
				require.Len(t, idleList60, 2)

				return handles
			},
			expectedCountsMatcherFn: func(handles []ConnectionHandle) error {
				// Active connections
				// ------------------
				count := 0
				for i := 0; i < 4; i++ {
					count += handles[i].(*testConnHandle).transferConnectionCount()
				}
				// 1 from draining, 1 from running.
				if count != 2 {
					return errors.Newf("require 2, but got %v", count)
				}
				// Idle connections
				// ----------------
				count = 0
				for i := 0; i < 3; i++ {
					count += handles[i+4].(*testConnHandle).transferConnectionCount()
				}
				// 1 from draining, 1 from running.
				if count != 2 {
					return errors.Newf("require 2, but got %v", count)
				}
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			handles := tc.handlesFn(t)

			// Attempt the rebalance.
			b.rebalance(ctx)

			// Wait until rebalance queue gets processed.
			testutils.SucceedsSoon(t, func() error {
				if tc.expectedCountsMatcherFn != nil {
					if err := tc.expectedCountsMatcherFn(handles); err != nil {
						return err
					}
				} else {
					var counts []int
					for _, h := range handles {
						counts = append(counts, h.(*testConnHandle).transferConnectionCount())
					}
					if !reflect.DeepEqual(tc.expectedCounts, counts) {
						return errors.Newf("require %v, but got %v", tc.expectedCounts, counts)
					}
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

func TestBalancer_RebalanceTenant_WithRebalancingDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

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
		DisableRebalancing(),
		TimeSource(timeSource),
	)
	require.NoError(t, err)

	tenantID := roachpb.MustMakeTenantID(10)
	pods := []*tenant.Pod{
		{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:80", State: tenant.DRAINING},
		{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:81", State: tenant.RUNNING},
	}
	for _, pod := range pods {
		require.True(t, directoryCache.upsertPod(pod))
	}

	// Create 100 active connections, all to the draining pod.
	const numConns = 100
	var handles []ConnectionHandle
	for i := 0; i < numConns; i++ {
		handle := makeTestHandle()
		handles = append(handles, handle)
		_ = NewServerAssignment(tenantID, b.connTracker, handle, pods[0].Addr)
	}

	assertZeroTransfers := func() {
		count := 0
		for i := 0; i < numConns; i++ {
			count += handles[i].(*testConnHandle).transferConnectionCount()
		}
		require.Equal(t, 0, count)
	}

	// Attempt the rebalance, and wait for a while. No rebalancing should occur.
	b.RebalanceTenant(ctx, tenantID)
	time.Sleep(1 * time.Second)

	// Queue should be empty, and no additional connections should be moved.
	b.queue.mu.Lock()
	queueLen := b.queue.queue.Len()
	b.queue.mu.Unlock()
	require.Equal(t, 0, queueLen)
	assertZeroTransfers()

	// Advance the timer by some rebalance interval. No rebalancing should
	// occur since the loop is disabled.
	timeSource.Advance(2 * rebalanceInterval)
	time.Sleep(1 * time.Second)

	// Queue should be empty, and no additional connections should be moved.
	b.queue.mu.Lock()
	queueLen = b.queue.queue.Len()
	b.queue.mu.Unlock()
	require.Equal(t, 0, queueLen)
	assertZeroTransfers()
}

func TestBalancer_RebalanceTenant_WithDefaultDelay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

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

	tenantID := roachpb.MustMakeTenantID(10)
	pods := []*tenant.Pod{
		{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:80", State: tenant.DRAINING},
		{TenantID: tenantID.ToUint64(), Addr: "127.0.0.30:81", State: tenant.RUNNING},
	}
	for _, pod := range pods {
		require.True(t, directoryCache.upsertPod(pod))
	}

	// Create 100 active connections, all to the draining pod.
	const numConns = 100
	var mu syncutil.Mutex
	assignments := make([]*ServerAssignment, numConns)
	makeTestConnHandle := func(idx int) *testConnHandle {
		var handle *testConnHandle
		handle = &testConnHandle{
			onTransferConnection: func() error {
				mu.Lock()
				defer mu.Unlock()
				assignments[idx].Close()
				assignments[idx] = NewServerAssignment(
					tenantID, b.connTracker, handle, pods[1].Addr,
				)
				return nil
			},
		}
		return handle
	}
	var handles []ConnectionHandle
	for i := 0; i < numConns; i++ {
		handle := makeTestConnHandle(i)
		handles = append(handles, handle)
		assignments[i] = NewServerAssignment(
			tenantID, b.connTracker, handle, pods[0].Addr,
		)
	}

	waitFor := func(numTransfers int) {
		testutils.SucceedsSoon(t, func() error {
			count := 0
			for i := 0; i < numConns; i++ {
				count += handles[i].(*testConnHandle).transferConnectionCount()
			}
			if count != numTransfers {
				return errors.Newf("require %d, but got %v", numTransfers, count)
			}
			return nil
		})
	}

	// Attempt the rebalance, and wait until 50 were moved
	// (i.e. 100 * defaultRebalanceRate).
	b.RebalanceTenant(ctx, tenantID)
	waitFor(50)

	// Run the rebalance again.
	b.RebalanceTenant(ctx, tenantID)

	// Queue should be empty, and no additional connections should be moved.
	b.queue.mu.Lock()
	queueLen := b.queue.queue.Len()
	b.queue.mu.Unlock()
	require.Equal(t, 0, queueLen)
	waitFor(50)

	// Advance time, rebalance, and wait until 75 (i.e. 50 + 25) connections
	// get moved.
	timeSource.Advance(defaultRebalanceDelay)
	b.RebalanceTenant(ctx, tenantID)
	waitFor(75)

	// Advance time, rebalance, and wait until 88 (i.e. 75 + 13) connections
	// get moved.
	timeSource.Advance(defaultRebalanceDelay)
	b.RebalanceTenant(ctx, tenantID)
	waitFor(88)
}

func TestEnqueueRebalanceRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	baseCtx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(baseCtx)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	b, err := NewBalancer(
		baseCtx,
		stopper,
		NewMetrics(),
		nil, /* directoryCache */
		NoRebalanceLoop(),
		TimeSource(timeSource),
	)
	require.NoError(t, err)

	var list []*ServerAssignment
	for i := 0; i < 15; i++ {
		list = append(list, &ServerAssignment{owner: makeTestHandle()})
	}
	b.enqueueRebalanceRequests(list)

	// Since rebalanceRate is 0.5, ceil(7.5) = 8 random assignments will be
	// transferred.
	testutils.SucceedsSoon(t, func() error {
		count := 0
		for _, a := range list {
			count += a.Owner().(*testConnHandle).transferConnectionCount()
		}
		if count != 8 {
			return errors.Newf("pending count 8, but got %d", count)
		}
		return nil
	})
}

func TestCollectRunningPodAssignments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	// Use a deterministic behavior in tests.
	defer func(
		oldFn func(src []*ServerAssignment, n int) ([]*ServerAssignment, []*ServerAssignment),
	) {
		partitionNRandom = oldFn
	}(partitionNRandom)
	partitionNRandom = func(src []*ServerAssignment, n int) ([]*ServerAssignment, []*ServerAssignment) {
		if n <= 0 {
			return nil, nil
		}
		if n >= len(src) {
			return src, nil
		}
		return src[:n], src[n:]
	}

	t.Run("no pods", func(t *testing.T) {
		require.Nil(t, collectRunningPodAssignments(
			map[string]*tenant.Pod{},
			[]*ServerAssignment{{addr: "1"}, {addr: "2"}},
			0,
		))
	})

	t.Run("no assignments", func(t *testing.T) {
		require.Nil(t, collectRunningPodAssignments(
			map[string]*tenant.Pod{"1": {State: tenant.RUNNING}},
			nil,
			0,
		))
	})

	for _, tc := range []struct {
		name                       string
		percentDeviation           float64
		pods                       map[string]*tenant.Pod
		partitionDistribution      []int
		expectedSetDistributionAny []map[string]int
	}{
		{
			name: "balanced partition",
			pods: map[string]*tenant.Pod{
				"1": {State: tenant.RUNNING},
				"2": {State: tenant.DRAINING},
				"3": {State: tenant.RUNNING},
			},
			// [1, 2] are bounds. Draining pod isn't included, even if it has
			// many assignments. Partition is already balanced.
			partitionDistribution:      []int{2, 4, 1},
			expectedSetDistributionAny: []map[string]int{{}},
		},
		{
			name: "multiple new pods",
			pods: map[string]*tenant.Pod{
				"1": {State: tenant.RUNNING},
				"2": {State: tenant.RUNNING},
				"3": {State: tenant.RUNNING},
			},
			// [1, 2] are bounds. New pods have no assignments (underutilized).
			partitionDistribution:      []int{3, 0, 0},
			expectedSetDistributionAny: []map[string]int{{"1": 2}},
		},
		{
			name: "single new pod",
			pods: map[string]*tenant.Pod{
				"1": {State: tenant.RUNNING},
				"2": {State: tenant.RUNNING},
				"3": {State: tenant.RUNNING},
				"4": {State: tenant.RUNNING},
				"5": {State: tenant.RUNNING},
				"6": {State: tenant.RUNNING},
			},
			// [1, 2] are bounds. New pod has no assignments (underutilized).
			partitionDistribution:      []int{1, 1, 2, 2, 2, 0},
			expectedSetDistributionAny: []map[string]int{{"3": 1}, {"4": 1}, {"5": 1}},
		},
		{
			name: "more overloaded pods", // Compared to underloaded ones.
			pods: map[string]*tenant.Pod{
				"1": {State: tenant.RUNNING},
				"2": {State: tenant.RUNNING},
				"3": {State: tenant.RUNNING},
				"4": {State: tenant.RUNNING},
				"5": {State: tenant.RUNNING},
				"6": {State: tenant.RUNNING},
			},
			// [1, 3] are bounds. Two overloaded pods.
			partitionDistribution:      []int{1, 4, 1, 3, 1, 4},
			expectedSetDistributionAny: []map[string]int{{"2": 1, "6": 1}},
		},
		{
			name: "more underloaded pods", // Compared to overloaded ones.
			pods: map[string]*tenant.Pod{
				"1": {State: tenant.RUNNING},
				"2": {State: tenant.RUNNING},
				"3": {State: tenant.RUNNING},
				"4": {State: tenant.RUNNING},
				"5": {State: tenant.RUNNING},
			},
			percentDeviation: 0.8,
			// [2, 27] are bounds. Exceed by 0, but short by 2+2=4. Greedily
			// pick the remaining 4 from pod 2.
			partitionDistribution:      []int{0, 25, 25, 25, 0},
			expectedSetDistributionAny: []map[string]int{{"2": 4}, {"3": 4}, {"4": 4}},
		},
		{
			name: "equally imbalanced",
			pods: map[string]*tenant.Pod{
				"1": {State: tenant.RUNNING},
				"2": {State: tenant.RUNNING},
				"3": {State: tenant.RUNNING},
				"4": {State: tenant.RUNNING},
				"5": {State: tenant.RUNNING},
			},
			// [6, 9] are bounds. Both exceed=short=18.
			partitionDistribution:      []int{0, 15, 0, 21, 0},
			expectedSetDistributionAny: []map[string]int{{"2": 6, "4": 12}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.percentDeviation == 0 {
				tc.percentDeviation = 0.15
			}

			// Ensure that every pod has an address.
			for addr, pod := range tc.pods {
				pod.Addr = addr
			}

			// Construct partition based on partition distribution.
			var partition []*ServerAssignment
			for i, count := range tc.partitionDistribution {
				for j := 0; j < count; j++ {
					partition = append(
						partition,
						&ServerAssignment{addr: fmt.Sprintf("%d", i+1)},
					)
				}
			}

			set := collectRunningPodAssignments(tc.pods, partition, tc.percentDeviation)
			setDistribution := make(map[string]int)
			for _, a := range set {
				setDistribution[a.Addr()]++
			}

			// Match one of the set distributions. There are multiple choices
			// here because map iteration is non deterministic.
			matched := false
			for _, expected := range tc.expectedSetDistributionAny {
				if reflect.DeepEqual(expected, setDistribution) {
					matched = true
					break
				}
			}
			require.True(t, matched, fmt.Sprintf("could not match expected set distribution; found=%v, expected=%v",
				setDistribution, tc.expectedSetDistributionAny))
		})
	}
}

func TestCollectDrainingPodAssignments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	t.Run("no pods", func(t *testing.T) {
		set := collectDrainingPodAssignments(
			map[string]*tenant.Pod{},
			[]*ServerAssignment{{addr: "1"}, {addr: "2"}},
			nil,
		)
		require.Nil(t, set)
	})

	t.Run("with pods", func(t *testing.T) {
		// Use a custom time source for testing.
		t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
		timeSource := timeutil.NewManualTime(t0)

		// Pod 3 just transitioned into the DRAINING state.
		pods := map[string]*tenant.Pod{
			"1": {State: tenant.RUNNING},
			"2": {State: tenant.DRAINING, StateTimestamp: timeSource.Now().Add(-minDrainPeriod)}, // 1m
			"3": {State: tenant.DRAINING, StateTimestamp: timeSource.Now()},                      // 0s
			"4": {State: tenant.RUNNING},
			"5": {State: tenant.DRAINING, StateTimestamp: timeSource.Now().Add(-minDrainPeriod).Add(-1 * time.Second)}, // 1m1s
			"6": {State: tenant.DRAINING, StateTimestamp: timeSource.Now().Add(-minDrainPeriod).Add(1 * time.Second)},  // 59s
		}

		// Create 3 assignments per pod, in addition to a non-existent pod 7.
		var partition []*ServerAssignment
		for i := 1; i <= 7; i++ {
			for count := 0; count < 3; count++ {
				partition = append(partition, &ServerAssignment{addr: fmt.Sprintf("%d", i)})
			}
		}

		// Empty partition.
		set := collectDrainingPodAssignments(pods, nil, timeSource)
		require.Nil(t, set)

		// Actual partition.
		set = collectDrainingPodAssignments(pods, partition, timeSource)
		distribution := make(map[string]int)
		for _, a := range set {
			distribution[a.Addr()]++
		}
		require.Equal(t, map[string]int{"2": 3, "5": 3}, distribution)
	})
}

func TestPartitionNRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	generateSrc := func(count int) []*ServerAssignment {
		var partition []*ServerAssignment
		for i := 0; i < count; i++ {
			partition = append(partition, &ServerAssignment{addr: fmt.Sprintf("%d", i)})
		}
		return partition
	}

	validateSrc := func(t *testing.T, src []*ServerAssignment, max int) {
		seen := make(map[int]bool)
		for _, a := range src {
			// Ensure that partition is still valid.
			idx, err := strconv.Atoi(a.Addr())
			require.NoError(t, err)
			require.True(t, idx >= 0 && idx <= max)

			// And there are no duplicates.
			require.False(t, seen[idx])
			seen[idx] = true
		}
	}

	t.Run("small n", func(t *testing.T) {
		random, rest := partitionNRandom(generateSrc(42), -1)
		require.Empty(t, random)
		require.Empty(t, rest)
		random, rest = partitionNRandom(generateSrc(42), 0)
		require.Empty(t, random)
		require.Empty(t, rest)
	})

	t.Run("large n", func(t *testing.T) {
		partition := generateSrc(42)
		// n > len(partition).
		random, rest := partitionNRandom(partition, 100)
		require.Equal(t, partition, random)
		require.Empty(t, rest)
	})

	t.Run("single assignment", func(t *testing.T) {
		partition := generateSrc(50)
		random, rest := partitionNRandom(partition, 1)
		require.Len(t, random, 1)
		require.Len(t, rest, 49)

		// Ensure that the item get moved to the last position, and src is
		// still valid.
		require.Equal(t, random[0], partition[len(partition)-1])
		validateSrc(t, partition, 50)
	})

	t.Run("multiple assignments", func(t *testing.T) {
		partition := generateSrc(11)
		random, rest := partitionNRandom(partition, 10)
		require.Len(t, random, 10)
		require.Len(t, rest, 1)

		// Ensure that all items get moved to the end of list, and they are
		// unique.
		seen := make(map[string]bool)
		for i := 0; i < 10; i++ {
			a := partition[len(partition)-i-1]
			require.False(t, seen[a.Addr()])
			seen[a.Addr()] = true
		}
		for _, a := range random {
			require.True(t, seen[a.Addr()])
		}

		// And partition is still valid.
		validateSrc(t, partition, 11)
	})
}

func TestRebalancerQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

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
	require.Equal(t, int64(1), q.metrics.RebalanceReqQueued.Value())
	q.enqueue(req3)
	require.Equal(t, int64(1), q.metrics.RebalanceReqQueued.Value())
	q.enqueue(req2)
	require.Len(t, q.elements, 1)
	require.Equal(t, 1, q.queue.Len())
	require.Equal(t, int64(1), q.metrics.RebalanceReqQueued.Value())

	// Create another request.
	conn2 := &testConnHandle{}
	req4 := &rebalanceRequest{
		createdAt: timeSource.Now(),
		conn:      conn2,
	}
	q.enqueue(req4)
	require.Equal(t, int64(2), q.metrics.RebalanceReqQueued.Value())
	require.Len(t, q.elements, 2)
	require.Equal(t, 2, q.queue.Len())

	// Dequeue the items.
	item, err := q.dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, req3, item)
	require.Equal(t, int64(1), q.metrics.RebalanceReqQueued.Value())
	item, err = q.dequeue(ctx)
	require.NoError(t, err)
	require.Equal(t, req4, item)
	require.Equal(t, int64(0), q.metrics.RebalanceReqQueued.Value())
	require.Empty(t, q.elements)
	require.Equal(t, 0, q.queue.Len())

	// Cancel the context. Dequeue should return immediately with an error.
	cancel()
	req4, err = q.dequeue(ctx)
	require.EqualError(t, err, context.Canceled.Error())
	require.Nil(t, req4)
	require.Equal(t, int64(0), q.metrics.RebalanceReqQueued.Value())
}

// TestRebalancerQueueBlocking tests the blocking behavior when invoking
// dequeue.
func TestRebalancerQueueBlocking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

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

	tenantID := roachpb.MustMakeTenantID(pod.TenantID)
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

// makeTestHandle returns a test handle that doesn't panic when
// TransferConnection is called.
func makeTestHandle() *testConnHandle {
	return &testConnHandle{
		onTransferConnection: func() error { return nil },
	}
}
