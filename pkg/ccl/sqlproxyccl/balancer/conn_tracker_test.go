// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package balancer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestConnTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	tenant20 := roachpb.MustMakeTenantID(20)
	sa := &ServerAssignment{addr: "127.0.0.10:8090", owner: &testConnHandle{}}

	// Run twice for idempotency.
	tracker.registerAssignment(tenant20, sa)
	tracker.registerAssignment(tenant20, sa)
	activeList, idleList := tracker.listAssignments(tenant20)
	require.Equal(t, []*ServerAssignment{sa}, activeList)
	require.Empty(t, idleList)

	connsMap := tracker.GetConnsMap(tenant20)
	require.Len(t, connsMap, 1)
	h, ok := connsMap[sa.Addr()]
	require.True(t, ok)
	require.Equal(t, []ConnectionHandle{sa.Owner()}, h)

	tenantIDs := tracker.getTenantIDs()
	require.Len(t, tenantIDs, 1)
	require.Equal(t, tenant20, tenantIDs[0])

	// Non-existent.
	connsMap = tracker.GetConnsMap(roachpb.MustMakeTenantID(42))
	require.Empty(t, connsMap)
	activeList, idleList = tracker.listAssignments(roachpb.MustMakeTenantID(42))
	require.Empty(t, activeList)
	require.Empty(t, idleList)

	// Run twice for idempotency.
	tracker.unregisterAssignment(tenant20, sa)
	tracker.unregisterAssignment(tenant20, sa)
	activeList, idleList = tracker.listAssignments(tenant20)
	require.Empty(t, activeList)
	require.Empty(t, idleList)

	// Once the assignment gets unregistered, we shouldn't return that tenant
	// since there are no active connections.
	tenantIDs = tracker.getTenantIDs()
	require.Empty(t, tenantIDs)

	// Ensure methods are thread-safe.
	var wg sync.WaitGroup
	const clients = 50
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			tenantID := roachpb.MustMakeTenantID(uint64(1 + rand.Intn(5)))
			sa := &ServerAssignment{
				addr:  fmt.Sprintf("127.0.0.10:%d", rand.Intn(5)),
				owner: &testConnHandle{},
			}
			tracker.registerAssignment(tenantID, sa)
			time.Sleep(250 * time.Millisecond)
			tracker.unregisterAssignment(tenantID, sa)
		}()
	}

	wg.Wait()

	// Ensure that once all clients have returned, the tenant entries are empty.
	tenantIDs = tracker.getTenantIDs()
	require.Empty(t, tenantIDs)
	for _, entry := range tracker.mu.tenants {
		require.Empty(t, entry.assignments.active)
		require.Empty(t, entry.assignments.idle)
	}
	activeList, idleList = tracker.listAssignments(tenant20)
	require.Empty(t, activeList)
	require.Empty(t, idleList)
}

func TestConnTracker_GetConnsMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	makeConn := func(tenID int, addr string) (roachpb.TenantID, *ServerAssignment) {
		return roachpb.MustMakeTenantID(uint64(tenID)), &ServerAssignment{
			addr:  addr,
			owner: &testConnHandle{},
		}
	}

	tenant10, sa1 := makeConn(10, "127.0.0.10:1010")
	_, sa2 := makeConn(10, "127.0.0.10:1020")
	tracker.registerAssignment(tenant10, sa1)
	tracker.registerAssignment(tenant10, sa2)

	// Ensure that map contains two handles for tenant 10, one per pod.
	initialConnsMap := tracker.GetConnsMap(tenant10)
	require.Equal(t, map[string][]ConnectionHandle{
		sa1.Addr(): {sa1.Owner()},
		sa2.Addr(): {sa2.Owner()},
	}, initialConnsMap)

	// Add a new assignment, and unregister after.
	tenant20, sa3 := makeConn(20, "127.0.0.10:1020")
	tracker.registerAssignment(tenant20, sa3)
	tracker.unregisterAssignment(tenant20, sa3)

	// Ensure that tenants with no connections do not show up.
	connsMap := tracker.GetConnsMap(tenant20)
	require.Empty(t, connsMap)

	// Add a new assignment for tenant 10.
	_, sa4 := makeConn(10, "127.0.0.10:1020")
	tracker.registerAssignment(tenant10, sa4)

	// Existing initialConnsMap does not change. This shows snapshotting.
	require.Equal(t, map[string][]ConnectionHandle{
		sa1.Addr(): {sa1.Owner()},
		sa2.Addr(): {sa2.Owner()},
	}, initialConnsMap)

	// Fetch again, and we should have the updated entry.
	connsMap = tracker.GetConnsMap(tenant10)
	require.Equal(t, map[string][]ConnectionHandle{
		sa1.Addr(): {sa1.Owner()},
		sa2.Addr(): {sa2.Owner(), sa4.Owner()},
	}, connsMap)

	// Disconnect everything.
	tracker.unregisterAssignment(tenant10, sa1)
	tracker.unregisterAssignment(tenant10, sa2)
	tracker.unregisterAssignment(tenant10, sa4)
	connsMap = tracker.GetConnsMap(tenant10)
	require.Empty(t, connsMap)
}

func TestConnTrackerPartitionsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	tracker, err := NewConnTracker(ctx, stopper, timeSource)
	require.NoError(t, err)

	// Create three assignments: two for tenant-10, and one for tenant-20.
	// Use a dummy address for all of them.
	const addr = "127.0.0.10:1020"
	tenant10, tenant20 := roachpb.MustMakeTenantID(10), roachpb.MustMakeTenantID(20)
	h1, h2, h3 := &testConnHandle{}, &testConnHandle{}, &testConnHandle{}
	s1 := NewServerAssignment(tenant10, tracker, h1, addr)
	defer s1.Close()
	s2 := NewServerAssignment(tenant10, tracker, h2, addr)
	defer s2.Close()
	s3 := NewServerAssignment(tenant20, tracker, h3, addr)
	defer s3.Close()

	e10 := tracker.getEntry(tenant10, false)
	e20 := tracker.getEntry(tenant20, false)
	activeList, idleList := e10.listAssignments()
	require.Len(t, activeList, 2)
	require.Empty(t, idleList)
	activeList, idleList = e20.listAssignments()
	require.Len(t, activeList, 1)
	require.Empty(t, idleList)

	// Update idle states for h1 and h3.
	h1.setIdle(true)
	h3.setIdle(true)

	// Now advance the time, and partitions should update after the timer fires.
	// The manual time is a little flaky, so we'll continuously advance until
	// the update fires. Without this loop, we may advance, but the timer does
	// not get fired.
	testutils.SucceedsSoon(t, func() error {
		timeSource.Advance(partitionsRefreshInterval)

		activeList10, idleList10 := e10.listAssignments()
		activeList20, idleList20 := e20.listAssignments()
		if len(activeList10) == 1 && len(idleList10) == 1 &&
			len(activeList20) == 0 && len(idleList20) == 1 {
			return nil
		}
		return errors.New("partitions have not been updated yet")
	})
}

func TestTenantEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	entry := newTenantEntry()

	// Create a ServerAssignment without a tracker since that's not necessary
	// here.
	const addr = "10.0.0.1:12345"
	h1 := &testConnHandle{}
	s := &ServerAssignment{addr: addr, owner: h1}

	// Add a new assignment.
	require.True(t, entry.addAssignment(s))
	require.False(t, entry.addAssignment(s))
	require.Equal(t, 1, entry.assignmentsCount())
	activeListInitial, idleListInitial := entry.listAssignments()
	require.Len(t, activeListInitial, 1)
	require.Empty(t, idleListInitial)

	// Test for connsMap.
	//
	// TODO(jaylim-crl): Remove this block when getConnsMap gets removed.
	connsMap := entry.getConnsMap()
	require.Len(t, connsMap, 1)
	arr, ok := connsMap[addr]
	require.True(t, ok)
	require.Equal(t, []ConnectionHandle{h1}, arr)

	// Move assignment to the idle partition.
	h1.setIdle(true)
	entry.refreshPartitions()
	activeList, idleList := entry.listAssignments()
	require.Empty(t, activeList)
	require.Len(t, idleList, 1)

	// Verify that listAssignments are snapshots. They should not be updated
	// when we call refreshPartitions.
	require.Len(t, activeListInitial, 1)
	require.Empty(t, idleListInitial)

	// Trying to add an assignment would still fail.
	require.False(t, entry.addAssignment(s))

	// Remove the assignment when it's idle.
	require.True(t, entry.removeAssignment(s))
	require.False(t, entry.removeAssignment(s))
	require.Equal(t, 0, entry.assignmentsCount())
	activeList, idleList = entry.listAssignments()
	require.Empty(t, activeList)
	require.Empty(t, idleList)

	// Test for connsMap.
	//
	// TODO(jaylim-crl): Remove this block when getConnsMap gets removed.
	connsMap = entry.getConnsMap()
	require.Empty(t, connsMap)

	// Add the assignment again, and remove when it's active.
	h1.setIdle(false)
	require.True(t, entry.addAssignment(s))
	require.True(t, entry.removeAssignment(s))
}

func TestTenantEntry_refreshPartitions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	entry := newTenantEntry()

	h1 := &testConnHandle{}
	s1 := &ServerAssignment{addr: "1", owner: h1}
	h2 := &testConnHandle{}
	s2 := &ServerAssignment{addr: "2", owner: h2}

	// Add new assignments.
	require.True(t, entry.addAssignment(s1))
	require.True(t, entry.addAssignment(s2))
	activeList, idleList := entry.listAssignments()
	require.Len(t, activeList, 2)
	require.Empty(t, idleList)
	require.Equal(t, 2, entry.assignmentsCount())

	// Refresh should be a no-op.
	entry.refreshPartitions()
	activeList, idleList = entry.listAssignments()
	require.Len(t, activeList, 2)
	require.Empty(t, idleList)

	// Move s1 to the idle partition.
	h1.setIdle(true)
	entry.refreshPartitions()
	activeList, idleList = entry.listAssignments()
	require.Len(t, activeList, 1)
	require.Len(t, idleList, 1)

	// Move s1 to the active partition, and s2 to the idle partition.
	h1.setIdle(false)
	h2.setIdle(true)
	entry.refreshPartitions()
	activeList, idleList = entry.listAssignments()
	require.Equal(t, []*ServerAssignment{s1}, activeList)
	require.Equal(t, []*ServerAssignment{s2}, idleList)

	// Refresh should be a no-op.
	entry.refreshPartitions()
	activeList, idleList = entry.listAssignments()
	require.Len(t, activeList, 1)
	require.Len(t, idleList, 1)

	// Move both back to active partitions.
	h1.setIdle(false)
	h2.setIdle(false)
	entry.refreshPartitions()
	activeList, idleList = entry.listAssignments()
	require.Len(t, activeList, 2)
	require.Empty(t, idleList)

	// Ensure that partitions are always disjoint sets. This validates that
	// an assignment doesn't get re-added during a partition refresh once it has
	// been removed.
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h := &testConnHandle{}
			s := &ServerAssignment{addr: fmt.Sprint(i), owner: h1}

			time.Sleep(time.Duration(rand.Int31n(75)+25) * time.Millisecond) // 25-100ms
			require.True(t, entry.addAssignment(s))

			time.Sleep(time.Duration(rand.Int31n(75)+25) * time.Millisecond) // 25-100ms
			if rand.Float32() < 0.5 {
				h.setIdle(true)
			} else {
				h.setIdle(false)
			}

			time.Sleep(time.Duration(rand.Int31n(75)+25) * time.Millisecond) // 25-100ms
			require.True(t, entry.removeAssignment(s))
		}(i)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		wg.Wait()
		cancel()
	}()

	// Keep refreshing partitions, and validating for correctness.
	for ctx.Err() != nil {
		entry.refreshPartitions()

		// Validate that the lists are disjoint.
		activeList, idleList = entry.listAssignments()
		inActive := make(map[*ServerAssignment]struct{})
		for _, sa := range activeList {
			inActive[sa] = struct{}{}
		}
		for _, sa := range idleList {
			_, ok := inActive[sa]
			require.False(t, ok)
		}
	}
}
