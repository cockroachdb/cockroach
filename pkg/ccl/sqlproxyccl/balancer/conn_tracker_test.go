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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

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

func TestConnTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	tenantID, handle := makeConn(20, "127.0.0.10:8090")
	require.True(t, tracker.OnConnect(tenantID, handle))
	require.False(t, tracker.OnConnect(tenantID, handle))

	cache := tracker.GetTenantCache(tenantID)
	require.Equal(t, 1, cache.ActiveCountByAddr(handle.remoteAddr))

	connsMap := tracker.GetConnsMap(tenantID)
	require.Len(t, connsMap, 1)
	h, ok := connsMap[handle.remoteAddr]
	require.True(t, ok)
	require.Equal(t, []ConnectionHandle{handle}, h)

	tenantIDs := tracker.GetTenantIDs()
	require.Len(t, tenantIDs, 1)
	require.Equal(t, tenantID, tenantIDs[0])

	// Non-existent.
	connsMap = tracker.GetConnsMap(roachpb.MakeTenantID(42))
	require.Empty(t, connsMap)
	nilCache := tracker.GetTenantCache(roachpb.MakeTenantID(42))
	require.Nil(t, nilCache)

	require.True(t, tracker.OnDisconnect(tenantID, handle))
	require.False(t, tracker.OnDisconnect(tenantID, handle))
	require.Equal(t, 0, cache.ActiveCountByAddr(handle.remoteAddr))

	// Once the handle gets disconnected, we shouldn't return that tenant since
	// there are no active connections.
	tenantIDs = tracker.GetTenantIDs()
	require.Empty(t, tenantIDs)

	// Ensure methods are thread-safe.
	var wg sync.WaitGroup
	const clients = 50
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			tenantID, handle := makeConn(1+rand.Intn(5), fmt.Sprintf("127.0.0.10:%d", rand.Intn(5)))
			require.True(t, tracker.OnConnect(tenantID, handle))
			cache := tracker.GetTenantCache(tenantID)
			require.True(t, cache.ActiveCountByAddr(handle.remoteAddr) >= 1)
			time.Sleep(250 * time.Millisecond)
			require.True(t, tracker.OnDisconnect(tenantID, handle))
		}()
	}

	wg.Wait()

	tenantIDs = tracker.GetTenantIDs()
	require.Empty(t, tenantIDs)
	for _, entry := range tracker.mu.tenants {
		require.Empty(t, entry.mu.conns)
	}
}

func TestConnTracker_GetConnsMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tracker, err := NewConnTracker(ctx, stopper, nil /* timeSource */)
	require.NoError(t, err)

	tenant10, handle1 := makeConn(10, "127.0.0.10:1010")
	_, handle2 := makeConn(10, "127.0.0.10:1020")
	require.True(t, tracker.OnConnect(tenant10, handle1))
	require.True(t, tracker.OnConnect(tenant10, handle2))

	// Ensure that map contains two handles for tenant 10, one per pod.
	initialConnsMap := tracker.GetConnsMap(tenant10)
	require.Equal(t, map[string][]ConnectionHandle{
		handle1.remoteAddr: {handle1},
		handle2.remoteAddr: {handle2},
	}, initialConnsMap)

	// Add a new handle, and disconnect after.
	tenant20, handle3 := makeConn(20, "127.0.0.10:1020")
	require.True(t, tracker.OnConnect(tenant20, handle3))
	require.True(t, tracker.OnDisconnect(tenant20, handle3))

	// Ensure that tenants with no connections do not show up.
	connsMap := tracker.GetConnsMap(tenant20)
	require.Empty(t, connsMap)

	// Add a new handle for tenant 10.
	_, handle4 := makeConn(10, "127.0.0.10:1020")
	require.True(t, tracker.OnConnect(tenant10, handle4))

	// Existing initialConnsMap does not change. This shows snapshotting.
	require.Equal(t, map[string][]ConnectionHandle{
		handle1.remoteAddr: {handle1},
		handle2.remoteAddr: {handle2},
	}, initialConnsMap)

	// Fetch again, and we should have the updated entry.
	connsMap = tracker.GetConnsMap(tenant10)
	require.Equal(t, map[string][]ConnectionHandle{
		handle1.remoteAddr: {handle1},
		handle2.remoteAddr: {handle2, handle4},
	}, connsMap)

	// Disconnect everything.
	require.True(t, tracker.OnDisconnect(tenant10, handle1))
	require.True(t, tracker.OnDisconnect(tenant10, handle2))
	require.True(t, tracker.OnDisconnect(tenant10, handle4))
	connsMap = tracker.GetConnsMap(tenant10)
	require.Empty(t, connsMap)
}

func TestConnTrackerCacheRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)

	tracker, err := NewConnTracker(ctx, stopper, timeSource)
	require.NoError(t, err)

	// Create three handles: two for tenant-10, and one for tenant-20. Use a
	// dummy address for all of them.
	const addr = "127.0.0.10:1020"
	tenant10, h1 := makeConn(10, addr)
	_, h2 := makeConn(10, addr)
	tenant20, h3 := makeConn(20, addr)

	require.Nil(t, tracker.GetTenantCache(tenant10))
	require.Nil(t, tracker.GetTenantCache(tenant20))

	// Connect all the handles.
	require.True(t, tracker.OnConnect(tenant10, h1))
	require.True(t, tracker.OnConnect(tenant10, h2))
	require.True(t, tracker.OnConnect(tenant20, h3))

	cache10 := tracker.GetTenantCache(tenant10)
	cache20 := tracker.GetTenantCache(tenant20)
	require.Equal(t, 2, cache10.ActiveCountByAddr(addr))
	require.Equal(t, 1, cache20.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache10.IdleCountByAddr(addr))
	require.Equal(t, 0, cache20.IdleCountByAddr(addr))

	// Update idle state, and cache stays the same.
	h1.setIdle(true)
	h3.setIdle(true)
	require.Equal(t, 2, cache10.ActiveCountByAddr(addr))
	require.Equal(t, 1, cache20.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache10.IdleCountByAddr(addr))
	require.Equal(t, 0, cache20.IdleCountByAddr(addr))

	// Now advance the time, and cache should update after the timer fires.
	// The manual time is a little flaky, so we'll continuously advance until
	// the update fires. Without this loop, we may advance, but the timer does
	// not get fired.
	testutils.SucceedsSoon(t, func() error {
		timeSource.Advance(cacheRefreshInterval)
		if cache10.IdleCountByAddr(addr) == 1 && cache20.IdleCountByAddr(addr) == 1 {
			return nil
		}
		return errors.New("idle count has not been updated yet")
	})
	require.Equal(t, 1, cache10.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache20.ActiveCountByAddr(addr))
	require.Equal(t, 1, cache10.IdleCountByAddr(addr))
	require.Equal(t, 1, cache20.IdleCountByAddr(addr))
}

func TestTenantEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entry := newTenantEntry()
	cache := entry.getCache()

	const addr = "10.0.0.1:12345"
	h1 := newTestTrackerConnHandle(addr)

	// Add a new handle.
	require.True(t, entry.addHandle(h1))
	require.False(t, entry.addHandle(h1))
	require.Equal(t, 1, entry.getConnsCount())
	require.Equal(t, 1, cache.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache.IdleCountByAddr(addr))
	connsMap := entry.getConnsMap()
	require.Len(t, connsMap, 1)

	// Cache would still be a snapshot until refreshed.
	h1.setIdle(true)
	require.Equal(t, 1, cache.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache.IdleCountByAddr(addr))
	entry.refreshCache()
	require.Equal(t, 0, cache.ActiveCountByAddr(addr))
	require.Equal(t, 1, cache.IdleCountByAddr(addr))

	// Remove the handle.
	require.True(t, entry.removeHandle(h1))
	require.False(t, entry.removeHandle(h1))
	require.Equal(t, 0, entry.getConnsCount())
	require.Empty(t, entry.getConnsMap())
	require.Len(t, connsMap, 1)

	// The update should be made in the right partition.
	require.Equal(t, 0, cache.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache.IdleCountByAddr(addr))

	// Add two handles, one active (h2), and one idle (h1). New connections are
	// always regarded as active until refreshed.
	h2 := newTestTrackerConnHandle(addr)
	require.True(t, entry.addHandle(h1))
	require.True(t, entry.addHandle(h2))
	require.Equal(t, 2, cache.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache.IdleCountByAddr(addr))
	entry.refreshCache()
	require.Equal(t, 1, cache.ActiveCountByAddr(addr))
	require.Equal(t, 1, cache.IdleCountByAddr(addr))

	// Reset h1's state back to active, and disconnect that handle. The cache
	// should be stale.
	h1.setIdle(false)
	require.True(t, entry.removeHandle(h1))
	require.Equal(t, 0, cache.ActiveCountByAddr(addr))
	require.Equal(t, 1, cache.IdleCountByAddr(addr))
	entry.refreshCache()
	require.Equal(t, 1, cache.ActiveCountByAddr(addr))
	require.Equal(t, 0, cache.IdleCountByAddr(addr))
}

// testTrackerConnHandle is a test connection handle that only implements a
// small subset of methods used for testing.
type testTrackerConnHandle struct {
	ConnectionHandle
	ctx        context.Context
	remoteAddr string

	mu struct {
		syncutil.Mutex
		transferConnCount int
		idle              bool
	}
}

var _ ConnectionHandle = &testTrackerConnHandle{}

func newTestTrackerConnHandleWithContext(
	ctx context.Context, remoteAddr string,
) *testTrackerConnHandle {
	return &testTrackerConnHandle{ctx: ctx, remoteAddr: remoteAddr}
}

func newTestTrackerConnHandle(remoteAddr string) *testTrackerConnHandle {
	return &testTrackerConnHandle{remoteAddr: remoteAddr}
}

// Context implements the ConnectionHandle interface.
func (h *testTrackerConnHandle) Context() context.Context {
	if h.ctx != nil {
		return h.ctx
	}
	return context.Background()
}

// ServerRemoteAddr implements the ConnectionHandle interface.
func (h *testTrackerConnHandle) ServerRemoteAddr() string {
	return h.remoteAddr
}

// TransferConnection implements the ConnectionHandle interface.
func (h *testTrackerConnHandle) TransferConnection() error {
	if h.ctx != nil && h.ctx.Err() != nil {
		return h.ctx.Err()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.transferConnCount++
	return nil
}

func (h *testTrackerConnHandle) transferConnectionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.transferConnCount
}

// IsIdle implements the ConnectionHandle interface.
func (h *testTrackerConnHandle) IsIdle() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.idle
}

func (h *testTrackerConnHandle) setIdle(idle bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.idle = idle
}

func makeConn(tenantID int, podAddr string) (roachpb.TenantID, *testTrackerConnHandle) {
	return roachpb.MakeTenantID(uint64(tenantID)), newTestTrackerConnHandle(podAddr)
}
