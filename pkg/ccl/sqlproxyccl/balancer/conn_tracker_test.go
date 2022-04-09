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
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConnTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tracker := NewConnTracker()

	tenantID, handle := makeConn(20, "127.0.0.10:8090")
	require.True(t, tracker.OnConnect(tenantID, handle))
	require.False(t, tracker.OnConnect(tenantID, handle))

	conns := tracker.GetConns(tenantID)
	require.Len(t, conns, 1)
	require.Equal(t, handle, conns[0])

	// Non-existent.
	conns = tracker.GetConns(roachpb.MakeTenantID(10))
	require.Empty(t, conns)

	require.True(t, tracker.OnDisconnect(tenantID, handle))
	require.False(t, tracker.OnDisconnect(tenantID, handle))

	// Ensure methods are thread-safe.
	var wg sync.WaitGroup
	const clients = 50
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			tenantID, handle := makeConn(1+rand.Intn(5), fmt.Sprintf("127.0.0.10:%d", rand.Intn(5)))
			require.True(t, tracker.OnConnect(tenantID, handle))
			time.Sleep(250 * time.Millisecond)
			require.True(t, tracker.OnDisconnect(tenantID, handle))
		}()
	}

	wg.Wait()
	for _, entry := range tracker.mu.tenants {
		require.Empty(t, entry.mu.conns)
	}
}

func TestConnTracker_GetAllConns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tracker := NewConnTracker()

	tenant10, handle1 := makeConn(10, "127.0.0.10:1010")
	_, handle2 := makeConn(10, "127.0.0.10:1020")
	require.True(t, tracker.OnConnect(tenant10, handle1))
	require.True(t, tracker.OnConnect(tenant10, handle2))

	// Ensure that map contains two handles for tenant 10.
	tenantConns := tracker.GetAllConns()
	require.Len(t, tenantConns, 1)
	conns, ok := tenantConns[tenant10]
	require.True(t, ok)
	require.Len(t, conns, 2)

	// Add a new handle, and disconnect after.
	tenant20, handle3 := makeConn(20, "127.0.0.10:1020")
	require.True(t, tracker.OnConnect(tenant20, handle3))
	require.True(t, tracker.OnDisconnect(tenant20, handle3))

	// Ensure that tenants with no connections do not show up.
	tenantConns = tracker.GetAllConns()
	require.Len(t, tenantConns, 1)
	conns, ok = tenantConns[tenant10]
	require.True(t, ok)
	require.Len(t, conns, 2)

	// Add a new handle for tenant 10.
	_, handle4 := makeConn(10, "127.0.0.10:1020")
	require.True(t, tracker.OnConnect(tenant10, handle4))

	// Existing tenantConns does not change. This shows snapshotting.
	require.Len(t, tenantConns, 1)
	conns, ok = tenantConns[tenant10]
	require.True(t, ok)
	require.Len(t, conns, 2)

	// Fetch again, and we should have the updated entry.
	tenantConns = tracker.GetAllConns()
	require.Len(t, tenantConns, 1)
	conns, ok = tenantConns[tenant10]
	require.True(t, ok)
	require.Len(t, conns, 3)

	// Disconnect everything.
	require.True(t, tracker.OnDisconnect(tenant10, handle1))
	require.True(t, tracker.OnDisconnect(tenant10, handle2))
	require.True(t, tracker.OnDisconnect(tenant10, handle4))
	tenantConns = tracker.GetAllConns()
	require.Empty(t, tenantConns)
}

func TestTenantEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entry := newTenantEntry()

	h1 := newTestTrackerConnHandle("10.0.0.1:12345")
	require.True(t, entry.addHandle(h1))
	require.False(t, entry.addHandle(h1))

	conns := entry.getConns()
	require.Len(t, conns, 1)

	require.True(t, entry.removeHandle(h1))
	require.False(t, entry.removeHandle(h1))

	require.Empty(t, entry.getConns())
	require.Len(t, conns, 1)
}

// testTrackerConnHandle is a test connection handle that only implements a
// small subset of methods used for testing.
type testTrackerConnHandle struct {
	ConnectionHandle
	ctx               context.Context
	remoteAddr        string
	transferConnCount int32
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
	atomic.AddInt32(&h.transferConnCount, 1)
	return nil
}

func (h *testTrackerConnHandle) transferConnectionCount() int {
	return int(atomic.LoadInt32(&h.transferConnCount))
}

func makeConn(tenantID int, podAddr string) (roachpb.TenantID, *testTrackerConnHandle) {
	return roachpb.MakeTenantID(uint64(tenantID)), newTestTrackerConnHandle(podAddr)
}
