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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConnTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tracker := NewConnTracker()
	makeConn := func(tenantID int, podAddr string) (roachpb.TenantID, *testTrackerConnHandle) {
		return roachpb.MakeTenantID(uint64(tenantID)), newTestTrackerConnHandle(podAddr)
	}

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
// small subset of methods used for testing the connection tracker.
type testTrackerConnHandle struct {
	ConnectionHandle
	remoteAddr string
}

var _ ConnectionHandle = &testTrackerConnHandle{}

func newTestTrackerConnHandle(remoteAddr string) *testTrackerConnHandle {
	return &testTrackerConnHandle{remoteAddr: remoteAddr}
}

// Context implements the ConnectionHandle interface.
func (h *testTrackerConnHandle) Context() context.Context {
	return context.Background()
}

// ServerRemoteAddr implements the ConnectionHandle interface.
func (h *testTrackerConnHandle) ServerRemoteAddr() string {
	return h.remoteAddr
}
