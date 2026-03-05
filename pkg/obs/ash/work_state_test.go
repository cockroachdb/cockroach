// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/require"
)

func TestWorkStateStack(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)

	gid := goid.Get()

	tenantID := roachpb.MustMakeTenantID(5)

	// Push first work state.
	clear1 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 100}, WorkCPU, "query-1")

	state, ok := getWorkState(gid)
	require.True(t, ok, "first work state not found")
	require.Equal(t, tenantID, state.TenantID)
	require.Equal(t, uint64(100), state.WorkloadInfo.WorkloadID)
	require.Equal(t, WorkCPU, state.WorkEventType)
	require.Equal(t, "query-1", state.WorkEvent)
	require.Nil(t, state.prev)

	// Push second work state (nested).
	clear2 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 200}, WorkIO, "kv-read")

	state, ok = getWorkState(gid)
	require.True(t, ok, "second work state not found")
	require.Equal(t, tenantID, state.TenantID)
	require.Equal(t, uint64(200), state.WorkloadInfo.WorkloadID)
	require.Equal(t, WorkIO, state.WorkEventType)
	require.Equal(t, "kv-read", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, uint64(100), state.prev.WorkloadInfo.WorkloadID)

	// Push third work state (deeply nested).
	clear3 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 300}, WorkNetwork, "rpc-call")

	state, ok = getWorkState(gid)
	require.True(t, ok, "third work state not found")
	require.Equal(t, uint64(300), state.WorkloadInfo.WorkloadID)
	require.Equal(t, WorkNetwork, state.WorkEventType)
	require.Equal(t, "rpc-call", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, uint64(200), state.prev.WorkloadInfo.WorkloadID)
	require.NotNil(t, state.prev.prev)
	require.Equal(t, uint64(100), state.prev.prev.WorkloadInfo.WorkloadID)

	// Pop third work state; second should be restored.
	clear3()

	state, ok = getWorkState(gid)
	require.True(t, ok, "second work state not restored after popping third")
	require.Equal(t, uint64(200), state.WorkloadInfo.WorkloadID)
	require.Equal(t, WorkIO, state.WorkEventType)

	// Pop second work state; first should be restored.
	clear2()

	state, ok = getWorkState(gid)
	require.True(t, ok, "first work state not restored after popping second")
	require.Equal(t, uint64(100), state.WorkloadInfo.WorkloadID)
	require.Equal(t, WorkCPU, state.WorkEventType)

	// Pop first work state; goroutine should have no state.
	clear1()

	_, ok = getWorkState(gid)
	require.False(t, ok, "work state should be cleared after popping all")
}

func TestWorkStateRetiredList(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)
	// Push and pop a work state.
	clear := SetWorkState(roachpb.SystemTenantID, WorkloadInfo{WorkloadID: 42}, WorkCPU, "test")
	clear()

	// The work state should no longer be visible.
	var found bool
	rangeWorkStates(func(gid int64, state WorkState) bool {
		found = true
		return true
	})
	require.False(t, found, "cleared work state should not be visible")
}

func TestWorkStateInterleavedNestedPushPopWithReclaim(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(7)

	// Push first state.
	clear1 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 10}, WorkCPU, "query")

	// Sample active states (as the sampler would).
	var count int
	rangeWorkStates(func(gid int64, state WorkState) bool {
		count++
		require.Equal(t, uint64(10), state.WorkloadInfo.WorkloadID)
		return true
	})
	require.Equal(t, 1, count)

	// Push nested state.
	clear2 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 20}, WorkIO, "batch-eval")

	// Pop the nested state while the outer one is still active.
	clear2()
	reclaimRetiredWorkStates()

	// The outer state should still be visible and valid.
	gid := goid.Get()
	state, ok := getWorkState(gid)
	require.True(t, ok, "outer state should still be active")
	require.Equal(t, uint64(10), state.WorkloadInfo.WorkloadID)
	require.Nil(t, state.prev, "prev pointer should have been nilled before retire")

	// Push another nested state.
	clear3 := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 30}, WorkLock, "lock-wait")

	// Sample again.
	count = 0
	rangeWorkStates(func(gid int64, state WorkState) bool {
		count++
		require.Equal(t, uint64(30), state.WorkloadInfo.WorkloadID)
		return true
	})
	require.Equal(t, 1, count)

	// Pop in reverse order.
	clear3()
	reclaimRetiredWorkStates()

	state, ok = getWorkState(gid)
	require.True(t, ok, "outer state should be restored")
	require.Equal(t, uint64(10), state.WorkloadInfo.WorkloadID)

	clear1()
	reclaimRetiredWorkStates()

	_, ok = getWorkState(gid)
	require.False(t, ok, "all states should be cleared")
}

func TestWorkStateDisabled(t *testing.T) {
	enabled.Store(false)

	clear := SetWorkState(
		roachpb.SystemTenantID, WorkloadInfo{WorkloadID: 999}, WorkCPU, "should-not-appear",
	)
	defer clear()

	var found bool
	rangeWorkStates(func(gid int64, state WorkState) bool {
		found = true
		return true
	})
	require.False(t, found, "work state should not be registered when disabled")
}

func TestRetiredWorkStatesCapOverflow(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)

	// Drain any pre-existing retired states from other tests.
	reclaimRetiredWorkStates()

	tenantID := roachpb.MustMakeTenantID(1)

	// Fill the retired list to the cap by setting and clearing
	// work states without calling rangeWorkStates (which drains).
	for range maxRetiredWorkStates {
		clear := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 1}, WorkCPU, "fill")
		clear()
	}

	retiredWorkStates.Lock()
	require.Equal(t, maxRetiredWorkStates, len(retiredWorkStates.states))
	retiredWorkStates.Unlock()

	// One more retire should be silently dropped.
	clear := SetWorkState(tenantID, WorkloadInfo{WorkloadID: 1}, WorkCPU, "overflow")
	clear()

	retiredWorkStates.Lock()
	require.Equal(t, maxRetiredWorkStates, len(retiredWorkStates.states))
	retiredWorkStates.Unlock()

	// Clean up: drain the retired list back to the pool.
	reclaimRetiredWorkStates()
}
