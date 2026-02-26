// Copyright 2025 The Cockroach Authors.
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
	clear1 := SetWorkState(tenantID, 100, WORK_CPU, "query-1")

	state, ok := getWorkState(gid)
	require.True(t, ok, "first work state not found")
	require.Equal(t, tenantID, state.TenantID)
	require.Equal(t, uint64(100), state.WorkloadID)
	require.Equal(t, WORK_CPU, state.WorkEventType)
	require.Equal(t, "query-1", state.WorkEvent)
	require.Nil(t, state.prev)

	// Push second work state (nested).
	clear2 := SetWorkState(tenantID, 200, WORK_IO, "kv-read")

	state, ok = getWorkState(gid)
	require.True(t, ok, "second work state not found")
	require.Equal(t, tenantID, state.TenantID)
	require.Equal(t, uint64(200), state.WorkloadID)
	require.Equal(t, WORK_IO, state.WorkEventType)
	require.Equal(t, "kv-read", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, uint64(100), state.prev.WorkloadID)

	// Push third work state (deeply nested).
	clear3 := SetWorkState(tenantID, 300, WORK_NETWORK, "rpc-call")

	state, ok = getWorkState(gid)
	require.True(t, ok, "third work state not found")
	require.Equal(t, uint64(300), state.WorkloadID)
	require.Equal(t, WORK_NETWORK, state.WorkEventType)
	require.Equal(t, "rpc-call", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, uint64(200), state.prev.WorkloadID)
	require.NotNil(t, state.prev.prev)
	require.Equal(t, uint64(100), state.prev.prev.WorkloadID)

	// Pop third work state; second should be restored.
	clear3()

	state, ok = getWorkState(gid)
	require.True(t, ok, "second work state not restored after popping third")
	require.Equal(t, uint64(200), state.WorkloadID)
	require.Equal(t, WORK_IO, state.WorkEventType)

	// Pop second work state; first should be restored.
	clear2()

	state, ok = getWorkState(gid)
	require.True(t, ok, "first work state not restored after popping second")
	require.Equal(t, uint64(100), state.WorkloadID)
	require.Equal(t, WORK_CPU, state.WorkEventType)

	// Pop first work state; goroutine should have no state.
	clear1()

	_, ok = getWorkState(gid)
	require.False(t, ok, "work state should be cleared after popping all")
}

func TestWorkStateRetiredList(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)
	defer reclaimRetiredWorkStates()

	// Push and pop a work state.
	clear := SetWorkState(roachpb.SystemTenantID, 42, WORK_CPU, "test")
	clear()

	// The work state should no longer be visible.
	var found bool
	RangeWorkStates(func(gid int64, state WorkState) bool {
		found = true
		return true
	})
	require.False(t, found, "cleared work state should not be visible")
}

func TestWorkStateDisabled(t *testing.T) {
	enabled.Store(false)

	clear := SetWorkState(roachpb.SystemTenantID, 999, WORK_CPU, "should-not-appear")
	defer clear()

	var found bool
	RangeWorkStates(func(gid int64, state WorkState) bool {
		found = true
		return true
	})
	require.False(t, found, "work state should not be registered when disabled")
}
