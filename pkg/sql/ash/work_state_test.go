// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"testing"

	"github.com/petermattis/goid"
	"github.com/stretchr/testify/require"
)

// TestWorkStateStack verifies that sequential SetWorkState calls create a stack
// and that cleanup functions properly restore previous states.
func TestWorkStateStack(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)

	gid := goid.Get()

	// Initially no work state.
	_, ok := getWorkState(gid)
	require.False(t, ok)

	// Create a stack: ConnExecutor -> DistSenderRemote -> LockWait
	cleanup1 := SetWorkState(100, WORK_CPU, "ConnExecutor")
	state, ok := getWorkState(gid)
	require.True(t, ok)
	require.Equal(t, uint64(100), state.WorkloadID)
	require.Equal(t, WORK_CPU, state.WorkEventType)
	require.Equal(t, "ConnExecutor", state.WorkEvent)
	require.Nil(t, state.prev)

	cleanup2 := SetWorkState(100, WORK_NETWORK, "DistSenderRemote")
	state, ok = getWorkState(gid)
	require.True(t, ok)
	require.Equal(t, "DistSenderRemote", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, "ConnExecutor", state.prev.WorkEvent)

	cleanup3 := SetWorkState(100, WORK_LOCK, "LockWait")
	state, ok = getWorkState(gid)
	require.True(t, ok)
	require.Equal(t, "LockWait", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, "DistSenderRemote", state.prev.WorkEvent)
	require.NotNil(t, state.prev.prev)
	require.Equal(t, "ConnExecutor", state.prev.prev.WorkEvent)
	require.Nil(t, state.prev.prev.prev)

	// Pop LockWait - should restore DistSenderRemote.
	cleanup3()
	state, ok = getWorkState(gid)
	require.True(t, ok)
	require.Equal(t, "DistSenderRemote", state.WorkEvent)
	require.NotNil(t, state.prev)
	require.Equal(t, "ConnExecutor", state.prev.WorkEvent)
	require.Nil(t, state.prev.prev)

	// Pop DistSenderRemote - should restore ConnExecutor.
	cleanup2()
	state, ok = getWorkState(gid)
	require.True(t, ok)
	require.Equal(t, "ConnExecutor", state.WorkEvent)
	require.Nil(t, state.prev)

	// Pop ConnExecutor - stack should be empty.
	cleanup1()
	_, ok = getWorkState(gid)
	require.False(t, ok)

	// Clearing when empty is safe.
	clearWorkState(gid)
	_, ok = getWorkState(gid)
	require.False(t, ok)

	// Drain the retired list so pool objects are recycled.
	reclaimRetiredWorkStates()
}

// TestWorkStateRetiredList verifies that cleared work states are placed on the
// retired list (not immediately returned to the pool) and that
// reclaimRetiredWorkStates returns them to the pool.
func TestWorkStateRetiredList(t *testing.T) {
	enabled.Store(true)
	defer enabled.Store(false)

	cleanup := SetWorkState(42, WORK_CPU, "test")

	// Retired list should be empty before cleanup.
	retiredWorkStates.Lock()
	require.Empty(t, retiredWorkStates.states)
	retiredWorkStates.Unlock()

	// Cleanup moves the state to the retired list, NOT the pool.
	cleanup()

	retiredWorkStates.Lock()
	require.Len(t, retiredWorkStates.states, 1)
	// The retired state should still have its original field values
	// (not zeroed), proving it wasn't returned to the pool yet.
	require.Equal(t, uint64(42), retiredWorkStates.states[0].WorkloadID)
	require.Equal(t, "test", retiredWorkStates.states[0].WorkEvent)
	retiredWorkStates.Unlock()

	// Reclaim returns retired states to the pool and zeroes them.
	reclaimRetiredWorkStates()

	retiredWorkStates.Lock()
	require.Empty(t, retiredWorkStates.states)
	retiredWorkStates.Unlock()
}

// TestWorkStateDisabled verifies that SetWorkState is a no-op when disabled.
func TestWorkStateDisabled(t *testing.T) {
	enabled.Store(false)
	defer enabled.Store(true)

	gid := goid.Get()

	cleanup := SetWorkState(100, WORK_CPU, "ConnExecutor")
	_, ok := getWorkState(gid)
	require.False(t, ok)

	// Cleanup should be safe to call even when disabled.
	cleanup()
	_, ok = getWorkState(gid)
	require.False(t, ok)
}
