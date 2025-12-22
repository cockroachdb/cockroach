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
