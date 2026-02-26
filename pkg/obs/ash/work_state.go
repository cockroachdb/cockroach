// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"encoding/hex"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/petermattis/goid"
)

// WorkState represents the current work state of a goroutine.
// Goroutines explicitly register their state when performing work
// that should be captured by ASH sampling.
type WorkState struct {
	// TenantID identifies which tenant this work belongs to.
	TenantID roachpb.TenantID
	// WorkloadID identifies the workload (e.g., statement fingerprint).
	WorkloadID uint64
	// WorkEventType categorizes the type of work.
	WorkEventType WorkEventType
	// WorkEvent is a more specific identifier for the work event.
	WorkEvent string
	// prev is the previous work state for the goroutine if one exists.
	prev *WorkState
}

// workStatePool pools WorkState structs to reduce allocations.
var workStatePool = sync.Pool{
	New: func() any { return &WorkState{} },
}

// activeWorkStates maps goroutine IDs to their work state.
// This is a global map that the sampler reads from periodically.
var activeWorkStates sync.Map // int64 (goroutine ID) -> *WorkState

// maxRetiredWorkStates caps the retired list to prevent unbounded growth
// during bursts between sampler drain ticks. This value is based on an
// estimated ceiling of 10,000 QPS per node with a 3x multiplier for goroutines
// per query. States beyond this cap are not tracked for pooling and are instead
// garbage-collected.
const maxRetiredWorkStates = 30000

// retiredWorkStates holds WorkState objects that have been removed from
// activeWorkStates but cannot yet be returned to the pool. This is because
// RangeWorkStates may be dereferencing or copying a WorkState object.
// clearWorkState appends to this list; the sampler drains it after RangeWorkStates.
var retiredWorkStates struct {
	syncutil.Mutex
	states []*WorkState
}

// SetWorkState registers the current goroutine's work state.
// It returns a cleanup function that should be deferred to clear the state.
// If ASH sampling is disabled, this is a no-op and returns a no-op cleanup
// function.
//
// Example usage:
//
//	cleanup := ash.SetWorkState(tenantID, stmtFingerprintID, ash.WORK_LOCK, "LockWait")
//	defer cleanup()
//	// ... perform work ...
func SetWorkState(
	tenantID roachpb.TenantID, workloadID uint64, eventType WorkEventType, event string,
) func() {
	if !enabled.Load() {
		return noop
	}
	gid := goid.Get()
	state := workStatePool.Get().(*WorkState)
	state.TenantID = tenantID
	state.WorkloadID = workloadID
	state.WorkEventType = eventType
	state.WorkEvent = event
	prev, ok := getWorkState(gid)
	if ok {
		state.prev = prev
	} else {
		state.prev = nil
	}
	activeWorkStates.Store(gid, state)
	return clearWorkState
}

// noop is a pre-allocated no-op function returned when ASH is disabled.
var noop = func() {}

// EncodeUint64ToBytes returns the []byte representation of a uint64 value.
func EncodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

// EncodeStmtFingerprintIDToString returns the hex string representation of a
// statement fingerprint ID.
func EncodeStmtFingerprintIDToString(id uint64) string {
	return hex.EncodeToString(EncodeUint64ToBytes(id))
}

// ClearWorkState removes the work state for the current goroutine.
// It determines the goroutine ID via goid.Get(), so it must be called
// from the same goroutine that called SetWorkState. This is a
// package-level function (not a closure) so that returning it from
// SetWorkState does not require a heap allocation.
//
// The removed WorkState is placed on the retired list rather than
// immediately returned to the pool, because the sampler may be calling
// RangeWorkStates concurrently, and it is not safe to re-use the WorkState.
func clearWorkState() {
	_clearWorkState(goid.Get())
}

func _clearWorkState(gid int64) {
	v, ok := activeWorkStates.Load(gid)
	if !ok {
		return
	}
	state := v.(*WorkState)
	if state.prev != nil {
		activeWorkStates.Store(gid, state.prev)
	} else {
		activeWorkStates.Delete(gid)
	}
	retireWorkState(state)
}

// retireWorkState adds a WorkState to the retired list for deferred pool
// return. If the retired list has reached maxRetiredWorkStates, the state
// is not tracked for pooling and will be garbage-collected instead.
func retireWorkState(state *WorkState) {
	retiredWorkStates.Lock()
	defer retiredWorkStates.Unlock()
	if len(retiredWorkStates.states) >= maxRetiredWorkStates {
		return
	}
	retiredWorkStates.states = append(retiredWorkStates.states, state)
}

// reclaimRetiredWorkStates returns all retired WorkState objects to the pool.
func reclaimRetiredWorkStates() {
	states := drainRetiredWorkStates()
	for _, s := range states {
		*s = WorkState{}
		workStatePool.Put(s)
	}
}

// drainRetiredWorkStates atomically swaps out the retired list and returns it.
func drainRetiredWorkStates() []*WorkState {
	retiredWorkStates.Lock()
	defer retiredWorkStates.Unlock()
	states := retiredWorkStates.states
	retiredWorkStates.states = retiredWorkStates.states[:0]
	return states
}

func getWorkState(gid int64) (*WorkState, bool) {
	v, ok := activeWorkStates.Load(gid)
	if !ok {
		return nil, false
	}
	return v.(*WorkState), true
}

// RangeWorkStates iterates over all registered work states.
// The callback receives a copy of the WorkState to avoid data races during
// iteration.
func RangeWorkStates(fn func(gid int64, state WorkState) bool) {
	activeWorkStates.Range(func(key, value any) bool {
		return fn(key.(int64), *value.(*WorkState))
	})
}
