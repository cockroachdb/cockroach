// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"encoding/hex"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/petermattis/goid"
)

// Well-known workload IDs for internal operations.
// These are used to attribute background work that doesn't have a user workload.
const (
	// IntentResolutionWorkloadID is the workload ID used for intent resolution.
	IntentResolutionWorkloadID uint64 = 0xFFFFFFFFFFFFFF01
	// TxnHeartbeatWorkloadID is the workload ID used for transaction heartbeats.
	TxnHeartbeatWorkloadID uint64 = 0xFFFFFFFFFFFFFF02
	// RangeLookupWorkloadID is the workload ID used for range lookups.
	RangeLookupWorkloadID uint64 = 0xFFFFFFFFFFFFFF03
	// LeaseRequestWorkloadID is the workload ID used for lease acquisition.
	LeaseRequestWorkloadID uint64 = 0xFFFFFFFFFFFFFF04
)

// WorkState represents the current work state of a goroutine.
// Goroutines explicitly register their state when performing work
// that should be captured by ASH sampling.
type WorkState struct {
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

// SetWorkState registers the current goroutine's work state.
// It returns a cleanup function that should be deferred to clear the state.
// If ASH sampling is disabled, this is a no-op and returns a no-op cleanup function.
//
// Example usage:
//
//	cleanup := ash.SetWorkState(stmtFingerprintID, ash.WORK_LOCK, "LockWait")
//	defer cleanup()
//	// ... perform work ...
func SetWorkState(workloadID uint64, eventType WorkEventType, event string) func() {
	if !enabled.Load() {
		return func() {}
	}
	gid := goid.Get()
	state := workStatePool.Get().(*WorkState)
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
	return func() {
		clearWorkState(gid)
	}
}

// EncodeUint64ToBytes returns the []byte representation of an uint64 value.
func EncodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

// EncodeStmtFingerprintIDToString returns the hex string representation of a statement fingerprint ID.
func EncodeStmtFingerprintIDToString(id uint64) string {
	return hex.EncodeToString(EncodeUint64ToBytes(id))
}

// clearWorkState removes the work state for the given goroutine and returns it to the pool.
func clearWorkState(gid int64) {
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
	// Clear the state before returning to pool.
	state.WorkloadID = 0
	state.WorkEventType = 0
	state.WorkEvent = ""
	state.prev = nil
	workStatePool.Put(state)
}

func getWorkState(gid int64) (*WorkState, bool) {
	v, ok := activeWorkStates.Load(gid)
	if !ok {
		return nil, false
	}
	return v.(*WorkState), true
}

// RangeWorkStates iterates over all registered work states.
// The callback receives a copy of the WorkState to avoid data races during iteration.
func RangeWorkStates(fn func(gid int64, state WorkState) bool) {
	activeWorkStates.Range(func(key, value any) bool {
		return fn(key.(int64), *value.(*WorkState))
	})
}
