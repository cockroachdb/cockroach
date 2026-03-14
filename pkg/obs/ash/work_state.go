// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
)

// WorkState represents the current work state of a goroutine.
// Goroutines explicitly register their state when performing work
// that should be captured by ASH sampling.
type WorkState struct {
	// TenantID identifies which tenant this work belongs to.
	TenantID roachpb.TenantID
	// WorkloadInfo groups workload identity fields.
	WorkloadInfo WorkloadInfo
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
var activeWorkStates syncutil.Map[int64, WorkState]

// activeWorkStatesCount tracks the number of goroutines with an
// active work state. It is incremented when a goroutine is first
// registered in activeWorkStates and decremented when it is fully
// removed. Nested (stacked) states do not change the count because
// the map key already exists.
var activeWorkStatesCount atomic.Int64

// maxRetiredWorkStates caps the retired list to prevent unbounded growth
// during bursts between sampler drain ticks. This value is based on an
// estimated ceiling of 10,000 QPS per node with a 3x multiplier for goroutines
// per query. States beyond this cap are not tracked for pooling and are instead
// garbage-collected.
const maxRetiredWorkStates = 30000

// retiredWorkStates holds WorkState objects that have been removed from
// activeWorkStates but cannot yet be returned to the pool. This is because
// rangeWorkStates may be dereferencing or copying a WorkState object.
// clearWorkState appends to this list; rangeWorkStates drains it after
// iteration completes.
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
//	info := ash.WorkloadInfo{WorkloadID: stmtFingerprintID, AppNameID: appNameID}
//	cleanup := ash.SetWorkState(tenantID, info, ash.WorkLock, "LockWait")
//	defer cleanup()
//	// ... perform work ...
func SetWorkState(
	tenantID roachpb.TenantID, info WorkloadInfo, eventType WorkEventType, event string,
) func() {
	if !enabled.Load() {
		return noop
	}
	if !tenantID.IsSet() {
		tenantID = roachpb.SystemTenantID
	}
	gid := goid.Get()
	state := workStatePool.Get().(*WorkState)
	state.TenantID = tenantID
	state.WorkloadInfo = info
	state.WorkEventType = eventType
	state.WorkEvent = event
	prev, ok := getWorkState(gid)
	if ok {
		state.prev = prev
	} else {
		state.prev = nil
		activeWorkStatesCount.Add(1)
	}
	activeWorkStates.Store(gid, state)

	// In test builds, we panic if the closure is called from a different goroutine.
	// It's unlikely, but better to catch improper usage of ASH API in tests.
	if buildutil.CrdbTestBuild {
		return func() {
			if clearGid := goid.Get(); clearGid != gid {
				panic(errors.AssertionFailedf(
					"clearWorkState called from goroutine %d, but SetWorkState was called from goroutine %d",
					clearGid, gid,
				))
			}
			_clearWorkState(gid)
		}
	}

	return clearWorkState
}

// noop is a pre-allocated no-op function returned when ASH is disabled.
var noop = func() {}

// ClearWorkState removes the work state for the current goroutine.
// It determines the goroutine ID via goid.Get(), so it must be called
// from the same goroutine that called SetWorkState. This is a
// package-level function (not a closure) so that returning it from
// SetWorkState does not require a heap allocation.
//
// The removed WorkState is placed on the retired list rather than
// immediately returned to the pool, because the sampler may be calling
// rangeWorkStates concurrently, and it is not safe to re-use the WorkState.
func clearWorkState() {
	_clearWorkState(goid.Get())
}

func _clearWorkState(gid int64) {
	state, ok := activeWorkStates.Load(gid)
	if !ok {
		return
	}
	prev := state.prev
	// Nil-ing state.prev here would be good hygiene, but is not required:
	// the sampler does not read prev from copied WorkStates, and reclaim
	// zeroes the struct anyway. We skip it to avoid a race with the
	// sampler's concurrent *value copy in rangeWorkStates.
	if prev != nil {
		activeWorkStates.Store(gid, prev)
	} else {
		activeWorkStates.Delete(gid)
		activeWorkStatesCount.Add(-1)
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

// reclaimRetiredWorkStates atomically drains the retired list and
// returns all retired WorkState objects to the pool.
func reclaimRetiredWorkStates() {
	retiredWorkStates.Lock()
	states := retiredWorkStates.states
	retiredWorkStates.states = nil
	retiredWorkStates.Unlock()

	for _, s := range states {
		*s = WorkState{}
		workStatePool.Put(s)
	}
}

func getWorkState(gid int64) (*WorkState, bool) {
	return activeWorkStates.Load(gid)
}

// rangeWorkStates iterates over all registered work states and then
// reclaims any retired WorkState objects back to the pool. The reclaim
// step is folded into this function because the retired list exists
// solely to defer pool returns while iteration is in progress — once
// iteration completes, it is always safe to reclaim.
//
// The callback receives a copy of the WorkState to avoid data races
// during iteration.
func rangeWorkStates(fn func(gid int64, state WorkState) bool) {
	activeWorkStates.Range(func(key int64, value *WorkState) bool {
		return fn(key, *value)
	})
	reclaimRetiredWorkStates()
}
