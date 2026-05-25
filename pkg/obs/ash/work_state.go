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
	"github.com/cockroachdb/cockroach/pkg/util/system"
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

// numWorkStateShards is the number of shards used to partition
// goroutine work states. Must be a power of two. Sharding reduces
// contention on the underlying syncutil.Map mutexes when many goroutines
// call SetWorkState concurrently on high-core machines; without it,
// every Store hits the same mutex and dirty-map pointer, causing
// cache-line invalidation cascades across cores.
//
// 32 shards keeps average contention at ~2 cores per shard on
// 64-vCPU machines. On low-core machines the overhead is negligible:
// shard selection is a single bitwise AND, each empty shard costs only
// the syncutil.Map zero value, and rangeWorkStates iterates all shards
// just once per second.
const numWorkStateShards = 32

// Compile-time assertion: numWorkStateShards must be a power of two.
const _ = -uint(numWorkStateShards & (numWorkStateShards - 1))

// maxRetiredPerShard caps each shard's retired list independently.
const maxRetiredPerShard = maxRetiredWorkStates / numWorkStateShards

// workStateMapShard is a single shard of the work state map.
type workStateMapShard struct {
	m syncutil.Map[int64, WorkState]

	retired struct {
		syncutil.Mutex
		states []*WorkState
	}

	// Padding ensures each shard occupies its own cache line, so that a
	// write to one shard's mutex doesn't invalidate the cache line holding
	// an adjacent shard's mutex on other cores.
	_ [system.CacheLineSize]byte
}

// activeWorkStateShards partitions goroutine work states across N
// shards, each with its own syncutil.Map. Shard assignment uses
// goroutineID & (N-1).
var activeWorkStateShards [numWorkStateShards]workStateMapShard

// workStateShardIndex returns the shard index for the given goroutine ID.
func workStateShardIndex(gid int64) uint64 {
	return uint64(gid) & (numWorkStateShards - 1)
}

// workStateShard returns the syncutil.Map shard for the given goroutine ID.
func workStateShard(gid int64) *syncutil.Map[int64, WorkState] {
	return &activeWorkStateShards[workStateShardIndex(gid)].m
}

// activeWorkStatesCount tracks the number of goroutines with an
// active work state. It is incremented when a goroutine is first
// registered in activeWorkStates and decremented when it is fully
// removed. Nested (stacked) states do not change the count because
// the map key already exists.
var activeWorkStatesCount atomic.Int64

// maxRetiredWorkStates caps the total retired list to prevent unbounded
// growth during bursts between sampler drain ticks.
const maxRetiredWorkStates = 30000

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
	workStateShard(gid).Store(gid, state)

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
	idx := workStateShardIndex(gid)
	shard := &activeWorkStateShards[idx]
	state, ok := shard.m.Load(gid)
	if !ok {
		return
	}
	prev := state.prev
	// Nil-ing state.prev here would be good hygiene, but is not required:
	// the sampler does not read prev from copied WorkStates, and reclaim
	// zeroes the struct anyway. We skip it to avoid a race with the
	// sampler's concurrent *value copy in rangeWorkStates.
	if prev != nil {
		shard.m.Store(gid, prev)
	} else {
		shard.m.Delete(gid)
		activeWorkStatesCount.Add(-1)
	}
	retireWorkState(shard, state)
}

// retireWorkState adds a WorkState to the shard-local retired list for
// deferred pool return.
func retireWorkState(shard *workStateMapShard, state *WorkState) {
	shard.retired.Lock()
	defer shard.retired.Unlock()
	if len(shard.retired.states) >= maxRetiredPerShard {
		return
	}
	shard.retired.states = append(shard.retired.states, state)
}

// reclaimRetiredWorkStates drains all per-shard retired lists and returns
// WorkState objects to the pool.
func reclaimRetiredWorkStates() {
	for i := range activeWorkStateShards {
		shard := &activeWorkStateShards[i]
		states := func() []*WorkState {
			shard.retired.Lock()
			defer shard.retired.Unlock()
			s := shard.retired.states
			shard.retired.states = nil
			return s
		}()
		for _, s := range states {
			*s = WorkState{}
			workStatePool.Put(s)
		}
	}
}

// totalRetiredWorkStates returns the total number of retired work states
// across all shards. This is only used in tests.
func totalRetiredWorkStates() int {
	total := 0
	for i := range activeWorkStateShards {
		total += func() int {
			shard := &activeWorkStateShards[i]
			shard.retired.Lock()
			defer shard.retired.Unlock()
			return len(shard.retired.states)
		}()
	}
	return total
}

func getWorkState(gid int64) (*WorkState, bool) {
	return workStateShard(gid).Load(gid)
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
	for i := range activeWorkStateShards {
		done := false
		activeWorkStateShards[i].m.Range(func(key int64, value *WorkState) bool {
			if !fn(key, *value) {
				done = true
				return false
			}
			return true
		})
		if done {
			break
		}
	}
	reclaimRetiredWorkStates()
}
