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
// contention on the per-shard mutexes when many goroutines call
// SetWorkState concurrently on high-core machines.
//
// 32 shards keeps average contention at ~2 cores per shard on
// 64-vCPU machines. On low-core machines the overhead is negligible:
// shard selection is a single bitwise AND, each empty shard costs only
// the map header, and rangeWorkStates iterates all shards just once
// per second.
const numWorkStateShards = 32

// Compile-time assertion: numWorkStateShards must be a power of two.
const _ = -uint(numWorkStateShards & (numWorkStateShards - 1))

// maxRetiredPerShard caps each shard's retired list independently.
const maxRetiredPerShard = maxRetiredWorkStates / numWorkStateShards

// workStateMapShard is a single shard of the work state map. Each
// shard uses a plain map protected by a mutex rather than syncutil.Map,
// because ASH's access pattern (unique, never-reused goroutine IDs as
// keys) hits the worst case for the synchronized map's read/dirty
// split: every Store inserts a new key, forcing the slow path. A plain
// mutex+map avoids the dirty map rebuild cycle triggered by Range().
type workStateMapShard struct {
	syncutil.Mutex
	m map[int64]*WorkState

	// retired holds WorkState objects that have been removed from
	// activeWorkStates but cannot yet be returned to the pool. This is because
	// rangeWorkStates may be dereferencing or copying a WorkState object.
	// clearWorkState appends to this list; rangeWorkStates drains it after
	// iteration completes via reclaimRetiredWorkStates.
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
// shards, each with its own mutex-protected map. Shard assignment uses
// goroutineID & (N-1).
var activeWorkStateShards [numWorkStateShards]workStateMapShard

func init() {
	for i := range activeWorkStateShards {
		activeWorkStateShards[i].m = make(map[int64]*WorkState)
	}
}

// workStateShard returns the shard for the given goroutine ID.
func workStateShard(gid int64) *workStateMapShard {
	return &activeWorkStateShards[uint64(gid)&(numWorkStateShards-1)]
}

// activeWorkStatesCount tracks the number of goroutines with an
// active work state. It is incremented when a goroutine is first
// registered in activeWorkStates and decremented when it is fully
// removed. Nested (stacked) states do not change the count because
// the map key already exists.
var activeWorkStatesCount atomic.Int64

// maxRetiredWorkStates caps the total retired list to prevent unbounded
// growth during bursts between sampler drain ticks.
// This value is based on an estimated ceiling of 10,000 QPS per node with
// a 3x multiplier for goroutines per query. States beyond this cap are not
// tracked for pooling and are instead garbage-collected.
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

	pushWorkState(workStateShard(gid), gid, state)

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

// pushWorkState stores state into the shard's map and sets the prev pointer
// appropriately.
func pushWorkState(shard *workStateMapShard, gid int64, state *WorkState) {
	shard.Lock()
	defer shard.Unlock()
	if prev, ok := shard.m[gid]; ok {
		state.prev = prev
	} else {
		state.prev = nil
		activeWorkStatesCount.Add(1)
	}
	shard.m[gid] = state
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
	shard := workStateShard(gid)
	if state := popWorkState(shard, gid); state != nil {
		retireWorkState(shard, state)
	}
}

// popWorkState removes and returns the current work state for gid,
// restoring any previous state. Returns nil if no state exists.
func popWorkState(shard *workStateMapShard, gid int64) *WorkState {
	shard.Lock()
	defer shard.Unlock()
	state, ok := shard.m[gid]
	if !ok {
		return nil
	}
	prev := state.prev
	state.prev = nil
	if prev != nil {
		shard.m[gid] = prev
	} else {
		delete(shard.m, gid)
		activeWorkStatesCount.Add(-1)
	}
	return state
}

// retireWorkState adds a WorkState to the shard-local retired list for
// deferred pool return. If the retired list has reached maxRetiredPerShard,
// the state is not tracked for pooling and will be garbage-collected instead.
func retireWorkState(shard *workStateMapShard, state *WorkState) {
	shard.retired.Lock()
	defer shard.retired.Unlock()
	if len(shard.retired.states) >= maxRetiredPerShard {
		return
	}
	shard.retired.states = append(shard.retired.states, state)
}

// reclaimRetiredWorkStates drains all per-shard retired lists and returns
// retired WorkState objects to the pool.
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
	shard := workStateShard(gid)
	shard.Lock()
	defer shard.Unlock()
	state, ok := shard.m[gid]
	return state, ok
}

// workStateSnapshot holds a goroutine ID and a copied WorkState value
// for iteration outside the shard lock.
type workStateSnapshot struct {
	gid   int64
	state WorkState
}

// rangeWorkStates iterates over all registered work states and then
// reclaims any retired WorkState objects back to the pool. The reclaim
// step is folded into this function because the retired list exists
// solely to defer pool returns while iteration is in progress — once
// iteration completes, it is always safe to reclaim.
//
// Each shard's entries are snapshotted (value-copied) under the shard
// lock, then the callback runs outside the lock. This keeps the lock
// hold time proportional to the shard size (brief) rather than to
// the callback's execution time, avoiding interference with concurrent
// SetWorkState/clearWorkState calls from worker goroutines.
func rangeWorkStates(fn func(gid int64, state WorkState) bool) {
	var snapshot []workStateSnapshot
	for i := range activeWorkStateShards {
		snapshot = snapshotShard(&activeWorkStateShards[i], snapshot)

		done := false
		for _, entry := range snapshot {
			if !fn(entry.gid, entry.state) {
				done = true
				break
			}
		}
		if done {
			break
		}
	}
	reclaimRetiredWorkStates()
}

// snapshotShard copies all entries from a shard into buf under the
// shard lock. The returned slice reuses buf's backing array when
// possible to reduce allocations across shards.
func snapshotShard(shard *workStateMapShard, buf []workStateSnapshot) []workStateSnapshot {
	shard.Lock()
	defer shard.Unlock()
	if cap(buf) < len(shard.m) {
		buf = make([]workStateSnapshot, 0, len(shard.m))
	} else {
		buf = buf[:0]
	}
	for gid, state := range shard.m {
		buf = append(buf, workStateSnapshot{gid: gid, state: *state})
	}
	return buf
}
