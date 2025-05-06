// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestReplicaLogStorage is a randomized stress test for the log storage type.
//
// It generates a typical raft log workload which includes log appends with or
// without overlapping, and truncations. The operations are applied to both
// replicaLogStorage and the reference raft.LogStorage implementation
// (raft.MemoryStorage), and the test makes sure the observed state matches
// between the two.
//
// This exercises the whole raft log storage stack, which includes Pebble,
// sideloaded storage, and the entry cache. It also exercises the use of the two
// mutexes, and helps to find concurrency issues. The test also self-documents
// the "canonical" way of using the replicaLogStorage.
func TestReplicaLogStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := newReplicaLogStorageTest(t)
	rt.start(ctx)
	defer rt.stop(ctx)

	seed := rand.Uint64()
	t.Logf("seed: %d", seed)
	rng := newRNG(seed)

	// Start separate threads for truncations, and reads under mu. Use stable seed
	// for each thread.
	// NB: stable seeds do not achieve reproducibility here, but we try hard. Note
	// that at least the append thread below is reproducible.
	for i := 0; i < 5; i++ {
		fork := newRNG(rng.Uint64())
		// TODO(pav-kv): add a raftMu-holding log reader too.
		require.NoError(t, rt.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{TaskName: "read"},
			func(ctx context.Context) { rt.readLoop(ctx, fork) }))
	}
	{
		fork := newRNG(rng.Uint64())
		require.NoError(t, rt.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{TaskName: "trunc"},
			func(ctx context.Context) { rt.truncateLoop(ctx, fork) }))
	}
	// Run the append loop synchronously, and terminate all other tasks when done.
	rt.writeLoop(ctx, rng, 1000)
	cancel()
}

type replicaLogStorageTest struct {
	t *testing.T

	stopper *stop.Stopper

	ls *replicaLogStorage
	mu struct {
		syncutil.RWMutex
		committed uint64
		readable  uint64
	}
	raftMu syncutil.Mutex

	term uint64
	ms   *raft.MemoryStorage
}

func newReplicaLogStorageTest(t *testing.T) *replicaLogStorageTest {
	const rangeID = 10
	rt := &replicaLogStorageTest{
		t:       t,
		stopper: stop.NewStopper(),
		term:    1,
		ms:      raft.NewMemoryStorage(),
	}

	st := cluster.MakeTestingClusterSettings()
	eng := storage.NewDefaultInMemForTesting()
	sideloaded := logstore.NewDiskSideloadStorage(st, rangeID,
		eng.GetAuxiliaryDir(), nil /* limiter: unused */, eng)

	rt.ls = &replicaLogStorage{
		ctx:     context.Background(),
		cache:   raftentry.NewCache(1 << 5), // stress eviction
		onSync:  noopSyncCallback{},
		metrics: newStoreMetrics(metric.TestSampleInterval),
		ls: &logstore.LogStore{
			RangeID:     rangeID,
			Engine:      eng,
			Sideload:    sideloaded,
			StateLoader: logstore.NewStateLoader(rangeID),
			SyncWaiter:  logstore.NewSyncWaiterLoop(),
			Settings:    st,
		},
	}
	rt.ls.mu.RWMutex = &rt.mu.RWMutex
	rt.ls.raftMu.Mutex = &rt.raftMu
	return rt
}

func (rt *replicaLogStorageTest) start(ctx context.Context) {
	rt.ls.ls.SyncWaiter.Start(ctx, rt.stopper)
}

func (rt *replicaLogStorageTest) stop(ctx context.Context) {
	rt.stopper.Stop(ctx)
	rt.ls.ls.Engine.Close()

	cache := rt.ls.cache.Metrics()
	rt.t.Logf("cache loads: %d", cache.Accesses.Count())
	rt.t.Logf("cache hits: %d", cache.Hits.Count())
	rt.t.Logf("read bytes: %d", rt.ls.metrics.RaftStorageReadBytes.Count())

	rt.mu.RLock()
	defer rt.mu.RUnlock()
	require.Equal(rt.t, rt.ms.LastIndex(), rt.mu.readable)
	rt.t.Logf("term=%d, log=(%d, %d], committed=%d",
		rt.term, rt.ms.Compacted(), rt.ms.LastIndex(), rt.mu.committed)
}

func (rt *replicaLogStorageTest) append(ctx context.Context, app raft.StorageAppend) {
	// While a write is in flight, raft will never attempt to read the affected
	// log indices. In reality, there can be multiple writes in flight, and raft
	// tracks the lowest unreadable index (the beginning of the "unstable" log).
	// We don't do multiple in-flight writes here for simplicity, and also to
	// increase the "readable" surface and test reads harder.
	rt.mu.Lock()
	if entries := app.Entries; len(entries) > 0 {
		rt.mu.readable = entries[0].Index - 1
	}
	rt.mu.committed = max(rt.mu.committed, app.HardState.Commit)
	rt.mu.Unlock()
	// Do the log storage write.
	rt.raftMu.Lock()
	defer rt.raftMu.Unlock()
	var stats logstore.AppendStats
	state, err := rt.ls.appendRaftMuLocked(ctx, app, &stats)
	require.NoError(rt.t, err)
	// Mirror the write to the in-memory storage. Note that it has its own mutex.
	// From this point on, the write is visible in both storages.
	rt.ms.Mutex.Lock()
	if !raft.IsEmptyHardState(app.HardState) {
		rt.ms.SetHardStateLocked(app.HardState)
	}
	err = rt.ms.AppendLocked(app.Entries)
	rt.ms.Mutex.Unlock()
	require.NoError(rt.t, err)
	// Finally, update the in-memory state.
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ls.updateStateRaftMuLockedMuLocked(state)
	if ln := len(app.Entries); ln > 0 {
		rt.mu.readable = app.Entries[ln-1].Index
	}
}

func (rt *replicaLogStorageTest) truncate(
	ctx context.Context, prev, next kvserverpb.RaftTruncatedState,
) {
	if next.Index == prev.Index {
		return
	}
	// Prepare the batch.
	b := rt.ls.ls.Engine.NewWriteBatch()
	defer b.Close()
	// Update the in-memory state ahead of the truncation, so that these entries
	// can no longer be read.
	rt.raftMu.Lock()
	defer rt.raftMu.Unlock()
	require.NoError(rt.t, handleTruncatedStateBelowRaftPreApply(
		ctx, prev, next, rt.ls.ls.StateLoader, b))
	rt.ls.stagePendingTruncationRaftMuLocked(pendingTruncation{
		RaftTruncatedState: next,
		expectedFirstIndex: prev.Index + 1,
	})
	// Write the truncation to both storages.
	require.NoError(rt.t, b.Commit(false /* sync */))
	require.NoError(rt.t, rt.ms.Compact(uint64(next.Index)))
}

func (rt *replicaLogStorageTest) read(rng *rand.Rand) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	// The readable interval is (compacted, readable].
	prev, last := uint64(rt.ls.shMu.trunc.Index), rt.mu.readable
	if last <= prev { // all compacted
		return
	}
	// Bias reads to the left or right end where writes are hot.
	if count := rng.Uint64N(last - prev); rng.IntN(2) == 0 {
		prev += count
	} else {
		last -= count
	}
	// The moment of truth.
	ref, err := rt.ms.Entries(prev+1, last+1, math.MaxUint64)
	require.NoErrorf(rt.t, err, "(%d,%d]", prev, last)
	entries, err := rt.ls.Entries(prev+1, last+1, math.MaxUint64)
	require.NoErrorf(rt.t, err, "(%d,%d]", prev, last)
	require.Equalf(rt.t, ref, entries, "(%d,%d]", prev, last)
}

func (rt *replicaLogStorageTest) termAt(index uint64) uint64 {
	ref, err := rt.ms.Term(index)
	require.NoError(rt.t, err)
	term, err := rt.ls.Term(index)
	require.NoError(rt.t, err)
	require.Equal(rt.t, ref, term)
	return term
}

func (rt *replicaLogStorageTest) genAppend(rng *rand.Rand) raft.StorageAppend {
	index := rt.ms.LastIndex()
	rt.mu.RLock()
	committed := rt.mu.committed
	rt.mu.RUnlock()
	// Force sync by making Responses non-empty. Raft populates it with messages
	// contingent on durability of this write. See StorageAppend.MustSync().
	app := raft.StorageAppend{Responses: []raftpb.Message{{}}}
	require.True(rt.t, app.MustSync())

	// Usually, raft appends entries at the end of the log. It may append with an
	// overlap when there is a new leader. In this case, it can only append after
	// some entry in the [committed, last] interval.
	const pTermBump = 10
	if termBump := rng.IntN(100) < pTermBump; termBump {
		// NB: the committed index is >= compacted index, so the entry at the given
		// index exists in the log.
		index = committed + rng.Uint64N(index-committed+1)
		rt.term++
		app.HardState.Term = rt.term // update HardState
	}
	app.LeadTerm = rt.term
	app.HardState.Commit = committed + rng.Uint64N(index-committed+1) // commit some

	entries := make([]raftpb.Entry, rng.IntN(10))
	for i := range entries {
		entries[i] = raftpb.Entry{Index: index + 1 + uint64(i), Term: rt.term}
	}
	app.Entries = entries
	return app
}

func (rt *replicaLogStorageTest) genTruncate(
	rng *rand.Rand,
) (prev, next kvserverpb.RaftTruncatedState) {
	// We can only truncate entries <= committed index.
	rt.mu.RLock()
	committed := rt.mu.committed
	comp := uint64(rt.ls.shMu.trunc.Index)
	rt.mu.RUnlock()

	require.Equal(rt.t, rt.ms.Compacted(), comp)
	index := comp + rng.Uint64N(committed-comp+1)

	return kvserverpb.RaftTruncatedState{
			Index: kvpb.RaftIndex(comp),
			Term:  kvpb.RaftTerm(rt.termAt(comp)),
		}, kvserverpb.RaftTruncatedState{
			Index: kvpb.RaftIndex(index),
			Term:  kvpb.RaftTerm(rt.termAt(index)),
		}
}

func (rt *replicaLogStorageTest) writeLoop(ctx context.Context, rng *rand.Rand, iterations int) {
	for range iterations {
		rt.append(ctx, rt.genAppend(rng))
	}
}

func (rt *replicaLogStorageTest) truncateLoop(ctx context.Context, rng *rand.Rand) {
	for {
		prev, next := rt.genTruncate(rng)
		rt.truncate(ctx, prev, next)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (rt *replicaLogStorageTest) readLoop(ctx context.Context, rng *rand.Rand) {
	for {
		rt.read(rng)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

type noopSyncCallback struct{}

func (noopSyncCallback) OnLogSync(context.Context, raft.StorageAppendAck, logstore.WriteStats) {
}

func newRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(1237, seed))
}
