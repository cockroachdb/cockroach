// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// testEngines holds the separated log and state engines used by truncator
// tests.
type testEngines struct {
	Engines
	seq wag.Seq
}

// makeTestEngines creates separated log and state engines. The caller must
// defer Close() to ensure engines are closed before leak checks.
func makeTestEngines() testEngines {
	return testEngines{
		Engines: MakeSeparatedEnginesForTesting(
			storage.NewDefaultInMemForTesting(),
			storage.NewDefaultInMemForTesting(),
		),
	}
}

// writeAppliedState writes the RangeAppliedState for a range to the state
// engine.
func (e *testEngines) writeAppliedState(
	t *testing.T, rangeID roachpb.RangeID, index kvpb.RaftIndex,
) {
	t.Helper()
	as := kvserverpb.RangeAppliedState{RaftAppliedIndex: index}
	keyBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	require.NoError(t, storage.MVCCPutProto(
		context.Background(), e.StateEngine(), keyBuf.RangeAppliedStateKey(),
		hlc.Timestamp{}, &as, storage.MVCCWriteOptions{},
	))
}

// writeReplicaID writes the RaftReplicaID for a range to the state engine.
func (e *testEngines) writeReplicaID(t *testing.T, fullReplicaID roachpb.FullReplicaID) {
	t.Helper()
	rid := kvserverpb.RaftReplicaID{ReplicaID: fullReplicaID.ReplicaID}
	keyBuf := keys.MakeRangeIDPrefixBuf(fullReplicaID.RangeID)
	require.NoError(t, storage.MVCCPutProto(
		context.Background(), e.StateEngine(), keyBuf.RaftReplicaIDKey(),
		hlc.Timestamp{}, &rid, storage.MVCCWriteOptions{},
	))
}

// writeRangeTombstone writes the RangeTombstone for a range to the state
// engine.
func (e *testEngines) writeRangeTombstone(
	t *testing.T, rangeID roachpb.RangeID, nextReplicaID roachpb.ReplicaID,
) {
	t.Helper()
	ts := kvserverpb.RangeTombstone{NextReplicaID: nextReplicaID}
	keyBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	require.NoError(t, storage.MVCCPutProto(
		context.Background(), e.StateEngine(), keyBuf.RangeTombstoneKey(),
		hlc.Timestamp{}, &ts, storage.MVCCWriteOptions{},
	))
}

// writeWAGNode writes a WAG node with the given events to the log engine.
func (e *testEngines) writeWAGNode(t *testing.T, events ...wagpb.Event) {
	t.Helper()
	require.NoError(t, wag.Write(e.LogEngine(), e.seq.Next(), wagpb.Node{Events: events}))
}

// listWAGNodes returns the WAG node indices in the log engine.
func (e *testEngines) listWAGNodes(t *testing.T) []uint64 {
	t.Helper()
	var iter wag.Iterator
	var indices []uint64
	for index := range iter.Iter(context.Background(), e.LogEngine()) {
		indices = append(indices, index)
	}
	require.NoError(t, iter.Error())
	return indices
}

// writeRaftLogEntry writes a dummy raft log entry to the log engine.
func (e *testEngines) writeRaftLogEntry(
	t *testing.T, rangeID roachpb.RangeID, index kvpb.RaftIndex,
) {
	t.Helper()
	keyBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	require.NoError(t, e.LogEngine().PutUnversioned(keyBuf.RaftLogKey(index), []byte("entry")))
}

// truncateWAGNodes repeatedly calls TruncateAppliedWAGNodeAndClearRaftState
// until no more nodes can be deleted.
func (e *testEngines) truncateWAGNodes(t *testing.T, clearSideloaded SideloadClearer) {
	t.Helper()
	require.NoError(t, e.StateEngine().Flush())
	stateReader := e.StateEngine().NewReader(storage.GuaranteedDurability)
	defer stateReader.Close()
	for {
		b := e.LogEngine().NewBatch()
		ok, err := TruncateAppliedWAGNodeAndClearRaftState(context.Background(), WrapRaft(e.LogEngine()), stateReader, clearSideloaded)
		require.NoError(t, err)
		require.NoError(t, b.Commit(false))
		if !ok {
			return
		}
	}
}

// raftLogIndices returns the raft log indices for a given range in ascending
// order.
func (e *testEngines) raftLogIndices(t *testing.T, rangeID roachpb.RangeID) []kvpb.RaftIndex {
	t.Helper()
	keyBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	start := keyBuf.RaftLogPrefix()
	end := start.PrefixEnd()
	iter, err := e.LogEngine().NewEngineIterator(context.Background(), storage.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	require.NoError(t, err)
	defer iter.Close()

	var indices []kvpb.RaftIndex
	ok, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: start})
	require.NoError(t, err)
	for ; ok; ok, err = iter.NextEngineKey() {
		key, err := iter.UnsafeEngineKey()
		require.NoError(t, err)
		index, err := keys.DecodeRaftLogKeyFromSuffix(key.Key[len(start):])
		require.NoError(t, err)
		indices = append(indices, index)
	}
	require.NoError(t, err)
	return indices
}

// MakeSideloadClearer returns a SideloadClearer that truncates sideloaded
// files for a given range using disk-based sideload storage.
func (e *testEngines) makeSideloadClearer(
	st *cluster.Settings, baseDir string, limiter *rate.Limiter, env *fs.Env,
) SideloadClearer {
	return func(ctx context.Context, rangeID roachpb.RangeID, upToIndex kvpb.RaftIndex) error {
		ss := logstore.NewDiskSideloadStorage(st, rangeID, baseDir, limiter, env)
		return ss.TruncateTo(ctx, upToIndex)
	}
}

func TestTruncateApplied(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	r2 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 2}

	for _, tc := range []struct {
		setup           func(t *testing.T, e *testEngines)
		wantWAGIndicies []uint64
	}{
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply})
				e.writeReplicaID(t, r1)
				e.writeAppliedState(t, 1, 9)
			},
			// Raft applied index is 9, no WAG truncation should happen.
			wantWAGIndicies: []uint64{1, 2, 3},
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply})
				e.writeReplicaID(t, r1)
				e.writeAppliedState(t, 1, 10)
			},
			// Raft applied index is 10, only the first WAG node should be truncated.
			wantWAGIndicies: []uint64{2, 3},
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 15), Type: wagpb.EventInit})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventApply})
				e.writeReplicaID(t, r1)
				e.writeAppliedState(t, 1, 20)
			},
			// Raft applied index is 20, all WAG nodes should be truncated.
			wantWAGIndicies: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventDestroy})
				e.writeRangeTombstone(t, r1.RangeID, r1.ReplicaID+1)
				e.writeAppliedState(t, 1, 0)
			},
			// A range tombstone confirms destruction, WAG nodes to that replica
			// should be truncated.
			wantWAGIndicies: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventSubsume})
				e.writeRangeTombstone(t, r1.RangeID, r1.ReplicaID+1)
				e.writeAppliedState(t, 1, 0)
			},
			// A range tombstone confirms subsumption, WAG nodes to that replica
			// should be truncated.
			wantWAGIndicies: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventDestroy})
				e.writeReplicaID(t, r2)
				e.writeAppliedState(t, 1, 20)
			},
			// A newer replica supersedes the old one, WAG nodes for the old
			// replica should be truncated.
			wantWAGIndicies: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventSubsume})
				e.writeReplicaID(t, r2)
				e.writeAppliedState(t, 1, 20)
			},
			// A newer replica supersedes the subsumed one, WAG nodes for the old
			// replica should be truncated.
			wantWAGIndicies: nil,
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventDestroy})
				e.writeReplicaID(t, r1)
				// EventDestroy won't be deleted if the replica id still matches the
				// WAG node's replica ID because it means that the event hasn't applied
				// yet. In this case, we won't even read the applied index.
				e.writeAppliedState(t, 1, 30)
			},
			wantWAGIndicies: []uint64{2},
		},
		{
			setup: func(t *testing.T, e *testEngines) {
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate})
				e.writeWAGNode(t, wagpb.Event{Addr: wagpb.MakeAddr(r1, 20), Type: wagpb.EventSubsume})
				e.writeReplicaID(t, r1)
				// EventSubsume won't be deleted if the replica id still matches the
				// WAG node's replica ID because it means that the event hasn't applied
				// yet. In this case, we won't even read the applied index.
				e.writeAppliedState(t, 1, 30)
			},
			wantWAGIndicies: []uint64{2},
		},
	} {
		t.Run("", func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			tc.setup(t, &e)
			e.truncateWAGNodes(t, nil /* clearSideloaded */)
			require.Equal(t, tc.wantWAGIndicies, e.listWAGNodes(t))
		})
	}
}

// TestTruncateAndClearRaftState verifies that
// TruncateAppliedWAGNodeAndClearRaftState only clears raft log entries and
// sideloaded files up to the destroyed/subsumed replica's last index. Entries
// and files beyond that index may belong to a newer replica and must be
// preserved.
func TestTruncateAndClearRaftState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	r1 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 1}
	r2 := roachpb.FullReplicaID{RangeID: 1, ReplicaID: 2}

	for _, eventType := range []wagpb.EventType{wagpb.EventDestroy, wagpb.EventSubsume} {
		t.Run(eventType.String(), func(t *testing.T) {
			e := makeTestEngines()
			defer e.Close()
			ctx := context.Background()

			// Write WAG nodes: create then destroy/subsume at index 20.
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 10), Type: wagpb.EventCreate,
			})
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r1, 20), Type: eventType,
			})

			// Create a WAG node for a newer replica for the same range.
			e.writeWAGNode(t, wagpb.Event{
				Addr: wagpb.MakeAddr(r2, 21), Type: wagpb.EventCreate,
			})

			// Tombstone confirms destruction/subsumption.
			e.writeRangeTombstone(t, 1 /* rangeID */, 2 /* nextReplicaID */)

			// Raft log entries at indices 10-25. (Indices at or below lastIndex=20)
			// belong to the old replica. Entries at 21-25 (above lastIndex=20)
			// belong to a newer replica reusing the same RangeID.
			for idx := 10; idx <= 25; idx++ {
				e.writeRaftLogEntry(t, 1 /* rangeID */, kvpb.RaftIndex(idx))
			}

			// Create sideloaded files using the log engine's VFS.
			st := cluster.MakeTestingClusterSettings()
			baseDir := e.LogEngine().GetAuxiliaryDir()
			limiter := rate.NewLimiter(rate.Inf, math.MaxInt64)
			env := e.LogEngine().Env()
			ss := logstore.NewDiskSideloadStorage(st, 1, baseDir, limiter, env)
			for _, idx := range []kvpb.RaftIndex{10, 15, 20, 21, 25} {
				require.NoError(t, ss.Put(ctx, idx, 1 /* term */, []byte("sst-data")))
			}
			clearer := e.makeSideloadClearer(st, baseDir, limiter, env)

			e.truncateWAGNodes(t, clearer)
			// Raft log entries 10-20 (at or below lastIndex=20) must be deleted. The
			// rest shouldn't be deleted by the WAG truncator.
			require.Equal(t,
				[]kvpb.RaftIndex{21, 22, 23, 24, 25},
				e.raftLogIndices(t, 1 /* rangeID */),
			)
			// Sideloaded files at indices 10, 15, and 20 (at or below lastIndex=20)
			// must be deleted.
			for _, idx := range []kvpb.RaftIndex{10, 15, 20} {
				_, err := ss.Get(ctx, idx, 1)
				require.Errorf(t, err, "sideloaded file at index %d should have been deleted", idx)
			}
			// Sideloaded files at indices 21 and 25 (above lastIndex=20) must be
			// preserved.
			for _, idx := range []kvpb.RaftIndex{21, 25} {
				data, err := ss.Get(ctx, idx, 1)
				require.NoErrorf(t, err, "sideloaded file at index %d should be preserved", idx)
				require.Equal(t, []byte("sst-data"), data)
			}
			// All WAG nodes must be deleted.
			require.Equal(t, []uint64{3}, e.listWAGNodes(t))
		})
	}
}
