// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// verifySnap verifies that the replica descriptor before and after the
// snapshot, and the subsumed replicas, represent a possible Store transition:
//
//  1. The StartKey of the replica does not change.
//  2. Before the snapshot, all replicas don't overlap (the pre-snapshot
//     replica, if initialized, and the subsumed replicas, if any).
//  3. The snapshot overlaps all the subsumed replicas (by definition).
//
// The returned value is the end key of the "user keyspace" that the snapshot
// clears as a side effect (note that the pre-snapshot replica or the last
// subsumed replica may be wider than the snapshot). If the cleared space is not
// wider than desc.RSpan(), then nil is returned.
//
// Consider the cases below on possible transitions, and see TestVerifySnap.
//
// Initial snapshot, zero or more subsumed replicas within its span (c <= d):
//
//	   snap: [a-----------)d
//	subsume:   ...[b--)c
//	  clear: none
//
// Initial snapshot, the last subsumed replica extends beyond its span (c < d):
//
//	   snap: [a-------)c
//	subsume:   ...[b-------)d
//	  clear:          [c---)d
//
// The replica is not expanding (b <= c). There can't be subsumed replicas:
//
//	 snap: [a---)b
//	 prev: [a--------)c
//	clear:      [b---)c  // none, if b == c (common case)
//
// The replica is expanding (b < e), all subsumed replicas are inside (d <= e):
//
//	   snap: [a----------------)e
//	   prev: [a---)b
//	subsume:       ...[c---)d
//	  clear: none
//
// The replica is expanding (b < d), one subsumed replica sticks out (d < e):
//
//	   snap: [a-----------)d
//	   prev: [a---)b
//	subsume:       ...[c-------)e
//	  clear:              [d---)e
//
// The supplied list of subsumed replicas must be sorted by key, for convenience
// of the above checks.
func verifySnap(
	prev, desc *roachpb.RangeDescriptor, subsume []kvstorage.DestroyReplicaInfo,
) (roachpb.RKey, error) {
	// A snapshot never changes the start key, because splits and merges (the only
	// operations changing the key bounds) always leave it intact.
	if prev.IsInitialized() && !prev.StartKey.Equal(desc.StartKey) {
		return nil, errors.Errorf("start key changed: %v -> %v", prev.StartKey, desc.StartKey)
	}
	// By Store invariants, all subsumed replicas do not overlap each other and
	// the pre-snapshot replica. For efficiently checking this property, we
	// require them ordered by key.
	//
	// There can't be a subsumed replica with any key <= desc.StartKey, so the
	// pre-snapshot replica (if initialized) is the first in the ordered list.
	//
	// The subsumed replicas are initialized, so we assume Key < EndKey for them
	// without checking. Only prev can be uninitialized, which is correctly
	// handled because its EndKey=nil does not compare higher than other keys.
	last := prev.RSpan()
	for i := range subsume {
		next := subsume[i].Keys
		if last.EndKey.Compare(next.Key) > 0 {
			return nil, errors.AssertionFailedf("replicas unordered/overlap: %v %v", last, next)
		}
		last = next
	}
	// Now check that all subsumed replicas overlap the snapshot.
	span := desc.RSpan()
	if ln := len(subsume); ln > 0 {
		// A snapshot never subsumes a replica to its left, because such replicas
		// have EndKey <= span.Key, so don't overlap the snapshot. The start key
		// also can't match ours because the replica can't subsume itself.
		if first := subsume[0].Keys; first.Key.Compare(span.Key) <= 0 {
			return nil, errors.AssertionFailedf("subsumed replica to the left from snapshot: %v %v", first, span)
		}
		// The rightmost subsumed replica must start strictly inside the snapshot.
		if last.Key.Compare(span.EndKey) >= 0 {
			return nil, errors.AssertionFailedf("subsumed replica does not overlap snapshot: %v %v", last, span)
		}
	}
	// Find out whether the key space that the snapshot overrides is wider than
	// the snapshot, i.e. max(prev.EndKey, subsumed[].EndKey) > span.EndKey.
	if last.EndKey.Compare(span.EndKey) > 0 {
		return last.EndKey, nil
	}
	return nil, nil
}

// snapWriteBuilder contains the data needed to prepare the on-disk state for a
// snapshot.
//
// TODO(pav-kv): move this struct to kvstorage package.
type snapWriteBuilder struct {
	todoEng  storage.Engine
	sl       kvstorage.StateLoader
	writeSST func(context.Context, func(context.Context, storage.Writer) error) error

	truncState kvserverpb.RaftTruncatedState
	hardState  raftpb.HardState
	desc       *roachpb.RangeDescriptor // corresponds to the range descriptor in the snapshot
	origDesc   *roachpb.RangeDescriptor // pre-snapshot range descriptor
	// NB: subsume must be in sorted order by DestroyReplicaInfo start key.
	subsume []kvstorage.DestroyReplicaInfo

	clearEnd roachpb.RKey
}

// prepareSnapApply prepares the storage write that represents applying a
// snapshot to a replica. The replicated keyspace of the snapshot is already
// prepared, and this method extends it with unreplicated/raft keyspace tweaks,
// as well as destroying the subsumed replicas if any.
//
// The pre-snapshot replica can be initialized or uninitialized, and will become
// initialized after this write is applied. A snapshot catches up the replica,
// so multiple splits and merges could have happened since its pre-snapshot
// state. In general, its EndKey can change (replica shrunk or expanded), and in
// the expand case the snapshot can overlap other ranges' replicas on our Store.
//
// In order to maintain the Store invariant (all replicas don't overlap), the
// overlapping replicas are destroyed ("subsumed") by this write. This is a
// valid transition because the overlap signifies that these ranges were merged
// into ours at some point.
//
// See verifySnap for more details on the possible combinations of the snapshot,
// pre-snapshot replica, and subsumed replicas. Consider the most general case:
//
//	[----- snapshot -----)[        )
//	[----- replica & subsumed -----)
//
// If the already written keyspace of the snapshot is narrower than the entire
// overridden keyspace (the pre-snapshot replica and the subsumed replicas), we
// additionally clear the remainder. This includes the MVCC, lock table and
// system keyspace derived from the "user keys" span.
//
// The constructed write is either a Pebble batch or ingestion (depending on the
// size and other factors). Ingesting is more restrictive, so it shapes the code
// choices here: the writes are decomposed into non-overlapping SSTables, and
// each one is written in key order. We also avoid overlapping with the already
// written replicated keys of the snapshot.
func (s *snapWriteBuilder) prepareSnapApply(ctx context.Context) error {
	clearEnd, err := verifySnap(s.origDesc, s.desc, s.subsume)
	if err != nil {
		log.KvDistribution.Fatalf(ctx, "%v", err)
		return err
	}
	s.clearEnd = clearEnd

	// TODO(pav-kv): assert that our replica already exists in storage. Note that
	// it can be either uninitialized or initialized.
	_ = applySnapshotTODO // 1.1 + 1.3 + 2.4 + 3.1
	// TODO(sep-raft-log): rewriteRaftState now only touches raft engine keys, so
	// it will be convenient to redirect it to a raft engine batch.
	if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
		return kvstorage.RewriteRaftState(ctx, kvstorage.RaftWO(w), s.sl, s.hardState, s.truncState)
	}); err != nil {
		return err
	}
	_ = applySnapshotTODO // 1.2 + 2.1 + 2.2 + 2.3 (diff) + 3.2
	if err := s.clearSubsumedReplicaDiskData(ctx); err != nil {
		return err
	}

	_ = applySnapshotTODO // 2.3 (split)
	return s.clearResidualDataOnNarrowSnapshot(ctx)
}

// clearSubsumedReplicaDiskData clears the on disk data of the subsumed
// replicas by creating SSTs with range deletion tombstones. We have to be
// careful here not to have overlapping ranges with the SSTs we have already
// created since that will throw an error while we are ingesting them. This
// method requires that each of the subsumed replicas raftMu is held, and that
// the Reader reflects the latest I/O each of the subsumed replicas has done
// (i.e. Reader was instantiated after all raftMu were acquired).
//
// NB: does nothing if there are no subsumed replicas.
func (s *snapWriteBuilder) clearSubsumedReplicaDiskData(ctx context.Context) error {
	// In the common case, the subsumed replicas' end key does not extend beyond
	// the snapshot end key (sn <= b):
	//
	//	   subsumed: [a---s1---...---sn)
	//	   snapshot: [a---------------b)
	//	or snapshot: [a-------------------b)
	//
	// In this case, we do not need to clear the range-local, lock table, and user
	// keys owned by subsumed replicas, since clearing [a, b) will cover it. We
	// only need to clear the per-replica RangeID-local key spans here.
	//
	// This leaves the only other case, where the subsumed replicas extend past
	// the snapshot's end key (s[n-1] < b < sn):
	//
	//	subsumed: [a---s1---...---s[n-1]---sn)
	//	snapshot: [a--------------------b)
	//
	// This is only possible if we're not only learning about merges through the
	// snapshot, but also a split -- that's the only way the bounds of the
	// snapshot can be narrower than the bounds of all the subsumed replicas. In
	// this case, we do need to clear range-local, lock table, and user keys in
	// the span [b, sn). We do this in
	// clearResidualDataOnNarrowSnapshot, not here.

	// TODO(sep-raft-log): need different readers for raft and state engine.
	reader := storage.Reader(s.todoEng)
	for _, sub := range s.subsume {
		// We have to create an SST for the subsumed replica's range-id local keys.
		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			return kvstorage.SubsumeReplica(ctx, kvstorage.TODOReaderWriter(reader, w), sub)
		}); err != nil {
			return err
		}
	}
	return nil
}

// clearResidualDataOnNarrowSnapshot clears the overlapping replicas' data not
// covered by the snapshot. Specifically, the data between the snapshot's end
// key and that of the keyspace covered by s.origDesc + s.subsume.descs, if the
// former is lower (i.e. the snapshot is "narrower"). Note that the start keys
// of the snapshot and s.origDesc[^1] match, so the residual data may only exist
// between the end keys. clearResidualDataOnNarrowSnapshot is a no-op if there
// is no narrowing business to speak of.
//
// Visually, the picture looks as follows:
//
// The simplest case is when the snapshot isn't subsuming any replicas.
// original descriptor: [a-----------------------------c)
// snapshot descriptor: [a---------------------b)
// cleared: [b, c)
//
// In the more general case[^2]:
//
// store descriptors:   [a----------------s1---...---sn)
// snapshot descriptor: [a---------------------b)
// cleared: [b, sn)
//
// Practically speaking, the simple case above corresponds to a replica learning
// about a split through a snapshot. The more general case corresponds to a
// replica learning about a series of merges and at least one split through the
// snapshot.
//
// [1] Assuming s.origDesc is initialized. If it isn't, and we're applying an
// initial snapshot, the snapshot may still be narrower than the end key of the
// right-most subsumed replica.
//
// [2] In the diagram , S1...Sn correspond to subsumed replicas with end keys
// s1...sn respectively. These are all replicas on the store that overlap with
// the snapshot descriptor, covering the range [a,b), and the right-most
// descriptor is that of replica Sn.
func (s *snapWriteBuilder) clearResidualDataOnNarrowSnapshot(ctx context.Context) error {
	// Return early if the snapshot does not narrow the keyspace.
	if s.clearEnd == nil {
		return nil
	}
	// TODO(sep-raft-log): read from the state machine engine here.
	reader := storage.Reader(s.todoEng)
	for _, span := range rditer.Select(0, rditer.SelectOpts{
		Ranged: rditer.SelectRangedOptions{
			RSpan:      roachpb.RSpan{Key: s.desc.EndKey, EndKey: s.clearEnd},
			SystemKeys: true,
			LockTable:  true,
			UserKeys:   true,
		},
	}) {
		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			return storage.ClearRangeWithHeuristic(
				ctx, reader, w, span.Key, span.EndKey, kvstorage.ClearRangeThresholdPointKeys(),
			)
		}); err != nil {
			return err
		}
	}
	return nil
}
