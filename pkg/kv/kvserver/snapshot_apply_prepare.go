// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
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

// snapWriter helps preparing the snapshot write to storage.
// TODO(pav-kv): move this and other structs in this file to kvstorage package.
type snapWriter struct {
	todoEng  storage.Engine
	writeSST func(context.Context, func(context.Context, storage.Writer) error) error
}

// snapWrite contains the data needed to prepare a snapshot write to storage.
type snapWrite struct {
	sl         logstore.StateLoader
	truncState kvserverpb.RaftTruncatedState
	hardState  raftpb.HardState
	desc       *roachpb.RangeDescriptor // corresponds to the range descriptor in the snapshot
	origDesc   *roachpb.RangeDescriptor // pre-snapshot range descriptor
	// NB: subsume must be in sorted order by DestroyReplicaInfo start key.
	subsume []kvstorage.DestroyReplicaInfo
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
func (s *snapWriter) prepareSnapApply(ctx context.Context, sw snapWrite) error {
	clearEnd, err := verifySnap(sw.origDesc, sw.desc, sw.subsume)
	if err != nil {
		log.KvDistribution.Fatalf(ctx, "%v", err)
		return err
	}
	_ = applySnapshotTODO

	// TODO(pav-kv): assert that our replica already exists in storage. Note that
	// it can be either uninitialized or initialized.
	// TODO(sep-raft-log): RewriteRaftState now only touches raft engine keys, so
	// it will be convenient to redirect it to a raft engine batch.
	if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
		return kvstorage.RewriteRaftState(ctx, kvstorage.RaftWO(w), sw.sl, sw.hardState, sw.truncState)
	}); err != nil {
		return err
	}

	// TODO(sep-raft-log): need different readers for raft and state engine.
	reader := storage.Reader(s.todoEng)
	for _, sub := range sw.subsume {
		// We have to create an SST for the subsumed replica's range-id local keys.
		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			return kvstorage.SubsumeReplica(ctx, kvstorage.TODOReaderWriter(reader, w), sub)
		}); err != nil {
			return err
		}
	}

	// If the snapshot is narrower than the replica+subsumed keyspace that it
	// overrides, clear the remainder.
	if clearEnd == nil {
		return nil
	}
	for _, span := range rditer.Select(0, rditer.SelectOpts{
		Ranged: rditer.SelectRangedOptions{
			RSpan:      roachpb.RSpan{Key: sw.desc.EndKey, EndKey: clearEnd},
			SystemKeys: true,
			LockTable:  true,
			UserKeys:   true,
		},
	}) {
		if err := s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
			// TODO(sep-raft-log): read from the state machine engine here.
			return storage.ClearRangeWithHeuristic(
				ctx, reader, w, span.Key, span.EndKey, kvstorage.ClearRangeThresholdPointKeys(),
			)
		}); err != nil {
			return err
		}
	}
	return nil
}
