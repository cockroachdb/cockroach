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
	"github.com/cockroachdb/pebble"
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
	// eng encapsulates the storage engines of the Store. It knows whether the
	// raft/log and state engines are separated, or the same engine must be used.
	eng kvstorage.Engines
	// batch is the pending write batch when the snapshot is applied using a batch
	// rather than ingestion, or nil if the snapshot is applied using ingestion.
	//
	// The batch is scoped only to the state machine engine when engines are
	// separated. Under a single engine, the batch contains the entire mutation
	// spanning both logical engines.
	batch storage.WriteBatch
	// raftWO is the pending write batch into the raft engine. Not nil iff
	// separated engines are enabled. If nil, the raft state is written into the
	// combined engine using writeSST.
	raftWO storage.WriteBatch
	// writeSST provides a Writer which the caller uses to populate a subset of
	// the pending snapshot write. One writeSST call corresponds to one keys
	// subspace (such as replicated RangeID-local) and/or one range.
	//
	// Writes within one writeSST call are sorted by key. Distinct writeSST calls
	// write into non-overlapping spans. Both requirements are dictated by the
	// common case in which the snapshot is applied as a Pebble ingestion.
	//
	// Commonly, writeSST provides a Writer backed by a newly created SSTable
	// which will be part of a Pebble ingestion. Less commonly, when the snapshot
	// is simple/small and applying it as a batch is more efficient, the Writer is
	// backed by a shared Pebble batch (one for the entire snapshot application).
	writeSST func(context.Context, func(context.Context, storage.Writer) error) error
}

// applyAsBatch instructs the snapWriter that the snapshot application is going
// to be written as a Pebble batch rather than ingestion.
//
// Must be called at most once.
func (s *snapWriter) applyAsBatch() kvstorage.StateWO {
	if s.batch != nil {
		panic("applyAsBatch called twice")
	}
	if s.eng.Separated() {
		s.batch = s.eng.StateEngine().NewWriteBatch()
	} else {
		s.batch = s.eng.Engine().NewWriteBatch()
	}
	// Redirect all writes to the newly created batch.
	s.writeSST = func(ctx context.Context, w func(context.Context, storage.Writer) error) error {
		return w(ctx, s.batch)
	}
	return s.batch
}

// commit commits the snapshot application to storage. If engines are separated,
// it first commits/syncs the raft engine batch, and then commits the state
// machine mutation (as ingestion or batch). Otherwise, it commits the entire
// mutation to a single engine (as ingestion or batch).
func (s *snapWriter) commit(
	ctx context.Context, ing snapIngestion,
) (pebble.IngestOperationStats, error) {
	if s.eng.Separated() {
		// TODO(sep-raft-log): populate the WAG node.
		if err := s.raftWO.Commit(true /* sync */); err != nil {
			return pebble.IngestOperationStats{}, err
		}
	}
	if s.batch != nil {
		// If engines are separated then the raft engine batch will contain a WAG
		// node that guarantees durability of the state machine write, so we don't
		// need so sync the state machine batch.
		if err := s.batch.Commit(!s.eng.Separated()); err != nil {
			return pebble.IngestOperationStats{}, err
		}
		// TODO(pav-kv): return stats instead of managing them in the caller.
		return pebble.IngestOperationStats{}, nil
	}

	ingestTo := s.eng.StateEngine()
	if !s.eng.Separated() {
		ingestTo = s.eng.Engine()
	}
	stats, err := ingestTo.IngestAndExciseWithBlobs(
		ctx, ing.localSSTs, ing.shared, ing.external, ing.exciseSpan)
	if err != nil {
		return pebble.IngestOperationStats{}, errors.Wrapf(err,
			"while ingesting %s and excising %v", ing.localSSTs, ing.exciseSpan)
	}
	return stats, nil
}

// close closes the underlying storage batches, if any. Must be called exactly
// once, at the end of the snapWriter lifetime.
func (s *snapWriter) close() {
	if s.batch != nil {
		s.batch.Close()
	}
	if s.raftWO != nil {
		s.raftWO.Close()
	}
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

	// With separated engines, redirect the raft/log engine writes into a
	// dedicated batch.
	if s.eng.Separated() {
		s.raftWO = s.eng.LogEngine().NewWriteBatch()
	}

	// TODO(pav-kv): assert that our replica already exists in storage. Note that
	// it can be either uninitialized or initialized.
	if err := s.rewriteRaftState(ctx, &sw); err != nil {
		return err
	}
	for _, sub := range sw.subsume {
		if err := s.subsumeReplica(ctx, sub); err != nil {
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
			return storage.ClearRangeWithHeuristic(
				ctx, s.eng.StateEngine(), w, span.Key, span.EndKey,
				kvstorage.ClearRangeThresholdPointKeys(),
			)
		}); err != nil {
			return err
		}
	}
	return nil
}

// rewriteRaftState rewrites the raft state of the given replica with the
// provided state. Specifically, it rewrites HardState and RaftTruncatedState,
// and clears the raft log.
//
// The method knows how to handle separated and single engine, so that the
// caller is not concerned about it.
func (s *snapWriter) rewriteRaftState(ctx context.Context, sw *snapWrite) error {
	// If engines are separated, write directly to the raft engine batch.
	if s.raftWO != nil {
		return kvstorage.RewriteRaftState(ctx, kvstorage.RaftWO(s.raftWO), sw.sl, sw.hardState, sw.truncState)
	}
	// Otherwise, write to an SSTable in the state machine engine, or s.batch if
	// applying the snapshot as a batch.
	return s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
		return kvstorage.RewriteRaftState(ctx, kvstorage.RaftWO(w), sw.sl, sw.hardState, sw.truncState)
	})
}

// subsumeReplica destroys the given "subsumed" replica, except it leaves its
// "user keys" spans intact which are inherited and overridden by the caller.
//
// The method knows how to handle separated and single engine, so that the
// caller is not concerned about it.
func (s *snapWriter) subsumeReplica(ctx context.Context, sub kvstorage.DestroyReplicaInfo) error {
	// Create an SST for the subsumed replica's RangeID-local keys. Note that it's
	// redirected to s.batch if the snapshot is written as a batch.
	return s.writeSST(ctx, func(ctx context.Context, w storage.Writer) error {
		// If engines are separated, write the relevant subset of keys directly to
		// the raft engine batch, instead of the combined SST/batch.
		raftWO := kvstorage.RaftWO(s.raftWO)
		if raftWO == nil {
			raftWO = w
		}
		return kvstorage.SubsumeReplica(ctx, kvstorage.ReadWriter{
			State: kvstorage.State{RO: s.eng.StateEngine(), WO: kvstorage.StateWO(w)},
			Raft:  kvstorage.Raft{RO: s.eng.LogEngine(), WO: raftWO},
		}, sub)
	})
}

// snapIngestion encodes the parameters needed to run a Pebble ingestion for
// applying a snapshot.
//
// TODO(sep-raft-log): this information needs to be stored in the corresponding
// WAG node. Use the proto counterparts of this type (see wagpb.Ingestion).
type snapIngestion struct {
	localSSTs  pebble.LocalSSTables
	shared     []pebble.SharedSSTMeta
	external   []pebble.ExternalFile
	exciseSpan roachpb.Span
}
