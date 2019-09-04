// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// DestroyReason indicates if a replica is alive, destroyed, corrupted or pending destruction.
type DestroyReason int

const (
	// The replica is alive.
	destroyReasonAlive DestroyReason = iota
	// The replica is in the process of being removed but has not been removed
	// yet. It exists to avoid races between two threads which may decide to
	// destroy a replica (e.g. processing a ChangeRelicasTrigger removing the
	// range and receiving a raft message with a higher replica ID).
	destroyReasonRemovalPending
	// The replica has been GCed.
	destroyReasonRemoved
	// The replica has been merged into its left-hand neighbor, but its left-hand
	// neighbor hasn't yet subsumed it.
	destroyReasonMergePending
)

type destroyStatus struct {
	reason DestroyReason
	err    error
}

func (s destroyStatus) String() string {
	return fmt.Sprintf("{%v %d}", s.err, s.reason)
}

func (s *destroyStatus) Set(err error, reason DestroyReason) {
	s.err = err
	s.reason = reason
}

// IsAlive returns true when a replica is alive.
func (s destroyStatus) IsAlive() bool {
	return s.reason == destroyReasonAlive
}

// Removed returns whether the replica has been removed.
func (s destroyStatus) Removed() bool {
	return s.reason == destroyReasonRemoved
}

// RemovalPending returns whether the replica is removed or in the process of
// being removed.
func (s destroyStatus) RemovalPending() bool {
	return s.reason == destroyReasonRemovalPending || s.reason == destroyReasonRemoved
}

func (r *Replica) preDestroyRaftMuLocked(
	ctx context.Context,
	reader engine.Reader,
	writer engine.Writer,
	nextReplicaID roachpb.ReplicaID,
	clearOpt clearRangeOption,
	mustClearRange bool,
) error {
	desc := r.Desc()
	err := clearRangeData(desc, reader, writer, clearOpt, mustClearRange)
	if err != nil {
		return err
	}

	// Save a tombstone to ensure that replica IDs never get reused.
	//
	// NB: Legacy tombstones (which are in the replicated key space) are wiped
	// in clearRangeData, but that's OK since we're writing a new one in the same
	// batch (and in particular, sequenced *after* the wipe).
	return r.setTombstoneKey(ctx, writer, nextReplicaID)
}

func (r *Replica) postDestroyRaftMuLocked(ctx context.Context, ms enginepb.MVCCStats) error {
	// Suggest the cleared range to the compactor queue.
	//
	// TODO(benesch): we would ideally atomically suggest the compaction with
	// the deletion of the data itself.
	desc := r.Desc()
	r.store.compactor.Suggest(ctx, storagepb.SuggestedCompaction{
		StartKey: roachpb.Key(desc.StartKey),
		EndKey:   roachpb.Key(desc.EndKey),
		Compaction: storagepb.Compaction{
			Bytes:            ms.Total(),
			SuggestedAtNanos: timeutil.Now().UnixNano(),
		},
	})

	// NB: we need the nil check below because it's possible that we're GC'ing a
	// Replica without a replicaID, in which case it does not have a sideloaded
	// storage.
	//
	// TODO(tschottdorf): at node startup, we should remove all on-disk
	// directories belonging to replicas which aren't present. A crash before a
	// call to postDestroyRaftMuLocked will currently leave the files around
	// forever.
	if r.raftMu.sideloaded != nil {
		return r.raftMu.sideloaded.Clear(ctx)
	}

	return nil
}

// destroyUninitializedReplicaRaftMuLocked is called when we know that an
// uninitialized replica (which knows its replica ID but not its key range) has
// been removed and re-added as a different replica (with a new replica
// ID). We're safe to GC its hard state because nobody cares about our votes
// anymore. We can't GC the range's data because we don't know where it is.
// Fortunately this isn't a problem because the range must not have data
// because a replica must apply a snapshot before having data and if we had
// applied a snapshot then we'd be initialized. This replica may have been
// created in anticipation of a split in which case we'll clear its data when
// the split trigger is applied.
func (r *Replica) destroyUninitializedReplicaRaftMuLocked(
	ctx context.Context, nextReplicaID roachpb.ReplicaID,
) error {
	batch := r.Engine().NewWriteOnlyBatch()
	defer batch.Close()

	// Clear the range ID local data including the hard state.
	// We don't know about any user data so we can't clear any user data.
	// See the comment on this method for why this is safe.
	if err := r.preDestroyRaftMuLocked(
		ctx,
		r.Engine(),
		batch,
		nextReplicaID,
		clearRangeIDLocalOnly,
		false, /* mustClearRange */
	); err != nil {
		return err
	}

	// We need to sync here because we are potentially deleting sideloaded
	// proposals from the file system next. We could write the tombstone only in
	// a synchronous batch first and then delete the data alternatively, but
	// then need to handle the case in which there is both the tombstone and
	// leftover replica data.
	if err := batch.Commit(true); err != nil {
		return err
	}

	if r.raftMu.sideloaded != nil {
		if err := r.raftMu.sideloaded.Clear(ctx); err != nil {
			log.Warningf(ctx, "failed to remove sideload storage for %v: %v", r, err)
		}
	}
	return nil
}

// destroyRaftMuLocked deletes data associated with a replica, leaving a
// tombstone.
func (r *Replica) destroyRaftMuLocked(ctx context.Context, nextReplicaID roachpb.ReplicaID) error {
	startTime := timeutil.Now()

	ms := r.GetMVCCStats()

	batch := r.Engine().NewWriteOnlyBatch()
	defer batch.Close()
	if err := r.preDestroyRaftMuLocked(
		ctx,
		r.Engine(),
		batch,
		nextReplicaID,
		clearAll,
		false, /* mustClearRange */
	); err != nil {
		return err
	}
	preTime := timeutil.Now()

	// We need to sync here because we are potentially deleting sideloaded
	// proposals from the file system next. We could write the tombstone only in
	// a synchronous batch first and then delete the data alternatively, but
	// then need to handle the case in which there is both the tombstone and
	// leftover replica data.
	if err := batch.Commit(true); err != nil {
		return err
	}
	commitTime := timeutil.Now()

	if err := r.postDestroyRaftMuLocked(ctx, ms); err != nil {
		return err
	}

	log.Infof(ctx, "removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
		ms.KeyCount+ms.SysCount, ms.KeyCount, ms.SysCount,
		commitTime.Sub(startTime).Seconds()*1000,
		preTime.Sub(startTime).Seconds()*1000,
		commitTime.Sub(preTime).Seconds()*1000)
	return nil
}

// cancelPendingCommandsLocked cancels all outstanding proposals.
// It requires that both mu and raftMu are held.
func (r *Replica) cancelPendingCommandsLocked() {
	r.raftMu.AssertHeld()
	r.mu.AssertHeld()
	r.mu.proposalBuf.FlushLockedWithoutProposing()
	for _, p := range r.mu.proposals {
		r.cleanupFailedProposalLocked(p)
		// NB: each proposal needs its own version of the error (i.e. don't try to
		// share the error across proposals).
		p.finishApplication(proposalResult{
			Err: roachpb.NewError(roachpb.NewAmbiguousResultError("removing replica")),
		})
	}
}

// setTombstoneKey writes a tombstone to disk to ensure that replica IDs never
// get reused. It determines what the minimum next replica ID can be using
// the provided nextReplicaID and the Replica's own ID.
//
// We have to be careful to set the right key, since a replica can be using an
// ID that it hasn't yet received a RangeDescriptor for if it receives raft
// requests for that replica ID (as seen in #14231).
func (r *Replica) setTombstoneKey(
	ctx context.Context, eng engine.Writer, externalNextReplicaID roachpb.ReplicaID,
) error {
	r.mu.Lock()
	nextReplicaID := r.mu.state.Desc.NextReplicaID
	if nextReplicaID < externalNextReplicaID {
		nextReplicaID = externalNextReplicaID
	}
	if nextReplicaID > r.mu.minReplicaID {
		r.mu.minReplicaID = nextReplicaID
	}
	r.mu.Unlock()

	tombstoneKey := keys.RaftTombstoneKey(r.RangeID)
	tombstone := &roachpb.RaftTombstone{
		NextReplicaID: nextReplicaID,
	}
	// "Blind" because ms == nil and timestamp == hlc.Timestamp{}.
	return engine.MVCCBlindPutProto(ctx, eng, nil, tombstoneKey,
		hlc.Timestamp{}, tombstone, nil)
}
