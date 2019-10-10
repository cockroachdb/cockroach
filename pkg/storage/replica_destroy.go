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
	"math"

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
	// The replica has been GCed or is in the process of being synchronously
	// removed.
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

// mergedTombstoneReplicaID is the replica ID written into the tombstone
// for replicas which are part of a range which is known to have been merged.
// This value should prevent any messages from stale replicas of that range from
// ever resurrecting merged replicas. Whenever merging or subsuming a replica we
// know new replicas can never be created so this value is used even if we
// don't know the current replica ID.
const mergedTombstoneReplicaID roachpb.ReplicaID = math.MaxInt32

func (r *Replica) preDestroyRaftMuLocked(
	ctx context.Context,
	reader engine.Reader,
	writer engine.Writer,
	nextReplicaID roachpb.ReplicaID,
	clearRangeIDLocalOnly bool,
	mustUseClearRange bool,
) error {
	desc := r.Desc()
	err := clearRangeData(desc, reader, writer, clearRangeIDLocalOnly, mustUseClearRange)
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
	if ms != (enginepb.MVCCStats{}) {
		desc := r.Desc()
		r.store.compactor.Suggest(ctx, storagepb.SuggestedCompaction{
			StartKey: roachpb.Key(desc.StartKey),
			EndKey:   roachpb.Key(desc.EndKey),
			Compaction: storagepb.Compaction{
				Bytes:            ms.Total(),
				SuggestedAtNanos: timeutil.Now().UnixNano(),
			},
		})
	}

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

// destroyRaftMuLocked deletes data associated with a replica, leaving a
// tombstone. The Replica may not be initialized in which case only the
// range ID local data is removed.
func (r *Replica) destroyRaftMuLocked(ctx context.Context, nextReplicaID roachpb.ReplicaID) error {
	startTime := timeutil.Now()

	ms := r.GetMVCCStats()
	batch := r.Engine().NewWriteOnlyBatch()
	defer batch.Close()
	clearRangeIDLocalOnly := !r.IsInitialized()
	if err := r.preDestroyRaftMuLocked(
		ctx,
		r.Engine(),
		batch,
		nextReplicaID,
		clearRangeIDLocalOnly,
		false, /* mustUseClearRange */
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
	if r.IsInitialized() {
		log.Infof(ctx, "removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
			ms.KeyCount+ms.SysCount, ms.KeyCount, ms.SysCount,
			commitTime.Sub(startTime).Seconds()*1000,
			preTime.Sub(startTime).Seconds()*1000,
			commitTime.Sub(preTime).Seconds()*1000)
	} else {
		log.Infof(ctx, "removed uninitialized range in %0.0fms [clear=%0.0fms commit=%0.0fms]",
			commitTime.Sub(startTime).Seconds()*1000,
			preTime.Sub(startTime).Seconds()*1000,
			commitTime.Sub(preTime).Seconds()*1000)
	}
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
	if nextReplicaID > r.mu.tombstoneMinReplicaID {
		r.mu.tombstoneMinReplicaID = nextReplicaID
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
