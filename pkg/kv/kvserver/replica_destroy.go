// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
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

// removePreemptiveSnapshot is a migration put in place during the 20.1 release
// to remove any on-disk preemptive snapshots which may still be resident on a
// 19.2 node's store due to a rapid upgrade.
//
// As of 19.2, a range can never process commands while it is not part of a
// range. It is still possible that on-disk replica state exists which does not
// contain this Store exist from when this store was running 19.1 and was not
// removed during 19.2. For the sake of this method and elsewhere we'll define
// a preemptive snapshot as on-disk data corresponding to an initialized range
// which has a range descriptor which does not contain this store.
//
// In 19.1 when a Replica processes a command which removes it from the range
// it does not immediately destroy its data. In fact it just assumed that it
// would not be getting more commands appended to its log. In some cases a
// Replica would continue to apply commands even while it was not a part of the
// range. This was unfortunate as it is not possible to uphold certain invariants
// for stores which are not a part of a range when it processes a command. Not
// only did the above behavior of processing commands regardless of whether the
// current store was in the range wasn't just happenstance, it was a fundamental
// property that was relied upon for the change replicas protocol. In 19.2 we
// adopt learner Replicas which are added to the raft group as a non-voting
// member before receiving a snapshot of the state. In all previous versions
// we did not have voters. The legacy protocol instead relied on sending a
// snapshot of raft state and then the Replica had to apply log entries up to
// the point where it was added to the range.
//
// TODO(ajwerner): Remove during 20.2.
func removePreemptiveSnapshot(
	ctx context.Context, s *Store, desc *roachpb.RangeDescriptor,
) (err error) {
	batch := s.Engine().NewWriteOnlyBatch()
	defer batch.Close()
	const rangeIDLocalOnly = false
	const mustClearRange = false
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to remove preemptive snapshot for range %v", desc)
		}
	}()
	if err := clearRangeData(desc, s.Engine(), batch, rangeIDLocalOnly, mustClearRange); err != nil {
		return err
	}
	if err := writeTombstoneKey(ctx, batch, desc.RangeID, desc.NextReplicaID); err != nil {
		return err
	}
	if err := batch.Commit(true); err != nil {
		return err
	}
	log.Infof(ctx, "removed preemptive snapshot for %v", desc)
	return nil
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
	reader storage.Reader,
	writer storage.Writer,
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

// disconnectReplicationRaftMuLocked is called when a Replica is being removed.
// It cancels all outstanding proposals, closes the proposalQuota if there
// is one, and removes the in-memory raft state.
func (r *Replica) disconnectReplicationRaftMuLocked(ctx context.Context) {
	r.raftMu.AssertHeld()
	r.readOnlyCmdMu.Lock()
	defer r.readOnlyCmdMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	// NB: In the very rare scenario that we're being removed but currently
	// believe we are the leaseholder and there are more requests waiting for
	// quota than total quota then failure to close the proposal quota here could
	// leave those requests stuck forever.
	if pq := r.mu.proposalQuota; pq != nil {
		pq.Close("destroyed")
	}
	r.mu.proposalBuf.FlushLockedWithoutProposing()
	for _, p := range r.mu.proposals {
		r.cleanupFailedProposalLocked(p)
		// NB: each proposal needs its own version of the error (i.e. don't try to
		// share the error across proposals).
		p.finishApplication(ctx, proposalResult{
			Err: roachpb.NewError(roachpb.NewAmbiguousResultError("removing replica")),
		})
	}
	r.mu.internalRaftGroup = nil
}

// setTombstoneKey writes a tombstone to disk to ensure that replica IDs never
// get reused. It determines what the minimum next replica ID can be using
// the provided nextReplicaID and the Replica's own ID.
//
// We have to be careful to set the right key, since a replica can be using an
// ID that it hasn't yet received a RangeDescriptor for if it receives raft
// requests for that replica ID (as seen in #14231).
func (r *Replica) setTombstoneKey(
	ctx context.Context, writer storage.Writer, externalNextReplicaID roachpb.ReplicaID,
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
	return writeTombstoneKey(ctx, writer, r.RangeID, nextReplicaID)
}

func writeTombstoneKey(
	ctx context.Context,
	writer storage.Writer,
	rangeID roachpb.RangeID,
	nextReplicaID roachpb.ReplicaID,
) error {
	tombstoneKey := keys.RangeTombstoneKey(rangeID)
	tombstone := &roachpb.RangeTombstone{
		NextReplicaID: nextReplicaID,
	}
	// "Blind" because ms == nil and timestamp == hlc.Timestamp{}.
	return storage.MVCCBlindPutProto(ctx, writer, nil, tombstoneKey,
		hlc.Timestamp{}, tombstone, nil)
}
