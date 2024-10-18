// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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

func (r *Replica) postDestroyRaftMuLocked(ctx context.Context, ms enginepb.MVCCStats) error {
	// TODO(tschottdorf): at node startup, we should remove all on-disk
	// directories belonging to replicas which aren't present. A crash before a
	// call to postDestroyRaftMuLocked will currently leave the files around
	// forever.
	//
	// TODO(tbg): coming back in 2021, the above should be outdated. The ReplicaID
	// is set on creation and never changes over the lifetime of a Replica. Also,
	// the replica is always contained in its descriptor. So this code below should
	// be removable.
	//
	// TODO(pavelkalinnikov): coming back in 2023, the above may still happen if:
	// (1) state machine syncs, (2) OS crashes before (3) sideloaded was able to
	// sync the files removal. The files should be cleaned up on restart.
	if err := r.raftMu.sideloaded.Clear(ctx); err != nil {
		return err
	}

	// Release the reference to this tenant in metrics, we know the tenant ID is
	// valid if the replica is initialized.
	if r.tenantMetricsRef != nil {
		r.store.metrics.releaseTenant(ctx, r.tenantMetricsRef)
	}

	// Unhook the tenant rate limiter if we have one.
	if r.tenantLimiter != nil {
		r.store.tenantRateLimiters.Release(r.tenantLimiter)
	}

	return nil
}

// destroyRaftMuLocked deletes data associated with a replica, leaving a
// tombstone. The Replica may not be initialized in which case only the
// range ID local data is removed.
func (r *Replica) destroyRaftMuLocked(ctx context.Context, nextReplicaID roachpb.ReplicaID) error {
	startTime := timeutil.Now()

	ms := r.GetMVCCStats()
	batch := r.store.TODOEngine().NewWriteBatch()
	defer batch.Close()
	desc := r.Desc()
	inited := desc.IsInitialized()

	opts := kvstorage.ClearRangeDataOptions{
		ClearReplicatedBySpan: desc.RSpan(), // zero if !inited
		// TODO(tbg): if it's uninitialized, we might as well clear
		// the replicated state because there isn't any. This seems
		// like it would be simpler, but needs a code audit to ensure
		// callers don't call this in in-between states where the above
		// assumption doesn't hold.
		ClearReplicatedByRangeID:   inited,
		ClearUnreplicatedByRangeID: true,
	}
	// TODO(sep-raft-log): need both engines separately here.
	if err := kvstorage.DestroyReplica(ctx, r.RangeID, r.store.TODOEngine(), batch, nextReplicaID, opts); err != nil {
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
// is one, releases all held flow tokens, and removes the in-memory raft state.
func (r *Replica) disconnectReplicationRaftMuLocked(ctx context.Context) {
	r.raftMu.AssertHeld()
	r.flowControlV2.OnDestroyRaftMuLocked(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	// NB: In the very rare scenario that we're being removed but currently
	// believe we are the leaseholder and there are more requests waiting for
	// quota than total quota then failure to close the proposal quota here could
	// leave those requests stuck forever.
	if pq := r.mu.proposalQuota; pq != nil {
		pq.Close("destroyed")
	}
	r.mu.replicaFlowControlIntegration.onDestroyed(ctx)
	r.mu.proposalBuf.FlushLockedWithoutProposing(ctx)
	for _, p := range r.mu.proposals {
		r.cleanupFailedProposalLocked(p)
		// NB: each proposal needs its own version of the error (i.e. don't try to
		// share the error across proposals).
		p.finishApplication(ctx, makeProposalResultErr(kvpb.NewAmbiguousResultError(apply.ErrRemoved)))
	}

	if !r.mu.destroyStatus.Removed() {
		log.Fatalf(ctx, "removing raft group before destroying replica %s", r)
	}
	r.mu.internalRaftGroup = nil
	r.mu.raftTracer.Close()
}
