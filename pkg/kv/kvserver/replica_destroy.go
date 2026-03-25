// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
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

// setDestroyStatusRemovedRaftMuLocked marks the replica as removed. The caller
// must hold raftMu; readOnlyCmdMu and mu are acquired internally.
func (r *Replica) setDestroyStatusRemovedRaftMuLocked() {
	r.raftMu.AssertHeld()
	r.readOnlyCmdMu.Lock()
	defer r.readOnlyCmdMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shMu.destroyStatus.Set(
		kvpb.NewRangeNotFoundError(r.RangeID, r.StoreID()),
		destroyReasonRemoved,
	)
}

// postDestroyRaftMuLocked is called after the replica destruction is durably
// written to Pebble.
func (r *Replica) postDestroyRaftMuLocked(ctx context.Context) {
	// Clearing sideloaded storage may fail (e.g. due to I/O errors), but we log
	// and continue. We've already committed the replica removal, and any future
	// replica instantiation will go through a snapshot path which clears the
	// sideloaded storage.
	// Leaking the files is preferable to crashing or having to recover from a
	// partially-destroyed replica state.
	//
	// TODO(#136416): at node startup, we could remove all on-disk directories
	// belonging to replicas which aren't present. A crash before a call to
	// postDestroyRaftMuLocked or hitting an error below will currently leave the
	// files around forever.
	if err := r.logStorage.ls.Sideload.Clear(ctx); err != nil {
		log.KvDistribution.Warningf(ctx, "failed to clear sideloaded storage: %v", err)
		err = nil // ignore intentionally
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
}

// pendingReplicaDestruction holds a staged but uncommitted replica destruction.
// The engine batch has been populated with all writes needed to destroy the
// replica's on-disk state and install a tombstone. Call MustCommit to durably
// apply the destruction.
type pendingReplicaDestruction struct {
	batch       kvstorage.Batch[storage.WriteBatch]
	ms          enginepb.MVCCStats
	initialized bool
	stageTime   time.Time
	clearTime   time.Time
}

// MustCommit sync-commits the staged destruction batch and logs the result.
//
// A batch commit is expected to be infallible (the data is already in memory
// and just needs to be written to the WAL). Callers rely on this by performing
// irreversible in-memory state changes (e.g. setting destroyStatus) between
// staging and committing. Making commit fallible would force callers to handle
// rollback of those in-memory changes, adding significant complexity. If a
// commit does fail, we are in an unrecoverable situation and must terminate.
//
// We sync here because we are potentially deleting sideloaded proposals from
// the file system next, and don't want a crash in the middle to leave a log
// entry with a missing sideloaded file.
func (p *pendingReplicaDestruction) MustCommit(ctx context.Context) {
	if err := p.batch.Commit(true /* sync */); err != nil {
		log.KvDistribution.Fatalf(ctx, "unable to commit replica destruction batch: %v", err)
	}
	commitTime := timeutil.Now()
	if p.initialized {
		log.KvDistribution.Infof(ctx,
			"removed %d (%d+%d) keys in %0.0fms [clear=%0.0fms commit=%0.0fms]",
			p.ms.KeyCount+p.ms.SysCount, p.ms.KeyCount, p.ms.SysCount,
			commitTime.Sub(p.stageTime).Seconds()*1000,
			p.clearTime.Sub(p.stageTime).Seconds()*1000,
			commitTime.Sub(p.clearTime).Seconds()*1000,
		)
	} else {
		log.KvDistribution.Infof(ctx,
			"removed uninitialized range in %0.0fms [clear=%0.0fms commit=%0.0fms]",
			commitTime.Sub(p.stageTime).Seconds()*1000,
			p.clearTime.Sub(p.stageTime).Seconds()*1000,
			commitTime.Sub(p.clearTime).Seconds()*1000,
		)
	}
}

// Close must be the last call made on this type.
func (p *pendingReplicaDestruction) Close() {
	p.batch.Close()
}

// stageDestroyReplica builds a batch that, when committed, will destroy the
// replica's on-disk state and install a tombstone. The returned
// pendingReplicaDestruction must have Close called when it is no longer needed.
//
// This function performs engine reads (to validate replica ID and tombstone
// state) and stages engine writes into a batch, but does not commit. If any
// engine read fails (e.g. due to context cancellation or I/O error), the error
// is returned and the caller can abort with no side effects.
func stageDestroyReplica(
	ctx context.Context,
	bf *kvstorage.BatchFactory,
	stateRO kvstorage.StateRO,
	raftRO kvstorage.RaftRO,
	info kvstorage.DestroyReplicaInfo,
	nextReplicaID roachpb.ReplicaID,
	ms enginepb.MVCCStats,
) (pendingReplicaDestruction, error) {
	stageTime := timeutil.Now()
	batch := bf.NewWriteBatch()
	stateWO, raftWO := kvstorage.StateWO(batch.State()), batch.Raft()
	if err := kvstorage.DestroyReplica(
		ctx, kvstorage.ReadWriter{
			State: kvstorage.State{RO: stateRO, WO: stateWO},
			Raft:  kvstorage.Raft{RO: raftRO, WO: raftWO},
		},
		info, nextReplicaID,
	); err != nil {
		batch.Close()
		return pendingReplicaDestruction{}, err
	}
	return pendingReplicaDestruction{
		batch:       batch,
		ms:          ms,
		initialized: len(info.Keys.EndKey) > 0,
		stageTime:   stageTime,
		clearTime:   timeutil.Now(),
	}, nil
}

// destroyRaftMuLocked deletes data associated with a replica, leaving a
// tombstone. The Replica may not be initialized in which case only the
// range ID local data is removed.
//
// If an error is returned from this method, the removal failed but no
// side effects have occurred, i.e. the caller may be able to handle the
// error cleanly.
func (r *Replica) destroyRaftMuLocked(ctx context.Context, nextReplicaID roachpb.ReplicaID) error {
	pending, err := stageDestroyReplica(
		ctx, &r.store.batchFactory,
		r.store.StateEngine(), r.store.LogEngine(),
		r.destroyInfoRaftMuLocked(), nextReplicaID,
		r.GetMVCCStats(),
	)
	if err != nil {
		return err
	}
	defer pending.Close()
	pending.MustCommit(ctx)
	r.postDestroyRaftMuLocked(ctx)
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
	r.mu.proposalBuf.FlushLockedWithoutProposing(ctx)
	for _, p := range r.mu.proposals {
		r.cleanupFailedProposalLocked(p)
		// NB: each proposal needs its own version of the error (i.e. don't try to
		// share the error across proposals).
		p.finishApplication(ctx, makeProposalResultErr(kvpb.NewAmbiguousResultError(apply.ErrRemoved)))
	}

	if !r.shMu.destroyStatus.Removed() {
		log.KvDistribution.Fatalf(ctx, "removing raft group before destroying replica %s", r)
	}
	r.mu.internalRaftGroup = nil
	r.mu.raftTracer.Close()
}
