// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/coreos/etcd/raft"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// LocalProposalData is data belonging to a proposal that is only relevant
// on the node on which the command was proposed.
//
// TODO(tschottdorf): once the WriteBatch is available in the replicated
// proposal data (i.e. once we really do proposer-evaluted KV), experiment with
// holding on to the proposer's constructed engine.Batch in this struct, which
// could give a performance gain.
type LocalProposalData struct {
	// TODO(andreimatei): idKey is legacy at this point: We could easily key
	// commands by their MaxLeaseIndex, and doing so should be ok with a stop-
	// the-world migration. However, various test facilities depend on the
	// command ID for e.g. replay protection.
	idKey           storagebase.CmdIDKey
	proposedAtTicks int
	ctx             context.Context

	Err   *roachpb.Error
	Reply *roachpb.BatchResponse
	done  chan proposalResult // Used to signal waiting RPC handler

	Batch engine.Batch
	// The stats delta that the application of the Raft command would cause.
	// On a split, contains only the contributions to the left-hand side.
	delta enginepb.MVCCStats

	// The new (estimated, i.e. not necessarily consistently replicated)
	// raftLogSize.
	raftLogSize *int64
	// intents stores any intents encountered but not conflicted with. They
	// should be handed off to asynchronous intent processing on the proposer,
	// so that an attempt to resolve them is made.
	// In particular, this is the pathway used by EndTransaction to communicate
	// its non-local intents up the stack.
	//
	// This is a pointer to allow the zero (and as an unwelcome side effect,
	// all) values to be compared.
	intents *[]intentsWithArg
	// Whether we successfully or non-successfully requested a lease.
	leaseMetricsResult *bool

	// TODO(tschottdorf): there is no need to ever have these actions below
	// taken on the followers, correct?

	// When set (in which case we better be the first range), call
	// gossipFirstRange if the Replica holds the lease.
	gossipFirstRange bool
	// Call maybeGossipSystemConfig.
	maybeGossipSystemConfig bool
	// Call maybeAddToSplitQueue.
	maybeAddToSplitQueue bool
	// Call maybeAddToReplicaGCQueue.
	addToReplicaGCQueue bool
	// Call maybeGossipNodeLiveness with the specified Span, if set.
	maybeGossipNodeLiveness *roachpb.Span
}

// ProposalData is the result of preparing a Raft proposal. That is, the
// proposer (which holds the lease, at least in the case in which the command
// will complete successfully) has evaluated the proposal and is holding on to:
//
// a) changes to be written to disk when applying the command
// b) changes to the state which may require special handling (i.e. code
//    execution) on all Replicas
// c) data which isn't sent to the followers but the proposer needs for tasks
//    it must run when the command has applied (such as resolving intents).
type ProposalData struct {
	LocalProposalData
	storagebase.ReplicatedProposalData
}

func coalesceBool(lhs *bool, rhs bool) {
	*lhs = *lhs || rhs
}

// MergeAndDestroy absorbs the supplied ProposalData while validating that the
// resulting ProposalData makes sense. For example, it is forbidden to absorb
// two lease updates or log truncations, or multiple splits and/or merges.
//
// The passed ProposalData must not be used once passed to Merge.
func (p *ProposalData) MergeAndDestroy(q ProposalData) error {
	// ==================
	// ReplicatedProposalData.
	// ==================
	if q.State.RaftAppliedIndex != 0 {
		return errors.New("must not specify RaftApplyIndex")
	}
	if q.State.LeaseAppliedIndex != 0 {
		return errors.New("must not specify RaftApplyIndex")
	}
	if p.State.Desc == nil {
		p.State.Desc = q.State.Desc
	} else if q.State.Desc != nil {
		return errors.New("conflicting RangeDescriptor")
	}
	if p.State.Lease.Empty() {
		p.State.Lease = q.State.Lease
	} else if !q.State.Lease.Empty() {
		return errors.New("conflicting Lease")
	}
	if p.State.TruncatedState == nil {
		p.State.TruncatedState = q.State.TruncatedState
	} else if q.State.TruncatedState != nil {
		return errors.New("conflicting TruncatedState")
	}
	p.State.GCThreshold.Forward(q.State.GCThreshold)
	p.State.TxnSpanGCThreshold.Forward(q.State.TxnSpanGCThreshold)
	if (q.State.Stats != enginepb.MVCCStats{}) {
		return errors.New("must not specify Stats")
	}
	if p.State.Frozen == storagebase.ReplicaState_FROZEN_UNSPECIFIED {
		p.State.Frozen = q.State.Frozen
	} else if q.State.Frozen != storagebase.ReplicaState_FROZEN_UNSPECIFIED {
		return errors.New("conflicting FrozenStatus")
	}

	p.BlockReads = p.BlockReads || q.BlockReads

	if p.Split == nil {
		p.Split = q.Split
	} else if q.Split != nil {
		return errors.New("conflicting Split")
	}

	if p.Merge == nil {
		p.Merge = q.Merge
	} else if q.Merge != nil {
		return errors.New("conflicting Merge")
	}

	if p.ComputeChecksum == nil {
		p.ComputeChecksum = q.ComputeChecksum
	} else if q.ComputeChecksum != nil {
		return errors.New("conflicting ComputeChecksum")
	}

	// ==================
	// LocalProposalData.
	// ==================

	if p.raftLogSize == nil {
		p.raftLogSize = q.raftLogSize
	} else if q.raftLogSize != nil {
		return errors.New("conflicting raftLogSize")
	}

	if q.intents != nil {
		if p.intents == nil {
			p.intents = q.intents
		} else {
			*p.intents = append(*p.intents, *q.intents...)
		}
	}

	if p.leaseMetricsResult == nil {
		p.leaseMetricsResult = q.leaseMetricsResult
	} else if q.leaseMetricsResult != nil {
		return errors.New("conflicting leaseMetricsResult")
	}

	if p.maybeGossipNodeLiveness == nil {
		p.maybeGossipNodeLiveness = q.maybeGossipNodeLiveness
	} else if q.maybeGossipNodeLiveness != nil {
		return errors.New("conflicting maybeGossipNodeLiveness")
	}

	coalesceBool(&p.gossipFirstRange, q.gossipFirstRange)
	coalesceBool(&p.maybeGossipSystemConfig, q.maybeGossipSystemConfig)
	coalesceBool(&p.maybeAddToSplitQueue, q.maybeAddToSplitQueue)
	coalesceBool(&p.addToReplicaGCQueue, q.addToReplicaGCQueue)
	return nil
}

// TODO(tschottdorf): we should find new homes for the checksum, lease
// code, and various others below to leave here only the core logic.
// Not moving anything right now to avoid awkward diffs.

func (r *Replica) gcOldChecksumEntriesLocked(now time.Time) {
	for id, val := range r.mu.checksums {
		// The timestamp is valid only if set.
		if !val.gcTimestamp.IsZero() && now.After(val.gcTimestamp) {
			delete(r.mu.checksums, id)
		}
	}
}

func (r *Replica) computeChecksumPostApply(
	ctx context.Context, args roachpb.ComputeChecksumRequest,
) {
	stopper := r.store.Stopper()
	id := args.ChecksumID
	now := timeutil.Now()
	r.mu.Lock()
	var notify chan struct{}
	if c, ok := r.mu.checksums[id]; !ok {
		// There is no record of this ID. Make a new notification.
		notify = make(chan struct{})
	} else if !c.started {
		// A CollectChecksumRequest is waiting on the existing notification.
		notify = c.notify
	} else {
		// A previous attempt was made to compute the checksum.
		r.mu.Unlock()
		return
	}

	r.gcOldChecksumEntriesLocked(now)

	// Create an entry with checksum == nil and gcTimestamp unset.
	r.mu.checksums[id] = replicaChecksum{started: true, notify: notify}
	desc := *r.mu.state.Desc
	r.mu.Unlock()
	snap := r.store.NewSnapshot()

	// Compute SHA asynchronously and store it in a map by UUID.
	if err := stopper.RunAsyncTask(ctx, func(ctx context.Context) {
		defer snap.Close()
		var snapshot *roachpb.RaftSnapshotData
		if args.Snapshot {
			snapshot = &roachpb.RaftSnapshotData{}
		}
		sha, err := r.sha512(desc, snap, snapshot)
		if err != nil {
			log.Errorf(ctx, "%v", err)
			sha = nil
		}
		r.computeChecksumDone(ctx, id, sha, snapshot)
	}); err != nil {
		defer snap.Close()
		log.Error(ctx, errors.Wrapf(err, "could not run async checksum computation (ID = %s)", id))
		// Set checksum to nil.
		r.computeChecksumDone(ctx, id, nil, nil)
	}
}

func (r *Replica) leasePostApply(
	ctx context.Context, newLease roachpb.Lease, replicaID roachpb.ReplicaID, prevLease roachpb.Lease,
) {
	iAmTheLeaseHolder := newLease.Replica.ReplicaID == replicaID
	leaseChangingHands := prevLease.Replica.StoreID != newLease.Replica.StoreID

	if leaseChangingHands && iAmTheLeaseHolder {
		// If this replica is a new holder of the lease, update the low water
		// mark of the timestamp cache. Note that clock offset scenarios are
		// handled via a stasis period inherent in the lease which is documented
		// in on the Lease struct.
		//
		// The introduction of lease transfers implies that the previous lease
		// may have been shortened and we are now applying a formally overlapping
		// lease (since the old lease holder has promised not to serve any more
		// requests, this is kosher). This means that we don't use the old
		// lease's expiration but instead use the new lease's start to initialize
		// the timestamp cache low water.
		log.Infof(ctx, "new range lease %s following %s [physicalTime=%s]",
			newLease, prevLease, r.store.Clock().PhysicalTime())
		r.mu.Lock()
		r.mu.tsCache.SetLowWater(newLease.Start)
		r.mu.Unlock()

		// Gossip the first range whenever its lease is acquired. We check to
		// make sure the lease is active so that a trailing replica won't process
		// an old lease request and attempt to gossip the first range.
		if r.IsFirstRange() &&
			r.leaseStatus(&newLease, r.store.Clock().Now()).state == leaseValid {
			r.gossipFirstRange(ctx)
		}
	}
	if leaseChangingHands && !iAmTheLeaseHolder {
		// We're not the lease holder, reset our timestamp cache, releasing
		// anything currently cached. The timestamp cache is only used by the
		// lease holder. Note that we'll call SetLowWater when we next acquire
		// the lease.
		r.mu.Lock()
		r.mu.tsCache.Clear(r.store.Clock().Now())
		r.mu.Unlock()
	}

	if !iAmTheLeaseHolder &&
		r.leaseStatus(&newLease, r.store.Clock().Now()).state == leaseValid {
		// If this replica is the raft leader but it is not the new lease holder,
		// then try to transfer the raft leadership to match the lease. We like it
		// when leases and raft leadership are collocated because that facilitates
		// quick command application (requests generally need to make it to both the
		// lease holder and the raft leader before being applied by other replicas).
		//
		// TODO(andrei): We want to do this attempt when a lease changes hands, and
		// then periodically check that the collocation is fine. So we keep checking
		// it here on lease extensions, which happen periodically, but that's pretty
		// arbitrary. There might be a more natural place elsewhere where this
		// periodic check should happen.
		r.maybeTransferRaftLeadership(ctx, replicaID, newLease.Replica.ReplicaID)
	}
}

// maybeTransferRaftLeadership attempts to transfer the leadership away from
// this node to target, if this node is the current raft leader.
// The transfer might silently fail, particularly (only?) if the transferee is
// behind on applying the log.
func (r *Replica) maybeTransferRaftLeadership(
	ctx context.Context, replicaID roachpb.ReplicaID, target roachpb.ReplicaID,
) {
	err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		if raftGroup.Status().RaftState == raft.StateLeader {
			// Only the raft leader can attempt a leadership transfer.
			log.Infof(ctx, "range %s: transferring raft leadership to replica ID %v",
				r, target)
			raftGroup.TransferLeader(uint64(target))
		}
		return true, nil
	})
	if err != nil {
		// An error here indicates that this Replica has been destroyed
		// while lacking the necessary synchronization (or even worse, it
		// fails spuriously - could be a storage error), and so we avoid
		// sweeping that under the rug.
		//
		// TODO(tschottdorf): this error is not handled any more
		// at this level.
		log.Fatal(ctx, NewReplicaCorruptionError(err))
	}
}

func (r *Replica) handleProposalData(
	ctx context.Context, originReplica roachpb.ReplicaDescriptor, pd ProposalData,
) {
	if pd.BlockReads {
		r.readOnlyCmdMu.Lock()
		defer r.readOnlyCmdMu.Unlock()
		pd.BlockReads = false
	}

	// Update MVCC stats and Raft portion of ReplicaState.
	r.mu.Lock()
	r.mu.state.Stats = pd.State.Stats
	r.mu.state.RaftAppliedIndex = pd.State.RaftAppliedIndex
	r.mu.state.LeaseAppliedIndex = pd.State.LeaseAppliedIndex
	r.mu.Unlock()

	pd.State.Stats = enginepb.MVCCStats{}
	pd.State.LeaseAppliedIndex = 0
	pd.State.RaftAppliedIndex = 0

	// The above are always present, so we assert only if there are
	// "nontrivial" actions below.
	shouldAssert := (pd.ReplicatedProposalData != storagebase.ReplicatedProposalData{})

	// Process Split or Merge. This needs to happen after stats update because
	// of the ContainsEstimates hack.

	if pd.Split != nil {
		// TODO(tschottdorf): We want to let the usual MVCCStats-delta
		// machinery update our stats for the left-hand side. But there is no
		// way to pass up an MVCCStats object that will clear out the
		// ContainsEstimates flag. We should introduce one, but the migration
		// makes this worth a separate effort (ContainsEstimates would need to
		// have three possible values, 'UNCHANGED', 'NO', and 'YES').
		// Until then, we're left with this rather crude hack.
		{
			r.mu.Lock()
			r.mu.state.Stats.ContainsEstimates = false
			stats := r.mu.state.Stats
			r.mu.Unlock()
			if err := setMVCCStats(ctx, r.store.Engine(), r.RangeID, stats); err != nil {
				log.Fatal(ctx, errors.Wrap(err, "unable to write MVCC stats"))
			}
		}

		splitPostApply(
			r.AnnotateCtx(context.TODO()),
			pd.Split.RHSDelta,
			&pd.Split.SplitTrigger,
			r,
		)
		pd.Split = nil
	}

	if pd.Merge != nil {
		if err := r.store.MergeRange(ctx, r, pd.Merge.LeftDesc.EndKey,
			pd.Merge.RightDesc.RangeID,
		); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf(ctx, "failed to update store after merging range: %s", err)
		}
		pd.Merge = nil
	}

	// Update the remaining ReplicaState.

	if pd.State.Frozen != storagebase.ReplicaState_FROZEN_UNSPECIFIED {
		r.mu.Lock()
		r.mu.state.Frozen = pd.State.Frozen
		r.mu.Unlock()
	}
	pd.State.Frozen = storagebase.ReplicaState_FrozenEnum(0)

	if newDesc := pd.State.Desc; newDesc != nil {
		pd.State.Desc = nil // for assertion

		if err := r.setDesc(newDesc); err != nil {
			// Log the error. There's not much we can do because the commit may
			// have already occurred at this point.
			log.Fatalf(
				ctx,
				"failed to update range descriptor to %+v: %s",
				newDesc, err,
			)
		}
	}

	if newLease := pd.State.Lease; !newLease.Empty() {
		pd.State.Lease = roachpb.Lease{} // for assertion

		r.mu.Lock()
		replicaID := r.mu.replicaID
		prevLease := r.mu.state.Lease
		r.mu.state.Lease = newLease
		r.mu.Unlock()

		r.leasePostApply(ctx, newLease, replicaID, prevLease)
	}

	if newTruncState := pd.State.TruncatedState; newTruncState != nil {
		pd.State.TruncatedState = nil // for assertion
		r.mu.Lock()
		r.mu.state.TruncatedState = newTruncState
		r.mu.Unlock()
		// Clear any entries in the Raft log entry cache for this range up
		// to and including the most recently truncated index.
		r.store.raftEntryCache.clearTo(r.RangeID, newTruncState.Index+1)
	}

	if newThresh := pd.State.GCThreshold; newThresh != hlc.ZeroTimestamp {
		r.mu.Lock()
		r.mu.state.GCThreshold = newThresh
		r.mu.Unlock()
		pd.State.GCThreshold = hlc.ZeroTimestamp
	}

	if newThresh := pd.State.TxnSpanGCThreshold; newThresh != hlc.ZeroTimestamp {
		r.mu.Lock()
		r.mu.state.TxnSpanGCThreshold = newThresh
		r.mu.Unlock()
		pd.State.TxnSpanGCThreshold = hlc.ZeroTimestamp
	}

	// ======================
	// Non-state updates and actions.
	// ======================
	r.store.metrics.addMVCCStats(pd.delta)
	pd.delta = enginepb.MVCCStats{}

	if originReplica.StoreID == r.store.StoreID() {
		// On the replica on which this command originated, resolve skipped
		// intents asynchronously - even on failure.
		//
		// TODO(tschottdorf): EndTransaction will use this pathway to return
		// intents which should immediately be resolved. However, there's
		// a slight chance that an error between the origin of that intents
		// slice and here still results in that intent slice arriving here
		// without the EndTransaction having committed. We should clearly
		// separate the part of the ProposalData which also applies on errors.
		if pd.intents != nil {
			r.store.intentResolver.processIntentsAsync(r, *pd.intents)
		}
	}
	pd.intents = nil

	// The above are present too often, so we assert only if there are
	// "nontrivial" actions below.
	shouldAssert = shouldAssert || (pd.LocalProposalData != LocalProposalData{})

	if pd.raftLogSize != nil {
		r.mu.Lock()
		r.mu.raftLogSize = *pd.raftLogSize
		r.mu.Unlock()
		pd.raftLogSize = nil
	}

	if pd.gossipFirstRange {
		// We need to run the gossip in an async task because gossiping requires
		// the range lease and we'll deadlock if we try to acquire it while
		// holding processRaftMu. Specifically, Replica.redirectOnOrAcquireLease
		// blocks waiting for the lease acquisition to finish but it can't finish
		// because we're not processing raft messages due to holding
		// processRaftMu (and running on the processRaft goroutine).
		if err := r.store.Stopper().RunAsyncTask(ctx, func(ctx context.Context) {
			hasLease, pErr := r.getLeaseForGossip(ctx)

			if pErr != nil {
				log.Infof(ctx, "unable to gossip first range; hasLease=%t, err=%s", hasLease, pErr)
			} else if !hasLease {
				return
			}
			r.gossipFirstRange(ctx)
		}); err != nil {
			log.Infof(ctx, "unable to gossip first range: %s", err)
		}
		pd.gossipFirstRange = false
	}

	if pd.addToReplicaGCQueue {
		if _, err := r.store.replicaGCQueue.Add(r, replicaGCPriorityRemoved); err != nil {
			// Log the error; the range should still be GC'd eventually.
			log.Errorf(ctx, "unable to add to replica GC queue: %s", err)
		}
		pd.addToReplicaGCQueue = false
	}

	if pd.maybeAddToSplitQueue {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
		pd.maybeAddToSplitQueue = false
	}

	if pd.maybeGossipSystemConfig {
		r.maybeGossipSystemConfig()
		pd.maybeGossipSystemConfig = false
	}

	if originReplica.StoreID == r.store.StoreID() {
		if pd.leaseMetricsResult != nil {
			r.store.metrics.leaseRequestComplete(*pd.leaseMetricsResult)
		}
		if pd.maybeGossipNodeLiveness != nil {
			r.maybeGossipNodeLiveness(*pd.maybeGossipNodeLiveness)
		}
	}
	// Satisfy the assertions for all of the items processed only on the
	// proposer (the block just above).
	pd.leaseMetricsResult = nil
	pd.maybeGossipNodeLiveness = nil

	if pd.ComputeChecksum != nil {
		r.computeChecksumPostApply(ctx, *pd.ComputeChecksum)
		pd.ComputeChecksum = nil
	}

	if (pd != ProposalData{}) {
		log.Fatalf(context.TODO(), "unhandled field in ProposalData: %s", pretty.Diff(pd, ProposalData{}))
	}

	if shouldAssert {
		// Assert that the on-disk state doesn't diverge from the in-memory
		// state as a result of the side effects.
		r.assertState(r.store.Engine())
	}
}
