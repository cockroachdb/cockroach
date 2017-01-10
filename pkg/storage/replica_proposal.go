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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/coreos/etcd/raft"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// leaseMetricsType is used to distinguish between various lease
// operations and potentially outcomes.
type leaseMetricsType int

const (
	leaseRequestSuccess leaseMetricsType = iota
	leaseRequestError
	leaseTransferSuccess
	leaseTransferError
)

// ProposalData is data about a command which allows it to be
// evaluated, proposed to raft, and for the result of the command to
// be returned to the caller.
type ProposalData struct {
	// The caller's context, used for logging proposals and reproposals.
	ctx context.Context

	// idKey uniquely identifies this proposal.
	// TODO(andreimatei): idKey is legacy at this point: We could easily key
	// commands by their MaxLeaseIndex, and doing so should be ok with a stop-
	// the-world migration. However, various test facilities depend on the
	// command ID for e.g. replay protection.
	idKey storagebase.CmdIDKey

	// proposedAtTicks is the (logical) time at which this command was
	// last (re-)proposed.
	proposedAtTicks int

	// command is serialized and proposed to raft. In the event of
	// reproposals its MaxLeaseIndex field is mutated.
	command storagebase.RaftCommand

	// endCmds.finish is called after command execution to update the timestamp cache &
	// command queue.
	endCmds *endCmds

	// doneCh is used to signal the waiting RPC handler (the contents of
	// proposalResult come from LocalEvalResult)
	doneCh chan proposalResult

	// Local contains the results of evaluating the request in
	// propEvalKV, tying the upstream evaluation of the request to the
	// downstream application of the command. If propEvalKV is false,
	// Local is nil.
	Local *LocalEvalResult

	// Request is the client's original BatchRequest.
	// TODO(tschottdorf): tests which use TestingCommandFilter use this.
	// Decide how that will work in the future, presumably the
	// CommandFilter would run at proposal time or we allow an opaque
	// struct to be attached to a proposal which is then available as it
	// applies. Other than tests, we only need a few bits of the request
	// here; this could be replaced with isLease and isChangeReplicas
	// booleans.
	Request *roachpb.BatchRequest
}

// finish first invokes the endCmds function and then sends the
// specified proposalResult on the proposal's done channel. endCmds is
// invoked here in order to allow the original client to be cancelled
// and possibly no longer listening to this done channel, and so can't
// be counted on to invoke endCmds itself.
func (proposal *ProposalData) finish(pr proposalResult) {
	if proposal.endCmds != nil {
		proposal.endCmds.done(pr.Reply, pr.Err, pr.ProposalRetry)
		proposal.endCmds = nil
	}
	proposal.doneCh <- pr
	close(proposal.doneCh)
}

// LocalEvalResult is data belonging to an evaluated command that is
// only used on the node on which the command was proposed. Note that
// the proposing node may die before the local results are processed,
// so any side effects here are only best-effort.
//
// TODO(tschottdorf): once the WriteBatch is available in the replicated
// proposal data (i.e. once we really do proposer-evaluted KV), experiment with
// holding on to the proposer's constructed engine.Batch in this struct, which
// could give a performance gain.
type LocalEvalResult struct {
	// The error resulting from the proposal. Most failing proposals will
	// fail-fast, i.e. will return an error to the client above Raft. However,
	// some proposals need to commit data even on error, and in that case we
	// treat the proposal like a successful one, except that the error stored
	// here will be sent to the client when the associated batch commits. In
	// the common case, this field is nil.
	Err   *roachpb.Error
	Reply *roachpb.BatchResponse

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
	//
	// TODO(tschottdorf): Update this counter correctly with prop-eval'ed KV
	// in the following case:
	// - proposal does not fail fast and goes through Raft
	// - downstream-of-Raft logic identifies a conflict and returns an error
	// The downstream-of-Raft logic does not exist at time of writing.
	leaseMetricsResult *leaseMetricsType

	// When set (in which case we better be the first range), call
	// gossipFirstRange if the Replica holds the lease.
	gossipFirstRange bool
	// Call maybeGossipSystemConfig.
	maybeGossipSystemConfig bool
	// Call maybeAddToSplitQueue.
	maybeAddToSplitQueue bool
	// Call maybeGossipNodeLiveness with the specified Span, if set.
	maybeGossipNodeLiveness *roachpb.Span
}

func (lResult *LocalEvalResult) detachIntents() []intentsWithArg {
	if lResult == nil || lResult.intents == nil {
		return nil
	}
	intents := *lResult.intents
	lResult.intents = nil
	return intents
}

// EvalResult is the result of evaluating a KV request. That is, the
// proposer (which holds the lease, at least in the case in which the command
// will complete successfully) has evaluated the request and is holding on to:
//
// a) changes to be written to disk when applying the command
// b) changes to the state which may require special handling (i.e. code
//    execution) on all Replicas
// c) data which isn't sent to the followers but the proposer needs for tasks
//    it must run when the command has applied (such as resolving intents).
type EvalResult struct {
	Local      LocalEvalResult
	Replicated storagebase.ReplicatedEvalResult
	WriteBatch *storagebase.WriteBatch
}

// coalesceBool ORs rhs into lhs and then zeroes rhs.
func coalesceBool(lhs *bool, rhs *bool) {
	*lhs = *lhs || *rhs
	*rhs = false
}

// MergeAndDestroy absorbs the supplied EvalResult while validating that the
// resulting EvalResult makes sense. For example, it is forbidden to absorb
// two lease updates or log truncations, or multiple splits and/or merges.
//
// The passed EvalResult must not be used once passed to Merge.
func (p *EvalResult) MergeAndDestroy(q EvalResult) error {
	if q.Replicated.State.RaftAppliedIndex != 0 {
		return errors.New("must not specify RaftApplyIndex")
	}
	if q.Replicated.State.LeaseAppliedIndex != 0 {
		return errors.New("must not specify RaftApplyIndex")
	}
	if p.Replicated.State.Desc == nil {
		p.Replicated.State.Desc = q.Replicated.State.Desc
	} else if q.Replicated.State.Desc != nil {
		return errors.New("conflicting RangeDescriptor")
	}
	q.Replicated.State.Desc = nil

	if p.Replicated.State.Lease == nil {
		p.Replicated.State.Lease = q.Replicated.State.Lease
	} else if q.Replicated.State.Lease != nil {
		return errors.New("conflicting Lease")
	}
	q.Replicated.State.Lease = nil

	if p.Replicated.State.TruncatedState == nil {
		p.Replicated.State.TruncatedState = q.Replicated.State.TruncatedState
	} else if q.Replicated.State.TruncatedState != nil {
		return errors.New("conflicting TruncatedState")
	}
	q.Replicated.State.TruncatedState = nil

	p.Replicated.State.GCThreshold.Forward(q.Replicated.State.GCThreshold)
	q.Replicated.State.GCThreshold = hlc.ZeroTimestamp
	p.Replicated.State.TxnSpanGCThreshold.Forward(q.Replicated.State.TxnSpanGCThreshold)
	q.Replicated.State.TxnSpanGCThreshold = hlc.ZeroTimestamp

	if (q.Replicated.State.Stats != enginepb.MVCCStats{}) {
		return errors.New("must not specify Stats")
	}

	if p.Replicated.State.Frozen == storagebase.ReplicaState_FROZEN_UNSPECIFIED {
		p.Replicated.State.Frozen = q.Replicated.State.Frozen
	} else if q.Replicated.State.Frozen != storagebase.ReplicaState_FROZEN_UNSPECIFIED {
		return errors.New("conflicting FrozenStatus")
	}
	q.Replicated.State.Frozen = storagebase.ReplicaState_FROZEN_UNSPECIFIED

	p.Replicated.BlockReads = p.Replicated.BlockReads || q.Replicated.BlockReads
	q.Replicated.BlockReads = false

	if p.Replicated.Split == nil {
		p.Replicated.Split = q.Replicated.Split
	} else if q.Replicated.Split != nil {
		return errors.New("conflicting Split")
	}
	q.Replicated.Split = nil

	if p.Replicated.Merge == nil {
		p.Replicated.Merge = q.Replicated.Merge
	} else if q.Replicated.Merge != nil {
		return errors.New("conflicting Merge")
	}
	q.Replicated.Merge = nil

	if p.Replicated.ChangeReplicas == nil {
		p.Replicated.ChangeReplicas = q.Replicated.ChangeReplicas
	} else if q.Replicated.ChangeReplicas != nil {
		return errors.New("conflicting ChangeReplicas")
	}
	q.Replicated.ChangeReplicas = nil

	if p.Replicated.ComputeChecksum == nil {
		p.Replicated.ComputeChecksum = q.Replicated.ComputeChecksum
	} else if q.Replicated.ComputeChecksum != nil {
		return errors.New("conflicting ComputeChecksum")
	}
	q.Replicated.ComputeChecksum = nil

	if p.Replicated.RaftLogDelta == nil {
		p.Replicated.RaftLogDelta = q.Replicated.RaftLogDelta
	} else if q.Replicated.RaftLogDelta != nil {
		return errors.New("conflicting RaftLogDelta")
	}
	q.Replicated.RaftLogDelta = nil

	if q.Local.intents != nil {
		if p.Local.intents == nil {
			p.Local.intents = q.Local.intents
		} else {
			*p.Local.intents = append(*p.Local.intents, *q.Local.intents...)
		}
	}
	q.Local.intents = nil

	if p.Local.leaseMetricsResult == nil {
		p.Local.leaseMetricsResult = q.Local.leaseMetricsResult
	} else if q.Local.leaseMetricsResult != nil {
		return errors.New("conflicting leaseMetricsResult")
	}
	q.Local.leaseMetricsResult = nil

	if p.Local.maybeGossipNodeLiveness == nil {
		p.Local.maybeGossipNodeLiveness = q.Local.maybeGossipNodeLiveness
	} else if q.Local.maybeGossipNodeLiveness != nil {
		return errors.New("conflicting maybeGossipNodeLiveness")
	}
	q.Local.maybeGossipNodeLiveness = nil

	coalesceBool(&p.Local.gossipFirstRange, &q.Local.gossipFirstRange)
	coalesceBool(&p.Local.maybeGossipSystemConfig, &q.Local.maybeGossipSystemConfig)
	coalesceBool(&p.Local.maybeAddToSplitQueue, &q.Local.maybeAddToSplitQueue)

	if (q != EvalResult{}) {
		log.Fatalf(context.TODO(), "unhandled EvalResult: %s", pretty.Diff(q, EvalResult{}))
	}

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
	ctx context.Context,
	newLease *roachpb.Lease,
	replicaID roachpb.ReplicaID,
	prevLease *roachpb.Lease,
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
		if log.V(1) {
			log.Infof(ctx, "new range lease %s following %s [physicalTime=%s]",
				newLease, prevLease, r.store.Clock().PhysicalTime())
		}
		r.mu.Lock()
		r.mu.tsCache.SetLowWater(newLease.Start)
		r.mu.Unlock()

		// Gossip the first range whenever its lease is acquired. We check to
		// make sure the lease is active so that a trailing replica won't process
		// an old lease request and attempt to gossip the first range.
		if r.IsFirstRange() && r.IsLeaseValid(newLease, r.store.Clock().Now()) {
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

	if !iAmTheLeaseHolder && r.IsLeaseValid(newLease, r.store.Clock().Now()) {
		// If this replica is the raft leader but it is not the new lease holder,
		// then try to transfer the raft leadership to match the lease. We like it
		// when leases and raft leadership are collocated because that facilitates
		// quick command application (requests generally need to make it to both the
		// lease holder and the raft leader before being applied by other replicas).
		// Note that this condition is also checked periodically when computing
		// replica metrics.
		r.maybeTransferRaftLeadership(ctx, newLease.Replica.ReplicaID)
	}

	// Notify the store that a lease change occurred and it may need to
	// gossip the updated store descriptor (with updated capacity).
	if leaseChangingHands && (prevLease.OwnedBy(r.store.StoreID()) ||
		newLease.OwnedBy(r.store.StoreID())) {
		r.store.maybeGossipOnCapacityChange(ctx, leaseChangeEvent)
	}

	// Potentially re-gossip if the range contains system data (e.g. system
	// config or node liveness).
	if iAmTheLeaseHolder {
		if err := r.maybeGossipSystemConfig(ctx); err != nil {
			log.Error(ctx, err)
		}
		if err := r.maybeGossipNodeLiveness(ctx, keys.NodeLivenessSpan); err != nil {
			log.Error(ctx, err)
		}
	}
}

// maybeTransferRaftLeadership attempts to transfer the leadership
// away from this node to target, if this node is the current raft
// leader. We don't attempt to transfer leadership if the transferee
// is behind on applying the log.
func (r *Replica) maybeTransferRaftLeadership(ctx context.Context, target roachpb.ReplicaID) {
	err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// Only the raft leader can attempt a leadership transfer.
		if status := raftGroup.Status(); status.RaftState == raft.StateLeader {
			// Only attempt this if the target has all the log entries.
			if pr, ok := status.Progress[uint64(target)]; ok && pr.Match == r.mu.lastIndex {
				log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", target)
				r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
				raftGroup.TransferLeader(uint64(target))
			}
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

func (r *Replica) handleReplicatedEvalResult(
	ctx context.Context, rResult storagebase.ReplicatedEvalResult,
) (shouldAssert bool) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		rResult.IsLeaseRequest = false
		rResult.IsConsistencyRelated = false
		rResult.IsFreeze = false
		rResult.Timestamp = hlc.ZeroTimestamp
	}

	if rResult.BlockReads {
		r.readOnlyCmdMu.Lock()
		defer r.readOnlyCmdMu.Unlock()
		rResult.BlockReads = false
	}

	// Update MVCC stats and Raft portion of ReplicaState.
	r.mu.Lock()
	r.mu.state.Stats.Add(rResult.Delta)
	if rResult.State.RaftAppliedIndex != 0 {
		r.mu.state.RaftAppliedIndex = rResult.State.RaftAppliedIndex
	}
	if rResult.State.LeaseAppliedIndex != 0 {
		r.mu.state.LeaseAppliedIndex = rResult.State.LeaseAppliedIndex
	}
	needsSplitBySize := r.needsSplitBySizeLocked()
	r.mu.Unlock()

	r.store.metrics.addMVCCStats(rResult.Delta)
	rResult.Delta = enginepb.MVCCStats{}

	const raftLogCheckFrequency = 1 + RaftLogQueueStaleThreshold/4
	if rResult.State.RaftAppliedIndex%raftLogCheckFrequency == 1 {
		r.store.raftLogQueue.MaybeAdd(r, r.store.Clock().Now())
	}
	if needsSplitBySize {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
	}

	rResult.State.Stats = enginepb.MVCCStats{}
	rResult.State.LeaseAppliedIndex = 0
	rResult.State.RaftAppliedIndex = 0

	// The above are always present, so we assert only if there are
	// "nontrivial" actions below.
	shouldAssert = (rResult != storagebase.ReplicatedEvalResult{})

	// Process Split or Merge. This needs to happen after stats update because
	// of the ContainsEstimates hack.

	if rResult.Split != nil {
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
			r.AnnotateCtx(ctx),
			rResult.Split.RHSDelta,
			&rResult.Split.SplitTrigger,
			r,
		)
		rResult.Split = nil
	}

	if rResult.Merge != nil {
		if err := r.store.MergeRange(ctx, r, rResult.Merge.LeftDesc.EndKey,
			rResult.Merge.RightDesc.RangeID,
		); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf(ctx, "failed to update store after merging range: %s", err)
		}
		rResult.Merge = nil
	}

	// Update the remaining ReplicaState.

	if rResult.State.Frozen != storagebase.ReplicaState_FROZEN_UNSPECIFIED {
		r.mu.Lock()
		r.mu.state.Frozen = rResult.State.Frozen
		r.mu.Unlock()
	}
	rResult.State.Frozen = storagebase.ReplicaState_FROZEN_UNSPECIFIED

	if newDesc := rResult.State.Desc; newDesc != nil {
		if err := r.setDesc(newDesc); err != nil {
			// Log the error. There's not much we can do because the commit may
			// have already occurred at this point.
			log.Fatalf(
				ctx,
				"failed to update range descriptor to %+v: %s",
				newDesc, err,
			)
		}
		rResult.State.Desc = nil
	}

	if change := rResult.ChangeReplicas; change != nil {
		if change.ChangeType == roachpb.REMOVE_REPLICA &&
			r.store.StoreID() == change.Replica.StoreID {
			// This wants to run as late as possible, maximizing the chances
			// that the other nodes have finished this command as well (since
			// processing the removal from the queue looks up the Range at the
			// lease holder, being too early here turns this into a no-op).
			if _, err := r.store.replicaGCQueue.Add(r, replicaGCPriorityRemoved); err != nil {
				// Log the error; the range should still be GC'd eventually.
				log.Errorf(ctx, "unable to add to replica GC queue: %s", err)
			}
		}
		rResult.ChangeReplicas = nil
	}

	if newLease := rResult.State.Lease; newLease != nil {
		rResult.State.Lease = nil // for assertion

		r.mu.Lock()
		replicaID := r.mu.replicaID
		prevLease := r.mu.state.Lease
		r.mu.state.Lease = newLease
		r.mu.Unlock()

		r.leasePostApply(ctx, newLease, replicaID, prevLease)
	}

	if newTruncState := rResult.State.TruncatedState; newTruncState != nil {
		rResult.State.TruncatedState = nil // for assertion
		r.mu.Lock()
		r.mu.state.TruncatedState = newTruncState
		r.mu.Unlock()
		// Clear any entries in the Raft log entry cache for this range up
		// to and including the most recently truncated index.
		r.store.raftEntryCache.clearTo(r.RangeID, newTruncState.Index+1)
	}

	if newThresh := rResult.State.GCThreshold; newThresh != hlc.ZeroTimestamp {
		r.mu.Lock()
		r.mu.state.GCThreshold = newThresh
		r.mu.Unlock()
		rResult.State.GCThreshold = hlc.ZeroTimestamp
	}

	if newThresh := rResult.State.TxnSpanGCThreshold; newThresh != hlc.ZeroTimestamp {
		r.mu.Lock()
		r.mu.state.TxnSpanGCThreshold = newThresh
		r.mu.Unlock()
		rResult.State.TxnSpanGCThreshold = hlc.ZeroTimestamp
	}

	if rResult.ComputeChecksum != nil {
		r.computeChecksumPostApply(ctx, *rResult.ComputeChecksum)
		rResult.ComputeChecksum = nil
	}

	if rResult.RaftLogDelta != nil {
		r.mu.Lock()
		r.mu.raftLogSize += *rResult.RaftLogDelta
		if r.mu.raftLogSize < 0 {
			// Ensure raftLogSize is not negative since it isn't persisted between
			// server restarts.
			r.mu.raftLogSize = 0
		}
		r.mu.Unlock()
		rResult.RaftLogDelta = nil
	}

	if (rResult != storagebase.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "unhandled field in ReplicatedEvalResult: %s", pretty.Diff(rResult, storagebase.ReplicatedEvalResult{}))
	}
	return shouldAssert
}

func (r *Replica) handleLocalEvalResult(
	ctx context.Context, originReplica roachpb.ReplicaDescriptor, lResult LocalEvalResult,
) (shouldAssert bool) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Err = nil
		lResult.Reply = nil
	}

	// ======================
	// Non-state updates and actions.
	// ======================

	// The caller is required to detach and handle intents.
	if lResult.intents != nil {
		log.Fatalf(ctx, "LocalEvalResult.intents should be nil: %+v", lResult.intents)
	}

	// The above are present too often, so we assert only if there are
	// "nontrivial" actions below.
	shouldAssert = (lResult != LocalEvalResult{})

	if lResult.gossipFirstRange {
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
		lResult.gossipFirstRange = false
	}

	if lResult.maybeAddToSplitQueue {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
		lResult.maybeAddToSplitQueue = false
	}

	if lResult.maybeGossipSystemConfig {
		r.maybeGossipSystemConfig(ctx)
		lResult.maybeGossipSystemConfig = false
	}

	if originReplica.StoreID == r.store.StoreID() {
		if lResult.leaseMetricsResult != nil {
			switch metric := *lResult.leaseMetricsResult; metric {
			case leaseRequestSuccess, leaseRequestError:
				r.store.metrics.leaseRequestComplete(metric == leaseRequestSuccess)
			case leaseTransferSuccess, leaseTransferError:
				r.store.metrics.leaseTransferComplete(metric == leaseTransferSuccess)
			}
		}
		if lResult.maybeGossipNodeLiveness != nil {
			r.maybeGossipNodeLiveness(ctx, *lResult.maybeGossipNodeLiveness)
		}
	}
	// Satisfy the assertions for all of the items processed only on the
	// proposer (the block just above).
	lResult.leaseMetricsResult = nil
	lResult.maybeGossipNodeLiveness = nil

	if (lResult != LocalEvalResult{}) {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, LocalEvalResult{}))
	}

	return shouldAssert
}

func (r *Replica) handleEvalResult(
	ctx context.Context,
	originReplica roachpb.ReplicaDescriptor,
	lResult *LocalEvalResult,
	rResult *storagebase.ReplicatedEvalResult,
) {
	// Careful: `shouldAssert = f() || g()` will not run both if `f()` is true.
	shouldAssert := false
	if rResult != nil {
		shouldAssert = r.handleReplicatedEvalResult(ctx, *rResult) || shouldAssert
	}
	if lResult != nil {
		shouldAssert = r.handleLocalEvalResult(ctx, originReplica, *lResult) || shouldAssert
	}
	if shouldAssert {
		// Assert that the on-disk state doesn't diverge from the in-memory
		// state as a result of the side effects.
		r.assertState(r.store.Engine())
	}
}
