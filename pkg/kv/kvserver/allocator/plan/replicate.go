// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plan

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/benignerror"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// newReplicaGracePeriod is the amount of time that we allow for a new
	// replica's raft state to catch up to the leader's before we start
	// considering it to be behind for the sake of rebalancing. We choose a
	// large value here because snapshots of large replicas can take a while
	// in high latency clusters, and not allowing enough of a cushion can
	// make rebalance thrashing more likely (#17879).
	newReplicaGracePeriod = 5 * time.Minute
)

type ReplicateChange struct {
	Action allocatorimpl.AllocatorAction
	// Replica is the Replica of the range associated with the replicate change.
	Replica AllocatorReplica
	// Op is a planned operation associated with a replica.
	Op AllocationOp
	// Stats tracks the metrics generated during change planning.
	Stats ReplicateStats
}

// ReplicationPlanner provides methods to plan replication changes for a single
// range, given the replica for that range.
type ReplicationPlanner interface {
	// ShouldPlanChange determines whether a replication change should be planned
	// for the range the replica belongs to. The relative priority of is also
	// returned.
	ShouldPlanChange(
		ctx context.Context,
		now hlc.ClockTimestamp,
		repl AllocatorReplica,
		desc *roachpb.RangeDescriptor,
		conf *roachpb.SpanConfig,
		opts PlannerOptions,
	) (bool, float64)
	// PlanOneChange calls the allocator to determine an action to be taken upon a
	// range. The fn then calls back into the allocator to get the changes
	// necessary to act upon the action and returns them as a ReplicateQueueChange.
	PlanOneChange(
		ctx context.Context,
		repl AllocatorReplica,
		desc *roachpb.RangeDescriptor,
		conf *roachpb.SpanConfig,
		opts PlannerOptions,
	) (ReplicateChange, error)
}

// PlannerOptions declares a set of options that can be set when calling
// planner methods.
type PlannerOptions struct {
	// Scatter indicates whether the range is being scattered.
	Scatter bool
	// CanTransferLease indicates whether the lease can be transferred.
	CanTransferLease bool
}

// LeaseCheckReplica contains methods that may be used to check a replica's
// lease.
type LeaseCheckReplica interface {
	HasCorrectLeaseType(lease roachpb.Lease) bool
	LeaseStatusAt(ctx context.Context, now hlc.ClockTimestamp) kvserverpb.LeaseStatus
	LeaseViolatesPreferences(context.Context, *roachpb.SpanConfig) bool
	OwnsValidLease(context.Context, hlc.ClockTimestamp) bool
}

// AllocatorReplica contains methods used for planning changes for the range a
// replica belongs to.
type AllocatorReplica interface {
	LeaseCheckReplica
	RangeUsageInfo() allocator.RangeUsageInfo
	RaftStatus() *raft.Status
	GetFirstIndex() kvpb.RaftIndex
	LastReplicaAdded() (roachpb.ReplicaID, time.Time)
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID
	SendStreamStats(*rac2.RangeSendStreamStats)
}

// ReplicaPlanner implements the ReplicationPlanner interface.
type ReplicaPlanner struct {
	storePool storepool.AllocatorStorePool
	allocator allocatorimpl.Allocator
	knobs     ReplicaPlannerTestingKnobs
}

// ReplicaPlannerTestingKnobs declares the set of knobs that can be used in
// testing the replica planner.
type ReplicaPlannerTestingKnobs struct {
	// DisableReplicaRebalancing disables rebalancing of replicas but otherwise
	// leaves the replicate queue operational.
	DisableReplicaRebalancing bool
	// AllowVoterRemovalWhenNotLeader allows removing voters when this replica is
	// not the leader.
	AllowVoterRemovalWhenNotLeader bool
}

var _ ReplicationPlanner = &ReplicaPlanner{}

// NewReplicaPlanner returns a new ReplicaPlanner which implements the
// ReplicationPlanner interface.
func NewReplicaPlanner(
	allocator allocatorimpl.Allocator,
	storePool storepool.AllocatorStorePool,
	knobs ReplicaPlannerTestingKnobs,
) ReplicaPlanner {
	return ReplicaPlanner{
		storePool: storePool,
		allocator: allocator,
		knobs:     knobs,
	}
}

// ShouldPlanChange determines whether a replication change should be planned
// for the range the replica belongs to. The relative priority of is also
// returned.
func (rp ReplicaPlanner) ShouldPlanChange(
	ctx context.Context,
	now hlc.ClockTimestamp,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	opts PlannerOptions,
) (shouldPlanChange bool, priority float64) {

	log.KvDistribution.VEventf(ctx, 6,
		"computing range action desc=%s config=%s",
		desc, conf.String())
	action, priority := rp.allocator.ComputeAction(ctx, rp.storePool, conf, desc)

	if action == allocatorimpl.AllocatorNoop {
		log.KvDistribution.VEventf(ctx, 2, "no action to take")
		return false, 0
	} else if action != allocatorimpl.AllocatorConsiderRebalance {
		log.KvDistribution.VEventf(ctx, 2, "repair needed (%s), enqueuing", action)
		return true, priority
	}

	voterReplicas := desc.Replicas().VoterDescriptors()
	nonVoterReplicas := desc.Replicas().NonVoterDescriptors()
	if !rp.knobs.DisableReplicaRebalancing {
		scorerOptions := rp.allocator.ScorerOptions(ctx)
		rangeUsageInfo := repl.RangeUsageInfo()
		_, _, _, ok := rp.allocator.RebalanceVoter(
			ctx,
			rp.storePool,
			conf,
			repl.RaftStatus(),
			voterReplicas,
			nonVoterReplicas,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			scorerOptions,
		)
		if ok {
			log.KvDistribution.VEventf(ctx, 2, "rebalance target found for voter, enqueuing")
			return true, 0
		}
		_, _, _, ok = rp.allocator.RebalanceNonVoter(
			ctx,
			rp.storePool,
			conf,
			repl.RaftStatus(),
			voterReplicas,
			nonVoterReplicas,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			scorerOptions,
		)
		if ok {
			log.KvDistribution.VEventf(ctx, 2, "rebalance target found for non-voter, enqueuing")
			return true, 0
		}
		log.KvDistribution.VEventf(ctx, 2, "no rebalance target found, not enqueuing")
	}

	return false, 0
}

// PlanOneChange calls the allocator to determine an action to be taken upon a
// range. The fn then calls back into the allocator to get the changes
// necessary to act upon the action and returns them as a ReplicateQueueChange.
func (rp ReplicaPlanner) PlanOneChange(
	ctx context.Context,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	opts PlannerOptions,
) (change ReplicateChange, _ error) {
	// Initially set the change to be a no-op, it is then modified below if a
	// step may be taken for this replica.
	change = ReplicateChange{
		Action:  allocatorimpl.AllocatorNoop,
		Op:      AllocationNoop{},
		Replica: repl,
	}
	log.KvDistribution.VEventf(ctx, 6,
		"planning range change desc=%s config=%s",
		desc, conf.String())

	voterReplicas, nonVoterReplicas,
		liveVoterReplicas, deadVoterReplicas,
		liveNonVoterReplicas, deadNonVoterReplicas := allocatorimpl.LiveAndDeadVoterAndNonVoterReplicas(rp.storePool, desc)

	// NB: the replication layer ensures that the below operations don't cause
	// unavailability; see kvserver.execChangeReplicasTxn.
	action, allocatorPrio := rp.allocator.ComputeAction(ctx, rp.storePool, conf, desc)
	log.KvDistribution.VEventf(ctx, 1, "next replica action: %s", action)

	var err error
	var op AllocationOp
	var stats ReplicateStats
	removeIdx := -1
	nothingToDo := false
	switch action {
	case allocatorimpl.AllocatorNoop, allocatorimpl.AllocatorRangeUnavailable:
		// We're either missing liveness information or the range is known to have
		// lost quorum. Either way, it's not a good idea to make changes right now.
		// Let the scanner requeue it again later.

	// Add replicas, replace dead replicas, or replace decommissioning replicas.
	case allocatorimpl.AllocatorAddVoter, allocatorimpl.AllocatorAddNonVoter,
		allocatorimpl.AllocatorReplaceDeadVoter, allocatorimpl.AllocatorReplaceDeadNonVoter,
		allocatorimpl.AllocatorReplaceDecommissioningVoter, allocatorimpl.AllocatorReplaceDecommissioningNonVoter:
		var existing, remainingLiveVoters, remainingLiveNonVoters []roachpb.ReplicaDescriptor

		existing, remainingLiveVoters, remainingLiveNonVoters, removeIdx, nothingToDo, err =
			allocatorimpl.DetermineReplicaToReplaceAndFilter(
				rp.storePool,
				action,
				voterReplicas, nonVoterReplicas,
				liveVoterReplicas, deadVoterReplicas,
				liveNonVoterReplicas, deadNonVoterReplicas,
			)
		if nothingToDo || err != nil {
			// Nothing to do.
			break
		}

		switch action.TargetReplicaType() {
		case allocatorimpl.VoterTarget:
			op, stats, err = rp.addOrReplaceVoters(
				ctx, repl, desc, conf, existing, remainingLiveVoters, remainingLiveNonVoters,
				removeIdx, action.ReplicaStatus(), allocatorPrio,
			)
		case allocatorimpl.NonVoterTarget:
			op, stats, err = rp.addOrReplaceNonVoters(
				ctx, repl, desc, conf, existing, remainingLiveVoters, remainingLiveNonVoters,
				removeIdx, action.ReplicaStatus(), allocatorPrio,
			)
		default:
			panic(fmt.Sprintf("unsupported targetReplicaType: %v", action.TargetReplicaType()))
		}

	// Remove replicas.
	case allocatorimpl.AllocatorRemoveVoter:
		op, stats, err = rp.removeVoter(ctx, repl, desc, conf, voterReplicas, nonVoterReplicas)
	case allocatorimpl.AllocatorRemoveNonVoter:
		op, stats, err = rp.removeNonVoter(ctx, repl, desc, conf, voterReplicas, nonVoterReplicas)

	// Remove decommissioning replicas.
	//
	// NB: these two paths will only be hit when the range is over-replicated and
	// has decommissioning replicas; in the common case we'll hit
	// AllocatorReplaceDecommissioning{Non}Voter above.
	case allocatorimpl.AllocatorRemoveDecommissioningVoter:
		op, stats, err = rp.removeDecommissioning(ctx, repl, desc, conf, allocatorimpl.VoterTarget)
	case allocatorimpl.AllocatorRemoveDecommissioningNonVoter:
		op, stats, err = rp.removeDecommissioning(ctx, repl, desc, conf, allocatorimpl.NonVoterTarget)

	// Remove dead replicas.
	//
	// NB: these two paths below will only be hit when the range is
	// over-replicated and has dead replicas; in the common case we'll hit
	// AllocatorReplaceDead{Non}Voter above.
	case allocatorimpl.AllocatorRemoveDeadVoter:
		op, stats, err = rp.removeDead(ctx, repl, deadVoterReplicas, allocatorimpl.VoterTarget)
	case allocatorimpl.AllocatorRemoveDeadNonVoter:
		op, stats, err = rp.removeDead(ctx, repl, deadNonVoterReplicas, allocatorimpl.NonVoterTarget)
	// Rebalance replicas.
	//
	// NB: Rebalacing attempts to balance replica counts among stores of
	// equivalent localities. This action is returned by default for EVERY
	// replica, when no other action applies. However, it has another important
	// role in satisfying the zone constraints appled to a range, by performing
	// swaps when the voter and total replica counts are correct in aggregate,
	// yet incorrect per locality. See #90110.
	case allocatorimpl.AllocatorConsiderRebalance:
		op, stats, err = rp.considerRebalance(
			ctx,
			repl,
			desc,
			conf,
			voterReplicas,
			nonVoterReplicas,
			allocatorPrio,
			opts.Scatter,
		)
	case allocatorimpl.AllocatorFinalizeAtomicReplicationChange, allocatorimpl.AllocatorRemoveLearner:
		op = AllocationFinalizeAtomicReplicationOp{}
	default:
		err = errors.Errorf("unknown allocator action %v", action)
	}

	// If an operation was found, then wrap it in the change being returned. If
	// no operation was found for the allocator action then return a noop.
	if op == nil {
		op = AllocationNoop{}
	}
	change = ReplicateChange{
		Action:  action,
		Replica: repl,
		Op:      op,
		Stats:   stats,
	}
	return change, err
}

// addOrReplaceVoters adds or replaces a voting replica. If removeIdx is -1, an
// addition is carried out. Otherwise, removeIdx must be a valid index into
// existingVoters and specifies which voter to replace with a new one.
//
// The method preferably issues an atomic replica swap, but may not be able to
// do this in all cases, such as when the range consists of a single replica. As
// a fall back, only the addition is carried out; the removal is then a
// follow-up step for the next scanner cycle.
func (rp ReplicaPlanner) addOrReplaceVoters(
	ctx context.Context,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	existingVoters []roachpb.ReplicaDescriptor,
	remainingLiveVoters, remainingLiveNonVoters []roachpb.ReplicaDescriptor,
	removeIdx int,
	replicaStatus allocatorimpl.ReplicaStatus,
	allocatorPriority float64,
) (op AllocationOp, stats ReplicateStats, _ error) {
	var replacing *roachpb.ReplicaDescriptor
	if removeIdx >= 0 {
		replacing = &existingVoters[removeIdx]
	}

	// The allocator should not try to re-add this replica since there is a reason
	// we're removing it (i.e. dead or decommissioning). If we left the replica in
	// the slice, the allocator would not be guaranteed to pick a replica that
	// fills the gap removeRepl leaves once it's gone.
	newVoter, details, err := rp.allocator.AllocateVoter(ctx, rp.storePool, conf, remainingLiveVoters, remainingLiveNonVoters, replacing, replicaStatus)
	if err != nil {
		return nil, stats, err
	}

	isReplace := removeIdx >= 0
	if isReplace && newVoter.StoreID == existingVoters[removeIdx].StoreID {
		return nil, stats, errors.AssertionFailedf("allocator suggested to replace replica on s%d with itself", newVoter.StoreID)
	}

	// We only want to up-replicate if there are suitable allocation targets such
	// that, either the replication goal is met, or it is possible to get to the next
	// odd number of replicas. A consensus group of size 2n has worse failure
	// tolerance properties than a group of size 2n - 1 because it has a larger
	// quorum. For example, up-replicating from 1 to 2 replicas only makes sense
	// if it is possible to be able to go to 3 replicas.
	if err := rp.allocator.CheckAvoidsFragileQuorum(ctx, rp.storePool, conf,
		existingVoters, remainingLiveNonVoters,
		replicaStatus, allocatorimpl.VoterTarget, newVoter, isReplace); err != nil {
		// It does not seem possible to go to the next odd replica state. Note
		// that AllocateVoter returns an allocatorError (a PurgatoryError)
		// when purgatory is requested.
		return nil, stats, errors.Wrap(err, "avoid up-replicating to fragile quorum")
	}

	// Figure out whether we should be promoting an existing non-voting replica to
	// a voting replica or if we ought to be adding a voter afresh.
	var ops []kvpb.ReplicationChange
	replDesc, found := desc.GetReplicaDescriptor(newVoter.StoreID)
	if found {
		if replDesc.Type != roachpb.NON_VOTER {
			return nil, stats, errors.AssertionFailedf("allocation target %s for a voter"+
				" already has an unexpected replica: %s", newVoter, replDesc)
		}
		// If the allocation target has a non-voter already, we will promote it to a
		// voter.
		stats.NonVoterPromotionsCount++
		ops = kvpb.ReplicationChangesForPromotion(newVoter)
	} else {
		stats = stats.trackAddReplicaCount(allocatorimpl.VoterTarget)
		ops = kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, newVoter)
	}
	if !isReplace {
		log.KvDistribution.Infof(ctx, "adding voter %+v: %s",
			newVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	} else {
		stats = stats.trackRemoveMetric(allocatorimpl.VoterTarget, replicaStatus)
		removeVoter := existingVoters[removeIdx]
		log.KvDistribution.Infof(ctx, "replacing voter %s with %+v: %s",
			removeVoter, newVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
		// NB: We may have performed a promotion of a non-voter above, but we will
		// not perform a demotion here and instead just remove the existing replica
		// entirely. This is because we know that the `removeVoter` is either dead
		// or decommissioning (see `Allocator.computeAction`). This means that after
		// this allocation is executed, we could be one non-voter short. This will
		// be handled by the replicateQueue's next attempt at this range.
		ops = append(ops,
			kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, roachpb.ReplicationTarget{
				StoreID: removeVoter.StoreID,
				NodeID:  removeVoter.NodeID,
			})...)
	}

	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             repl.RangeUsageInfo(),
		Chgs:              ops,
		AllocatorPriority: allocatorPriority,
		Reason:            kvserverpb.ReasonRangeUnderReplicated,
		Details:           details,
	}

	return op, stats, nil
}

// addOrReplaceNonVoters adds a non-voting replica to `repl`s range.
func (rp ReplicaPlanner) addOrReplaceNonVoters(
	ctx context.Context,
	repl AllocatorReplica,
	_ *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	existingNonVoters []roachpb.ReplicaDescriptor,
	liveVoterReplicas, liveNonVoterReplicas []roachpb.ReplicaDescriptor,
	removeIdx int,
	replicaStatus allocatorimpl.ReplicaStatus,
	allocatorPrio float64,
) (op AllocationOp, stats ReplicateStats, _ error) {
	var replacing *roachpb.ReplicaDescriptor
	if removeIdx >= 0 {
		replacing = &existingNonVoters[removeIdx]
	}

	newNonVoter, details, err := rp.allocator.AllocateNonVoter(ctx, rp.storePool, conf, liveVoterReplicas, liveNonVoterReplicas, replacing, replicaStatus)
	if err != nil {
		return nil, stats, err
	}

	stats = stats.trackAddReplicaCount(allocatorimpl.NonVoterTarget)
	ops := kvpb.MakeReplicationChanges(roachpb.ADD_NON_VOTER, newNonVoter)
	if removeIdx < 0 {
		log.KvDistribution.Infof(ctx, "adding non-voter %+v: %s",
			newNonVoter, rangeRaftProgress(repl.RaftStatus(), existingNonVoters))
	} else {
		stats = stats.trackRemoveMetric(allocatorimpl.NonVoterTarget, replicaStatus)
		removeNonVoter := existingNonVoters[removeIdx]
		log.KvDistribution.Infof(ctx, "replacing non-voter %s with %+v: %s",
			removeNonVoter, newNonVoter, rangeRaftProgress(repl.RaftStatus(), existingNonVoters))
		ops = append(ops,
			kvpb.MakeReplicationChanges(roachpb.REMOVE_NON_VOTER, roachpb.ReplicationTarget{
				StoreID: removeNonVoter.StoreID,
				NodeID:  removeNonVoter.NodeID,
			})...)
	}

	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             repl.RangeUsageInfo(),
		Chgs:              ops,
		AllocatorPriority: allocatorPrio,
		Reason:            kvserverpb.ReasonRangeUnderReplicated,
		Details:           details,
	}
	return op, stats, nil
}

// findRemoveVoter takes a list of voting replicas and picks one to remove,
// making sure to not remove a newly added voter or to violate the zone configs
// in the process.
//
// TODO(aayush): The structure around replica removal is not great. The entire
// logic of this method should probably live inside Allocator.RemoveVoter. Doing
// so also makes the flow of adding new replicas and removing replicas more
// symmetric.
func (rp ReplicaPlanner) findRemoveVoter(
	ctx context.Context,
	repl interface {
		LastReplicaAdded() (roachpb.ReplicaID, time.Time)
		RaftStatus() *raft.Status
	},
	conf *roachpb.SpanConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (roachpb.ReplicationTarget, string, error) {
	// This retry loop involves quick operations on local state, so a
	// small MaxBackoff is good (but those local variables change on
	// network time scales as raft receives responses).
	//
	// TODO(bdarnell): There's another retry loop at process(). It
	// would be nice to combine these, but I'm keeping them separate
	// for now so we can tune the options separately.
	retryOpts := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	timeout := 5 * time.Second

	var candidates []roachpb.ReplicaDescriptor
	deadline := timeutil.Now().Add(timeout)
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next() && timeutil.Now().Before(deadline); {
		lastReplAdded, lastAddedTime := repl.LastReplicaAdded()
		if timeutil.Since(lastAddedTime) > newReplicaGracePeriod {
			lastReplAdded = 0
		}
		raftStatus := repl.RaftStatus()
		if raftStatus == nil || raftStatus.RaftState != raftpb.StateLeader {
			// If requested, assume all replicas are up-to-date.
			if rp.knobs.AllowVoterRemovalWhenNotLeader {
				candidates = allocatorimpl.FilterUnremovableReplicasWithoutRaftStatus(
					ctx, existingVoters, existingVoters, lastReplAdded)
				break
			}
			// If we've lost raft leadership, we're unlikely to regain it so give up immediately.
			return roachpb.ReplicationTarget{}, "",
				benignerror.New(errors.Errorf("not raft leader while range needs removal"))
		}
		candidates = allocatorimpl.FilterUnremovableReplicas(ctx, raftStatus, existingVoters, lastReplAdded)
		log.KvDistribution.VEventf(ctx, 3, "filtered unremovable replicas from %v to get %v as candidates for removal: %s",
			existingVoters, candidates, rangeRaftProgress(raftStatus, existingVoters))
		if len(candidates) > 0 {
			break
		}
		if len(raftStatus.Progress) <= 2 {
			// HACK(bdarnell): Downreplicating to a single node from
			// multiple nodes is not really supported. There are edge
			// cases in which the two peers stop communicating with each
			// other too soon and we don't reach a satisfactory
			// resolution. However, some tests (notably
			// TestRepartitioning) get into this state, and if the
			// replication queue spends its entire timeout waiting for the
			// downreplication to finish the test will time out. As a
			// hack, just fail-fast when we're trying to go down to a
			// single replica.
			break
		}
		// After upreplication, the candidates for removal could still
		// be catching up. The allocator determined that the range was
		// over-replicated, and it's important to clear that state as
		// quickly as we can (because over-replicated ranges may be
		// under-diversified). If we return an error here, this range
		// probably won't be processed again until the next scanner
		// cycle, which is too long, so we retry here.
	}
	if len(candidates) == 0 {
		// If we timed out and still don't have any valid candidates, give up.
		return roachpb.ReplicationTarget{}, "", benignerror.New(
			errors.Errorf(
				"no removable replicas from range that needs a removal: %s",
				rangeRaftProgress(repl.RaftStatus(), existingVoters),
			),
		)
	}

	return rp.allocator.RemoveVoter(
		ctx,
		rp.storePool,
		conf,
		candidates,
		existingVoters,
		existingNonVoters,
		rp.allocator.ScorerOptions(ctx),
	)
}

func (rp ReplicaPlanner) removeVoter(
	ctx context.Context,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (op AllocationOp, stats ReplicateStats, _ error) {
	removeVoter, details, err := rp.findRemoveVoter(ctx, repl, conf, existingVoters, existingNonVoters)
	if err != nil {
		return nil, stats, err
	}

	transferOp, err := rp.maybeTransferLeaseAwayTarget(
		ctx, repl, desc, conf, removeVoter.StoreID)
	if err != nil {
		return nil, stats, err
	}
	// We found a lease transfer opportunity, exit early.
	if transferOp != nil {
		return transferOp, stats, nil
	}
	stats = stats.trackRemoveMetric(allocatorimpl.VoterTarget, allocatorimpl.Alive)

	// Remove a replica.
	log.KvDistribution.Infof(ctx, "removing voting replica %+v due to over-replication: %s",
		removeVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	// TODO(aayush): Directly removing the voter here is a bit of a missed
	// opportunity since we could potentially be 1 non-voter short and the
	// `target` could be a valid store for a non-voter. In such a scenario, we
	// could save a bunch of work by just performing an atomic demotion of a
	// voter.
	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             repl.RangeUsageInfo(),
		Chgs:              kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, removeVoter),
		AllocatorPriority: 0.0, // unused
		Reason:            kvserverpb.ReasonRangeOverReplicated,
		Details:           details,
	}
	return op, stats, nil
}

func (rp ReplicaPlanner) removeNonVoter(
	ctx context.Context,
	repl AllocatorReplica,
	_ *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
) (op AllocationOp, stats ReplicateStats, _ error) {
	removeNonVoter, details, err := rp.allocator.RemoveNonVoter(
		ctx,
		rp.storePool,
		conf,
		existingNonVoters,
		existingVoters,
		existingNonVoters,
		rp.allocator.ScorerOptions(ctx),
	)
	if err != nil {
		return nil, stats, err
	}
	stats = stats.trackRemoveMetric(allocatorimpl.NonVoterTarget, allocatorimpl.Alive)

	log.KvDistribution.Infof(ctx, "removing non-voting replica %+v due to over-replication: %s",
		removeNonVoter, rangeRaftProgress(repl.RaftStatus(), existingVoters))
	target := roachpb.ReplicationTarget{
		NodeID:  removeNonVoter.NodeID,
		StoreID: removeNonVoter.StoreID,
	}

	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             repl.RangeUsageInfo(),
		Chgs:              kvpb.MakeReplicationChanges(roachpb.REMOVE_NON_VOTER, target),
		AllocatorPriority: 0.0, // unused
		Reason:            kvserverpb.ReasonRangeOverReplicated,
		Details:           details,
	}
	return op, stats, nil
}

func (rp ReplicaPlanner) removeDecommissioning(
	ctx context.Context,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	targetType allocatorimpl.TargetReplicaType,
) (op AllocationOp, stats ReplicateStats, _ error) {
	var decommissioningReplicas []roachpb.ReplicaDescriptor
	switch targetType {
	case allocatorimpl.VoterTarget:
		decommissioningReplicas = rp.storePool.DecommissioningReplicas(
			desc.Replicas().VoterDescriptors(),
		)
	case allocatorimpl.NonVoterTarget:
		decommissioningReplicas = rp.storePool.DecommissioningReplicas(
			desc.Replicas().NonVoterDescriptors(),
		)
	default:
		panic(fmt.Sprintf("unknown targetReplicaType: %s", targetType))
	}

	if len(decommissioningReplicas) == 0 {
		return nil, stats, errors.AssertionFailedf("range of %[1]ss %[2]s was identified as having decommissioning %[1]ss, "+
			"but no decommissioning %[1]ss were found", targetType, repl)
	}
	decommissioningReplica := decommissioningReplicas[0]

	transferOp, err := rp.maybeTransferLeaseAwayTarget(
		ctx, repl, desc, conf, decommissioningReplica.StoreID)
	if err != nil {
		return nil, stats, err
	}
	// We found a lease transfer opportunity, exit early.
	if transferOp != nil {
		return transferOp, stats, nil
	}

	stats = stats.trackRemoveMetric(targetType, allocatorimpl.Decommissioning)

	log.KvDistribution.Infof(ctx, "removing decommissioning %s %+v from store", targetType, decommissioningReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  decommissioningReplica.NodeID,
		StoreID: decommissioningReplica.StoreID,
	}

	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             repl.RangeUsageInfo(),
		Chgs:              kvpb.MakeReplicationChanges(targetType.RemoveChangeType(), target),
		AllocatorPriority: 0.0, // unused
		Reason:            kvserverpb.ReasonStoreDecommissioning,
		Details:           "",
	}
	return op, stats, nil
}

func (rp ReplicaPlanner) removeDead(
	ctx context.Context,
	repl AllocatorReplica,
	deadReplicas []roachpb.ReplicaDescriptor,
	targetType allocatorimpl.TargetReplicaType,
) (op AllocationOp, stats ReplicateStats, _ error) {
	if len(deadReplicas) == 0 {
		return nil, stats, errors.AssertionFailedf(
			"range of %[1]s %[2]s was identified as having dead %[1]ss, but no dead %[1]ss were found",
			targetType,
			repl,
		)
	}
	deadReplica := deadReplicas[0]
	stats = stats.trackRemoveMetric(targetType, allocatorimpl.Dead)

	log.KvDistribution.Infof(ctx, "removing dead %s %+v from store", targetType, deadReplica)
	target := roachpb.ReplicationTarget{
		NodeID:  deadReplica.NodeID,
		StoreID: deadReplica.StoreID,
	}

	// NB: When removing a dead voter, we don't check whether to transfer the
	// lease away because if the removal target is dead, it's not the voter being
	// removed (and if for some reason that happens, the removal is simply going
	// to fail).
	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             repl.RangeUsageInfo(),
		Chgs:              kvpb.MakeReplicationChanges(targetType.RemoveChangeType(), target),
		AllocatorPriority: 0.0, // unused
		Reason:            kvserverpb.ReasonStoreDead,
		Details:           "",
	}

	return op, stats, nil
}

func (rp ReplicaPlanner) considerRebalance(
	ctx context.Context,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	existingVoters, existingNonVoters []roachpb.ReplicaDescriptor,
	allocatorPrio float64,
	scatter bool,
) (op AllocationOp, stats ReplicateStats, _ error) {
	// When replica rebalancing is not enabled return early.
	if rp.knobs.DisableReplicaRebalancing {
		return nil, stats, nil
	}

	rebalanceTargetType := allocatorimpl.VoterTarget

	scorerOpts := allocatorimpl.ScorerOptions(rp.allocator.ScorerOptions(ctx))
	if scatter {
		scorerOpts = rp.allocator.ScorerOptionsForScatter(ctx)
	}
	rangeUsageInfo := repl.RangeUsageInfo()
	addTarget, removeTarget, details, ok := rp.allocator.RebalanceVoter(
		ctx,
		rp.storePool,
		conf,
		repl.RaftStatus(),
		existingVoters,
		existingNonVoters,
		rangeUsageInfo,
		storepool.StoreFilterThrottled,
		scorerOpts,
	)
	if !ok {
		// If there was nothing to do for the set of voting replicas on this
		// range, attempt to rebalance non-voters.
		log.KvDistribution.VInfof(ctx, 2, "no suitable rebalance target for voters")
		addTarget, removeTarget, details, ok = rp.allocator.RebalanceNonVoter(
			ctx,
			rp.storePool,
			conf,
			repl.RaftStatus(),
			existingVoters,
			existingNonVoters,
			rangeUsageInfo,
			storepool.StoreFilterThrottled,
			scorerOpts,
		)
		rebalanceTargetType = allocatorimpl.NonVoterTarget
	}

	if !ok {
		log.KvDistribution.VInfof(ctx, 2, "no suitable rebalance target for non-voters")
		return nil, stats, nil
	}
	// If we have a valid rebalance action (ok == true) and we haven't
	// transferred our lease away, find the rebalance changes and return them
	// in an operation.
	chgs, performingSwap, err := ReplicationChangesForRebalance(ctx, desc, len(existingVoters), addTarget,
		removeTarget, rebalanceTargetType)
	if err != nil {
		return nil, stats, err
	}

	stats = stats.trackRebalanceReplicaCount(rebalanceTargetType)
	if performingSwap {
		stats.VoterDemotionsCount++
		stats.NonVoterPromotionsCount++
	}

	log.KvDistribution.Infof(ctx,
		"rebalancing %s %+v to %+v: %s",
		rebalanceTargetType,
		removeTarget,
		addTarget,
		rangeRaftProgress(repl.RaftStatus(), existingVoters))

	op = AllocationChangeReplicasOp{
		LeaseholderStore:  repl.StoreID(),
		Usage:             rangeUsageInfo,
		Chgs:              chgs,
		AllocatorPriority: allocatorPrio,
		Reason:            kvserverpb.ReasonRebalance,
		Details:           details,
	}
	return op, stats, nil
}

// maybeTransferLeaseAwayTarget is called whenever a replica on a given store
// is slated for removal. If the store corresponds to the store of the caller
// (which is very likely to be the leaseholder), then this removal would fail.
// Instead, this method will attempt to transfer the lease away, and returns
// true to indicate to the caller that it should not pursue the current
// replication change further because it is no longer the leaseholder. When the
// returned bool is false, it should continue. On error, the caller should also
// stop
func (rp ReplicaPlanner) maybeTransferLeaseAwayTarget(
	ctx context.Context,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	removeStoreID roachpb.StoreID,
) (op AllocationOp, _ error) {
	if removeStoreID != repl.StoreID() {
		return nil, nil
	}
	usageInfo := repl.RangeUsageInfo()
	// The local replica was selected as the removal target, but that replica
	// is the leaseholder, so transfer the lease instead. We don't check that
	// the current store has too many leases in this case under the
	// assumption that replica balance is a greater concern. Also note that
	// AllocatorRemoveVoter action takes preference over AllocatorConsiderRebalance
	// (rebalancing) which is where lease transfer would otherwise occur. We
	// need to be able to transfer leases in AllocatorRemoveVoter in order to get
	// out of situations where this store is overfull and yet holds all the
	// leases. The fullness checks need to be ignored for cases where
	// a replica needs to be removed for constraint violations.
	target := rp.allocator.TransferLeaseTarget(
		ctx,
		rp.storePool,
		desc,
		conf,
		desc.Replicas().VoterDescriptors(),
		repl,
		usageInfo,
		false, /* forceDecisionWithoutStats */
		allocator.TransferLeaseOptions{
			Goal: allocator.LeaseCountConvergence,
			// NB: This option means that the allocator is asked to not consider the
			// current replica in its set of potential candidates.
			ExcludeLeaseRepl: true,
		},
	)

	if target == (roachpb.ReplicaDescriptor{}) {
		return nil, nil
	}
	log.KvDistribution.Infof(ctx, "transferring away lease to s%d", target.StoreID)

	op = AllocationTransferLeaseOp{
		Source:             repl.StoreID(),
		Target:             target.StoreID,
		Usage:              usageInfo,
		bypassSafetyChecks: false,
	}

	return op, nil
}
