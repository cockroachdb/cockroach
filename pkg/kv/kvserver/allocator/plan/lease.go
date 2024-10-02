// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plan

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// LeasePlanner implements the ReplicationPlanner interface.
type LeasePlanner struct {
	storePool storepool.AllocatorStorePool
	allocator allocatorimpl.Allocator
}

var _ ReplicationPlanner = &LeasePlanner{}

// NewLeasePlanner returns a new LeasePlanner which implements the
// ReplicationPlanner interface.
func NewLeasePlanner(
	allocator allocatorimpl.Allocator, storePool storepool.AllocatorStorePool,
) LeasePlanner {
	return LeasePlanner{
		storePool: storePool,
		allocator: allocator,
	}
}

// ShouldPlanChange determines whether a lease transfer should be planned for
// the range the replica belongs to. The relative priority is also returned.
// The only planned changes produced by the lease queue are lease transfers.
func (lp LeasePlanner) ShouldPlanChange(
	ctx context.Context,
	now hlc.ClockTimestamp,
	repl AllocatorReplica,
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	opts PlannerOptions,
) (shouldPlanChange bool, priority float64) {
	leaseStatus := repl.LeaseStatusAt(ctx, now)
	if !leaseStatus.IsValid() {
		// The range has an invalid lease. If this replica is the raft leader then
		// we'd like it to hold a valid lease. We enqueue it regardless of being a
		// leader or follower, where the leader at the time of processing will
		// succeed.
		//
		// Note that the lease planner won't plan a change which acquires the
		// invalid lease, it will in pre-processing, see
		// queue.replicaCanBeProcessed.
		log.KvDistribution.VEventf(ctx, 2, "invalid lease, enqueuing")
		return true, 0
	}
	if leaseStatus.OwnedBy(repl.StoreID()) && !repl.HasCorrectLeaseType(leaseStatus.Lease) {
		// This replica holds (or held) an incorrect lease type, switch it to the
		// correct type. Typically when changing kv.expiration_leases_only.enabled
		// or kv.raft.leader_fortification.fraction_enabled. Although this logic
		// also exists as part of the raft tick loop, it won't be called for
		// quiesced replicas so is necessary here as well.
		//
		// Similar to above, the lease planner won't switch the lease type, this is
		// done in plan.replicaCanBeProcessed.
		log.KvDistribution.VEventf(ctx, 2, "incorrect lease type, enqueueing")
		return true, 0
	}

	if !opts.CanTransferLease {
		log.VEventf(ctx, 3, "can't transfer lease, not enqueueing")
		return false, 0
	}

	decision := lp.allocator.ShouldTransferLease(
		ctx, lp.storePool, desc, conf, desc.Replicas().VoterDescriptors(),
		repl, repl.RangeUsageInfo())

	if decision.ShouldTransfer() {
		log.KvDistribution.VEventf(ctx,
			2, "lease transfer needed %v, enqueuing", decision)
		return true, decision.Priority()
	}

	log.KvDistribution.VEventf(ctx,
		3, "no lease transfer needed %v, not enqueueing", decision)
	return false, 0
}

// PlanOneChange calls the allocator to determine if the lease should be
// transferred for the range and if so, which existing voting replica should be
// the new leaseholder. If successful in finding a target, the lease transfer
// is returned as a ReplicateChange.
func (lp LeasePlanner) PlanOneChange(
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

	// If there range were unavailable, then a lease transfer wouldn't succeed.
	// When the range needs to finalize an atomic replication change, avoid
	// transferring the lease which could delay completing the change.
	action, _ := lp.allocator.ComputeAction(ctx, lp.storePool, conf, desc)
	switch action {
	case allocatorimpl.AllocatorRangeUnavailable, allocatorimpl.AllocatorFinalizeAtomicReplicationChange:
		log.KvDistribution.VEventf(ctx,
			3, "range %s, can't transfer lease", action)
		return change, nil
	default:
	}

	if !opts.CanTransferLease {
		log.VEventf(ctx, 3, "can't transfer lease, not enqueueing")
		return change, nil
	}

	usage := repl.RangeUsageInfo()
	existingVoters := desc.Replicas().VoterDescriptors()
	// Learner replicas aren't allowed to become the leaseholder or raft leader,
	// so only consider the `VoterDescriptors` replicas.
	target := lp.allocator.TransferLeaseTarget(
		ctx,
		lp.storePool,
		desc,
		conf,
		existingVoters,
		repl,
		usage,
		false, /* forceDecisionWithoutStats */
		allocator.TransferLeaseOptions{
			Goal:                   allocator.FollowTheWorkload,
			ExcludeLeaseRepl:       false,
			CheckCandidateFullness: true,
		},
	)
	if target == (roachpb.ReplicaDescriptor{}) {
		// If we don't find a suitable target, but we own a lease violating the
		// lease preferences, and there is a more suitable target, return an error
		// to place the replica in purgatory and retry sooner. This typically
		// happens when we've just acquired a violating lease and we eagerly
		// enqueue the replica before we've received Raft leadership, which
		// prevents us from finding appropriate lease targets since we can't
		// determine if any are behind.
		if lp.allocator.LeaseholderShouldMoveDueToPreferences(
			ctx,
			lp.storePool,
			conf,
			repl,
			existingVoters,
			false, /* excludeReplsInNeedOfSnap */
		) {
			return change, CantTransferLeaseViolatingPreferencesError{RangeID: desc.RangeID}
		}
		// There is no target and no more preferred leaseholders, a no-op.
		return change, nil
	}

	change.Op = AllocationTransferLeaseOp{
		Source:             repl.StoreID(),
		Target:             target.StoreID,
		Usage:              usage,
		bypassSafetyChecks: false,
	}
	return change, nil
}

// CantTransferLeaseViolatingPreferencesError is an error returned when a lease
// violates the lease preferences, but we couldn't find a valid target to
// transfer the lease to. It indicates that the replica should be sent to
// purgatory, to retry the transfer faster.
type CantTransferLeaseViolatingPreferencesError struct {
	RangeID roachpb.RangeID
}

var _ errors.SafeFormatter = CantTransferLeaseViolatingPreferencesError{}

func (e CantTransferLeaseViolatingPreferencesError) Error() string { return fmt.Sprint(e) }

func (e CantTransferLeaseViolatingPreferencesError) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e CantTransferLeaseViolatingPreferencesError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("can't transfer r%d lease violating preferences, no suitable target", e.RangeID)
	return nil
}

func (CantTransferLeaseViolatingPreferencesError) PurgatoryErrorMarker() {}
