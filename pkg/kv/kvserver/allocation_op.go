// Copyright 2022 The Cockroach Authors.
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
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ReplicateQueueChange is a planned change, associated with an allocator
// action. It originates from the replicate queue planner and is used to apply
// an allocation step.
type ReplicateQueueChange struct {
	Action allocatorimpl.AllocatorAction
	// ForceRequeue is set when the replica associated with the
	// ReplicateQueueChange should be requeued regardless of whether an error
	// was returned in planning or application of a planned change.
	ForceRequeue bool
	// DryRun indicates that the change was generated for a dry run and should
	// not be acted upon.
	DryRun bool
	// Step is a planned operation associated with a replica.
	Step *AllocationStep
}

// AllocationStep represents an operation to be applied for a specific replica.
type AllocationStep struct {
	// Replica is the replica of the range associated with the allocation step.
	// At the moment it must be the case that the replica is also the
	// leaseholder of the range.
	Replica *Replica
	// Op is an allocation operation to be applied.
	Op AllocationOp
}

// AllocationOp represents an atomic allocation operation to be applied against
// some replica.
//
// TODO(kvoli): Add AllocationRelocateRangeOp and AllocationFinalizeChangeOp.
type AllocationOp interface{}

// AllocationTransferLeaseOp represents an operation to transfer a range lease to another
// store, from the current one.
type AllocationTransferLeaseOp struct {
	target             roachpb.StoreID
	QPS                float64
	bypassSafetyChecks bool
}

// AllocationChangeReplicasOp represents an operation to execute a change replicas txn.
type AllocationChangeReplicasOp struct {
	chgs              roachpb.ReplicationChanges
	priority          kvserverpb.SnapshotRequest_Priority
	allocatorPriority float64
	reason            kvserverpb.RangeLogEventReason
	details           string
}

// AllocationNoop represents no operation.
type AllocationNoop struct{}

// applyChange applies a range allocation change. It is responsible only for
// application and returns an error if unsuccessful.
//
// TODO(kvoli): Currently applyChange is only called by the replicate queue. It
// is desirable to funnel all allocation changes via one function. Move this
// application phase onto a separate, struct that will be used by both the
// replicate queue and the store rebalancer.
func (rq *replicateQueue) applyChange(ctx context.Context, change *ReplicateQueueChange) error {
	if change.DryRun {
		return nil
	}

	step := change.Step
	var err error
	switch op := step.Op.(type) {
	case *AllocationNoop:
		// Nothing to do.
	case *AllocationTransferLeaseOp:
		err = rq.TransferLease(ctx, step.Replica, step.Replica.StoreID(), op.target, op.QPS)
	case *AllocationChangeReplicasOp:
		err = rq.changeReplicas(
			ctx,
			step.Replica,
			op.chgs,
			step.Replica.Desc(),
			op.priority,
			op.allocatorPriority,
			op.reason,
			op.details,
			change.DryRun,
		)
	default:
		panic(fmt.Sprintf("Unknown operation %+v, unable to apply replicate queue change", op))
	}

	return err
}

// ShouldRequeue determines whether a replica should be requeued into the
// replicate queue, using the planned change and error returned from either
// application or planning.
func (rq *replicateQueue) ShouldRequeue(
	ctx context.Context, change *ReplicateQueueChange, err error,
) bool {
	var requeue bool

	if change.ForceRequeue {
		// Override the logic below to force requeueing on errors or despite
		// the operation type. This is used to special case scenarios such as
		// unexpected state in the replicate queue processing.
		requeue = true

	} else if _, ok := change.Step.Op.(AllocationNoop); ok {
		// Don't requeue on a noop, as the replica had nothing to do the first
		// time around.
		requeue = false

	} else if err != nil {
		// Don't requeue the replica when an error was returned, as it is
		// likely that a similar error would be returned again unless the
		// cluster state had changed.
		requeue = false

	} else if change.Action == allocatorimpl.AllocatorConsiderRebalance {
		// Don't requeue after a successful rebalance operation.
		requeue = false

	} else if lhRemoved(ctx, change, err) {
		// Don't requeue if the leaseholder was removed as a voter or the range
		// lease was transferred away.
		requeue = false

	} else {
		// Otherwise, requeue to see if there is more work to do. As the
		// operation succeeded and was planned for a repair action i.e. not
		// rebalancing.
		requeue = true
	}

	return requeue
}

// TrackChangeOutcome updates the metrics and local state to reflect the
// success or failure of a replicate queue change. TrackChangeOutcome returns
// true if the replica should be requeued, false otherwise.
func (rq *replicateQueue) TrackChangeOutcome(
	ctx context.Context, change *ReplicateQueueChange, err error,
) (requeue bool) {
	// TODO(kvoli): The results tracking currently ignore what operation was
	// planned and instead adopts the allocator action to update the metrics.
	// In cases where the action was AllocatorRemoveX, yet a lease transfer
	// operation was returned, it will treat it as a successful or failed
	// AllocatorRemoveX. This is despite no operation to remove a replica
	// having occurred on this store. This should be updated to accurately
	// reflect which operation was applied.
	rq.metrics.trackResultByAllocatorAction(ctx, change.Action, err, change.DryRun)

	// Update the local state of the storepool if the action did not return an
	// error and this is not a dry run.
	if err == nil && !change.DryRun {
		UpdateStorePoolAfterStep(rq.allocator.StorePool, change.Step)
	}

	return rq.ShouldRequeue(ctx, change, err)
}

func lhRemoved(ctx context.Context, change *ReplicateQueueChange, err error) bool {
	// When an error has occurred the leaseholder could not have been removed,
	// as the change was not applied successfully.
	if err != nil {
		return false
	}
	// If a lease transfer was applied successfully, the leaseholder has been
	// removed.
	if _, ok := change.Step.Op.(*AllocationTransferLeaseOp); ok {
		return true
	}

	// If the replica on this store (leaseholder) was removed as a voter, then
	// it must be the case that the lease was also transferred in a step.
	if op, ok := change.Step.Op.(*AllocationChangeReplicasOp); ok {
		for _, chg := range op.chgs.VoterRemovals() {
			if chg.StoreID == change.Step.Replica.StoreID() {
				return true
			}
		}
	}

	return false
}

// UpdateStorePoolAfterStep updates the local storepool to reflect the
// application of a step. Currently this only supports
// AllocationChangeReplicasOp.
func UpdateStorePoolAfterStep(storepool *storepool.StorePool, step *AllocationStep) {
	switch op := step.Op.(type) {
	case AllocationNoop:
	// Nothing to do.
	case AllocationTransferLeaseOp:
	// Nothing to do
	//
	// TODO(kvoli): Currently the local storepool is updated directly in the
	// lease transfer call, rather than in this function. Move the storepool
	// tracking from rq.TransferLease to this function once #89771 is merged.
	case AllocationChangeReplicasOp:
		rangeUsageInfo := rangeUsageInfoForRepl(step.Replica)
		for _, chg := range op.chgs {
			storepool.UpdateLocalStoreAfterRebalance(
				chg.Target.StoreID, rangeUsageInfo, chg.ChangeType)
		}
	}
}
