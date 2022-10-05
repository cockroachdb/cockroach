// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queue

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/storerebalancer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type replicateQueue struct {
	baseQueue
	allocator           allocatorimpl.Allocator
	pendingChange       *state.ReplicaChange
	pendingChangeLoad   *allocator.RangeUsageInfo
	pendingTransfer     *state.LeaseTransferChange
	pendingTransferLoad *allocator.RangeUsageInfo
	lastLeaseTransfer   time.Time
}

// NewReplicateQueue returns a new replicate queue.
func NewReplicateQueue(
	storeID state.StoreID,
	stateChanger state.Changer,
	allocator allocatorimpl.Allocator,
	start time.Time,
	settings *config.SimulationSettings,
) RangeQueue {
	return &replicateQueue{
		baseQueue: baseQueue{
			priorityQueue: priorityQueue{items: make([]*replicaItem, 0, 1)},
			storeID:       storeID,
			stateChanger:  stateChanger,
			next:          start,
			settings:      settings,
		},
		allocator: allocator,
	}
}

// MaybeAdd proposes a replica for inclusion into the ReplicateQueue, if it
// meets the criteria it is enqueued. The criteria is currently if the
// allocator returns a non-noop, then the replica is added.
func (rq *replicateQueue) MaybeAdd(
	ctx context.Context, replica state.Replica, state state.State,
) bool {
	if !rq.settings.ReplicateQueueEnabled {
		return false
	}

	rng, ok := state.Range(replica.Range())
	if !ok {
		panic("unexpected no range found")
	}

	action, priority := rq.allocator.ComputeAction(ctx, rng.SpanConfig(), rng.Descriptor())
	if action == allocatorimpl.AllocatorNoop {
		return false
	}

	heap.Push(
		rq,
		&replicaItem{
			rangeID:   roachpb.RangeID(replica.Range()),
			replicaID: replica.Descriptor().ReplicaID,
			priority:  priority,
		},
	)
	return true
}

// Tick processes updates in the ReplicateQueue. Only one replica is
// processed at a time and the duration taken to process a replica depends
// on the action taken. Replicas in the queue are processed in order of
// priority, then in FIFO order on ties. The Tick function currently only
// supports processing ConsiderRebalance actions on replicas.
// TODO(kvoli,lidorcarmel): Support taking additional actions, beyond consider
// rebalance.
func (rq *replicateQueue) Tick(ctx context.Context, tick time.Time, s state.State) {
	// Nothing to do, queue is disabled.
	if !rq.settings.ReplicateQueueEnabled {
		return
	}

	// If this tick is before the completion time of the pending actions taken
	// by this replicate queue, return early as there's nothing to do yet.
	if tick.Before(rq.next) {
		return
	}

	// Check if there are any pending transfers and if so update the local
	// state to reflect their completion.
	rq.checkPendingTransfer(tick)
	// Check if there are any pending changes and if so update the local state
	// to reflect their completion. This call will also attempt to transfer the
	// lease away if possible, return early if that is the case.
	rq.checkPendingChanges(ctx, tick, s)

	for rq.priorityQueue.Len() != 0 && !rq.next.After(tick) {
		item := heap.Pop(rq).(*replicaItem)
		if item == nil {
			continue
		}

		rng, ok := s.Range(state.RangeID(item.rangeID))
		if !ok {
			panic("unexpected no range found")
		}

		action, _ := rq.allocator.ComputeAction(ctx, rng.SpanConfig(), rng.Descriptor())

		switch action {
		case allocatorimpl.AllocatorConsiderRebalance:
			rq.considerRebalance(ctx, rq.next, rng, s)
		case allocatorimpl.AllocatorNoop:
		default:
			log.Infof(ctx, "s%d: allocator action %s for range %s is unsupported by the simulator "+
				"replicate queue, ignoring.", rq.storeID, action, rng)
		}

		// Try shedding the lease if we did nothing else for this range.
		if !rq.next.After(tick) {
			rq.maybeShedLease(ctx, tick, rng, s)
		}
	}

}

// checkPendingChanges checks whether a pending change exists, if so it updates
// the local state to reflect the changes success. This function will then try
// to transfer the successful changes lease away, if possible it returns true,
// otherwise false.
func (rq *replicateQueue) checkPendingChanges(ctx context.Context, tick time.Time, s state.State) {
	if rq.pendingChange == nil {
		return
	}

	// Update the local storepool state after the rebalance.
	rq.allocator.StorePool.UpdateLocalStoreAfterRebalance(
		roachpb.StoreID(rq.pendingChange.Add),
		*rq.pendingChangeLoad,
		roachpb.ADD_VOTER,
	)

	rq.allocator.StorePool.UpdateLocalStoreAfterRebalance(
		roachpb.StoreID(rq.pendingChange.Remove),
		*rq.pendingChangeLoad,
		roachpb.REMOVE_VOTER,
	)

	pendingRangeID := rq.pendingChange.RangeID
	rq.pendingChange = nil
	rq.pendingChangeLoad = nil

	// Now try shedding this lease after the change.
	rng, ok := s.Range(pendingRangeID)
	if !ok {
		panic("unexpected no range found")
	}
	rq.maybeShedLease(ctx, tick, rng, s)
}

// checkPendingTransfer checks whether a pending transfer exists, if so it
// updates the local state to refelct the transfers success.
func (rq *replicateQueue) checkPendingTransfer(tick time.Time) {
	if rq.pendingTransfer == nil {
		return
	}

	// Update the local state after the transfer.
	rq.allocator.StorePool.UpdateLocalStoresAfterLeaseTransfer(
		roachpb.StoreID(rq.pendingTransfer.Author),
		roachpb.StoreID(rq.pendingTransfer.TransferTarget),
		rq.pendingTransferLoad.QueriesPerSecond,
	)

	rq.pendingTransfer = nil
	rq.pendingTransferLoad = nil
}

// considerRebalance simulates the logic of the replicate queue when given a
// considerRebalance action. It will first ask the allocator for add and remove
// targets for a range. It will then enqueue the replica change into the state
// changer and update the time to process the next replica, with the completion
// time returned.
func (rq *replicateQueue) considerRebalance(
	ctx context.Context, tick time.Time, rng state.Range, s state.State,
) {
	load := s.ReplicaLoad(rng.RangeID(), rq.storeID).Load()
	add, remove, _, ok := rq.allocator.RebalanceVoter(
		ctx,
		rng.SpanConfig(),
		nil, /* raftStatus */
		rng.Descriptor().Replicas().VoterDescriptors(),
		rng.Descriptor().Replicas().NonVoterDescriptors(),
		load,
		storepool.StoreFilterNone,
		rq.allocator.ScorerOptions(ctx),
	)

	// We were unable to find a rebalance target for the range.
	if !ok {
		return
	}

	// There was a rebalance target but there was no way to add the replica,
	// attempt transferring the lease away.
	if add == (roachpb.ReplicationTarget{}) {
		rq.maybeTransferLeaseAway(ctx, tick, rng, s)
		return
	}

	// Enqueue the change to be processed and update when the next replica
	// processing can occur with the completion time of the change.
	// 	NB: This limits concurrency to at most one change at a time per
	// 	ReplicateQueue.
	change := state.ReplicaChange{
		RangeID: state.RangeID(rng.Descriptor().RangeID),
		Add:     state.StoreID(add.StoreID),
		Remove:  state.StoreID(remove.StoreID),
		Wait:    rq.settings.ReplicaChangeDelayFn()(rng.Size(), true),
		Author:  rq.storeID,
	}

	if completeAt, ok := rq.stateChanger.Push(tick, &change); ok {
		rq.pendingChange = &change
		rq.pendingChangeLoad = &load
		rq.next = completeAt
		return
	}
}

func (rq *replicateQueue) maybeTransferLeaseAway(
	ctx context.Context, tick time.Time, rng state.Range, s state.State,
) {
	rq.shedLease(ctx, tick, rng, s,
		allocator.TransferLeaseOptions{
			Goal:             allocator.LeaseCountConvergence,
			DryRun:           false,
			ExcludeLeaseRepl: true,
		},
	)
}

func (rq *replicateQueue) maybeShedLease(
	ctx context.Context, tick time.Time, rng state.Range, s state.State,
) {
	rq.shedLease(ctx, tick, rng, s,
		allocator.TransferLeaseOptions{
			Goal:                   allocator.FollowTheWorkload,
			ExcludeLeaseRepl:       false,
			CheckCandidateFullness: true,
			DryRun:                 false,
		},
	)
}

func (rq *replicateQueue) shedLease(
	ctx context.Context,
	tick time.Time,
	rng state.Range,
	s state.State,
	opts allocator.TransferLeaseOptions,
) {
	// If transfers are not enabled, nothing to do.
	if !rq.settings.ReplQueueTransfersEnabled {
		return
	}

	repl, ok := s.LeaseHolderReplica(rng.RangeID())
	// No longer the leaseholder, cannot transfer lease.
	if !ok || repl.StoreID() != rq.storeID {
		return
	}

	// It has been too soon since the last lease transfer from this replicate
	// queue, skip.
	if tick.Before(rq.lastLeaseTransfer.Add(1 * time.Second)) {
		return
	}

	candidateRepl := storerebalancer.NewSimulatorReplica(repl, s)
	target := rq.allocator.TransferLeaseTarget(
		ctx,
		rng.SpanConfig(),
		rng.Descriptor().Replicas().Descriptors(),
		candidateRepl,
		candidateRepl.Stats(),
		false,
		opts,
	)

	// No rebalance candidate or candidate is the current leaseholder, skip.
	if target == (roachpb.ReplicaDescriptor{}) || target.StoreID == candidateRepl.StoreID() {
		return
	}

	change := &state.LeaseTransferChange{
		RangeID:        rng.RangeID(),
		TransferTarget: state.StoreID(target.StoreID),
		Author:         rq.storeID,
		Wait:           rq.settings.ReplicaChangeBaseDelay,
	}

	if next, ok := rq.stateChanger.Push(tick, change); ok {
		rq.next = next
		usage := candidateRepl.RangeUsageInfo()
		rq.pendingTransfer = change
		rq.pendingTransferLoad = &usage
		rq.lastLeaseTransfer = tick
	}
}
