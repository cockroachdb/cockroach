// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storerebalancer

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/op"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"go.etcd.io/etcd/raft/v3"
)

// storeRebalancerPhase represents the current phase the store rebalancer is
// in. The phase dictates what rebalancing type will be attempted.
type storeRebalancerPhase int

const (
	// rebalancerSleeping indicates that the store rebalancer is not in a
	// rebalance loop nor has any pending operations. It transfers out of this
	// state when the sleep interval has completed.
	rebalancerSleeping storeRebalancerPhase = iota
	// leaseRebalancing indicates that the store rebalancer is searching for or
	// waiting on lease rebalancing.
	leaseRebalancing
	// rangeRebalancing indicates that the store rebalancer is searching for or
	// waiting on range (replica+lease) rebalancing.
	rangeRebalancing
)

// StoreRebalancer is a tickable actor which scans the replicas on the store
// associated with it and attempts to perform lease, then, range rebalancing.
type StoreRebalancer interface {
	Tick(context.Context, time.Time, state.State)
}

// storeRebalancerState mantains the store rebalancer state used in the three
// phases. It maintains these over tick boundaries for tracking multi-tick
// operations such as relocations and tracking which phase the store rebalancer
// was last in.
type storeRebalancerState struct {
	phase storeRebalancerPhase
	rctx  *kvserver.RebalanceContext

	pendingRelocate, pendingTransfer kvserver.CandidateReplica
	pendingRelocateTargets           []roachpb.ReplicationTarget
	pendingRelocateExistingVoters    []roachpb.ReplicaDescriptor
	pendingTransferTarget            roachpb.ReplicaDescriptor

	pendingTicket op.DispatchedTicket
	lastTick      time.Time
}

type storeRebalancerControl struct {
	storeID         state.StoreID
	settings        *config.SimulationSettings
	sr              *kvserver.StoreRebalancer
	rebalancerState *storeRebalancerState

	allocator  allocatorimpl.Allocator
	controller op.Controller
}

// NewStoreRebalancer returns a new simulator store rebalancer.
func NewStoreRebalancer(
	start time.Time,
	storeID state.StoreID,
	controller op.Controller,
	allocator allocatorimpl.Allocator,
	settings *config.SimulationSettings,
	getRaftStatusFn func(replica kvserver.CandidateReplica) *raft.Status,
) StoreRebalancer {
	return newStoreRebalancerControl(start, storeID, controller, allocator, settings, getRaftStatusFn)
}

func newStoreRebalancerControl(
	start time.Time,
	storeID state.StoreID,
	controller op.Controller,
	allocator allocatorimpl.Allocator,
	settings *config.SimulationSettings,
	getRaftStatusFn func(replica kvserver.CandidateReplica) *raft.Status,
) *storeRebalancerControl {
	sr := kvserver.SimulatorStoreRebalancer(
		roachpb.StoreID(storeID),
		allocator,
		getRaftStatusFn,
	)

	return &storeRebalancerControl{
		sr:       sr,
		settings: settings,
		rebalancerState: &storeRebalancerState{
			lastTick: start.Add(-settings.LBRebalancingInterval),
		},
		storeID:    storeID,
		allocator:  allocator,
		controller: controller,
	}

}

func (src *storeRebalancerControl) scorerOptions() *allocatorimpl.QPSScorerOptions {
	return &allocatorimpl.QPSScorerOptions{
		StoreHealthOptions:    allocatorimpl.StoreHealthOptions{},
		Deterministic:         true,
		QPSRebalanceThreshold: src.settings.LBRebalanceQPSThreshold,
		MinRequiredQPSDiff:    src.settings.LBMinRequiredQPSDiff,
	}
}

func (src *storeRebalancerControl) makeRebalanceContext(
	state state.State,
) *kvserver.RebalanceContext {
	allStoresList, _, _ := src.allocator.StorePool.GetStoreList(storepool.StoreFilterSuspect)
	options := src.scorerOptions()

	store, ok := state.Store(src.storeID)
	if !ok {
		return nil
	}
	localDesc := store.Descriptor()

	return kvserver.NewRebalanceContext(
		&localDesc,
		options,
		kvserver.LBRebalancingMode(src.settings.LBRebalancingMode),
		allocatorimpl.OverfullQPSThreshold(options, allStoresList.CandidateQueriesPerSecond.Mean),
		allStoresList.ToMap(),
		allStoresList,
		hottestRanges(state, src.storeID),
		[]kvserver.CandidateReplica{},
	)
}

func (src *storeRebalancerControl) checkPendingTicket() (done bool, next time.Time, _ error) {
	ticket := src.rebalancerState.pendingTicket
	op, ok := src.controller.Check(ticket)
	if !ok {
		return true, time.Time{}, nil
	}
	done, next = op.Done()
	if !done {
		return false, op.Next(), nil
	}
	return true, next, op.Errors()
}

func (src *storeRebalancerControl) Tick(ctx context.Context, tick time.Time, state state.State) {
	switch src.rebalancerState.phase {
	case rebalancerSleeping:
		src.phaseSleep(ctx, tick, state)
	case leaseRebalancing:
		src.phaseLeaseRebalancing(ctx, tick, state)
	case rangeRebalancing:
		src.phaseRangeRebalancing(ctx, tick, state)
	}
}

// phaseSleep checks whether the store rebalancer should continue sleeping. If
// not, it performs a state transfer to prologue.
func (src *storeRebalancerControl) phaseSleep(ctx context.Context, tick time.Time, s state.State) {
	sleepedTick := src.rebalancerState.lastTick.Add(src.settings.LBRebalancingInterval)
	if tick.After(sleepedTick) {
		src.rebalancerState.lastTick = sleepedTick
		src.phasePrologue(ctx, tick, s)
	}
}

// phasePrologue gathers all the necessary state including hot ranges, store
// pool list and thresholds. It synchronously transfers into the leases phase
// if it passes the should rebalance store check, otherwise it transfers
// directly into the epilogue phase.
func (src *storeRebalancerControl) phasePrologue(
	ctx context.Context, tick time.Time, s state.State,
) {
	rctx := src.makeRebalanceContext(s)
	if !src.sr.ShouldRebalanceStore(ctx, rctx) {
		src.phaseEpilogue(ctx, tick)
		return
	}

	src.rebalancerState.rctx = rctx
	src.rebalancerState.phase = leaseRebalancing
	src.phaseLeaseRebalancing(ctx, tick, s)
}

func (src *storeRebalancerControl) checkPendingLeaseRebalance() bool {
	// No pending lease rebalance, we can continue to searching for targets.
	if src.rebalancerState.pendingTransfer == nil {
		return true
	}

	done, _, err := src.checkPendingTicket()
	if !done {
		// No more we can do in this tick - we need to wait for the
		// transfer to complete.
		return false
	}

	if err == nil {
		// The transfer has completed without error, update the local
		// state to reflect it's success.
		src.sr.PostLeaseRebalance(
			src.rebalancerState.rctx,
			src.rebalancerState.pendingTransfer,
			src.rebalancerState.pendingTransferTarget,
		)
	}

	// Any pending transfer completed, either by succeeding or failing - we may
	// continue searching for transfer targets now.
	src.rebalancerState.pendingTicket = -1
	src.rebalancerState.pendingTransfer = nil
	src.rebalancerState.pendingTransferTarget = roachpb.ReplicaDescriptor{}
	return true
}

func (src *storeRebalancerControl) applyLeaseRebalance(
	ctx context.Context,
	tick time.Time,
	s state.State,
	candidateReplica kvserver.CandidateReplica,
	target roachpb.ReplicaDescriptor,
) {
	transferOp := op.NewTransferLeaseOp(
		tick,
		candidateReplica.GetRangeID(),
		candidateReplica.StoreID(),
		target.StoreID,
		candidateReplica.QPS(),
	)

	// Dispatch the transfer and updating the pending transfer state.
	ticket := src.controller.Dispatch(ctx, tick, s, transferOp)
	src.rebalancerState.pendingTransfer = candidateReplica
	src.rebalancerState.pendingTransferTarget = target
	src.rebalancerState.pendingTicket = ticket
}

func (src *storeRebalancerControl) phaseLeaseRebalancing(
	ctx context.Context, tick time.Time, s state.State,
) {
	for {
		// Check the pending transfer state, if we can't continue to searching
		// for targets return early.
		if !src.checkPendingLeaseRebalance() {
			return
		}

		outcome, candidateReplica, target := src.sr.RebalanceLeases(ctx, src.rebalancerState.rctx)
		if outcome == kvserver.NoRebalanceNeeded || outcome == kvserver.NoRebalanceTarget {
			break
		}

		// applyLeaseRebalance applies the lease rebalance found, updating the
		// pending state.
		src.applyLeaseRebalance(ctx, tick, s, candidateReplica, target)
	}

	// Check whether we transfer to range based rebalance, after trying lease
	// based rebalancing.
	if src.sr.TransferToRebalanceRanges(ctx, src.rebalancerState.rctx) {
		src.rebalancerState.phase = rangeRebalancing
		src.phaseRangeRebalancing(ctx, tick, s)
		return
	}

	// No more rebalancing should be attempted, move to epilogue and clear the
	// rebalancer state.
	src.phaseEpilogue(ctx, tick)
}

func (src *storeRebalancerControl) checkPendingRangeRebalance() bool {
	// No pending range rebalance, we can continue to searching for targets.
	if src.rebalancerState.pendingRelocate == nil {
		return true
	}

	done, _, err := src.checkPendingTicket()
	if !done {
		// No more we can do in this tick - we need to wait for the
		// relocate to complete.
		return false
	}

	if err == nil {
		// When the rebalance completes without error, update the local
		// store descriptor and continue searching for range
		// rebalancing targets.
		src.sr.PostRangeRebalance(
			src.rebalancerState.rctx,
			src.rebalancerState.pendingRelocate,
			src.rebalancerState.pendingRelocateTargets,
			[]roachpb.ReplicationTarget{}, /* non-voter targets */
			src.rebalancerState.pendingRelocateExistingVoters,
			[]roachpb.ReplicaDescriptor{}, /* old non-voters */
		)
	}

	// Any range rebalance has completed, either by succeeding or failing - we
	// may continue searching for range rebalance targets now.
	src.rebalancerState.pendingTicket = -1
	src.rebalancerState.pendingRelocate = nil
	src.rebalancerState.pendingRelocateTargets = []roachpb.ReplicationTarget{}
	return true
}

func (src *storeRebalancerControl) applyRangeRebalance(
	ctx context.Context,
	tick time.Time,
	s state.State,
	candidateReplica kvserver.CandidateReplica,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
) {
	relocateOp := op.NewRelocateRangeOp(
		tick,
		candidateReplica.Desc().StartKey.AsRawKey(),
		voterTargets,
		nonVoterTargets,
		true,
	)

	// Dispatch the relocate range op and update the pending range rebalance
	// state.
	ticket := src.controller.Dispatch(ctx, tick, s, relocateOp)
	src.rebalancerState.pendingRelocate = candidateReplica
	src.rebalancerState.pendingRelocateTargets = voterTargets
	src.rebalancerState.pendingRelocateExistingVoters = candidateReplica.Desc().Replicas().VoterDescriptors()
	src.rebalancerState.pendingTicket = ticket
}

func (src *storeRebalancerControl) phaseRangeRebalancing(
	ctx context.Context, tick time.Time, s state.State,
) {
	for {
		// Check the pending range rebalance state, if we can't continue to
		// searching for targets return early.
		if !src.checkPendingRangeRebalance() {
			return
		}

		outcome, candidateReplica, voterTargets, nonVoterTargets := src.sr.RebalanceRanges(ctx, src.rebalancerState.rctx)
		if outcome == kvserver.NoRebalanceNeeded || outcome == kvserver.NoRebalanceTarget {
			break
		}
		// applyRangeRebalance applies the range rebalance found, updating the
		// pending state.
		src.applyRangeRebalance(ctx, tick, s, candidateReplica, voterTargets, nonVoterTargets)
	}

	// Log the rebalancing outcome, we ignore whether we were succesful or not,
	// as it doesn't change the period we will wait before searching for
	// balancing targets again. Move to epilogue and cleanup.
	src.sr.LogRangeRebalanceOutcome(ctx, src.rebalancerState.rctx)
	src.phaseEpilogue(ctx, tick)
}

// phaseEpilogue clears the rebalancing context and updates the last tick
// interval. This transfers into a sleeping phase.
func (src *storeRebalancerControl) phaseEpilogue(ctx context.Context, tick time.Time) {
	src.rebalancerState.phase = rebalancerSleeping
	src.rebalancerState.rctx = nil
	src.rebalancerState.lastTick = tick
}
