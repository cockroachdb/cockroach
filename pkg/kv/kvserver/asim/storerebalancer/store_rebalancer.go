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
	rebalancerSleeping = iota
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
// not, it performs a state transfer to prelogue.
func (src *storeRebalancerControl) phaseSleep(ctx context.Context, tick time.Time, s state.State) {
	sleepedTick := src.rebalancerState.lastTick.Add(src.settings.LBRebalancingInterval)
	if tick.After(sleepedTick) {
		src.rebalancerState.lastTick = sleepedTick
		src.phasePrologue(ctx, tick, s)
	}
}

// phasePrologue gathers all the necessary state including hot ranges, store
// pool list and thresholds. It synchronously transfers into the leases phase.
func (src *storeRebalancerControl) phasePrologue(
	ctx context.Context, tick time.Time, s state.State,
) {
	src.rebalancerState.rctx = src.makeRebalanceContext(s)
	src.rebalancerState.phase = leaseRebalancing
	src.phaseLeaseRebalancing(ctx, tick, s)
}

func (src *storeRebalancerControl) phaseLeaseRebalancing(
	ctx context.Context, tick time.Time, s state.State,
) {
	for {
		if src.rebalancerState.pendingTransfer != nil {
			done, _, err := src.checkPendingTicket()
			if !done {
				// No more we can do in this tick - we need to wait for the
				// transfer to complete.
				return
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

			src.rebalancerState.pendingTransfer = nil
			src.rebalancerState.pendingTransferTarget = roachpb.ReplicaDescriptor{}
		}

		outcome, candidateReplica, target := src.sr.RebalanceLeases(ctx, src.rebalancerState.rctx)
		if outcome == kvserver.NoRebalanceNeeded {
			src.phaseEpilogue(ctx, tick)
			return
		}

		if outcome == kvserver.NoRebalanceTarget {
			src.sr.TransferToRebalanceRanges(ctx, src.rebalancerState.rctx)
			src.rebalancerState.phase = rangeRebalancing
			src.phaseRangeRebalancing(ctx, tick, s)
			return
		}

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
}

func (src *storeRebalancerControl) phaseRangeRebalancing(
	ctx context.Context, tick time.Time, s state.State,
) {
	for {
		if src.rebalancerState.pendingRelocate != nil {
			done, _, err := src.checkPendingTicket()
			if !done {
				// No more we can do in this tick - we need to wait for the
				// relocate to complete.
				return
			}

			if err == nil {
				// When the rebalance completes without error, update the local
				// store descriptor and continue searching for range
				// rebalancing targets.
				src.sr.PostRangeRebalance(
					src.rebalancerState.rctx,
					src.rebalancerState.pendingRelocate,
					src.rebalancerState.pendingRelocateTargets,
				)
			}

			src.rebalancerState.pendingRelocate = nil
			src.rebalancerState.pendingRelocateTargets = []roachpb.ReplicationTarget{}
		}

		outcome, candidateReplica, voterTargets, nonVoterTargets := src.sr.RebalanceRanges(ctx, src.rebalancerState.rctx)
		if outcome == kvserver.NoRebalanceNeeded {
			src.phaseEpilogue(ctx, tick)
			return
		}

		if outcome == kvserver.NoRebalanceTarget {
			src.phaseEpilogue(ctx, tick)
			return
		}

		relocateOp := op.NewRelocateRangeOp(
			tick,
			candidateReplica.Desc().StartKey.AsRawKey(),
			voterTargets,
			nonVoterTargets,
			true,
		)

		// Dispatch relocate range and update the pending relocate state.
		ticket := src.controller.Dispatch(
			ctx,
			tick,
			s,
			relocateOp,
		)
		src.rebalancerState.pendingRelocate = candidateReplica
		src.rebalancerState.pendingRelocateTargets = voterTargets
		src.rebalancerState.pendingTicket = ticket
	}
}

// phaseEpilogue clears the rebalancing context and updates the last tick
// interval. This transfers into a sleeping phase.
func (src *storeRebalancerControl) phaseEpilogue(ctx context.Context, tick time.Time) {
	src.rebalancerState.phase = rebalancerSleeping
	src.rebalancerState.rctx = nil
	src.rebalancerState.lastTick = tick
}
