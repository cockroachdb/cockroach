// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/op"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/types"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/mmaintegration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
)

// MMAStoreRebalancer is a rebalancer that uses the MMA allocator to rebalance
// a store.
type MMAStoreRebalancer struct {
	log.AmbientContext
	localStoreID state.StoreID
	controller   op.Controller
	allocator    mmaprototype.Allocator
	as           *mmaintegration.AllocatorSync
	settings     *config.SimulationSettings

	// lastRebalanceTime is the last time allocator.ComputeChanges was called.
	// This is used to determine when to allocator.ComputeChanges again.
	lastRebalanceTime types.Tick
	// pendingChangeIdx is the index of the next pendingChange to be processed,
	// When pendingChangeIdx == len(pendingChanges), there are no more pending
	// changes to process.
	pendingChangeIdx int
	// pendingChanges is the most recent list of pendingChanges from the
	// allocator.
	pendingChanges []pendingChangeAndRangeUsageInfo
	// pendingTicket is the ticket of the operation currently in progress, if
	// there is one, otherwise -1.
	pendingTicket op.DispatchedTicket
	// currentlyRebalancing is true if the rebalancer is currently in the process of
	// computing or applying rebalance changes.
	currentlyRebalancing bool
	// TODO: This is not currently hooked up to the simulator state, nor is it
	// hooked up to be passed into the mma allocator. This is a placeholder for
	// tracking the last time a store shed leases, with the intention to use it
	// to populate StoreCapacity the no_leases_shed_last_rebalance field.
	lastLeaseTransfer types.Tick
}

type pendingChangeAndRangeUsageInfo struct {
	change       mmaprototype.PendingRangeChange
	usage        allocator.RangeUsageInfo
	syncChangeID mmaintegration.SyncChangeID
}

// NewMMAStoreRebalancer creates a new MMAStoreRebalancer.
func NewMMAStoreRebalancer(
	localStoreID state.StoreID,
	localNodeID state.NodeID,
	allocator mmaprototype.Allocator,
	as *mmaintegration.AllocatorSync,
	controller op.Controller,
	settings *config.SimulationSettings,
) *MMAStoreRebalancer {
	msr := &MMAStoreRebalancer{
		AmbientContext:       log.MakeTestingAmbientCtxWithNewTracer(),
		localStoreID:         localStoreID,
		allocator:            allocator,
		as:                   as,
		controller:           controller,
		settings:             settings,
		pendingChangeIdx:     0,
		pendingTicket:        -1,
		currentlyRebalancing: false,
	}
	msr.AddLogTag(fmt.Sprintf("n%ds%d", localNodeID, localStoreID), "")
	return msr
}

// Tick is called periodically to check for and apply rebalancing operations
// using mmaprototype.Allocator.
func (msr *MMAStoreRebalancer) Tick(ctx context.Context, tick types.Tick, s state.State) {
	ctx = msr.AnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, "t", tick.Sub(types.Tick{Start: msr.settings.StartTime, Tick: 0, Count: 0}))

	if !msr.currentlyRebalancing &&
		tick.Sub(msr.lastRebalanceTime) < msr.settings.LBRebalancingInterval {
		// We are waiting out the rebalancing interval. Nothing to do.
		return
	}

	if !kvserverbase.LoadBasedRebalancingModeIsMMA(&msr.settings.ST.SV) {
		// When the store rebalancer isn't set to use the multi-metric mode, the
		// legacy store rebalancer is used.
		return
	}

	if !msr.currentlyRebalancing {
		// NB: This path is only hit when the rebalancer is first started, or when
		// the rebalancing interval has elapsed and we have waited out the
		// LBRebalancingInterval.
		msr.currentlyRebalancing = true
		msr.lastRebalanceTime = tick
	}

	for {
		// First, check for any pending changes that are in progress from prior
		// loop iterations on this tick, or prior ticks.
		if msr.pendingTicket != -1 {
			curChange := msr.pendingChanges[msr.pendingChangeIdx]
			if !curChange.syncChangeID.IsValid() {
				panic("invalid sync change ID")
			}
			// There is a pending operation we are waiting on. Check the status.
			op, ok := msr.controller.Check(msr.pendingTicket)
			if !ok {
				panic(fmt.Sprintf("operation not found for pending ticket=%d change=%v",
					msr.pendingTicket, curChange))
			}

			if done, _ := op.Done(); done {
				// The operation is done, we check whether it was successful and notify
				// the allocator accordingly.
				msr.pendingTicket = -1
				success := true
				if err := op.Errors(); err != nil {
					log.KvDistribution.Infof(ctx, "operation for pendingChange=%v failed: %v", curChange.change, err)
					success = false
				} else {
					log.KvDistribution.VInfof(ctx, 1, "operation for pendingChange=%v completed successfully", curChange.change)
				}
				msr.as.PostApply(curChange.syncChangeID, success)
				msr.pendingChangeIdx++
			} else {
				log.KvDistribution.VInfof(ctx, 1, "operation for pendingChange=%v is still in progress", curChange.change)
				// Operation is still in progress, nothing to do this tick.
				return
			}
		}
		if msr.pendingChangeIdx == len(msr.pendingChanges) {
			// No pending changes to process, see if there are any new changes to
			// compute.
			msr.pendingChanges = nil
			msr.pendingChangeIdx = 0
			msr.lastRebalanceTime = tick
			log.KvDistribution.VInfof(ctx, 1, "no more pending changes to process, will call compute changes again")
			storeLeaseholderMsg := MakeStoreLeaseholderMsgFromState(s, msr.localStoreID)
			pendingChanges := msr.allocator.ComputeChanges(ctx, &storeLeaseholderMsg, mmaprototype.ChangeOptions{
				LocalStoreID: roachpb.StoreID(msr.localStoreID),
			})
			log.KvDistribution.Infof(ctx, "store %d: computed %d changes", msr.localStoreID, len(pendingChanges))
			for i, change := range pendingChanges {
				usageInfo := s.RangeUsageInfo(state.RangeID(change.RangeID), msr.localStoreID)
				msr.pendingChanges = append(msr.pendingChanges, pendingChangeAndRangeUsageInfo{
					change: change,
					usage:  usageInfo,
				})
				log.KvDistribution.Infof(ctx, "%v-th change: %v", i+1, change)
			}
			if len(msr.pendingChanges) == 0 {
				// Nothing to do, there were no changes returned.
				msr.currentlyRebalancing = false
				log.KvDistribution.VInfof(ctx, 1, "no pending changes to process, will wait for next tick")
				return
			}

			// Record the last time a lease transfer was requested.
			for _, change := range msr.pendingChanges {
				if change.change.IsTransferLease() {
					msr.lastLeaseTransfer = tick
				}
			}
		}

		curChange := msr.pendingChanges[msr.pendingChangeIdx]
		if _, ok := s.Range(state.RangeID(curChange.change.RangeID)); !ok {
			// TODO: if ranges can go away because of merge, we should not be panicking here.
			panic(fmt.Sprintf("range doesn't exist returned from change=%v", curChange))
		}

		var curOp op.ControlledOperation
		if msr.pendingChanges[msr.pendingChangeIdx].change.IsTransferLease() {
			curOp = op.NewTransferLeaseOp(
				tick,
				curChange.change.RangeID,
				roachpb.StoreID(msr.localStoreID),
				curChange.change.LeaseTransferTarget(),
			)
		} else if curChange.change.IsChangeReplicas() {
			curOp = op.NewChangeReplicasOp(
				tick,
				curChange.change.RangeID,
				curChange.change.ReplicationChanges(),
			)
		} else {
			panic(fmt.Sprintf("unexpected pending change type: %v", curChange))
		}
		log.KvDistribution.VInfof(ctx, 1, "dispatching operation for pendingChange=%v", curChange.change)
		msr.pendingChanges[msr.pendingChangeIdx].syncChangeID =
			msr.as.MMAPreApply(ctx, curChange.usage, curChange.change)
		msr.pendingTicket = msr.controller.Dispatch(ctx, tick, s, curOp)
	}
}
