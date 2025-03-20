// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mma"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/op"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
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
	allocator    mma.Allocator
	settings     *config.SimulationSettings

	// lastRebalanceTime is the last time allocator.ComputeChanges was called.
	// This is used to determine when to allocator.ComputeChanges again.
	lastRebalanceTime time.Time
	// pendingChangeIdx is the index of the next pendingChange to be processed,
	// When pendingChangeIdx == len(pendingChanges), there are no more pending
	// changes to process.
	pendingChangeIdx int
	// pendingChanges is the most recent list of pendingChanges from the
	// allocator.
	pendingChanges []mma.PendingRangeChange
	// pendingTicket is the ticket of the operation currently in progress, if
	// there is one, otherwise -1.
	pendingTicket op.DispatchedTicket
}

// NewMMAStoreRebalancer creates a new MMAStoreRebalancer.
func NewMMAStoreRebalancer(
	localStoreID state.StoreID,
	localNodeID state.NodeID,
	allocator mma.Allocator,
	controller op.Controller,
	settings *config.SimulationSettings,
) *MMAStoreRebalancer {
	msr := &MMAStoreRebalancer{
		AmbientContext:   log.MakeTestingAmbientCtxWithNewTracer(),
		localStoreID:     localStoreID,
		allocator:        allocator,
		controller:       controller,
		settings:         settings,
		pendingChangeIdx: 0,
		pendingTicket:    -1,
	}
	msr.AddLogTag(fmt.Sprintf("n%ds%d", localNodeID, localStoreID), "")
	return msr
}

// Tick is called periodically to check for and apply rebalancing operations
// using mma.Allocator.
func (msr *MMAStoreRebalancer) Tick(ctx context.Context, tick time.Time, s state.State) {
	ctx = msr.ResetAndAnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, "t", tick.Sub(msr.settings.StartTime))
	if msr.pendingTicket == -1 &&
		tick.Sub(msr.lastRebalanceTime) < msr.settings.LBRebalancingInterval {
		// We are waiting out the rebalancing interval. Nothing to do.
		return
	}

	if msr.settings.LBRebalancingMode != int64(kvserver.LBRebalancingMultiMetric) {
		// When the store rebalancer isn't set to use the multi-metric mode, the
		// legacy store rebalancer is used.
		return
	}

	if msr.pendingTicket == -1 &&
		msr.pendingChangeIdx == len(msr.pendingChanges) {
		// No pending operations and there are no more pending changes. Can call
		// into allocator.ComputeChanges again.
		storeLeaseholderMsg := MakeStoreLeaseholderMsgFromState(s, msr.localStoreID)
		msr.allocator.ProcessStoreLeaseholderMsg(&storeLeaseholderMsg)
		msr.lastRebalanceTime = tick
		msr.pendingChangeIdx = 0
		msr.pendingChanges = msr.allocator.ComputeChanges(ctx, mma.ChangeOptions{
			LocalStoreID: roachpb.StoreID(msr.localStoreID),
		})
		log.Infof(ctx, "store %d: computed %d changes %v", msr.localStoreID, len(msr.pendingChanges), msr.pendingChanges)
	}

	for {
		if msr.pendingTicket != -1 {
			curChange := msr.pendingChanges[msr.pendingChangeIdx]
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
					log.VInfof(ctx, 1, "operation for pendingChange=%v failed: %v", curChange, err)
					success = false
				}
				msr.allocator.AdjustPendingChangesDisposition(
					[]mma.PendingRangeChange{curChange}, success)
				msr.pendingChangeIdx++
			} else {
				log.VInfof(ctx, 1, "operation for pendingChange=%v is still in progress", curChange)
				// Operation is still in progress, nothing to do this tick.
				return
			}
		}

		if msr.pendingChangeIdx == len(msr.pendingChanges) {
			// No more pending changes to process.
			msr.pendingChanges = nil
			msr.pendingChangeIdx = 0
			log.VInfof(ctx, 1, "no more pending changes to process")
			return
		}

		curChange := msr.pendingChanges[msr.pendingChangeIdx]
		if _, ok := s.Range(state.RangeID(curChange.RangeID)); !ok {
			panic(fmt.Sprintf("range doesn't exist returned from change=%v", curChange))
		}

		var curOp op.ControlledOperation
		if msr.pendingChanges[msr.pendingChangeIdx].IsTransferLease() {
			curOp = op.NewTransferLeaseOp(
				tick,
				roachpb.RangeID(curChange.RangeID),
				roachpb.StoreID(msr.localStoreID),
				curChange.LeaseTransferTarget(),
			)
		} else if curChange.IsChangeReplicas() {
			curOp = op.NewChangeReplicasOp(
				tick,
				roachpb.RangeID(curChange.RangeID),
				curChange.ReplicationChanges(),
			)
		} else {
			panic(fmt.Sprintf("unexpected pending change type: %v", curChange))
		}
		msr.pendingTicket = msr.controller.Dispatch(ctx, tick, s, curOp)
	}
}
