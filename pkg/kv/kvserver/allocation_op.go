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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// AllocationOp represents an atomic allocation operation to be applied against
// a specific replica.
//
// TODO(kvoli): Add AllocationRelocateRangeOp.
type AllocationOp interface {
	// trackPlanningMetrics tracks the metrics that have been generated in
	// creating this operation.
	trackPlanningMetrics()
	// applyImpact updates the given storepool to reflect the result of
	// applying this operation.
	applyImpact(storepool storepool.AllocatorStorePool)
	// lhBeingRemoved returns true when the leaseholder is will be removed if
	// this operation succeeds, otherwise false.
	lhBeingRemoved() bool
}

// AllocationTransferLeaseOp represents an operation to transfer a range lease to another
// store, from the current one.
type AllocationTransferLeaseOp struct {
	target, source     roachpb.StoreID
	usage              allocator.RangeUsageInfo
	bypassSafetyChecks bool
	sideEffects        func()
}

var _ AllocationOp = &AllocationTransferLeaseOp{}

// lhBeingRemoved returns true when the leaseholder is will be removed if this
// operation succeeds, otherwise false. This is always true for lease
// transfers.
func (o AllocationTransferLeaseOp) lhBeingRemoved() bool {
	return true
}

func (o AllocationTransferLeaseOp) applyImpact(storepool storepool.AllocatorStorePool) {
	// TODO(kvoli): Currently the local storepool is updated directly in the
	// lease transfer call, rather than in this function. Move the storepool
	// tracking from rq.TransferLease to this function once #89771 is merged.
}

// trackPlanningMetrics tracks the metrics that have been generated in creating
// this operation.
func (o AllocationTransferLeaseOp) trackPlanningMetrics() {
	if o.sideEffects != nil {
		o.sideEffects()
	}
}

// AllocationChangeReplicasOp represents an operation to execute a change
// replicas txn.
type AllocationChangeReplicasOp struct {
	usage             allocator.RangeUsageInfo
	lhStore           roachpb.StoreID
	chgs              kvpb.ReplicationChanges
	priority          kvserverpb.SnapshotRequest_Priority
	allocatorPriority float64
	reason            kvserverpb.RangeLogEventReason
	details           string
	sideEffects       func()
}

var _ AllocationOp = &AllocationChangeReplicasOp{}

// lhBeingRemoved returns true when the voter removals for this change replicas
// operation includes the leaseholder store.
func (o AllocationChangeReplicasOp) lhBeingRemoved() bool {
	for _, chg := range o.chgs.VoterRemovals() {
		if chg.StoreID == o.lhStore {
			return true
		}
	}
	return false
}

// applyEstimatedImpact updates the given storepool to reflect the result
// of applying this operation.
func (o AllocationChangeReplicasOp) applyImpact(storepool storepool.AllocatorStorePool) {
	for _, chg := range o.chgs {
		storepool.UpdateLocalStoreAfterRebalance(chg.Target.StoreID, o.usage, chg.ChangeType)
	}
}

// trackPlanningMetrics tracks the metrics that have been generated in creating
// this operation.
func (o AllocationChangeReplicasOp) trackPlanningMetrics() {
	if o.sideEffects != nil {
		o.sideEffects()
	}
}

// AllocationFinalizeAtomicReplicationOp represents an operation to finalize an
// atomic change replicas operation and remove any remaining learners.
type AllocationFinalizeAtomicReplicationOp struct{}

var _ AllocationOp = &AllocationFinalizeAtomicReplicationOp{}

// TODO(kvoli): This always returns false, however it is possible that the LH
// may have been removed here.
func (o AllocationFinalizeAtomicReplicationOp) lhBeingRemoved() bool                               { return false }
func (o AllocationFinalizeAtomicReplicationOp) applyImpact(storepool storepool.AllocatorStorePool) {}
func (o AllocationFinalizeAtomicReplicationOp) trackPlanningMetrics()                              {}

// AllocationNoop represents no operation.
type AllocationNoop struct{}

var _ AllocationOp = &AllocationNoop{}

func (o AllocationNoop) lhBeingRemoved() bool                               { return false }
func (o AllocationNoop) applyImpact(storepool storepool.AllocatorStorePool) {}
func (o AllocationNoop) trackPlanningMetrics()                              {}

// effectBuilder is a utility struct to track a list of effects, which may be
// used to construct a single effect function that in turn calls all tracked
// effects.
type effectBuilder struct {
	e []func()
}

// add appends an effect to be rolled into a single effect when calling f().
// The return value of this function must be used.
func (b effectBuilder) add(effect func()) effectBuilder {
	b.e = append(b.e, effect)
	return b
}

func (b effectBuilder) f() func() {
	// NB: Avoid heap allocations when not necessary.
	if len(b.e) == 0 {
		return func() {}
	}

	return func() {
		for _, effect := range b.e {
			effect()
		}
	}
}
