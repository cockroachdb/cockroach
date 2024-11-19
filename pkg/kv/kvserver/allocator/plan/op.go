// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plan

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
	// ApplyImpact updates the given storepool to reflect the result of
	// applying this operation.
	ApplyImpact(storepool storepool.AllocatorStorePool)
	// LHBeingRemoved returns true when the leaseholder is will be removed if
	// this operation succeeds, otherwise false.
	LHBeingRemoved() bool
}

// AllocationTransferLeaseOp represents an operation to transfer a range lease to another
// store, from the current one.
type AllocationTransferLeaseOp struct {
	Target, Source     roachpb.StoreID
	Usage              allocator.RangeUsageInfo
	bypassSafetyChecks bool
}

var _ AllocationOp = &AllocationTransferLeaseOp{}

// LHBeingRemoved returns true when the leaseholder is will be removed if this
// operation succeeds, otherwise false. This is always true for lease
// transfers.
func (o AllocationTransferLeaseOp) LHBeingRemoved() bool {
	return true
}

func (o AllocationTransferLeaseOp) ApplyImpact(storepool storepool.AllocatorStorePool) {
	// TODO(kvoli): Currently the local storepool is updated directly in the
	// lease transfer call, rather than in this function. Move the storepool
	// tracking from rq.TransferLease to this function once #89771 is merged.
}

// AllocationChangeReplicasOp represents an operation to execute a change
// replicas txn.
type AllocationChangeReplicasOp struct {
	Usage             allocator.RangeUsageInfo
	LeaseholderStore  roachpb.StoreID
	Chgs              kvpb.ReplicationChanges
	AllocatorPriority float64
	Reason            kvserverpb.RangeLogEventReason
	Details           string
}

var _ AllocationOp = &AllocationChangeReplicasOp{}

// LHBeingRemoved returns true when the voter removals for this change replicas
// operation includes the leaseholder store.
func (o AllocationChangeReplicasOp) LHBeingRemoved() bool {
	for _, chg := range o.Chgs.VoterRemovals() {
		if chg.StoreID == o.LeaseholderStore {
			return true
		}
	}
	return false
}

// applyEstimatedImpact updates the given storepool to reflect the result
// of applying this operation.
func (o AllocationChangeReplicasOp) ApplyImpact(storepool storepool.AllocatorStorePool) {
	for _, chg := range o.Chgs {
		storepool.UpdateLocalStoreAfterRebalance(chg.Target.StoreID, o.Usage, chg.ChangeType)
	}
}

// AllocationFinalizeAtomicReplicationOp represents an operation to finalize an
// atomic change replicas operation and remove any remaining learners.
type AllocationFinalizeAtomicReplicationOp struct{}

var _ AllocationOp = &AllocationFinalizeAtomicReplicationOp{}

// TODO(kvoli): This always returns false, however it is possible that the LH
// may have been removed here.
func (o AllocationFinalizeAtomicReplicationOp) LHBeingRemoved() bool                               { return false }
func (o AllocationFinalizeAtomicReplicationOp) ApplyImpact(storepool storepool.AllocatorStorePool) {}

// AllocationNoop represents no operation.
type AllocationNoop struct{}

var _ AllocationOp = &AllocationNoop{}

func (o AllocationNoop) LHBeingRemoved() bool                               { return false }
func (o AllocationNoop) ApplyImpact(storepool storepool.AllocatorStorePool) {}
