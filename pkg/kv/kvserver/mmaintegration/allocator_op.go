// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// trackedAllocatorChange represents a change registered with AllocatorSync
// (e.g. lease transfer or change replicas).
type trackedAllocatorChange struct {
	// isMMARegistered is true if the change has been successfully registered
	// with mma. When true, mmaChange is the registered change, and PostApply
	// should inform mma by passing mmaChange to
	// AdjustPendingChangesDisposition. It is false if mma is disabled or the
	// change could not be registered with mma, in which case PostApply must not
	// inform mma.
	isMMARegistered bool
	mmaChange       mmaprototype.PendingRangeChange
	// Usage is range load usage.
	usage allocator.RangeUsageInfo
	// Exactly one of the following two fields will be set.
	leaseTransferOp  *leaseTransferOp
	changeReplicasOp *changeReplicasOp
}

// leaseTransferOp represents a lease transfer operation.
type leaseTransferOp struct {
	transferFrom, transferTo roachpb.StoreID
}

// changeReplicasOp represents a change replicas operation.
type changeReplicasOp struct {
	// chgs is the replication changes that are applied to the range. len(chgs)
	// may be = [1,4].
	chgs kvpb.ReplicationChanges
}
