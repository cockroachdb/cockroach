// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// trackedAllocatorChange represents a change registered with AllocatorSync
// (e.g. lease transfer or change replicas). Usage is range load usage.
type trackedAllocatorChange struct {
	usage allocator.RangeUsageInfo
	// Only one of the following two fields will be set.
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
