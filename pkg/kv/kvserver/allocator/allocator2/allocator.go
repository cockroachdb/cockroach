// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import "github.com/cockroachdb/cockroach/pkg/roachpb"

type ChangeOptions struct {
	dryRun bool
}

// Allocator is the interface for a distributed allocator. We expect that the
// core of the allocator implementation will not know or care whether the
// allocator is distributed or centralized, but there may be specializations
// for the distributed case at higher layers of the implementation.
// Additionally, there may be:
//   - adapter layers that bridge the gap between this interface and the current
//     integration with the old allocator.
//   - changes to this interface to make the integration for the new allocator
//     be less different than integration with the old allocator.
type Allocator interface {
	// Methods to update the state of the external world. The allocator starts
	// with no knowledge.

	// AddNodeID informs the allocator of a new node.
	AddNodeID(nodeID roachpb.NodeID) error

	// AddStore informs the allocator of a new store.
	AddStore(store roachpb.StoreDescriptor) error

	// ChangeStore informs the allocator that something about the store
	// descriptor has changed.
	ChangeStore(store roachpb.StoreDescriptor) error

	// RemoveNodeAndStores tells the allocator to remove the nodeID and all its
	// stores.
	RemoveNodeAndStores(nodeID roachpb.NodeID) error

	// UpdateFailureDetectionSummary tells the allocator about the current
	// failure detection state for a node. A node starts in the fdOK state.
	UpdateFailureDetectionSummary(nodeID roachpb.NodeID, fd failureDetectionSummary) error

	// ProcessNodeLoadResponse provides frequent updates to the state of the
	// external world.
	//
	// Only the node on which this allocator is running will provide a non-empty
	// nodeLoadResponse.leaseholderStores slice.
	//
	// TODO(kvoli,sumeer): The storeLoadMsg may need a combination of gossip changes
	// and local synthesis:
	//
	// - storeLoadMsg.storeRanges is the list of ranges at the node, and is
	//   needed for accounting for load adjustments, and avoids making the
	//   assumption that all changes have been applied. However, for expediency
	//   of integration, we could relax this and synthesize this list as a
	//   function of (a) timestamp of the gossiped load, (b) timestamp of the
	//   proposed change, (c) timestamp of the latest leaseholderStores slice
	//   that shows the change as being enacted.
	//
	// - storeLoadMsg.storeRanges does not need to be the complete list of
	//   ranges -- it can be filtered down to only the ranges for which the
	//   local node is the leaseholder.
	//
	// - storeLoadMsg.topKRanges could be locally synthesized by estimating the
	//   load on a follower based on the measured load at the leaseholder.
	ProcessNodeLoadResponse(resp *nodeLoadResponse) error

	// TODO(sumeer): only a subset of the fields in pendingReplicaChange are
	// relevant to the caller. Hide the remaining.

	// Methods related to making changes.

	// AdjustPendingChangesDisposition is optional feedback to inform the
	// allocator of success or failure of proposed changes. For successful
	// changes, this is a faster way to know about success than waiting for the
	// next ProcessNodeLoadResponse from the local node. For failed changes, in
	// the absence of this feedback, proposed changes that have not been enacted
	// in N seconds will be garbage collected and assumed to have failed.
	//
	// Calls to AdjustPendingChangesDisposition must be correctly sequenced with
	// full state updates from the local node provided in
	// ProcessNodeLoadResponse.
	AdjustPendingChangesDisposition(changes []pendingReplicaChange, success bool) error

	// ComputeChanges is called periodically and frequently, say every 10s.
	// Unless ChangeOptions.dryRun is true, changes returned are remembered by
	// the allocator, to avoid re-proposing the same change and to make
	// adjustments to the load.
	ComputeChanges(opts ChangeOptions) []*pendingReplicaChange

	// AdminRelocateOne is a helper for AdminRelocateRange.
	//
	// The allocator must know the RangeID and its SpanConfig. The stores listed
	// in the targets must also be known to the allocator. The implementation
	// ignores the allocator's state of the range's replicas and leaseholder,
	// and utilizes the information provided in desc and leaseholderStore
	// (leaseholderStore is -1 if there is no leaseholder) -- for this reason it
	// does not remember the proposed changes. The implementation ignores the
	// current load on the targets, though it returns an error if any of the
	// targets has a failure detector state != fdOK. Additionally, it checks
	// that the voterTargets and nonVoterTargets satisfy all the range
	// constraints. Leaseholder preferences are ignored if
	// transferLeaseToFirstVoter is true, else the leaseholder is picked
	// randomly from the best set of voters (best defined by leaseholder
	// preferences. Should we also use load?). Diversity scores are ignored
	// since the set of voters and non-voters have been explicitly specified.
	//
	// If the returned slice is empty and error is nil, there are no more
	// changes to make.
	//
	// Note for reviewer: the intention here is to subsume some of the code in
	// replica_send.go, and this method will be called from
	// Replica.relocateReplicas (ReplocateOne code is no longer needed).
	//
	// TODO(sumeer): is leaseholderStore needed. Presumably we are doing this at
	// this node because one of it's stores is the leaseholder. But if this node
	// has multiple stores we do need to know the leaseholder. Also, presumably
	// we have to do the leaseholder transfer as the last step since otherwise
	// AdminRelocateOne may no longer be possible to call at this node.
	AdminRelocateOne(desc *roachpb.RangeDescriptor, leaseholderStore roachpb.StoreID,
		voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
		transferLeaseToFirstVoter bool) ([]pendingReplicaChange, error)

	// AdminScatterOne is a helper for AdminScatterRequest. Consider it a more
	// aggressive rebalance, with more randomization. The difference wrt
	// rebalancing is that (a) the candidate range for rebalancing is being
	// specified by the caller, instead of having to be figured out by the
	// allocator. Like rebalancing, the candidate store to shed from and the
	// store to add to is computed by the callee, and the result is provided in
	// []pendingReplicaChange. Exactly one such pair of remove-add will be
	// specified in the pending changes. Unless dryRun is true, these pending
	// changes are remembered by the callee and the caller should not call again
	// for the same range until it has enacted the change and informed the
	// callee about the change success or failure via
	// AdjustPendingChangesDisposition.
	//
	// If the caller will call AdminScatterOne in a loop to potentially scatter
	// all the replicas, only the last call should specify
	// canTransferLease=true, since transferring the lease will make it no
	// longer possible to call AdminScatterOne on this node.
	//
	// TODO(sumeer):
	// - understand what is really happening as a consequence of
	//   ScorerOptionsForScatter.
	AdminScatterOne(
		rangeID roachpb.RangeID, canTransferLease bool, opts ChangeOptions,
	) ([]pendingReplicaChange, error)
}
