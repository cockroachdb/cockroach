// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ChangeOptions is passed to ComputeChanges and AdminScatterOne.
type ChangeOptions struct {
	LocalStoreID roachpb.StoreID
	// DryRun tells the allocator not to update its internal state with the
	// proposed pending changes.
	DryRun bool
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
	LoadSummaryForAllStores(context.Context) string
	Metrics() *MMAMetrics
	// Methods to update the state of the external world. The allocator starts
	// with no knowledge.

	// SetStore informs the allocator about a new store, or when something about
	// the store attributes and locality has changed. The allocator's knowledge
	// about the nodes in the cluster is a side effect of this method.
	SetStore(store StoreAttributesAndLocality)

	// ProcessStoreLoadMsg provides frequent the state of every store and its
	// associated node in the cluster.
	ProcessStoreLoadMsg(ctx context.Context, msg *StoreLoadMsg)

	// Methods related to making changes.

	// AdjustPendingChangeDisposition is optional feedback to inform the
	// allocator of success or failure of proposed changes to a range. For
	// successful changes, this is a faster way to know about success than
	// waiting for the next ProcessNodeLoadResponse from the local node. For
	// failed changes, in the absence of this feedback, proposed changes that
	// have not been enacted in N seconds will be garbage collected and assumed
	// to have failed.
	//
	// Calls to AdjustPendingChangeDisposition must be correctly sequenced with
	// full state updates from the local node provided in
	// ProcessNodeLoadResponse.
	AdjustPendingChangeDisposition(change ExternalRangeChange, success bool)

	// RegisterExternalChange informs this allocator about yet to complete
	// changes to the cluster which were not initiated by this allocator. The
	// ownership of all state inside change is handed off to the callee. If ok
	// is true, the change was registered, and the caller is returned an
	// ExternalRangeChange that it should subsequently use in a call to
	// AdjustPendingChangeDisposition when the changes are completed, either
	// successfully or not. If ok is false, the change was not registered.
	RegisterExternalChange(change PendingRangeChange) (_ ExternalRangeChange, ok bool)

	// ComputeChanges is called periodically and frequently, say every 10s.
	//
	// It accepts the latest StoreLeaseholderMsg for ChangeOptions.LocalStoreID.
	//
	// Currently, the only proposed changes are rebalancing (include lease
	// transfers) of ranges for which this node is a leaseholder, to improve
	// load balance across all stores in the cluster. Later we will add support
	// for up-replication, constraint satisfaction etc. (the core helper classes
	// already support such changes, so this limitation is mainly placed for
	// ease of incremental integration).
	//
	// Unless ChangeOptions.DryRun is true, changes returned are remembered by
	// the allocator, to avoid re-proposing the same change and to make
	// adjustments to the load.
	ComputeChanges(
		ctx context.Context, msg *StoreLeaseholderMsg, opts ChangeOptions) []ExternalRangeChange

	// AdminRelocateOne is a helper for AdminRelocateRange.
	//
	// The allocator must know the stores listed in the targets. The
	// implementation ignores the allocator's state of the range's replicas and
	// leaseholder, and utilizes the information provided in desc, conf, and
	// leaseholderStore (leaseholderStore is -1 if there is no leaseholder) --
	// for this reason it does not remember the proposed changes. The
	// implementation ignores the current load on the targets, though it returns
	// an error if any of the targets has a failure detector state != fdOK.
	// Additionally, it checks that the voterTargets and nonVoterTargets satisfy
	// all the range constraints. Leaseholder preferences are ignored if
	// transferLeaseToFirstVoter is true, else the leaseholder is picked
	// randomly from the best set of voters (best defined by leaseholder
	// preferences. Load is not considered). Diversity scores are ignored since
	// the set of voters and non-voters have been explicitly specified.
	//
	// If the returned slice is empty and error is nil, there are no more
	// changes to make.
	//
	// Note for future integration work: the intention here is to subsume some
	// of the code in replica_command.go, and this method will be called from
	// Replica.relocateReplicas (RelocateOne code is no longer needed).
	//
	// NB: leaseholderStore is needed because this function may not be called on
	// the leaseholder. Also, even if it is being called at the leaseholder
	// node, the node may have multiple stores, and we need to know which store
	// is the leaseholder. This also implies that the leaseholder transfer does
	// not need to be the last step. For the same reason, the rangeID may not be
	// known to the allocator, which is why the range's SpanConfig is provided.
	AdminRelocateOne(desc *roachpb.RangeDescriptor, conf *roachpb.SpanConfig,
		leaseholderStore roachpb.StoreID,
		voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
		transferLeaseToFirstVoter bool) ([]pendingReplicaChange, error)

	// AdminScatterOne is a helper for AdminScatterRequest. Consider it a more
	// aggressive rebalance, with more randomization. The difference wrt
	// rebalancing is that (a) the candidate range for rebalancing is being
	// specified by the caller, instead of having to be figured out by the
	// allocator. Like rebalancing, the candidate store to shed from and the
	// store to add to is computed by the callee, and the result is provided in
	// []pendingReplicaChange. Exactly one such pair of remove-add will be
	// specified in the pending changes. Unless DryRun is true, these pending
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

	// KnownStores is a temporary hack to deal with staleness in calling SetStore.
	//
	// TODO(sumeer): remove once the integration is properly done.
	KnownStores() map[roachpb.StoreID]struct{}

	// BuildMMARebalanceAdvisor is called by the allocator sync to build a
	// MMARebalanceAdvisor for the given existing store and candidates. The
	// advisor should be later passed to IsInConflictWithMMA to determine if a
	// given candidate is in conflict with the existing store.
	//
	// TODO(sumeer): merge the above comment with the comment in the
	// implementation.
	BuildMMARebalanceAdvisor(existing roachpb.StoreID, cands []roachpb.StoreID) *MMARebalanceAdvisor

	// IsInConflictWithMMA is called by the allocator sync to determine if the
	// given candidate is in conflict with the existing store.
	//
	// TODO(sumeer): merge the above comment with the comment in the
	// implementation.
	IsInConflictWithMMA(
		ctx context.Context, cand roachpb.StoreID, advisor *MMARebalanceAdvisor, cpuOnly bool) bool
}

// Avoid unused lint errors.

var _ = ChangeOptions{}.DryRun
