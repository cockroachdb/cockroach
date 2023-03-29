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

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// These values can sometimes be used in replicaType, replicaIDAndType,
// replicaState, specifically when used in the context of a
// pendingReplicaChange.
const (
	// unknownReplicaID is used with a change that proposes to add a replica
	// (since it does not know the future ReplicaID).
	unknownReplicaID roachpb.ReplicaID = -1
	// noReplicaID is used with a change that is removing a replica.
	noReplicaID roachpb.ReplicaID = -2
)

type replicaType struct {
	replicaType   roachpb.ReplicaType
	isLeaseholder bool
}

type replicaIDAndType struct {
	// replicaID can be set to unknownReplicaID or noReplicaID.
	roachpb.ReplicaID
	replicaType
}

type replicaState struct {
	replicaIDAndType
	// voterIsLagging can be set for a VOTER_FULL replica that has fallen behind
	// (and possibly even needs a snapshot to catch up). It is a hint to the
	// allocator not to transfer the lease to this replica.
	voterIsLagging bool
	// replicationPaused is set to true if replication to this replica is
	// paused. This can be a desirable replica to shed for an overloaded store.
	replicationPaused bool
}

// Unique ID, in the context of this data-structure and when receiving updates
// about enactment having happened or having been rejected (by the component
// responsible for change enactment).
type changeID uint64

// pendingReplicaChange is a proposed change to a single replica. Some
// external entity (the leaseholder of the range) may choose to enact this
// change. It may not be enacted if it will cause some invariant (like the
// number of replicas, or having a leaseholder) to be violated. If not
// enacted, the allocator will either be told about the lack of enactment, or
// will eventually expire from the allocator's state after
// pendingChangeExpiryDuration. Such expiration without enactment should be
// rare. pendingReplicaChanges can be paired, when a range is being moved from
// one store to another -- that pairing is not captured here, and captured in
// the changes suggested by the allocator to the external entity.
type pendingReplicaChange struct {
	changeID

	// The load this change adds to a store. The values will be negative if the
	// load is being removed.
	loadDelta loadVector

	storeID roachpb.StoreID
	rangeID roachpb.RangeID

	// NB: 0 is not a valid ReplicaID, but this component does not care about
	// this level of detail (the special consts defined above use negative
	// ReplicaID values as markers).
	//
	// Only following cases can happen:
	//
	// - prev.replicaID >= 0 && next.replicaID == noReplicaID: outgoing replica.
	//   prev.isLeaseholder is false, since we shed a lease first.
	//
	// - prev.replicaID == noReplicaID && next.replicaID == unknownReplicaID:
	//   incoming replica, next.replicaType must be VOTER_FULL or NON_VOTER.
	//   Both isLeaseholder fields must be false.
	//
	// - prev.replicaID >= 0 && next.replicaID >= 0: can be a change to
	//   isLeaseholder, or replicaType. next.replicaType must be VOTER_FULL or
	//   NON_VOTER.
	prev replicaState
	next replicaIDAndType

	// The wall time at which this pending change was initiated. Used for
	// expiry.
	startTime time.Time

	// When the change is known to be enacted based on the authoritative
	// information received from the leaseholder, this value is set, so that
	// even if the store with a replica affected by this pending change does not
	// tell us about the enactment, we can garbage collect this change.
	enactedAtTime time.Time
}

type pendingChangesOldestFirst []*pendingReplicaChange

func (p *pendingChangesOldestFirst) removeChangeAtIndex(index int) {
	n := len(*p)
	copy((*p)[index:n-1], (*p)[index+1:n])
	*p = (*p)[:n-1]
}

type storeInitState int8

const (
	// partialInit is the state iff only the StoreID and adjusted.replicas have
	// been initialized in this store. This can happen if the leaseholder of a
	// range sends information about a store that has a replica before the
	// allocator is explicitly told about this new store.
	partialInit storeInitState = iota
	fullyInit
	// When the store is known to be removed but it is still referenced as a
	// replica by some leaseholders. This is different from a dead store, from
	// which we will rebalance away. For a removed store we'll just wait until
	// there are no ranges referencing it.
	removed
)

// storeState ...
type storeState struct {
	storeInitState
	storeLoad
	adjusted struct {
		load loadVector
		// loadReplicas is computed from storeLoadMsg.storeRanges, and adjusted
		// for pending changes.
		loadReplicas map[roachpb.RangeID]replicaType
		// Pending changes for computing loadReplicas and load.
		// Added to at the same time as clusterState.pendingChanges.
		//
		// Removed from lifecyle is slightly different from those other pending changes.
		// If clusterState.pendingChanges is removing a pending change because:
		//
		// - rejected by enacting module, it will also remove from
		//   loadPendingChanges. Similarly time-based GC from
		//   clusterState.pendingChanges will also remove from here.
		//
		// - leaseholder provided state shows that the change has been enacted, it
		//   will set enactedAtTime, but not remove from loadPendingChanges since
		//   this pending change is still needed to compensate for the store
		//   reported load.
		//
		// Unilateral removal from loadPendingChanges happens if the load reported
		// by the store shows that this pending change has been enacted. We no
		// longer need to adjust the load for this pending change.
		//
		// Removal from loadPendingChanges also happens if sufficient duration has
		// elapsed from enactedAtTime.
		//
		// In summary, guaranteed removal of a load pending change because of
		// failure of enactment or GC happens via clusterState.pendingChanges.
		// Only the case where enactment happened is where a load pending change
		// can live on -- but since that will set enactedAtTime, we are guaranteed
		// to always remove it.
		loadPendingChanges map[changeID]*pendingReplicaChange

		secondaryLoad secondaryLoadVector

		// replicas is computed from the authoritative information provided by
		// various leaseholders in storeLeaseholderMsgs and adjusted for pending
		// changes in cluserState.pendingChanges/rangeState.pendingChanges.
		replicas map[roachpb.RangeID]replicaState
	}
	// This is a locally incremented seqnum which is incremented whenever the
	// adjusted or reported load information for this store or the containing
	// node is updated. It is utilized for cache invalidation of the
	// storeLoadSummary stored in meansForStoreSet.
	loadSeqNum uint64

	// max(|1-(adjustedLoad[i]/reportedLoad[i])|)
	//
	// If maxFractionPending is greater than some threshold, we don't add or
	// remove more load unless we are shedding load due to failure detection.
	// This is to allow the effect of the changes to stabilize since our
	// adjustments to load vectors are estimates, and there can be overhead on
	// these nodes due to making the change.
	maxFractionPending float64

	// A summary of the current load on this store.
	loadSummary loadSummary

	localityTiers
}

// failureDetectionSummary is provided by an external entity and never
// computed inside the allocator. It is one of the inputs to loadSummary that
// is computed by the allocator.
type failureDetectionSummary uint8

// TODO(sumeer): should this more similar to node liveness
// (storeStatusUnavailable), drain (storeStatusDraining), storeStatusSuspect,
// storeStatusDead etc.
const (
	fdOK failureDetectionSummary = iota
	fdFailureLikely
	fdSuspectShedLeases
)

type nodeState struct {
	stores []roachpb.StoreID
	nodeLoad
	adjustedCPU loadValue

	// This loadSummary is only based on the cpu. It is incorporated into the
	// loadSummary computed for each store on this node.
	loadSummary loadSummary
	fdSummary   failureDetectionSummary
}

type storeIDAndReplicaState struct {
	roachpb.StoreID
	// Only valid ReplicaTypes are used here.
	replicaState
}

// rangeState is periodically updated based in reporting by the leaseholder.
type rangeState struct {
	// replicas is the adjusted replicas. It is always consistent with
	// the storeState.adjusted.replicas in the corresponding stores.
	replicas []storeIDAndReplicaState
	conf     *normalizedSpanConfig
	// Only 1 or 2 changes (latter represents a least transfer or rebalance that
	// adds and removes replicas).
	//
	// Life-cycle matches clusterState.pendingChanges. The consolidated
	// rangeState.pendingChanges across all ranges in clusterState.ranges will
	// be identical to clusterState.pendingChanges.
	pendingChanges []*pendingReplicaChange
	// If non-nil, it is up-to-date. Typically, non-nil for a range that has no
	// pendingChanges and is not satisfying some constraint, since we don't want
	// to repeat the analysis work every time we consider it.
	constraints *rangeAnalyzedConstraints

	// TODO(sumeer): populate and use.
	diversityIncreaseLastFailedAttempt time.Time

	// lastHeardTime is the latest time when this range was heard about from any
	// store, via storeLeaseholderMsg or storeLoadMsg. Despite the best-effort
	// GC it is possible we will miss something. If this lastHeardTime is old
	// enough, use some other source to verify that this range still exists.
	lastHeardTime time.Time
}

// clusterState is the state of the cluster known to the allocator, including
// adjustments based on pending changes. It does not include additional
// indexing needed for constraint matching, or for tracking ranges that may
// need attention etc. (those happen at a higher layer).
type clusterState struct {
	nodes  map[roachpb.NodeID]*nodeState
	stores map[roachpb.StoreID]*storeState
	ranges map[roachpb.RangeID]*rangeState
	// Added to when a change is proposed. Will also add to corresponding
	// rangeState.pendingChanges and to the effected storeStates.
	//
	// Removed from based on rangeMsg, explicit rejection by enacting module, or
	// time-based GC. There is no explicit acceptance by enacting module since
	// the single source of truth of a rangeState is the leaseholder.
	pendingChanges map[changeID]*pendingReplicaChange

	*constraintMatcher
	localityTierInterner
}

func newClusterState() *clusterState {
	return &clusterState{
		nodes:             map[roachpb.NodeID]*nodeState{},
		stores:            map[roachpb.StoreID]*storeState{},
		ranges:            map[roachpb.RangeID]*rangeState{},
		pendingChanges:    map[changeID]*pendingReplicaChange{},
		constraintMatcher: newConstraintMatcher(),
	}
}

//======================================================================
// clusterState mutators
//======================================================================

func (cs *clusterState) processNodeLoadResponse(resp *nodeLoadResponse) {
	// TODO:
}

func (cs *clusterState) addNodeID(nodeID roachpb.NodeID) {
	// TODO:
}

func (cs *clusterState) addStore(store roachpb.StoreDescriptor) {
	// TODO:
}

func (cs *clusterState) changeStore(store roachpb.StoreDescriptor) {
	// TODO:
}

func (cs *clusterState) removeNodeAndStores(nodeID roachpb.NodeID) {

}

// If the pending change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingChangeGCDuration = 5 * time.Minute

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges() {
	// TODO:
}

// Called by enacting module.
func (cs *clusterState) pendingChangesRejected(
	rangeID roachpb.RangeID, changes []pendingReplicaChange,
) {
	// TODO:
}

func (cs *clusterState) addPendingChanges(rangeID roachpb.RangeID, changes []pendingReplicaChange) {
	// TODO:
}

func (cs *clusterState) updateFailureDetectionSummary(
	nodeID roachpb.NodeID, fd failureDetectionSummary,
) {
	// TODO:
}

//======================================================================
// clusterState accessors:
//
// Not all accesses need to use these accessors.
//======================================================================

// For meansMemo.
var _ loadInfoProvider = &clusterState{}

func (cs *clusterState) getStoreReportedLoad(roachpb.StoreID) *storeLoad {
	// TODO:
	return nil
}

func (cs *clusterState) getNodeReportedLoad(roachpb.NodeID) *nodeLoad {
	// TODO:
	return nil
}

// canAddLoad returns true if the delta can be added to the store without
// causing it to be overloaded (or the node to be overloaded). It does not
// change any state between the call and return.
func (cs *clusterState) canAddLoad(ss *storeState, delta loadVector, means *meansForStoreSet) bool {
	// TODO:
	return false
}

func (cs *clusterState) computeLoadSummary(
	ss *storeState, msl *meanStoreLoad, mnl *meanNodeLoad,
) (sls loadSummary, nls loadSummary, fd failureDetectionSummary) {
	ns := cs.nodes[ss.NodeID]
	sls = loadLow
	for i := range msl.load {
		ls := loadSummaryForDimension(ss.adjusted.load[i], ss.capacity[i], msl.load[i], msl.util[i])
		if ls < sls {
			sls = ls
		}
	}
	nls = loadSummaryForDimension(ns.adjustedCPU, ns.capacityCPU, mnl.loadCPU, mnl.utilCPU)
	return sls, nls, ns.fdSummary
}
