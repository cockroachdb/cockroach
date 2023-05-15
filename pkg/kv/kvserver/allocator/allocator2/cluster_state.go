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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
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

func (rt replicaIDAndType) String() string {
	var replicaIDString string
	switch rt.ReplicaID {
	case unknownReplicaID:
		replicaIDString = "unknown"
	case noReplicaID:
		replicaIDString = "none"
	default:
		replicaIDString = rt.ReplicaID.String()
	}
	return fmt.Sprintf("replica-id=%s lease=%v type=%v",
		replicaIDString, rt.isLeaseholder, rt.replicaType.replicaType,
	)
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

type replicaChange struct {
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
}

func (rc replicaChange) String() string {
	return fmt.Sprintf("store-id=%v range-id=%v delta=%v prev=(%v) next=(%v)",
		rc.storeID, rc.rangeID, rc.loadDelta, rc.prev, rc.next)
}

func makeLeaseTransferChanges(
	rLoad rangeLoad, rState *rangeState, addStoreID, removeStoreID roachpb.StoreID,
) [2]replicaChange {
	var add, remove storeIDAndReplicaState
	for _, replica := range rState.replicas {
		if replica.StoreID == addStoreID {
			add = replica
		}
		if replica.StoreID == removeStoreID {
			remove = replica
		}
	}

	removeLease := replicaChange{
		storeID: remove.StoreID,
		rangeID: rLoad.RangeID,
		prev:    remove.replicaState,
		next:    remove.replicaIDAndType,
	}
	addLease := replicaChange{
		storeID: add.StoreID,
		rangeID: rLoad.RangeID,
		prev:    add.replicaState,
		next:    add.replicaIDAndType,
	}
	addLease.next.isLeaseholder = true
	removeLease.next.isLeaseholder = false

	nonRaftCPU := rLoad.load[cpu] - rLoad.raftCPU
	removeLease.loadDelta[cpu] -= nonRaftCPU
	addLease.loadDelta[cpu] += nonRaftCPU
	return [2]replicaChange{removeLease, addLease}
}

func makeAddReplicaChange(
	rLoad rangeLoad, addStoreID roachpb.StoreID, rType roachpb.ReplicaType,
) replicaChange {
	addReplica := replicaChange{
		storeID: addStoreID,
		rangeID: rLoad.RangeID,
		prev: replicaState{
			replicaIDAndType: replicaIDAndType{
				ReplicaID: noReplicaID,
			},
		},
		next: replicaIDAndType{
			ReplicaID: unknownReplicaID,
			replicaType: replicaType{
				replicaType: rType,
			},
		},
	}
	addReplica.loadDelta[cpu] += rLoad.raftCPU
	return addReplica
}

func makeRemoveReplicaChange(rLoad rangeLoad, remove storeIDAndReplicaState) replicaChange {
	removeReplica := replicaChange{
		storeID: remove.StoreID,
		rangeID: rLoad.RangeID,
		prev:    remove.replicaState,
		next: replicaIDAndType{
			ReplicaID: noReplicaID,
		},
	}
	removeReplica.loadDelta[cpu] -= rLoad.raftCPU
	return removeReplica
}

func makeRebalanceReplicaChanges(
	rLoad rangeLoad, rState *rangeState, addStoreID, removeStoreID roachpb.StoreID,
) [2]replicaChange {
	var remove storeIDAndReplicaState
	for _, replica := range rState.replicas {
		if replica.StoreID == removeStoreID {
			remove = replica
		}
	}
	addReplicaChange := makeAddReplicaChange(rLoad, addStoreID, remove.replicaType.replicaType)
	removeReplicaChange := makeRemoveReplicaChange(rLoad, remove)
	return [2]replicaChange{addReplicaChange, removeReplicaChange}
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
	replicaChange

	// The wall time at which this pending change was initiated. Used for
	// expiry.
	startTime time.Time

	// When the change is known to be enacted based on the authoritative
	// information received from the leaseholder, this value is set, so that
	// even if the store with a replica affected by this pending change does not
	// tell us about the enactment, we can garbage collect this change.
	enactedAtTime time.Time
}

func (prc pendingReplicaChange) String() string {
	return fmt.Sprintf("id=%d start=%d %v",
		prc.changeID, prc.startTime.UnixNano(), prc.replicaChange)
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

// storeState maintains the complete state about a store as known to the
// allocator.
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

	localityTiers
}

func newStoreState(storeID roachpb.StoreID, nodeID roachpb.NodeID) *storeState {
	ss := &storeState{
		storeLoad: storeLoad{
			StoreID:    storeID,
			NodeID:     nodeID,
			topKRanges: []rangeLoad{},
		},
	}
	ss.adjusted.loadReplicas = map[roachpb.RangeID]replicaType{}
	ss.adjusted.loadPendingChanges = map[changeID]*pendingReplicaChange{}
	ss.adjusted.replicas = map[roachpb.RangeID]replicaState{}
	return ss
}

// failureDetectionSummary is provided by an external entity and never
// computed inside the allocator.
type failureDetectionSummary uint8

func (fds failureDetectionSummary) String() string {
	return failureDetectionSummaryMap[fds]
}

var failureDetectionSummaryMap map[failureDetectionSummary]string = map[failureDetectionSummary]string{
	fdOK:      "ok",
	fdSuspect: "suspect",
	fdDrain:   "drain",
	fdDead:    "dead",
}

// All state transitions are permitted by the allocator. For example, fdDead
// => fdOk is allowed since the allocator can simply stop shedding replicas
// and then start adding replicas (if underloaded).
const (
	fdOK failureDetectionSummary = iota
	// Don't add replicas or leases.
	fdSuspect
	// Move leases away. Don't add replicas or leases.
	fdDrain
	// Node is dead, so move leases and replicas away from it.
	fdDead
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

func newNodeState(nodeID roachpb.NodeID) *nodeState {
	return &nodeState{
		stores: []roachpb.StoreID{},
		nodeLoad: nodeLoad{
			nodeID: nodeID,
		},
	}
}

type storeIDAndReplicaState struct {
	roachpb.StoreID
	// Only valid ReplicaTypes are used here.
	replicaState
}

type replicaSet []storeIDAndReplicaState

func predVoterFullOrIncoming(repl storeIDAndReplicaState) bool {
	switch repl.replicaType.replicaType {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING:
		return true
	default:
		return false
	}
}

func predNonVoter(repl storeIDAndReplicaState) bool {
	return repl.replicaType.replicaType == roachpb.NON_VOTER
}

func (rs replicaSet) voters() replicaSet {
	return rs.filterReplicas(predVoterFullOrIncoming)
}

func (rs replicaSet) nonVoters() replicaSet {
	return rs.filterReplicas(predNonVoter)
}

func (rs replicaSet) filterReplicas(pred func(storeIDAndReplicaState) bool) replicaSet {
	fastpath := true
	out := rs
	for i := range rs {
		if pred(rs[i]) {
			if !fastpath {
				out = append(out, rs[i])
			}
		} else {
			if fastpath {
				out = nil
				out = append(out, rs[:i]...)
				fastpath = false
			}
		}
	}
	return out
}

func (rs replicaSet) stores() storeIDPostingList {
	sl := make([]roachpb.StoreID, len(rs))
	for i := range rs {
		sl[i] = rs[i].StoreID
	}
	return makeStoreIDPostingList(sl)
}

// rangeState is periodically updated based in reporting by the leaseholder.
type rangeState struct {
	// replicas is the adjusted replicas. It is always consistent with
	// the storeState.adjusted.replicas in the corresponding stores.
	replicas replicaSet
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

func newRangeState() *rangeState {
	return &rangeState{
		replicas:       replicaSet{},
		pendingChanges: []*pendingReplicaChange{},
	}
}

func (rs *rangeState) removePendingChange(change *pendingReplicaChange) {
	i := 0
	n := len(rs.pendingChanges)
	for ; i < n; i++ {
		if rs.pendingChanges[i] == change {
			rs.pendingChanges[i], rs.pendingChanges[n-1] = rs.pendingChanges[n-1], rs.pendingChanges[i]
			rs.pendingChanges = rs.pendingChanges[:n-1]
			break
		}
	}
	rs.constraints = nil
}

// clusterState is the state of the cluster known to the allocator, including
// adjustments based on pending changes. It does not include additional
// indexing needed for constraint matching, or for tracking ranges that may
// need attention etc. (those happen at a higher layer).
type clusterState struct {
	nodes     map[roachpb.NodeID]*nodeState
	storeList storeIDPostingList
	stores    map[roachpb.StoreID]*storeState
	ranges    map[roachpb.RangeID]*rangeState
	// Added to when a change is proposed. Will also add to corresponding
	// rangeState.pendingChanges and to the effected storeStates.
	//
	// Removed from based on rangeMsg, explicit rejection by enacting module, or
	// time-based GC. There is no explicit acceptance by enacting module since
	// the single source of truth of a rangeState is the leaseholder.
	pendingChanges map[changeID]*pendingReplicaChange
	changeSeqGen   changeID

	*constraintMatcher
	*localityTierInterner

	clock *hlc.Clock
}

func newClusterState(clock *hlc.Clock, interner *stringInterner) *clusterState {
	return &clusterState{
		nodes:                map[roachpb.NodeID]*nodeState{},
		stores:               map[roachpb.StoreID]*storeState{},
		ranges:               map[roachpb.RangeID]*rangeState{},
		pendingChanges:       map[changeID]*pendingReplicaChange{},
		constraintMatcher:    newConstraintMatcher(interner),
		localityTierInterner: newLocalityTierInterner(interner),
		clock:                clock,
	}
}

//======================================================================
// clusterState mutators
//======================================================================

func (cs *clusterState) processNodeLoadResponse(resp *nodeLoadResponse) error {
	if resp.lastLoadSeqNum != fullUpdateLoadSeqNum {
		// TODO(kvoli,sumeerbhola): Handle delta responses.
		panic("unimplemented")
	}
	now := cs.clock.PhysicalTime()
	cs.gcPendingChanges(now)
	if err := cs.processNodeLoadMsg(resp.nodeLoad); err != nil {
		return err
	}
	for _, msg := range resp.stores {
		if err := cs.processStoreLoadMsg(now, msg); err != nil {
			return err
		}
	}
	for _, msg := range resp.leaseholderStores {
		if err := cs.processStoreLeaseMsg(now, msg); err != nil {
			return err
		}
	}
	return nil
}

func (cs *clusterState) processNodeLoadMsg(msg nodeLoad) error {
	if ns, exists := cs.nodes[msg.nodeID]; exists {
		ns.adjustedCPU = msg.reportedCPU
		ns.reportedCPU = msg.reportedCPU
		ns.nodeLoad = msg
		return nil
	}

	return errors.Errorf(
		"node %d doesn't exist", msg.nodeID)
}

func (cs *clusterState) processStoreLoadMsg(now time.Time, msg storeLoadMsg) error {
	var ss *storeState
	var exists bool
	if ss, exists = cs.stores[msg.StoreID]; !exists {
		panic("unhandled")
	}
	ss.storeLoad.reportedLoad = msg.load
	ss.storeLoad.capacity = msg.capacity
	ss.storeLoad.reportedSecondaryLoad = msg.secondaryLoad
	ss.storeLoad.meanNonTopKRangeLoad = msg.meanNonTopKRangeLoad
	ss.storeLoad.topKRanges = msg.topKRanges
	for _, r := range msg.storeRanges {
		// TODO(kvoli,sumeerbhola): Handle pending changes.
		ss.adjusted.loadReplicas[r.RangeID] = r.replicaType
		rs, ok := cs.ranges[r.RangeID]
		if !ok {
			rs = newRangeState()
			cs.ranges[r.RangeID] = rs
		}
		rs.lastHeardTime = now
	}
	ss.loadSeqNum++

	return nil
}

func (cs *clusterState) processStoreLeaseMsg(now time.Time, msg storeLeaseholderMsg) error {
	for _, rMsg := range msg.ranges {
		for _, replica := range rMsg.replicas {
			ss := cs.stores[replica.StoreID]
			ss.adjusted.replicas[rMsg.RangeID] = replica.replicaState
		}
		rs, ok := cs.ranges[rMsg.RangeID]
		if !ok {
			rs = newRangeState()
			cs.ranges[rMsg.RangeID] = rs
		}
		rs.replicas = rMsg.replicas
		rs.lastHeardTime = now
		normSpanConfig, err := makeNormalizedSpanConfig(&rMsg.conf, cs.constraintMatcher.interner)
		if err != nil {
			return err
		}
		rs.conf = normSpanConfig
	}
	return nil
}

func (cs *clusterState) addNodeID(nodeID roachpb.NodeID) error {
	if _, exists := cs.nodes[nodeID]; exists {
		return errors.Errorf("node with ID %d already exists", nodeID)
	}
	cs.nodes[nodeID] = newNodeState(nodeID)
	return nil
}

func (cs *clusterState) addStore(store roachpb.StoreDescriptor) error {
	// We expect to have not know of the store previously. The store's node must
	// be added first via addNodeID.
	var ns *nodeState
	var ss *storeState
	var exists bool
	if ns, exists = cs.nodes[store.Node.NodeID]; !exists {
		return errors.Errorf(
			"store %d's node %d not known", store.StoreID, store.Node.NodeID)
	}
	if ss, exists = cs.stores[store.StoreID]; exists {
		switch ss.storeInitState {
		case partialInit:
			// The adjusted.replicas have been initialized on this store. This can
			// occur if the leaseholder sends information prior to the allocator
			// learning of the store.
		case fullyInit, removed:
			return errors.Errorf("store %d already fully initialized", store.StoreID)
		}
	} else {
		ss = newStoreState(store.StoreID, store.Node.NodeID)
		cs.stores[store.StoreID] = ss
		ns.stores = append(ns.stores, store.StoreID)
	}

	ss.storeLoad.StoreDescriptor = store
	ss.localityTiers = cs.localityTierInterner.intern(store.Locality())
	cs.constraintMatcher.setStore(store)
	cs.storeList.insert(store.StoreID)
	return nil
}

func (cs *clusterState) changeStore(store roachpb.StoreDescriptor) error {
	var ss *storeState
	var exists bool
	if _, exists = cs.nodes[store.Node.NodeID]; !exists {
		return errors.Errorf(
			"store %d's node %d not known", store.StoreID, store.Node.NodeID)
	}
	if ss, exists = cs.stores[store.StoreID]; !exists {
		return errors.Errorf("store %d does not exist", store.StoreID)
	} else {
		switch ss.storeInitState {
		case fullyInit:
			// We expect the store being changed have been added via addStore - so it
			// should be fully initialized.
		case partialInit:
			return errors.Errorf("store %d not fully initialized", store.StoreID)
		case removed:
			return errors.Errorf(
				"changing store %d that has been removed", store.StoreID)
		}
	}

	ss.storeLoad.StoreDescriptor = store
	ss.localityTiers = cs.localityTierInterner.intern(store.Locality())
	cs.constraintMatcher.setStore(store)
	return nil
}

func (cs *clusterState) removeNodeAndStores(nodeID roachpb.NodeID) error {
	var ns *nodeState
	var exists bool
	if ns, exists = cs.nodes[nodeID]; !exists {
		return errors.Errorf(
			"node %d doesn't exist", nodeID)
	}

	for _, storeID := range ns.stores {
		// TODO(kvoli): Handle cleaning up range state left around.
		ss := cs.stores[storeID]
		ss.storeInitState = removed
		cs.storeList.remove(storeID)
	}
	delete(cs.nodes, nodeID)

	return nil
}

// If the pending change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingChangeGCDuration = 5 * time.Minute

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges(now time.Time) {
	// TODO(sumeer):
}

// Called by enacting module.
func (cs *clusterState) pendingChangesRejected(
	rangeID roachpb.RangeID, changes []pendingReplicaChange,
) {
	// TODO(sumeer):
}

// makePendingChanges takes a set of changes for a range and applies the
// changes as pending. The application updates the adjusted load, tracked
// pending changes and changeID to reflect the pending application.
func (cs *clusterState) makePendingChanges(
	rangeID roachpb.RangeID, changes []replicaChange,
) []*pendingReplicaChange {
	now := cs.clock.PhysicalTime()
	var pendingChanges []*pendingReplicaChange
	for _, change := range changes {
		pendingChanges = append(pendingChanges, cs.makePendingChange(now, change))
	}
	return pendingChanges
}

func (cs *clusterState) makePendingChange(
	now time.Time, change replicaChange,
) *pendingReplicaChange {
	storeState := cs.stores[change.storeID]
	rangeState := cs.ranges[change.rangeID]
	if change.prev.ReplicaID >= 0 && change.next.ReplicaID == noReplicaID {
		// The change is to remove a replica.
		var idx int
		for idx = range rangeState.replicas {
			if rangeState.replicas[idx].StoreID == change.storeID {
				break
			}
		}
		// NB: This will NPE if the replica being removed doesn't exist.
		rangeState.replicas = append(
			rangeState.replicas[:idx],
			rangeState.replicas[idx+1:]...,
		)
		delete(storeState.adjusted.loadReplicas, change.rangeID)
		delete(storeState.adjusted.replicas, change.rangeID)
	} else if change.prev.ReplicaID == noReplicaID && change.next.ReplicaID == unknownReplicaID {
		// The change is to add a new replica.
		pendingRepl := storeIDAndReplicaState{
			StoreID: change.storeID,
			replicaState: replicaState{
				replicaIDAndType: change.next,
			},
		}
		storeState.adjusted.loadReplicas[change.rangeID] = pendingRepl.replicaType
		storeState.adjusted.replicas[change.rangeID] = pendingRepl.replicaState
		rangeState.replicas = append(rangeState.replicas, pendingRepl)
	} else if change.prev.ReplicaID >= 0 && change.next.ReplicaID >= 0 {
		// The change is to update an existing replica.
		var idx int
		for idx = range rangeState.replicas {
			if rangeState.replicas[idx].StoreID == change.storeID {
				break
			}
		}
		replState := storeState.adjusted.replicas[change.rangeID]
		replState.replicaIDAndType = change.next
		// NB: This will NPE if the replica being updated doesn't exist.
		rangeState.replicas[idx].replicaState = replState
		storeState.adjusted.loadReplicas[change.rangeID] = replState.replicaType
		storeState.adjusted.replicas[change.rangeID] = replState
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}

	// Apply the load delta to the node and store.
	storeState.adjusted.load.add(change.loadDelta)
	cs.nodes[storeState.NodeID].adjustedCPU += change.loadDelta[cpu]
	// Grab the changeID and store the pending change.
	cs.changeSeqGen++
	cid := cs.changeSeqGen
	pendingChange := &pendingReplicaChange{
		changeID:      cid,
		replicaChange: change,
		startTime:     now,
	}
	cs.pendingChanges[cid] = pendingChange
	storeState.adjusted.loadPendingChanges[cid] = pendingChange
	rangeState.pendingChanges = append(rangeState.pendingChanges, pendingChange)
	return pendingChange
}

func (cs *clusterState) undoPendingChange(cid changeID) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		panic(fmt.Sprintf("pending change %d doesn't exist", cid))
	}
	rangeState := cs.ranges[change.rangeID]
	storeState := cs.stores[change.storeID]
	if change.prev.ReplicaID >= 0 && change.next.ReplicaID == noReplicaID {
		// The change is to remove a replica.
		prevRepl := storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: change.prev,
		}
		storeState.adjusted.loadReplicas[change.rangeID] = prevRepl.replicaType
		storeState.adjusted.replicas[change.rangeID] = prevRepl.replicaState
		rangeState.replicas = append(rangeState.replicas, prevRepl)
	} else if change.prev.ReplicaID == noReplicaID && change.next.ReplicaID == unknownReplicaID {
		// The change was to add a new replica.
		var idx int
		for idx = range rangeState.replicas {
			if rangeState.replicas[idx].StoreID == change.storeID {
				break
			}
		}
		// NB: This will NPE if the replica being removed doesn't exist.
		rangeState.replicas = append(
			rangeState.replicas[:idx],
			rangeState.replicas[idx+1:]...,
		)
		delete(storeState.adjusted.loadReplicas, change.rangeID)
		delete(storeState.adjusted.replicas, change.rangeID)
	} else if change.prev.ReplicaID >= 0 && change.next.ReplicaID >= 0 {
		// The change was to update an existing replica.
		var idx int
		for idx = range rangeState.replicas {
			if rangeState.replicas[idx].StoreID == change.storeID {
				break
			}
		}
		replState := change.prev
		// NB: This will NPE if the replica being updated doesn't exist.
		rangeState.replicas[idx].replicaState = replState
		storeState.adjusted.loadReplicas[change.rangeID] = replState.replicaType
		storeState.adjusted.replicas[change.rangeID] = replState
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}

	negate := change.loadDelta.clone()
	negate.negate()
	storeState.adjusted.load.add(negate)
	cs.nodes[storeState.NodeID].adjustedCPU += negate[cpu]
	delete(cs.pendingChanges, cid)
	delete(storeState.adjusted.loadPendingChanges, cid)
	rangeState.removePendingChange(change)
}

func (cs *clusterState) updateFailureDetectionSummary(
	nodeID roachpb.NodeID, fd failureDetectionSummary,
) error {
	if ns, ok := cs.nodes[nodeID]; ok {
		ns.fdSummary = fd
		return nil
	}
	return errors.Errorf("node %d doesn't exist", nodeID)
}

//======================================================================
// clusterState accessors:
//
// Not all accesses need to use these accessors.
//======================================================================

// For meansMemo.
var _ loadInfoProvider = &clusterState{}

func (cs *clusterState) getStoreReportedLoad(storeID roachpb.StoreID) *storeLoad {
	if storeState, ok := cs.stores[storeID]; ok {
		return &storeState.storeLoad
	}
	return nil
}

func (cs *clusterState) getNodeReportedLoad(nodeID roachpb.NodeID) *nodeLoad {
	if nodeState, ok := cs.nodes[nodeID]; ok {
		return &nodeState.nodeLoad
	}
	return nil
}

func (cs *clusterState) getStoreLocalities(stores ...roachpb.StoreID) []localityTiers {
	storeLocalities := make([]localityTiers, 0, len(stores))
	for _, storeID := range stores {
		if storeState, ok := cs.stores[storeID]; ok {
			storeLocalities = append(storeLocalities, storeState.localityTiers)
		} else {
			continue
		}
	}
	return storeLocalities
}

// canAddLoad returns true if the delta can be added to the store without
// causing it to be overloaded (or the node to be overloaded). It does not
// change any state between the call and return.
func (cs *clusterState) canAddLoad(ss *storeState, delta loadVector, means *meansForStoreSet) bool {
	// TODO(sumeer):
	return false
}

func (cs *clusterState) computeLoadSummary(
	storeID roachpb.StoreID, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	ss := cs.stores[storeID]
	ns := cs.nodes[ss.NodeID]
	sls := loadLow
	for i := range msl.load {
		ls := loadSummaryForDimension(ss.adjusted.load[i], ss.capacity[i], msl.load[i], msl.util[i])
		// Take the maximum load summary for each store.
		if ls < sls {
			sls = ls
		}
	}
	nls := loadSummaryForDimension(ns.adjustedCPU, ns.capacityCPU, mnl.loadCPU, mnl.utilCPU)
	return storeLoadSummary{
		sls:        sls,
		nls:        nls,
		fd:         ns.fdSummary,
		loadSeqNum: ss.loadSeqNum,
	}
}

// Avoid unused lint errors.

var _ = (&pendingChangesOldestFirst{}).removeChangeAtIndex
var _ = (&clusterState{}).processNodeLoadResponse
var _ = (&clusterState{}).addStore
var _ = (&clusterState{}).removeNodeAndStores
var _ = (&clusterState{}).gcPendingChanges
var _ = (&clusterState{}).pendingChangesRejected
var _ = (&clusterState{}).updateFailureDetectionSummary
var _ = (&clusterState{}).getStoreReportedLoad
var _ = (&clusterState{}).getNodeReportedLoad
var _ = (&clusterState{}).canAddLoad
var _ = (&clusterState{}).computeLoadSummary
var _ = unknownReplicaID
var _ = noReplicaID
var _ = fdSuspect
var _ = fdDrain
var _ = fdDead
var _ = partialInit
var _ = fullyInit
var _ = removed
var _ = pendingChangeGCDuration
var _ = replicaType{}.replicaType
var _ = replicaType{}.isLeaseholder
var _ = replicaIDAndType{}.ReplicaID
var _ = replicaIDAndType{}.replicaType
var _ = replicaState{}.replicaIDAndType
var _ = replicaState{}.voterIsLagging
var _ = replicaState{}.replicationPaused
var _ = pendingReplicaChange{}.changeID
var _ = pendingReplicaChange{}.loadDelta
var _ = pendingReplicaChange{}.storeID
var _ = pendingReplicaChange{}.rangeID
var _ = pendingReplicaChange{}.prev
var _ = pendingReplicaChange{}.next
var _ = pendingReplicaChange{}.startTime
var _ = pendingReplicaChange{}.enactedAtTime
var _ = storeState{}.storeInitState
var _ = storeState{}.storeLoad
var _ = storeState{}.adjusted.loadReplicas
var _ = storeState{}.adjusted.loadPendingChanges
var _ = storeState{}.adjusted.secondaryLoad
var _ = storeState{}.adjusted.replicas
var _ = storeState{}.maxFractionPending
var _ = storeState{}.localityTiers
var _ = nodeState{}.stores
var _ = nodeState{}.nodeLoad
var _ = nodeState{}.adjustedCPU
var _ = nodeState{}.loadSummary
var _ = nodeState{}.fdSummary
var _ = storeIDAndReplicaState{}.StoreID
var _ = storeIDAndReplicaState{}.replicaState
var _ = rangeState{}.replicas
var _ = rangeState{}.conf
var _ = rangeState{}.pendingChanges
var _ = rangeState{}.constraints
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
var _ = rangeState{}.lastHeardTime
var _ = clusterState{}.nodes
var _ = clusterState{}.stores
var _ = clusterState{}.ranges
var _ = clusterState{}.pendingChanges
var _ = clusterState{}.constraintMatcher
var _ = clusterState{}.localityTierInterner
