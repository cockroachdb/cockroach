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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

// SafeFormat implements the redact.SafeFormatter interface.
func (rt replicaIDAndType) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print("replica-id=")
	switch rt.ReplicaID {
	case unknownReplicaID:
		w.Print("unknown")
	case noReplicaID:
		w.Print("none")
	default:
		w.Print(rt.ReplicaID)
	}
	w.Printf(" lease=%v type=%v", rt.isLeaseholder, rt.replicaType.replicaType)
}

func (rt replicaIDAndType) String() string {
	return redact.StringWithoutMarkers(rt)
}

// prev is the state before the proposed change and next is the state after
// the proposed change. rit is the current observed state.
func (rit replicaIDAndType) subsumesChange(prev replicaIDAndType, next replicaIDAndType) bool {
	if rit.ReplicaID == noReplicaID && next.ReplicaID == noReplicaID {
		// Removal has happened.
		return true
	}
	notSubsumed := (rit.ReplicaID == noReplicaID && next.ReplicaID != noReplicaID) ||
		(rit.ReplicaID != noReplicaID && next.ReplicaID == noReplicaID)
	if notSubsumed {
		return false
	}
	// Both rit and next have replicaIDs != noReplicaID. We don't actually care
	// about the replicaID's since we don't control them. If the replicaTypes
	// are as expected, and if we were either not trying to change the
	// leaseholder, or that leaseholder change has happened, then the change has
	// been subsumed.
	switch rit.replicaType.replicaType {
	case roachpb.VOTER_INCOMING:
		// Already seeing the load, so consider the change done.
		rit.replicaType.replicaType = roachpb.VOTER_FULL
	}
	// rit.replicaId equal to LEARNER, VOTER_DEMOTING* are left as-is. If next
	// is trying to remove a replica, this store is still seeing some of the
	// load.
	if rit.replicaType == next.replicaType && (prev.isLeaseholder == next.isLeaseholder ||
		rit.isLeaseholder == next.isLeaseholder) {
		return true
	}
	return false
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
	loadDelta          loadVector
	secondaryLoadDelta secondaryLoadVector

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

func (rc replicaChange) isRemoval() bool {
	return rc.prev.ReplicaID >= 0 && rc.next.ReplicaID == noReplicaID
}

func (rc replicaChange) isAddition() bool {
	return rc.prev.ReplicaID == noReplicaID && rc.next.ReplicaID == unknownReplicaID
}

func (rc replicaChange) isUpdate() bool {
	return rc.prev.ReplicaID >= 0 && rc.next.ReplicaID >= 0
}

func makeLeaseTransferChanges(
	rLoad rangeLoad, rState *rangeState, addStoreID, removeStoreID roachpb.StoreID,
) [2]replicaChange {
	addIdx, removeIdx := -1, -1
	for i, replica := range rState.replicas {
		if replica.StoreID == addStoreID {
			addIdx = i
		}
		if replica.StoreID == removeStoreID {
			removeIdx = i
		}
	}
	if removeIdx == -1 {
		panic(fmt.Sprintf(
			"existing leaseholder replica doesn't exist on store %v", removeIdx))
	}
	if addIdx == -1 {
		panic(fmt.Sprintf(
			"new leaseholder replica doesn't exist on store %v", addStoreID))
	}
	remove := rState.replicas[removeIdx]
	add := rState.replicas[addIdx]

	removeLease := replicaChange{
		storeID: removeStoreID,
		rangeID: rLoad.RangeID,
		prev:    remove.replicaState,
		next:    remove.replicaIDAndType,
	}
	addLease := replicaChange{
		storeID: addStoreID,
		rangeID: rLoad.RangeID,
		prev:    add.replicaState,
		next:    add.replicaIDAndType,
	}
	addLease.next.isLeaseholder = true
	removeLease.next.isLeaseholder = false

	// Only account for the leaseholder CPU, all other primary load dimensions
	// are ignored. Byte size and write bytes are not impacted by having a range
	// lease.
	nonRaftCPU := rLoad.load[cpu] - rLoad.raftCPU
	removeLease.loadDelta[cpu] -= nonRaftCPU
	addLease.loadDelta[cpu] += nonRaftCPU
	// Also account for the lease count.
	removeLease.secondaryLoadDelta[leaseCount] = -1
	addLease.secondaryLoadDelta[leaseCount] = 1
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

	addReplica.loadDelta.add(rLoad.load)
	// Set the load delta for CPU to be just the raft CPU. The non-raft CPU we
	// assume is associated with the lease.
	addReplica.loadDelta[cpu] = rLoad.raftCPU
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
	removeReplica.loadDelta.sub(rLoad.load)
	// Set the load delta for CPU to be just the raft CPU. The non-raft CPU we
	// assume is associated with the lease.
	removeReplica.loadDelta[cpu] = -rLoad.raftCPU
	return removeReplica
}

// TODO(kvoli,sumeerbhola): If the leaseholder is being rebalanced, we need to
// ensure the incoming replica is eligible to take the lease.
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
	if remove.isLeaseholder {
		// The existing leaseholder is being removed. The incoming replica will
		// take the lease load, in addition to the replica load.
		addReplicaChange.next.isLeaseholder = true
		addReplicaChange.loadDelta = loadVector{}
		removeReplicaChange.loadDelta = loadVector{}
		addReplicaChange.loadDelta.add(rLoad.load)
		removeReplicaChange.loadDelta.sub(rLoad.load)
		addReplicaChange.secondaryLoadDelta[leaseCount] = 1
		addReplicaChange.secondaryLoadDelta[leaseCount] = -1
	}

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

type pendingChangesOldestFirst []*pendingReplicaChange

func (p *pendingChangesOldestFirst) removeChangeAtIndex(index int) {
	n := len(*p)
	copy((*p)[index:n-1], (*p)[index+1:n])
	*p = (*p)[:n-1]
}

func (p *pendingChangesOldestFirst) Len() int {
	return len(*p)
}

func (p *pendingChangesOldestFirst) Less(i, j int) bool {
	return (*p)[i].startTime.Before((*p)[j].startTime)
}

func (p *pendingChangesOldestFirst) Swap(i, j int) {
	(*p)[i], (*p)[j] = (*p)[j], (*p)[i]
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

// applyChangeLoadDelta adds the change load delta to the store's adjusted load.
func (ss *storeState) applyChangeLoadDelta(change replicaChange) {
	ss.adjusted.load.add(change.loadDelta)
	ss.adjusted.secondaryLoad.add(change.secondaryLoadDelta)
	ss.loadSeqNum++
}

// undoChangeLoadDelta subtracts the change load delta from the store's
// adjusted load.
func (ss *storeState) undoChangeLoadDelta(change replicaChange) {
	ss.adjusted.load.sub(change.loadDelta)
	ss.adjusted.secondaryLoad.sub(change.secondaryLoadDelta)
	ss.loadSeqNum++
}

// failureDetectionSummary is provided by an external entity and never
// computed inside the allocator.
type failureDetectionSummary uint8

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

// SafeFormat implements the redact.SafeFormatter interface.
func (fds failureDetectionSummary) SafeFormat(w redact.SafePrinter, _ rune) {
	switch fds {
	case fdOK:
		w.Print("ok")
	case fdSuspect:
		w.Print("suspect")
	case fdDrain:
		w.Print("drain")
	case fdDead:
		w.Print("dead")
	}
}

func (fds failureDetectionSummary) String() string {
	return redact.StringWithoutMarkers(fds)
}

func newNodeState(nodeID roachpb.NodeID) *nodeState {
	return &nodeState{
		stores: []roachpb.StoreID{},
		nodeLoad: nodeLoad{
			nodeID: nodeID,
		},
	}
}

type nodeState struct {
	stores []roachpb.StoreID
	nodeLoad
	adjustedCPU loadValue

	// This loadSummary is only based on the cpu. It is incorporated into the
	// loadSummary computed for each store on this node.
	loadSummary loadSummary
	fdSummary   failureDetectionSummary
}

// applyChangeLoadDelta adds the change load delta to the node's adjusted load.
func (ns *nodeState) applyChangeLoadDelta(change replicaChange) {
	ns.adjustedCPU += change.loadDelta[cpu]
}

// undoChangeLoadDelta subtracts the change load delta from the node's adjusted
// load.
func (ns *nodeState) undoChangeLoadDelta(change replicaChange) {
	ns.adjustedCPU -= change.loadDelta[cpu]
}

type storeIDAndReplicaState struct {
	roachpb.StoreID
	// Only valid ReplicaTypes are used here.
	replicaState
}

func newRangeState() *rangeState {
	return &rangeState{
		replicas:       []storeIDAndReplicaState{},
		pendingChanges: []*pendingReplicaChange{},
	}
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

func (rs *rangeState) removePendingChangeTracking(cid changeID) {
	i := 0
	n := len(rs.pendingChanges)
	for ; i < n; i++ {
		if rs.pendingChanges[i].changeID == cid {
			rs.pendingChanges[i], rs.pendingChanges[n-1] = rs.pendingChanges[n-1], rs.pendingChanges[i]
			rs.pendingChanges = rs.pendingChanges[:n-1]
			break
		}
	}
	rs.constraints = nil
}

func (rs *rangeState) setReplica(repl storeIDAndReplicaState) {
	for i := range rs.replicas {
		if rs.replicas[i].StoreID == repl.StoreID {
			rs.replicas[i].replicaState = repl.replicaState
			return
		}
	}
	rs.replicas = append(rs.replicas, repl)
}

func (rs *rangeState) removeReplica(storeID roachpb.StoreID) {
	var i, n int
	n = len(rs.replicas)
	for ; i < n; i++ {
		if rs.replicas[i].StoreID == storeID {
			rs.replicas[i], rs.replicas[n-1] = rs.replicas[n-1], rs.replicas[i]
			rs.replicas = rs.replicas[:n-1]
			break
		}
	}
}

// clusterState is the state of the cluster known to the allocator, including
// adjustments based on pending changes. It does not include additional
// indexing needed for constraint matching, or for tracking ranges that may
// need attention etc. (those happen at a higher layer).
type clusterState struct {
	nodes  map[roachpb.NodeID]*nodeState
	stores map[roachpb.StoreID]*storeState
	ranges map[roachpb.RangeID]*rangeState
	// storeList contains identical stores to those in the store state map. The
	// list is kept for deterministic iteration over stores.
	storeList storeIDPostingList
	// Added to when a change is proposed. Will also add to corresponding
	// rangeState.pendingChanges and to the effected storeStates.
	//
	// Removed from based on rangeMsg, explicit rejection by enacting module, or
	// time-based GC. There is no explicit acceptance by enacting module since
	// the single source of truth of a rangeState is the leaseholder.
	pendingChanges    map[changeID]*pendingReplicaChange
	pendingChangeList []*pendingReplicaChange
	changeSeqGen      changeID

	*constraintMatcher
	*localityTierInterner

	clock hlc.WallClock
}

func newClusterState(clock hlc.WallClock, interner *stringInterner) *clusterState {
	return &clusterState{
		nodes:                map[roachpb.NodeID]*nodeState{},
		stores:               map[roachpb.StoreID]*storeState{},
		ranges:               map[roachpb.RangeID]*rangeState{},
		pendingChanges:       map[changeID]*pendingReplicaChange{},
		pendingChangeList:    []*pendingReplicaChange{},
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

	now := cs.clock.Now()
	cs.gcPendingChanges(now)

	// Ensure that the stores and node being reported are known to the allocator.
	// If they are not, return an error and don't process the load message.
	ns, ok := cs.nodes[resp.nodeID]
	if !ok {
		return errors.Errorf(
			"node %v doesn't exist", resp.nodeID)
	}
	for _, msg := range resp.stores {
		if _, ok := cs.stores[msg.StoreID]; !ok {
			return errors.Errorf(
				"store %v doesn't exist", msg.StoreID)
		}
	}

	// Handle the leaseholder store messages first. These have do not effect load
	// delta adjustments but will mark pending changes as enacted.
	for _, leaseMsg := range resp.leaseholderStores {
		for _, rangeMsg := range leaseMsg.ranges {
			rs, ok := cs.ranges[rangeMsg.RangeID]
			if !ok {
				rs = newRangeState()
				cs.ranges[rangeMsg.RangeID] = rs
			}
			// Update the authoritative store and range replicas.
			for _, replica := range rangeMsg.replicas {
				ss := cs.stores[replica.StoreID]
				ss.adjusted.replicas[rangeMsg.RangeID] = replica.replicaState
			}
			rs.replicas = rangeMsg.replicas

			// Find any pending changes which are now enacted, according to the
			// leaseholder's authoritative view.
			var remainingChanges []*pendingReplicaChange
			var changeIdx int
			for changeIdx < len(rs.pendingChanges) {
				change := rs.pendingChanges[changeIdx]
				ss := cs.stores[change.storeID]
				replIdAndType, ok := ss.adjusted.replicas[rangeMsg.RangeID]
				if !ok {
					replIdAndType.ReplicaID = noReplicaID
				}
				if replIdAndType.subsumesChange(change.prev.replicaIDAndType, change.next) {
					// Mark change as enacted. Enacting a change does not remove the
					// corresponding load adjustments. The store load message will do
					// that, or GC, indiciating that the change has been reflected in the
					// store load.
					cs.markPendingChangeEnacted(change.changeID, now)
				} else {
					remainingChanges = append(remainingChanges, change)
				}
			}

			// Re-apply the remaining changes. These do not update the load state,
			// only the authoritative replicas tracking in store state and range
			// state.
			for _, change := range remainingChanges {
				cs.applyReplicaChange(change.replicaChange, false /* applyLoad */)
			}

			rs.lastHeardTime = now
			normSpanConfig, err := makeNormalizedSpanConfig(&rangeMsg.conf, cs.constraintMatcher.interner)
			if err != nil {
				return err
			}
			rs.conf = normSpanConfig
		}
	}

	// Handle the node load, updating the reported load and set the adjusted load
	// to be equal to the reported load initially. Any remaining pending changes
	// will be re-applied to the reported load.
	ns.reportedCPU = resp.reportedCPU
	ns.adjustedCPU = resp.reportedCPU
	ns.nodeLoad = resp.nodeLoad
	for _, msg := range resp.stores {
		// NB: The store must exist, we checked it above.
		ss := cs.stores[msg.StoreID]
		initialLoadSeqNum := ss.loadSeqNum
		newLoadReplicas := map[roachpb.RangeID]replicaType{}
		oldLoadReplicas := ss.adjusted.loadReplicas
		for _, r := range msg.storeRanges {
			// Upate the last heard from time for the range, creating a new range state
			// if we have never heard about this range before.
			rs, ok := cs.ranges[r.RangeID]
			if !ok {
				rs = newRangeState()
				cs.ranges[r.RangeID] = rs
			}
			rs.lastHeardTime = now
			newLoadReplicas[r.RangeID] = r.replicaType
		}
		// Update the store reported load. The reported load ignores any
		// adjustments.
		ss.storeLoad.reportedLoad = msg.load
		ss.storeLoad.capacity = msg.capacity
		ss.storeLoad.reportedSecondaryLoad = msg.secondaryLoad
		ss.storeLoad.meanNonTopKRangeLoad = msg.meanNonTopKRangeLoad
		ss.storeLoad.topKRanges = msg.topKRanges
		// Reset the adjusted load to be the reported load. We will re-apply any
		// remaining pending change deltas to the updated adjusted load.
		ss.adjusted.load = ss.storeLoad.reportedLoad
		ss.adjusted.secondaryLoad = msg.secondaryLoad
		// Find any pending changes for the range which involve this store. These
		// pending changes can now be removed from the loadPendingChanges. We don't
		// need to un-do the corresponding delta adjustment as the reported load
		// already contains the effect.
		for _, change := range ss.adjusted.loadPendingChanges {
			newRepl, inNew := newLoadReplicas[change.rangeID]
			oldRepl, inOld := oldLoadReplicas[change.rangeID]
			var changeReported bool
			// Note to the reviewer: It isn't clear whether this was the intended
			// approach for invalidation for load updates given the kernel prototype.
			//
			// TODO(kvoli): Unify this with subsumesChange.
			if change.isAddition() && inNew {
				// The replica has been added,
				changeReported = true
			} else if change.isRemoval() && !inNew {
				// The replica has been removed.
				changeReported = true
			} else if change.isUpdate() && inNew {
				// The replica update has been applied.
				if newRepl == oldRepl {
					changeReported = true
				}
			}
			if changeReported {
				delete(ss.adjusted.loadPendingChanges, change.changeID)
			} else {
				// The pending change hasn't been reported as done, re-apply the load
				// delta to the adjusted load and include it in the new adjusted load
				// replicas.
				cs.applyChangeLoadDelta(change.replicaChange)
				if inOld {
					// If the pending change is to remove a replica, we don't set it in
					// the load replicas. Otherwise, update the load replicas to be the
					// change's next replica state.
					newLoadReplicas[change.rangeID] = change.next.replicaType
				}
			}
		}
		ss.adjusted.loadReplicas = newLoadReplicas
		ss.loadSeqNum = initialLoadSeqNum + 1
	}
	return nil
}

func (cs *clusterState) setStore(store roachpb.StoreDescriptor) error {
	var ns *nodeState
	var ss *storeState
	var exists bool
	if ns, exists = cs.nodes[store.Node.NodeID]; !exists {
		ns = newNodeState(store.Node.NodeID)
		cs.nodes[store.Node.NodeID] = ns
	}
	if ss, exists = cs.stores[store.StoreID]; exists {
		switch ss.storeInitState {
		case partialInit, fullyInit:
			// When in partialInit, the adjusted.replicas have been initialized on
			// this store. This can occur if the leaseholder sends information prior to
			// the allocator learning of the store.
		case removed:
			// TODO(kvoli,sumeerbhola): Should we allow setting the store after it
			// has been tombstoned?
			return errors.Errorf("setting store %d already removed", store.StoreID)
		}
	} else {
		ss = newStoreState(store.StoreID, store.Node.NodeID)
		cs.stores[store.StoreID] = ss
		ns.stores = append(ns.stores, store.StoreID)
		cs.storeList.insert(store.StoreID)
	}
	// Mark the store as fully initialized any time it is set.
	ss.storeInitState = fullyInit
	ss.storeLoad.StoreDescriptor = store
	ss.localityTiers = cs.localityTierInterner.intern(store.Locality())
	cs.constraintMatcher.setStore(store)
	// Update the locality of any other stores on the same node. It is unlikely
	// the locality changed, however possible after a restart, in which case the
	// old locality would be updated one at a time.
	for _, storeID := range ns.stores {
		nodeStore := cs.stores[storeID]
		if ss.localityTiers.str != nodeStore.str {
			nodeStore.localityTiers = ss.localityTiers
			nodeStore.StoreDescriptor.Node = store.Node
			cs.constraintMatcher.setStore(nodeStore.StoreDescriptor)
		}
	}

	return nil
}

func (cs *clusterState) removeNodeAndStores(nodeID roachpb.NodeID) error {
	var ns *nodeState
	var exists bool
	if ns, exists = cs.nodes[nodeID]; !exists {
		return errors.Errorf(
			"removing node %d doesn't exist", nodeID)
	}
	for _, storeID := range ns.stores {
		// NB: Ranges and pending changes may still reference this store. The
		// changes will be GC'd later and the left over replicas removed when the
		// leaseholder providides an update.
		ss := cs.stores[storeID]
		ss.storeInitState = removed
		cs.storeList.remove(storeID)
		cs.constraintMatcher.removeStore(storeID)
	}
	delete(cs.nodes, nodeID)
	return nil
}

// If the pending change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingChangeGCDuration = 5 * time.Minute

// if an enacted pending change is not reflected in a store's reported load
// after this GC duration, we forget it in the data-structure.
const enactedPendingChangeGCDuration = 15 * time.Second

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges(now time.Time) {
	gcBeforeTime := now.Add(-pendingChangeGCDuration)
	oldestFirst := pendingChangesOldestFirst(cs.pendingChangeList)
	sort.Sort(&oldestFirst)
	var removeChangeIds []changeID
	var i, n int
	n = len(cs.pendingChangeList)
	for ; i < n; i++ {
		change := cs.pendingChangeList[i]
		// GC the change If either the start time of the pending change is too
		// old.
		if !change.startTime.After(gcBeforeTime) {
			removeChangeIds = append(removeChangeIds, change.changeID)
		}
	}
	for _, rmChange := range removeChangeIds {
		cs.undoPendingChange(rmChange)
	}

	// Find any enacted pending changes which were enacted more than
	// enactedPendingChangeGCDuration duration ago. These can also be GC'd.
	//
	// TODO(kvoli,sumeerbhola): Track a list of enacted changes for faster GC.
	gcEnactedBeforeTime := now.Add(-enactedPendingChangeGCDuration)
	for _, ss := range cs.stores {
		for _, change := range ss.adjusted.loadPendingChanges {
			if !(change.enactedAtTime == time.Time{}) &&
				!change.enactedAtTime.After(gcEnactedBeforeTime) {
				// NB: We only need to undo the load adjustments and load replicas. The
				// change was enacted, so it will already be removed from the
				// store.adjusted.replicas/range.replicas.
				cs.undoReplicaChange(change.replicaChange, false /* undoRepls*/)
				delete(ss.adjusted.loadPendingChanges, change.changeID)
			}
		}
	}
}

// Called by enacting module.
func (cs *clusterState) pendingChangesRejected(changes []changeID) {
	// Wipe rejected changes, including load adjustments, tracking and replica
	// changes.
	//
	// TODO(sumeerbhola,kvoli): This won't handle rejecting changes which have
	// been partially reported as complete by the store load. Handle this or
	// reason that it won't happen.
	for _, changeID := range changes {
		cs.undoPendingChange(changeID)
	}
}

// makePendingChanges takes a set of changes for a range and applies the
// changes as pending. The application updates the adjusted load, tracked
// pending changes and changeID to reflect the pending application.
func (cs *clusterState) makePendingChanges(
	rangeID roachpb.RangeID, changes []replicaChange,
) []*pendingReplicaChange {
	now := cs.clock.Now()
	var pendingChanges []*pendingReplicaChange
	for _, change := range changes {
		pendingChange := cs.makePendingChange(now, change)
		pendingChanges = append(pendingChanges, pendingChange)
	}
	return pendingChanges
}

func (cs *clusterState) makePendingChange(
	now time.Time, change replicaChange,
) *pendingReplicaChange {
	// Apply the load and replica change to state.
	cs.applyReplicaChange(change, true /* applyLoad */)
	// Grab the next change ID, then add the pending change to the pending change
	// trackers on the range, store and cluster state.
	cs.changeSeqGen++
	cid := cs.changeSeqGen
	pendingChange := &pendingReplicaChange{
		changeID:      cid,
		replicaChange: change,
		startTime:     now,
		enactedAtTime: time.Time{},
	}
	storeState := cs.stores[change.storeID]
	rangeState := cs.ranges[change.rangeID]
	cs.pendingChanges[cid] = pendingChange
	cs.pendingChangeList = append(cs.pendingChangeList, pendingChange)
	storeState.adjusted.loadPendingChanges[cid] = pendingChange
	rangeState.pendingChanges = append(rangeState.pendingChanges, pendingChange)
	return pendingChange
}

func (cs *clusterState) applyReplicaChange(change replicaChange, applyLoad bool) {
	storeState := cs.stores[change.storeID]
	rangeState := cs.ranges[change.rangeID]
	if change.isRemoval() {
		// The change is to remove a replica.
		var idx int
		for idx = range rangeState.replicas {
			if rangeState.replicas[idx].StoreID == change.storeID {
				break
			}
		}
		delete(storeState.adjusted.replicas, change.rangeID)
		rangeState.removeReplica(change.storeID)
		if applyLoad {
			delete(storeState.adjusted.loadReplicas, change.rangeID)
		}
	} else if change.isAddition() {
		// The change is to add a new replica.
		pendingRepl := storeIDAndReplicaState{
			StoreID: change.storeID,
			replicaState: replicaState{
				replicaIDAndType: change.next,
			},
		}
		storeState.adjusted.replicas[change.rangeID] = pendingRepl.replicaState
		rangeState.setReplica(pendingRepl)
		if applyLoad {
			storeState.adjusted.loadReplicas[change.rangeID] = pendingRepl.replicaType
		}
	} else if change.isUpdate() {
		// The change is to update an existing replica.
		replState := storeState.adjusted.replicas[change.rangeID]
		replState.replicaIDAndType = change.next
		storeState.adjusted.replicas[change.rangeID] = replState
		rangeState.setReplica(storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: replState,
		})
		if applyLoad {
			storeState.adjusted.loadReplicas[change.rangeID] = replState.replicaType
		}
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}

	if applyLoad {
		cs.applyChangeLoadDelta(change)
	}
}

func (cs *clusterState) markPendingChangeEnacted(cid changeID, enactedAt time.Time) {
	change := cs.pendingChanges[cid]
	change.enactedAtTime = enactedAt
	cs.removePendingChangeTracking(change.changeID)
}

func (cs *clusterState) undoPendingChange(cid changeID) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		// The pending change doesn't exist.
		return
	}
	// Undo the change effects, including changes to the range replicas.
	cs.undoReplicaChange(change.replicaChange, true /* undoRepls */)
	// Remove the change from tracking all tracking.
	cs.removePendingChangeTracking(cid)
	delete(cs.stores[change.storeID].adjusted.loadPendingChanges, change.changeID)
}

func (cs *clusterState) undoReplicaChange(change replicaChange, undoRepls bool) {
	rangeState := cs.ranges[change.rangeID]
	storeState := cs.stores[change.storeID]
	if change.isRemoval() {
		// The change was to remove a replica. Add the replica back using the prev
		// state stored in the replica change.
		prevRepl := storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: change.prev,
		}
		storeState.adjusted.loadReplicas[change.rangeID] = prevRepl.replicaType
		if undoRepls {
			rangeState.setReplica(prevRepl)
			storeState.adjusted.replicas[change.rangeID] = prevRepl.replicaState
		}
	} else if change.isAddition() {
		// The change was to add a new replica.
		delete(storeState.adjusted.loadReplicas, change.rangeID)
		if undoRepls {
			delete(storeState.adjusted.replicas, change.rangeID)
			rangeState.removeReplica(change.storeID)
		}
	} else if change.isUpdate() {
		// The change was to update an existing replica.
		replState := change.prev
		storeState.adjusted.loadReplicas[change.rangeID] = replState.replicaType
		if undoRepls {
			rangeState.setReplica(storeIDAndReplicaState{
				StoreID:      change.storeID,
				replicaState: replState,
			})
			storeState.adjusted.replicas[change.rangeID] = replState
		}
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}
	// Undo the load adjustments that were applied for this change.
	cs.undoChangeLoadDelta(change)
}

// TODO(kvoli,sumeerbhola): The load of the store and node can become negative
// when applying or undoing load adjustments. For load adjustments to be
// reversible quickly, we aren't able to zero out the value when negative. We
// should handle the negative values when using them.

// applyChangeLoadDelta adds the change load delta to the adjusted load of the
// store and node affected.
func (cs *clusterState) applyChangeLoadDelta(change replicaChange) {
	ss := cs.stores[change.storeID]
	ns := cs.nodes[ss.NodeID]
	ss.applyChangeLoadDelta(change)
	ns.applyChangeLoadDelta(change)
}

// undoChangeLoadDelta subtracts the change load delta from the adjusted load
// of the store and node affected.
func (cs *clusterState) undoChangeLoadDelta(change replicaChange) {
	ss := cs.stores[change.storeID]
	ns := cs.nodes[ss.NodeID]
	ss.undoChangeLoadDelta(change)
	ns.undoChangeLoadDelta(change)
}

// removePendingChangeTracking removes the given from the cluster and range
// pending change tracking. If the change doesn't exist, this is a no-op.
// Note the change's effects aren't removed, it is no longer tracked.
func (cs *clusterState) removePendingChangeTracking(cid changeID) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		// Nothing to do.
		return
	}
	cs.ranges[change.rangeID].removePendingChangeTracking(change.changeID)
	delete(cs.pendingChanges, change.changeID)
	i := 0
	n := len(cs.pendingChangeList)
	for ; i < n; i++ {
		if cs.pendingChangeList[i].changeID == cid {
			cs.pendingChangeList[i], cs.pendingChangeList[n-1] = cs.pendingChangeList[n-1], cs.pendingChangeList[i]
			cs.pendingChangeList = cs.pendingChangeList[:n-1]
			break
		}
	}
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
		// TODO(kvoli,sumeerbhola): Handle negative adjusted store/node loads.
		ls := loadSummaryForDimension(ss.adjusted.load[i], ss.capacity[i], msl.load[i], msl.util[i])
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
var _ = (&clusterState{}).canAddLoad
var _ = replicaState{}.voterIsLagging
var _ = replicaState{}.replicationPaused
var _ = storeState{}.maxFractionPending
var _ = nodeState{}.loadSummary
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
