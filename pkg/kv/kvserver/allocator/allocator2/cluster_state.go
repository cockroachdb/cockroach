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

// makeAddReplicaChange creates a replica change which adds the replica type
// to the store addStoreID. The load impact of adding the replica does not
// account for whether the replica is becoming the leaseholder or not.
//
// TODO(kvoli,sumeerbhola): Add promotion/demotion changes.
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

// makeRemoveReplicaChange creates a replica change which removes the replica
// given. The load impact of removing the replica does not account for whether
// the replica was the previous leaseholder or not.
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

// makeRebalanceReplicaChanges creates to replica changes, adding a replica and
// removing another. If the replica being rebalanced is the current
// leaseholder, the impact of the rebalance also includes the lease load.
//
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

// SafeFormat implements the redact.SafeFormatter interface.
func (sis storeInitState) SafeFormat(w redact.SafePrinter, _ rune) {
	switch sis {
	case partialInit:
		w.Print("partial")
	case fullyInit:
		w.Print("full")
	case removed:
		w.Print("removed")
	default:
		w.Print("unknown")
	}
}

func (sis storeInitState) String() string {
	return redact.StringWithoutMarkers(sis)
}

// storeChangeRateLimiter and helpers.
//
// Usage:
// - construct a single storeChangeRateLimiter for the allocator using
//   newStoreChangeRateLimiter. If all allocators are operating at 10s
//   intervals, a gcThreshold of 15s should be enough.
// - storeEnactedHistory is a member of storeState, so one per store.
// - storeEnactedHistory.addEnactedChange must be called every time a change
//   is known to be enacted.
// - Before every rebalancing pass, call
//   storeChangeRateLimiter.initForRebalancePass. Then iterate over each store
//   and call storeChangeRateLimiter.updateForRebalancePass. The latter step
//   updates the rate limiting state for the store -- this "rate limiting" is
//   simply a bool, which represents whether changes are allowed to the store
//   for load reasons (changes for failure reasons, to shed load, are always
//   allowed), in this rebalancing pass. Note, changes made during the
//   rebalancing pass, and rate limiting those, is not in scope of this rate
//   limiter -- that happens via storeState.adjusted.loadPendingChanges.
// - During the rebalancing pass, use
//   storeEnactedHistory.allowLoadBasedChanges, to decide whether a store can
//   be the source or target for load based rebalancing.
//
// TODO(sumeer): unit test.
//
// TODO(sumeer,kvoli): integrate this into the rest of allocator2.

// enactedReplicaChange is information about a change at a store that was
// enacted. It is an internal implementation detail of storeEnactedHistory and
// storeChangeRateLimiter.
type enactedReplicaChange struct {
	// When the change is known to be enacted, based on the authoritative
	// information received from the leaseholder.
	enactedAtTime time.Time
	// The load this change adds to a store. The values are always positive,
	// even for load subtraction.
	loadDelta      loadVector
	secondaryDelta secondaryLoadVector
}

// storeEnactedHistory is a member of storeState. Users should only call
// addEnactedChange and allowLoadBasedChanges.
type storeEnactedHistory struct {
	changes        []enactedReplicaChange
	totalDelta     loadVector
	totalSecondary secondaryLoadVector
	allowChanges   bool
}

func (h *storeEnactedHistory) addEnactedChange(change *pendingReplicaChange) {
	c := enactedReplicaChange{
		enactedAtTime: change.enactedAtTime,
		loadDelta:     change.loadDelta,
	}
	for i, l := range c.loadDelta {
		if l < 0 {
			c.loadDelta[i] = -l
		}
	}
	if change.prev.isLeaseholder != change.next.isLeaseholder {
		c.secondaryDelta[leaseCount] = 1
	}
	h.changes = append(h.changes, c)
	sort.Slice(h.changes, func(i, j int) bool {
		return h.changes[i].enactedAtTime.Before(h.changes[j].enactedAtTime)
	})
	h.totalDelta.add(c.loadDelta)
	h.totalSecondary.add(c.secondaryDelta)
}

func (h *storeEnactedHistory) allowLoadBasedChanges() bool {
	return h.allowChanges
}

func (h *storeEnactedHistory) gcHistory(now time.Time, gcThreshold time.Duration) {
	i := 0
	for ; i < len(h.changes); i++ {
		c := h.changes[i]
		if now.Sub(c.enactedAtTime) > gcThreshold {
			h.totalDelta.sub(c.loadDelta)
			h.totalSecondary.sub(c.secondaryDelta)
		} else {
			break
		}
	}
	h.changes = h.changes[i:]
}

// storeChangeRateLimiter is a rate limiter for rebalancing to/from this
// store, for this allocator, under the assumption that the cluster uses many
// (distributed) allocators. The rate limiting heuristic assumes that the
// other allocators are also running with such a rate limiter, and is intended
// to prevent a thundering herd problem where all allocators add/remove too
// much load from a store, resulting in perpetual rebalancing without
// improving the cluster.
type storeChangeRateLimiter struct {
	gcThreshold   time.Duration
	numAllocators int
	clusterMean   meanStoreLoad
}

func newStoreChangeRateLimiter(gcThreshold time.Duration) *storeChangeRateLimiter {
	return &storeChangeRateLimiter{
		gcThreshold: gcThreshold,
	}
}

func (crl *storeChangeRateLimiter) initForRebalancePass(
	numAllocators int, clusterMean meanStoreLoad,
) {
	crl.numAllocators = numAllocators
	crl.clusterMean = clusterMean
}

// TODO(sumeer): also consider utilization in the cluster and capacity of this
// store, since that is necessary to make this reasonable for heterogeneous
// clusters.

func (crl *storeChangeRateLimiter) updateForRebalancePass(h *storeEnactedHistory, now time.Time) {
	h.gcHistory(now, crl.gcThreshold)
	if len(h.changes) == 0 {
		// If there is nothing in the history, changes are allowed. This is the
		// path we will fall into for underloaded clusters where the mean is very
		// low and the projectedDelta will become too high even with a single
		// change.
		//
		// Additionally, if the actual number of allocators that can add or remove
		// load from a store is much lower than numAllocators (because of
		// constraints) we may often need to use this fallback. We do assume that
		// typical clusters will be configured in a manner that each store will be
		// able to satisfy some constraint for a substantial fraction of the
		// ranges in the cluster (say > 50%), so the pessimistic behavior encoded
		// in the code below will not significantly slow down allocator changes.
		//
		// Another situation that can present a problem is cluster configurations
		// where a significant number of nodes can only have follower replicas. Some options:
		// - Add a cluster setting effective_fraction_allocators, that defaults to
		//   1.0 that can be adjusted by the operator to the fraction of nodes
		//   that can do allocation.
		// - Estimate the number of allocators based on the gossiped lease count
		//   by each store.
		//
		// We currently prefer the second option.
		h.allowChanges = true
		return
	}
	h.allowChanges = true
	for i := range h.totalDelta {
		projectedDelta := h.totalDelta[i] * loadValue(crl.numAllocators)
		if float64(projectedDelta) > 0.5*float64(crl.clusterMean.load[i]) {
			h.allowChanges = false
			return
		}
	}
	for i := range h.totalSecondary {
		projectedDelta := h.totalSecondary[i] * loadValue(crl.numAllocators)
		if float64(projectedDelta) > 0.5*float64(crl.clusterMean.secondaryLoad[i]) {
			h.allowChanges = false
			return
		}
	}
}

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

		enactedHistory storeEnactedHistory

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

// The time duration between a change happening at a store, and when the
// effect of that change is seen in the load information computed by that
// store.
//
// TODO(sumeer): set this based on an understanding of ReplicaStats etc.
const lagForChangeReflectedInLoad = 5 * time.Second

// NB: this is a heuristic.
//
// TODO(sumeer): this interface is just a placeholder. We should integrate
// this into the storeState update function, in which case we can directly
// remove from the loadPendingChanges map.
func (ss *storeState) computePendingChangesReflectedInLatestLoad(
	latestLoadTime time.Time,
) []*pendingReplicaChange {
	var changes []*pendingReplicaChange
	for _, change := range ss.adjusted.loadPendingChanges {
		if change.enactedAtTime.IsZero() {
			// Not yet enacted, based on the information provided by the
			// leaseholder, which is always considered most up-to-date (because of
			// AdjustPendingChangesDisposition)
			continue
		}
		// Is enacted.
		if latestLoadTime.Sub(change.enactedAtTime) > lagForChangeReflectedInLoad {
			changes = append(changes, change)
		}
	}
	return changes
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
	// Added to when a change is proposed. Will also add to corresponding
	// rangeState.pendingChanges and to the affected storeStates.
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

	// Handle the leaseholder store messages first. These have do not affect load
	// delta adjustments but will mark pending changes as enacted.
	for _, leaseMsg := range resp.leaseholderStores {
		for _, rangeMsg := range leaseMsg.ranges {
			rs, ok := cs.ranges[rangeMsg.RangeID]
			if !ok {
				rs = newRangeState()
				cs.ranges[rangeMsg.RangeID] = rs
			}

			// Unilaterally remove the existing range state replicas. Then add back
			// the replicas reported by the range message.
			//
			// TODO(kvoli,sumeerbhola): Use a set difference with rangeMsg.replicas
			// and rs.replicas instead.
			for _, replica := range rs.replicas {
				delete(cs.stores[replica.StoreID].adjusted.replicas, rangeMsg.RangeID)
			}
			// Set the range state replicas to be equal to the range leaseholder
			// replicas. The pending changes which aren't enacted in the range
			// message are hanled and added back below.
			rs.replicas = rangeMsg.replicas
			for _, replica := range rangeMsg.replicas {
				cs.stores[replica.StoreID].adjusted.replicas[rangeMsg.RangeID] = replica.replicaState
			}

			// Find any pending changes which are now enacted, according to the
			// leaseholder's authoritative view.
			var remainingChanges, enactedChanges []*pendingReplicaChange
			for _, change := range rs.pendingChanges {
				ss := cs.stores[change.storeID]
				replIdAndType, ok := ss.adjusted.replicas[rangeMsg.RangeID]
				if !ok {
					replIdAndType.ReplicaID = noReplicaID
				}
				// The change has been enacted according to the leaseholder.
				if replIdAndType.subsumesChange(change.prev.replicaIDAndType, change.next) {
					enactedChanges = append(enactedChanges, change)
				} else {
					remainingChanges = append(remainingChanges, change)
				}
			}

			for _, change := range enactedChanges {
				// Mark the change as enacted. Enacting a change does not remove the
				// corresponding load adjustments. The store load message will do that,
				// or GC, indiciating that the change has been reflected in the store
				// load.
				cs.markPendingChangeEnacted(change.changeID, now)
			}
			// Re-apply the remaining changes. These do not update the load state,
			// only the authoritative replica tracking in store state and range
			// state.
			for _, change := range remainingChanges {
				cs.applyReplicaChange(change.replicaChange, changeEffectTypeReplicas)
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
		// The store's load seqeunce number is incremented on each load change. The
		// store's load is updated below.
		ss.loadSeqNum++
		newLoadReplicas := map[roachpb.RangeID]replicaType{}
		oldLoadReplicas := ss.adjusted.loadReplicas
		for _, r := range msg.storeRanges {
			// Update the last heard from time for the range, creating a new range state
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
			_, inOld := oldLoadReplicas[change.rangeID]
			var replIdAndType replicaIDAndType
			if inNew {
				replIdAndType.replicaType = newRepl
				// We don't know the actual replica ID in the new state, only that it
				// exists. Use the placeholder unknownReplicaID, so that
				// subsumesChange(...) recognizes that a replica exists.
				replIdAndType.ReplicaID = unknownReplicaID
			} else {
				replIdAndType.ReplicaID = noReplicaID
			}
			// Consider the change included in the store's load when (a) the change
			// is subsumed by the replica state or (b) the change was enacted more
			// than enactedPendingChangeGCDuration duration ago.
			considerIncluded := replIdAndType.subsumesChange(change.prev.replicaIDAndType, change.next) ||
				(change.enactedAtTime != time.Time{} &&
					now.Sub(change.enactedAtTime) > enactedPendingChangeGCDuration)
			if considerIncluded {
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
	}
	// Mark the store as fully initialized any time it is set.
	ss.storeInitState = fullyInit
	ss.storeLoad.StoreDescriptor = store
	ss.localityTiers = cs.localityTierInterner.intern(store.Locality())
	cs.constraintMatcher.setStore(store)
	// NB: If the locality has changed, it implies any other stores on the same
	// node also have had their locality changed. We don't proactively update the
	// locality of other stores here as we expect setStore to be called for each
	// store on the same node quickly. Any inconsistenty should be short lived
	// and transient.
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
}

// Called by enacting module.
func (cs *clusterState) pendingChangesRejected(changes []changeID) {
	// Wipe rejected changes, including load adjustments, tracking and replica
	// changes.
	//
	// NB: This doesn't handle rejecting changes which have been reported as
	// complete by the store load, but not reported by the leaseholder. These
	// will be GC'd eventually regardless.
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
	cs.applyReplicaChange(change, changeEffectTypeAll)
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

func (cs *clusterState) applyReplicaChange(change replicaChange, effectType changeEffectType) {
	storeState := cs.stores[change.storeID]
	rangeState := cs.ranges[change.rangeID]
	if change.isRemoval() {
		// The change is to remove a replica.
		if effectType == changeEffectTypeReplicas || effectType == changeEffectTypeAll {
			delete(storeState.adjusted.replicas, change.rangeID)
		}
		if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
			delete(storeState.adjusted.loadReplicas, change.rangeID)
		}
		rangeState.removeReplica(change.storeID)
	} else if change.isAddition() {
		// The change is to add a new replica.
		pendingRepl := storeIDAndReplicaState{
			StoreID: change.storeID,
			replicaState: replicaState{
				replicaIDAndType: change.next,
			},
		}
		if effectType == changeEffectTypeReplicas || effectType == changeEffectTypeAll {
			storeState.adjusted.replicas[change.rangeID] = pendingRepl.replicaState
			rangeState.setReplica(pendingRepl)
		}
		if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
			storeState.adjusted.loadReplicas[change.rangeID] = pendingRepl.replicaType
		}
	} else if change.isUpdate() {
		// The change is to update an existing replica.
		replState := storeState.adjusted.replicas[change.rangeID]
		replState.replicaIDAndType = change.next
		if effectType == changeEffectTypeReplicas || effectType == changeEffectTypeAll {
			storeState.adjusted.replicas[change.rangeID] = replState
			rangeState.setReplica(storeIDAndReplicaState{
				StoreID:      change.storeID,
				replicaState: replState,
			})
		}
		if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
			storeState.adjusted.loadReplicas[change.rangeID] = replState.replicaType
		}
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}

	if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
		cs.applyChangeLoadDelta(change)
	}
}

func (cs *clusterState) markPendingChangeEnacted(cid changeID, enactedAt time.Time) {
	change := cs.pendingChanges[cid]
	change.enactedAtTime = enactedAt
	cs.removePendingChangeTracking(change.changeID)
}

// There are two categories of pending change data-structures:
//
// - load adjusting: storeState.adjusted.loadPendingChanges
// - authoritative: clusterState.pendingChanges, rangeState.pendingChanges
//
// Load adjusting pending changes are removed when one of these conditions is
// true: (1) a store reports the change as complete via its store load; or (2)
// the pending change was reported as enacted by the leaseholder and more than
// enactedPendingChangeGCDuration duration has elapsed; or (3) the pending
// change was started more than pendingChangeGCDuration duration ago.
//
// Load adjusting pending changes apply a load impact on the store/node when
// not removed (storeState.adjusted.load, storeState.adjusted.secondaryLoad,
// nodeState.adjustedCPU) and additionally affect the replicas in
// storeState.adjusted.loadReplicas.
//
// Authoritative pending changes are removed when one of these conditions is
// true: (1) a leaseholder reports the change as enacted; or (2) the pending
// change was started more than pendingChangeGCDuration ago.
//
// Authoritative pending changes do not apply a load impact on the associated
// store/node. Authoritative pending changes update rangeState.replicas and
// storeState.adjusted.replicas to be the state of the range if the change were
// applied.

// changeEffectType is provided when applying or undoing a replica change. The
// effect type describes what parts of the clusterState should be adjusted.
type changeEffectType uint8

const (
	// changeEffectTypeReplicas affects the authoritative replicas, tracked in
	// rangeState.replicas and storeState.adjusted.replicas.
	changeEffectTypeReplicas = iota
	// changeEffectTypeLoad affects the adjusted load tracked in
	// storeState.adjusted.load and nodeState.adjustedCPU. The adjusted load
	// replicas (storeState.adjusted.loadReplicas) is also affected.
	changeEffectTypeLoad
	// changeEffectTypeAll is the union of the two above changeEffectTypes.
	changeEffectTypeAll
)

// undoPendingChange reverses the change with ID cid, if it exists.
func (cs *clusterState) undoPendingChange(cid changeID) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		// The pending change doesn't exist.
		return
	}
	// Undo the change effects, including changes to the range replicas and load.
	cs.undoReplicaChange(change.replicaChange, changeEffectTypeAll)
	// Remove the change from tracking all tracking.
	cs.removePendingChangeTracking(cid)
	delete(cs.stores[change.storeID].adjusted.loadPendingChanges, change.changeID)
}

func (cs *clusterState) undoReplicaChange(change replicaChange, effectType changeEffectType) {
	rangeState := cs.ranges[change.rangeID]
	storeState := cs.stores[change.storeID]
	if change.isRemoval() {
		// The change was to remove a replica. Add the replica back using the prev
		// state stored in the replica change.
		prevRepl := storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: change.prev,
		}

		if effectType == changeEffectTypeReplicas || effectType == changeEffectTypeAll {
			storeState.adjusted.loadReplicas[change.rangeID] = prevRepl.replicaType
		}
		if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
			rangeState.setReplica(prevRepl)
			storeState.adjusted.replicas[change.rangeID] = prevRepl.replicaState
		}
	} else if change.isAddition() {
		// The change was to add a new replica.
		if effectType == changeEffectTypeReplicas || effectType == changeEffectTypeAll {
			delete(storeState.adjusted.replicas, change.rangeID)
			rangeState.removeReplica(change.storeID)
		}
		if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
			delete(storeState.adjusted.loadReplicas, change.rangeID)
		}
	} else if change.isUpdate() {
		// The change was to update an existing replica.
		replState := change.prev
		if effectType == changeEffectTypeReplicas || effectType == changeEffectTypeAll {
			rangeState.setReplica(storeIDAndReplicaState{
				StoreID:      change.storeID,
				replicaState: replState,
			})
			storeState.adjusted.replicas[change.rangeID] = replState
		}
		if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
			storeState.adjusted.loadReplicas[change.rangeID] = replState.replicaType
		}
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}

	if effectType == changeEffectTypeLoad || effectType == changeEffectTypeAll {
		// Undo the load adjustments that were applied for this change.
		cs.undoChangeLoadDelta(change)
	}
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

// removePendingChangeTracking removes the given change from the cluster and
// range pending change tracking. If the change doesn't exist, this is a no-op.
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
var _ = (&storeChangeRateLimiter{}).initForRebalancePass
var _ = (&storeChangeRateLimiter{}).updateForRebalancePass
var _ = newStoreChangeRateLimiter
var _ = (&storeEnactedHistory{}).addEnactedChange
var _ = (&storeEnactedHistory{}).allowLoadBasedChanges
var _ = (&storeEnactedHistory{}).gcHistory
var _ = enactedReplicaChange{}
var _ = (&storeState{}).computePendingChangesReflectedInLatestLoad
