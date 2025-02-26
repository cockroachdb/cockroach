// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	w.Printf(" type=%v", rt.replicaType.replicaType)
	if rt.isLeaseholder {
		w.Print(" leaseholder=true")
	}
}

func (rt replicaIDAndType) String() string {
	return redact.StringWithoutMarkers(rt)
}

// subsumesChange returns true if rit subsumes prev and next. prev is the state
// before the proposed change and next is the state after the proposed change.
// rit is the current observed state.
func (rit replicaIDAndType) subsumesChange(prev, next replicaIDAndType) bool {
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
	// TODO(kvoli,sumeer): in some cases we normalize the types, but we don't
	// document it clearly. And documentation is insufficient -- we should use a
	// different type, and centralize the normalization code. Examples: this
	// normalization; the pendingReplicaChange.{prev,next} fields;
	// analyzedConstraintsBuf.tryAddingStore. It may not be possible to normalize
	// in the same way, as here we are concerned with load, unlike in
	// analyzedConstraintsBuf.tryAddingStore.
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
	// TODO(kvoli,sumeerbhola): Add in rac2.SendQueue information to prevent
	// lease transfers to replicas which are not able to take the lease due to a
	// send queue.
}

// changeID is a unique ID, in the context of this data-structure and when
// receiving updates about enactment having happened or having been rejected
// (by the component responsible for change enactment).
type changeID uint64

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
	rangeID roachpb.RangeID,
	existingReplicas []storeIDAndReplicaState,
	rLoad rangeLoad,
	addStoreID, removeStoreID roachpb.StoreID,
) [2]replicaChange {
	addIdx, removeIdx := -1, -1
	for i, replica := range existingReplicas {
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
	remove := existingReplicas[removeIdx]
	add := existingReplicas[addIdx]

	removeLease := replicaChange{
		storeID: removeStoreID,
		rangeID: rangeID,
		prev:    remove.replicaState,
		next:    remove.replicaIDAndType,
	}
	addLease := replicaChange{
		storeID: addStoreID,
		rangeID: rangeID,
		prev:    add.replicaState,
		next:    add.replicaIDAndType,
	}
	removeLease.next.isLeaseholder = false
	addLease.next.isLeaseholder = true
	removeLease.secondaryLoadDelta[leaseCount] = -1
	addLease.secondaryLoadDelta[leaseCount] = 1

	// Only account for the leaseholder CPU, all other primary load dimensions
	// are ignored. Byte size and write bytes are not impacted by having a range
	// lease.
	nonRaftCPU := rLoad.load[cpu] - rLoad.raftCPU
	removeLease.loadDelta[cpu] = -nonRaftCPU
	addLease.loadDelta[cpu] = nonRaftCPU
	return [2]replicaChange{removeLease, addLease}
}

// makeAddReplicaChange creates a replica change which adds the replica type
// to the store addStoreID. The load impact of adding the replica does not
// account for whether the replica is becoming the leaseholder or not.
//
// TODO(kvoli,sumeerbhola): Add promotion/demotion changes.
func makeAddReplicaChange(
	rangeID roachpb.RangeID, rLoad rangeLoad, addStoreID roachpb.StoreID, rType roachpb.ReplicaType,
) replicaChange {
	addReplica := replicaChange{
		storeID: addStoreID,
		rangeID: rangeID,
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
func makeRemoveReplicaChange(
	rangeID roachpb.RangeID, rLoad rangeLoad, remove storeIDAndReplicaState,
) replicaChange {
	removeReplica := replicaChange{
		storeID: remove.StoreID,
		rangeID: rangeID,
		prev:    remove.replicaState,
		next: replicaIDAndType{
			ReplicaID: noReplicaID,
		},
	}
	removeReplica.loadDelta.subtract(rLoad.load)
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
	rangeID roachpb.RangeID,
	existingReplicas []storeIDAndReplicaState,
	rLoad rangeLoad,
	addStoreID, removeStoreID roachpb.StoreID,
) [2]replicaChange {
	var remove storeIDAndReplicaState
	for _, replica := range existingReplicas {
		if replica.StoreID == removeStoreID {
			remove = replica
		}
	}

	addReplicaChange := makeAddReplicaChange(rangeID, rLoad, addStoreID, remove.replicaType.replicaType)
	removeReplicaChange := makeRemoveReplicaChange(rangeID, rLoad, remove)
	if remove.isLeaseholder {
		// The existing leaseholder is being removed. The incoming replica will
		// take the lease load, in addition to the replica load.
		addReplicaChange.next.isLeaseholder = true
		addReplicaChange.loadDelta = loadVector{}
		removeReplicaChange.loadDelta = loadVector{}
		addReplicaChange.loadDelta.add(rLoad.load)
		removeReplicaChange.loadDelta.subtract(rLoad.load)
		addReplicaChange.secondaryLoadDelta[leaseCount] = 1
		removeReplicaChange.secondaryLoadDelta[leaseCount] = -1
	}

	return [2]replicaChange{addReplicaChange, removeReplicaChange}
}

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

	// TODO(kvoli,sumeerbhola): Consider adopting an explicit expiration time,
	// after which the change is considered to have been rejected. This would
	// allow a different expiration time for different types of changes, e.g.,
	// lease transfers would have a smaller expiration time than replica
	// additions.

	// When the change is known to be enacted based on the authoritative
	// information received from the leaseholder, this value is set, so that even
	// if the store with a replica affected by this pending change does not tell
	// us about the enactment, we can garbage collect this change.
	enactedAtTime time.Time
}

// TODO(kvoli): This will eventually be used to represent the state of a node's
// membership. This corresponds to non-decommissioning, decommissioning and
// decommissioned. Fill in commentary and use.
type storeMembership int8

const (
	storeMembershipMember storeMembership = iota
	storeMembershipRemoving
	storeMembershipRemoved
)

func (s storeMembership) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s storeMembership) SafeFormat(w redact.SafePrinter, _ rune) {
	switch s {
	case storeMembershipMember:
		w.Print("full")
	case storeMembershipRemoving:
		w.Print("removing")
	case storeMembershipRemoved:
		w.Print("removed")
	}
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
// TODO(sumeer,kvoli): integrate this into the rest of mma.

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
	slices.SortFunc(h.changes, func(a, b enactedReplicaChange) int {
		return a.enactedAtTime.Compare(b.enactedAtTime)
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
			h.totalDelta.subtract(c.loadDelta)
			h.totalSecondary.subtract(c.secondaryDelta)
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
	storeMembership
	storeLoad
	adjusted struct {
		load          loadVector
		secondaryLoad secondaryLoadVector
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
		//   reported load. Once computePendingChangesReflectedInLatestLoad
		//   determines that the latest load state must include the pending
		//   change, it will be removed.
		//
		// In summary, guaranteed removal of a load pending change because of
		// failure of enactment or GC happens via clusterState.pendingChanges.
		// Only the case where enactment happened is where a load pending change
		// can live on -- but since that will set enactedAtTime, we are guaranteed
		// to eventually remove it.
		loadPendingChanges map[changeID]*pendingReplicaChange
		// replicas is computed from the authoritative information provided by
		// various leaseholders in storeLeaseholderMsgs and adjusted for pending
		// changes in clusterState.pendingChanges/rangeState.pendingChanges.
		replicas map[roachpb.RangeID]replicaState
		// topKRanges along some load dimensions. If the store is closer to
		// hitting the resource limit on some resource ranges that are higher in
		// that resource dimension should be over-represented in this map. It
		// includes ranges whose replicas are being removed via pending changes,
		// since those pending changes may be reversed, and we don't want to
		// bother recomputing the top-k.
		//
		// We need to keep this top-k up-to-date incrementally. Since
		// storeLeaseholderMsg is incremental about the ranges it reports, that
		// may provide a building block for the incremental computation.
		//
		// TODO(sumeer): figure out at least one reasonable way to do this, even
		// if we postpone it to a later code iteration.
		topKRanges map[roachpb.RangeID]rangeLoad
		// TODO(kvoli,sumeerbhola): Update enactedHistory when integrating the
		// storeChangeRateLimiter.
		enactedHistory storeEnactedHistory
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
// NOTE: The gossip interval is 10s  (see gossip.go StoresInterval). On a
// signficant load change (% delta), the store will gossip more frequently (see
// kvserver/store_gossip.go).
//
// TODO(sumeer): set this based on an understanding of ReplicaStats, Gossip
// etc.
const lagForChangeReflectedInLoad = 10 * time.Second

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
			StoreID: storeID,
			NodeID:  nodeID,
		},
	}
	ss.adjusted.loadPendingChanges = map[changeID]*pendingReplicaChange{}
	ss.adjusted.replicas = map[roachpb.RangeID]replicaState{}
	ss.adjusted.topKRanges = map[roachpb.RangeID]rangeLoad{}
	return ss
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

func (fds failureDetectionSummary) String() string {
	return redact.StringWithoutMarkers(fds)
}

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

// rangeState is periodically updated based on reporting by the leaseholder.
type rangeState struct {
	rangeID roachpb.RangeID
	// replicas is the adjusted replicas. It is always consistent with
	// the storeState.adjusted.replicas in the corresponding stores.
	replicas []storeIDAndReplicaState
	conf     *normalizedSpanConfig

	load rangeLoad

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
}

func newRangeState() *rangeState {
	return &rangeState{
		replicas:       []storeIDAndReplicaState{},
		pendingChanges: []*pendingReplicaChange{},
	}
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
	ts     timeutil.TimeSource
	nodes  map[roachpb.NodeID]*nodeState
	stores map[roachpb.StoreID]*storeState
	ranges map[roachpb.RangeID]*rangeState

	// Added to when a change is proposed. Will also add to corresponding
	// rangeState.pendingChanges and to the affected storeStates.
	//
	// Removed from based on rangeMsg, explicit rejection by enacting module, or
	// time-based GC. There is no explicit acceptance by enacting module since
	// the single source of truth of a rangeState is the leaseholder.
	pendingChanges map[changeID]*pendingReplicaChange
	changeSeqGen   changeID

	*constraintMatcher
	*localityTierInterner
}

func newClusterState(ts timeutil.TimeSource, interner *stringInterner) *clusterState {
	return &clusterState{
		ts:                   ts,
		nodes:                map[roachpb.NodeID]*nodeState{},
		stores:               map[roachpb.StoreID]*storeState{},
		ranges:               map[roachpb.RangeID]*rangeState{},
		pendingChanges:       map[changeID]*pendingReplicaChange{},
		constraintMatcher:    newConstraintMatcher(interner),
		localityTierInterner: newLocalityTierInterner(interner),
	}
}

func (cs *clusterState) processNodeLoadMsg(msg *nodeLoadMsg) {
	now := cs.ts.Now()
	cs.gcPendingChanges(now)

	ns := cs.nodes[msg.nodeID]
	// Handle the node load, updating the reported load and set the adjusted load
	// to be equal to the reported load initially. Any remaining pending changes
	// will be re-applied to the reported load.
	ns.reportedCPU = msg.reportedCPU
	ns.adjustedCPU = msg.reportedCPU
	ns.nodeLoad = msg.nodeLoad
	for _, storeMsg := range msg.stores {
		ss := cs.stores[storeMsg.StoreID]

		// The store's load seqeunce number is incremented on each load change. The
		// store's load is updated below.
		ss.loadSeqNum++
		ss.storeLoad.reportedLoad = storeMsg.load
		ss.storeLoad.capacity = storeMsg.capacity
		ss.storeLoad.reportedSecondaryLoad = storeMsg.secondaryLoad

		// Reset the adjusted load to be the reported load. We will re-apply any
		// remaining pending change deltas to the updated adjusted load.
		ss.adjusted.load = storeMsg.load
		ss.adjusted.secondaryLoad = storeMsg.secondaryLoad

		// Find any pending changes for range's which involve this store. These
		// pending changes can now be removed from the loadPendingChanges. We don't
		// need to undo the corresponding delta adjustment as the reported load
		// already contains the effect.
		for _, change := range ss.computePendingChangesReflectedInLatestLoad(msg.loadTime) {
			delete(ss.adjusted.loadPendingChanges, change.changeID)
			delete(cs.pendingChanges, change.changeID)
		}

		for _, change := range ss.adjusted.loadPendingChanges {
			// The pending change hasn't been reported as done, re-apply the load
			// delta to the adjusted load and include it in the new adjusted load
			// replicas.
			cs.applyChangeLoadDelta(change.replicaChange)
		}
	}
}

func (cs *clusterState) processStoreLeaseholderMsg(msg *storeLeaseholderMsg) {
	now := cs.ts.Now()
	cs.gcPendingChanges(now)

	for _, rangeMsg := range msg.ranges {
		rs, ok := cs.ranges[rangeMsg.RangeID]
		if !ok {
			// This is the first time we've seen this range.
			rs = newRangeState()
			cs.ranges[rangeMsg.RangeID] = rs
		}
		// Set the range state and store state to match the range message state
		// initially. The pending changes which are not enacted in the range
		// message are handled and added back below.
		rs.load = rangeMsg.rangeLoad
		rs.replicas = rangeMsg.replicas
		for _, replica := range rs.replicas {
			delete(cs.stores[replica.StoreID].adjusted.replicas, rangeMsg.RangeID)
		}
		for _, replica := range rangeMsg.replicas {
			cs.stores[replica.StoreID].adjusted.replicas[rangeMsg.RangeID] = replica.replicaState
		}

		// Find any pending changes which are now enacted, according to the
		// leaseholder.
		var remainingChanges, enactedChanges []*pendingReplicaChange
		for _, change := range rs.pendingChanges {
			ss := cs.stores[change.storeID]
			adjustedReplicas, ok := ss.adjusted.replicas[rangeMsg.RangeID]
			if !ok {
				adjustedReplicas.ReplicaID = noReplicaID
			}
			if adjustedReplicas.subsumesChange(change.prev.replicaIDAndType, change.next) {
				// The change has been enacted according to the leaseholder.
				enactedChanges = append(enactedChanges, change)
			} else {
				remainingChanges = append(remainingChanges, change)
			}
		}

		for _, change := range enactedChanges {
			// Mark the change as enacted. Enacting a change does not remove the
			// corresponding load adjustments. The store load message will do that,
			// or GC, indiciating that the change is been reflected in the store
			// load.
			cs.markPendingChangeEnacted(change.changeID, now)
		}
		// Re-apply the remaining changes.
		for _, change := range remainingChanges {
			cs.applyReplicaChange(change.replicaChange)
		}
		normSpanConfig, err := makeNormalizedSpanConfig(&rangeMsg.conf, cs.constraintMatcher.interner)
		if err != nil {
			// TODO(kvoli): Should we log as a warning here, or return further back out?
			panic(err)
		}
		rs.conf = normSpanConfig
	}
}

// If the pending change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingChangeGCDuration = 5 * time.Minute

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges(now time.Time) {
	gcBeforeTime := now.Add(-pendingChangeGCDuration)
	var removeChangeIds []changeID
	for _, pendingChange := range cs.pendingChanges {
		if !pendingChange.startTime.After(gcBeforeTime) {
			removeChangeIds = append(removeChangeIds, pendingChange.changeID)
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
	for _, changeID := range changes {
		cs.undoPendingChange(changeID)
	}
}

func (cs *clusterState) markPendingChangeEnacted(cid changeID, enactedAt time.Time) {
	change := cs.pendingChanges[cid]
	change.enactedAtTime = enactedAt
}

// undoPendingChange reverses the change with ID cid.
func (cs *clusterState) undoPendingChange(cid changeID) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		panic(fmt.Sprintf("change %v not found", cid))
	}
	rs, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}
	// Wipe the analyzed constraints, as the range has changed.
	rs.constraints = nil

	// Undo the change delta as well as the replica change.
	cs.undoReplicaChange(change.replicaChange)
	i := 0
	n := len(rs.pendingChanges)
	for ; i < n; i++ {
		if rs.pendingChanges[i].changeID == cid {
			rs.pendingChanges[i], rs.pendingChanges[n-1] = rs.pendingChanges[n-1], rs.pendingChanges[i]
			rs.pendingChanges = rs.pendingChanges[:n-1]
			break
		}
	}
	delete(cs.pendingChanges, change.changeID)
	delete(cs.stores[change.storeID].adjusted.loadPendingChanges, change.changeID)
}

// createPendingChanges takes a set of changes for a range and applies the
// changes as pending. The application updates the adjusted load, tracked
// pending changes and changeID to reflect the pending application.
func (cs *clusterState) createPendingChanges(
	rangeID roachpb.RangeID, changes ...replicaChange,
) []*pendingReplicaChange {
	var pendingChanges []*pendingReplicaChange
	now := cs.ts.Now()

	for _, change := range changes {
		cs.applyReplicaChange(change)
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
		storeState.adjusted.loadPendingChanges[cid] = pendingChange
		rangeState.pendingChanges = append(rangeState.pendingChanges, pendingChange)
		pendingChanges = append(pendingChanges, pendingChange)
	}
	return pendingChanges
}

func (cs *clusterState) applyReplicaChange(change replicaChange) {
	storeState, ok := cs.stores[change.storeID]
	if !ok {
		panic(fmt.Sprintf("store %v not found in cluster state", change.storeID))
	}
	rangeState, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}

	if change.isRemoval() {
		delete(storeState.adjusted.replicas, change.rangeID)
		rangeState.removeReplica(change.storeID)
	} else if change.isAddition() {
		pendingRepl := storeIDAndReplicaState{
			StoreID: change.storeID,
			replicaState: replicaState{
				replicaIDAndType: change.next,
			},
		}
		storeState.adjusted.replicas[change.rangeID] = pendingRepl.replicaState
		rangeState.setReplica(pendingRepl)
	} else if change.isUpdate() {
		replState := storeState.adjusted.replicas[change.rangeID]
		replState.replicaIDAndType = change.next
		storeState.adjusted.replicas[change.rangeID] = replState
		rangeState.setReplica(storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: replState,
		})
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}
	cs.applyChangeLoadDelta(change)
}

func (cs *clusterState) undoReplicaChange(change replicaChange) {
	rangeState := cs.ranges[change.rangeID]
	storeState := cs.stores[change.storeID]
	if change.isRemoval() {
		prevRepl := storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: change.prev,
		}
		rangeState.setReplica(prevRepl)
		storeState.adjusted.replicas[change.rangeID] = prevRepl.replicaState
	} else if change.isAddition() {
		delete(storeState.adjusted.replicas, change.rangeID)
		rangeState.removeReplica(change.storeID)
	} else if change.isUpdate() {
		replState := change.prev
		rangeState.setReplica(storeIDAndReplicaState{
			StoreID:      change.storeID,
			replicaState: replState,
		})
		storeState.adjusted.replicas[change.rangeID] = replState
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}
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
	ss.adjusted.load.add(change.loadDelta)
	ss.adjusted.secondaryLoad.add(change.secondaryLoadDelta)
	ss.loadSeqNum++

	cs.nodes[ss.NodeID].adjustedCPU += change.loadDelta[cpu]
}

// undoChangeLoadDelta subtracts the change load delta from the adjusted load
// of the store and node affected.
func (cs *clusterState) undoChangeLoadDelta(change replicaChange) {
	ss := cs.stores[change.storeID]
	ss.adjusted.load.subtract(change.loadDelta)
	ss.adjusted.secondaryLoad.subtract(change.secondaryLoadDelta)
	ss.loadSeqNum++

	cs.nodes[ss.NodeID].adjustedCPU -= change.loadDelta[cpu]
}

func (cs *clusterState) setStore(desc roachpb.StoreDescriptor) {
	ns, ok := cs.nodes[desc.Node.NodeID]
	if !ok {
		// This is the first time seeing the associated node.
		ns = newNodeState(desc.Node.NodeID)
		cs.nodes[desc.Node.NodeID] = ns
	}
	ns.stores = append(ns.stores, desc.StoreID)
	ss, ok := cs.stores[desc.StoreID]
	if !ok {
		// This is the first time seeing this store.
		ss = newStoreState(desc.StoreID, desc.Node.NodeID)
		ss.localityTiers = cs.localityTierInterner.intern(desc.Locality())
		cs.constraintMatcher.setStore(desc)
		cs.stores[desc.StoreID] = ss
	}
	ss.StoreDescriptor = desc
}

func (cs *clusterState) setStoreMembership(storeID roachpb.StoreID, state storeMembership) {
	if ss, ok := cs.stores[storeID]; ok {
		ss.storeMembership = state
	} else {
		panic(fmt.Sprintf("store %d not found in cluster state", storeID))
	}
}

func (cs *clusterState) updateFailureDetectionSummary(
	nodeID roachpb.NodeID, fd failureDetectionSummary,
) {
	ns := cs.nodes[nodeID]
	ns.fdSummary = fd
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
	var highDiskSpaceUtil bool
	for i := range msl.load {
		// TODO(kvoli,sumeerbhola): Handle negative adjusted store/node loads.
		ls := loadSummaryForDimension(ss.adjusted.load[i], ss.capacity[i], msl.load[i], msl.util[i])
		if ls < sls {
			sls = ls
		}
		if loadDimension(i) == byteSize {
			highDiskSpaceUtil = highDiskSpaceUtilization(ss.adjusted.load[i], ss.capacity[i])
		}
	}
	nls := loadSummaryForDimension(ns.adjustedCPU, ns.capacityCPU, mnl.loadCPU, mnl.utilCPU)
	return storeLoadSummary{
		sls:                      sls,
		nls:                      nls,
		highDiskSpaceUtilization: highDiskSpaceUtil,
		fd:                       ns.fdSummary,
		maxFractionPending:       ss.maxFractionPending,
		loadSeqNum:               ss.loadSeqNum,
	}
}

// Avoid unused lint errors.
var _ = replicaState{}.voterIsLagging
var _ = storeState{}.maxFractionPending
var _ = nodeState{}.loadSummary
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
var _ = rangeState{}.rangeID
var _ = enactedReplicaChange{}
var _ = storeEnactedHistory{}.changes
var _ = storeEnactedHistory{}.totalDelta
var _ = storeEnactedHistory{}.totalSecondary
var _ = storeEnactedHistory{}.allowChanges
var _ = (*storeEnactedHistory).addEnactedChange
var _ = (*storeEnactedHistory).allowLoadBasedChanges
var _ = (*storeEnactedHistory).gcHistory
var _ = storeChangeRateLimiter{}.gcThreshold
var _ = storeChangeRateLimiter{}.numAllocators
var _ = storeChangeRateLimiter{}.clusterMean
var _ = newStoreChangeRateLimiter
var _ = (*storeChangeRateLimiter).initForRebalancePass
var _ = (*storeChangeRateLimiter).updateForRebalancePass
var _ = (*clusterState).canAddLoad
