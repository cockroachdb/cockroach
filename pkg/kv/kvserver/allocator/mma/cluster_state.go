// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"slices"
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
	// information received from the leaseholder, this value is set, so that we
	// can garbage collect this change.
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
	storeInitState
	storeLoad
	adjusted struct {
		load loadVector
		// loadReplicas is computed from the authoritative information provided by
		// various leaseholders in storeLeaseholderMsgs, and adjusted for
		// loadPendingChanges.
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

		enactedHistory storeEnactedHistory

		secondaryLoad secondaryLoadVector

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
		// TODO(sumeer): figure out at least one reasonable way to do this, even
		// if we postpone it to a later code iteration.
		topKRanges map[roachpb.RangeID]rangeLoad
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

// rangeState is periodically updated based on reporting by the leaseholder.
type rangeState struct {
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
	*localityTierInterner
}

func newClusterState(interner *stringInterner) *clusterState {
	return &clusterState{
		nodes:                map[roachpb.NodeID]*nodeState{},
		stores:               map[roachpb.StoreID]*storeState{},
		ranges:               map[roachpb.RangeID]*rangeState{},
		pendingChanges:       map[changeID]*pendingReplicaChange{},
		constraintMatcher:    newConstraintMatcher(interner),
		localityTierInterner: newLocalityTierInterner(interner),
	}
}

//======================================================================
// clusterState mutators
//======================================================================

func (cs *clusterState) processNodeLoadMsg(msg *nodeLoadMsg) {
	// TODO(sumeer):
}

func (cs *clusterState) processStoreLeaseholderMsg(msg *storeLeaseholderMsg) {}

func (cs *clusterState) addNodeID(nodeID roachpb.NodeID) {
	// TODO(sumeer):
}

func (cs *clusterState) addStore(store roachpb.StoreDescriptor) {
	// TODO(sumeer):
}

func (cs *clusterState) changeStore(store roachpb.StoreDescriptor) {
	// TODO(sumeer):
}

func (cs *clusterState) removeNodeAndStores(nodeID roachpb.NodeID) {
	// TODO(sumeer):
}

// If the pending change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingChangeGCDuration = 5 * time.Minute

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges() {
	// TODO(sumeer):
}

// Called by enacting module.
func (cs *clusterState) pendingChangesRejected(
	rangeID roachpb.RangeID, changes []pendingReplicaChange,
) {
	// TODO(sumeer):
}

func (cs *clusterState) addPendingChanges(rangeID roachpb.RangeID, changes []pendingReplicaChange) {
	// TODO(sumeer):
}

func (cs *clusterState) updateFailureDetectionSummary(
	nodeID roachpb.NodeID, fd failureDetectionSummary,
) {
	// TODO(sumeer):
}

//======================================================================
// clusterState accessors:
//
// Not all accesses need to use these accessors.
//======================================================================

// For meansMemo.
var _ loadInfoProvider = &clusterState{}

func (cs *clusterState) getStoreReportedLoad(roachpb.StoreID) *storeLoad {
	// TODO(sumeer):
	return nil
}

func (cs *clusterState) getNodeReportedLoad(roachpb.NodeID) *nodeLoad {
	// TODO(sumeer):
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
var _ = (&clusterState{}).processNodeLoadMsg
var _ = (&clusterState{}).processStoreLeaseholderMsg
var _ = (&clusterState{}).addNodeID
var _ = (&clusterState{}).addStore
var _ = (&clusterState{}).changeStore
var _ = (&clusterState{}).removeNodeAndStores
var _ = (&clusterState{}).gcPendingChanges
var _ = (&clusterState{}).pendingChangesRejected
var _ = (&clusterState{}).addPendingChanges
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
var _ = rangeState{}.load
var _ = rangeState{}.pendingChanges
var _ = rangeState{}.constraints
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
var _ = clusterState{}.nodes
var _ = clusterState{}.stores
var _ = clusterState{}.ranges
var _ = clusterState{}.pendingChanges
var _ = clusterState{}.constraintMatcher
var _ = clusterState{}.localityTierInterner
var _ = (&storeChangeRateLimiter{}).initForRebalancePass
var _ = (&storeChangeRateLimiter{}).updateForRebalancePass
var _ = newStoreChangeRateLimiter
var _ = (&storeEnactedHistory{}).addEnactedChange
var _ = (&storeEnactedHistory{}).allowLoadBasedChanges
var _ = (&storeEnactedHistory{}).gcHistory
var _ = enactedReplicaChange{}
var _ = (&storeState{}).computePendingChangesReflectedInLatestLoad
