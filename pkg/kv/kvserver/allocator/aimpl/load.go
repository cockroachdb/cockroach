// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aimpl

import (
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type loadDimension uint8

// TODO(sumeer): make thread safe.

// TODO(sumeer): what do we do about equalizing leaseCounts? Should we bother
// with them given trying to equalize the lease counts may interfere with
// other real resources. For real resources we don't have contradictory
// signals -- we can expect some stores/nodes to be underloaded in all
// dimensions and don't necessarily have to solve an incremental
// multi-dimensional bin-packing problem (incremental in the sense that it
// should make small mutations to the current placement, and not a large
// reshuffle).
//
// We could add leaseCount as a resource and not include it in the usual
// rebalancing. Then after the regular rebalancing pass (which does all the
// "real" rebalancing), do another pass that sees if there is scope to move
// some leases between stores that do not have any pending changes and are not
// overloaded (and will not get overloaded by the movement). The objective
// here potentially needs a rethink -- perhaps we should be equalizing leases
// for non-quiescent ranges, or better still equalizing for leases that have
// above some threshold of leaseholder activity (since that is what buys us
// more headroom in case of a load surge).

const (
	// Nanos per second
	cpu loadDimension = iota
	// Bytes per second.
	writeBandwidth
	// Bytes.
	byteSize
	numLoadDimensions
)

// NB: try to minimize specialized code for these individual dimensions to
// allow for easy addition of additional dimensions, or at least localize it
// to loadSummaryForDimension(). Currently we have specialized code for cpu
// since leaseholder transfers effect only part of the cpu. The dimensions are
// not in any order of importance (see the later comment on how rebalancing
// from overloaded nodes does not try to make a choice on what replicas to
// shed -- that is mostly captured by the node/store itself in how it selects
// the top-k).

// TODO(sumeer): add diskBandwidth in the above list, which is typically
// capacity limited by the sum of read and write bandwidth. For now, we allow
// the store provided writeBandwidth capacity (see comment with
// storeLoad.capacity) to achieve our IO load-balancing needs.
//
// We can't estimate the diskBandwidth for a range, and the disk bandwidth on
// a store can be skewed by a compaction backlog, and we don't want to
// over-react because of that. Some options to work around this:
//
// First option: is to interpolate the disk bandwidth using the write
// bandwidth, based on a multiplier computed at the cluster level (so as to
// not be skewed by compaction backlog at certain stores). This interpolation
// would allow us to compute the disk bandwidth of a range. Risk is that if we
// use the interpolated value we could under-react to a store overload. So we
// could use the actual measured bandwidth for the store, and use an
// interpolated value for the range.
//
// Second option: it is a variant of the first where the range bandwidth is
// not interpolated based on a cluster-wide multiplier, but is locally
// synthesized at the overloaded store based on what ranges are seeing high
// reads and writes. The store synthesizes a usage value for these ranges in
// the rangeLoad.load vector. Say chooses the expensive ranges as each using
// 5% of the disk bandwidth. Since the bandwidth utilization is high, we will
// try to shed load (and these ranges will be in the top-k), and we will shed
// up to 4 of them until we reach the 20% threshold of
// fractionPendingThresholdForNoChange. There should be other nodes that can
// accommodate a new range that consumes 5% of the disk bandwidth.

type loadValue int64
type loadVector [numLoadDimensions]loadValue

// Capacity information.
const (
	unknownCapacity loadValue = math.MaxInt64
	// parentCapacity is currently only used for cpu at the store level.
	parentCapacity loadValue = math.MaxInt64 - 1
)

type rangeLoad struct {
	load loadVector
	// Nanos per second. raftCPU <= load[cpu]. Handling this as a special case,
	// rather than trying to (over) generalize, since currently this is the only
	// resource broken down into two components.
	raftCPU loadValue
}

// topK is chosen high-enough that for overloaded nodes we can afford to
// rebalance away only the top-K ranges and use a subsequent updated store
// load from the store (with different top-K) to rebalance more. Also chosen
// to not encompass all ranges since it will increase network overhead of
// sending the load information. The choice of 10 is arbitrary and subject to
// revision. And individual stores could choose their own topK value when
// reporting -- so an overloaded store with all tiny load ranges could report
// more, and another with two very high load ranges that consume most of the
// resources could report two.
const topK = 10

const (
	// unknownReplicaID is used with a change that proposes to add a replica
	// (since it does not know the future ReplicaID).
	unknownReplicaID roachpb.ReplicaID = -1
	// noReplicaID is used with a change that is removing a replica.
	noReplicaID roachpb.ReplicaID = -1
)

type replicaIDAndType struct {
	// replicaID can also be set to unknownReplicaID and noReplicaID.
	replicaID     roachpb.ReplicaID
	replicaType   roachpb.ReplicaType
	isLeaseholder bool
}

// prev is the state before the proposed change and next is the state after
// the proposed change. rit is the current observed state.
func (rit replicaIDAndType) subsumesChange(prev replicaIDAndType, next replicaIDAndType) bool {
	if rit.replicaID == noReplicaID && next.replicaID == noReplicaID {
		// Removal has happened.
		return true
	}
	notSubsumed := (rit.replicaID == noReplicaID && next.replicaID != noReplicaID) ||
		(rit.replicaID != noReplicaID && next.replicaID == noReplicaID)
	if notSubsumed {
		return false
	}
	// Both rit and next have replicaIDs != noReplicaID. We don't actually care
	// about the replicaID's since we don't control them. If the replicaTypes
	// are as expected, and if we were either not trying to change the
	// leaseholder, or that leaseholder change has happened, then the change has
	// been subsumed.
	switch rit.replicaType {
	case roachpb.VOTER_INCOMING:
		// Already seeing the load, so consider the change done.
		rit.replicaType = roachpb.VOTER_FULL
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

// storeLoad is the load information for a store. Roughly, this is the
// information we need each store to provide us periodically, either fully, or
// as deltas from the previous load.
//
// TODO(sumeer): implement the delta approach, since we need it for network
// efficiency in a centralized allocator.
type storeLoad struct {
	// Aggregate store load. In general, we don't require this to be a sum of
	// the range loads (since a sharded allocator may only have information
	// about a subset of ranges).
	//
	// TODO(sumeer): check that any assumptions about the sum have not crept
	// into parts of the code.
	load loadVector
	// top-k ranges along some dimensions, chosen by the store. If the store is
	// closer to hitting the resource limit on some resource it should choose to
	// use that resource consumption to choose the top-k.
	//
	// This is extremely important: the allocator does not try to make a
	// decision on what ranges to shed for an overloaded node -- it simply tries
	// to find new homes for the ranges in this top-k. We decentralize this
	// decision to reduce the time complexity of rebalancing.
	topKRanges map[roachpb.RangeID]rangeLoad
	// Mean load for the non-top-k ranges. This is used to estimate the load
	// change for transferring them.
	meanNonTopKRangeLoad rangeLoad
	// The replicas on this store.
	replicas map[roachpb.RangeID]replicaIDAndType

	// Capacity information for this store.
	//
	// capacity[cpu] is parentCapacity.
	//
	// capacity[writeBandwidth] is typically unknownCapacity. However, if the
	// LSM on the store is getting overloaded, whether it is because of disk
	// bandwidth being reached or some other resource bottleneck (compactions
	// not keeping up), the store can set this to a synthesized value that
	// indicates high utilization, in order to shed some load.
	//
	// capacity[byteSize] is the actual capacity.
	capacity loadVector

	attrs roachpb.Attributes
}

// pendingChange is a proposed change. Some external entity (the leaseholder
// of the range) may choose to enact this change. It may not be enacted if it
// will cause some invariant (like the number of replicas, or having a
// leaseholder) to be violated. If not enacted, the allocator is not told
// about the lack of enactment -- the change will simply expire from the
// allocator's state after pendingChangeExpiryDuration. Such expiration
// without enactment should be rare. pendingChanges can be paired, when a
// range is being moved from one store to another -- that pairing is not
// captured here, and captured in the changes suggested by the allocator to
// the external entity.
type pendingChange struct {
	// The load this change adds to a store. The values will be negative if the
	// load is being removed.
	rangeLoadDelta rangeLoad

	storeID roachpb.StoreID
	rangeID roachpb.RangeID

	// Only following cases can happen:
	//
	// TODO(sumeer): IIRC, 0 is not a valid ReplicaID. If so, update the comment
	// below.
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
	prev replicaIDAndType
	next replicaIDAndType

	// Only used when we are removing a replica. This is needed in that case
	// since we remove from the topKRanges map, and we need to remember to undo
	// that change if needed. For all other changes we don't touch the
	// topKRanges map or the rangeLoad in it (even if we are adding a
	// leaseholder) -- we simply change the storeLoad.load.
	//
	// INVARIANT: prev.replicaID >= 0, and next.replicaID == noReplicaID.
	wasInTopK bool

	// The wall time at which this pending change was initiated. Used for
	// expiry.
	startTime time.Time
}

// TODO(sumeer): the code to handle NON_VOTER => VOTER_FULL transition and
// vice versa is horribly incomplete. The fix needs to be bottom up, by
// starting with data-structures like rangeAnalyzedConstraints.

// Makes a partial change to add a voter. This is partial in that we haven't
// yet selected the target store. rLoad captures the load that will be added.
func makePartialPendingChangeToAddVoter(rangeID roachpb.RangeID, rLoad rangeLoad) *pendingChange {
	return &pendingChange{
		rangeLoadDelta: rLoad,
		storeID:        0,
		rangeID:        rangeID,
		prev:           replicaIDAndType{replicaID: noReplicaID},
		next: replicaIDAndType{
			replicaID:     unknownReplicaID,
			replicaType:   roachpb.VOTER_FULL,
			isLeaseholder: false,
		},
		wasInTopK: false,
		startTime: time.Now(),
	}
}

// Makes a partial change to assume the lease. This is partial in that we haven't
// yet selected tht target store. rLoad is the load on the current leaseholder.
func makePartialPendingChangeToAddLeaseholder(
	rangeID roachpb.RangeID, rLoad rangeLoad,
) *pendingChange {
	cpuLoad := rLoad.load[cpu] - rLoad.raftCPU
	if cpuLoad < 0 {
		cpuLoad = 0
	}
	rLoad = rangeLoad{}
	rLoad.load[cpu] = cpuLoad
	return &pendingChange{
		rangeLoadDelta: rLoad,
		storeID:        0,
		rangeID:        rangeID,
		prev:           replicaIDAndType{},
		next:           replicaIDAndType{},
		wasInTopK:      false,
		startTime:      time.Now(),
	}
}

// If the pending change does not happen within this expiry duration, we
// forget it in the data-structure. 20s is an arbitrary choice.
const pendingChangeExpiryDuration = 20 * time.Second

type pendingChangesAgeDescending []*pendingChange

func (p *pendingChangesAgeDescending) removeChangeAtIndex(index int) {
	n := len(*p)
	copy((*p)[index:n-1], (*p)[index+1:n])
	*p = (*p)[:n-1]
}

// storeLoadAdjusted is the load adjusted for pending changes.
//
// TODO(sumeer): rename this to storeState since this is more than load.
type storeLoadAdjusted struct {
	storeID roachpb.StoreID
	// lastLoadSeqNum is used when the store is sending load deltas to the
	// allocator, to detect missing deltas and ask the store for the full load.
	// Asking for the full load should be rare, to optimize network bandwidth
	// usage.
	lastLoadSeqNum uint64

	// originalLoad is remembered since it is useful in computing cluster-wide
	// means and utilization.
	originalLoad loadVector

	// The adjusted load, based on what was received from the store and pending
	// changes. The state in the pending changes allows for the reverse
	// computation to be applied if the change is garbage collected.
	adjusted storeLoad
	// max(|1-(adjusted.load[i]/originalLoad[i])|)
	//
	// If maxFractionPending is greater than some threshold, we don't add or
	// remove more load unless we are shedding load due to failure detection.
	// This is to allow the effect of the changes to stabilize since our
	// adjustments to load vectors are estimates, and there can be overhead on
	// these nodes due to making the change.
	maxFractionPending float64
	// Pending changes from oldest startTime to newest startTime. Can be a bit
	// approximate in this ordering without any correctness issue.
	pendingChanges pendingChangesAgeDescending

	// An enum summary of the current load.
	loadSummary loadSummary

	// The load of the node containing this store.
	// TODO(sumeer): nodeLoad should be called nodeState.
	nodeLoad *nodeLoad
}

// Called periodically, since may not be receiving updates from store.
func (sl *storeLoadAdjusted) gcStoreLoad(
	now time.Time,
	ranges map[roachpb.RangeID]*rangeState,
	msl *meanStoreLoad,
	nodeSummary loadSummary,
	rangesToCheckForAttention map[roachpb.RangeID]struct{},
) {
	i := 0
	for ; i < len(sl.pendingChanges); i++ {
		change := sl.pendingChanges[i]
		if now.Sub(change.startTime) < pendingChangeExpiryDuration {
			break
		}
		sl.undoPendingChange(change, ranges, true)
		rangesToCheckForAttention[change.rangeID] = struct{}{}
	}
	if i > 0 {
		sl.pendingChanges = sl.pendingChanges[i:]
	}
	sl.updateLoadSummaryAndMaxFractionPending(msl, nodeSummary)
}

func (sl *storeLoadAdjusted) undoPendingChange(
	change *pendingChange, ranges map[roachpb.RangeID]*rangeState, updateNodeLoad bool,
) {
	rs, ok := ranges[change.rangeID]
	if !ok {
		panic("unknown range")
	}
	rs.removePendingChange(change)
	if change.next.replicaID == noReplicaID {
		// Was a removal.
		rs.addStore(sl.storeID)
		if change.wasInTopK {
			rl := change.rangeLoadDelta
			for i := range rl.load {
				rl.load[i] = -rl.load[i]
			}
			rl.raftCPU = -rl.raftCPU
			sl.adjusted.topKRanges[change.rangeID] = rl
		}
		sl.adjusted.replicas[change.rangeID] = change.prev
	} else if change.prev.replicaID == noReplicaID {
		// Was an addition.
		rs.removeStore(sl.storeID)
		delete(sl.adjusted.replicas, change.rangeID)
	} else {
		sl.adjusted.replicas[change.rangeID] = change.prev
	}
	for i := range sl.adjusted.load {
		sl.adjusted.load[i] -= change.rangeLoadDelta.load[i]
	}
	if updateNodeLoad {
		sl.nodeLoad.loadCPUAdjusted -= change.rangeLoadDelta.load[cpu]
	}
	// NB: does not update loadSummary for store or node since that is done once
	// after making a sequence of such calls.
}

func (sl *storeLoadAdjusted) redoPendingChange(change *pendingChange) {
	if change.next.replicaID == noReplicaID && change.wasInTopK {
		// Removal. Use the latest load info for this to be reversible.
		var rLoad rangeLoad
		rLoad, changeInTopK := sl.adjusted.topKRanges[change.rangeID]
		if !changeInTopK {
			rLoad = sl.adjusted.meanNonTopKRangeLoad
		}
		for i := range rLoad.load {
			rLoad.load[i] = -rLoad.load[i]
		}
		rLoad.raftCPU = -rLoad.raftCPU
		change.rangeLoadDelta = rLoad
		change.wasInTopK = changeInTopK
	}
	sl.doPendingChange(change)
}

func (sl *storeLoadAdjusted) doInitialPendingChange(change *pendingChange) {
	sl.doPendingChange(change)
	sl.pendingChanges = append(sl.pendingChanges, change)
}

// NB: no ranges parameter since either change is already in that list, or
// caller will add it.
func (sl *storeLoadAdjusted) doPendingChange(change *pendingChange) {
	if change.next.replicaID == noReplicaID {
		// Is a removal.
		if change.wasInTopK {
			delete(sl.adjusted.topKRanges, change.rangeID)
		}
		delete(sl.adjusted.replicas, change.rangeID)
	} else {
		// Is an addition or an update.
		sl.adjusted.replicas[change.rangeID] = change.next
	}
	for i := range sl.adjusted.load {
		sl.adjusted.load[i] += change.rangeLoadDelta.load[i]
		if loadDimension(i) == cpu {
			sl.nodeLoad.loadCPUAdjusted += change.rangeLoadDelta.load[i]
		}
	}
	// NB: does not update loadSummary for store or node since that is done once
	// after making a sequence of such calls.
}

// updateLoad is called when new load is received from the store. This is the
// non-incremental update case. It assumes that nodeLoad.loadCPU* have already
// been updated with the new load information.
//
// TODO(sumeer): proto and method for incremental updating of load.
func (sl *storeLoadAdjusted) updateLoad(
	now time.Time,
	newStoreLoad storeLoad,
	ranges map[roachpb.RangeID]*rangeState,
	msl *meanStoreLoad,
	nodeSummary loadSummary,
	rangesToCheckForAttention map[roachpb.RangeID]struct{},
) {
	// Decide which of the pending changes to undo.
	//
	// GC first.
	i := 0
	for ; i < len(sl.pendingChanges); i++ {
		if now.Sub(sl.pendingChanges[i].startTime) < pendingChangeExpiryDuration {
			break
		}
		// updateNodeLoad=false, since nodeLoad has already been updated with the
		// latest load.
		sl.undoPendingChange(sl.pendingChanges[i], ranges, false)
		rangesToCheckForAttention[sl.pendingChanges[i].rangeID] = struct{}{}
	}
	if i > 0 {
		sl.pendingChanges = sl.pendingChanges[i:]
	}
	// Now look through all pending changes to see what has been enacted. These
	// are also "undone" so we can remove them from pending changes in
	// storeLoadAdjusted and in rangeState.
	for i = 0; i < len(sl.pendingChanges); i++ {
		change := sl.pendingChanges[i]
		// If newStoreLoad changes to be a delta, this computation is still viable.
		idAndType, ok := newStoreLoad.replicas[change.rangeID]
		if !ok {
			idAndType.replicaID = noReplicaID
		}
		if idAndType.subsumesChange(change.prev, change.next) {
			sl.undoPendingChange(change, ranges, false)
			rangesToCheckForAttention[change.rangeID] = struct{}{}
			sl.pendingChanges.removeChangeAtIndex(i)
		}
	}

	sl.originalLoad = newStoreLoad.load
	// Now have only the changes that are still relevant applied to adjusted, so
	// these changes will be kept. Apply these changes to newStoreLoad. It is
	// viable to do this computation in the future with new load being a diff.
	oldAdjusted := sl.adjusted
	sl.adjusted = newStoreLoad
	for i := range sl.pendingChanges {
		sl.redoPendingChange(sl.pendingChanges[i])
	}

	// Now both reflect the same pending changes. There may still be diffs due
	// to other reasons. Fix the ranges map to account for those diffs. This
	// iteration equal to O(num-replicas) will go away if we are sending load
	// diffs.
	for rangeID, oldIDAndType := range oldAdjusted.replicas {
		newIDAndType, ok := sl.adjusted.replicas[rangeID]
		if !ok {
			// Existing replica that no longer exists.
			rState := ranges[rangeID]
			rState.removeStore(sl.storeID)
			rangesToCheckForAttention[rangeID] = struct{}{}
		} else {
			if oldIDAndType.replicaType != newIDAndType.replicaType {
				rangesToCheckForAttention[rangeID] = struct{}{}
				rState := ranges[rangeID]
				rState.constraints = nil
			}
		}
	}
	for rangeID, newIDAndType := range sl.adjusted.replicas {
		oldIDAndType, ok := oldAdjusted.replicas[rangeID]
		if !ok {
			// New replica that was not there previously.
			rState := ranges[rangeID]
			rState.addStore(sl.storeID)
			rangesToCheckForAttention[rangeID] = struct{}{}
		} else {
			if oldIDAndType.replicaType != newIDAndType.replicaType {
				rangesToCheckForAttention[rangeID] = struct{}{}
				rState := ranges[rangeID]
				rState.constraints = nil
			}
		}
	}
	sl.updateLoadSummaryAndMaxFractionPending(msl, nodeSummary)
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func (sl *storeLoadAdjusted) updateLoadSummaryAndMaxFractionPending(
	msl *meanStoreLoad, nodeSummary loadSummary,
) {
	sl.loadSummary, sl.maxFractionPending =
		sl.computeLoadSummaryAndMaxFractionPending(msl, nodeSummary)
}

// Read-only method. Helper for updateLoadSummaryAndMaxFractionPending and also for
// testing prospective changes.
func (sl *storeLoadAdjusted) computeLoadSummaryAndMaxFractionPending(
	msl *meanStoreLoad, nodeSummary loadSummary,
) (storeSummary loadSummary, maxFractionPending float64) {
	for i := range sl.originalLoad {
		if sl.originalLoad[i] == 0 && sl.adjusted.load[i] == 0 {
			continue
		}
		// If there was no load along some resource dimension and we have added
		// some, we set maxFractionPending = 1.0. This means we will start
		// slow on a new node wrt adding new ranges and gather pace over time.
		if sl.adjusted.load[i] > 0 && sl.originalLoad[i] == 0 {
			maxFractionPending = 1.0
			break
		}
		frac := float64(sl.adjusted.load[i]) / float64(sl.originalLoad[i])
		if frac >= 2.0 {
			maxFractionPending = 1.0
			break
		}
		// [0, 2).
		frac = math.Abs(1.0 - frac)
		maxFractionPending = maxFloat(frac, maxFractionPending)
	}

	if nodeSummary <= failureImminent {
		storeSummary = nodeSummary
		return storeSummary, maxFractionPending
	}
	const fractionPendingThresholdForNoChange = 0.2
	if maxFractionPending > fractionPendingThresholdForNoChange {
		storeSummary = loadNoChange
		return storeSummary, maxFractionPending
	}
	incomingReplicaCount := 0
	// Don't want many incoming snapshots since they will be throttled and will
	// cause pending changes to unnecessarily expire. In general, we don't want
	// the allocator to plan things that can not be done *now*, since by the
	// time something can be done, the load situation may have changed.
	//
	// TODO(sumeer): make this configurable.
	const maxPendingIncomingReplicas = 2
	for i := range sl.pendingChanges {
		change := sl.pendingChanges[i]
		if change.next.replicaID == unknownReplicaID {
			incomingReplicaCount++
			if incomingReplicaCount > maxPendingIncomingReplicas {
				storeSummary = loadNoChange
				return storeSummary, maxFractionPending
			}
		}
	}

	storeSummary = nodeSummary
	for i := range sl.adjusted.load {
		loadSummary := loadSummaryForDimension(
			sl.adjusted.load[i], sl.adjusted.capacity[i], msl.load[i], msl.util[i])
		if storeSummary > loadSummary {
			storeSummary = loadSummary
		}
	}
	return storeSummary, maxFractionPending
}

// canAddLoad returns true if the delta can be added to the store without
// causing it to be overloaded. It does not change any state between the call
// and return.
func (sl *storeLoadAdjusted) canAddLoad(
	delta loadVector, mnl meanNodeLoad, msl *meanStoreLoad,
) bool {
	sl.nodeLoad.loadCPUAdjusted += delta[cpu]
	for i := range delta {
		sl.adjusted.load[i] += delta[i]
	}
	nodeSummary := sl.nodeLoad.computeLoadSummary(mnl)
	var rv bool
	if sl.nodeLoad.loadSummary < loadNoChange {
		rv = false
	} else {
		storeSummary, _ := sl.computeLoadSummaryAndMaxFractionPending(msl, nodeSummary)
		rv = storeSummary >= loadNoChange
	}
	// Undo the changes made earlier.
	sl.nodeLoad.loadCPUAdjusted -= delta[cpu]
	for i := range delta {
		sl.adjusted.load[i] -= delta[i]
	}
	return rv
}

// Computes the loadSummary for a particular load dimension.
func loadSummaryForDimension(
	load loadValue, capacity loadValue, meanLoad loadValue, meanUtil float64,
) loadSummary {
	if capacity == parentCapacity {
		return loadLow
	}
	loadSummary := loadLow
	// Heuristics: this is all very rough and subject to revision. There are two
	// uses for this loadSummary: to find source stores to shed load and to
	// decide whether the added load on a target store is acceptable (without
	// driving it to overload). This latter use case may be better served by a
	// distance measure since we don't want to get too close to overload since
	// we could overshoot (an alternative to distance would be to slightly
	// over-estimate the load addition due to a range move, and then ask for the
	// load summary).
	//
	// The load summarization could be specialized for each load dimension e.g.
	// we may want to do a different summarization for cpu and byteSize since
	// the consequence of running out-of-disk is much more severe.
	//
	// The capacity may be unknownCapacity. Even if we have a known capacity, we
	// may want to consider how far we are away from mean. The mean isn't very
	// useful when there are heterogeneous nodes. It also does not help when
	// there are constraints only satisfied by a subset of nodes that have much
	// higher utilization. Even though we permit very general constraint and
	// locality specifications, it may be that the set of attributes used in
	// constraints and lease preferences are such that we can partition stores
	// into sets that share the same attributes and have the same locality
	// tiers, and use the means for each set. But even that is not very helpful
	// because some ranges may have fewer constraints than others.
	fractionAbove := float64(load)/float64(meanLoad) - 1.0
	if fractionAbove > 0.2 {
		loadSummary = overloadSlow
	} else if fractionAbove < -0.2 {
		loadSummary = loadLow
	} else {
		loadSummary = loadNormal
	}
	if capacity != unknownCapacity {
		// Further tune the summary based on utilization.
		fractionUsed := float64(load) / float64(capacity)
		if fractionUsed > 0.9 {
			if meanUtil < fractionUsed {
				return overloadUrgent
			}
			return overloadSlow
		}
		// INVARIANT: fractionUsed <= 0.9
		if fractionUsed > 0.75 {
			if meanUtil < fractionUsed {
				return overloadSlow
			} else {
				return loadSummary
			}
		}
		// INVARIANT: fractionUsed <= 0.75
		if fractionUsed < 0.5 && fractionUsed < meanUtil {
			return loadLow
		}
	}
	return loadSummary
}

// Used for ordering stores that are overloaded and need to shed load.
type storeLoadByDecreasingLoad []*storeLoadAdjusted

func (s storeLoadByDecreasingLoad) Len() int {
	return len(s)
}

func (s storeLoadByDecreasingLoad) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s storeLoadByDecreasingLoad) Less(i, j int) bool {
	return s[i].loadSummary < s[j].loadSummary
}

// loadSummary aggregates across all load dimensions and other input in
// storeLoadAdjusted and nodeLoad. These could also be scores instead of an
// enum, but eventually we want to decide what scores are roughly equal when
// deciding on rebalancing priority, and to decide how to order the stores we
// will try to rebalance to. So we simply use an enum.
//
// TODO(sumeer): these levels deliberately don't say anything about the action
// to take. Do we need a level that will explicitly direct us to shed
// leaseholders but do no other load shedding?
type loadSummary uint8

const (
	failureImminent loadSummary = iota
	overloadUrgent
	overloadSlow
	// Don't add or remove load from this store.
	loadNoChange
	loadNormal
	loadLow
)

// failureDetectionSummary is provided by an external entity and never
// computed inside the allocator. It is one of the inputs to loadSummary that
// is computed by the allocator.
type failureDetectionSummary uint8

// TODO(sumeer): make this more explicit wrt node liveness
// (storeStatusUnavailable), drain (storeStatusDraining), storeStatusSuspect,
// storeStatusDead etc.
const (
	fdOK failureDetectionSummary = iota
	fdFailureImminent
	fdFailed
)

// Information about the node.
type nodeLoad struct {
	// The latest load information received from the node.
	loadCPU     loadValue
	capacityCPU loadValue
	// Adjusted value based on pending changes.
	loadCPUAdjusted loadValue

	// This loadSummary is only based on the cpu. It is incorporated into the
	// loadSummary computed for each store on this node.
	loadSummary loadSummary
	fdSummary   failureDetectionSummary

	// Pre-processed roachpb.Locality.
	localityState localityState
}

func (n *nodeLoad) updateLoadSummary(mnl meanNodeLoad) {
	n.loadSummary = n.computeLoadSummary(mnl)
}

func (n *nodeLoad) computeLoadSummary(mnl meanNodeLoad) loadSummary {
	nodeSummary := loadSummaryForDimension(n.loadCPUAdjusted, n.capacityCPU, mnl.loadCPU, mnl.utilCPU)
	if n.fdSummary == fdFailureImminent || n.fdSummary == fdFailed {
		nodeSummary = failureImminent
	}
	return nodeSummary
}

type rangeState struct {
	// Adjusted replicas. Only tracking the storeID here. Use the storeID to
	// lookup in storeLoad.replicas using the rangeID, to get the type. We don't
	// maintain a consistent state of the replicas or what types they are -- a
	// replica is at a store if the store still claims it is at the store.
	//
	// TODO(sumeer): there is a risk in such self-reporting. If a store has been
	// removed from a raft group but it has stopped reporting load, we will
	// still think it is in that group and not realize that the group is
	// under-replicated. This is not a problem in a
	// distributed allocator where the only ranges being considered at this
	// allocator shard are the ones for which this shard is the leaseholder (so
	// it has authoritative information). I think for a centralized allocator we will need the current
	// leaseholder to report its full view of membership as part of the
	// storeLoad. We can use that to mark some stores as probably-removed in the
	// rangeState while continuing to maintain the range in the corresponding
	// storeLoadAdjusted -- we won't use these probably-removed replicas in
	// deciding whether the range needs attention.
	//
	// Such reporting by the leaseholder is also needed when it pauses a
	// replica. We will use that as a signal of overload of that store if that
	// store is not self-reporting overload (mark it as fdFailureImminent?).
	// Additionally it will be used as a hint on which ranges to select to first
	// remove from the store (since removing the paused ones is more efficient
	// in that the store does not need to catchup on those ranges).

	replicas []roachpb.StoreID
	conf     *spanConfig
	// Only 1 or 2 changes (latter represents a least transfer or rebalance that
	// adds and removes replicas).
	//
	// TODO(sumeer): we don't actually this slice other than as a count of
	// pending changes. So we could change this to be a count.
	pendingChanges []*pendingChange

	// If non-nil, it is up-to-date. Typically, non-nil for a range that has no
	// pendingChanges and is not satisfying some constraint, since we don't want
	// to repeat the analysis work every time we consider it.
	constraints *rangeAnalyzedConstraints

	// TODO(sumeer): populate and use.
	diversityIncreaseLastFailedAttempt time.Time
}

func (rs *rangeState) removeStore(storeID roachpb.StoreID) {
	// Linear scan. And swap with last.
	i := 0
	n := len(rs.replicas)
	for ; i < n; i++ {
		if rs.replicas[i] == storeID {
			rs.replicas[i], rs.replicas[n-1] = rs.replicas[n-1], rs.replicas[i]
			rs.replicas = rs.replicas[:n-1]
			break
		}
	}
	rs.constraints = nil
}

func (rs *rangeState) addStore(storeID roachpb.StoreID) {
	// Linear scan to dedup. Add to end.
	for _, s := range rs.replicas {
		if s == storeID {
			return
		}
	}
	rs.replicas = append(rs.replicas, storeID)
	rs.constraints = nil
}

func (rs *rangeState) removePendingChange(change *pendingChange) {
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

// The mean store load in the cluster. Used for loadSummary calculation.
type meanStoreLoad struct {
	load     loadVector
	capacity loadVector
	// Util is 0 for cpu, writeBandwidth. Non-zero for byteSize.
	util [numLoadDimensions]float64
}

// The mean node load in the cluster. Uses for loadSummary calculation.
type meanNodeLoad struct {
	loadCPU     loadValue
	capacityCPU loadValue
	utilCPU     float64
}

// TODO(sumeer): initialization and certain updates. We are missing:
// - Adding a new store and new node.
// - Removing a node and store.
// - Change in attributes of a store.
// - Init and update of posting lists caused by changes in attributes.
// - Init and adding/removing from ranges map. storeLoadAdjusted.updateLoad
//   assumes a RangeID that it sees is already in the map (with a populated
//   spanConfig).

// NB: we are trying to strike a balance between precomputing things (for
// efficiency), but not having to keep too many data-structures mutually
// consistent. We have likely not found the right balance yet. We may need to
// denormalize more if all the "joins" using hash map lookups become a
// significant cost in allocator microbenchmarks.

type loadState struct {
	meanStoreLoad meanStoreLoad

	meanNodeLoad meanNodeLoad

	nodes  map[roachpb.NodeID]*nodeLoad
	stores map[roachpb.StoreID]*storeLoadAdjusted
	ranges map[roachpb.RangeID]*rangeState

	// Ranges that are under-replicated, over-replicated, don't satisfy
	// constraints, have low diversity etc. Avoid iterating through all ranges.
	// A range is removed from this map if it has pending changes -- when those
	// pending changes go away (via expiry or otherwise) it gets added back so
	// we can check on it.
	rangesNeedingAttention map[roachpb.RangeID]struct{}
}

// Called periodically. Means are computed using the non-adjusted loads.
func (ls *loadState) computeMeans() {
	n := len(ls.stores)
	if n == 0 {
		ls.meanStoreLoad = meanStoreLoad{}
		ls.meanNodeLoad = meanNodeLoad{}
		return
	}
	var msl meanStoreLoad
	for _, sl := range ls.stores {
		for i := range msl.load {
			msl.load[i] += sl.originalLoad[i]
			if sl.adjusted.capacity[i] == parentCapacity || sl.adjusted.capacity[i] == unknownCapacity {
				msl.capacity[i] = parentCapacity
			} else {
				msl.capacity[i] += sl.adjusted.capacity[i]
			}
		}
	}
	for i := range msl.load {
		if msl.capacity[i] != parentCapacity {
			msl.util[i] = float64(msl.load[i]) / float64(msl.capacity[i])
			msl.capacity[i] /= loadValue(n)
		} else {
			msl.util[i] = 0
		}
		msl.load[i] /= loadValue(n)
	}

	var mnl meanNodeLoad
	n = len(ls.nodes)
	for _, nl := range ls.nodes {
		mnl.loadCPU += nl.loadCPU
		mnl.capacityCPU += nl.capacityCPU
	}
	mnl.utilCPU = float64(mnl.loadCPU) / float64(mnl.capacityCPU)
	mnl.loadCPU /= loadValue(n)
	mnl.capacityCPU /= loadValue(n)
}

func (ls *loadState) pendingChangeToDropReplica(
	rangeID roachpb.RangeID, storeID roachpb.StoreID,
) *pendingChange {
	sl := ls.stores[storeID]
	rLoad, wasInTopK := sl.adjusted.topKRanges[rangeID]
	if !wasInTopK {
		rLoad = sl.adjusted.meanNonTopKRangeLoad
	}
	for i := range rLoad.load {
		rLoad.load[i] = -rLoad.load[i]
	}
	rLoad.raftCPU = -rLoad.raftCPU
	change := &pendingChange{
		rangeLoadDelta: rLoad,
		storeID:        storeID,
		rangeID:        rangeID,
		prev:           sl.adjusted.replicas[rangeID],
		next:           replicaIDAndType{replicaID: noReplicaID},
		wasInTopK:      wasInTopK,
		startTime:      time.Now(),
	}
	sl.doInitialPendingChange(change)
	rs, ok := ls.ranges[rangeID]
	if !ok {
		panic("unknown range")
	}
	rs.removeStore(storeID)
	rs.pendingChanges = append(rs.pendingChanges, change)
	sl.nodeLoad.updateLoadSummary(ls.meanNodeLoad)
	sl.updateLoadSummaryAndMaxFractionPending(&ls.meanStoreLoad, sl.nodeLoad.loadSummary)
	return change
}

// change is already filled out. Need to change loadState to be consistent.
func (ls *loadState) pendingChangeToAddVoterReplica(change *pendingChange) {
	// TODO(sumeer):
}

// change is already filled out. Need to change loadState to be consistent.
func (ls *loadState) pendingChangeToAddLease(change *pendingChange) {
	// TODO(sumeer):
}

func (ls *loadState) pendingChangeToRemoveLease(
	store *storeLoadAdjusted, rLoad rangeLoad,
) *pendingChange {
	// TODO(sumeer):
	return nil
}

func (ls *loadState) diversityAddReplicaScore(
	rangeID roachpb.RangeID,
	candidateLocState localityState,
	candidateReplicaType roachpb.ReplicaType,
	diversities map[string]float64,
) float64 {
	// TODO(sumeer): this caching logic can be made correct, but we need to
	// handle the case that some add candidates are already NON_VOTERs and some
	// are not, and we don't handle this properly below. In general, the current
	// code is suspect wrt NON_VOTERs as mentioned in various todos.
	diversity, ok := diversities[candidateLocState.str]
	if ok {
		return diversity
	}
	var sumScore float64
	var numSamples int
	rState := ls.ranges[rangeID]
	// We don't need to calculate the overall diversityScore for the range, just
	// how well the new store would fit, because for any store that we might
	// consider adding the pairwise average diversity of the existing replicas
	// is the same.
	for _, storeID := range rState.replicas {
		storeLoad := ls.stores[storeID]
		replicaType := storeLoad.adjusted.replicas[rangeID].replicaType
		if replicaType == roachpb.VOTER_FULL || replicaType == roachpb.VOTER_INCOMING ||
			(candidateReplicaType == roachpb.NON_VOTER && (replicaType == roachpb.NON_VOTER ||
				replicaType == roachpb.VOTER_DEMOTING_NON_VOTER)) {
			ls := ls.stores[storeID].nodeLoad.localityState
			sumScore += ls.tiers.diversityScore(candidateLocState.tiers)
			numSamples++
		}
	}
	// If the range has no replicas, any node would be a perfect fit.
	if numSamples == 0 {
		diversities[candidateLocState.str] = roachpb.MaxDiversityScore
		return roachpb.MaxDiversityScore
	}
	score := sumScore / float64(numSamples)
	diversities[candidateLocState.str] = score
	return score
}

func (ls *loadState) diversityRemoveReplicaScore(
	rangeID roachpb.RangeID,
	candidateStoreID roachpb.StoreID,
	candidateLocState localityState,
	candidateReplicaType roachpb.ReplicaType,
	diversities map[string]float64,
) float64 {
	diversity, ok := diversities[candidateLocState.str]
	if ok {
		return diversity
	}
	var sumScore float64
	var numSamples int
	rState := ls.ranges[rangeID]
	// We don't need to calculate the overall diversityScore for the range,
	// because the original overall diversityScore of this range is always the
	// same.
	for _, storeID := range rState.replicas {
		if storeID == candidateStoreID {
			continue
		}
		storeLoad := ls.stores[storeID]
		replicaType := storeLoad.adjusted.replicas[rangeID].replicaType
		if replicaType == roachpb.VOTER_FULL || replicaType == roachpb.VOTER_INCOMING ||
			(candidateReplicaType == roachpb.NON_VOTER && (replicaType == roachpb.NON_VOTER ||
				replicaType == roachpb.VOTER_DEMOTING_NON_VOTER)) {
			ls := ls.stores[storeID].nodeLoad.localityState
			sumScore += ls.tiers.diversityScore(candidateLocState.tiers)
			numSamples++
		}
	}
	// If the range has no replicas, any node would be a perfect fit.
	if numSamples == 0 {
		diversities[candidateLocState.str] = roachpb.MaxDiversityScore
		return roachpb.MaxDiversityScore
	}
	score := sumScore / float64(numSamples)
	diversities[candidateLocState.str] = score
	return score
}

// Returns the delta diversity.
func (ls *loadState) diversityRebalanceReplicaScore(
	rangeID roachpb.RangeID,
	sourceStoreID roachpb.StoreID,
	sourceLocState localityState,
	targetStoreID roachpb.StoreID,
	targetLocState localityState,
	replicaType roachpb.ReplicaType,
	diversities map[string]float64,
) float64 {
	// TODO(sumeer): this caching logic can be made sound, but we need to handle
	// the case that some target candidates are already NON_VOTERs and some are
	// not, and we don't handle this properly below.
	diversity, ok := diversities[targetLocState.str]
	if ok {
		return diversity
	}
	var sumScore float64
	var numSamples int
	rState := ls.ranges[rangeID]
	// We don't need to calculate the overall diversityScore for the range,
	// because the original overall diversityScore of this range is always the
	// same.
	for _, storeID := range rState.replicas {
		if storeID == targetStoreID || storeID == sourceStoreID {
			continue
		}
		storeLoad := ls.stores[storeID]
		replicaType := storeLoad.adjusted.replicas[rangeID].replicaType
		if replicaType == roachpb.VOTER_FULL || replicaType == roachpb.VOTER_INCOMING ||
			(replicaType == roachpb.NON_VOTER && (replicaType == roachpb.NON_VOTER ||
				replicaType == roachpb.VOTER_DEMOTING_NON_VOTER)) {
			ls := ls.stores[storeID].nodeLoad.localityState
			sumScore +=
				ls.tiers.diversityScore(targetLocState.tiers) - ls.tiers.diversityScore(sourceLocState.tiers)
			numSamples++
		}
	}
	// If the range has no replicas, any node would be a perfect fit.
	if numSamples == 0 {
		diversities[targetLocState.str] = roachpb.MaxDiversityScore
		return roachpb.MaxDiversityScore
	}
	score := sumScore / float64(numSamples)
	diversities[targetLocState.str] = score
	return score

}
