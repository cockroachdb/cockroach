// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

// These values can sometimes be used in ReplicaType, ReplicaIDAndType,
// ReplicaState, specifically when used in the context of a
// pendingReplicaChange.
const (
	// unknownReplicaID is used with a change that proposes to add a replica
	// (since it does not know the future ReplicaID).
	unknownReplicaID roachpb.ReplicaID = -1
	// noReplicaID is used with a change that is removing a replica.
	noReplicaID roachpb.ReplicaID = -2
)

type ReplicaType struct {
	ReplicaType   roachpb.ReplicaType
	IsLeaseholder bool
}

type ReplicaIDAndType struct {
	// ReplicaID can be set to unknownReplicaID or noReplicaID.
	roachpb.ReplicaID
	// In general, all roachpb.ReplicaTypes can be represented here. Some
	// contexts that use ReplicaIDAndType may only represent a subset of
	// roachpb.ReplicaTypes -- the commentary in those contexts will specify if
	// that is the case.
	ReplicaType
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rt ReplicaIDAndType) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print("replica-id=")
	switch rt.ReplicaID {
	case unknownReplicaID:
		w.Print("unknown")
	case noReplicaID:
		w.Print("none")
	default:
		w.Print(rt.ReplicaID)
	}
	w.Printf(" type=%v", rt.ReplicaType.ReplicaType)
	if rt.IsLeaseholder {
		w.Print(" leaseholder=true")
	}
}

func (rt ReplicaIDAndType) String() string {
	return redact.StringWithoutMarkers(rt)
}

// subsumesChange returns true if rit subsumes prev and next. prev is the state
// before the proposed change and next is the state after the proposed change.
// rit is the current observed state.
func (rit *ReplicaIDAndType) subsumesChange(prev, next ReplicaIDAndType) bool {
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
	switch rit.ReplicaType.ReplicaType {
	case roachpb.VOTER_INCOMING:
		// Already seeing the load, so consider the change done.
		rit.ReplicaType.ReplicaType = roachpb.VOTER_FULL
	}
	// rit.replicaId equal to LEARNER, VOTER_DEMOTING* are left as-is. If next
	// is trying to remove a replica, this change has not finished yet, and the
	// store is still seeing the load corresponding to the state it is exiting.
	if rit.ReplicaType == next.ReplicaType && (prev.IsLeaseholder == next.IsLeaseholder ||
		rit.IsLeaseholder == next.IsLeaseholder) {
		return true
	}
	return false
}

type ReplicaState struct {
	ReplicaIDAndType
	// VoterIsLagging can be set for a VOTER_FULL replica that has fallen
	// behind, i.e., it's matching log is less than the current committed log.
	// It is a hint to the allocator not to transfer the lease to this replica.
	VoterIsLagging bool
	// TODO(kvoli,sumeerbhola): Consider adding in rac2.SendQueue information to
	// prevent lease transfers to replicas which are not able to take the lease
	// due to a send queue. Do we even need this, given the VoterIsLagging
	// should be true whenever there is a send queue? I suppose if we are
	// force-flushing to a replica, it is possible for VoterIsLagging to be
	// false and the send queue to be non-empty. But we could incorporate the
	// send-queue state into VoterIsLagging -- having two bools doesn't seem
	// beneficial.
}

// ChangeID is a unique ID, in the context of this data-structure and when
// receiving updates about enactment having happened or having been rejected
// (by the component responsible for change enactment).
type ChangeID uint64

type ReplicaChange struct {
	// The load this change adds to a store. The values will be negative if the
	// load is being removed.
	loadDelta          LoadVector
	secondaryLoadDelta SecondaryLoadVector

	// target is the target {store,node} for the change.
	target  roachpb.ReplicationTarget
	rangeID roachpb.RangeID

	// NB: 0 is not a valid ReplicaID, but this component does not care about
	// this level of detail (the special consts defined above use negative
	// ReplicaID values as markers).
	//
	// Only following cases can happen:
	//
	// - prev.replicaID >= 0 && next.replicaID == noReplicaID: outgoing replica.
	//   prev.IsLeaseholder is false, since we shed a lease first.
	//
	// - prev.replicaID == noReplicaID && next.replicaID == unknownReplicaID:
	//   incoming replica, next.ReplicaType must be VOTER_FULL or NON_VOTER.
	//   Both IsLeaseholder fields must be false.
	//
	// - prev.replicaID >= 0 && next.replicaID >= 0: can be a change to
	//   IsLeaseholder, or ReplicaType. next.ReplicaType must be VOTER_FULL or
	//   NON_VOTER.
	prev ReplicaState
	next ReplicaIDAndType
}

func (rc ReplicaChange) String() string {
	return redact.StringWithoutMarkers(rc)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rc ReplicaChange) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%v %v (%v)->(%v)", rc.rangeID, rc.target, rc.prev, rc.next)
}

// isRemoval returns true if the change is a removal of a replica.
func (rc ReplicaChange) isRemoval() bool {
	return rc.prev.ReplicaID >= 0 && rc.next.ReplicaID == noReplicaID
}

// isAddition returns true if the change is an addition of a replica.
func (rc ReplicaChange) isAddition() bool {
	return rc.prev.ReplicaID == noReplicaID && rc.next.ReplicaID == unknownReplicaID
}

// isUpdate returns true if the change is an update to the replica type or
// leaseholder status. This includes promotion/demotion changes.
func (rc ReplicaChange) isUpdate() bool {
	return rc.prev.ReplicaID >= 0 && rc.next.ReplicaID >= 0
}

// isPromoDemo returns true if the change is a promotion or demotion of a
// replica.
func (rc ReplicaChange) isPromoDemo() bool {
	return rc.prev.ReplicaID >= 0 && rc.next.ReplicaID >= 0 &&
		rc.prev.ReplicaType.ReplicaType != rc.next.ReplicaType.ReplicaType
}

func MakeLeaseTransferChanges(
	rangeID roachpb.RangeID,
	existingReplicas []StoreIDAndReplicaState,
	rLoad RangeLoad,
	addTarget, removeTarget roachpb.ReplicationTarget,
) [2]ReplicaChange {
	addIdx, removeIdx := -1, -1
	for i, replica := range existingReplicas {
		if replica.StoreID == addTarget.StoreID {
			addIdx = i
		}
		if replica.StoreID == removeTarget.StoreID {
			removeIdx = i
		}
	}
	if removeIdx == -1 {
		panic(fmt.Sprintf(
			"existing leaseholder replica doesn't exist on store %v", removeIdx))
	}
	if addIdx == -1 {
		panic(fmt.Sprintf(
			"new leaseholder replica doesn't exist on store %v", addTarget))
	}

	remove := existingReplicas[removeIdx]
	add := existingReplicas[addIdx]
	// Sanity check the lease transfer, we cannot transfer a lease to a replica
	// that is already a leaseholder, nor can we transfer a lease from a replica
	// that is not the leaseholder.
	if !remove.IsLeaseholder {
		panic(fmt.Sprintf(
			"r%v lease transfer-from target %v isn't the leaseholder %v replicas=%v",
			rangeID, removeTarget, remove.ReplicaState, existingReplicas))
	}
	if add.IsLeaseholder {
		panic(fmt.Sprintf(
			"r%v lease transfer-to target %v is already the leaseholder %v replicas=%v",
			rangeID, addTarget, add.ReplicaState, existingReplicas))
	}

	removeLease := ReplicaChange{
		target:  removeTarget,
		rangeID: rangeID,
		prev:    remove.ReplicaState,
		next:    remove.ReplicaIDAndType,
	}
	addLease := ReplicaChange{
		target:  addTarget,
		rangeID: rangeID,
		prev:    add.ReplicaState,
		next:    add.ReplicaIDAndType,
	}
	removeLease.next.IsLeaseholder = false
	addLease.next.IsLeaseholder = true
	removeLease.secondaryLoadDelta[LeaseCount] = -1
	addLease.secondaryLoadDelta[LeaseCount] = 1

	// Only account for the leaseholder CPU, all other primary load dimensions
	// are ignored. Byte size and write bytes are not impacted by having a range
	// lease.
	nonRaftCPU := rLoad.Load[CPURate] - rLoad.RaftCPU
	removeLease.loadDelta[CPURate] = -nonRaftCPU
	addLease.loadDelta[CPURate] = loadToAdd(nonRaftCPU)
	return [2]ReplicaChange{removeLease, addLease}
}

// MakeAddReplicaChange creates a replica change which adds the replica type
// to the store addStoreID. The load impact of adding the replica does not
// account for whether the replica is becoming the leaseholder or not.
func MakeAddReplicaChange(
	rangeID roachpb.RangeID,
	rLoad RangeLoad,
	replicaState ReplicaState,
	addTarget roachpb.ReplicationTarget,
) ReplicaChange {
	replicaState.ReplicaID = unknownReplicaID
	addReplica := ReplicaChange{
		target:  addTarget,
		rangeID: rangeID,
		prev: ReplicaState{
			ReplicaIDAndType: ReplicaIDAndType{
				ReplicaID: noReplicaID,
			},
		},
		next: replicaState.ReplicaIDAndType,
	}
	addReplica.next.ReplicaID = unknownReplicaID
	addReplica.loadDelta.add(loadVectorToAdd(rLoad.Load))
	if replicaState.IsLeaseholder {
		addReplica.secondaryLoadDelta[LeaseCount] = 1
	} else {
		// Set the load delta for CPU to be just the raft CPU. The non-raft CPU we
		// assume is associated with the lease.
		addReplica.loadDelta[CPURate] = loadToAdd(rLoad.RaftCPU)
	}
	return addReplica
}

// MakeRemoveReplicaChange creates a replica change which removes the replica
// given. The load impact of removing the replica does not account for whether
// the replica was the previous leaseholder or not.
func MakeRemoveReplicaChange(
	rangeID roachpb.RangeID,
	rLoad RangeLoad,
	replicaState ReplicaState,
	removeTarget roachpb.ReplicationTarget,
) ReplicaChange {
	removeReplica := ReplicaChange{
		target:  removeTarget,
		rangeID: rangeID,
		prev:    replicaState,
		next: ReplicaIDAndType{
			ReplicaID: noReplicaID,
		},
	}
	removeReplica.loadDelta.subtract(rLoad.Load)
	if replicaState.IsLeaseholder {
		removeReplica.secondaryLoadDelta[LeaseCount] = -1
	} else {
		// Set the load delta for CPU to be just the raft CPU. The non-raft CPU is
		// associated with the lease.
		removeReplica.loadDelta[CPURate] = -rLoad.RaftCPU
	}
	return removeReplica
}

// makeRebalanceReplicaChanges creates to replica changes, adding a replica and
// removing another. If the replica being rebalanced is the current
// leaseholder, the impact of the rebalance also includes the lease load.
func makeRebalanceReplicaChanges(
	rangeID roachpb.RangeID,
	existingReplicas []StoreIDAndReplicaState,
	rLoad RangeLoad,
	addTarget, removeTarget roachpb.ReplicationTarget,
) [2]ReplicaChange {
	var remove StoreIDAndReplicaState
	for _, replica := range existingReplicas {
		if replica.StoreID == removeTarget.StoreID {
			remove = replica
		}
	}
	if remove == (StoreIDAndReplicaState{}) {
		log.Fatalf(context.Background(), "remove target %s not in existing replicas", removeTarget)
	}

	addState := ReplicaState{
		ReplicaIDAndType: ReplicaIDAndType{
			ReplicaID:   unknownReplicaID,
			ReplicaType: remove.ReplicaType,
		},
	}
	addReplicaChange := MakeAddReplicaChange(rangeID, rLoad, addState, addTarget)
	removeReplicaChange := MakeRemoveReplicaChange(rangeID, rLoad, remove.ReplicaState, removeTarget)
	return [2]ReplicaChange{addReplicaChange, removeReplicaChange}
}

// PendingRangeChange is a proposed set of change(s) to a range. It can consist
// of multiple pending replica changes, such as adding or removing replicas, or
// transferring the lease.
type PendingRangeChange struct {
	RangeID               roachpb.RangeID
	pendingReplicaChanges []*pendingReplicaChange
}

func (prc PendingRangeChange) String() string {
	return redact.StringWithoutMarkers(prc)
}

// SafeFormat implements the redact.SafeFormatter interface.
//
// This is adhoc for debugging. A nicer string format would include the
// previous state and next state.
func (prc PendingRangeChange) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%v=[", prc.RangeID)
	found := false
	if prc.IsTransferLease() {
		w.Printf("transfer_to=%v", prc.LeaseTransferTarget())
		found = true
	}
	if prc.IsChangeReplicas() {
		w.Printf("change_replicas=%v", prc.ReplicationChanges())
		found = true
	}
	if !found {
		panic("unknown change type")
	}
	w.Print(" cids=")
	for i, c := range prc.pendingReplicaChanges {
		if i > 0 {
			w.Print(",")
		}
		w.Printf("%v", c.ChangeID)
	}
	w.Print("]")
}

// ChangeIDs returns the list of ChangeIDs associated with the pending range
// change.
func (prc PendingRangeChange) ChangeIDs() []ChangeID {
	cids := make([]ChangeID, len(prc.pendingReplicaChanges))
	for i, c := range prc.pendingReplicaChanges {
		cids[i] = c.ChangeID
	}
	return cids
}

// IsChangeReplicas returns true if the pending range change is a change
// replicas operation.
func (prc PendingRangeChange) IsChangeReplicas() bool {
	for _, c := range prc.pendingReplicaChanges {
		if c.isAddition() || c.isRemoval() || c.isPromoDemo() {
			continue
		} else {
			return false
		}
	}
	return true
}

// IsTransferLease returns true if the pending range change is a transfer lease
// operation.
func (prc PendingRangeChange) IsTransferLease() bool {
	if len(prc.pendingReplicaChanges) != 2 {
		return false
	}
	var foundAddLease, foundRemoveLease bool
	for _, c := range prc.pendingReplicaChanges {
		if c.isAddition() || c.isRemoval() || c.isPromoDemo() {
			// Any changes to the replica type or replicaID are not lease transfers,
			// since they require a replication change.
			return false
		}
		if c.prev.IsLeaseholder && !c.next.IsLeaseholder {
			foundRemoveLease = true
		} else if !c.prev.IsLeaseholder && c.next.IsLeaseholder {
			foundAddLease = true
		} else {
			return false
		}
	}
	return foundAddLease && foundRemoveLease
}

// ReplicationChanges returns the replication changes for the pending range
// change. It panics if the pending range change is not a change replicas
// operation.
//
// TODO: Support for promotion/demotion changes.
func (prc PendingRangeChange) ReplicationChanges() kvpb.ReplicationChanges {
	if !prc.IsChangeReplicas() {
		panic("RangeChange is not a change replicas")
	}
	chgs := make([]kvpb.ReplicationChange, len(prc.pendingReplicaChanges))
	for i, c := range prc.pendingReplicaChanges {
		chgs[i].Target = c.target
		if c.prev.ReplicaID == noReplicaID {
			chgs[i].ChangeType = roachpb.ADD_VOTER
		} else if c.next.ReplicaID == noReplicaID {
			chgs[i].ChangeType = roachpb.REMOVE_VOTER
		}
	}
	return chgs
}

// LeaseTransferTarget returns the store ID of the store that is the target of
// the lease transfer. It panics if the pending range change is not a transfer
// lease operation.
func (prc PendingRangeChange) LeaseTransferTarget() roachpb.StoreID {
	if !prc.IsTransferLease() {
		panic("pendingRangeChange is not a lease transfer")
	}
	for _, c := range prc.pendingReplicaChanges {
		if !c.prev.IsLeaseholder && c.next.IsLeaseholder {
			return c.target.StoreID
		}
	}
	panic("unreachable")
}

// LeaseTransferFrom returns the store ID of the store that is the source of
// the lease transfer. It panics if the pending range change is not a
func (prc PendingRangeChange) LeaseTransferFrom() roachpb.StoreID {
	if !prc.IsTransferLease() {
		panic("pendingRangeChange is not a lease transfer")
	}
	for _, c := range prc.pendingReplicaChanges {
		if c.prev.IsLeaseholder && !c.next.IsLeaseholder {
			return c.target.StoreID
		}
	}
	panic("unreachable")
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
	ChangeID
	ReplicaChange

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

// storeState maintains the complete state about a store as known to the
// allocator.
type storeState struct {
	storeMembership
	storeLoad
	adjusted struct {
		load          LoadVector
		secondaryLoad SecondaryLoadVector
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
		loadPendingChanges map[ChangeID]*pendingReplicaChange
		// replicas is computed from the authoritative information provided by
		// various leaseholders in storeLeaseholderMsgs and adjusted for pending
		// changes in clusterState.pendingChanges/rangeState.pendingChanges.
		//
		// NB: this can include LEARNER and VOTER_DEMOTING_LEARNER replicas.
		replicas map[roachpb.RangeID]ReplicaState
		// topKRanges along some load dimension. If the store is overloaded along
		// one resource dimension, that is the dimension chosen when picking the
		// top-k. It includes ranges whose replicas are being removed via pending
		// changes, since those pending changes may be reversed, and we don't want
		// to bother recomputing the top-k.
		//
		// We may decide to keep this top-k up-to-date incrementally. Since
		// StoreLeaseholderMsg is incremental about the ranges it reports, that
		// may provide a building block for the incremental computation.
		//
		// The key in this map is a local store-id.
		//
		// NB: this does not include LEARNER and VOTER_DEMOTING_LEARNER replicas.
		topKRanges map[roachpb.StoreID]*topKReplicas
	}
	// This is a locally incremented seqnum which is incremented whenever the
	// adjusted or reported load information for this store or the containing
	// node is updated. It is utilized for cache invalidation of the
	// storeLoadSummary stored in meansForStoreSet.
	loadSeqNum uint64

	// maxFractionPendingIncrease is computed for load dimensions where
	// adjusted.load[i] > reportedLoad[i], and maxFractionPendingDecrease is
	// computed for load dimensions where adjusted.load[i] < reportedLoad[i].
	// These are:
	// max(|1-(adjusted.load[i]/reportedLoad[i])|)
	//
	// If maxFractionPendingIncrease is greater than some threshold, we don't add
	// more load to the store. If maxFractionPendingIncrease is greater than zero,
	// we don't shed from that store, since we may be over-estimating the load on
	// that store.
	//
	// If maxFractionPendingDecrease is greater than some threshold, we don't
	// remove more load unless we are shedding load due to failure detection.
	//
	// This is to allow the effect of the changes to stabilize since our
	// adjustments to load vectors are estimates, and there can be overhead on
	// these nodes due to making the change.
	maxFractionPendingIncrease float64
	maxFractionPendingDecrease float64

	// TODO: consider adding a maxFractionPending at the node level since with
	// many stores on a node, some stores may have used up all the budget for
	// changes at the node.

	localityTiers

	// Time when this store started to be observed as overloaded. Set by
	// allocatorState.rebalanceStores.
	overloadStartTime time.Time
	// When overloaded this is equal to time.Time{}.
	overloadEndTime time.Time
}

// The time duration between a change happening at a store, and when the
// effect of that change is seen in the load information computed by that
// store.
//
// NOTE: The gossip interval is 10s  (see gossip.go StoresInterval). On a
// signficant load change (% delta), the store will gossip more frequently (see
// kvserver/store_gossip.go).
//
// This value has nothing to do with the gossip interval, since this is lag
// added to the origin timestamp of the gossip message. It probably has to do
// with the fact that we use NodeCapacity when constructing StoreLoadMsg, and
// that includes NodeCPURateUsage which is the actual observed node cpu rate,
// and we want to give some time for that to react to the change. We use
// RuntimeLoadMonitor for that node cpu rate -- need to look at that code to
// see whether this 10s makes sense.
const lagForChangeReflectedInLoad = 10 * time.Second

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

func (ss *storeState) computeMaxFractionPending() {
	fracIncrease := 0.0
	fracDecrease := 0.0
	for i := range ss.reportedLoad {
		if ss.reportedLoad[i] == 0 {
			fracIncrease = 1000
			fracDecrease = 1000
			break
		}
		f := math.Abs(float64(ss.adjusted.load[i]-ss.reportedLoad[i])) / float64(ss.reportedLoad[i])
		if ss.adjusted.load[i] > ss.reportedLoad[i] {
			if f > fracIncrease {
				fracIncrease = f
			}
		} else if f > fracDecrease {
			fracDecrease = f
		}
	}
	ss.maxFractionPendingIncrease = fracIncrease
	ss.maxFractionPendingDecrease = fracDecrease
}

func newStoreState(storeID roachpb.StoreID, nodeID roachpb.NodeID) *storeState {
	ss := &storeState{
		storeLoad: storeLoad{
			StoreID: storeID,
			NodeID:  nodeID,
		},
	}
	ss.adjusted.loadPendingChanges = map[ChangeID]*pendingReplicaChange{}
	ss.adjusted.replicas = map[roachpb.RangeID]ReplicaState{}
	ss.adjusted.topKRanges = map[roachpb.StoreID]*topKReplicas{}
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
	NodeLoad
	adjustedCPU LoadValue

	fdSummary failureDetectionSummary
}

func newNodeState(nodeID roachpb.NodeID) *nodeState {
	return &nodeState{
		stores: []roachpb.StoreID{},
		NodeLoad: NodeLoad{
			NodeID: nodeID,
		},
	}
}

type StoreIDAndReplicaState struct {
	roachpb.StoreID
	// Only valid ReplicaTypes are used here.
	ReplicaState
}

func (s StoreIDAndReplicaState) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s StoreIDAndReplicaState) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("s%v:%v", s.StoreID, s.ReplicaState.ReplicaIDAndType)
}

// rangeState is periodically updated based on reporting by the leaseholder.
type rangeState struct {
	// replicas is the adjusted replicas. It is always consistent with
	// the storeState.adjusted.replicas in the corresponding stores.
	replicas []StoreIDAndReplicaState
	conf     *normalizedSpanConfig

	load RangeLoad

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
	//
	// REMINDER: rangeAnalyzedConstraints ignores LEARNER and
	// VOTER_DEMOTING_LEARNER replicas. So if a voter/non-voter is being added
	// and is currently a LEARNER, calling one of the methods on
	// rangeAnalyzedConstraints that tells us about an unsatisfied constraint
	// can give us something that is already pending. Our expectation is that
	// the pending changes will be reflected in pendingChanges. Then it becomes
	// the responsibility of a higher layer (allocator) to notice that the
	// rangeState has pendingChanges, and not make any more changes.
	constraints *rangeAnalyzedConstraints

	// TODO(sumeer): populate and use.
	diversityIncreaseLastFailedAttempt time.Time
}

func newRangeState() *rangeState {
	return &rangeState{
		replicas:       []StoreIDAndReplicaState{},
		pendingChanges: []*pendingReplicaChange{},
	}
}

func (rs *rangeState) setReplica(repl StoreIDAndReplicaState) {
	for i := range rs.replicas {
		if rs.replicas[i].StoreID == repl.StoreID {
			rs.replicas[i].ReplicaState = repl.ReplicaState
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
			return
		}
	}
	// TODO(sumeer): uncomment once we fix the external change problem.
	// panic(errors.AssertionFailedf("store %v has no replica", storeID))
}

func (rs *rangeState) removePendingChangeTracking(changeID ChangeID) {
	n := len(rs.pendingChanges)
	for i := 0; i < n; i++ {
		if rs.pendingChanges[i].ChangeID == changeID {
			rs.pendingChanges[i], rs.pendingChanges[n-1] = rs.pendingChanges[n-1], rs.pendingChanges[i]
			rs.pendingChanges = rs.pendingChanges[:n-1]
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
	// Removed from based on RangeMsg (provided by the leaseholder),
	// AdjustPendingChangesDisposition (provided by the enacting module at the
	// leaseholder), or time-based GC.
	pendingChanges map[ChangeID]*pendingReplicaChange
	changeSeqGen   ChangeID

	*constraintMatcher
	*localityTierInterner
	meansMemo *meansMemo
}

func newClusterState(ts timeutil.TimeSource, interner *stringInterner) *clusterState {
	cs := &clusterState{
		ts:                   ts,
		nodes:                map[roachpb.NodeID]*nodeState{},
		stores:               map[roachpb.StoreID]*storeState{},
		ranges:               map[roachpb.RangeID]*rangeState{},
		pendingChanges:       map[ChangeID]*pendingReplicaChange{},
		constraintMatcher:    newConstraintMatcher(interner),
		localityTierInterner: newLocalityTierInterner(interner),
	}
	cs.meansMemo = newMeansMemo(cs, cs.constraintMatcher)
	return cs
}

func (cs *clusterState) processStoreLoadMsg(ctx context.Context, storeMsg *StoreLoadMsg) {
	now := cs.ts.Now()
	cs.gcPendingChanges(now)

	ns := cs.nodes[storeMsg.NodeID]
	ss := cs.stores[storeMsg.StoreID]
	// Handle the node load, updating the reported load and set the adjusted load
	// to be equal to the reported load initially. Any remaining pending changes
	// will be re-applied to the reported load.
	if ns == nil {
		panic(fmt.Sprintf("node %d not found storeMsg=%v", storeMsg.NodeID, *storeMsg))
	}
	if ss == nil {
		panic(fmt.Sprintf("store %d not found", storeMsg.StoreID))
	}
	ns.ReportedCPU += storeMsg.Load[CPURate] - ss.reportedLoad[CPURate]
	ns.CapacityCPU += storeMsg.Capacity[CPURate] - ss.capacity[CPURate]
	// Undo the adjustment for the store. We will apply the adjustment again
	// below.
	ns.adjustedCPU += storeMsg.Load[CPURate] - ss.adjusted.load[CPURate]

	// The store's load sequence number is incremented on each load change. The
	// store's load is updated below.
	ss.loadSeqNum++
	ss.storeLoad.reportedLoad = storeMsg.Load
	ss.storeLoad.capacity = storeMsg.Capacity
	ss.storeLoad.reportedSecondaryLoad = storeMsg.SecondaryLoad

	// Reset the adjusted load to be the reported load. We will re-apply any
	// remaining pending change deltas to the updated adjusted load.
	ss.adjusted.load = storeMsg.Load
	ss.adjusted.secondaryLoad = storeMsg.SecondaryLoad
	ss.maxFractionPendingIncrease, ss.maxFractionPendingDecrease = 0, 0

	// Find any load pending changes for ranges which involve this store, that
	// can now be removed from the loadPendingChanges. We don't need to undo the
	// corresponding delta adjustment as the reported load already contains the
	// effect.
	for _, change := range ss.computePendingChangesReflectedInLatestLoad(storeMsg.LoadTime) {
		log.Infof(ctx, "s%d not-pending %v", storeMsg.StoreID, change)
		delete(ss.adjusted.loadPendingChanges, change.ChangeID)
	}

	for _, change := range ss.adjusted.loadPendingChanges {
		// The pending change hasn't been reported as done, re-apply the load
		// delta to the adjusted load and include it in the new adjusted load
		// replicas.
		cs.applyChangeLoadDelta(change.ReplicaChange)
	}
	log.Infof(ctx, "s%d load %s(%s) adjusted %s", storeMsg.StoreID,
		storeMsg.Load, storeMsg.Capacity, ss.adjusted.load)
}

func (cs *clusterState) processStoreLeaseholderMsg(msg *StoreLeaseholderMsg) {
	cs.processStoreLeaseholderMsgInternal(msg, numTopKReplicas)
}

func (cs *clusterState) processStoreLeaseholderMsgInternal(
	msg *StoreLeaseholderMsg, numTopKReplicas int,
) {
	now := cs.ts.Now()
	cs.gcPendingChanges(now)

	for _, rangeMsg := range msg.Ranges {
		rs, ok := cs.ranges[rangeMsg.RangeID]
		if !ok {
			// This is the first time we've seen this range.
			rs = newRangeState()
			cs.ranges[rangeMsg.RangeID] = rs
		}
		// Set the range state and store state to match the range message state
		// initially. The pending changes which are not enacted in the range
		// message are handled and added back below.
		rs.load = rangeMsg.RangeLoad
		for _, replica := range rs.replicas {
			ss := cs.stores[replica.StoreID]
			if ss == nil {
				panic(fmt.Sprintf("store %d not found stores=%v", replica.StoreID, cs.stores))
			}
			delete(cs.stores[replica.StoreID].adjusted.replicas, rangeMsg.RangeID)
		}
		rs.replicas = append(rs.replicas[:0], rangeMsg.Replicas...)
		for _, replica := range rangeMsg.Replicas {
			cs.stores[replica.StoreID].adjusted.replicas[rangeMsg.RangeID] = replica.ReplicaState
		}

		// Find any pending changes which are now enacted, according to the
		// leaseholder.
		var remainingChanges, enactedChanges []*pendingReplicaChange
		for _, change := range rs.pendingChanges {
			ss := cs.stores[change.target.StoreID]
			adjustedReplicas, ok := ss.adjusted.replicas[rangeMsg.RangeID]
			if !ok {
				adjustedReplicas.ReplicaID = noReplicaID
			}
			if adjustedReplicas.subsumesChange(change.prev.ReplicaIDAndType, change.next) {
				// The change has been enacted according to the leaseholder.
				enactedChanges = append(enactedChanges, change)
			} else {
				remainingChanges = append(remainingChanges, change)
			}
		}

		for _, change := range enactedChanges {
			// Mark the change as enacted. Enacting a change does not remove the
			// corresponding load adjustments. The store load message will do that,
			// indicating that the change has been reflected in the store load.
			//
			// TODO: there was a previous bug where these changes were not being
			// removed now, and were being removed later when the load adjustment
			// incorporated them. Fixing this has introduced improved
			// example_skewed_cpu_even_ranges_mma in that it converges faster, but
			// introduced more thrashing in
			// example_skewed_cpu_even_ranges_mma_and_queues. I suspect the latter
			// is because MMA is acting faster to undo the effects of the changes
			// made by the replicate and lease queues.
			cs.pendingChangeEnacted(change.ChangeID, now)
		}
		// Re-apply the remaining changes.
		for _, change := range remainingChanges {
			cs.applyReplicaChange(change.ReplicaChange)
		}
		normSpanConfig, err := makeNormalizedSpanConfig(&rangeMsg.Conf, cs.constraintMatcher.interner)
		if err != nil {
			// TODO(kvoli): Should we log as a warning here, or return further back out?
			panic(err)
		}
		rs.conf = normSpanConfig
		// NB: Always recompute the analyzed range constraints for any range,
		// assuming the leaseholder wouldn't have sent the message if there was no
		// change.
		rs.constraints = nil
	}
	localss := cs.stores[msg.StoreID]
	cs.meansMemo.clear()
	clusterMeans := cs.meansMemo.getMeans(nil)
	for _, ss := range cs.stores {
		topk := ss.adjusted.topKRanges[msg.StoreID]
		if topk == nil {
			topk = &topKReplicas{k: numTopKReplicas}
			ss.adjusted.topKRanges[msg.StoreID] = topk
		}
		topk.startInit()
		sls := cs.computeLoadSummary(ss.StoreID, &clusterMeans.storeLoad, &clusterMeans.nodeLoad)
		if ss.StoreID == localss.StoreID {
			topk.dim = CPURate
		} else {
			topk.dim = WriteBandwidth
		}
		if sls.highDiskSpaceUtilization {
			topk.dim = ByteSize
		} else if sls.sls > loadNoChange {
			// If multiple dimensions are contributing the same loadSummary, we will pick
			// CPURate before WriteBandwidth before ByteSize.
			for i := range sls.dimSummary {
				if sls.dimSummary[i] == sls.sls {
					topk.dim = LoadDimension(i)
					break
				}
			}
		}
		// TODO: setting a threshold such that only ranges > some threshold of the
		// store's load in the top-k dimension are included in the top-k. We
		// should actually be using the min of this threshold and the n-th ranked
		// load (across all ranges) per dimension reported by the store in
		// StoreDescriptor, where say n is 50 (since it is possible that the store
		// has a massive range that consumes 50% of the load, and another 100
		// ranges that consume 0.5% each, and the only way to restore health is to
		// shed those 100 ranges).
		const (
			// minLeaseLoadFraction is the minimum fraction of the local store's load a
			// lease must contribute, in order to consider it worthwhile rebalancing when
			// overfull.
			minLeaseLoadFraction = 0.005
			// minReplicaLoadFraction is the minimum fraction of the local store's load a
			// replica (lease included) must contribute, in order to consider it
			// worthwhile rebalancing when overfull.
			minReplicaLoadFraction = 0.02
		)
		fraction := minReplicaLoadFraction
		if ss.StoreID == msg.StoreID && topk.dim == CPURate {
			// We are assuming we will be able to shed leases, but if we can't we
			// will start shedding replicas, so this is just a heuristic.
			fraction = minLeaseLoadFraction
		}
		topk.threshold = LoadValue(float64(ss.adjusted.load[topk.dim]) * fraction)
	}
	// TODO: replica is already adjusted for some ongoing changes, which may be
	// undone. So if s10 is a replica for range r1 whose leaseholder is the
	// local store s1 that is trying to transfer the lease away, s10 will not
	// see r1 below.
	for rangeID, state := range localss.adjusted.replicas {
		if !state.IsLeaseholder {
			// We may have transferred the lease away previously but still have a
			// replica. We don't want to add this replica to the topKReplicas as it
			// controls which ranges are eligible to be shed. When no longer the
			// leaseholder, we cannot shed a replica or lease.
			continue
		}
		rs := cs.ranges[rangeID]
		// TODO: replicas is also already adjusted.
		for _, replica := range rs.replicas {
			typ := replica.ReplicaState.ReplicaType.ReplicaType
			if isVoter(typ) || isNonVoter(typ) {
				ss := cs.stores[replica.StoreID]
				topk := ss.adjusted.topKRanges[msg.StoreID]
				switch topk.dim {
				case CPURate:
					l := rs.load.Load[CPURate]
					if !replica.ReplicaState.IsLeaseholder {
						l = rs.load.RaftCPU
					}
					topk.addReplica(rangeID, l)
				case WriteBandwidth:
					topk.addReplica(rangeID, rs.load.Load[WriteBandwidth])
				case ByteSize:
					topk.addReplica(rangeID, rs.load.Load[ByteSize])
				}
			}
		}
	}
	for _, ss := range cs.stores {
		topk := ss.adjusted.topKRanges[msg.StoreID]
		topk.doneInit()
	}

}

// If the pending change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingChangeGCDuration = 5 * time.Minute

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges(now time.Time) {
	gcBeforeTime := now.Add(-pendingChangeGCDuration)
	var removeChangeIds []ChangeID
	for _, pendingChange := range cs.pendingChanges {
		if !pendingChange.startTime.After(gcBeforeTime) {
			removeChangeIds = append(removeChangeIds, pendingChange.ChangeID)
		}
	}
	for _, rmChange := range removeChangeIds {
		cs.undoPendingChange(rmChange)
	}
}

func (cs *clusterState) pendingChangeEnacted(cid ChangeID, enactedAt time.Time) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		panic(fmt.Sprintf("change %v not found", cid))
	}
	change.enactedAtTime = enactedAt
	rs, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}

	rs.removePendingChangeTracking(change.ChangeID)
	delete(cs.pendingChanges, change.ChangeID)
}

func (cs *clusterState) markPendingChangeEnacted(cid ChangeID, enactedAt time.Time) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		panic(fmt.Sprintf("change %v not found", cid))
	}
	change.enactedAtTime = enactedAt
}

// undoPendingChange reverses the change with ID cid.
func (cs *clusterState) undoPendingChange(cid ChangeID) {
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
	// Undo the change delta as well as the replica change and remove the pending
	// change from all tracking (range, store, cluster).
	cs.undoReplicaChange(change.ReplicaChange)
	rs.removePendingChangeTracking(cid)
	delete(cs.stores[change.target.StoreID].adjusted.loadPendingChanges, change.ChangeID)
	delete(cs.pendingChanges, change.ChangeID)
}

// createPendingChanges takes a set of changes applies the changes as pending.
// The application updates the adjusted load, tracked pending changes and
// changeID to reflect the pending application.
func (cs *clusterState) createPendingChanges(changes ...ReplicaChange) []*pendingReplicaChange {
	var pendingChanges []*pendingReplicaChange
	now := cs.ts.Now()

	for _, change := range changes {
		cs.applyReplicaChange(change)
		cs.changeSeqGen++
		cid := cs.changeSeqGen
		pendingChange := &pendingReplicaChange{
			ChangeID:      cid,
			ReplicaChange: change,
			startTime:     now,
			enactedAtTime: time.Time{},
		}
		storeState := cs.stores[change.target.StoreID]
		rangeState := cs.ranges[change.rangeID]
		cs.pendingChanges[cid] = pendingChange
		storeState.adjusted.loadPendingChanges[cid] = pendingChange
		rangeState.pendingChanges = append(rangeState.pendingChanges, pendingChange)
		pendingChanges = append(pendingChanges, pendingChange)
	}
	return pendingChanges
}

func (cs *clusterState) applyReplicaChange(change ReplicaChange) {
	storeState, ok := cs.stores[change.target.StoreID]
	if !ok {
		panic(fmt.Sprintf("store %v not found in cluster state", change.target.StoreID))
	}
	rangeState, ok := cs.ranges[change.rangeID]
	if !ok {
		// This is the first time encountering this range, we add it to the cluster
		// state.
		//
		// TODO(wenyihu6): Don't do anything if the range is not known.
		//
		// TODO(kvoli): Pass in the range descriptor to construct the range state
		// here. Currently, when the replica change is a removal this won't work
		// because the range state will not contain the replica being removed.
		rangeState = newRangeState()
		cs.ranges[change.rangeID] = rangeState
	}

	if change.isRemoval() {
		delete(storeState.adjusted.replicas, change.rangeID)
		rangeState.removeReplica(change.target.StoreID)
	} else if change.isAddition() {
		pendingRepl := StoreIDAndReplicaState{
			StoreID: change.target.StoreID,
			ReplicaState: ReplicaState{
				ReplicaIDAndType: change.next,
			},
		}
		storeState.adjusted.replicas[change.rangeID] = pendingRepl.ReplicaState
		rangeState.setReplica(pendingRepl)
	} else if change.isUpdate() {
		replState := storeState.adjusted.replicas[change.rangeID]
		replState.ReplicaIDAndType = change.next
		storeState.adjusted.replicas[change.rangeID] = replState
		rangeState.setReplica(StoreIDAndReplicaState{
			StoreID:      change.target.StoreID,
			ReplicaState: replState,
		})
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}
	cs.applyChangeLoadDelta(change)
}

func (cs *clusterState) undoReplicaChange(change ReplicaChange) {
	rangeState := cs.ranges[change.rangeID]
	storeState := cs.stores[change.target.StoreID]
	if change.isRemoval() || change.isUpdate() {
		prevRepl := StoreIDAndReplicaState{
			StoreID:      change.target.StoreID,
			ReplicaState: change.prev,
		}
		rangeState.setReplica(prevRepl)
		storeState.adjusted.replicas[change.rangeID] = prevRepl.ReplicaState
	} else if change.isAddition() {
		delete(storeState.adjusted.replicas, change.rangeID)
		rangeState.removeReplica(change.target.StoreID)
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
func (cs *clusterState) applyChangeLoadDelta(change ReplicaChange) {
	ss := cs.stores[change.target.StoreID]
	ss.adjusted.load.add(change.loadDelta)
	ss.adjusted.secondaryLoad.add(change.secondaryLoadDelta)
	ss.loadSeqNum++
	ss.computeMaxFractionPending()
	cs.nodes[ss.NodeID].adjustedCPU += change.loadDelta[CPURate]
}

// undoChangeLoadDelta subtracts the change load delta from the adjusted load
// of the store and node affected.
func (cs *clusterState) undoChangeLoadDelta(change ReplicaChange) {
	ss := cs.stores[change.target.StoreID]
	ss.adjusted.load.subtract(change.loadDelta)
	ss.adjusted.secondaryLoad.subtract(change.secondaryLoadDelta)
	ss.loadSeqNum++
	ss.computeMaxFractionPending()
	cs.nodes[ss.NodeID].adjustedCPU -= change.loadDelta[CPURate]
}

// setStore updates the store descriptor in the cluster state. If the store
// hasn't been seen before, it is also added to the cluster state.
//
// TODO: We currently assume that the locality and attributes associated with a
// store/node are fixed. This is a reasonable assumption for the locality,
// however it is not for the attributes.
func (cs *clusterState) setStore(desc roachpb.StoreDescriptor) {
	ns, ok := cs.nodes[desc.Node.NodeID]
	if !ok {
		// This is the first time seeing the associated node.
		ns = newNodeState(desc.Node.NodeID)
		cs.nodes[desc.Node.NodeID] = ns
	}
	ss, ok := cs.stores[desc.StoreID]
	if !ok {
		// This is the first time seeing this store.
		ss = newStoreState(desc.StoreID, desc.Node.NodeID)
		ss.localityTiers = cs.localityTierInterner.intern(desc.Locality())
		ss.overloadStartTime = cs.ts.Now()
		ss.overloadEndTime = cs.ts.Now()
		cs.constraintMatcher.setStore(desc)
		cs.stores[desc.StoreID] = ss
		ns.stores = append(ns.stores, desc.StoreID)
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

func (cs *clusterState) getNodeReportedLoad(nodeID roachpb.NodeID) *NodeLoad {
	if nodeState, ok := cs.nodes[nodeID]; ok {
		return &nodeState.NodeLoad
	}
	return nil
}

// canShedAndAddLoad returns true if the delta can be added to the target
// store and removed from the src store, such that the relative load summaries
// will not get worse.
//
// It does not change any state between the call and return.
//
// overloadDim, if not set to NumLoadDimensions, represents the dimension that
// is overloaded in the source.
func (cs *clusterState) canShedAndAddLoad(
	ctx context.Context,
	srcSS *storeState,
	targetSS *storeState,
	delta LoadVector,
	means *meansForStoreSet,
	onlyConsiderTargetCPUSummary bool,
	overloadedDim LoadDimension,
) bool {
	targetNS := cs.nodes[targetSS.NodeID]
	// Add the delta.
	deltaToAdd := loadVectorToAdd(delta)
	targetSS.adjusted.load.add(deltaToAdd)
	targetNS.adjustedCPU += deltaToAdd[CPURate]
	targetSLS := computeLoadSummary(targetSS, targetNS, &means.storeLoad, &means.nodeLoad)
	// Undo the addition.
	targetSS.adjusted.load.subtract(deltaToAdd)
	targetNS.adjustedCPU -= deltaToAdd[CPURate]

	// Remove the delta.
	srcNS := cs.nodes[srcSS.NodeID]
	srcSS.adjusted.load.subtract(delta)
	srcNS.adjustedCPU -= delta[CPURate]
	srcSLS := computeLoadSummary(srcSS, srcNS, &means.storeLoad, &means.nodeLoad)
	// Undo the removal.
	srcSS.adjusted.load.add(delta)
	srcNS.adjustedCPU += delta[CPURate]

	// If there is no pending change in targetSS, we are willing to tolerate
	// this move as long as targetSS is no worst than srcSS. Else we are
	// stricter and expect targetSS to be better than loadNoChange -- this will
	// delay making a potentially non-ideal choice of targetSS until it has no
	// pending changes.
	//
	// The target's aggregate summary or overload dimension summary must have
	// been < loadNoChange. And the source must have been > loadNoChange. It is
	// possible that both are overloadSlow in aggregate. We want to make sure
	// that this exchange doesn't make the target summaries worse than the
	// source's summaries, both in aggregate, and in the dimension being shed.
	var targetSummary loadSummary
	if onlyConsiderTargetCPUSummary {
		targetSummary = targetSLS.dimSummary[CPURate]
		if targetSummary < targetSLS.nls {
			targetSummary = targetSLS.nls
		}
	} else {
		targetSummary = targetSLS.sls
		if targetSummary < targetSLS.nls {
			targetSummary = targetSLS.nls
		}
	}
	overloadedDimPermitsChange := true
	if overloadedDim != NumLoadDimensions {
		overloadedDimPermitsChange =
			targetSLS.dimSummary[overloadedDim] <= srcSLS.dimSummary[overloadedDim]
	}
	canAddLoad := !targetSLS.highDiskSpaceUtilization && overloadedDimPermitsChange &&
		(targetSummary < loadNoChange ||
			(targetSLS.maxFractionPendingIncrease < epsilon &&
				targetSLS.maxFractionPendingDecrease < epsilon &&
				targetSLS.sls <= srcSLS.sls &&
				// NB: targetSLS.nls <= targetSLS.sls is not a typo, in that we are
				// comparing targetSLS with itself. The nls only captures node-level
				// CPU, so if a store that is overloaded wrt WriteBandwidth wants to
				// shed to a store that is overloaded wrt CPURate, we need to permit
				// that. However, the nls of the former will be less than the that of
				// the latter. By looking at the nls of the target here, we are making
				// sure that it is no worse than the sls of the target, since if it
				// is, the node is overloaded wrt CPU due to some other store on that
				// node, and we should be shedding that load first.
				overloadedDimPermitsChange && targetSLS.nls <= targetSLS.sls))
	log.Infof(ctx, "can add load to n%vs%v: %v targetSLS[%v] srcSLS[%v]",
		targetNS.NodeID, targetSS.StoreID, canAddLoad, targetSLS, srcSLS)
	return canAddLoad
}

func (cs *clusterState) computeLoadSummary(
	storeID roachpb.StoreID, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	ss := cs.stores[storeID]
	ns := cs.nodes[ss.NodeID]
	return computeLoadSummary(ss, ns, msl, mnl)
}

func computeLoadSummary(
	ss *storeState, ns *nodeState, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	sls := loadLow
	var highDiskSpaceUtil bool
	var dimSummary [NumLoadDimensions]loadSummary
	for i := range msl.load {
		// TODO(kvoli,sumeerbhola): Handle negative adjusted store/node loads.
		ls := loadSummaryForDimension(
			LoadDimension(i), ss.adjusted.load[i], ss.capacity[i], msl.load[i], msl.util[i])
		if ls > sls {
			sls = ls
		}
		dimSummary[i] = ls
		switch LoadDimension(i) {
		case ByteSize:
			highDiskSpaceUtil = highDiskSpaceUtilization(ss.adjusted.load[i], ss.capacity[i])
		}
	}
	nls := loadSummaryForDimension(CPURate, ns.adjustedCPU, ns.CapacityCPU, mnl.loadCPU, mnl.utilCPU)
	return storeLoadSummary{
		sls:                        sls,
		nls:                        nls,
		dimSummary:                 dimSummary,
		highDiskSpaceUtilization:   highDiskSpaceUtil,
		fd:                         ns.fdSummary,
		maxFractionPendingIncrease: ss.maxFractionPendingIncrease,
		maxFractionPendingDecrease: ss.maxFractionPendingDecrease,
		loadSeqNum:                 ss.loadSeqNum,
	}
}

// Avoid unused lint errors.
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
