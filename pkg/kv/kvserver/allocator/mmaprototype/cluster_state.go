// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

type ReplicaChangeType int

const (
	AddLease ReplicaChangeType = iota
	RemoveLease
	AddReplica
	RemoveReplica
)

func (s ReplicaChangeType) String() string {
	switch s {
	case AddLease:
		return "AddLease"
	case RemoveLease:
		return "RemoveLease"
	case AddReplica:
		return "AddReplica"
	case RemoveReplica:
		return "RemoveReplica"
	default:
		panic("unknown ReplicaChangeType")
	}
}

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
	//   prev.IsLeaseholder can be true or false, since we can transfer the
	//   lease as part of moving the replica.
	//
	// - prev.replicaID == noReplicaID && next.replicaID == unknownReplicaID:
	//   incoming replica, next.ReplicaType must be VOTER_FULL or NON_VOTER.
	//   next.IsLeaseholder can be true or false.
	//
	// - prev.replicaID >= 0 && next.replicaID >= 0: can be a change to
	//   IsLeaseholder, or ReplicaType. next.ReplicaType must be VOTER_FULL or
	//   NON_VOTER.
	prev              ReplicaState
	next              ReplicaIDAndType
	replicaChangeType ReplicaChangeType
}

func (rc ReplicaChange) String() string {
	return redact.StringWithoutMarkers(rc)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rc ReplicaChange) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%v type: %v target store %v (%v)->(%v)", rc.rangeID, rc.replicaChangeType, rc.target, rc.prev, rc.next)
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
		target:            removeTarget,
		rangeID:           rangeID,
		prev:              remove.ReplicaState,
		next:              remove.ReplicaIDAndType,
		replicaChangeType: RemoveLease,
	}
	addLease := ReplicaChange{
		target:            addTarget,
		rangeID:           rangeID,
		prev:              add.ReplicaState,
		next:              add.ReplicaIDAndType,
		replicaChangeType: AddLease,
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
// to the store addStoreID.
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
		next:              replicaState.ReplicaIDAndType,
		replicaChangeType: AddReplica,
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
// given.
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
		replicaChangeType: RemoveReplica,
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
func (prc PendingRangeChange) ReplicationChanges() kvpb.ReplicationChanges {
	if !prc.IsChangeReplicas() {
		panic("RangeChange is not a change replicas")
	}
	chgs := make([]kvpb.ReplicationChange, len(prc.pendingReplicaChanges))
	for i, c := range prc.pendingReplicaChanges {
		chgs[i].Target = c.target
		if c.prev.ReplicaID == noReplicaID {
			switch c.next.ReplicaType.ReplicaType {
			case roachpb.VOTER_FULL:
				chgs[i].ChangeType = roachpb.ADD_VOTER
			case roachpb.NON_VOTER:
				chgs[i].ChangeType = roachpb.ADD_NON_VOTER
			default:
				panic(errors.AssertionFailedf("unexpected replica type %s", c.next.ReplicaType.ReplicaType))
			}
		} else if c.next.ReplicaID == noReplicaID {
			switch c.prev.ReplicaType.ReplicaType {
			case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.VOTER_DEMOTING_LEARNER:
				chgs[i].ChangeType = roachpb.REMOVE_VOTER
			case roachpb.NON_VOTER, roachpb.LEARNER:
				chgs[i].ChangeType = roachpb.REMOVE_NON_VOTER
			default:
				panic(errors.AssertionFailedf("unexpected replica type %s", c.prev.ReplicaType.ReplicaType))
			}
		} else {
			panic("todo: support for promotion/demotion changes")
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

// TODO: we need to rethink how to model pending changes. pendingReplicaChange
// was developed to allow us to have a simple way of representing all changes,
// by composing one or more pendingReplicaChanges (each of which only touches
// one store). For replica moves and lease transfers this results in a pair of
// changes. For replica moves the pair is an addition at one store and a
// removal at another store. These can be enacted separately, and noticed as
// enacted separately, so having them become non-pending separately is useful.
//
// Lease transfers create a hazard with this modeling approach which can
// result in no leaseholder or two leaseholders in the internal state of MMA.
// As a reminder, we don't care if internal state of MMA is out-of-sync with
// the real world: the StoreLeaseholderMsgs will eventually correct the state.
// But we care about internal consistency. Also, as a reminder we want a
// single rangeState for a range, regardless of the number of local stores.
// This helps in having a single reality inside MMA, even if it is currently
// incorrect.
//
// Example of the hazard: Consider a node with two stores s1 and s2, and range
// r1, where s1 is the leaseholder. Say we decide to move the replica from s1
// to s2 -- this is permitted since they are never in the same quorum. As part
// of this move, the lease will also be transferred from s1 to s2. We
// currently produce two pendingReplicaChanges, c1 removes the replica and
// lease from s1, and c2 adds the replica and lease to s2. Say we receive a
// StoreLeaseholderMsg from s2, which says that it is the leaseholder and has
// a replica. This will cause us to mark c2 as done (no longer pending). But
// we can't mark c1 as done since both the lease loss and replica loss are in
// a single change. And then something causes c2 to be undone, e.g., GC or a
// spurious error on the transfer path which causes the enacter to say that
// the change was not successful. Now we will undo c1 and have s1 also as the
// leaseholder, which is incorrect.
//
// To avoid this hazard, we need to model this as 3 pending changes, c1 for
// removing the replica from s1, c2 for adding the replica to s2, and c3 for
// transferring the lease from s1 to s2. c3 has to model load changes to two
// stores. Having a single change c3 model the lease transfer means it is
// either consider enacted or failed as a whole, so there will be exactly one
// leaseholder.
//
// TODO(wenyi): For the prototype we are keeping this old modeling approach,
// since we think it will suffice for single store roachtests. We have
// discussed checking that a pending change is consistent with the current
// rangeState. In the hazard example above, when c2 is enacted, because of a
// StoreLeaseholderMsg from s2, we will check that c1 is still valid for the
// current rangeState. From a replica loss perspective, it is valid, but from
// a lease loss perspective, it is no longer valid since the lease is already
// lost. We may have to relax the consistency checking to only check that the
// replica loss is valid.

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
	StoreAttributesAndLocality
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
		// This is consistent with the union of state in clusterState.ranges,
		// filtered for replicas that are on this store.
		//
		// NB: this can include LEARNER and VOTER_DEMOTING_LEARNER replicas.
		replicas map[roachpb.RangeID]ReplicaState
		// topKRanges along some load dimension. If the store is overloaded along
		// one resource dimension, that is the dimension chosen when picking the
		// top-k.
		//
		// It includes ranges whose replicas are being removed via pending
		// changes, or lease transfers. That is, it does not account for pending
		// or enacted changes made since the last time top-k was computed.
		//
		// The key in this map is a local store-id.
		//
		// NB: this does not include LEARNER and VOTER_DEMOTING_LEARNER replicas.
		//
		// We may decide to keep this top-k up-to-date incrementally instead of
		// recomputing it from scratch on each StoreLeaseholderMsg. Since
		// StoreLeaseholderMsg is incremental about the ranges it reports, that
		// may provide a building block for the incremental computation.
		//
		// Example:
		// Assume the local node has two stores, s1 and s2.
		// - s1 has a range r100 with replicas {s1,s3,s4} and it is leaseholder.
		// - s2 has a range r200 with replicas {s2,s4,s5}, and it is the leaseholder.
		// - s1 and s2 have a range r300 with replicas {s1,s2,s5}, but neither is the leaseholder.
		// - s1 and s2 have a range r400 with replicas {s1,s2,s5}, and s2 holds the lease.
		//
		// Then the mmma will maintain storeStates for s1-s5 (ss1-ss5), but in each
		// of them, the topKRanges will only consider the ranges for which a local
		// store holds the lease. In the example above, we get:
		//
		// ss1.topKRanges = {
		//   s1: topK(r100)
		//   s2: topK(r400)
		// }
		// ss2.topKRanges = {
		//   s2: topK(r200,r400)
		// }
		// ss3.topKRanges = {
		//   s1: topK(r100)
		// }
		// ss4.topKRanges = {
		//   s1: topK(r100)
		//   s2: topK(r200)
		// }
		// ss5.topKRanges = {
		//   s2: topK(r200,r400)
		// }
		//
		// Note that the sort order of each topKRanges is determined by the type of
		// load that is most important for the store to shed, and that ranges with
		// only minimal contribution (relative to the mean) to the load are not even
		// considered for inclusion in topKRanges.
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
		if ss.reportedLoad[i] == ss.adjusted.load[i] && ss.reportedLoad[i] == 0 {
			// Avoid setting ss.maxFractionPendingIncrease and
			// ss.maxFractionPendingDecrease to 1000 when the reported load and
			// adjusted load are both 0 since some dimension is expected to have zero
			// (e.g. write bandwidth during read-only workloads).
			continue
		}
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

func newStoreState() *storeState {
	ss := &storeState{}
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
	w.Printf("s%v:%v voterIsLagging:%v", s.StoreID, s.ReplicaState.ReplicaIDAndType, s.ReplicaState.VoterIsLagging)
}

// rangeState is periodically updated based on reporting by the leaseholder.
type rangeState struct {
	// localRangOwner is used for rangeState GC. The StoreID mentioned here is
	// the local store that last included that range in its StoreLeaseholderMsg,
	// and therefore is considered the "owner" of the rangeState.
	//
	// NB: we can't use the current leaseholder to decide when to GC, since when
	// the lease is being transferred from local store s1 to remote store s3,
	// the rangeState will already state s3 as the leaseholder (either because
	// the lease transfer was considered successful, or when it is pending).
	//
	// When transferring a replica (and lease) from local store s1 to local
	// store s2, the localRangeOwner will initially be s1. If a
	// StoreLeaseholderMsg from s2 arrives with the range before the
	// StoreLeaseholderMsg from s1 without the range, the localRangeOwner will
	// be updated to s2. If the opposite happens, the rangeState will be garbage
	// collected, and later the StoreLeaseholderMsg from s2 will recreate it.
	localRangeOwner roachpb.StoreID
	// replicas is the adjusted replicas. It is always consistent with
	// the storeState.adjusted.replicas in the corresponding stores.
	//
	// NB: Because pendingReplicaChange individually represents the change to a
	// single store, a lease transfer or replica movement is represented using
	// two pendingReplicaChanges. This is convenient since it allows composition
	// of the simple pendingReplicaChange to represent various changes. And it
	// allows for the enactment to be decoupled e.g. a new replica is added
	// during the joint config and starts experiencing load before the old
	// replica is removed. One unintended side-effect of this data-structural
	// choice is that we could undo the effect of a lease transfer change on the
	// lease shedding store before undoing the effect of the lease addition on
	// the new leaseholder store (e.g. when doing time based GC -- though
	// currently time-based GC is done for the whole clusterState so unclear if
	// this is really possible). This allows multiple leaseholders, or no
	// leaseholder, but only when there are still some pendingReplicaChanges for
	// the rangeState. We consider this harmless for the prototype since we will
	// not make changes to a range that has pending changes. However, we cannot
	// continue with this weakness in multi-store setting -- see the hazard
	// discussed where pendingReplicaChange is declared, and how we plan to fix
	// it.
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

	// lastFailedChange is the latest time at which a change to the range needed
	// to be undone. It is used to backoff from making another change.
	//
	// One case where this is useful is when the rangeState does not know about
	// an ongoing change started by some other component. In that case, mmaprototype can
	// produce another change, which the callee is unable to enact because there
	// is an ongoing change. We will record that failure time here, and wait for
	// some time interval before trying again. During that time, it is likely
	// that the change we did not know about has been enacted.
	//
	// One may wonder how such unknown changes can happen, given that other
	// components call mmaprototype.Allocator.RegisterExternalChanges. One example is
	// when MMA does not currently know about a range. Say the lease gets
	// transferred to the local store, but MMA has not yet been called with a
	// StoreLeaseholderMsg, but replicateQueue already knows about this lease,
	// and decides to initiate a transfer of replicas between two remote stores
	// (to equalize replica counts). When mmaprototype.Allocator.RegisterExternalChanges
	// is called, there is no record of this range in MMA (since it wasn't the
	// leaseholder), and the change is ignored. When the next
	// StoreLeaseholderMsg is provided to MMA it now knows about the range, and
	// when asked to do rebalancing, it may choose to initiate a change to the
	// range, while the other unknown change is ongoing. This scenario occurred
	// in an asim test, which timed out (the timeout happened because asim does
	// not advance time if a change is immediately invalid, and which can result
	// in the mmaprototype store rebalancer being stuck in a tight loop with the
	// simulation time not advancing).
	lastFailedChange time.Time
	// TODO(sumeer): populate and use.
	diversityIncreaseLastFailedAttempt time.Time
}

func newRangeState(localRangeOwner roachpb.StoreID) *rangeState {
	return &rangeState{
		replicas:        []StoreIDAndReplicaState{},
		pendingChanges:  []*pendingReplicaChange{},
		localRangeOwner: localRangeOwner,
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

func replicaSetIsValid(replicas []StoreIDAndReplicaState) (bool, string) {
	hasSeenLeaseholder := false
	for _, repl := range replicas {
		if repl.ReplicaState.IsLeaseholder {
			if hasSeenLeaseholder {
				// More than one leaseholder.
				return false, "more than one leaseholder"
			}
			hasSeenLeaseholder = true
		}
	}
	return hasSeenLeaseholder, "no leaseholder"
}

func (rs *rangeState) removePendingChangeTracking(changeID ChangeID) {
	n := len(rs.pendingChanges)
	found := false
	for i := 0; i < n; i++ {
		if rs.pendingChanges[i].ChangeID == changeID {
			rs.pendingChanges[i], rs.pendingChanges[n-1] = rs.pendingChanges[n-1], rs.pendingChanges[i]
			rs.pendingChanges = rs.pendingChanges[:n-1]
			found = true
			break
		}
	}
	if !found {
		panic(fmt.Sprintf("pending change %v not found in rangeState %v", changeID, rs.pendingChanges))
	}
}

// clusterState is the state of the cluster known to the allocator, including
// adjustments based on pending changes. It does not include additional
// indexing needed for constraint matching, or for tracking ranges that may
// need attention etc. (those happen at a higher layer).
//
// We maintain one clusterState per node, even in multi-store settings. This
// allows us to allow for coordination between the different local store
// rebalancers, and queues making changes. There are production clusters where
// the number of stores is an order of magnitude larger than the number of
// nodes, and even though we have rebalancing components per store, we want to
// reduce the sub-optimal decisions they make -- having a single clusterState
// is important for that.
type clusterState struct {
	ts     timeutil.TimeSource
	nodes  map[roachpb.NodeID]*nodeState
	stores map[roachpb.StoreID]*storeState
	// A range is present in the ranges map if any of the local stores is the
	// leaseholder for that range. If a local store is shedding the lease for
	// the range, this map will continue to contain that range until that change
	// is enacted according to a StoreLeaseholderMsg. However, rangeState
	// internally maintains adjusted state, along with the pending changes, so
	// that adjustment will already be reflected in that adjusted state.
	//
	// Of course, if the lease shedding was done as part of moving the replica
	// from one local store to another local store, then the rangeState will
	// just change hands, and continue to be in the map if the
	// StoreLeaseholderMsg of the new (local) leaseholder store is received with
	// the enacted change before the StoreLeaseholderMsg of the old (local)
	// leaseholder store is received with the enacted change (if vice versa, the
	// range will be removed, and later a new rangeState will be created).
	//
	// Maintaining a single rangeState for a RangeID, instead of one per local
	// store which is the leaseholder, allows us to ensure there is one view of
	// the range across clusterState. One complication around this is that we
	// also need to gc from ranges, and having a view per local store that is
	// the leaseholder would allow for more efficient gc when receiving a
	// StoreLeaseholderMsg. For now, we avoid denormalizing and taking the
	// efficiency hit.
	//
	// TODO(sumeer): we use StoreLeaseholderMsg as the fully authoritative
	// source of truth. But clusterState.pendingChangesEnacted also marks things
	// as enacted, but doesn't remove ranges from this map. This could be made
	// cleaner.
	ranges map[roachpb.RangeID]*rangeState

	scratchRangeMap map[roachpb.RangeID]struct{}

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
		scratchRangeMap:      map[roachpb.RangeID]struct{}{},
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
		log.VInfof(ctx, 2, "s%d not-pending %v", storeMsg.StoreID, change)
		delete(ss.adjusted.loadPendingChanges, change.ChangeID)
	}

	for _, change := range ss.adjusted.loadPendingChanges {
		// The pending change hasn't been reported as done, re-apply the load
		// delta to the adjusted load and include it in the new adjusted load
		// replicas.
		cs.applyChangeLoadDelta(change.ReplicaChange)
	}
	log.VInfof(ctx, 2, "processStoreLoadMsg for store s%v: %v",
		storeMsg.StoreID, ss.adjusted.load)
}

func (cs *clusterState) processStoreLeaseholderMsg(
	ctx context.Context, msg *StoreLeaseholderMsg, metrics *MMAMetrics,
) {
	cs.processStoreLeaseholderMsgInternal(ctx, msg, numTopKReplicas, metrics)
}

func (cs *clusterState) processStoreLeaseholderMsgInternal(
	ctx context.Context, msg *StoreLeaseholderMsg, numTopKReplicas int, metrics *MMAMetrics,
) {
	now := cs.ts.Now()
	cs.gcPendingChanges(now)

	clear(cs.scratchRangeMap)
	for _, rangeMsg := range msg.Ranges {
		cs.scratchRangeMap[rangeMsg.RangeID] = struct{}{}
		rs, ok := cs.ranges[rangeMsg.RangeID]
		if !ok {
			// This is the first time we've seen this range.
			if !rangeMsg.Populated {
				panic(errors.AssertionFailedf("rangeMsg for new range r%v is not populated", rangeMsg.RangeID))
			}
			rs = newRangeState(msg.StoreID)
			cs.ranges[rangeMsg.RangeID] = rs
		} else if rs.localRangeOwner != msg.StoreID {
			rs.localRangeOwner = msg.StoreID
		}
		if !rangeMsg.Populated {
			// When there are no pending changes, confirm that the membership state
			// is consistent. If not, fall through and make it consistent. We have
			// seen an example where AdjustPendingChangesDisposition lied about
			// being successful, and have not been able to find the root cause. So
			// we'd rather force eventual consistency here.
			if len(rs.pendingChanges) > 0 {
				continue
			}
			mayHaveDiverged := false
			if len(rs.replicas) == len(rangeMsg.Replicas) {
				for i := range rs.replicas {
					// Since we stuff rangeMsg.Replicas directly into the
					// rangeState.replicas slice, in the common case we expect both of
					// them to have the same replica at the same position. If they
					// don't, they have either diverged, or there have been adjustments
					// made n rangeState.replicas that have changed the ordering. The
					// latter may be a false positive, but we don't mind the small
					// amount of additional work below.
					if rs.replicas[i] != rangeMsg.Replicas[i] {
						mayHaveDiverged = true
						break
					}
				}
			} else {
				mayHaveDiverged = true
			}
			if !mayHaveDiverged {
				continue
			}
		}
		// Set the range state and store state to match the range message state
		// initially. The pending changes which are not enacted in the range
		// message are handled and added back below.
		if rangeMsg.Populated {
			rs.load = rangeMsg.RangeLoad
		}
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
		var remainingReplicaChanges []ReplicaChange
		for _, change := range rs.pendingChanges {
			ss := cs.stores[change.target.StoreID]
			adjustedReplica, ok := ss.adjusted.replicas[rangeMsg.RangeID]
			if !ok {
				adjustedReplica.ReplicaID = noReplicaID
			}
			if adjustedReplica.subsumesChange(change.prev.ReplicaIDAndType, change.next) {
				// The change has been enacted according to the leaseholder.
				enactedChanges = append(enactedChanges, change)
			} else {
				remainingChanges = append(remainingChanges, change)
				remainingReplicaChanges = append(remainingReplicaChanges, change.ReplicaChange)
			}
		}
		// rs.pendingChanges is the union of remainingChanges and enactedChanges.
		// These changes are also in cs.pendingChanges.
		if len(enactedChanges) > 0 {
			log.Infof(ctx, "enactedChanges %v", enactedChanges)
		}
		for _, change := range enactedChanges {
			// Mark the change as enacted. Enacting a change does not remove the
			// corresponding load adjustments. The store load message will do that,
			// indicating that the change has been reflected in the store load.
			//
			// There was a previous bug where these changes were not being
			// removed now, and were being removed later when the load adjustment
			// incorporated them. Fixing this has introduced improved
			// example_skewed_cpu_even_ranges_mma in that it converges faster, but
			// introduced more thrashing in
			// example_skewed_cpu_even_ranges_mma_and_queues. I suspect the latter
			// is because MMA is acting faster to undo the effects of the changes
			// made by the replicate and lease queues.
			cs.pendingChangeEnacted(change.ChangeID, now, true)
		}
		// rs.pendingChanges only contains remainingChanges, and these are also in
		// cs.pendingChanges and storeState's loadPendingChanges. Their load
		// effect is also incorporated into the storeStates, but not in the range
		// membership (since we undid that above).
		if len(remainingChanges) > 0 {
			log.Infof(ctx, "remainingChanges %v", remainingChanges)
			// Temporarily set the rs.pendingChanges to nil, since
			// preCheckOnApplyReplicaChanges returns false if there are any pending
			// changes, and these are the changes that are pending. This is hacky
			// and should be cleaned up.
			rs.pendingChanges = nil
			valid, reason := cs.preCheckOnApplyReplicaChanges(remainingReplicaChanges)
			// Restore it.
			rs.pendingChanges = remainingChanges
			if valid {
				// Re-apply the remaining changes. Note that the load change was not
				// undone above, so we pass !applyLoadChange, to avoid applying it
				// again. Also note that applyReplicaChange does not add to the various
				// pendingChanges data-structures, which is what we want here since
				// these changes are already in those data-structures.
				for _, change := range remainingChanges {
					cs.applyReplicaChange(change.ReplicaChange, false)
				}
			} else {
				// The current state provided by the leaseholder does not permit these
				// changes, so we need to drop them. This should be rare, but can happen
				// if the leaseholder executed a change that MMA was completely unaware
				// of.
				log.Infof(ctx, "remainingChanges %v are no longer valid due to %v",
					remainingChanges, reason)
				if metrics != nil {
					metrics.DroppedDueToStateInconsistency.Inc(1)
				}
				// We did not undo the load change above, or remove it from the various
				// pendingChanges data-structures. We do those things now.
				for _, change := range remainingChanges {
					rs.removePendingChangeTracking(change.ChangeID)
					delete(cs.stores[change.target.StoreID].adjusted.loadPendingChanges, change.ChangeID)
					delete(cs.pendingChanges, change.ChangeID)
					cs.undoChangeLoadDelta(change.ReplicaChange)
				}
				if n := len(rs.pendingChanges); n > 0 {
					panic(errors.AssertionFailedf("expected no pending changes but found %d", n))
				}
			}
		}
		if rangeMsg.Populated {
			normSpanConfig, err := makeNormalizedSpanConfig(&rangeMsg.Conf, cs.constraintMatcher.interner)
			if err != nil {
				// TODO(kvoli): Should we log as a warning here, or return further back out?
				panic(err)
			}
			rs.conf = normSpanConfig
		}
		// NB: Always recompute the analyzed range constraints for any range,
		// assuming the leaseholder wouldn't have sent the message if there was no
		// change, or we noticed a divergence in membership above and fell through
		// here.
		rs.constraints = nil
	}
	// Remove ranges for which this is the localRangeOwner, but for which it is
	// no longer the leaseholder.
	for r, rs := range cs.ranges {
		_, ok := cs.scratchRangeMap[r]
		if ok {
			continue
		}
		// Not the leaseholder for this range. Consider removing it.
		//
		// In a multi-store setting this is inefficient, since we are iterating
		// over all ranges for which any local store is the owner. We could be
		// more efficient by maintaining an additional
		// map[roachpb.StoreID]map[roachpb.RangeID]*rangeState, where the first
		// map is keyed by a local StoreID. But we will only do this if we find
		// the efficiency gains are worth it.
		if rs.localRangeOwner != msg.StoreID {
			continue
		}
		// Since this range is going away, mark all the pending changes as
		// enacted. This will allow the load adjustments to also be garbage
		// collected in the future.
		for _, change := range rs.pendingChanges {
			cs.pendingChangeEnacted(change.ChangeID, now, true)
		}
		// Remove from the storeStates.
		for _, replica := range rs.replicas {
			ss := cs.stores[replica.StoreID]
			if ss == nil {
				panic(fmt.Sprintf("store %d not found stores=%v", replica.StoreID, cs.stores))
			}
			delete(cs.stores[replica.StoreID].adjusted.replicas, r)
		}
		delete(cs.ranges, r)
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
		// Setting a threshold such that only ranges > some threshold of the
		// store's load in the top-k dimension are included in the top-k. These
		// values are copied from store_rebalancer.go:
		// kvserver.{minLeaseLoadFraction, minReplicaLoadFraction}, which are 0.5%
		// and 2% respectively.
		//
		// Arguably, this is not a reasonable way to exclude ranges, and one could
		// argue that one should not exclude any ranges. The difficulty is that in
		// a N node cluster, there are N allocators, each with a partial view of
		// the cluster (based on what each node's stores are leaseholders for). If
		// we don't exclude any ranges, and an allocator at n1 sees that a remote
		// store s3 has high WriteBandwidth, it can try to move a replica of range
		// r1 from s3 to some other store. But it is possible that range r1, even
		// though it has the highest WriteBandwidth of the ranges n1 knows about,
		// is not significant compared to other ranges on s3, and having some
		// other allocator move those other ranges is preferable. Further
		// complicating this is that constraints may prevent those other bigger
		// (from a WriteBandwidth perspective) ranges to be moved, so eventually
		// we may have to fall back to shedding the smaller ranges.
		//
		// One way to solve this problem is to include the range count in
		// StoreLoadMsg, so that each allocator can compute the mean range load
		// along the overloaded dimension for a store. Then set a threshold that
		// is a multiple of the mean, and gradually ratchet it down (akin to how
		// we use time and the various *GraceDurations to adjust ignoreLevel).
		//
		// Do we really need to solve this problem? Have we had any incidents with
		// the current store rebalancer that can be attributed to these
		// thresholds?
		//
		// Old comment: We should actually be using the min of this threshold and
		// the n-th ranked load (across all ranges) per dimension reported by the
		// store, where say n is 50 (since it is possible that the store has a
		// massive range that consumes 50% of the load, and another 100 ranges
		// that consume 0.5% each, and the only way to restore health is to shed
		// those 100 ranges).
		const (
			// minLeaseLoadFraction is the minimum fraction of the local store's load a
			// lease must contribute, in order to consider it worthwhile rebalancing when
			// overfull.
			//
			// TODO(tbg): I set this to zero following the discussion in [1] about
			// experiment [2].
			//
			// [1]: https://cockroachlabs.slack.com/archives/C048HDZJSAY/p1751032541196659?thread_ts=1751026215.841039&cid=C048HDZJSAY
			// [2]: https://docs.google.com/document/d/1F35E9pOhtMlGAhKeidTyxRPaOpD3oP3DmT3cVvqVbhE/edit?tab=t.0
			//
			// TODO(sumeer): I set these back to the original values, after adding
			// the meanLoad logic below. We need to rerun the roachtest to see if
			// this suffices.
			minLeaseLoadFraction = 0.005
			// minReplicaLoadFraction is the minimum fraction of the local store's
			// load a replica (lease included) must contribute, in order to consider
			// it worthwhile rebalancing when overfull.
			minReplicaLoadFraction = 0.02
		)
		fraction := minReplicaLoadFraction
		if ss.StoreID == msg.StoreID && topk.dim == CPURate {
			// We are assuming we will be able to shed leases, but if we can't we
			// will start shedding replicas, so this is just a heuristic.
			fraction = minLeaseLoadFraction
		}
		threshold := LoadValue(float64(ss.adjusted.load[topk.dim]) * fraction)
		if ss.reportedSecondaryLoad[ReplicaCount] > 0 {
			// Allow all ranges above 90% of the mean. This is quite arbitrary.
			meanLoad := (ss.adjusted.load[topk.dim] * 9) / (ss.reportedSecondaryLoad[ReplicaCount] * 10)
			threshold = min(meanLoad, threshold)
		}
		topk.threshold = threshold
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
			//
			// NB: this should only happen when the lease transfer has begun, but
			// not yet completed. Once completed, this store will not have this
			// range in its set of replicas since there can be only one replica of a
			// range on a node, and clusterState only maintains ranges for which
			// some local store is a leaseholder.
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
					topk.addReplica(ctx, rangeID, l, replica.StoreID, msg.StoreID)
				case WriteBandwidth:
					topk.addReplica(ctx, rangeID, rs.load.Load[WriteBandwidth], replica.StoreID, msg.StoreID)
				case ByteSize:
					topk.addReplica(ctx, rangeID, rs.load.Load[ByteSize], replica.StoreID, msg.StoreID)
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
	var replicaChanges []ReplicaChange
	for _, pendingChange := range cs.pendingChanges {
		if !pendingChange.startTime.After(gcBeforeTime) {
			removeChangeIds = append(removeChangeIds, pendingChange.ChangeID)
			replicaChanges = append(replicaChanges, pendingChange.ReplicaChange)
		}
	}
	if len(replicaChanges) > 0 {
		if valid, reason := cs.preCheckOnUndoReplicaChanges(replicaChanges); !valid {
			log.Infof(context.Background(), "did not undo change %v: due to %v", removeChangeIds, reason)
			return
		}
		for _, rmChange := range removeChangeIds {
			cs.undoPendingChange(rmChange, true)
		}
	}
}

func (cs *clusterState) pendingChangeEnacted(cid ChangeID, enactedAt time.Time, requireFound bool) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		if requireFound {
			panic(fmt.Sprintf("change %v not found %v", cid, printMapPendingChanges(cs.pendingChanges)))
		} else {
			return
		}
	}
	change.enactedAtTime = enactedAt
	rs, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}

	rs.removePendingChangeTracking(change.ChangeID)
	delete(cs.pendingChanges, change.ChangeID)
}

// undoPendingChange reverses the change with ID cid.
func (cs *clusterState) undoPendingChange(cid ChangeID, requireFound bool) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		if requireFound {
			panic(fmt.Sprintf("change %v not found %v", cid, printMapPendingChanges(cs.pendingChanges)))
		} else {
			return
		}
	}
	rs, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}
	// Wipe the analyzed constraints, as the range has changed.
	rs.constraints = nil
	rs.lastFailedChange = cs.ts.Now()
	// Undo the change delta as well as the replica change and remove the pending
	// change from all tracking (range, store, cluster).
	cs.undoReplicaChange(change.ReplicaChange)
	rs.removePendingChangeTracking(cid)
	delete(cs.stores[change.target.StoreID].adjusted.loadPendingChanges, change.ChangeID)
	delete(cs.pendingChanges, change.ChangeID)
}

func printMapPendingChanges(changes map[ChangeID]*pendingReplicaChange) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "pending(%d)", len(changes))
	for k, v := range changes {
		fmt.Fprintf(&buf, "\nchange-id=%d store-id=%v node-id=%v range-id=%v load-delta=%v start=%v",
			k, v.target.StoreID, v.target.NodeID, v.rangeID,
			v.loadDelta, v.startTime,
		)
		if !(v.enactedAtTime == time.Time{}) {
			fmt.Fprintf(&buf, " enacted=%v",
				v.enactedAtTime)
		}
	}
	return buf.String()
}

//lint:ignore U1000 used in tests
func printPendingChanges(changes []*pendingReplicaChange) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "pending(%d)", len(changes))
	for _, change := range changes {
		fmt.Fprintf(&buf, "\nchange-id=%d store-id=%v node-id=%v range-id=%v load-delta=%v start=%v",
			change.ChangeID, change.target.StoreID, change.target.NodeID, change.rangeID,
			change.loadDelta, change.startTime,
		)
		if !(change.enactedAtTime == time.Time{}) {
			fmt.Fprintf(&buf, " enacted=%v",
				change.enactedAtTime)
		}
		fmt.Fprintf(&buf, "\n  prev=(%v)\n  next=(%v)", change.prev, change.next)
	}
	return buf.String()
}

// createPendingChanges takes a set of changes applies the changes as pending.
// The application updates the adjusted load, tracked pending changes and
// changeID to reflect the pending application.
func (cs *clusterState) createPendingChanges(changes ...ReplicaChange) []*pendingReplicaChange {
	var pendingChanges []*pendingReplicaChange
	now := cs.ts.Now()
	for _, change := range changes {
		cs.applyReplicaChange(change, true)
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
		log.VInfof(context.Background(), 3, "createPendingChanges: change_id=%v, range_id=%v, change=%v", cid, change.rangeID, change)
		pendingChanges = append(pendingChanges, pendingChange)
	}
	return pendingChanges
}

func (cs *clusterState) preCheckOnApplyReplicaChanges(
	changes []ReplicaChange,
) (valid bool, reason string) {
	if len(changes) == 0 {
		return false, "no changes to apply"
	}
	// preApplyReplicaChange is called before applying a change to the cluster
	// state.
	if len(changes) != 1 && len(changes) != 2 && len(changes) != 4 {
		panic(fmt.Sprintf(
			"applying replica changes must be of length 1, 2, or 4 but got %v in %v",
			len(changes), changes))
	}

	rangeID := changes[0].rangeID
	curr, ok := cs.ranges[rangeID]
	// Return early if range already has some pending changes or the range does not exist.
	if !ok {
		return false, "range does not exist in cluster state"
	}
	if len(curr.pendingChanges) > 0 {
		log.VInfof(context.Background(), 2, "range %d has pending changes: %v",
			rangeID, curr.pendingChanges)
		return false, "range has pending changes"
	}

	// Make a deep copy of the range state
	copiedCurr := rangeState{
		replicas: append([]StoreIDAndReplicaState{}, curr.replicas...),
	}
	for _, change := range changes {
		// Check that all changes correspond to the same range. Panic otherwise.
		if change.rangeID != rangeID {
			panic(fmt.Sprintf("unexpected change rangeID %d != %d", change.rangeID, rangeID))
		}
		if change.isRemoval() {
			// TODO: why are we not confirming that the replica actually exists in
			// copiedCurr?
			copiedCurr.removeReplica(change.target.StoreID)
		} else if change.isAddition() || change.isUpdate() {
			// TODO: shouldn't isAddition case check that the replica does not exist
			// in copiedCurr?
			pendingRepl := StoreIDAndReplicaState{
				StoreID: change.target.StoreID,
				ReplicaState: ReplicaState{
					ReplicaIDAndType: change.next,
				},
			}
			copiedCurr.setReplica(pendingRepl)
		} else {
			panic(fmt.Sprintf("unknown replica change %+v", change))
		}
	}
	// check on the final state of currCopy and whether it is valid
	return replicaSetIsValid(copiedCurr.replicas)
}

// TODO: this is unnecessary since if we always check against the current
// state before allowing a chang to be added (including re-addition after a
// StoreLeaseholderMsg), we should never have invalidity during an undo.
// Which is why this function now panics except for the trivial cases of no
// changes or the range not existing in the cluster state.
//
// This is also justified by the current callers. If this were to return false
// in non-trivial cases, what is the caller supposed to do? These changes have
// been reflected on both the membership and load information. Undoing the
// latter is trivial since it is just subtraction of numbers. But it can't
// undo the membership changes. So we presumably have left membership in an
// inconsistent state.
func (cs *clusterState) preCheckOnUndoReplicaChanges(changes []ReplicaChange) (bool, string) {
	if len(changes) == 0 {
		return false, "no changes to apply"
	}
	rangeID := changes[0].rangeID
	curr, ok := cs.ranges[rangeID]
	if !ok {
		return false, "range does not exist in cluster state"
	}

	copiedCurr := &rangeState{
		replicas: append([]StoreIDAndReplicaState{}, curr.replicas...),
	}
	for _, change := range changes {
		// TODO: for isRemoval it should check that the replica does not exist in
		// copiedCurr. for isUpdate it should check that the replica exists in
		// copiedCurr.
		if change.isRemoval() || change.isUpdate() {
			prevRepl := StoreIDAndReplicaState{
				StoreID:      change.target.StoreID,
				ReplicaState: change.prev,
			}
			copiedCurr.setReplica(prevRepl)
		} else if change.isAddition() {
			// TODO: for isAddition it should check that the replica exists in
			// copiedCurr.
			copiedCurr.removeReplica(change.target.StoreID)
		} else {
			panic(fmt.Sprintf("unknown replica change %+v", change))
		}
	}
	if ok, reason := replicaSetIsValid(copiedCurr.replicas); !ok {
		panic(fmt.Sprintf("undo should always be valid: %s", reason))
	}
	return true, ""
}

func (cs *clusterState) applyReplicaChange(change ReplicaChange, applyLoadChange bool) {
	storeState, ok := cs.stores[change.target.StoreID]
	if !ok {
		panic(fmt.Sprintf("store %v not found in cluster state", change.target.StoreID))
	}
	rangeState, ok := cs.ranges[change.rangeID]
	if !ok {
		// This is the first time encountering this range, we add it to the cluster
		// state.
		//
		// TODO(kvoli): Pass in the range descriptor to construct the range state
		// here. Currently, when the replica change is a removal this won't work
		// because the range state will not contain the replica being removed.
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}

	log.VInfof(context.Background(), 2, "applying replica change %v to range %d on store %d",
		change, change.rangeID, change.target.StoreID)
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
	if applyLoadChange {
		cs.applyChangeLoadDelta(change)
	}
}

func (cs *clusterState) undoReplicaChange(change ReplicaChange) {
	log.Infof(context.Background(), "undoing replica change %v to range %d on store %d",
		change, change.rangeID, change.target.StoreID)
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

// setStore updates the store attributes and locality in the cluster state. If
// the store hasn't been seen before, it is also added to the cluster state.
//
// TODO: We currently assume that the locality and attributes associated with a
// store/node are fixed. This is a reasonable assumption for the locality,
// however it is not for the attributes.
func (cs *clusterState) setStore(sal StoreAttributesAndLocality) {
	ns, ok := cs.nodes[sal.NodeID]
	if !ok {
		// This is the first time seeing the associated node.
		ns = newNodeState(sal.NodeID)
		cs.nodes[sal.NodeID] = ns
	}
	_, ok = cs.stores[sal.StoreID]
	if !ok {
		// This is the first time seeing this store.
		ss := newStoreState()
		ss.localityTiers = cs.localityTierInterner.intern(sal.locality())
		ss.overloadStartTime = cs.ts.Now()
		ss.overloadEndTime = cs.ts.Now()
		ss.StoreAttributesAndLocality = sal
		cs.constraintMatcher.setStore(sal)
		cs.stores[sal.StoreID] = ss
		ns.stores = append(ns.stores, sal.StoreID)
	}
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

func (cs *clusterState) getStoreReportedLoad(storeID roachpb.StoreID) (roachpb.NodeID, *storeLoad) {
	if storeState, ok := cs.stores[storeID]; ok {
		return storeState.NodeID, &storeState.storeLoad
	}
	return 0, nil
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
// overloadDim represents the dimension that is overloaded in the source and
// the function requires that the target must be currently < loadNoChange
// along that dimension.
func (cs *clusterState) canShedAndAddLoad(
	ctx context.Context,
	srcSS *storeState,
	targetSS *storeState,
	delta LoadVector,
	means *meansForStoreSet,
	onlyConsiderTargetCPUSummary bool,
	overloadedDim LoadDimension,
) (canAddLoad bool) {
	if overloadedDim == NumLoadDimensions {
		panic("overloadedDim must not be NumLoadDimensions")
	}
	// TODO(tbg): in experiments, we often see interesting behavior right when
	// the load delta addition flips the loadSummary for either the target or the
	// source, which suggests it might be useful to add this to verbose logging.

	targetNS := cs.nodes[targetSS.NodeID]
	// Add the delta.
	deltaToAdd := loadVectorToAdd(delta)
	targetSS.adjusted.load.add(deltaToAdd)
	// TODO(tbg): why does NodeLoad have an adjustedCPU field but not fields for
	// the other load dimensions? We just added deltaToAdd to targetSS.adjusted,
	// shouldn't this be wholly reflected in targetNS as well, not just for CPU?
	// Or maybe CPU is the only dimension that matters at the node level. It feels
	// sloppy/confusing though.
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

	var reason strings.Builder
	defer func() {
		if canAddLoad {
			log.VInfof(ctx, 3, "can add load to n%vs%v: %v targetSLS[%v] srcSLS[%v]",
				targetNS.NodeID, targetSS.StoreID, canAddLoad, targetSLS, srcSLS)
		} else {
			log.VInfof(ctx, 2, "cannot add load to n%vs%v: due to %s", targetNS.NodeID, targetSS.StoreID, reason.String())
			log.VInfof(ctx, 2, "[target_sls:%v,src_sls:%v]", targetSLS, srcSLS)
		}
	}()
	if targetSLS.highDiskSpaceUtilization {
		reason.WriteString("targetSLS.highDiskSpaceUtilization")
		return false
	}
	// We define targetSummary as a summarization across all dimensions of the
	// target. A targetSummary < loadNoChange always accepts the change. When
	// the targetSummary >= loadNoChange, we are stricter and require both that
	// there are no pending changes in the target, and the target is "not worse"
	// in a way that will cause thrashing, where the details are defined below.
	// The no pending changes requirement is to delay making a potentially
	// non-ideal choice of the target.
	//
	// NB: The target's overload dimension summary must have been <
	// loadNoChange, and the source must have been > loadNoChange.
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

	if targetSummary < loadNoChange {
		return true
	}
	if targetSummary >= overloadUrgent {
		reason.WriteString("overloadUrgent")
		return false
	}
	// Need to consider additional factors.
	//
	// It is possible that both are overloadSlow in aggregate. We want to make
	// sure that this exchange doesn't make the target worse than the source in
	// the dimension being shed.
	overloadedDimPermitsChange :=
		targetSLS.dimSummary[overloadedDim] <= srcSLS.dimSummary[overloadedDim]
	// For the other dimensions, we want to make sure that the target is not
	// getting worse than it was before the change, if it was already overloaded
	// in that dimension. This is to prevent thrashing. One way to do this is to
	// simply reject the change if for any i != overloadedDim,
	// initialTargetSLS.dimSummary[i] < targetSLS.dimSummary[i] && targetSLS.dimSummary[i] > loadNoChange
	//
	// where initialTargetSLS is the target's load summary before the attempted
	// change.
	//
	// This is what we initially did, but note that this is not quite
	// strict enough. We may have picked the top range wrt overloadedDim, to
	// shed from the source, but it may also add significant load to the target
	// along a different dimension, dim2, along with the target is already
	// overloaded. This happens because the set of ranges this store can fiddle
	// with are limited. To improve this, we also check that the target is
	// seeing dim2 increase at a smaller fraction than it is seeing
	// overloadedDim increase.
	//
	// That boolean predicate can also be too strict, in that we should permit
	// transitions to overloadSlow along one dimension, to allow for an
	// exchange.
	overloadedDimFractionIncrease := math.MaxFloat64
	if targetSS.adjusted.load[overloadedDim] > 0 {
		overloadedDimFractionIncrease = float64(deltaToAdd[overloadedDim]) /
			float64(targetSS.adjusted.load[overloadedDim])
	}
	otherDimensionsBecameWorseInTarget := false
	for i := range targetSLS.dimSummary {
		dim := LoadDimension(i)
		if dim == overloadedDim {
			continue
		}
		if targetSLS.dimSummary[i] <= loadNoChange {
			continue
		}
		// This is an overloaded dimension in the target. Only allow small
		// increases along this dimension.
		dimFractionIncrease := math.MaxFloat64
		if targetSS.adjusted.load[dim] > 0 {
			dimFractionIncrease = float64(deltaToAdd[dim]) / float64(targetSS.adjusted.load[dim])
		}
		// The use of 33% is arbitrary.
		if dimFractionIncrease > overloadedDimFractionIncrease/3 {
			log.Infof(ctx, "%v: %f > %f/3", dim, dimFractionIncrease, overloadedDimFractionIncrease)
			otherDimensionsBecameWorseInTarget = true
			break
		}
	}
	canAddLoad = overloadedDimPermitsChange && !otherDimensionsBecameWorseInTarget &&
		targetSLS.maxFractionPendingIncrease < epsilon &&
		targetSLS.maxFractionPendingDecrease < epsilon &&
		// NB: targetSLS.nls <= targetSLS.sls is not a typo, in that we are
		// comparing targetSLS with itself. The nls only captures node-level
		// CPU, so if a store that is overloaded wrt WriteBandwidth wants to
		// shed to a store that is overloaded wrt CPURate, we need to permit
		// that. However, the nls of the former will be less than the that of
		// the latter. By looking at the nls of the target here, we are making
		// sure that it is no worse than the sls of the target, since if it
		// is, the node is overloaded wrt CPU due to some other store on that
		// node, and we should be shedding that load first.
		targetSLS.nls <= targetSLS.sls
	if canAddLoad {
		return true
	}
	if !overloadedDimPermitsChange {
		if reason.Len() != 0 {
			reason.WriteRune(',')
		}
		reason.WriteString("!overloadedDimPermitsChange")
	}
	if otherDimensionsBecameWorseInTarget {
		if reason.Len() != 0 {
			reason.WriteRune(',')
		}
		reason.WriteString("otherDimensionsBecameWorseInTarget")
	}
	if targetSummary >= loadNoChange {
		if reason.Len() != 0 {
			reason.WriteRune(',')
		}
		reason.WriteString(fmt.Sprintf("target_summary(%s)>=loadNoChange", targetSummary))
	}
	if targetSLS.maxFractionPendingIncrease >= epsilon || targetSLS.maxFractionPendingDecrease >= epsilon {
		if reason.Len() != 0 {
			reason.WriteRune(',')
		}
		reason.WriteString(fmt.Sprintf("targetSLS.frac_pending(%.2for%.2f>=epsilon)",
			targetSLS.maxFractionPendingIncrease, targetSLS.maxFractionPendingDecrease))
	}
	if targetSLS.sls > srcSLS.sls {
		if reason.Len() != 0 {
			reason.WriteRune(',')
		}
		reason.WriteString(fmt.Sprintf("target-store(%s)>src-store(%s)", targetSLS.sls, srcSLS.sls))
	}
	if targetSLS.nls > targetSLS.sls {
		if reason.Len() != 0 {
			reason.WriteRune(',')
		}
		reason.WriteString(fmt.Sprintf("target-node(%s)>target-store(%s)",
			targetSLS.nls, targetSLS.sls))
	}
	return false
}

func (cs *clusterState) computeLoadSummary(
	storeID roachpb.StoreID, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	ss := cs.stores[storeID]
	ns := cs.nodes[ss.NodeID]
	return computeLoadSummary(ss, ns, msl, mnl)
}

// TODO(wenyihu6): check to make sure obs here is correct
func (cs *clusterState) loadSummaryForAllStores() string {
	var b strings.Builder
	clusterMeans := cs.meansMemo.getMeans(nil)
	b.WriteString(fmt.Sprintf("cluster means: (stores-load %s) (stores-capacity %s)\n",
		clusterMeans.storeLoad.load, clusterMeans.storeLoad.capacity))
	b.WriteString(fmt.Sprintf("(nodes-cpu-load %d) (nodes-cpu-capacity %d)\n",
		clusterMeans.nodeLoad.loadCPU, clusterMeans.nodeLoad.capacityCPU))
	for storeID, ss := range cs.stores {
		sls := cs.meansMemo.getStoreLoadSummary(clusterMeans, storeID, ss.loadSeqNum)
		b.WriteString(fmt.Sprintf("evaluating store s%d for shedding: load summary %v", storeID, sls))
	}
	return b.String()
}

func computeLoadSummary(
	ss *storeState, ns *nodeState, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	sls := loadLow
	var highDiskSpaceUtil bool
	var dimSummary [NumLoadDimensions]loadSummary
	var worstDim LoadDimension
	for i := range msl.load {
		// TODO(kvoli,sumeerbhola): Handle negative adjusted store/node loads.
		ls := loadSummaryForDimension(
			ss.StoreID, 0 /*NodeID(for logging)*/, LoadDimension(i), ss.adjusted.load[i], ss.capacity[i], msl.load[i], msl.util[i])
		if ls > sls {
			sls = ls
			worstDim = LoadDimension(i)
		}
		dimSummary[i] = ls
		switch LoadDimension(i) {
		case ByteSize:
			highDiskSpaceUtil = highDiskSpaceUtilization(ss.adjusted.load[i], ss.capacity[i])
		}
	}
	nls := loadSummaryForDimension(0 /*StoreID(for logging)*/, ns.NodeID, CPURate, ns.adjustedCPU, ns.CapacityCPU, mnl.loadCPU, mnl.utilCPU)
	return storeLoadSummary{
		worstDim:                   worstDim,
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

type StoreAttributesAndLocality struct {
	roachpb.StoreID
	roachpb.NodeID
	NodeAttrs    roachpb.Attributes
	NodeLocality roachpb.Locality
	StoreAttrs   roachpb.Attributes
}

// locality returns the locality of the Store, which is the Locality of the
// node plus an extra tier for the node itself. Copied from
// StoreDescriptor.Locality.
func (saal StoreAttributesAndLocality) locality() roachpb.Locality {
	return saal.NodeLocality.AddTier(
		roachpb.Tier{Key: "node", Value: saal.NodeID.String()})
}

// Avoid unused lint errors.
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
