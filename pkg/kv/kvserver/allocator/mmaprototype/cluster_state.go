// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// These values can sometimes be used in ReplicaType, ReplicaIDAndType,
// ReplicaState, specifically when used in the context of a
// pendingReplicaChange.
//
// NB: In practice, the kvserver code never generates 0 as a valid ReplicaID.
// The MMA code does not special case 0 to be an invalid value, because as of
// the time of writing this comment, the 0 value being invalid was an
// undocumented invariant of the kvserver code. Instead, the code here uses
// two negative values to represent special cases.
const (
	// unknownReplicaID is used with a change that proposes to add a replica
	// (since it does not know the future ReplicaID).
	unknownReplicaID roachpb.ReplicaID = -1
	// noReplicaID is used with a change that is removing a replica.
	noReplicaID roachpb.ReplicaID = -2

	nodeIDForLogging  = 0
	storeIDForLogging = 0
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
func (rit ReplicaIDAndType) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print("replica-id=")
	switch rit.ReplicaID {
	case unknownReplicaID:
		w.Print("unknown")
	case noReplicaID:
		w.Print("none")
	default:
		w.Print(rit.ReplicaID)
	}
	w.Printf(" type=%v", rit.ReplicaType.ReplicaType)
	if rit.IsLeaseholder {
		w.Print(" leaseholder=true")
	}
}

func (rit ReplicaIDAndType) String() string {
	return redact.StringWithoutMarkers(rit)
}

// subsumesChange returns true if rit subsumes next, where next is the state
// after the proposed change, and rit is the current observed state.
//
// NB: this method uses a value receiver since it mutates the value as part of
// its computation.
func (rit ReplicaIDAndType) subsumesChange(next ReplicaIDAndType) bool {
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
		// Already a voter, so consider the change done.
		rit.ReplicaType.ReplicaType = roachpb.VOTER_FULL
	}
	// NB: rit.replicaType.ReplicaType equal to LEARNER, VOTER_DEMOTING* are
	// left as-is. If next is trying to remove a replica, this change has not
	// finished yet, and the store is still seeing the load corresponding to the
	// state it is exiting. If next is trying to demote to a NON_VOTER, but
	// rit.ReplicaType.ReplicaType is equal to VOTER_DEMOTING_NON_VOTER, then it
	// is still a voter, so the change is not yet done.

	if rit.ReplicaType.ReplicaType == next.ReplicaType.ReplicaType &&
		rit.IsLeaseholder == next.IsLeaseholder {
		return true
	}
	return false
}

type ReplicaState struct {
	ReplicaIDAndType
	// LeaseDisposition can be set for a VOTER_FULL replica and communicates the
	// availability of this replica for lease transfers.
	LeaseDisposition LeaseDisposition
}

// changeID is a unique ID, in the context of this data-structure and when
// receiving updates about enactment having happened or having been rejected
// (by the component responsible for change enactment).
type changeID uint64

type ReplicaChangeType int

const (
	Unknown ReplicaChangeType = iota
	AddLease
	RemoveLease
	// AddReplica represents a single replica being added.
	AddReplica
	// RemoveReplica represents a single replica being removed.
	RemoveReplica
	// ChangeReplica represents a promotion to VOTER or demotion to NON_VOTER.
	// It can also be shedding or acquiring the lease.
	ChangeReplica
)

func (s ReplicaChangeType) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case AddLease:
		return "AddLease"
	case RemoveLease:
		return "RemoveLease"
	case AddReplica:
		return "AddReplica"
	case RemoveReplica:
		return "RemoveReplica"
	case ChangeReplica:
		return "ChangeReplica"
	default:
		panic("unknown ReplicaChangeType")
	}
}

func replicaExists(replicaID roachpb.ReplicaID) bool {
	return replicaID >= 0 || replicaID == unknownReplicaID
}

// ReplicaChange describes a change to a replica.
type ReplicaChange struct {
	// The load this change adds to a store. The values will be negative if the
	// load is being removed.
	loadDelta          LoadVector
	secondaryLoadDelta SecondaryLoadVector

	// target is the target {store,node} for the change.
	target roachpb.ReplicationTarget
	// rangeID is the same as that in the PendingRangeChange.RangeID this change
	// is part of. It is duplicated here since the individual
	// pendingReplicaChanges are kept in various maps keyed by the changeID, and
	// having the RangeID field on each change is convenient.
	rangeID roachpb.RangeID

	// NB: 0 is not a valid ReplicaID, but this component does not care about
	// this level of detail (the special constants defined above use negative
	// ReplicaID values as markers).
	//
	// We define exists(replicaID) =
	//  replicaID >= 0 || replicaID == unknownReplicaID.
	//
	// Only following cases can happen:
	//
	// - exists(prev.replicaID) && next.replicaID == noReplicaID: outgoing
	//   replica. prev.IsLeaseholder can be true or false, since we can transfer
	//   the lease as part of moving the replica. ReplicaChangeType is
	//   RemoveReplica.
	//
	// - prev.replicaID == noReplicaID && next.replicaID == unknownReplicaID:
	//   incoming replica, next.ReplicaType must be VOTER_FULL or NON_VOTER.
	//   next.IsLeaseholder can be true or false. ReplicaChangeType is
	//   AddReplica.
	//
	// - exists(prev.replicaID) && exists(next.replicaID):
	//   - If prev.ReplicaType == next.ReplicaType, ReplicaChangeType must be
	//     AddLease or RemoveLease, with a change in the IsLeaseholder bit.
	//   - If prev.ReplicaType != next.ReplicaType, ReplicaChangeType is
	//     ChangeReplica, and this is a promotion/demotion. The IsLeaseholder
	//     bit can change or be false in both prev and next (it can't be true in
	//     both since a promoted replica can't have been the leaseholder and a
	//     replica being demoted cannot retain the lease).
	//
	// NB: The prev value is always the state before the change. This is the
	// latest source of truth provided by the leaseholder in the RangeMsg, so
	// will have real ReplicaIDs (if already a replica) and real ReplicaTypes
	// (including types beyond VOTER_FULL and NON_VOTER). This source-of-truth
	// claim is guaranteed by REQUIREMENT(change-computation) documented
	// elsewhere, and the fact that new changes are computed only when there are
	// no pending changes for a range.
	//
	// The ReplicaType in next is either the zero value (for removals), or
	// {VOTER_FULL, NON_VOTER} for additions/change, i.e., it represents the
	// final goal state.
	//
	// TODO(tbg): in MakeLeaseTransferChanges, next.ReplicaType.ReplicaType is
	// simply the current value, and not necessarily {VOTER_FULL, NON_VOTER}.
	// So the above comment is incorrect. We should clean this up.
	//
	// The prev field is mutable after creation, to ensure that an undo restores
	// the state to the latest source of truth from the leaseholder.
	prev ReplicaState
	next ReplicaIDAndType
}

func (rc ReplicaChange) String() string {
	return redact.StringWithoutMarkers(rc)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rc ReplicaChange) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%v type: %v target store %v (%v)->(%v)", rc.rangeID, rc.replicaChangeType(), rc.target, rc.prev, rc.next)
}

// isRemoval returns true if the change is a removal of a replica.
func (rc ReplicaChange) isRemoval() bool {
	return rc.replicaChangeType() == RemoveReplica
}

// isAddition returns true if the change is an addition of a replica.
func (rc ReplicaChange) isAddition() bool {
	return rc.replicaChangeType() == AddReplica
}

// isUpdate returns true if the change is an update to the replica type or
// leaseholder status. This includes promotion/demotion changes.
func (rc ReplicaChange) isUpdate() bool {
	changeType := rc.replicaChangeType()
	return changeType == AddLease || changeType == RemoveLease || changeType == ChangeReplica
}

// replicaChangeType returns the type of change currently represented by prev
// and next. Since prev is mutable, the result of this method can change over
// time.
func (rc ReplicaChange) replicaChangeType() ReplicaChangeType {
	prevExists := replicaExists(rc.prev.ReplicaID)
	nextExists := replicaExists(rc.next.ReplicaID)
	if !prevExists && !nextExists {
		return Unknown
	}
	if prevExists && !nextExists {
		return RemoveReplica
	}
	// INVARIANT: nextExists.

	if !prevExists {
		return AddReplica
	}
	if rc.prev.ReplicaType.ReplicaType == rc.next.ReplicaType.ReplicaType {
		if rc.prev.ReplicaType.IsLeaseholder == rc.next.ReplicaType.IsLeaseholder {
			return Unknown
		}
		if rc.next.IsLeaseholder {
			return AddLease
		}
		return RemoveLease
	}
	return ChangeReplica
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
// to the store addStoreID.
func MakeAddReplicaChange(
	rangeID roachpb.RangeID,
	rLoad RangeLoad,
	replicaIDAndType ReplicaIDAndType,
	addTarget roachpb.ReplicationTarget,
) ReplicaChange {
	replicaIDAndType.ReplicaType.ReplicaType = mapReplicaTypeToVoterOrNonVoter(
		replicaIDAndType.ReplicaType.ReplicaType)
	replicaIDAndType.ReplicaID = unknownReplicaID
	addReplica := ReplicaChange{
		target:  addTarget,
		rangeID: rangeID,
		prev: ReplicaState{
			ReplicaIDAndType: ReplicaIDAndType{
				ReplicaID: noReplicaID,
			},
		},
		next: replicaIDAndType,
	}
	addReplica.next.ReplicaID = unknownReplicaID
	addReplica.loadDelta.add(loadVectorToAdd(rLoad.Load))
	if replicaIDAndType.IsLeaseholder {
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

// MakeReplicaTypeChange creates a replica change which changes the type of
// the replica.
func MakeReplicaTypeChange(
	rangeID roachpb.RangeID,
	rLoad RangeLoad,
	prev ReplicaState,
	next ReplicaIDAndType,
	target roachpb.ReplicationTarget,
) ReplicaChange {
	next.ReplicaID = unknownReplicaID
	next.ReplicaType.ReplicaType = mapReplicaTypeToVoterOrNonVoter(next.ReplicaType.ReplicaType)
	change := ReplicaChange{
		target:  target,
		rangeID: rangeID,
		prev:    prev,
		next:    next,
	}
	if next.IsLeaseholder {
		change.secondaryLoadDelta[LeaseCount] = 1
		change.loadDelta[CPURate] = loadToAdd(rLoad.Load[CPURate] - rLoad.RaftCPU)
	} else if prev.IsLeaseholder {
		change.secondaryLoadDelta[LeaseCount] = -1
		change.loadDelta[CPURate] = rLoad.RaftCPU - rLoad.Load[CPURate]
	}
	return change
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
		log.KvDistribution.Fatalf(context.Background(), "remove target %s not in existing replicas", removeTarget)
	}

	addIDAndType := ReplicaIDAndType{
		ReplicaID:   unknownReplicaID,
		ReplicaType: remove.ReplicaType,
	}
	addReplicaChange := MakeAddReplicaChange(rangeID, rLoad, addIDAndType, addTarget)
	removeReplicaChange := MakeRemoveReplicaChange(rangeID, rLoad, remove.ReplicaState, removeTarget)
	return [2]ReplicaChange{addReplicaChange, removeReplicaChange}
}

func mapReplicaTypeToVoterOrNonVoter(rType roachpb.ReplicaType) roachpb.ReplicaType {
	switch rType {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.VOTER_DEMOTING_LEARNER, roachpb.VOTER_DEMOTING_NON_VOTER:
		return roachpb.VOTER_FULL
	case roachpb.NON_VOTER, roachpb.LEARNER:
		return roachpb.NON_VOTER
	default:
		panic(errors.AssertionFailedf("unknown replica type %v", rType))
	}
}

// TODO(sumeer): place PendingRangeChange in a different file after
// https://github.com/cockroachdb/cockroach/pull/158024 merges.
//
// TODO(sumeer): PendingRangeChange is exported only because external callers
// need to be able to represent load changes when calling
// RegisterExternalChange. The ExternalRangeChange type does not represent
// load. There is possibly a better way to structure this, by including the
// LoadVector and SecondaryLoadVector in the ExternalRangeChange type, and
// unexporting PendingRangeChange.

// PendingRangeChange is a container for a proposed set of change(s) to a
// range. It can consist of multiple pending replica changes, such as adding
// or removing replicas, or transferring the lease. There is at most one
// change per store in the set.
//
// The clusterState or anything contained in it, does not contain
// PendingRangeChange, and instead individual *pendingReplicaChange are stored
// in various maps and slices. Note that *pendingReplicaChanges contain
// mutable fields.
type PendingRangeChange struct {
	RangeID               roachpb.RangeID
	pendingReplicaChanges []*pendingReplicaChange
}

// MakePendingRangeChange creates a PendingRangeChange for the given rangeID
// and changes. Certain internal aspects of the change, like the change-ids,
// start time etc., are not yet initialized, since those use internal state of
// MMA. Those will be initialized by MMA when this change is later handed to
// MMA for tracking, in clusterState.addPendingRangeChange. For external
// callers of MakePendingRangeChange, this happens transitively when
// Allocator.RegisterExternalChange is called.
func MakePendingRangeChange(rangeID roachpb.RangeID, changes []ReplicaChange) PendingRangeChange {
	for _, c := range changes {
		if c.rangeID != rangeID {
			panic(errors.AssertionFailedf("all changes must be to the same range %d != %d",
				c.rangeID, rangeID))
		}
	}
	prcs := make([]*pendingReplicaChange, len(changes))
	for i, c := range changes {
		prcs[i] = &pendingReplicaChange{
			ReplicaChange: c,
		}
	}
	return PendingRangeChange{
		RangeID:               rangeID,
		pendingReplicaChanges: prcs,
	}
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
	nextAddOrChangeReplicaStr := func(next ReplicaType) string {
		if next.ReplicaType == roachpb.NON_VOTER {
			return "non-voter"
		}
		if next.IsLeaseholder {
			return "voter-leaseholder"
		}
		return "voter"
	}
	for i, c := range prc.pendingReplicaChanges {
		if i > 0 {
			w.Print(" ")
		}
		w.Printf("id:%d", c.changeID)
		switch c.replicaChangeType() {
		case Unknown:
			w.Printf("unknown-change:s%v", c.target.StoreID)
		case AddLease:
			w.Printf("add-lease:s%v", c.target.StoreID)
		case RemoveLease:
			w.Printf("remove-lease:s%v", c.target.StoreID)
		case AddReplica:
			w.Printf("add-%s:s%v", nextAddOrChangeReplicaStr(c.next.ReplicaType), c.target.StoreID)
		case RemoveReplica:
			w.Printf("remove-replica:s%v", c.target.StoreID)
		case ChangeReplica:
			w.Printf("change-to-%s:s%v", nextAddOrChangeReplicaStr(c.next.ReplicaType),
				c.target.StoreID)
		}
	}
	w.Print("]")
}

// StringForTesting prints the untransformed internal state for testing.
func (prc PendingRangeChange) StringForTesting() string {
	var b strings.Builder
	fmt.Fprintf(&b, "range r%v\n", prc.RangeID)
	for _, c := range prc.pendingReplicaChanges {
		fmt.Fprintf(&b, " %s\n", c.ReplicaChange.String())
		fmt.Fprintf(&b, "  load: %s\n", c.loadDelta.String())
		fmt.Fprintf(&b, "  secondary-load: %s\n", c.secondaryLoadDelta.String())
	}
	return b.String()
}

// SortForTesting sorts the internal pendingReplicaChanges slice to be
// deterministic, in increasing order of StoreID, for testing purposes.
func (prc PendingRangeChange) SortForTesting() {
	slices.SortFunc(prc.pendingReplicaChanges, func(a, b *pendingReplicaChange) int {
		return cmp.Compare(a.target.StoreID, b.target.StoreID)
	})
}

// isPureTransferLease returns true if the pending range change is purely a
// transfer lease operation (i.e., it is not a combined replication change and
// lease transfer).
//
// TODO(sumeer): this is a duplicate of
// ExternalRangeChange.IsPureTransferLease. Consider unifying.
func (prc PendingRangeChange) isPureTransferLease() bool {
	if len(prc.pendingReplicaChanges) != 2 {
		return false
	}
	var addLease, removeLease int
	for _, c := range prc.pendingReplicaChanges {
		switch c.replicaChangeType() {
		case AddLease:
			addLease++
		case RemoveLease:
			removeLease++
		default:
			// Any other change type is not a pure lease transfer (e.g. they
			// require a replication change).
			return false
		}
	}
	if addLease != 1 || removeLease != 1 {
		panic(errors.AssertionFailedf("unexpected add (%d) or remove lease (%d) in lease transfer",
			addLease, removeLease))
	}
	return true
}

// pendingReplicaChange is a proposed change to a single replica. Some
// external entity (the leaseholder of the range) may choose to enact this
// change. It may not be enacted if it will cause some invariant (like the
// number of replicas, or having a leaseholder) to be violated. If not
// enacted, the allocator will either be told about the lack of enactment, or
// will eventually expire from the allocator's state at gcTime. Such
// expiration without enactment should be rare. pendingReplicaChanges can be
// paired, when a range is being moved from one store to another -- that
// pairing is not captured here, and captured in the changes suggested by the
// allocator to the external entity.
type pendingReplicaChange struct {
	changeID
	ReplicaChange

	// The wall time at which this pending change was initiated. Used for
	// expiry. All replica changes in a PendingRangeChange have the same
	// startTime.
	startTime time.Time
	// gcTime represents a time when the unenacted change should be GC'd.
	//
	// Mutable after creation.
	gcTime time.Time

	// When the change is known to be enacted based on the authoritative
	// information received from the leaseholder, this value is set, so that even
	// if the store with a replica affected by this pending change does not tell
	// us about the enactment, we can garbage collect this change.
	//
	// Mutable after creation.
	enactedAtTime time.Time
}

// storeState maintains the complete state about a store as known to the
// allocator.
type storeState struct {
	status Status
	storeLoad
	storeAttributesAndLocalityWithNodeTier
	adjusted struct {
		// NB: these load values can become negative due to applying pending
		// changes. We need to let them be negative to retain the ability to undo
		// pending changes.
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
		loadPendingChanges map[changeID]*pendingReplicaChange
		// replicas is computed from the authoritative information provided by
		// various leaseholders in storeLeaseholderMsgs and adjusted for pending
		// changes in clusterState.pendingChanges/rangeState.pendingChanges.
		//
		// This is consistent with the union of state in clusterState.ranges,
		// filtered for replicas that are on this store.
		//
		// NB: this can include roachpb.ReplicaTypes other than VOTER_FULL and
		// NON_VOTER, e.g. LEARNER, VOTER_DEMOTING_LEARNER etc.
		replicas map[roachpb.RangeID]ReplicaState
		// topKRanges along some load dimension. If the store is overloaded along
		// one resource dimension, that is the dimension chosen when picking the
		// top-k.
		//
		// It includes ranges whose replicas are being removed via pending
		// changes, or lease transfers. That is, it does not account for
		// pending or enacted changes made since the last time top-k was
		// computed. It does account for pending changes that were pending
		// when the top-k was computed.
		//
		// The key in this map is a local store-id.
		//
		// NB: this only includes replicas that satisfy the isVoter() or
		// isNonVoter() methods, i.e., {VOTER_FULL, VOTER_INCOMING, NON_VOTER,
		// VOTER_DEMOTING_NON_VOTER}. This is justified in that these are the
		// only replica types where the allocator wants to explicitly consider
		// shedding, since the other states are transient states, that are
		// either going away, or will soon transition to a full-fledged state.
		//
		// We may decide to keep this top-k up-to-date incrementally instead of
		// recomputing it from scratch on each StoreLeaseholderMsg.
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
	// rebalanceStores.
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
	ss.maxFractionPendingIncrease, ss.maxFractionPendingDecrease = computeMaxFractionPendingIncDec(ss.reportedLoad, ss.adjusted.load)
}

func computeMaxFractionPendingIncDec(rep, adj LoadVector) (maxFracInc, maxFracDec float64) {
	for i := range rep {
		inc, dec := func(rep, adj LoadValue) (inc, dec float64) {
			// The fraction pending expresses the absolute difference of the adjusted
			// and reported load as a multiple of the reported load. Note that this
			// is the case even if the adjusted load is negative: if, say, the adjusted
			// load is -50 and the reported load is 100, it is still correct to say that
			// a "magnitude 1.5x" change is pending (from 100 to -50).
			diff := adj - rep

			switch {
			case diff == 0:
				// Reported and adjusted are equal, so nothing is pending.
				// This also handles the case in which both are zero.
				// We don't need to update maxFracInc or maxFracDec because
				// they started at zero and only go up from there.
				return 0, 0
			case rep == 0:
				// The adjusted load is nonzero, but the reported one is zero. We can't
				// express the load change as a multiple of zero. Arbitrarily assign large
				// value to both increase and decrease, indicating that no more changes
				// should be made until either the pending change clears (and we get a
				// zero diff above) or we register positive reported load.
				return 1000, 1000
			case diff > 0:
				// Vanilla case of adjusted > reported, i.e. we have load incoming.
				// We don't need to update maxFracDec.
				return math.Abs(float64(diff) / float64(rep)), 0
			case diff < 0:
				// Vanilla case of adjusted < reported, i.e. we have load incoming.
				// We don't need to update maxFracInc.
				return 0, math.Abs(float64(diff) / float64(rep))
			default:
				panic("impossible")
			}
		}(rep[i], adj[i])
		maxFracInc = max(maxFracInc, inc)
		maxFracDec = max(maxFracDec, dec)
	}
	return maxFracInc, maxFracDec
}

func newStoreState() *storeState {
	ss := &storeState{}
	ss.adjusted.loadPendingChanges = map[changeID]*pendingReplicaChange{}
	ss.adjusted.replicas = map[roachpb.RangeID]ReplicaState{}
	ss.adjusted.topKRanges = map[roachpb.StoreID]*topKReplicas{}
	return ss
}

type nodeState struct {
	stores []roachpb.StoreID
	NodeLoad
	// NB: adjustedCPU can be negative.
	adjustedCPU LoadValue
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
	w.Printf("s%v:%v lease disposition:%v", s.StoreID, s.ReplicaState.ReplicaIDAndType, s.ReplicaState.LeaseDisposition)
}

// rangeState is periodically updated based on reporting by the leaseholder.
type rangeState struct {
	// localRangeOwner is used for rangeState GC. The StoreID mentioned here is
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
	// be updated to s2. If the reverse ordering happens, the rangeState will be
	// garbage collected, and later the StoreLeaseholderMsg from s2 will
	// recreate it.
	localRangeOwner roachpb.StoreID
	// replicas is the adjusted replicas after applying pendingChanges. It is
	// always consistent with the storeState.adjusted.replicas in the
	// corresponding stores.
	//
	// INVARIANT: There must be exactly one replica marked as the leaseholder in
	// this slice. Additionally, the leaseholder replica in the slice may not be
	// a local store when there are pending changes that transfer the lease
	// away, since replicas contains the final state after applying pending
	// changes (we continue to track the rangeState in clusterState.ranges until
	// it is wiped out by a StoreLeaseholderMsg from the current localRangeOwner
	// that no longer mentions the range (we've discussed this above).
	//
	// A Note about Pending Changes:
	//
	// 1. Overview
	//
	// One should think of pendingChanges as a transient override of the
	// authoritative state provided by the leaseholder in a RangeMsg.Replicas
	// (i.e., eventually, RangeMsg.Replicas wins). The allocator wants to track
	// the effect of the ongoing changes, to prevent itself from making more
	// changes that overshoot a goal, which is why these changes are
	// incorporated into replicas (and storeState.adjusted.load etc.). It also
	// wants to know exactly what is pending, so when it sees RangeMsg.Replicas
	// that are still at the initial state, or an intermediate state, it can
	// continue anticipating that these pending changes will happen. Tracking
	// what is pending also allows for undo in the case of explicit failure,
	// notified by AdjustPendingChangesDisposition, or GC.
	//
	// 2. Lifecycle
	// pendingChanges track proposed modifications to a range's replicas or
	// leaseholder that are not yet reflected in the leaseholder's authoritative
	// state. They are created by three sources: range rebalances, lease transfers
	// originating from MMA, or external changes via RegisterExternalChanges
	// (replicate or lease queue). There exists a pending change in a range state
	// iff there is also a corresponding one in clusterState's pendingChanges.
	//
	// A pending change is removed from tracking in one of three ways:
	// 1. Marked as enacted successfully: remove the pending changes. The adjusted
	// load remains until processStoreLoadMsg determines the change is reflected
	// in the latest store load message, based on whether
	// lagForChangeReflectedInLoad has elapsed since enactment.
	//
	// This happens when:
	// - The pending change is successfully applied via
	// AdjustPendingChangesDisposition(success).
	// - The pending change is considered subsumed based on the leaseholder msg.
	// - The leaseholder of the range has changed. This is a special case where
	// the leaseholder of the range has moved to a different store, and the
	// rangeMsg no longer contains the range. We assume that the pending change
	// has been enacted in this case.
	//
	// 2. Undone as failed: corresponding replica and load change is rolled back.
	// Note that for replica changes that originate from one action, some changes
	// can be considered done because of the leaseholder msg, and others can be
	// rolled back (say due to GC).
	//
	// This happens when:
	// - The pending change failed to apply via
	// AdjustPendingChangesDisposition(failed)).
	// - The pending change is garbage collected after this pending change has
	// been created for pending{Replica,Lease}ChangeGCDuration.
	//
	// 3. Dropped due to incompatibility: mma creates these pending changes while
	// working with an earlier authoritative leaseholder message. These changes
	// remain valid until a new authoritative message arrives that may reflect a
	// conflicting state. See preCheckOnApplyReplicaChanges for details on how
	// compatibility between the pending change and the new range state is
	// determined. When incompatibility is detected, the pending replica change is
	// discarded and the corresponding load adjustments are rolled back.
	//
	// This happens when:
	// - processStoreLeaseholderMsgInternal tries to apply the pending changes to
	// the received range state from the new leaseholder msg, but the pending
	// changes are incompatible with the new range state.
	//
	// 3. Modeling
	//
	// The slice of pendingChanges represent one decision. However, this
	// decision is not always executed atomically by the external system.
	//
	// The decision is modeled using at most one pendingReplicaChange per
	// replica (and store) in the pre-change rangeState.replicas. This means
	// that when we see a new RangeMsg.Replicas, and have an existing list of
	// pending changes, we can individually compare each pending change to the
	// state in RangeMsg.Replicas and decide whether it is (a) already
	// incorporated or (b) can still apply in the future or (c) is inconsistent.
	// Even complex decisions don't need to refer to a replica multiple times,
	// so this is not a problematic restriction.
	//
	// This separability per replica allows for observing intermediate states
	// representing partial application (case (a) in the previous paragraph),
	// and leaving other changes as pending. A simple example of this is a range
	// move where replicas are at stores {s1, s2, s3} and a replica is moved
	// from s3 to s4. There will be two pending changes, for addition of s4, and
	// removal of s3, and replicas field will represent the final state {s1, s2,
	// s4}. If the leaseholder (say s1) provides a RangeMsg that lists replicas
	// {s1, s2, s3, s4}, the allocator realizes that the addition of s4 is done,
	// marks it as enacted and removes it from the pendingChanges slice. Note
	// that this capability to mark some pending changes as enacted is in some
	// ways overly lenient, but we don't try to narrow this leniency: e.g. if
	// one sees {s1, s2}, one will mark the removal pending change as enacted,
	// and keep the addition pending change. But the external system does not
	// make a replica change in this manner, and there was probably some kind of
	// other event that happened (e.g. some other component made a change
	// without the allocator's knowledge), and it is likely that s4 will never
	// be added (we will GC it after partiallyEnactedGCDuration).
	//
	// The modeling of intermediate states is not perfect. Say that in the above
	// example s3 was the local leaseholder. And say that the decision is
	// removing both the leaseholder and replica from s3, and adding a replica
	// and leaseholder to s4. This will be modeled with two pending changes, one
	// that removes the replica and leaseholder from s3, and another that adds
	// the replica and leaseholder to s4. An intermediate state that can be
	// observed is {s1, s2, s3, s4} with the lease still at s3. But the pending
	// change for adding s4 includes both that it has a replica, and it has the
	// lease, so we will not mark it done, and keep pretending that the change
	// is pending. However, we will change the prev state to indicate that s4
	// has a replica, so that undo (say due to GC) rolls back to the latest
	// source-of-truth from the leaseholder.
	//
	// 4. Non Atomicity Hazard
	//
	// Since a decision is represented with multiple pending changes, and we
	// allow for individual changes to be considered enacted or failed, we have
	// to contend with the hazard of having two leaseholders or no leaseholders.
	// In the earlier example, say s3 and s4 were both local stores (a
	// multi-store node), it may be possible to observe an intermediate state
	// {s1, s2, s3, s4} where s4 is the leaseholder. We need to ensure that if
	// we subsequently get a spurious
	// AdjustPendingChangesDisposition(success=false) call, or time-based GC
	// causes the s3 removal to be undone, there will not be two replicas marked
	// as the leaseholder. The other extreme is believing that the s3 transfer
	// is done and the s4 incoming replica (and lease) failed (this may not
	// actually be possible because of the surrounding code).
	//
	// This hazard is dealt with in the same way outlined in the earlier
	// example: when the leaseholder msg from s4 arrives that lists {s1, s2, s3,
	// s4} as replicas, the prev state for the s3 change is updated to indicate
	// that it is not the leaseholder. This means that if the change is undone,
	// it will return to a prev state where it has a replica but not the lease.
	//
	// Additionally, when processing a RangeMsg, if any of the pending changes
	// is considered inconsistent, all the pending changes are discarded. This
	// avoids a situation where the RangeMsg presents a state that causes the
	// addition of s4 to be thrown away while keeping the removal of s1. This is
	// mostly being defensive to avoid any chance of internal inconsistency.
	replicas []StoreIDAndReplicaState
	// conf is nil iff developer error or basic span config normalization failed
	// (invalid config), in which case the range analysis is skipped,
	// rangeAnalyzedConstraints is always nil, and the range is excluded from
	// rebalancing. Note that if only doStructuralNormalization failed, conf will
	// be non-nil.
	conf *normalizedSpanConfig

	load RangeLoad

	// The pending changes to this range, that are already reflected in
	// replicas. There is at most one change per store in this slice (same
	// invariant as PendingRangeChange).
	//
	// Life-cycle matches clusterState.pendingChanges. The consolidated
	// rangeState.pendingChanges across all ranges in clusterState.ranges will
	// be identical to clusterState.pendingChanges.
	pendingChanges []*pendingReplicaChange

	// Nil if conf is nil (since conf is required input), or if constraint
	// analysis fails (e.g., no leaseholder found). When nil, the range is
	// excluded from rebalancing. When non-nil, it is up-to-date and cached to
	// avoid repeating analysis work.
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
	// components call mmaprototype.Allocator.RegisterExternalChange. One example is
	// when MMA does not currently know about a range. Say the lease gets
	// transferred to the local store, but MMA has not yet been called with a
	// StoreLeaseholderMsg, but replicateQueue already knows about this lease,
	// and decides to initiate a transfer of replicas between two remote stores
	// (to equalize replica counts). When mmaprototype.Allocator.RegisterExternalChange
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

func (rs *rangeState) removeReplica(storeID roachpb.StoreID) error {
	var i, n int
	n = len(rs.replicas)
	for ; i < n; i++ {
		if rs.replicas[i].StoreID == storeID {
			rs.replicas[i], rs.replicas[n-1] = rs.replicas[n-1], rs.replicas[i]
			rs.replicas = rs.replicas[:n-1]
			return nil
		}
	}
	return errors.Errorf("store %v has no replica", storeID)
}

func replicaSetIsValid(replicas []StoreIDAndReplicaState) error {
	hasSeenLeaseholder := false
	for _, repl := range replicas {
		if repl.ReplicaState.IsLeaseholder {
			if hasSeenLeaseholder {
				// More than one leaseholder.
				return errors.Errorf("more than one leaseholder")
			}
			hasSeenLeaseholder = true
		}
	}
	if hasSeenLeaseholder {
		return nil
	}
	return errors.Errorf("no leaseholder")
}

func (rs *rangeState) removePendingChangeTracking(changeID changeID) {
	n := len(rs.pendingChanges)
	found := false
	for i := 0; i < n; i++ {
		if rs.pendingChanges[i].changeID == changeID {
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

// clearAnalyzedConstraints clears the analyzed constraints for the range state.
// This should be used when rs is deleted or rs.constraints needs to be reset.
func (rs *rangeState) clearAnalyzedConstraints() {
	if rs.constraints == nil {
		return
	}
	releaseRangeAnalyzedConstraints(rs.constraints)
	rs.constraints = nil
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
//
// REQUIREMENT(change-computation): Any request to compute a change
// (rebalancing stores, or a change specifically for a range), must be atomic
// with the leaseholder providing the current authoritative state for all its
// ranges (for rebalancing) or for the specific range (for a range-specific
// change).
type clusterState struct {
	ts     timeutil.TimeSource
	nodes  map[roachpb.NodeID]*nodeState
	stores map[roachpb.StoreID]*storeState
	// A range is present in the ranges map if any of the local stores is the
	// leaseholder for that range according to the StoreLeaseholderMsgs from the
	// local stores, or if the local leaseholder is transferring the lease to
	// a non-local store. In the latter case, there is a pending change reflecting
	// the lease transfer, and the adjusted range state (which already reflects
	// that transfer) will show the lease on a non-local store. If/once this
	// change gets enacted via a StoreLeaseholderMsg, range state is
	// dropped.
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
	ranges map[roachpb.RangeID]*rangeState

	scratchRangeMap map[roachpb.RangeID]struct{}
	scratchStoreSet storeSet           // scratch space for pre-means filtering
	scratchMeans    meansLoad          // scratch space for recomputed means
	scratchDisj     [1]constraintsConj // scratch space for getMeans call

	// Added to when a change is proposed. Will also add to corresponding
	// rangeState.pendingChanges and to the affected storeStates.
	//
	// Removed from based on RangeMsg (provided by the leaseholder),
	// AdjustPendingChangesDisposition (provided by the enacting module at the
	// leaseholder), or time-based GC.
	pendingChanges map[changeID]*pendingReplicaChange
	changeSeqGen   changeID

	*constraintMatcher
	*localityTierInterner
	meansMemo *meansMemo

	mmaid int // a counter for rebalanceStores calls, for logging
}

func newClusterState(ts timeutil.TimeSource, interner *stringInterner) *clusterState {
	cs := &clusterState{
		ts:                   ts,
		nodes:                map[roachpb.NodeID]*nodeState{},
		stores:               map[roachpb.StoreID]*storeState{},
		ranges:               map[roachpb.RangeID]*rangeState{},
		scratchRangeMap:      map[roachpb.RangeID]struct{}{},
		pendingChanges:       map[changeID]*pendingReplicaChange{},
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
		log.KvDistribution.VEventf(ctx, 2, "s%d not-pending %v", storeMsg.StoreID, change)
		delete(ss.adjusted.loadPendingChanges, change.changeID)
	}

	for _, change := range ss.adjusted.loadPendingChanges {
		// The pending change hasn't been reported as done, re-apply the load
		// delta to the adjusted load and include it in the new adjusted load
		// replicas.
		cs.applyChangeLoadDelta(change.ReplicaChange)
	}
	log.KvDistribution.VEventf(ctx, 2, "processStoreLoadMsg for store s%v: %v, capacity: %v",
		storeMsg.StoreID, ss.adjusted.load, ss.storeLoad.capacity)
}

func (cs *clusterState) processStoreLeaseholderMsg(
	ctx context.Context, msg *StoreLeaseholderMsg, metrics *counterMetrics,
) {
	cs.processStoreLeaseholderMsgInternal(ctx, msg, numTopKReplicas, metrics)
}

func (cs *clusterState) processStoreLeaseholderMsgInternal(
	ctx context.Context, msg *StoreLeaseholderMsg, numTopKReplicas int, metrics *counterMetrics,
) {
	now := cs.ts.Now()
	cs.gcPendingChanges(now)

	clear(cs.scratchRangeMap)
	for _, rangeMsg := range msg.Ranges {
		cs.scratchRangeMap[rangeMsg.RangeID] = struct{}{}
		rs, ok := cs.ranges[rangeMsg.RangeID]
		if !ok {
			// This is the first time we've seen this range.
			if !rangeMsg.MaybeSpanConfIsPopulated {
				panic(errors.AssertionFailedf("rangeMsg for new range r%v is not populated", rangeMsg.RangeID))
			}
			rs = newRangeState(msg.StoreID)
			cs.ranges[rangeMsg.RangeID] = rs
		} else if rs.localRangeOwner != msg.StoreID {
			rs.localRangeOwner = msg.StoreID
		}
		rs.load = rangeMsg.RangeLoad
		if !rangeMsg.MaybeSpanConfIsPopulated && len(rs.pendingChanges) == 0 {
			// Common case: no pending changes, and span config not provided.
			//
			// Confirm that the membership state is consistent. If not, fall through
			// and make it consistent. We have seen an example where
			// AdjustPendingChangesDisposition lied about being successful, and have
			// not been able to find the root cause. So we'd rather force eventual
			// consistency here.
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
				// This is the common case where there were no pending changes, and no
				// span config provided and no replicas changes. We don't need to do
				// any more processing of this RangeMsg.
				continue
			}
			// Else fall through and do the expensive work.
		}
		// Slow path, which reconstructs everything about the range.

		// Set the range state and store state to match the range message state
		// initially. The pending changes which are not enacted in the range
		// message are handled and added back below.
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
		// leaseholder. Note that this is the only code path where a subset of
		// pending changes for a replica can be considered enacted.
		var remainingChanges, enactedChanges []*pendingReplicaChange
		for _, change := range rs.pendingChanges {
			ss := cs.stores[change.target.StoreID]
			adjustedReplica, ok := ss.adjusted.replicas[rangeMsg.RangeID]
			if !ok {
				adjustedReplica.ReplicaID = noReplicaID
			}
			if adjustedReplica.subsumesChange(change.next) {
				// The change has been enacted according to the leaseholder.
				enactedChanges = append(enactedChanges, change)
			} else {
				// Not subsumed. Replace the prev with the latest source of truth from
				// the leaseholder. Note, this can be the noReplicaID case from above.
				change.prev = adjustedReplica
				remainingChanges = append(remainingChanges, change)
			}
		}
		if len(enactedChanges) > 0 && len(remainingChanges) > 0 {
			// There are remaining changes, so potentially update their gcTime.
			//
			// All remaining changes have the same gcTime.
			curGCTime := remainingChanges[0].gcTime
			revisedGCTime := now.Add(partiallyEnactedGCDuration)
			if revisedGCTime.Before(curGCTime) {
				// Shorten when the GC happens.
				for _, change := range remainingChanges {
					change.gcTime = revisedGCTime
				}
			}
		}
		// rs.pendingChanges is the union of remainingChanges and enactedChanges.
		// These changes are also in cs.pendingChanges.
		if len(enactedChanges) > 0 {
			log.KvDistribution.Infof(ctx, "enactedChanges %v", enactedChanges)
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
			cs.pendingChangeEnacted(change.changeID, now)
		}
		// INVARIANT: remainingChanges and rs.pendingChanges contain the same set
		// of changes, though possibly in different order.

		// rs.pendingChanges only contains remainingChanges, and these are also in
		// cs.pendingChanges and storeState's loadPendingChanges. Their load
		// effect is also incorporated into the storeStates, but not in the range
		// membership (since we undid that above).
		if len(remainingChanges) > 0 {
			log.KvDistribution.Infof(ctx, "remainingChanges %v", remainingChanges)
			// Temporarily set the rs.pendingChanges to nil, since
			// preCheckOnApplyReplicaChanges returns false if there are any pending
			// changes, and these are the changes that are pending. This is hacky
			// and should be cleaned up.
			//
			// NB: rs.pendingChanges contains the same changes as
			// remainingChanges, but they are not the same slice.
			rc := rs.pendingChanges
			rs.pendingChanges = nil
			err := cs.preCheckOnApplyReplicaChanges(PendingRangeChange{
				RangeID:               rangeMsg.RangeID,
				pendingReplicaChanges: remainingChanges,
			})
			// Restore it.
			rs.pendingChanges = rc

			if err == nil {
				// Re-apply the remaining changes. Note that the load change was not
				// undone above, so we pass !applyLoadChange, to avoid applying it
				// again. Also note that applyReplicaChange does not add to the various
				// pendingChanges data-structures, which is what we want here since
				// these changes are already in those data-structures.
				for _, change := range remainingChanges {
					cs.applyReplicaChange(change.ReplicaChange, false)
				}
			} else {
				reason := redact.Sprint(err)
				// The current state provided by the leaseholder does not permit these
				// changes, so we need to drop them. This should be rare, but can happen
				// if the leaseholder executed a change that MMA was completely unaware
				// of.
				log.KvDistribution.Infof(ctx, "remainingChanges %v are no longer valid due to %v",
					remainingChanges, reason)
				if metrics != nil {
					metrics.DroppedDueToStateInconsistency.Inc(1)
				}
				// We did not undo the load change above, or remove it from the various
				// pendingChanges data-structures. We do those things now.
				for _, change := range remainingChanges {
					rs.removePendingChangeTracking(change.changeID)
					delete(cs.stores[change.target.StoreID].adjusted.loadPendingChanges, change.changeID)
					delete(cs.pendingChanges, change.changeID)
					cs.undoChangeLoadDelta(change.ReplicaChange)
				}
				if n := len(rs.pendingChanges); n > 0 {
					panic(errors.AssertionFailedf("expected no pending changes but found %d", n))
				}
			}
		}
		if rangeMsg.MaybeSpanConfIsPopulated {
			normSpanConfig, err := makeNormalizedSpanConfig(&rangeMsg.MaybeSpanConf, cs.constraintMatcher.interner)
			if err != nil {
				log.KvDistribution.Warningf(
					ctx, "range r%v span config had errors in normalization: %v, normalized result: %v", rangeMsg.RangeID, err, normSpanConfig)
				metrics.incSpanConfigNormalizationErrorIfNonNil()
			}
			rs.conf = normSpanConfig
		}
		// Ensure (later) recomputation of the analyzed range constraints for the
		// range, by clearing the existing analyzed constraints. This is done
		// since in this slow path the span config or the replicas may have
		// changed.
		rs.clearAnalyzedConstraints()
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
		//
		// Gather the changeIDs, since calls to pendingChangeEnacted modify the
		// rs.pendingChanges slice.
		changeIDs := make([]changeID, len(rs.pendingChanges))
		for i, change := range rs.pendingChanges {
			changeIDs[i] = change.changeID
		}
		for _, changeID := range changeIDs {
			cs.pendingChangeEnacted(changeID, now)
		}
		// Remove from the storeStates.
		for _, replica := range rs.replicas {
			ss := cs.stores[replica.StoreID]
			if ss == nil {
				panic(fmt.Sprintf("store %d not found stores=%v", replica.StoreID, cs.stores))
			}
			delete(cs.stores[replica.StoreID].adjusted.replicas, r)
		}
		rs.clearAnalyzedConstraints()
		delete(cs.ranges, r)
	}
	localss := cs.stores[msg.StoreID]
	cs.meansMemo.clear()
	clusterMeans := cs.meansMemo.getMeans(nil)
	// Sort by store ID for deterministic log output in tests.
	storeIDs := make([]roachpb.StoreID, 0, len(cs.stores))
	for storeID := range cs.stores {
		storeIDs = append(storeIDs, storeID)
	}
	slices.Sort(storeIDs)
	for _, storeID := range storeIDs {
		ss := cs.stores[storeID]
		topk := ss.adjusted.topKRanges[msg.StoreID]
		if topk == nil {
			topk = &topKReplicas{k: numTopKReplicas}
			ss.adjusted.topKRanges[msg.StoreID] = topk
		}
		topk.startInit()
		sls := cs.computeLoadSummary(ctx, ss.StoreID, &clusterMeans.storeLoad, &clusterMeans.nodeLoad)
		if ss.StoreID == localss.StoreID {
			topk.dim = CPURate
		} else {
			topk.dim = WriteBandwidth
		}
		if sls.highDiskSpaceUtilization {
			// If disk space is running out, shedding bytes becomes the top priority.
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
		// The max(0, ...) is defensive, in case the adjusted load is negative.
		// Given that this is a most overloaded dim, the likelihood of the
		// adjusted load being negative is very low.
		adjustedStoreLoadValue := max(0, ss.adjusted.load[topk.dim])
		threshold := LoadValue(float64(adjustedStoreLoadValue) * fraction)
		if ss.reportedSecondaryLoad[ReplicaCount] > 0 {
			// Allow all ranges above 90% of the mean. This is quite arbitrary.
			meanLoad := (adjustedStoreLoadValue * 9) / (ss.reportedSecondaryLoad[ReplicaCount] * 10)
			threshold = min(meanLoad, threshold)
		}
		topk.threshold = threshold
	}
	// NB: localss.adjusted.replicas is already adjusted for ongoing changes,
	// which may be undone. For example, if s1 is the local store, and it is
	// transferring its replica for range r1 to remote store s2, then
	// localss.adjusted.replicas will not have r1. This is fine in that s1
	// should not be making more decisions about r1. If the change is undone
	// later, we will again compute the top-k, which will consider r1, before
	// computing new changes (due to REQUIREMENT(change-computation)).
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
		// NB: rs.replicas is already adjusted for ongoing changes. For
		// example, if s10 is a remote replica for range r1, which is being
		// transferred to another store s11, s10 will not see r1 below. We
		// make this choice to avoid cluttering the top-k for s10 with
		// replicas that are going away. If it is undone, r1 will not be in
		// the top-k for s10, but due to REQUIREMENT(change-computation), a
		// new authoritative state will be provided and the top-k recomputed,
		// before computing any new changes.
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

// If the pending replica change does not happen within this GC duration, we
// forget it in the data-structure.
const pendingReplicaChangeGCDuration = 5 * time.Minute

// If the pending lease transfer does not happen within this GC duration, we
// forget it in the data-structure.
const pendingLeaseTransferGCDuration = 1 * time.Minute

// partiallyEnactedGCDuration is the duration after which a pending change is
// GC'd if some other change that was part of the same decision has been
// enacted, and this duration has elapsed since that enactment. This is
// shorter than the normal pendingReplicaChangeGCDuration, since we want to
// clean up such partially enacted changes faster. Long-running decisions are
// those that involve a new range snapshot being sent, and that is the first
// change that is seen as enacted. Subsequent ones should be fast.
const partiallyEnactedGCDuration = 30 * time.Second

// Called periodically by allocator.
func (cs *clusterState) gcPendingChanges(now time.Time) {
	var rangesWithChanges map[roachpb.RangeID]struct{}
	for _, pendingChange := range cs.pendingChanges {
		if rangesWithChanges == nil {
			rangesWithChanges = make(map[roachpb.RangeID]struct{})
		}
		rangesWithChanges[pendingChange.rangeID] = struct{}{}
	}
	for rangeID := range rangesWithChanges {
		rs, ok := cs.ranges[rangeID]
		if !ok {
			panic(errors.AssertionFailedf("range %v not found in cluster state", rangeID))
		}
		if len(rs.pendingChanges) == 0 {
			panic(errors.AssertionFailedf("no pending changes in range %v", rangeID))
		}
		gcTime := rs.pendingChanges[0].gcTime
		if !gcTime.Before(now) {
			continue
		}
		if err := cs.preCheckOnUndoReplicaChanges(PendingRangeChange{
			RangeID:               rangeID,
			pendingReplicaChanges: rs.pendingChanges,
		}); err != nil {
			panic(err)
		}
		// Gather the changeIDs, since calls to undoPendingChange modify the
		// rs.pendingChanges slice.
		var changeIDs []changeID
		for _, pendingChange := range rs.pendingChanges {
			changeIDs = append(changeIDs, pendingChange.changeID)
		}
		for _, changeID := range changeIDs {
			cs.undoPendingChange(changeID)
		}
	}
}

func (cs *clusterState) pendingChangeEnacted(cid changeID, enactedAt time.Time) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		panic(fmt.Sprintf("change %v not found %v", cid, printMapPendingChanges(cs.pendingChanges)))
	}
	change.enactedAtTime = enactedAt
	rs, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}

	rs.removePendingChangeTracking(change.changeID)
	delete(cs.pendingChanges, change.changeID)
}

// undoPendingChange reverses the change with ID cid.
func (cs *clusterState) undoPendingChange(cid changeID) {
	change, ok := cs.pendingChanges[cid]
	if !ok {
		panic(errors.AssertionFailedf("change %v not found %v", cid, printMapPendingChanges(cs.pendingChanges)))
	}
	rs, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(errors.AssertionFailedf("range %v not found in cluster state", change.rangeID))
	}
	// Wipe the analyzed constraints, as the range has changed.
	rs.clearAnalyzedConstraints()
	rs.lastFailedChange = cs.ts.Now()
	// Undo the change delta as well as the replica change and remove the pending
	// change from all tracking (range, store, cluster).
	cs.undoReplicaChange(change.ReplicaChange)
	rs.removePendingChangeTracking(cid)
	delete(cs.stores[change.target.StoreID].adjusted.loadPendingChanges, change.changeID)
	delete(cs.pendingChanges, change.changeID)
}

func printMapPendingChanges(changes map[changeID]*pendingReplicaChange) string {
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

// addPendingRangeChange takes a range change containing a set of replica
// changes, and applies the changes as pending. The application updates the
// adjusted load, tracked pending changes and changeIDs to reflect the pending
// application. It updates the *pendingReplicaChanges inside the change.
//
// REQUIRES: all the replica changes are to the same range, and that the range
// has no pending changes.
func (cs *clusterState) addPendingRangeChange(change PendingRangeChange) {
	if len(change.pendingReplicaChanges) == 0 {
		return
	}
	rangeID := change.RangeID
	for _, c := range change.pendingReplicaChanges {
		if c.rangeID != rangeID {
			panic(errors.AssertionFailedf("all changes must be to the same range %d != %d",
				c.rangeID, rangeID))
		}
	}
	rs := cs.ranges[rangeID]
	if rs != nil && len(rs.pendingChanges) > 0 {
		panic(errors.AssertionFailedf("range r%v already has pending changes: %d",
			rangeID, len(rs.pendingChanges)))
	}
	// NB: rs != nil is also required, but we also check that in a method called
	// below.

	gcDuration := pendingReplicaChangeGCDuration
	if change.isPureTransferLease() {
		// Only the lease is being transferred.
		gcDuration = pendingLeaseTransferGCDuration
	}
	now := cs.ts.Now()
	for _, pendingChange := range change.pendingReplicaChanges {
		cs.applyReplicaChange(pendingChange.ReplicaChange, true)
		cs.changeSeqGen++
		cid := cs.changeSeqGen
		pendingChange.changeID = cid
		pendingChange.startTime = now
		pendingChange.gcTime = now.Add(gcDuration)
		pendingChange.enactedAtTime = time.Time{}
		storeState := cs.stores[pendingChange.target.StoreID]
		rangeState := cs.ranges[rangeID]
		cs.pendingChanges[cid] = pendingChange
		storeState.adjusted.loadPendingChanges[cid] = pendingChange
		rangeState.pendingChanges = append(rangeState.pendingChanges, pendingChange)
		log.KvDistribution.VEventf(context.Background(), 3,
			"addPendingRangeChange: change_id=%v, range_id=%v, change=%v",
			cid, rangeID, pendingChange.ReplicaChange)
	}
}

// preCheckOnApplyReplicaChanges does some validation of the changes being
// proposed. It ensures the range is known and has no pending changes already.
//
// It only needs to be called for (a) new changes that are being proposed, or
// (b) when we have reset the rangeState.replicas using a StoreLeaseholderMsg
// and we have some previously proposed pending changes that have not been
// enacted yet, and we want to re-validate them before adjusting
// rangeState.replicas.
//
// For a removal, it validates that the replica exists. For non-removal, it
// blind applies the change without validating whether the current state is
// ReplicaChange.prev -- this blind application allows this pre-check to
// succeed when a ReplicaChange has been partially applied e.g. a replica has
// been added at a store, but it has not yet received the lease. Finally, it
// checks that after the changes are applied there is exactly one leaseholder.
// It returns a non-nil error if any of these checks fail.
func (cs *clusterState) preCheckOnApplyReplicaChanges(rangeChange PendingRangeChange) error {
	if len(rangeChange.pendingReplicaChanges) == 0 {
		return nil
	}
	rangeID := rangeChange.RangeID
	curr, ok := cs.ranges[rangeID]
	// Return early if range already has some pending changes or the range does not exist.
	if !ok {
		return errors.Errorf("range does not exist in cluster state")
	}
	if len(curr.pendingChanges) > 0 {
		return errors.Errorf("range %d has pending changes: %v",
			rangeID, curr.pendingChanges)
	}

	// Make a deep copy of the range state
	copiedCurr := rangeState{
		replicas: append([]StoreIDAndReplicaState{}, curr.replicas...),
	}
	for _, change := range rangeChange.pendingReplicaChanges {
		// Check that all changes correspond to the same range. Panic otherwise.
		if change.rangeID != rangeID {
			panic(errors.AssertionFailedf("unexpected change rangeID %d != %d", change.rangeID, rangeID))
		}
		if change.isRemoval() {
			if err := copiedCurr.removeReplica(change.target.StoreID); err != nil {
				return err
			}
		} else if change.isAddition() || change.isUpdate() {
			// NB: see the blind apply comment on the method declaration.
			pendingRepl := StoreIDAndReplicaState{
				StoreID: change.target.StoreID,
				ReplicaState: ReplicaState{
					ReplicaIDAndType: change.next,
				},
			}
			copiedCurr.setReplica(pendingRepl)
		} else {
			panic(errors.AssertionFailedf("unknown replica change %+v", change))
		}
	}
	// check on the final state of currCopy and whether it is valid
	return replicaSetIsValid(copiedCurr.replicas)
}

// preCheckOnUndoReplicaChanges does some validation of the changes being
// proposed for undo.
//
// This method is defensive since if we always check against the current state
// before allowing a change to be added (including re-addition after a
// StoreLeaseholderMsg), we should never have invalidity during an undo, if
// all the changes are being undone.
func (cs *clusterState) preCheckOnUndoReplicaChanges(rangeChange PendingRangeChange) error {
	if len(rangeChange.pendingReplicaChanges) == 0 {
		return nil
	}
	rangeID := rangeChange.RangeID
	curr, ok := cs.ranges[rangeID]
	if !ok {
		return errors.Errorf("range %v does not exist in cluster state", rangeID)
	}

	copiedCurr := &rangeState{
		replicas: append([]StoreIDAndReplicaState{}, curr.replicas...),
	}
	for _, change := range rangeChange.pendingReplicaChanges {
		if change.rangeID != rangeID {
			panic(errors.AssertionFailedf("unexpected change rangeID %d != %d", change.rangeID, rangeID))
		}
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
			if err := copiedCurr.removeReplica(change.target.StoreID); err != nil {
				return err
			}
		} else {
			panic(errors.AssertionFailedf("unknown replica change %+v", change))
		}
	}
	if err := replicaSetIsValid(copiedCurr.replicas); err != nil {
		return err
	}
	return nil
}

// REQUIRES: the range and store are known to the allocator.
func (cs *clusterState) applyReplicaChange(change ReplicaChange, applyLoadChange bool) {
	storeState, ok := cs.stores[change.target.StoreID]
	if !ok {
		panic(fmt.Sprintf("store %v not found in cluster state", change.target.StoreID))
	}
	rangeState, ok := cs.ranges[change.rangeID]
	if !ok {
		panic(fmt.Sprintf("range %v not found in cluster state", change.rangeID))
	}

	log.KvDistribution.VEventf(context.Background(), 2, "applying replica change %v to range %d on store %d",
		change, change.rangeID, change.target.StoreID)
	if change.isRemoval() {
		delete(storeState.adjusted.replicas, change.rangeID)
		if err := rangeState.removeReplica(change.target.StoreID); err != nil {
			panic(err)
		}
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
	log.KvDistribution.Infof(context.Background(), "undoing replica change %v to range %d on store %d",
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
		if err := rangeState.removeReplica(change.target.StoreID); err != nil {
			panic(err)
		}
	} else {
		panic(fmt.Sprintf("unknown replica change %+v", change))
	}
	cs.undoChangeLoadDelta(change)
}

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
func (cs *clusterState) setStore(sal storeAttributesAndLocalityWithNodeTier) {
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
		// TODO(tbg): below is what we should be doing once asim and production code actually
		// have a way to update the health status. For now, we just set it to healthy initially
		// and that's where it will stay (outside of unit tests).
		//
		// At this point, the store's health is unknown. It will need to be marked
		// as healthy separately. Until we know more, we won't place leases or
		// replicas on it (nor will we try to shed any that are already reported to
		// have replicas on it).
		// ss.status = MakeStatus(HealthUnknown, LeaseDispositionRefusing, ReplicaDispositionRefusing)
		ss.status = MakeStatus(HealthOK, LeaseDispositionOK, ReplicaDispositionOK)
		ss.overloadStartTime = cs.ts.Now()
		ss.overloadEndTime = cs.ts.Now()
		cs.stores[sal.StoreID] = ss
		ns.stores = append(ns.stores, sal.StoreID)
	}

	// If the store is new or the locality/attributes changed, we need to update
	// the locality and attributes.
	loc := sal.NodeLocality
	oldAttrs := cs.stores[sal.StoreID].storeAttributesAndLocalityWithNodeTier
	locsChanged := cs.localityTierInterner.changed(cs.stores[sal.StoreID].localityTiers, loc)
	attrsChanged := !slices.Equal(
		oldAttrs.StoreAttrs.Attrs,
		sal.StoreAttrs.Attrs,
	) || !slices.Equal(
		oldAttrs.NodeAttrs.Attrs,
		sal.NodeAttrs.Attrs,
	)

	if !ok || attrsChanged || locsChanged {
		cs.stores[sal.StoreID].localityTiers = cs.localityTierInterner.intern(loc)
		cs.stores[sal.StoreID].storeAttributesAndLocalityWithNodeTier = sal
		cs.constraintMatcher.setStore(sal)
	}
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

// canShedAndAddLoad returns true if the delta can be added to the target store
// and removed from the src store. It does not change any state between the call
// and return.
//
// overloadDim represents the dimension that is overloaded in the source and the
// function requires that along that dimension, the target is < loadNoChange and
// the source is > loadNoChange.
//
// Broadly speaking, the method tries to ascertain that the target wouldn't be
// worse off than the source following the transfer. To do this, the method
// looks at a load summary for the target that would result from the load
// transfer (targetLoadSummary).
//
// When onlyConsiderTargetCPUSummary is true, the targetLoadSummary derives from
// the target's post-transfer CPU dimension only. This is appropriate when a lease is
// transferred, as this should only affect the CPU dimension, and we don't want
// lease transfers to be subject to stricter checks related to other dimensions.
// When onlyConsiderTargetCPUSummary is false, targetLoadSummary is the target's
// worst post-transfer load summary. In both cases, the node load summary is also
// considered.
//
// TODO(tbg): understand and explain why the node load summary is in the mix here.
//
// In either case, if the targetLoadSummary is < loadNoChange, the change is
// permitted right away. Otherwise, stricter checks apply: After the transfer,
// - the target must not be overloadUrgent,
// - the target has no pending changes (to delay making a potentially non-ideal
//   choice of the target),
// - the target's overloaded dimension's summary must not be worse than the
//   source's ("overloadedDimPermitsChange"),
// - along each of the other (!=overloadeDim) dimensions, the percentage
//   increase in load is at most a third of that of the overloaded dimension.
//   (e.g. if CPU goes up by 30%, WriteBandwidth can go up by at most 10%).
// - the target's node load summary must not be worse than the target's store
//   load summary. See inline comment for more details.

func (cs *clusterState) canShedAndAddLoad(
	ctx context.Context,
	srcSS *storeState,
	targetSS *storeState,
	delta LoadVector,
	means *meansLoad,
	onlyConsiderTargetCPUSummary bool,
	overloadedDim LoadDimension,
) (canAddLoad bool) {
	if overloadedDim == NumLoadDimensions {
		panic("overloadedDim must not be NumLoadDimensions")
	}
	// TODO(tbg): in experiments, we often see interesting behavior right when
	// the load delta addition flips the loadSummary for either the target or the
	// source, which suggests it might be useful to add this to verbose logging.

	// Compute srcSLS and targetSLS, which are the load summaries of the source
	// and target that would result from moving the lease.
	//
	// TODO(tbg): extract this into a helper and set it up so that it doesn't
	// temporarily modify the cluster state.
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
	targetSLS := computeLoadSummary(ctx, targetSS, targetNS, &means.storeLoad, &means.nodeLoad)
	// Undo the addition.
	targetSS.adjusted.load.subtract(deltaToAdd)
	targetNS.adjustedCPU -= deltaToAdd[CPURate]

	// Remove the delta.
	srcNS := cs.nodes[srcSS.NodeID]
	srcSS.adjusted.load.subtract(delta)
	srcNS.adjustedCPU -= delta[CPURate]
	srcSLS := computeLoadSummary(ctx, srcSS, srcNS, &means.storeLoad, &means.nodeLoad)
	// Undo the removal.
	srcSS.adjusted.load.add(delta)
	srcNS.adjustedCPU += delta[CPURate]

	var reason strings.Builder
	defer func() {
		if canAddLoad {
			log.KvDistribution.VEventf(ctx, 3, "can add load to n%vs%v: %v targetSLS[%v] srcSLS[%v]",
				targetNS.NodeID, targetSS.StoreID, canAddLoad, targetSLS, srcSLS)
		} else {
			log.KvDistribution.VEventf(ctx, 2, "cannot add load to n%vs%v: due to %s", targetNS.NodeID, targetSS.StoreID, reason.String())
			log.KvDistribution.VEventf(ctx, 2, "[target_sls:%v,src_sls:%v]", targetSLS, srcSLS)
		}
	}()
	if targetSLS.highDiskSpaceUtilization {
		reason.WriteString("targetSLS.highDiskSpaceUtilization")
		return false
	}

	// We define targetSummary as a "worst" of the considered load dimesions
	// (only CPU, or all).
	var targetSummary loadSummary
	if onlyConsiderTargetCPUSummary {
		targetSummary = targetSLS.dimSummary[CPURate]
	} else {
		targetSummary = targetSLS.sls
	}
	targetSummary = max(targetSummary, targetSLS.nls)

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
	var overloadedDimFractionIncrease float64
	if targetSS.adjusted.load[overloadedDim] > 0 {
		overloadedDimFractionIncrease = float64(deltaToAdd[overloadedDim]) /
			float64(targetSS.adjusted.load[overloadedDim])
	} else {
		// Else, the adjusted load on the overloadedDim is zero or negative, which
		// is possible, but extremely rare in practice. We arbitrarily set the
		// fraction increase to 1.0 in this case.
		overloadedDimFractionIncrease = 1.0
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
		if targetSS.adjusted.load[dim] > 0 {
			dimFractionIncrease := float64(deltaToAdd[dim]) / float64(targetSS.adjusted.load[dim])
			// The use of 33% is arbitrary.
			if dimFractionIncrease > overloadedDimFractionIncrease/3 {
				log.KvDistribution.Infof(ctx, "%v: %f > %f/3", dim, dimFractionIncrease, overloadedDimFractionIncrease)
				otherDimensionsBecameWorseInTarget = true
				break
			}
		}
		// Else the adjusted load in dimension dim is zero or negative, which is
		// possible, but extremely rare in practice. We ignore this dimension in
		// that case.
	}
	canAddLoad = overloadedDimPermitsChange && !otherDimensionsBecameWorseInTarget &&
		// NB: the target here is quite loaded, so we are stricter than in other
		// places and require that there are *no* pending changes (rather than
		// a threshold fraction).
		targetSLS.maxFractionPendingIncrease < epsilon &&
		targetSLS.maxFractionPendingDecrease < epsilon &&
		// NB: targetSLS.nls <= targetSLS.sls is not a typo, in that we are
		// comparing targetSLS with itself.
		//
		// Consider a node that has two stores:
		// - s1 is low on CPU
		// - s2 is very high on CPU, resulting in a node load summary of
		// overloadSlow or overloadUrgent)
		//
		// In this code path, targetSLS is >= loadNoChange, so there must be
		// some overload dimension in targetSLS. If it comes from write bandwidth
		// (or any other non-CPU dimension), without this check,s1 might be
		// considered an acceptable target for adding CPU load. But it is clearly
		// not a good target, since the node housing s1 is CPU overloaded - s2
		// should be shedding CPU load first.
		// This example motivates the condition below. If we reach this code,
		// we know that targetSLS >= loadNoChange, and we decide:
		// - at sls=loadNoChange, we require nls <= loadNoChange
		// - at sls=overloadSlow, we require nls <= overloadSlow
		// - at sls=overloadUrgent, we require nls <= overloadUrgent.
		// In other words, whenever a node level summary was "bumped up" beyond
		// the target's by some other local store, we reject the change.
		//
		// TODO(tbg): While the example illustrates that "something had to be
		// done", I don't understand why it makes sense to solve this exactly
		// as it was done. The node level summary is based on node-wide CPU
		// utilization as well as its distance from the mean (across the
		// candidate set). Store summaries a) reflect the worst dimension, and
		// b) on the CPU dimension are based on the store-apportioned capacity.
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
	ctx context.Context, storeID roachpb.StoreID, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	ss := cs.stores[storeID]
	ns := cs.nodes[ss.NodeID]
	return computeLoadSummary(ctx, ss, ns, msl, mnl)
}

// TODO(wenyihu6): check to make sure obs here is correct
func (cs *clusterState) loadSummaryForAllStores(ctx context.Context) string {
	var b strings.Builder
	clusterMeans := cs.meansMemo.getMeans(nil)
	b.WriteString(fmt.Sprintf("cluster means: (stores-load %s) (stores-capacity %s)\n",
		clusterMeans.storeLoad.load, clusterMeans.storeLoad.capacity))
	b.WriteString(fmt.Sprintf("(nodes-cpu-load %d) (nodes-cpu-capacity %d)\n",
		clusterMeans.nodeLoad.loadCPU, clusterMeans.nodeLoad.capacityCPU))
	for storeID, ss := range cs.stores {
		sls := cs.meansMemo.getStoreLoadSummary(ctx, clusterMeans, storeID, ss.loadSeqNum)
		b.WriteString(fmt.Sprintf("evaluating store s%d for shedding: load summary %v", storeID, sls))
	}
	return b.String()
}

func computeLoadSummary(
	ctx context.Context, ss *storeState, ns *nodeState, msl *meanStoreLoad, mnl *meanNodeLoad,
) storeLoadSummary {
	sls := loadLow
	var highDiskSpaceUtil bool
	var dimSummary [NumLoadDimensions]loadSummary
	var worstDim LoadDimension
	for i := range msl.load {
		ls := loadSummaryForDimension(ctx, ss.StoreID, nodeIDForLogging, LoadDimension(i), ss.adjusted.load[i], ss.capacity[i],
			msl.load[i], msl.util[i])
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
	nls := loadSummaryForDimension(ctx, storeIDForLogging, ns.NodeID, CPURate, ns.adjustedCPU, ns.CapacityCPU, mnl.loadCPU, mnl.utilCPU)
	return storeLoadSummary{
		worstDim:   worstDim,
		sls:        sls,
		nls:        nls,
		dimSummary: dimSummary,
		// TODO(tbg): remove highDiskSpaceUtilization.
		highDiskSpaceUtilization:   highDiskSpaceUtil,
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

// storeAttributesAndLocalityWithNodeTier is the mma-internal representation of
// StoreAttributesAndLocality. It differs in that the Locality has an extra tier
// `node=<node-id>`.
type storeAttributesAndLocalityWithNodeTier StoreAttributesAndLocality

// locality returns the locality of the Store, which is the Locality of the
// node plus an extra tier for the node itself. Copied from
// StoreDescriptor.Locality.
func (saal StoreAttributesAndLocality) withNodeTier() storeAttributesAndLocalityWithNodeTier {
	salwt := storeAttributesAndLocalityWithNodeTier(saal)
	salwt.NodeLocality = saal.NodeLocality.AddTier(
		roachpb.Tier{Key: "node", Value: saal.NodeID.String()})
	return salwt
}

// Avoid unused lint errors.
var _ = rangeState{}.diversityIncreaseLastFailedAttempt
