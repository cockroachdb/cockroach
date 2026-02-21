// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ExternalRangeChange is a proposed set of change(s) to a range. It can
// consist of multiple replica changes, such as adding or removing replicas,
// or transferring the lease. There is at most one change per store in the
// set. It is immutable after creation.
//
// It is a partial external representation of a set of changes that are
// internally modeled using a slice of *pendingReplicaChanges.
type ExternalRangeChange struct {
	origin       ChangeOrigin
	localStoreID roachpb.StoreID

	roachpb.RangeID
	Changes []ExternalReplicaChange
}

// ExternalReplicaChange is a proposed change to a single replica. Some
// external entity (the leaseholder of the range) may choose to enact this
// change.
//
// It is a partial external representation of a pendingReplicaChange that is
// internal to MMA. While pendingReplicaChange has fields that are mutable,
// since partial changes can be observed to a store, the ExternalReplicaChange
// is immutable and represents the original before and after state of the
// change.
type ExternalReplicaChange struct {
	changeID
	// Target is the target {store, node} for the change.
	Target roachpb.ReplicationTarget
	// Prev is the state before the change. See the detailed comment in
	// ReplicaChange.prev, which is shared by this field (except that this is
	// immutable).
	Prev ReplicaIDAndType
	// Next is the state after the change. See the detailed comment in
	// ReplicaChange.next, which is shared by this field.
	Next ReplicaIDAndType
	// ChangeType is a function of (Prev, Next) and a convenience.
	ChangeType ReplicaChangeType
}

func MakeExternalRangeChange(
	origin ChangeOrigin, localStoreID roachpb.StoreID, change PendingRangeChange,
) ExternalRangeChange {
	changes := make([]ExternalReplicaChange, len(change.pendingReplicaChanges))
	for i, rc := range change.pendingReplicaChanges {
		changeType := rc.replicaChangeType()
		if changeType == Unknown {
			panic(errors.AssertionFailedf("unknown replica change type"))
		}
		changes[i] = ExternalReplicaChange{
			changeID:   rc.changeID,
			Target:     rc.target,
			Prev:       rc.prev.ReplicaIDAndType,
			Next:       rc.next,
			ChangeType: changeType,
		}
	}
	return ExternalRangeChange{
		origin:       origin,
		localStoreID: localStoreID,
		RangeID:      change.RangeID,
		Changes:      changes,
	}
}

func (rc *ExternalRangeChange) String() string {
	return redact.StringWithoutMarkers(rc)
}

// SafeFormat implements the redact.SafeFormatter interface.
//
// This is adhoc for debugging. A nicer string format would include the
// previous state and next state.
func (rc *ExternalRangeChange) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("r%v=[", rc.RangeID)
	found := false
	if rc.IsPureTransferLease() {
		w.Printf("transfer_to=%v", rc.LeaseTransferTarget())
		found = true
	}
	if rc.IsChangeReplicas() {
		w.Printf("change_replicas=%v", rc.ReplicationChanges())
		found = true
	}
	if !found {
		panic("unknown change type")
	}
	w.SafeString(" cids=")
	for i, c := range rc.Changes {
		if i > 0 {
			w.SafeRune(',')
		}
		w.Printf("%v", c.changeID)
	}
	w.SafeRune(']')
}

// TODO(sumeer): A single ExternalRangeChange can model a bunch of replica
// changes and a lease transfer. Classifying the change as either
// IsChangeReplicas or IsPureTransferLease is unnecessarily limiting. The only
// code that really relies on this is integration code:
// mma_store_rebalancer.go, allocator_sync.go, asim. We should fix those and
// consider removing these methods.

// IsChangeReplicas returns true if the range change is a change replicas
// operation.
func (rc *ExternalRangeChange) IsChangeReplicas() bool {
	for _, c := range rc.Changes {
		if c.ChangeType == AddLease || c.ChangeType == RemoveLease {
			return false
		}
	}
	return true
}

// IsPureTransferLease returns true if the range change is purely a transfer
// lease operation (i.e., it is not a combined replication change and lease
// transfer).
func (rc *ExternalRangeChange) IsPureTransferLease() bool {
	if len(rc.Changes) != 2 {
		return false
	}
	var addLease, removeLease int
	for _, c := range rc.Changes {
		switch c.ChangeType {
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

// ReplicationChanges returns the replication changes for the range change. It
// panics if the range change is not a change replicas operation.
//
// TODO(tbg): The ReplicationChanges can include a new leaseholder replica,
// but the incoming leaseholder is not explicitly represented in
// kvpb.ReplicationChanges. This is an existing modeling deficiency in the
// kvserver code. In Replica.maybeTransferLeaseDuringLeaveJoint the first
// VOTER_INCOMING is considered the new leaseholder. So the code below places
// the new leaseholder (an ADD_VOTER) at index 0. It is not clear whether this
// is sufficient for all integration use-cases. Verify and fix as needed.
//
// TODO(sumeer): this method is limiting, since a single PendingRangeChange
// should be allowed to model any set of changes (see the existing TODO on
// IsChangeReplicas).
func (rc *ExternalRangeChange) ReplicationChanges() kvpb.ReplicationChanges {
	if !rc.IsChangeReplicas() {
		panic("RangeChange is not a change replicas")
	}
	chgs := make([]kvpb.ReplicationChange, 0, len(rc.Changes))
	newLeaseholderIndex := -1
	for _, c := range rc.Changes {
		switch c.ChangeType {
		case ChangeReplica, AddReplica, RemoveReplica:
			// These are the only permitted cases.
		default:
			panic(errors.AssertionFailedf("change type %v is not a change replicas", c.ChangeType))
		}
		// The kvserver code represents a change in replica type as an
		// addition and a removal of the same replica. For example, if a
		// replica changes from VOTER_FULL to NON_VOTER, we will emit a pair
		// of {ADD_NON_VOTER, REMOVE_VOTER} for the replica. The ordering of
		// this pair does not matter.
		//
		// TODO(tbg): confirm that the ordering does not matter.
		if c.ChangeType == ChangeReplica || c.ChangeType == AddReplica {
			chg := kvpb.ReplicationChange{Target: c.Target}
			isNewLeaseholder := false
			switch c.Next.ReplicaType.ReplicaType {
			case roachpb.VOTER_FULL:
				chg.ChangeType = roachpb.ADD_VOTER
				if c.Next.IsLeaseholder {
					isNewLeaseholder = true
				}
			case roachpb.NON_VOTER:
				chg.ChangeType = roachpb.ADD_NON_VOTER
			default:
				panic(errors.AssertionFailedf("unexpected replica type %s", c.Next.ReplicaType.ReplicaType))
			}
			if isNewLeaseholder {
				if newLeaseholderIndex >= 0 {
					panic(errors.AssertionFailedf(
						"multiple new leaseholders in change replicas"))
				}
				newLeaseholderIndex = len(chgs)
			}
			chgs = append(chgs, chg)
		}
		if c.ChangeType == ChangeReplica || c.ChangeType == RemoveReplica {
			chg := kvpb.ReplicationChange{Target: c.Target}
			prevType := mapReplicaTypeToVoterOrNonVoter(c.Prev.ReplicaType.ReplicaType)
			switch prevType {
			case roachpb.VOTER_FULL:
				chg.ChangeType = roachpb.REMOVE_VOTER
			case roachpb.NON_VOTER:
				chg.ChangeType = roachpb.REMOVE_NON_VOTER
			default:
				panic(errors.AssertionFailedf("unexpected replica type %s", c.Prev.ReplicaType.ReplicaType))
			}
			chgs = append(chgs, chg)
		}
	}
	if newLeaseholderIndex >= 0 {
		// Move the new leaseholder to index 0.
		chgs[0], chgs[newLeaseholderIndex] = chgs[newLeaseholderIndex], chgs[0]
	}
	return chgs
}

// LeaseTransferTarget returns the store ID of the store that is the target of
// the lease transfer. It panics if the range change is not a transfer lease
// operation.
func (rc *ExternalRangeChange) LeaseTransferTarget() roachpb.StoreID {
	if !rc.IsPureTransferLease() {
		panic("pendingRangeChange is not a lease transfer")
	}
	for _, c := range rc.Changes {
		if !c.Prev.IsLeaseholder && c.Next.IsLeaseholder {
			return c.Target.StoreID
		}
	}
	panic("unreachable")
}

// LeaseTransferFrom returns the store ID of the store that is the source of
// the lease transfer. It panics if the range change is not a transfer lease.
func (rc *ExternalRangeChange) LeaseTransferFrom() roachpb.StoreID {
	if !rc.IsPureTransferLease() {
		panic("pendingRangeChange is not a lease transfer")
	}
	for _, c := range rc.Changes {
		if c.Prev.IsLeaseholder && !c.Next.IsLeaseholder {
			return c.Target.StoreID
		}
	}
	panic("unreachable")
}
