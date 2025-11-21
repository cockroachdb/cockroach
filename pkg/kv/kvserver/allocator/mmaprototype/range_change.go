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
		w.Printf("%v", c.changeID)
	}
	w.Print("]")
}

// TODO(sumeer): A single PendingRangeChange can model a bunch of replica
// changes and a lease transfer. Classifying the change as either
// IsChangeReplicas or IsTransferLease is unnecessarily limiting. The only
// code that really relies on this is integration code:
// mma_store_rebalancer.go, allocator_sync.go, asim. We should fix those and
// consider removing these methods.

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
func (prc PendingRangeChange) ReplicationChanges() kvpb.ReplicationChanges {
	if !prc.IsChangeReplicas() {
		panic("RangeChange is not a change replicas")
	}
	chgs := make([]kvpb.ReplicationChange, 0, len(prc.pendingReplicaChanges))
	newLeaseholderIndex := -1
	for _, c := range prc.pendingReplicaChanges {
		switch c.replicaChangeType {
		case ChangeReplica, AddReplica, RemoveReplica:
			// These are the only permitted cases.
		default:
			panic(errors.AssertionFailedf("change type %v is not a change replicas", c.replicaChangeType))
		}
		// The kvserver code represents a change in replica type as an
		// addition and a removal of the same replica. For example, if a
		// replica changes from VOTER_FULL to NON_VOTER, we will emit a pair
		// of {ADD_NON_VOTER, REMOVE_VOTER} for the replica. The ordering of
		// this pair does not matter.
		//
		// TODO(tbg): confirm that the ordering does not matter.
		if c.replicaChangeType == ChangeReplica || c.replicaChangeType == AddReplica {
			chg := kvpb.ReplicationChange{Target: c.target}
			isNewLeaseholder := false
			switch c.next.ReplicaType.ReplicaType {
			case roachpb.VOTER_FULL:
				chg.ChangeType = roachpb.ADD_VOTER
				if c.next.IsLeaseholder {
					isNewLeaseholder = true
				}
			case roachpb.NON_VOTER:
				chg.ChangeType = roachpb.ADD_NON_VOTER
			default:
				panic(errors.AssertionFailedf("unexpected replica type %s", c.next.ReplicaType.ReplicaType))
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
		if c.replicaChangeType == ChangeReplica || c.replicaChangeType == RemoveReplica {
			chg := kvpb.ReplicationChange{Target: c.target}
			prevType := mapReplicaTypeToVoterOrNonVoter(c.prev.ReplicaType.ReplicaType)
			switch prevType {
			case roachpb.VOTER_FULL:
				chg.ChangeType = roachpb.REMOVE_VOTER
			case roachpb.NON_VOTER:
				chg.ChangeType = roachpb.REMOVE_NON_VOTER
			default:
				panic(errors.AssertionFailedf("unexpected replica type %s", c.prev.ReplicaType.ReplicaType))
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
