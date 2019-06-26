// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import "sort"

// ReplicaDescriptors is a set of replicas, usually the nodes/stores on which
// replicas of a range are stored.
type ReplicaDescriptors struct {
	wrapped []ReplicaDescriptor
}

// MakeReplicaDescriptors creates a ReplicaDescriptors wrapper from a raw slice
// of individual descriptors.
//
// All construction of ReplicaDescriptors is required to go through this method
// so we can guarantee sortedness, which is used to speed up accessor
// operations.
func MakeReplicaDescriptors(replicas []ReplicaDescriptor) ReplicaDescriptors {
	sort.Sort(byTypeThenReplicaID(replicas))
	return ReplicaDescriptors{wrapped: replicas}
}

// Unwrap returns every replica in the set. It is a placeholder for code that
// used to work on a slice of replicas until learner replicas are added. At that
// point, all uses of Unwrap will be migrated to All/Voters/Learners.
func (d ReplicaDescriptors) Unwrap() []ReplicaDescriptor {
	return d.wrapped
}

// All returns every replica in the set, including both voter replicas and
// learner replicas.
func (d ReplicaDescriptors) All() []ReplicaDescriptor {
	return d.wrapped
}

// Voters returns the voter replicas in the set.
func (d ReplicaDescriptors) Voters() []ReplicaDescriptor {
	// Note that the wrapped replicas are sorted first by type.
	for i := range d.wrapped {
		if d.wrapped[i].GetType() == ReplicaType_LEARNER {
			return d.wrapped[:i]
		}
	}
	return d.wrapped
}

// Learners returns the learner replicas in the set.
func (d ReplicaDescriptors) Learners() []ReplicaDescriptor {
	// Note that the wrapped replicas are sorted first by type.
	for i := range d.wrapped {
		if d.wrapped[i].GetType() == ReplicaType_LEARNER {
			return d.wrapped[i:]
		}
	}
	return nil
}

var _, _ = ReplicaDescriptors.All, ReplicaDescriptors.Learners

// AsProto returns the protobuf representation of these replicas, suitable for
// setting the InternalReplicas field of a RangeDescriptor. When possible the
// SetReplicas method of RangeDescriptor should be used instead, this is only
// here for the convenience of tests.
func (d ReplicaDescriptors) AsProto() []ReplicaDescriptor {
	return d.wrapped
}

// DeepCopy returns a copy of this set of replicas. Modifications to the
// returned set will not affect this one and vice-versa.
func (d ReplicaDescriptors) DeepCopy() ReplicaDescriptors {
	return ReplicaDescriptors{
		wrapped: append([]ReplicaDescriptor(nil), d.wrapped...),
	}
}

// AddReplica adds the given replica to this set.
func (d *ReplicaDescriptors) AddReplica(r ReplicaDescriptor) {
	d.wrapped = append(d.wrapped, r)
}

// RemoveReplica removes the matching replica from this set. If it wasn't found
// to remove, false is returned.
func (d *ReplicaDescriptors) RemoveReplica(
	nodeID NodeID, storeID StoreID,
) (ReplicaDescriptor, bool) {
	idx := -1
	for i := range d.wrapped {
		if d.wrapped[i].NodeID == nodeID && d.wrapped[i].StoreID == storeID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return ReplicaDescriptor{}, false
	}
	// Swap with the last element so we can simply truncate the slice.
	d.wrapped[idx], d.wrapped[len(d.wrapped)-1] = d.wrapped[len(d.wrapped)-1], d.wrapped[idx]
	removed := d.wrapped[len(d.wrapped)-1]
	d.wrapped = d.wrapped[:len(d.wrapped)-1]
	// The swap may have broken our sortedness invariant, so re-sort.
	sort.Sort(byTypeThenReplicaID(d.wrapped))
	return removed, true
}

// QuorumSize returns the number of voter replicas required for quorum in a raft
// group consisting of this set of replicas.
func (d ReplicaDescriptors) QuorumSize() int {
	return (len(d.Voters()) / 2) + 1
}

type byTypeThenReplicaID []ReplicaDescriptor

func (x byTypeThenReplicaID) Len() int      { return len(x) }
func (x byTypeThenReplicaID) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
func (x byTypeThenReplicaID) Less(i, j int) bool {
	if x[i].GetType() == x[j].GetType() {
		return x[i].ReplicaID < x[j].ReplicaID
	}
	return x[i].GetType() < x[j].GetType()
}
