// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package roachpb

// ReplicaDescriptors is a set of replicas, usually the nodes/stores on which
// replicas of a range are stored.
type ReplicaDescriptors struct {
	wrapped []ReplicaDescriptor
}

// MakeReplicaDescriptors creates a ReplicaDescriptors wrapper from a raw slice
// of individual descriptors.
func MakeReplicaDescriptors(replicas []ReplicaDescriptor) ReplicaDescriptors {
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
	return d.wrapped
}

// Learners returns the learner replicas in the set.
func (d ReplicaDescriptors) Learners() []ReplicaDescriptor {
	return d.wrapped
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

// RemoveReplica removes the given replica from this set. If it wasn't found to
// remove, false is returned.
func (d *ReplicaDescriptors) RemoveReplica(r ReplicaDescriptor) bool {
	idx := -1
	for i := range d.wrapped {
		if d.wrapped[i].Equal(r) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false
	}
	// Swap with the last element so we can simply truncate the slice.
	d.wrapped[idx], d.wrapped[len(d.wrapped)-1] = d.wrapped[len(d.wrapped)-1], d.wrapped[idx]
	d.wrapped = d.wrapped[:len(d.wrapped)-1]
	return true
}

// QuorumSize returns the number of voter replicas required for quorum in a raft
// group consisting of this set of replicas.
func (d ReplicaDescriptors) QuorumSize() int {
	return (len(d.Voters()) / 2) + 1
}
