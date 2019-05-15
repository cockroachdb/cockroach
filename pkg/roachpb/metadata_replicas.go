// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package roachpb

// ReplicaDescriptors is the set of nodes/stores on which replicas of a range
// are stored.
type ReplicaDescriptors struct {
	wrapped []ReplicaDescriptor
}

// MakeReplicaDescriptors creates a ReplicaDescriptors wrapper from a raw slice
// of individual descriptors.
func MakeReplicaDescriptors(replicas []ReplicaDescriptor) ReplicaDescriptors {
	return ReplicaDescriptors{wrapped: replicas}
}

// Wrapped returns every replica in the set. It is a placeholder for code that
// used to work on a slice of replicas until learner replicas are added. At that
// point, all uses of Wrapped will be migrated to All/Voters/Learners.
//
// WIP should I name this TODO to match the context one?
func (d ReplicaDescriptors) Wrapped() []ReplicaDescriptor {
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
	d.wrapped = append(d.wrapped)
}

// QuorumSize returns the number of voter replicas required for quorum in a raft
// group consisting of this set of replicas.
func (d ReplicaDescriptors) QuorumSize() int {
	return (len(d.wrapped) / 2) + 1
}
