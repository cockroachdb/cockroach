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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newReplicaType(t ReplicaType) *ReplicaType {
	return &t
}

func TestVotersLearnersAll(t *testing.T) {
	voter := newReplicaType(ReplicaType_VOTER)
	learner := newReplicaType(ReplicaType_LEARNER)
	tests := [][]ReplicaDescriptor{
		{},
		{{Type: voter}},
		{{Type: nil}},
		{{Type: learner}},
		{{Type: voter}, {Type: learner}, {Type: voter}},
		{{Type: nil}, {Type: learner}, {Type: nil}},
		{{Type: learner}, {Type: voter}, {Type: learner}},
		{{Type: learner}, {Type: nil}, {Type: learner}},
	}
	for i, test := range tests {
		r := MakeReplicaDescriptors(test)
		for _, voter := range r.Voters() {
			assert.Equal(t, ReplicaType_VOTER, voter.GetType(), "testcase %d", i)
		}
		for _, learner := range r.Learners() {
			assert.Equal(t, ReplicaType_LEARNER, learner.GetType(), "testcase %d", i)
		}
		assert.Equal(t, len(test), len(r.All()), "testcase %d", i)
	}
}

func TestReplicaDescriptorsRemove(t *testing.T) {
	tests := []struct {
		replicas []ReplicaDescriptor
		remove   ReplicationTarget
		expected bool
	}{
		{
			remove:   ReplicationTarget{NodeID: 1, StoreID: 1},
			expected: false,
		},
		{
			replicas: []ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
			remove:   ReplicationTarget{NodeID: 2, StoreID: 2},
			expected: false,
		},
		{
			replicas: []ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
			remove:   ReplicationTarget{NodeID: 1, StoreID: 1},
			expected: true,
		},
		{
			// Make sure we sort after the swap in removal.
			replicas: []ReplicaDescriptor{
				{NodeID: 1, StoreID: 1},
				{NodeID: 2, StoreID: 2},
				{NodeID: 3, StoreID: 3},
				{NodeID: 4, StoreID: 4, Type: newReplicaType(ReplicaType_LEARNER)},
			},
			remove:   ReplicationTarget{NodeID: 2, StoreID: 2},
			expected: true,
		},
	}
	for i, test := range tests {
		r := MakeReplicaDescriptors(test.replicas)
		lenBefore := len(r.All())
		removedDesc, ok := r.RemoveReplica(test.remove.NodeID, test.remove.StoreID)
		assert.Equal(t, test.expected, ok, "testcase %d", i)
		if ok {
			assert.Equal(t, test.remove.NodeID, removedDesc.NodeID, "testcase %d", i)
			assert.Equal(t, test.remove.StoreID, removedDesc.StoreID, "testcase %d", i)
			assert.Equal(t, lenBefore-1, len(r.All()), "testcase %d", i)
		} else {
			assert.Equal(t, lenBefore, len(r.All()), "testcase %d", i)
		}
		for _, voter := range r.Voters() {
			assert.Equal(t, ReplicaType_VOTER, voter.GetType(), "testcase %d", i)
		}
		for _, learner := range r.Learners() {
			assert.Equal(t, ReplicaType_LEARNER, learner.GetType(), "testcase %d", i)
		}
	}
}
