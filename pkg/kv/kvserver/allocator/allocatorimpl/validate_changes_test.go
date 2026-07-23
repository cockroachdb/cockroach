// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allocatorimpl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestValidateReplicationChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	twoVotersAndALearner := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 3, StoreID: 3},
			{NodeID: 4, StoreID: 4, Type: roachpb.LEARNER},
		},
	}
	twoReplicasOnOneNode := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 1, StoreID: 3, Type: roachpb.LEARNER},
		},
	}
	oneVoterAndOneNonVoter := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2, Type: roachpb.NON_VOTER},
		},
	}
	oneReplica := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
		},
	}

	type testCase struct {
		name          string
		rangeDesc     *roachpb.RangeDescriptor
		changes       kvpb.ReplicationChanges
		shouldFail    bool
		expErrorRegex string
	}

	tests := []testCase{
		{
			name:      "add a new voter to another node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
		},
		{
			name:      "remove a voter from an existing node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "remove a voter from the wrong node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist.*n2,s2",
		},
		{
			name:      "remove a voter from the wrong store",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist.*n1,s2",
		},
		{
			name:      "rebalance within a node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "rebalance within a node but attempt to remove from the wrong one",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 5}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist.*n1,s2",
		},
		{
			name:      "re-add an existing voter",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add a voter to a store that already has a VOTER_FULL",
		},
		{
			name:      "add voter to a node that already has one",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "node 1 already has a replica; only valid actions are a removal or a rebalance",
		},
		{
			name:      "add non-voter to a store that already has one",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add a non-voter to a store that already has a NON_VOTER",
		},
		{
			name:      "add non-voter to a node that already has one",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "node 2 already has a replica; only valid actions are a removal or a rebalance",
		},
		{
			name:      "add non-voter to a store that already has a voter",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add.* to a store.* that already has a replica",
		},
		{
			name:      "try to rebalance within a node, but also add an extra",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "more than 2 changes for the same node",
		},
		{
			name:      "try to add twice to the same node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 5, StoreID: 6}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 5, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "unexpected set of changes.* for node 5, which has no existing replicas",
		},
		{
			name:      "try to remove twice from the same store, while the range only has 1 replica",
			rangeDesc: oneReplica,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "only permissible order of operations within a store is add-remove",
		},
		{
			name:      "try to remove twice from the same node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail: true, expErrorRegex: "trying to remove a replica that doesn't exist",
		},
		{
			name:      "try to add on a node that already has a learner",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "only valid actions are a removal or a rebalance",
		},
		{
			name:      "add/remove multiple replicas",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 5, StoreID: 5}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 6, StoreID: 6}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
			},
		},
		{
			// NB: We would expect to be in a situation like this subtest right after
			// relocating a replica within a node.
			//
			// Regression test for #60545.
			name:      "remove a learner from a node that has two replicas",
			rangeDesc: twoReplicasOnOneNode,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
			},
		},
		{
			name:      "remove a voter from a node that has two replicas",
			rangeDesc: twoReplicasOnOneNode,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "remove a replica with the wrong type from a node that has two replicas",
			rangeDesc: twoReplicasOnOneNode,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "type of replica being removed.*does not match expectation",
		},
		{
			name:      "add to a different node while one node is in the midst of a lateral rebalance",
			rangeDesc: twoReplicasOnOneNode,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 4}},
			},
		},
		{
			name:      "add-remove to a node that is in the middle of a lateral rebalance",
			rangeDesc: twoReplicasOnOneNode,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 5}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
			},
			shouldFail:    true,
			expErrorRegex: "expected exactly one of them to be removed",
		},
		{
			name:      "remove two replicas from a node that is in the middle of a lateral rebalance",
			rangeDesc: twoReplicasOnOneNode,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
			},
			shouldFail:    true,
			expErrorRegex: "expected exactly one of them to be removed",
		},
		{
			name:      "remove then add within a node",
			rangeDesc: twoVotersAndALearner,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "only valid actions are.*add.remove",
		},
		{
			name:      "add to a node when we only have one replica",
			rangeDesc: oneReplica,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
		},
		{
			name: "adding a non-voter where we already have a voting replica, without an accompanying" +
				" removal of that voter",
			rangeDesc: oneReplica,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add.*NON_VOTER.*to a store.* that already has a replica",
		},
		{
			name:      "voter demotion",
			rangeDesc: oneReplica,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "non-voter promotion",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
		},
		{
			name:      "swapping voter with non-voter",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
		},
		{
			name:      "trying to promote a non-voter that doesnt exist",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: kvpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateReplicationChanges(test.rangeDesc, test.changes)
			if test.shouldFail {
				require.Regexp(t, test.expErrorRegex, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
