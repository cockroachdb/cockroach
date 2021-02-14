// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Regression test for #38308. Summary: a non-nullable field was added to
// RangeDescriptor which broke splits, merges, and replica changes if the
// cluster had been upgraded from a previous version of cockroach.
func TestRangeDescriptorUpdateProtoChangedAcrossVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Control our own split destiny.
	args := base.TestServerArgs{Knobs: base.TestingKnobs{Store: &StoreTestingKnobs{
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	}}}
	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	bKey := roachpb.Key("b")
	if err := kvDB.AdminSplit(ctx, bKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// protoVarintField returns an encoded proto field of type varint with the
	// given id.
	protoVarintField := func(fieldID int) []byte {
		var scratch [binary.MaxVarintLen64]byte
		const typ = 0 // varint type field
		tag := uint64(fieldID<<3) | typ
		tagLen := binary.PutUvarint(scratch[:], tag)
		// A proto message is a series of <tag><data> where <tag> is a varint
		// including the field id and the data type and <data> depends on the type.
		buf := append([]byte(nil), scratch[:tagLen]...)
		// The test doesn't care what we use for the field data, so use the tag
		// since the data is a varint and it's already an encoded varint.
		buf = append(buf, scratch[:tagLen]...)
		return buf
	}

	// Update the serialized RangeDescriptor proto for the b to max range to have
	// an unknown proto field. Previously, this would break splits, merges,
	// replica changes. The real regression was a missing field, but an extra
	// unknown field tests the same thing.
	{
		bDescKey := keys.RangeDescriptorKey(roachpb.RKey(bKey))
		bDescKV, err := kvDB.Get(ctx, bDescKey)
		require.NoError(t, err)
		require.NotNil(t, bDescKV.Value, `could not find "b" descriptor`)

		// Update the serialized proto with a new field we don't know about. The
		// proto encoding is just a series of these, so we can do this simply by
		// appending it.
		newBDescBytes, err := bDescKV.Value.GetBytes()
		require.NoError(t, err)
		newBDescBytes = append(newBDescBytes, protoVarintField(9999)...)

		newBDescValue := roachpb.MakeValueFromBytes(newBDescBytes)
		require.NoError(t, kvDB.Put(ctx, bDescKey, &newBDescValue))
	}

	// Verify that splits still work. We could also do a similar thing to test
	// merges and replica changes, but they all go through updateRangeDescriptor
	// so it's unnecessary.
	cKey := roachpb.Key("c")
	if err := kvDB.AdminSplit(ctx, cKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
}

func TestValidateReplicationChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	learnerType := roachpb.LEARNER
	twoVotersAndALearner := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 3, StoreID: 3},
			{NodeID: 4, StoreID: 4, Type: &learnerType},
		},
	}
	twoReplicasOnOneNode := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 1, StoreID: 3, Type: &learnerType},
		},
	}
	oneVoterAndOneNonVoter := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2, Type: roachpb.ReplicaTypeNonVoter()},
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
		changes       roachpb.ReplicationChanges
		shouldFail    bool
		expErrorRegex string
	}

	tests := []testCase{
		{
			name:      "add a new voter to another node",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
		},
		{
			name:      "remove a voter from an existing node",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "remove a voter from the wrong node",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist.*n2,s2",
		},
		{
			name:      "remove a voter from the wrong store",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist.*n1,s2",
		},
		{
			name:      "rebalance within a node",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "rebalance within a node but attempt to remove from the wrong one",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 5}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist.*n1,s2",
		},
		{
			name:      "re-add an existing voter",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add a voter to a store that already has a VOTER_FULL",
		},
		{
			name:      "add voter to a node that already has one",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "node 1 already has a replica; only valid actions are a removal or a rebalance",
		},
		{
			name:      "add non-voter to a store that already has one",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add a non-voter to a store that already has a NON_VOTER",
		},
		{
			name:      "add non-voter to a node that already has one",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "node 2 already has a replica; only valid actions are a removal or a rebalance",
		},
		{
			name:      "add non-voter to a store that already has a voter",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add.* to a store.* that already has a replica",
		},
		{
			name:      "try to rebalance within a node, but also add an extra",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
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
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 5, StoreID: 6}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 5, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "unexpected set of changes.* for node 5, which has no existing replicas",
		},
		{
			name:      "try to remove twice from the same store, while the range only has 1 replica",
			rangeDesc: oneReplica,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "only permissible order of operations within a store is add-remove",
		},
		{
			name:      "try to remove twice from the same node",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail: true, expErrorRegex: "trying to remove a replica that doesn't exist",
		},
		{
			name:      "try to add on a node that already has a learner",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 5}},
			},
			shouldFail:    true,
			expErrorRegex: "only valid actions are a removal or a rebalance",
		},
		{
			name:      "add/remove multiple replicas",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
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
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
			},
		},
		{
			name:      "remove a voter from a node that has two replicas",
			rangeDesc: twoReplicasOnOneNode,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "remove a replica with the wrong type from a node that has two replicas",
			rangeDesc: twoReplicasOnOneNode,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "type of replica being removed.*does not match expectation",
		},
		{
			name:      "add to a different node while one node is in the midst of a lateral rebalance",
			rangeDesc: twoReplicasOnOneNode,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 4}},
			},
		},
		{
			name:      "add-remove to a node that is in the middle of a lateral rebalance",
			rangeDesc: twoReplicasOnOneNode,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 5}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
			},
			shouldFail:    true,
			expErrorRegex: "expected exactly one of them to be removed",
		},
		{
			name:      "remove two replicas from a node that is in the middle of a lateral rebalance",
			rangeDesc: twoReplicasOnOneNode,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
			},
			shouldFail:    true,
			expErrorRegex: "expected exactly one of them to be removed",
		},
		{
			name:      "remove then add within a node",
			rangeDesc: twoVotersAndALearner,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
			shouldFail:    true,
			expErrorRegex: "only valid actions are.*add.remove",
		},
		{
			name:      "add to a node when we only have one replica",
			rangeDesc: oneReplica,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
			},
		},
		{
			name: "adding a non-voter where we already have a voting replica, without an accompanying" +
				" removal of that voter",
			rangeDesc: oneReplica,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to add.*NON_VOTER.*to a store.* that already has a replica",
		},
		{
			name:      "voter demotion",
			rangeDesc: oneReplica,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
		},
		{
			name:      "non-voter promotion",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
		},
		{
			name:      "swapping voter with non-voter",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
			},
		},
		{
			name:      "trying to promote a non-voter that doesnt exist",
			rangeDesc: oneVoterAndOneNonVoter,
			changes: roachpb.ReplicationChanges{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
			},
			shouldFail:    true,
			expErrorRegex: "trying to remove a replica that doesn't exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateReplicationChanges(test.rangeDesc, test.changes)
			if test.shouldFail {
				require.Regexp(t, test.expErrorRegex, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSynthesizeTargetsByChangeType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		name                                      string
		changes                                   []roachpb.ReplicationChange
		expPromotions, expDemotions               []int32
		expVoterAdditions, expVoterRemovals       []int32
		expNonVoterAdditions, expNonVoterRemovals []int32
	}

	mkTarget := func(t int32) roachpb.ReplicationTarget {
		return roachpb.ReplicationTarget{
			NodeID: roachpb.NodeID(t), StoreID: roachpb.StoreID(t),
		}
	}

	mkTargetList := func(targets []int32) []roachpb.ReplicationTarget {
		if len(targets) == 0 {
			return nil
		}
		res := make([]roachpb.ReplicationTarget, len(targets))
		for i, t := range targets {
			res[i] = mkTarget(t)
		}
		return res
	}

	tests := []testCase{
		{
			name: "simple voter addition",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
			},
			expVoterAdditions: []int32{2},
		},
		{
			name: "simple voter removal",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(2)},
			},
			expVoterRemovals: []int32{2},
		},
		{
			name: "simple non-voter addition",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(2)},
			},
			expNonVoterAdditions: []int32{2},
		},
		{
			name: "simple non-voter removal",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
			},
			expNonVoterRemovals: []int32{2},
		},
		{
			name: "promote non_voter to voter",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
			},
			expPromotions: []int32{2},
		},
		{
			name: "demote voter to non_voter",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
			},
			expDemotions: []int32{1},
		},
		{
			name: "swap voter with non_voter",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
			},
			expPromotions: []int32{2},
			expDemotions:  []int32{1},
		},
		{
			name: "swap with simple addition",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(4)},
			},
			expPromotions:     []int32{2},
			expDemotions:      []int32{1},
			expVoterAdditions: []int32{4},
		},
		{
			name: "swap with simple removal",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(4)},
			},
			expPromotions:    []int32{2},
			expDemotions:     []int32{1},
			expVoterRemovals: []int32{4},
		},
		{
			name: "swap with addition promotion",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(3)},
			},
			expPromotions: []int32{2, 3},
			expDemotions:  []int32{1},
		},
		{
			name: "swap with additional demotion",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(4)},
			},
			expPromotions: []int32{2},
			expDemotions:  []int32{1, 4},
		},
		{
			name: "two swaps",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(3)},
			},
			expPromotions: []int32{2, 3},
			expDemotions:  []int32{1, 4},
		},
		{
			name: "all at once",
			changes: []roachpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(5)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(6)},
			},
			expPromotions:        []int32{2, 3},
			expDemotions:         []int32{1},
			expVoterAdditions:    []int32{4},
			expNonVoterAdditions: []int32{5},
			expNonVoterRemovals:  []int32{6},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := synthesizeTargetsByChangeType(test.changes)
			require.Equal(t, result.nonVoterPromotions, mkTargetList(test.expPromotions))
			require.Equal(t, result.voterDemotions, mkTargetList(test.expDemotions))
			require.Equal(t, result.voterAdditions, mkTargetList(test.expVoterAdditions))
			require.Equal(t, result.voterRemovals, mkTargetList(test.expVoterRemovals))
			require.Equal(t, result.nonVoterAdditions, mkTargetList(test.expNonVoterAdditions))
			require.Equal(t, result.nonVoterRemovals, mkTargetList(test.expNonVoterRemovals))
		})
	}
}
