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

// TODO(aayush): This test has gotten unwieldy. It needs to be table-driven.
func TestValidateReplicationChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	learnerType := roachpb.LEARNER
	desc := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 3, StoreID: 3},
			{NodeID: 4, StoreID: 4, Type: &learnerType},
		},
	}

	// Test Case 1: Add a new replica to another node.
	err := validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
	})
	require.NoError(t, err)

	// Test Case 2: Remove a replica from an existing node.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
	})
	require.NoError(t, err)

	// Test Case 3: Remove a replica from wrong node.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
	})
	require.Regexp(t, "removing n2,s2 which is not in", err)

	// Test Case 4: Remove a replica from wrong store.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
	})
	require.Regexp(t, "removing n1,s2 which is not in", err)

	// Test Case 5: Re-balance a replica within a store.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
	})
	require.NoError(t, err)

	// Test Case 6: Re-balance a replica within a store, but attempt remove from
	// the wrong one.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
	})
	require.Regexp(t, "Expected replica to be removed from", err)

	// Test Case 7: Add replica to same node and store.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
	})
	require.Regexp(t, "unable to add replica n1,s1 which is already present", err)

	// Test Case 8: Add replica to same node and different store.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
	})
	require.Regexp(t, "unable to add replica 2", err)

	// Test Case 9: Try to rebalance a replica on the same node, but also add an extra.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
	})
	require.Regexp(t, "can only add-remove a replica within a node", err)

	// Test Case 10: Try to add twice to the same node.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 4}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 5}},
	})
	require.Regexp(t, "refer to n4 twice for change ADD_VOTER", err)

	// Test Case 11: Try to remove twice to the same node.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
	})
	require.Regexp(t, "refer to n1 twice for change REMOVE_VOTER", err)

	// Test Case 12: Try to add where there is already a learner.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 5}},
	})
	require.Error(t, err)

	// Test Case 13: Add/Remove multiple replicas.
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 5, StoreID: 5}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 6, StoreID: 6}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
	})
	require.NoError(t, err)

	// Test Case 14: We are rebalancing within a node and do a remove.
	descRebalancing := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 1, StoreID: 3, Type: &learnerType},
		},
	}
	err = validateReplicationChanges(descRebalancing, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
	})
	require.NoError(t, err)

	// Test Case 15: same as 14 but remove the second node
	err = validateReplicationChanges(descRebalancing, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 3}},
	})
	require.NoError(t, err)

	// Test Case 16: Do an add while rebalancing within a node
	err = validateReplicationChanges(descRebalancing, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
	})
	require.NoError(t, err)

	// Test Case 17: Remove/Add within a node is not allowed, since we expect Add/Remove
	err = validateReplicationChanges(desc, roachpb.ReplicationChanges{
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
	})
	require.Regexp(t, "can only add-remove a replica within a node, but got ", err)

	// Test Case 18: We are rebalancing within a node and have only one replica
	descSingle := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
		},
	}
	err = validateReplicationChanges(descSingle, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 2}},
	})
	require.NoError(t, err)

	// Test Case 18: Adding a non-voter where we already have a voting replica
	// without a simultaneous removal of that voter.
	err = validateReplicationChanges(descSingle, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
	})
	require.Regexp(t, "unable to add replica .* which is already present as a voter", err)

	// Test Case 19: Demoting a voter to a non-voter.
	err = validateReplicationChanges(descSingle, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
	})
	require.NoError(t, err)

	descWithOneVoterAndNonVoter := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2, Type: roachpb.ReplicaTypeNonVoter()},
		},
	}

	// Test Case 20: Adding a voter on a store where we already have a non-voter.
	err = validateReplicationChanges(descWithOneVoterAndNonVoter, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
	})
	require.Regexp(t, "unable to add replica .* which is already present as a non-voter", err)

	// Test Case 21: Promoting a non-voter to a voter.
	err = validateReplicationChanges(descWithOneVoterAndNonVoter, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
		{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
	})
	require.NoError(t, err)

	// Test Case 22: Swapping a voter with a non-voter.
	err = validateReplicationChanges(descWithOneVoterAndNonVoter, roachpb.ReplicationChanges{
		{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
		{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
		{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
	})
	require.NoError(t, err)
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
		return roachpb.ReplicationTarget{NodeID: roachpb.NodeID(t), StoreID: roachpb.StoreID(t)}
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
