// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plan

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestReplicationChangesForRebalance asserts that the replication changes for
// rebalancing are correct, given a a range descriptor and rebalance target.
func TestReplicationChangesForRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		name                    string
		desc                    *roachpb.RangeDescriptor
		addTarget, removeTarget roachpb.ReplicationTarget
		rebalanceTargetType     allocatorimpl.TargetReplicaType
		expectedChanges         []kvpb.ReplicationChange
		expectedPerformingSwap  bool
		expectedErrStr          string
	}{
		{
			name: "rf=1 rebalance voter 1->2",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.VOTER_FULL},
				},
			},
			addTarget:           roachpb.ReplicationTarget{NodeID: 2, StoreID: 2},
			removeTarget:        roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType: allocatorimpl.VoterTarget,
			expectedChanges: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 2, StoreID: 2}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			expectedPerformingSwap: false,
			expectedErrStr:         "",
		},
		{
			name: "rf=3 rebalance voter 1->4",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.VOTER_FULL},
					{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
					{NodeID: 3, StoreID: 3, Type: roachpb.VOTER_FULL},
				},
			},
			addTarget:           roachpb.ReplicationTarget{NodeID: 4, StoreID: 4},
			removeTarget:        roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType: allocatorimpl.VoterTarget,
			expectedChanges: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 4}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			expectedPerformingSwap: false,
			expectedErrStr:         "",
		},
		{
			name: "rf=3 rebalance voter 1->3 error: already has a voter",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.VOTER_FULL},
					{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
					{NodeID: 3, StoreID: 3, Type: roachpb.VOTER_FULL},
				},
			},
			addTarget:              roachpb.ReplicationTarget{NodeID: 3, StoreID: 3},
			removeTarget:           roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType:    allocatorimpl.VoterTarget,
			expectedChanges:        nil,
			expectedPerformingSwap: false,
			expectedErrStr:         "programming error: store being rebalanced to(3) already has a voting replica",
		},
		{
			name: "rf=3 rebalance non-voter: 1->4",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.NON_VOTER},
					{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
					{NodeID: 3, StoreID: 3, Type: roachpb.VOTER_FULL},
				},
			},
			addTarget:           roachpb.ReplicationTarget{NodeID: 4, StoreID: 4},
			removeTarget:        roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType: allocatorimpl.NonVoterTarget,
			expectedChanges: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 4, StoreID: 4}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			expectedPerformingSwap: false,
			expectedErrStr:         "",
		},
		{
			name: "rf=3 rebalance non-voter 1->3 error: already has a voter",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.NON_VOTER},
					{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
					{NodeID: 3, StoreID: 3, Type: roachpb.VOTER_FULL},
				},
			},
			addTarget:              roachpb.ReplicationTarget{NodeID: 3, StoreID: 3},
			removeTarget:           roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType:    allocatorimpl.NonVoterTarget,
			expectedChanges:        nil,
			expectedPerformingSwap: false,
			expectedErrStr:         "invalid rebalancing decision: trying to move non-voter to a store that already has a replica",
		},
		{
			name: "rf=3 rebalance non-voter 1->3 error: already has a non-voter",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.NON_VOTER},
					{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
					{NodeID: 3, StoreID: 3, Type: roachpb.NON_VOTER},
				},
			},
			addTarget:              roachpb.ReplicationTarget{NodeID: 3, StoreID: 3},
			removeTarget:           roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType:    allocatorimpl.NonVoterTarget,
			expectedChanges:        nil,
			expectedPerformingSwap: false,
			expectedErrStr:         "invalid rebalancing decision: trying to move non-voter to a store that already has a replica",
		},
		{
			name: "rf=3 rebalance voter 1->3 swap",
			desc: &roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{NodeID: 1, StoreID: 1, Type: roachpb.VOTER_FULL},
					{NodeID: 2, StoreID: 2, Type: roachpb.VOTER_FULL},
					{NodeID: 3, StoreID: 3, Type: roachpb.NON_VOTER},
				},
			},
			addTarget:           roachpb.ReplicationTarget{NodeID: 3, StoreID: 3},
			removeTarget:        roachpb.ReplicationTarget{NodeID: 1, StoreID: 1},
			rebalanceTargetType: allocatorimpl.VoterTarget,
			expectedChanges: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 3, StoreID: 3}},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
				{ChangeType: roachpb.REMOVE_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}},
			},
			expectedPerformingSwap: true,
			expectedErrStr:         "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chgs, performingSwap, err := ReplicationChangesForRebalance(
				ctx,
				tc.desc,
				len(tc.desc.Replicas().VoterDescriptors()),
				tc.addTarget,
				tc.removeTarget,
				tc.rebalanceTargetType,
			)
			require.Equal(t, tc.expectedChanges, chgs)
			require.Equal(t, tc.expectedPerformingSwap, performingSwap)
			if tc.expectedErrStr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrStr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
