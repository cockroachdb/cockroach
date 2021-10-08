// Copyright 2020 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCalcRangeCounterIsLiveMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	leaseStatus := kvserverpb.LeaseStatus{
		Lease: roachpb.Lease{
			Replica: roachpb.ReplicaDescriptor{
				NodeID:  10,
				StoreID: 11,
			},
		},
		State: kvserverpb.LeaseState_VALID,
	}

	// Regression test for a bug, see:
	// https://github.com/cockroachdb/cockroach/pull/39936#pullrequestreview-359059629

	threeVotersAndSingleNonVoter := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{
			{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 100, StoreID: 110, ReplicaID: 120, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 1000, StoreID: 1100, ReplicaID: 1200, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 2000, StoreID: 2100, ReplicaID: 2200, Type: roachpb.ReplicaTypeNonVoter()},
		}))

	oneVoterAndThreeNonVoters := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{
			{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 100, StoreID: 110, ReplicaID: 120, Type: roachpb.ReplicaTypeNonVoter()},
			{NodeID: 1000, StoreID: 1100, ReplicaID: 1200, Type: roachpb.ReplicaTypeNonVoter()},
			{NodeID: 2000, StoreID: 2100, ReplicaID: 2200, Type: roachpb.ReplicaTypeNonVoter()},
		}))

	{
		ctr, down, under, over := calcRangeCounter(1100, threeVotersAndSingleNonVoter, leaseStatus, liveness.IsLiveMap{
			1000: liveness.IsLiveMapEntry{IsLive: true}, // by NodeID
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */)

		require.True(t, ctr)
		require.True(t, down)
		require.True(t, under)
		require.False(t, over)
	}

	{
		ctr, down, under, over := calcRangeCounter(1000, threeVotersAndSingleNonVoter, leaseStatus, liveness.IsLiveMap{
			1000: liveness.IsLiveMapEntry{IsLive: false},
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */)

		// Does not confuse a non-live entry for a live one. In other words,
		// does not think that the liveness map has only entries for live nodes.
		require.False(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
	}

	{
		ctr, down, under, over := calcRangeCounter(11, threeVotersAndSingleNonVoter, leaseStatus, liveness.IsLiveMap{
			10:   liveness.IsLiveMapEntry{IsLive: true},
			100:  liveness.IsLiveMapEntry{IsLive: true},
			1000: liveness.IsLiveMapEntry{IsLive: true},
			2000: liveness.IsLiveMapEntry{IsLive: true},
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */)

		require.True(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
	}

	{
		// Single non-voter dead
		ctr, down, under, over := calcRangeCounter(11, oneVoterAndThreeNonVoters, leaseStatus, liveness.IsLiveMap{
			10:   liveness.IsLiveMapEntry{IsLive: true},
			100:  liveness.IsLiveMapEntry{IsLive: true},
			1000: liveness.IsLiveMapEntry{IsLive: false},
			2000: liveness.IsLiveMapEntry{IsLive: true},
		}, 1 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */)

		require.True(t, ctr)
		require.False(t, down)
		require.True(t, under)
		require.False(t, over)
	}

	{
		// All non-voters are dead, but range is not unavailable
		ctr, down, under, over := calcRangeCounter(11, oneVoterAndThreeNonVoters, leaseStatus, liveness.IsLiveMap{
			10:   liveness.IsLiveMapEntry{IsLive: true},
			100:  liveness.IsLiveMapEntry{IsLive: false},
			1000: liveness.IsLiveMapEntry{IsLive: false},
			2000: liveness.IsLiveMapEntry{IsLive: false},
		}, 1 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */)

		require.True(t, ctr)
		require.False(t, down)
		require.True(t, under)
		require.False(t, over)
	}

	{
		// More non-voters than needed
		ctr, down, under, over := calcRangeCounter(11, oneVoterAndThreeNonVoters, leaseStatus, liveness.IsLiveMap{
			10:   liveness.IsLiveMapEntry{IsLive: true},
			100:  liveness.IsLiveMapEntry{IsLive: true},
			1000: liveness.IsLiveMapEntry{IsLive: true},
			2000: liveness.IsLiveMapEntry{IsLive: true},
		}, 1 /* numVoters */, 3 /* numReplicas */, 4 /* clusterNodes */)

		require.True(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.True(t, over)
	}
}

func TestCalcRangeCounterLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rangeDesc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 10, ReplicaID: 100, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 2, StoreID: 20, ReplicaID: 200, Type: roachpb.ReplicaTypeNonVoter()},
			{NodeID: 3, StoreID: 30, ReplicaID: 300, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 4, StoreID: 40, ReplicaID: 400, Type: roachpb.ReplicaTypeVoterFull()},
		}))

	leaseStatus := kvserverpb.LeaseStatus{
		Lease: roachpb.Lease{
			Replica: roachpb.ReplicaDescriptor{
				NodeID:    3,
				StoreID:   30,
				ReplicaID: 300,
			},
		},
		State: kvserverpb.LeaseState_VALID,
	}
	leaseStatusInvalid := kvserverpb.LeaseStatus{
		Lease: roachpb.Lease{
			Replica: roachpb.ReplicaDescriptor{
				NodeID:    3,
				StoreID:   30,
				ReplicaID: 300,
			},
		},
		State: kvserverpb.LeaseState_ERROR,
	}

	testcases := []struct {
		desc          string
		storeID       roachpb.StoreID
		leaseStatus   kvserverpb.LeaseStatus
		liveNodes     []roachpb.NodeID
		expectCounter bool
	}{
		{
			desc:          "leaseholder is counter",
			storeID:       30,
			leaseStatus:   leaseStatus,
			liveNodes:     []roachpb.NodeID{1, 2, 3, 4},
			expectCounter: true,
		},
		{
			desc:          "invalid leaseholder is counter",
			storeID:       30,
			leaseStatus:   leaseStatusInvalid,
			liveNodes:     []roachpb.NodeID{1, 2, 3, 4},
			expectCounter: true,
		},
		{
			desc:          "non-leaseholder is not counter",
			storeID:       10,
			leaseStatus:   leaseStatus,
			liveNodes:     []roachpb.NodeID{1, 2, 3, 4},
			expectCounter: false,
		},
		{
			desc:          "non-leaseholder not counter with invalid lease",
			storeID:       10,
			leaseStatus:   leaseStatusInvalid,
			liveNodes:     []roachpb.NodeID{1, 2, 3, 4},
			expectCounter: false,
		},
		{
			desc:          "unavailable leaseholder is not counter",
			storeID:       30,
			leaseStatus:   leaseStatus,
			liveNodes:     []roachpb.NodeID{1, 2, 4},
			expectCounter: false,
		},
		{
			desc:          "first is counter with unavailable leaseholder",
			storeID:       10,
			leaseStatus:   leaseStatus,
			liveNodes:     []roachpb.NodeID{1, 2, 4},
			expectCounter: true,
		},
		{
			desc:          "other is not counter with unavailable leaseholder",
			storeID:       20,
			leaseStatus:   leaseStatus,
			liveNodes:     []roachpb.NodeID{1, 2, 4},
			expectCounter: false,
		},
		{
			desc:          "non-voter can be counter",
			storeID:       20,
			leaseStatus:   leaseStatus,
			liveNodes:     []roachpb.NodeID{2, 4},
			expectCounter: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			livenessMap := liveness.IsLiveMap{}
			for _, nodeID := range tc.liveNodes {
				livenessMap[nodeID] = liveness.IsLiveMapEntry{IsLive: true}
			}
			ctr, _, _, _ := calcRangeCounter(tc.storeID, rangeDesc, tc.leaseStatus, livenessMap,
				3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */)
			require.Equal(t, tc.expectCounter, ctr)
		})
	}
}
