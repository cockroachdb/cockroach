// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
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
			{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: roachpb.VOTER_FULL},
			{NodeID: 100, StoreID: 110, ReplicaID: 120, Type: roachpb.VOTER_FULL},
			{NodeID: 1000, StoreID: 1100, ReplicaID: 1200, Type: roachpb.VOTER_FULL},
			{NodeID: 2000, StoreID: 2100, ReplicaID: 2200, Type: roachpb.NON_VOTER},
		}))

	oneVoterAndThreeNonVoters := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{
			{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: roachpb.VOTER_FULL},
			{NodeID: 100, StoreID: 110, ReplicaID: 120, Type: roachpb.NON_VOTER},
			{NodeID: 1000, StoreID: 1100, ReplicaID: 1200, Type: roachpb.NON_VOTER},
			{NodeID: 2000, StoreID: 2100, ReplicaID: 2200, Type: roachpb.NON_VOTER},
		}))

	{
		ctr, down, under, over, _, decom := calcRangeCounter(1100, threeVotersAndSingleNonVoter, leaseStatus, livenesspb.NodeVitalityMap{
			1000: livenesspb.FakeNodeVitality(true), // by NodeID
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		require.True(t, ctr)
		require.True(t, down)
		require.True(t, under)
		require.False(t, over)
		require.False(t, decom)
	}

	{
		ctr, down, under, over, _, decom := calcRangeCounter(1000, threeVotersAndSingleNonVoter, leaseStatus, livenesspb.NodeVitalityMap{
			1000: livenesspb.FakeNodeVitality(false),
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		// Does not confuse a non-live entry for a live one. In other words,
		// does not think that the liveness map has only entries for live nodes.
		require.False(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
		require.False(t, decom)
	}

	{
		ctr, down, under, over, _, decom := calcRangeCounter(11, threeVotersAndSingleNonVoter, leaseStatus, livenesspb.NodeVitalityMap{
			10:   livenesspb.FakeNodeVitality(true),
			100:  livenesspb.FakeNodeVitality(true),
			1000: livenesspb.FakeNodeVitality(true),
			2000: livenesspb.FakeNodeVitality(true),
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		require.True(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
		require.False(t, decom)
	}

	{
		// Single non-voter dead
		ctr, down, under, over, _, decom := calcRangeCounter(11, oneVoterAndThreeNonVoters, leaseStatus, livenesspb.NodeVitalityMap{
			10:   livenesspb.FakeNodeVitality(true),
			100:  livenesspb.FakeNodeVitality(true),
			1000: livenesspb.FakeNodeVitality(false),
			2000: livenesspb.FakeNodeVitality(true),
		}, 1 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		require.True(t, ctr)
		require.False(t, down)
		require.True(t, under)
		require.False(t, over)
		require.False(t, decom)
	}

	{
		// All non-voters are dead, but range is not unavailable
		ctr, down, under, over, _, decom := calcRangeCounter(11, oneVoterAndThreeNonVoters, leaseStatus, livenesspb.NodeVitalityMap{
			10:   livenesspb.FakeNodeVitality(true),
			100:  livenesspb.FakeNodeVitality(false),
			1000: livenesspb.FakeNodeVitality(false),
			2000: livenesspb.FakeNodeVitality(false),
		}, 1 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		require.True(t, ctr)
		require.False(t, down)
		require.True(t, under)
		require.False(t, over)
		require.False(t, decom)
	}

	{
		// More non-voters than needed
		ctr, down, under, over, _, decom := calcRangeCounter(11, oneVoterAndThreeNonVoters, leaseStatus, livenesspb.NodeVitalityMap{
			10:   livenesspb.FakeNodeVitality(true),
			100:  livenesspb.FakeNodeVitality(true),
			1000: livenesspb.FakeNodeVitality(true),
			2000: livenesspb.FakeNodeVitality(true),
		}, 1 /* numVoters */, 3 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		require.True(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.True(t, over)
		require.False(t, decom)
	}

	{
		// Range larger than the threshold.
		ctr, _, _, _, large, _ := calcRangeCounter(1100, threeVotersAndSingleNonVoter, leaseStatus, livenesspb.NodeVitalityMap{
			1000: livenesspb.FakeNodeVitality(true), // by NodeID
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 1000 /* rangeTooLargeThreshold */, 2000 /* rangeSize */)

		require.True(t, ctr)
		require.True(t, large)
	}

	{
		ctr, _, _, _, large, _ := calcRangeCounter(1000, threeVotersAndSingleNonVoter, leaseStatus, livenesspb.NodeVitalityMap{
			1000: livenesspb.FakeNodeVitality(false),
		}, 3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 1000 /* rangeTooLargeThreshold */, 2000 /* rangeSize */)
		require.False(t, ctr)
		// Only the node responsible for the range can report if the range is too
		// large.
		require.False(t, large)
	}

	{
		// Decommissioning node.
		vitality := livenesspb.TestCreateNodeVitality(10, 100, 1000, 2000)
		vitality.Decommissioning(100, true /* alive */)
		ctr, down, under, over, _, decom := calcRangeCounter(11, threeVotersAndSingleNonVoter, leaseStatus, vitality.ScanNodeVitalityFromCache(),
			3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)

		require.True(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
		require.True(t, decom)
	}
}

func TestCalcRangeCounterLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rangeDesc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 10, ReplicaID: 100, Type: roachpb.VOTER_FULL},
			{NodeID: 2, StoreID: 20, ReplicaID: 200, Type: roachpb.NON_VOTER},
			{NodeID: 3, StoreID: 30, ReplicaID: 300, Type: roachpb.VOTER_FULL},
			{NodeID: 4, StoreID: 40, ReplicaID: 400, Type: roachpb.VOTER_FULL},
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
			livenessMap := livenesspb.NodeVitalityMap{}
			for _, nodeID := range tc.liveNodes {
				livenessMap[nodeID] = livenesspb.FakeNodeVitality(true)
			}
			ctr, _, _, _, _, _ := calcRangeCounter(tc.storeID, rangeDesc, tc.leaseStatus, livenessMap,
				3 /* numVoters */, 4 /* numReplicas */, 4 /* clusterNodes */, 0 /* rangeTooLargeThreshold */, 0 /* rangeSize */)
			require.Equal(t, tc.expectCounter, ctr)
		})
	}
}
