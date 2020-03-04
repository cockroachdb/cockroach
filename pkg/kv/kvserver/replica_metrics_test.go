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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCalcRangeCounterIsLiveMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Regression test for a bug, see:
	// https://github.com/cockroachdb/cockroach/pull/39936#pullrequestreview-359059629

	desc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaDescriptors([]roachpb.ReplicaDescriptor{
			{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 100, StoreID: 110, ReplicaID: 120, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 1000, StoreID: 1100, ReplicaID: 1200, Type: roachpb.ReplicaTypeVoterFull()},
		}))

	{
		ctr, down, under, over := calcRangeCounter(1100 /* storeID */, desc, IsLiveMap{
			1000: IsLiveMapEntry{IsLive: true}, // by NodeID
		}, 3, 3)

		require.True(t, ctr)
		require.True(t, down)
		require.True(t, under)
		require.False(t, over)
	}

	{
		ctr, down, under, over := calcRangeCounter(1000, desc, IsLiveMap{
			1000: IsLiveMapEntry{IsLive: false},
		}, 3, 3)

		// Does not confuse a non-live entry for a live one. In other words,
		// does not think that the liveness map has only entries for live nodes.
		require.False(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
	}
}
