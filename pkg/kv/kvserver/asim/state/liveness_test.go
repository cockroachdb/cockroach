// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestStatusTrackerConversion verifies that StatusTracker correctly converts
// asim's internal status representation (store-level liveness + node-level
// membership/draining) into NodeVitality objects used by the single-metric
// allocator. It checks that the resulting LivenessStatus and IsLive values
// match expectations for various combinations of liveness, membership, and
// draining states.
func TestStatusTrackerConversion(t *testing.T) {
	clock := &ManualSimClock{nanos: time.Date(2022, 03, 21, 11, 0, 0, 0, time.UTC).UnixNano()}
	hlcClock := hlc.NewClockForTesting(clock)

	testCases := []struct {
		desc           string
		storeLiveness  LivenessState
		membership     livenesspb.MembershipStatus
		draining       bool
		expectIsAlive  bool
		expectedStatus livenesspb.NodeLivenessStatus
	}{
		{
			desc:           "live active",
			storeLiveness:  LivenessLive,
			membership:     livenesspb.MembershipStatus_ACTIVE,
			expectIsAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_LIVE,
		},
		{
			desc:           "live draining",
			storeLiveness:  LivenessLive,
			membership:     livenesspb.MembershipStatus_ACTIVE,
			draining:       true,
			expectIsAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_DRAINING,
		},
		{
			desc:           "live decommissioning",
			storeLiveness:  LivenessLive,
			membership:     livenesspb.MembershipStatus_DECOMMISSIONING,
			expectIsAlive:  true,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONING,
		},
		{
			desc:           "unavailable active",
			storeLiveness:  LivenessUnavailable,
			membership:     livenesspb.MembershipStatus_ACTIVE,
			expectIsAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_UNAVAILABLE,
		},
		{
			desc:           "dead active",
			storeLiveness:  LivenessDead,
			membership:     livenesspb.MembershipStatus_ACTIVE,
			expectIsAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DEAD,
		},
		{
			desc:           "dead decommissioned",
			storeLiveness:  LivenessDead,
			membership:     livenesspb.MembershipStatus_DECOMMISSIONED,
			expectIsAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
		{
			desc:           "dead decommissioning (returns DECOMMISSIONED)",
			storeLiveness:  LivenessDead,
			membership:     livenesspb.MembershipStatus_DECOMMISSIONING,
			expectIsAlive:  false,
			expectedStatus: livenesspb.NodeLivenessStatus_DECOMMISSIONED,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			// Create a StatusTracker with one node and one store.
			m := StatusTracker{
				clock:          hlcClock,
				storeStatusMap: map[StoreID]StoreStatus{1: {Liveness: tc.storeLiveness}},
				nodeStatusMap:  map[NodeID]NodeStatus{1: {Membership: tc.membership, Draining: tc.draining}},
				storeToNode:    map[StoreID]NodeID{1: 1},
			}
			nv := m.convertToNodeVitality(1, hlcClock.Now())
			require.Equal(t, tc.expectIsAlive, nv.IsLive(livenesspb.Rebalance))
			require.Equal(t, tc.membership, nv.MembershipStatus())
			require.Equal(t, tc.expectedStatus, nv.LivenessStatus())
		})
	}
}
