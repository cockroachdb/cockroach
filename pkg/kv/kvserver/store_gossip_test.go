// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	math "math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestStoreGossipDeltaTrigger asserts that the delta between the last gosipped
// capacity and the current cached capacity will trigger gossip depending on
// the change.
func TestStoreGossipDeltaTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		desc                 string
		cached, lastGossiped roachpb.StoreCapacity
		expectedReason       string
		expectedShould       bool
		lastGossipTime       time.Time
	}{
		{
			desc:           "no delta (empty): shouldn't gossip",
			lastGossiped:   roachpb.StoreCapacity{},
			cached:         roachpb.StoreCapacity{},
			expectedReason: "",
			expectedShould: false,
		},
		{
			desc:           "no delta: shouldn't gossip",
			lastGossiped:   roachpb.StoreCapacity{QueriesPerSecond: 1000, WritesPerSecond: 1000, RangeCount: 1000, LeaseCount: 1000},
			cached:         roachpb.StoreCapacity{QueriesPerSecond: 1000, WritesPerSecond: 1000, RangeCount: 1000, LeaseCount: 1000},
			expectedReason: "",
			expectedShould: false,
		},
		{
			desc:           "delta less than abs: shouldn't gossip",
			lastGossiped:   roachpb.StoreCapacity{QueriesPerSecond: 100, WritesPerSecond: 100, RangeCount: 100, LeaseCount: 100},
			cached:         roachpb.StoreCapacity{QueriesPerSecond: 199, WritesPerSecond: 199, RangeCount: 104, LeaseCount: 104},
			expectedReason: "",
			expectedShould: false,
		},
		{
			desc:           "should gossip on qps delta (>50%)",
			lastGossiped:   roachpb.StoreCapacity{QueriesPerSecond: 100, WritesPerSecond: 100, RangeCount: 100, LeaseCount: 100},
			cached:         roachpb.StoreCapacity{QueriesPerSecond: 200, WritesPerSecond: 199, RangeCount: 96, LeaseCount: 96},
			expectedReason: "queries-per-second(100.0) change",
			expectedShould: true,
		},
		{
			desc:           "should gossip on all delta",
			lastGossiped:   roachpb.StoreCapacity{QueriesPerSecond: 100, WritesPerSecond: 100, RangeCount: 10, LeaseCount: 10},
			cached:         roachpb.StoreCapacity{QueriesPerSecond: 200, WritesPerSecond: 0, RangeCount: 15, LeaseCount: 5},
			expectedReason: "queries-per-second(100.0) writes-per-second(-100.0) range-count(5.0) lease-count(-5.0) change",
			expectedShould: true,
		},
		{
			desc:           "no delta: IO overload <= minimum",
			lastGossiped:   roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(0)},
			cached:         roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(allocatorimpl.DefaultLeaseIOOverloadThreshold - 1e9)},
			expectedReason: "",
			expectedShould: false,
		},
		{
			desc:           "no delta: IO overload unchanged",
			lastGossiped:   roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(allocatorimpl.DefaultLeaseIOOverloadThreshold)},
			cached:         roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(allocatorimpl.DefaultLeaseIOOverloadThreshold)},
			expectedReason: "",
			expectedShould: false,
		},
		{
			desc:           "should gossip on IO overload increase greater than min",
			lastGossiped:   roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(0)},
			cached:         roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(allocatorimpl.DefaultLeaseIOOverloadThreshold)},
			expectedReason: "io-overload(0.3) change",
			expectedShould: true,
		},
		{
			desc:           "should gossip on IO overload increase greater than min but last gossip was too recent",
			lastGossiped:   roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(0)},
			cached:         roachpb.StoreCapacity{IOThresholdMax: allocatorimpl.TestingIOThresholdWithScore(allocatorimpl.DefaultLeaseIOOverloadThreshold)},
			expectedReason: "",
			expectedShould: false,
			// Set the last gossip time to be some time far in the future, so the
			// next gossip time is also far in the future.
			lastGossipTime: timeutil.Unix(math.MaxInt64/2, 0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := &StoreConfig{}
			cfg.SetDefaults(1 /* numStores */)
			sg := NewStoreGossip(
				nil,
				nil,
				cfg.TestingKnobs.GossipTestingKnobs,
				&cluster.MakeTestingClusterSettings().SV,
				timeutil.DefaultTimeSource{},
			)
			sg.cachedCapacity.cached = tc.cached
			sg.cachedCapacity.lastGossiped = tc.lastGossiped
			t.Logf("lastGossipTime: %v", tc.lastGossipTime)
			sg.cachedCapacity.lastGossipedTime = tc.lastGossipTime

			should, reason := sg.shouldGossipOnCapacityDelta()
			require.Equal(t, tc.expectedReason, reason)
			require.Equal(t, tc.expectedShould, should)
		})
	}
}
