// Copyright 2022 The Cockroach Authors.
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
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := &StoreConfig{}
			cfg.SetDefaults(1 /* numStores */)
			sg := NewStoreGossip(nil, nil, cfg.TestingKnobs.GossipTestingKnobs)
			sg.cachedCapacity.cached = tc.cached
			sg.cachedCapacity.lastGossiped = tc.lastGossiped

			should, reason := sg.shouldGossipOnCapacityDelta()
			require.Equal(t, tc.expectedReason, reason)
			require.Equal(t, tc.expectedShould, should)
		})
	}
}
