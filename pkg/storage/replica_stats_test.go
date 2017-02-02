// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestReplicaStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	localities := map[roachpb.NodeID]roachpb.Locality{
		1: {
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "us-east1-a"},
			},
		},
		2: {
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "us-east1-b"},
			},
		},
		3: {
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-west1"},
				{Key: "zone", Value: "us-west1-a"},
			},
		},
	}
	localityOracle := func(nodeID roachpb.NodeID) roachpb.Locality {
		return localities[nodeID]
	}

	testCases := []struct {
		selfID   roachpb.NodeID
		reqs     []roachpb.NodeID
		expected perLocalityCounts
	}{
		{
			selfID:   1,
			reqs:     []roachpb.NodeID{},
			expected: perLocalityCounts{},
		},
		{
			selfID: 1,
			reqs:   []roachpb.NodeID{1, 1, 1},
			expected: perLocalityCounts{
				"region": map[string]int64{"us-east1": 3},
				"zone":   map[string]int64{"us-east1-a": 3},
			},
		},
		{
			selfID: 1,
			reqs:   []roachpb.NodeID{1, 2, 3},
			expected: perLocalityCounts{
				"region": map[string]int64{"us-east1": 2, "us-west1": 1},
				"zone":   map[string]int64{"us-east1-a": 1, "us-east1-b": 1, "us-west1-a": 1},
			},
		},
		{
			selfID:   1,
			reqs:     []roachpb.NodeID{4, 5, 6},
			expected: perLocalityCounts{},
		},
		{
			selfID: 1,
			reqs:   []roachpb.NodeID{1, 4, 2, 5, 3, 6},
			expected: perLocalityCounts{
				"region": map[string]int64{"us-east1": 2, "us-west1": 1},
				"zone":   map[string]int64{"us-east1-a": 1, "us-east1-b": 1, "us-west1-a": 1},
			},
		},
	}
	for i, tc := range testCases {
		rs := newReplicaStats(localities[tc.selfID], localityOracle)
		for _, req := range tc.reqs {
			rs.record(req)
		}
		actual, dur := rs.getRequestCounts()
		if dur == 0 {
			t.Errorf("%d: expected non-zero measurment duration, got: %v", i, dur)
		}
		if len(actual) != len(tc.expected) {
			t.Errorf("%d: len(actual)=%d, len(expected)=%d; actual=%+v",
				i, len(actual), len(tc.expected), actual)
		}
		for k, expV := range tc.expected {
			if actV, ok := actual[k]; !ok {
				t.Errorf("%d: result missing expected key %s; result=%+v", i, k, actual)
			} else {
				if len(actV) != len(expV) {
					t.Errorf("%d: len(actual[%s])=%d, len(expected[%s])=%d; actual[%s]=%+v",
						i, k, len(actV), k, len(expV), k, actV)
				}
				for k2, expV2 := range expV {
					if actV2 := actV[k2]; actV2 != expV2 {
						t.Errorf("%d: actual[%s][%s]=%d, expected[%s][%s]=%d; actual[%s]=%+v",
							i, k, k2, actV2, k, k2, expV2, k, actV)
					}
				}
			}
		}
	}
}
