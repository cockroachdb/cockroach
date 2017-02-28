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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
)

// TODO(DONOTMERGE): Add tests for windowing/decay logic.
func TestReplicaStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeLocality := func(localitySpec string) roachpb.Locality {
		var locality roachpb.Locality
		_ = locality.Set(localitySpec)
		return locality
	}

	gceLocalities := map[roachpb.NodeID]roachpb.Locality{
		1: makeLocality("region=us-east1,zone=us-east1-a"),
		2: makeLocality("region=us-east1,zone=us-east1-b"),
		3: makeLocality("region=us-west1,zone=us-west1-a"),
		4: {},
	}
	mismatchedLocalities := map[roachpb.NodeID]roachpb.Locality{
		1: makeLocality("region=us-east1,zone=a"),
		2: makeLocality("region=us-east1,zone=b"),
		3: makeLocality("region=us-west1,zone=a"),
		4: makeLocality("zone=us-central1-a"),
	}
	missingLocalities := map[roachpb.NodeID]roachpb.Locality{}

	testCases := []struct {
		localities map[roachpb.NodeID]roachpb.Locality
		reqs       []roachpb.NodeID
		expected   perLocalityCounts
	}{
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{},
			expected:   perLocalityCounts{},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{1, 1, 1},
			expected: perLocalityCounts{
				"region=us-east1,zone=us-east1-a": 3,
			},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{1, 2, 3},
			expected: perLocalityCounts{
				"region=us-east1,zone=us-east1-a": 1,
				"region=us-east1,zone=us-east1-b": 1,
				"region=us-west1,zone=us-west1-a": 1,
			},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{4, 5, 6},
			expected: perLocalityCounts{
				"": 3,
			},
		},
		{
			localities: gceLocalities,
			reqs:       []roachpb.NodeID{1, 4, 2, 5, 3, 6},
			expected: perLocalityCounts{
				"region=us-east1,zone=us-east1-a": 1,
				"region=us-east1,zone=us-east1-b": 1,
				"region=us-west1,zone=us-west1-a": 1,
				"": 3,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{},
			expected:   perLocalityCounts{},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{1, 1, 1},
			expected: perLocalityCounts{
				"region=us-east1,zone=a": 3,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{1, 2, 3, 4},
			expected: perLocalityCounts{
				"region=us-east1,zone=a": 1,
				"region=us-east1,zone=b": 1,
				"region=us-west1,zone=a": 1,
				"zone=us-central1-a":     1,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{4, 5, 6},
			expected: perLocalityCounts{
				"zone=us-central1-a": 1,
				"":                   2,
			},
		},
		{
			localities: mismatchedLocalities,
			reqs:       []roachpb.NodeID{1, 4, 2, 5, 3, 6},
			expected: perLocalityCounts{
				"region=us-east1,zone=a": 1,
				"region=us-east1,zone=b": 1,
				"region=us-west1,zone=a": 1,
				"zone=us-central1-a":     1,
				"":                       2,
			},
		},
		{
			localities: missingLocalities,
			reqs:       []roachpb.NodeID{},
			expected:   perLocalityCounts{},
		},
		{
			localities: missingLocalities,
			reqs:       []roachpb.NodeID{1, 1, 1},
			expected: perLocalityCounts{
				"": 3,
			},
		},
		{
			localities: missingLocalities,
			reqs:       []roachpb.NodeID{1, 2, 3, 4, 5, 6},
			expected: perLocalityCounts{
				"": 6,
			},
		},
	}
	for i, tc := range testCases {
		rs := newReplicaStats(func(nodeID roachpb.NodeID) string {
			return tc.localities[nodeID].String()
		})
		for _, req := range tc.reqs {
			rs.record(req)
		}
		actual, dur := rs.getRequestCounts()
		if dur == 0 {
			t.Errorf("%d: expected non-zero measurement duration, got: %v", i, dur)
		}
		if !reflect.DeepEqual(tc.expected, actual) {
			t.Errorf("%d: incorrect per-locality request counts: %s", i, pretty.Diff(tc.expected, actual))
		}
		rs.resetRequestCounts()
		if actual, _ := rs.getRequestCounts(); len(actual) != 0 {
			t.Errorf("%d: unexpected non-zero request counts after resetting: %+v", i, actual)
		}
	}
}
