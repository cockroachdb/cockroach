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
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
)

func TestReplicaStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	gceLocalities := map[roachpb.NodeID]string{
		1: "region=us-east1,zone=us-east1-a",
		2: "region=us-east1,zone=us-east1-b",
		3: "region=us-west1,zone=us-west1-a",
		4: "",
	}
	mismatchedLocalities := map[roachpb.NodeID]string{
		1: "region=us-east1,zone=a",
		2: "region=us-east1,zone=b",
		3: "region=us-west1,zone=a",
		4: "zone=us-central1-a",
	}
	missingLocalities := map[roachpb.NodeID]string{}

	testCases := []struct {
		localities map[roachpb.NodeID]string
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
		rs := newReplicaStats(clock, func(nodeID roachpb.NodeID) string {
			return tc.localities[nodeID]
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

func TestReplicaStatsDecay(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	floatMapsEqual := func(expected, actual map[string]float64) bool {
		if len(expected) != len(actual) {
			return false
		}
		for k, v1 := range expected {
			v2, ok := actual[k]
			if !ok {
				return false
			}
			if diff := math.Abs(v2 - v1); diff > 0.00000001 {
				return false
			}
		}
		return true
	}

	awsLocalities := map[roachpb.NodeID]string{
		1: "region=us-east-1,zone=us-east-1a",
		2: "region=us-east-1,zone=us-east-1b",
		3: "region=us-west-1,zone=us-west-1a",
	}

	rs := newReplicaStats(clock, func(nodeID roachpb.NodeID) string {
		return awsLocalities[nodeID]
	})

	{
		counts, dur := rs.getRequestCounts()
		if len(counts) != 0 {
			t.Errorf("expected empty request counts, got %+v", counts)
		}
		if dur != 0 {
			t.Errorf("expected duration = 0, got %v", dur)
		}
		manual.Increment(1)
		if _, dur := rs.getRequestCounts(); dur != 1 {
			t.Errorf("expected duration = 1, got %v", dur)
		}
		rs.resetRequestCounts()
	}

	{
		for _, req := range []roachpb.NodeID{1, 1, 2, 2, 3} {
			rs.record(req)
		}
		expected := perLocalityCounts{
			awsLocalities[1]: 2,
			awsLocalities[2]: 2,
			awsLocalities[3]: 1,
		}
		actual, dur := rs.getRequestCounts()
		if dur != 0 {
			t.Errorf("expected duration = 0, got %v", dur)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
		}

		for i := 0; i < len(rs.mu.requests)-1; i++ {
			manual.Increment(int64(rotateInterval))
			for k, v := range expected {
				expected[k] = v * decayFactor
			}
			actual, dur = rs.getRequestCounts()
			if expectedDur := rotateInterval * time.Duration(i+1); dur != expectedDur {
				t.Errorf("expected duration = %v, got %v", expectedDur, dur)
			}
			// We can't just use DeepEqual to compare these due to the float
			// multiplication inaccuracies.
			if !floatMapsEqual(expected, actual) {
				t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
			}
		}

		manual.Increment(int64(rotateInterval))
		expected = make(perLocalityCounts)
		if actual, _ := rs.getRequestCounts(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
		}
		rs.resetRequestCounts()
	}

	{
		for _, req := range []roachpb.NodeID{1, 1, 2, 2, 3} {
			rs.record(req)
		}
		manual.Increment(int64(rotateInterval))
		for _, req := range []roachpb.NodeID{2, 2, 3, 3, 3} {
			rs.record(req)
		}
		expected := perLocalityCounts{
			awsLocalities[1]: 2 * decayFactor,
			awsLocalities[2]: 2*decayFactor + 2,
			awsLocalities[3]: 1*decayFactor + 3,
		}
		if actual, _ := rs.getRequestCounts(); !reflect.DeepEqual(expected, actual) {
			t.Errorf("incorrect per-locality request counts: %s", pretty.Diff(expected, actual))
		}
	}
}
