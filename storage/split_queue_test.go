// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestSplitQueueShouldQueue verifies shouldQueue method correctly
// combines splits in accounting and zone configs with the size of
// the range.
func TestSplitQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Set accounting and zone configs.
	acctMap, err := NewPrefixConfigMap([]*PrefixConfig{
		{proto.KeyMin, nil, config1},
		{proto.Key("/dbA"), nil, config2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.gossip.AddInfo(gossip.KeyConfigAccounting, acctMap, 0); err != nil {
		t.Fatal(err)
	}

	zoneMap, err := NewPrefixConfigMap([]*PrefixConfig{
		{proto.KeyMin, nil, &proto.ZoneConfig{RangeMaxBytes: 64 << 20}},
		{proto.Key("/dbB"), nil, &proto.ZoneConfig{RangeMaxBytes: 64 << 20}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.gossip.AddInfo(gossip.KeyConfigZone, zoneMap, 0); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		start, end proto.Key
		bytes      int64
		shouldQ    bool
		priority   float64
	}{
		// No intersection, no bytes.
		{proto.KeyMin, proto.Key("/"), 0, false, 0},
		// Intersection in accounting, no bytes.
		{proto.Key("/"), proto.Key("/dbA1"), 0, true, 1},
		// Intersection in zone, no bytes.
		{proto.Key("/dbA"), proto.Key("/dbC"), 0, true, 1},
		// Multiple intersections, no bytes.
		{proto.KeyMin, proto.KeyMax, 0, true, 1},
		// No intersection, max bytes.
		{proto.KeyMin, proto.Key("/"), 64 << 20, false, 0},
		// No intersection, max bytes+1.
		{proto.KeyMin, proto.Key("/"), 64<<20 + 1, true, 1},
		// No intersection, max bytes * 2.
		{proto.KeyMin, proto.Key("/"), 64 << 21, true, 2},
		// Intersection, max bytes +1.
		{proto.KeyMin, proto.KeyMax, 64<<20 + 1, true, 2},
	}

	splitQ := newSplitQueue(nil, tc.gossip)

	for i, test := range testCases {
		if err := tc.rng.stats.SetMVCCStats(tc.rng.rm.Engine(), engine.MVCCStats{KeyBytes: test.bytes}); err != nil {
			t.Fatal(err)
		}
		copy := *tc.rng.Desc()
		copy.StartKey = test.start
		copy.EndKey = test.end
		if err := tc.rng.setDesc(&copy); err != nil {
			t.Fatal(err)
		}
		shouldQ, priority := splitQ.shouldQueue(proto.ZeroTimestamp, tc.rng)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if math.Abs(priority-test.priority) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, test.priority, priority)
		}
	}
}

////
// NOTE: tests which actually verify processing of the split queue are
// in client_split_test.go, which is in a different test package in
// order to allow for distributed transactions with a proper client.
