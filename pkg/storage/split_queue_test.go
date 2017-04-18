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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestSplitQueueShouldQueue verifies shouldQueue method correctly
// combines splits in zone configs with the size of the range.
func TestSplitQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Set zone configs.
	config.TestingSetZoneConfig(2000, config.ZoneConfig{RangeMaxBytes: 32 << 20})
	config.TestingSetZoneConfig(2002, config.ZoneConfig{RangeMaxBytes: 32 << 20})

	testCases := []struct {
		start, end roachpb.RKey
		bytes      int64
		maxBytes   int64
		shouldQ    bool
		priority   float64
	}{
		// No intersection, no bytes.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 0, 64 << 20, false, 0},
		// Intersection in zone, no bytes.
		{keys.MakeTablePrefix(2001), roachpb.RKeyMax, 0, 64 << 20, true, 1},
		// Already split at largest ID.
		{keys.MakeTablePrefix(2002), roachpb.RKeyMax, 0, 32 << 20, false, 0},
		// Multiple intersections, no bytes.
		{roachpb.RKeyMin, roachpb.RKeyMax, 0, 64 << 20, true, 1},
		// No intersection, max bytes.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64 << 20, 64 << 20, false, 0},
		// No intersection, max bytes+1.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64<<20 + 1, 64 << 20, true, 1},
		// No intersection, max bytes * 2.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64 << 21, 64 << 20, true, 2},
		// Intersection, max bytes +1.
		{keys.MakeTablePrefix(2000), roachpb.RKeyMax, 32<<20 + 1, 32 << 20, true, 2},
		// Split needed at table boundary, but no zone config.
		{keys.MakeTablePrefix(2001), roachpb.RKeyMax, 32<<20 + 1, 64 << 20, true, 1},
	}

	splitQ := newSplitQueue(tc.store, nil, tc.gossip)

	cfg, ok := tc.gossip.GetSystemConfig()
	if !ok {
		t.Fatal("config not set")
	}

	for i, test := range testCases {
		// Create a replica for testing that is not hooked up to the store. This
		// ensures that the store won't be mucking with our replica concurrently
		// during testing (e.g. via the system config gossip update).
		copy := *tc.repl.Desc()
		copy.StartKey = test.start
		copy.EndKey = test.end
		repl, err := NewReplica(&copy, tc.store, 0)
		if err != nil {
			t.Fatal(err)
		}

		repl.mu.Lock()
		repl.mu.state.Stats = enginepb.MVCCStats{KeyBytes: test.bytes}
		repl.mu.maxBytes = test.maxBytes
		repl.mu.Unlock()

		shouldQ, priority := splitQ.shouldQueue(context.TODO(), hlc.Timestamp{}, repl, cfg)
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
