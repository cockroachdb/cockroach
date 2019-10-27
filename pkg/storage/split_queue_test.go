// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
)

// TestSplitQueueShouldQueue verifies shouldSplitRange method correctly
// combines splits in zone configs with the size of the range.
func TestSplitQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	// Set zone configs.
	config.TestingSetZoneConfig(2000, zonepb.ZoneConfig{RangeMaxBytes: proto.Int64(32 << 20)})
	config.TestingSetZoneConfig(2002, zonepb.ZoneConfig{RangeMaxBytes: proto.Int64(32 << 20)})

	testCases := []struct {
		start, end roachpb.RKey
		bytes      int64
		maxBytes   int64
		shouldQ    bool
		priority   float64
	}{
		// No intersection, no bytes, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 0, 64 << 20, false, 0},
		// Intersection in zone, no bytes, no load.
		{keys.MakeTablePrefix(2001), roachpb.RKeyMax, 0, 64 << 20, true, 1},
		// Already split at largest ID, no load.
		{keys.MakeTablePrefix(2002), roachpb.RKeyMax, 0, 32 << 20, false, 0},
		// Multiple intersections, no bytes, no load.
		{roachpb.RKeyMin, roachpb.RKeyMax, 0, 64 << 20, true, 1},
		// No intersection, max bytes, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64 << 20, 64 << 20, false, 0},
		// No intersection, max bytes+1, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64<<20 + 1, 64 << 20, true, 1},
		// No intersection, max bytes * 2, no load.
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), 64 << 21, 64 << 20, true, 2},
		// Intersection, max bytes +1, no load.
		{keys.MakeTablePrefix(2000), roachpb.RKeyMax, 32<<20 + 1, 32 << 20, true, 2},
		// Split needed at table boundary, but no zone config, no load.
		{keys.MakeTablePrefix(2001), roachpb.RKeyMax, 32<<20 + 1, 64 << 20, true, 1},
	}

	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
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
		repl.mu.state.Stats = &enginepb.MVCCStats{KeyBytes: test.bytes}
		repl.mu.Unlock()
		zoneConfig := zonepb.DefaultZoneConfig()
		zoneConfig.RangeMaxBytes = proto.Int64(test.maxBytes)
		repl.SetZoneConfig(&zoneConfig)

		// Testing using shouldSplitRange instead of shouldQueue to avoid using the splitFinder
		// This tests the merge queue behavior too as a result. For splitFinder tests,
		// see split/split_test.go.
		shouldQ, priority := shouldSplitRange(
			ctx, repl.Desc(), repl.GetMVCCStats(), repl.GetMaxBytes(), cfg)
		if shouldQ != test.shouldQ {
			t.Errorf("%d: should queue expected %t; got %t", i, test.shouldQ, shouldQ)
		}
		if math.Abs(priority-test.priority) > 0.00001 {
			t.Errorf("%d: priority expected %f; got %f", i, test.priority, priority)
		}
	}
}
