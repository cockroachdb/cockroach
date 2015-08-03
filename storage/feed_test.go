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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestStoreEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)

	// Construct a set of fake ranges to synthesize events correctly. They do
	// not need to be added to a Store.
	desc1 := &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: proto.Key("a"),
		EndKey:   proto.Key("b"),
	}
	desc2 := &proto.RangeDescriptor{
		RaftID:   2,
		StartKey: proto.Key("b"),
		EndKey:   proto.Key("c"),
	}
	rng1 := &Range{
		stats: &rangeStats{
			raftID: desc1.RaftID,
			MVCCStats: engine.MVCCStats{
				LiveBytes:       400,
				KeyBytes:        40,
				ValBytes:        360,
				LastUpdateNanos: 10 * 1E9,
			},
		},
	}
	if err := rng1.setDesc(desc1); err != nil {
		t.Fatal(err)
	}
	rng2 := &Range{
		stats: &rangeStats{
			raftID: desc2.RaftID,
			MVCCStats: engine.MVCCStats{
				LiveBytes:       200,
				KeyBytes:        30,
				ValBytes:        170,
				LastUpdateNanos: 20 * 1E9,
			},
		},
	}
	if err := rng2.setDesc(desc2); err != nil {
		t.Fatal(err)
	}
	storeDesc := &proto.StoreDescriptor{
		StoreID: proto.StoreID(1),
		Node: proto.NodeDescriptor{
			NodeID: proto.NodeID(1),
		},
		Capacity: proto.StoreCapacity{
			Capacity:   100,
			Available:  100,
			RangeCount: 1,
		},
	}
	diffStats := &engine.MVCCStats{
		IntentBytes: 30,
		IntentAge:   20,
	}

	// A testCase corresponds to a single Store event type. Each case contains a
	// method which publishes a single event to the given storeEventPublisher,
	// and an expected result interface which should match the produced
	// event.
	testCases := []struct {
		name      string
		publishTo func(StoreEventFeed)
		expected  interface{}
	}{
		{
			"NewRange",
			func(feed StoreEventFeed) {
				feed.registerRange(rng1, false /* scan */)
			},
			&RegisterRangeEvent{
				StoreID: proto.StoreID(1),
				Desc: &proto.RangeDescriptor{
					RaftID:   1,
					StartKey: proto.Key("a"),
					EndKey:   proto.Key("b"),
				},
				Stats: engine.MVCCStats{
					LiveBytes:       400,
					KeyBytes:        40,
					ValBytes:        360,
					LastUpdateNanos: 10 * 1E9,
				},
			},
		},
		{
			"UpdateRange",
			func(feed StoreEventFeed) {
				feed.updateRange(rng1, proto.Put, diffStats)
			},
			&UpdateRangeEvent{
				StoreID: proto.StoreID(1),
				Desc: &proto.RangeDescriptor{
					RaftID:   1,
					StartKey: proto.Key("a"),
					EndKey:   proto.Key("b"),
				},
				Stats: engine.MVCCStats{
					LiveBytes:       400,
					KeyBytes:        40,
					ValBytes:        360,
					LastUpdateNanos: 10 * 1E9,
				},
				Method: proto.Put,
				Delta: engine.MVCCStats{
					IntentBytes: 30,
					IntentAge:   20,
				},
			},
		},
		{
			"RemoveRange",
			func(feed StoreEventFeed) {
				feed.removeRange(rng2)
			},
			&RemoveRangeEvent{
				StoreID: proto.StoreID(1),
				Desc: &proto.RangeDescriptor{
					RaftID:   2,
					StartKey: proto.Key("b"),
					EndKey:   proto.Key("c"),
				},
				Stats: engine.MVCCStats{
					LiveBytes:       200,
					KeyBytes:        30,
					ValBytes:        170,
					LastUpdateNanos: 20 * 1E9,
				},
			},
		},
		{
			"SplitRange",
			func(feed StoreEventFeed) {
				feed.splitRange(rng1, rng2)
			},
			&SplitRangeEvent{
				StoreID: proto.StoreID(1),
				Original: UpdateRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   1,
						StartKey: proto.Key("a"),
						EndKey:   proto.Key("b"),
					},
					Stats: engine.MVCCStats{
						LiveBytes:       400,
						KeyBytes:        40,
						ValBytes:        360,
						LastUpdateNanos: 10 * 1E9,
					},
					Delta: engine.MVCCStats{
						LiveBytes:       -200,
						KeyBytes:        -30,
						ValBytes:        -170,
						LastUpdateNanos: 20 * 1E9,
					},
				},
				New: RegisterRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   2,
						StartKey: proto.Key("b"),
						EndKey:   proto.Key("c"),
					},
					Stats: engine.MVCCStats{
						LiveBytes:       200,
						KeyBytes:        30,
						ValBytes:        170,
						LastUpdateNanos: 20 * 1E9,
					},
				},
			},
		},
		{
			"MergeRange",
			func(feed StoreEventFeed) {
				feed.mergeRange(rng1, rng2)
			},
			&MergeRangeEvent{
				StoreID: proto.StoreID(1),
				Merged: UpdateRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   1,
						StartKey: proto.Key("a"),
						EndKey:   proto.Key("b"),
					},
					Stats: engine.MVCCStats{
						LiveBytes:       400,
						KeyBytes:        40,
						ValBytes:        360,
						LastUpdateNanos: 10 * 1E9,
					},
					Delta: engine.MVCCStats{
						LiveBytes:       200,
						KeyBytes:        30,
						ValBytes:        170,
						LastUpdateNanos: 20 * 1E9,
					},
				},
				Removed: RemoveRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   2,
						StartKey: proto.Key("b"),
						EndKey:   proto.Key("c"),
					},
					Stats: engine.MVCCStats{
						LiveBytes:       200,
						KeyBytes:        30,
						ValBytes:        170,
						LastUpdateNanos: 20 * 1E9,
					},
				},
			},
		},
		{
			"StoreStatus",
			func(feed StoreEventFeed) {
				feed.storeStatus(storeDesc)
			},
			&StoreStatusEvent{
				Desc: storeDesc,
			},
		},
		{
			"ReplicationStatus",
			func(feed StoreEventFeed) {
				feed.replicationStatus(3, 2, 1)
			},
			&ReplicationStatusEvent{
				StoreID:              proto.StoreID(1),
				LeaderRangeCount:     3,
				ReplicatedRangeCount: 2,
				AvailableRangeCount:  1,
			},
		},
		{
			"StartStore",
			func(feed StoreEventFeed) {
				feed.startStore(100)
			},
			&StartStoreEvent{
				StoreID:   proto.StoreID(1),
				StartedAt: 100,
			},
		},
		{
			"BeginScanRanges",
			func(feed StoreEventFeed) {
				feed.beginScanRanges()
			},
			&BeginScanRangesEvent{
				StoreID: proto.StoreID(1),
			},
		},
		{
			"EndScanRanges",
			func(feed StoreEventFeed) {
				feed.endScanRanges()
			},
			&EndScanRangesEvent{
				StoreID: proto.StoreID(1),
			},
		},
	}

	// Compile expected events into a single slice.
	expectedEvents := make([]interface{}, len(testCases))
	for i := range testCases {
		expectedEvents[i] = testCases[i].expected
	}

	events := make([]interface{}, 0, len(expectedEvents))

	// Run test cases directly through a feed.
	stopper := stop.NewStopper()
	defer stopper.Stop()
	feed := util.NewFeed(stopper)
	feed.Subscribe(func(event interface{}) {
		events = append(events, event)
	})

	storefeed := NewStoreEventFeed(proto.StoreID(1), feed)
	for _, tc := range testCases {
		tc.publishTo(storefeed)
	}

	feed.Flush()

	if a, e := events, expectedEvents; !reflect.DeepEqual(a, e) {
		t.Errorf("received incorrect events.\nexpected: %v\nactual: %v", e, a)
	}
}
