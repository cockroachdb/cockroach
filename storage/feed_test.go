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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type storeEventConsumer struct {
	sub      *util.Subscription
	received []interface{}
}

func newConsumer(feed StoreEventFeed) *storeEventConsumer {
	return &storeEventConsumer{
		sub: feed.Subscribe(),
	}
}

func (sec *storeEventConsumer) process() {
	for e := range sec.sub.Events() {
		sec.received = append(sec.received, e)
	}
}

// startConsumerSet starts a StoreEventFeed and a number of associated
// consumers.
func startConsumerSet(count int) (*util.Stopper, StoreEventFeed, []*storeEventConsumer) {
	stopper := util.NewStopper()
	feed := NewStoreEventFeed()
	consumers := make([]*storeEventConsumer, count)
	for i := range consumers {
		consumers[i] = newConsumer(feed)
		stopper.RunWorker(consumers[i].process)
	}
	return stopper, feed, consumers
}

// waitForStopper stops the supplied util.Stopper and waits up to five seconds
// for it to complete.
func waitForStopper(t testing.TB, stopper *util.Stopper) {
	stopper.Stop()
	select {
	case <-stopper.IsStopped():
	case <-time.After(5 * time.Second):
		t.Fatalf("Stopper failed to stop after 5 seconds")
	}
}

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
			MVCCStats: proto.MVCCStats{
				LiveBytes: 400,
				KeyBytes:  40,
				ValBytes:  360,
			},
		},
	}
	rng1.SetDesc(desc1)
	rng2 := &Range{
		stats: &rangeStats{
			raftID: desc2.RaftID,
			MVCCStats: proto.MVCCStats{
				LiveBytes: 200,
				KeyBytes:  30,
				ValBytes:  170,
			},
		},
	}
	rng2.SetDesc(desc2)
	diffStats := &proto.MVCCStats{
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
				feed.newRange(rng1)
			},
			&NewRangeEvent{
				Desc: &proto.RangeDescriptor{
					RaftID:   1,
					StartKey: proto.Key("a"),
					EndKey:   proto.Key("b"),
				},
				Stats: proto.MVCCStats{
					LiveBytes: 400,
					KeyBytes:  40,
					ValBytes:  360,
				},
			},
		},
		{
			"UpdateRange",
			func(feed StoreEventFeed) {
				feed.updateRange(rng1, diffStats)
			},
			&UpdateRangeEvent{
				Desc: &proto.RangeDescriptor{
					RaftID:   1,
					StartKey: proto.Key("a"),
					EndKey:   proto.Key("b"),
				},
				Stats: proto.MVCCStats{
					LiveBytes: 400,
					KeyBytes:  40,
					ValBytes:  360,
				},
				Diff: proto.MVCCStats{
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
				Desc: &proto.RangeDescriptor{
					RaftID:   2,
					StartKey: proto.Key("b"),
					EndKey:   proto.Key("c"),
				},
				Stats: proto.MVCCStats{
					LiveBytes: 200,
					KeyBytes:  30,
					ValBytes:  170,
				},
			},
		},
		{
			"SplitRange",
			func(feed StoreEventFeed) {
				feed.splitRange(rng1, rng2, diffStats)
			},
			&SplitRangeEvent{
				Original: UpdateRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   1,
						StartKey: proto.Key("a"),
						EndKey:   proto.Key("b"),
					},
					Stats: proto.MVCCStats{
						LiveBytes: 400,
						KeyBytes:  40,
						ValBytes:  360,
					},
					Diff: proto.MVCCStats{
						IntentBytes: 30,
						IntentAge:   20,
					},
				},
				New: NewRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   2,
						StartKey: proto.Key("b"),
						EndKey:   proto.Key("c"),
					},
					Stats: proto.MVCCStats{
						LiveBytes: 200,
						KeyBytes:  30,
						ValBytes:  170,
					},
				},
			},
		},
		{
			"MergeRange",
			func(feed StoreEventFeed) {
				feed.mergeRange(rng1, rng2, diffStats)
			},
			&MergeRangeEvent{
				Merged: UpdateRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   1,
						StartKey: proto.Key("a"),
						EndKey:   proto.Key("b"),
					},
					Stats: proto.MVCCStats{
						LiveBytes: 400,
						KeyBytes:  40,
						ValBytes:  360,
					},
					Diff: proto.MVCCStats{
						IntentBytes: 30,
						IntentAge:   20,
					},
				},
				Removed: RemoveRangeEvent{
					Desc: &proto.RangeDescriptor{
						RaftID:   2,
						StartKey: proto.Key("b"),
						EndKey:   proto.Key("c"),
					},
					Stats: proto.MVCCStats{
						LiveBytes: 200,
						KeyBytes:  30,
						ValBytes:  170,
					},
				},
			},
		},
		{
			"BeginScanRanges",
			func(feed StoreEventFeed) {
				feed.beginScanRanges()
			},
			&BeginScanRangesEvent{},
		},
		{
			"EndScanRanges",
			func(feed StoreEventFeed) {
				feed.endScanRanges()
			},
			&EndScanRangesEvent{},
		},
	}

	// Compile expected events into a single slice.
	expectedEvents := make([]interface{}, len(testCases))
	for i := range testCases {
		expectedEvents[i] = testCases[i].expected
	}

	// assertEventsEqual verifies that the given set of events is equal to the
	// expectedEvents.
	verifyEventSlice := func(source string, events []interface{}) {
		if a, e := len(events), len(expectedEvents); a != e {
			t.Errorf("%s had wrong number of events %d, expected %d", source, a, e)
			return
		}

		for i := range events {
			if a, e := events[i], expectedEvents[i]; !reflect.DeepEqual(a, e) {
				t.Errorf("%s had wrong event for case %s: got %v, expected %v", source, testCases[i].name, a, e)
			}
		}
	}

	// Run test cases directly through a feed.
	stopper, feed, consumers := startConsumerSet(3)
	for _, tc := range testCases {
		tc.publishTo(feed)
	}
	feed.closeFeed()
	waitForStopper(t, stopper)
	for i, c := range consumers {
		verifyEventSlice(fmt.Sprintf("feed direct consumer %d", i), c.received)
	}
}
