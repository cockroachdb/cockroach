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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"reflect"
	"testing"

	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestNodeStatusMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)
	desc1 := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}
	desc2 := &roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
	}
	stats := engine.MVCCStats{
		LiveBytes:       1,
		KeyBytes:        2,
		ValBytes:        2,
		IntentBytes:     1,
		LiveCount:       1,
		KeyCount:        1,
		ValCount:        1,
		IntentCount:     1,
		IntentAge:       1,
		GCBytesAge:      1,
		LastUpdateNanos: 1 * 1E9,
	}

	stopper := stop.NewStopper()
	feed := util.NewFeed(stopper)
	monitor := NewNodeStatusMonitor(metric.NewRegistry(stopper.ShouldStop()), stopper.ShouldStop())
	monitor.StartMonitorFeed(feed)

	for i := 0; i < 3; i++ {
		id := roachpb.StoreID(i + 1)
		eventList := []interface{}{
			&storage.StartStoreEvent{
				StoreID: id,
			},
			&storage.RegisterRangeEvent{
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
			},
			&storage.UpdateRangeEvent{
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
				Delta:   stats,
			},
			&storage.UpdateRangeEvent{
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
				Delta:   stats,
			},
			&storage.UpdateRangeEvent{
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
				Delta:   stats,
			},
			&storage.SplitRangeEvent{
				StoreID: id,
				Original: storage.UpdateRangeEvent{
					StoreID: id,
					Desc:    desc1,
					Stats:   stats,
					Delta:   stats,
				},
				New: storage.RegisterRangeEvent{
					StoreID: id,
					Desc:    desc2,
					Stats:   stats,
				},
			},
			&storage.UpdateRangeEvent{
				StoreID: id,
				Desc:    desc2,
				Stats:   stats,
				Delta:   stats,
			},
			&storage.UpdateRangeEvent{
				StoreID: id,
				Desc:    desc2,
				Stats:   stats,
				Delta:   stats,
			},
			&CallSuccessEvent{
				NodeID: roachpb.NodeID(1),
				Method: roachpb.Get,
			},
			&CallSuccessEvent{
				NodeID: roachpb.NodeID(1),
				Method: roachpb.Put,
			},
			&CallErrorEvent{
				NodeID: roachpb.NodeID(1),
				Method: roachpb.Scan,
			},
		}
		for _, event := range eventList {
			feed.Publish(event)
		}
	}

	feed.Flush()
	stopper.Stop()

	expectedStats := engine.MVCCStats{
		LiveBytes:       6,
		KeyBytes:        12,
		ValBytes:        12,
		IntentBytes:     6,
		LiveCount:       6,
		KeyCount:        6,
		ValCount:        6,
		IntentCount:     6,
		IntentAge:       6,
		GCBytesAge:      6,
		LastUpdateNanos: 1 * 1E9,
	}

	if a, e := len(monitor.stores), 3; a != e {
		t.Fatalf("unexpected number of stores recorded by monitor; expected %d, got %d", e, a)
	}
	for id, store := range monitor.stores {
		if a, e := store.stats, expectedStats; !reflect.DeepEqual(a, e) {
			t.Errorf("monitored stats for store %d did not match expectation: %v", id, pretty.Diff(a, e))
		}
		if a, e := store.rangeCount.Count(), int64(2); a != e {
			t.Errorf("monitored range count for store %d did not match expectation: %d != %d", id, a, e)
		}
	}

	if a, e := monitor.numSuccess.Count(), int64(6); a != e {
		t.Errorf("monitored stats for node recorded wrong number of ops %d, expected %d", a, e)
	}
	if a, e := monitor.numError.Count(), int64(3); a != e {
		t.Errorf("monitored stats for node recorded wrong number of errors %d, expected %d", a, e)
	}
}
