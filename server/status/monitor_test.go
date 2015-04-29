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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

func TestNodeStatusMonitor(t *testing.T) {
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
	stats := proto.MVCCStats{
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

	monitorStopper := util.NewStopper()
	storeStopper := util.NewStopper()
	feed := &util.Feed{}
	monitor := NewNodeStatusMonitor()
	sub := feed.Subscribe()
	monitorStopper.RunWorker(func() {
		monitor.StartMonitorFeed(sub)
	})

	for i := 0; i < 3; i++ {
		id := proto.StoreID(i + 1)
		eventList := []interface{}{
			&storage.StartStoreEvent{
				StoreID: id,
			},
			&storage.BeginScanRangesEvent{ // Begin scan phase.
				StoreID: id,
			},
			&storage.UpdateRangeEvent{ // Update during scan, expect it to be ignored.
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
				Delta:   stats,
			},
			&storage.AddRangeEvent{
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
			},
			&storage.UpdateRangeEvent{ // Update during scan after add, should be picked up.
				StoreID: id,
				Desc:    desc1,
				Stats:   stats,
				Delta:   stats,
			},
			&storage.EndScanRangesEvent{ // End Scan.
				StoreID: id,
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
				New: storage.AddRangeEvent{
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
		}
		storeStopper.RunWorker(func() {
			for _, event := range eventList {
				feed.Publish(event)
			}
		})
	}

	storeStopper.Stop()
	feed.Close()
	monitorStopper.Stop()

	expectedStats := proto.MVCCStats{
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
		LastUpdateNanos: 6 * 1E9,
	}

	if a, e := len(monitor.stores), 3; a != e {
		t.Fatalf("unexpected number of stores recorded by monitor; expected %d, got %d", e, a)
	}
	for id, store := range monitor.stores {
		if a, e := store.stats, expectedStats; !reflect.DeepEqual(a, e) {
			t.Errorf("monitored stats for store %d did not match expectation: %v != %v", id, a, e)
		}
		if a, e := store.rangeCount, int64(2); a != e {
			t.Errorf("monitored range count for store %d did not match expectation: %d != %d", id, a, e)
		}
	}
}
