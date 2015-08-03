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

package storage_test

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

type storeEventReader struct {
	// If true, Update events will be recorded in full detail. If false, only a
	// count of update events will be recorded.
	recordUpdateDetail  bool
	perStoreFeeds       map[proto.StoreID][]string
	perStoreUpdateCount map[proto.StoreID]map[proto.Method]int
}

// recordEvent records the events received for all stores. Each event is
// recorded as a simple string value; this is less exhaustive than a full struct
// comparison, but should be easier to correct if future changes slightly modify
// these values.
func (ser *storeEventReader) recordEvent(event interface{}) {
	var sid proto.StoreID
	eventStr := ""
	switch event := event.(type) {
	case *storage.StartStoreEvent:
		sid = event.StoreID
		eventStr = "StartStore"
	case *storage.RegisterRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("RegisterRange scan=%t, rid=%d, live=%d",
			event.Scan, event.Desc.RaftID, event.Stats.LiveBytes)
	case *storage.UpdateRangeEvent:
		if event.Method == proto.InternalResolveIntent ||
			event.Method == proto.InternalResolveIntentRange {
			// Some Internal events are best effort calls that make this test
			// flaky. Ignore them.
			break
		}
		if ser.recordUpdateDetail {
			sid = event.StoreID
			eventStr = fmt.Sprintf("UpdateRange rid=%d, method=%s, livediff=%d",
				event.Desc.RaftID, event.Method.String(), event.Delta.LiveBytes)
		} else {
			m := ser.perStoreUpdateCount[event.StoreID]
			if m == nil {
				m = make(map[proto.Method]int)
				ser.perStoreUpdateCount[event.StoreID] = m
			}
			m[event.Method]++
		}
	case *storage.RemoveRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("RemoveRange rid=%d, live=%d",
			event.Desc.RaftID, event.Stats.LiveBytes)
	case *storage.SplitRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("SplitRange origId=%d, newId=%d, origKey=%d, newKey=%d",
			event.Original.Desc.RaftID, event.New.Desc.RaftID,
			event.Original.Stats.KeyBytes, event.New.Stats.KeyBytes)
	case *storage.MergeRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("MergeRange rid=%d, subId=%d, key=%d, subKey=%d",
			event.Merged.Desc.RaftID, event.Removed.Desc.RaftID,
			event.Merged.Stats.KeyBytes, event.Removed.Stats.KeyBytes)
	case *storage.BeginScanRangesEvent:
		sid = event.StoreID
		eventStr = "BeginScanRanges"
	case *storage.EndScanRangesEvent:
		sid = event.StoreID
		eventStr = "EndScanRanges"
	}
	if sid > 0 {
		ser.perStoreFeeds[sid] = append(ser.perStoreFeeds[sid], eventStr)
	}
}

func (ser *storeEventReader) readEvents(feed *util.Feed) {
	ser.perStoreFeeds = make(map[proto.StoreID][]string)
	ser.perStoreUpdateCount = make(map[proto.StoreID]map[proto.Method]int)
	feed.Subscribe(ser.recordEvent)
}

// eventFeedString describes the event information that was recorded by
// storeEventReader. The formatting is appropriate to paste into this test if as
// a new expected value.
func (ser *storeEventReader) eventFeedString() string {
	var buffer bytes.Buffer
	w := tabwriter.NewWriter(&buffer, 2, 1, 2, ' ', 0)

	var storeIDs sort.IntSlice
	for storeID := range ser.perStoreFeeds {
		storeIDs = append(storeIDs, int(storeID))
	}
	sort.Sort(storeIDs)

	for _, storeID := range storeIDs {
		if feed, ok := ser.perStoreFeeds[proto.StoreID(storeID)]; ok {
			fmt.Fprintf(w, "proto.StoreID(%d): {\n", storeID)
			for _, evt := range feed {
				fmt.Fprintf(w, "\t\"%s\",\n", evt)
			}
			fmt.Fprintf(w, "},\n")
		} else {
			panic("unreachable!")
		}
	}
	return buffer.String()
}

// updateCountString describes the update counts that were recorded by
// storeEventReader.  The formatting is appropriate to paste into this test if
// as a new expected value.
func (ser *storeEventReader) updateCountString() string {
	var buffer bytes.Buffer
	w := tabwriter.NewWriter(&buffer, 2, 1, 2, ' ', 0)

	var storeIDs sort.IntSlice
	for storeID := range ser.perStoreUpdateCount {
		storeIDs = append(storeIDs, int(storeID))
	}
	sort.Sort(storeIDs)

	for _, storeID := range storeIDs {
		if countset, ok := ser.perStoreUpdateCount[proto.StoreID(storeID)]; ok {
			fmt.Fprintf(w, "proto.StoreID(%d): {\n", storeID)

			var methodIDs sort.IntSlice
			for methodID := range countset {
				methodIDs = append(methodIDs, int(methodID))
			}
			sort.Sort(methodIDs)

			for _, methodID := range methodIDs {
				method := proto.Method(methodID)
				if count, okCount := countset[method]; okCount {
					fmt.Fprintf(w, "\tproto.%s:\t%d,\n", method, count)
				} else {
					panic("unreachable!")
				}
			}
		} else {
			panic("unreachable!")
		}
		fmt.Fprintf(w, "},\n")
	}
	return buffer.String()
}

func checkMatch(patternMap, lineMap map[proto.StoreID][]string) bool {
	if len(patternMap) != len(lineMap) {
		return false
	}
	for s, patterns := range patternMap {
		lines, ok := lineMap[s]
		if !ok {
			return false
		}
		if len(patterns) != len(lines) {
			return false
		}
		for i := 0; i < len(patterns); i++ {
			if !regexp.MustCompile(patterns[i]).MatchString(lines[i]) {
				log.Errorf("%d: %s did not match %s", i, patterns[i], lines[i])
				return false
			}
		}
	}
	return true
}

// TestMultiStoreEventFeed verifies that events on multiple stores are properly
// recieved by a single event reader.
func TestMultiStoreEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("disabled until #1531 is fixed")

	stopper := stop.NewStopper()

	// Create a multiTestContext which publishes all store events to the given
	// feed.
	feed := util.NewFeed(stopper)
	mtc := &multiTestContext{
		feed: feed,
	}

	// Start reading events from the feed before starting the stores.
	ser := &storeEventReader{
		recordUpdateDetail: false,
	}
	ser.readEvents(feed)

	mtc.Start(t, 3)
	defer mtc.Stop()

	// Replicate the default range.
	raftID := proto.RaftID(1)
	mtc.replicateRange(raftID, 0, 1, 2)

	// Add some data in a transaction
	err := mtc.db.Txn(func(txn *client.Txn) error {
		b := &client.Batch{}
		b.Put("a", "asdf")
		b.Put("c", "jkl;")
		return txn.Commit(b)
	})
	if err != nil {
		t.Fatalf("error putting data to db: %s", err)
	}

	// AdminSplit in between the two ranges.
	if err := mtc.db.AdminSplit("b"); err != nil {
		t.Fatalf("error splitting initial: %s", err)
	}

	// AdminSplit an empty range at the end of the second range.
	if err := mtc.db.AdminSplit("z"); err != nil {
		t.Fatalf("error splitting second range: %s", err)
	}

	// AdminMerge the empty range back into the second range.
	if err := mtc.db.AdminMerge("c"); err != nil {
		t.Fatalf("error merging final range: %s", err)
	}

	// Add an additional put through the system and wait for all
	// replicas to receive it.
	if _, err := mtc.db.Inc("aa", 5); err != nil {
		t.Fatalf("error putting data to db: %s", err)
	}
	util.SucceedsWithin(t, time.Second, func() error {
		for _, eng := range mtc.engines {
			val, _, err := engine.MVCCGet(eng, proto.Key("aa"), mtc.clock.Now(), true, nil)
			if err != nil {
				return err
			}
			if a, e := mustGetInteger(val), int64(5); a != e {
				return util.Errorf("expected aa = %d, got %d", e, a)
			}
		}
		return nil
	})

	// Close feed and wait for reader to receive all events.
	feed.Flush()
	stopper.Stop()

	// Compare events to expected values.
	expected := map[proto.StoreID][]string{
		proto.StoreID(1): {
			"StartStore",
			"BeginScanRanges",
			"RegisterRange scan=true, rid=1, live=.*",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origKey=336, newKey=15",
			"SplitRange origId=2, newId=3, origKey=15, newKey=0",
			"MergeRange rid=2, subId=3, key=15, subKey=0",
		},
		proto.StoreID(2): {
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"RegisterRange scan=false, rid=1, live=.*",
			"SplitRange origId=1, newId=2, origKey=336, newKey=15",
			"SplitRange origId=2, newId=3, origKey=15, newKey=0",
			"MergeRange rid=2, subId=3, key=15, subKey=0",
		},
		proto.StoreID(3): {
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"RegisterRange scan=false, rid=1, live=.*",
			"SplitRange origId=1, newId=2, origKey=336, newKey=15",
			"SplitRange origId=2, newId=3, origKey=15, newKey=0",
			"MergeRange rid=2, subId=3, key=15, subKey=0",
		},
	}
	if a, e := ser.perStoreFeeds, expected; !checkMatch(e, a) {
		t.Errorf("event feed did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Event feed information:\n%s", ser.eventFeedString())
	}

	// Expected count of update events on a per-method basis.
	expectedUpdateCount := map[proto.StoreID]map[proto.Method]int{
		proto.StoreID(1): {
			proto.Put:                 18,
			proto.ConditionalPut:      7,
			proto.Increment:           2,
			proto.Delete:              2,
			proto.EndTransaction:      6,
			proto.InternalLeaderLease: 3,
		},
		proto.StoreID(2): {
			proto.Put:                 16,
			proto.ConditionalPut:      6,
			proto.Increment:           2,
			proto.Delete:              2,
			proto.EndTransaction:      5,
			proto.InternalLeaderLease: 2,
		},
		proto.StoreID(3): {
			proto.Put:                 14,
			proto.ConditionalPut:      5,
			proto.Increment:           2,
			proto.Delete:              2,
			proto.EndTransaction:      4,
			proto.InternalLeaderLease: 2,
		},
	}
	if a, e := ser.perStoreUpdateCount, expectedUpdateCount; !reflect.DeepEqual(a, e) {
		t.Errorf("update counts did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Update count information:\n%s", ser.updateCountString())
	}
}
