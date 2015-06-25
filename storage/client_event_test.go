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
	"fmt"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

type storeEventReader struct {
	// If true, Update events will be recorded in full detail. If false, only a
	// count of update events will be recorded.
	recordUpdateDetail bool

	sync.Mutex
	perStoreFeeds       map[proto.StoreID][]string
	perStoreUpdateCount map[proto.StoreID]int
	perStoreTotalCount  map[proto.StoreID]int
}

// recordEvent records the events received for all stores. Each event is
// recorded as a simple string value; this is less exhaustive than a full struct
// comparison, but should be easier to correct if future changes slightly modify
// these values.
func (ser *storeEventReader) recordEvent(event interface{}) {
	var sid proto.StoreID
	eventStr := ""

	ser.Lock()
	defer ser.Unlock()
	switch event := event.(type) {
	case *storage.StartStoreEvent:
		sid = event.StoreID
		eventStr = "StartStore"
	case *storage.RegisterRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("RegisterRange scan=%t, rid=%d, live=%d",
			event.Scan, event.Desc.RaftID, event.Stats.LiveBytes)
	case *storage.UpdateRangeEvent:
		if event.Method == proto.InternalResolveIntent || event.Method == proto.InternalResolveIntentRange {
			// InternalResolveIntent is a best effort call that seems to make
			// this test flaky. Ignore them completely.
			break
		}
		if ser.recordUpdateDetail {
			sid = event.StoreID
			eventStr = fmt.Sprintf("UpdateRange rid=%d, method=%s, livediff=%d",
				event.Desc.RaftID, event.Method.String(), event.Delta.LiveBytes)
		} else {
			ser.perStoreUpdateCount[event.StoreID]++
			ser.perStoreTotalCount[event.StoreID]++
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
		ser.perStoreTotalCount[sid]++
	}
}

func (ser *storeEventReader) readEvents(sub *util.Subscription) {
	ser.Lock()
	ser.perStoreFeeds = make(map[proto.StoreID][]string)
	ser.perStoreUpdateCount = make(map[proto.StoreID]int)
	ser.perStoreTotalCount = make(map[proto.StoreID]int)
	ser.Unlock()
	for e := range sub.Events() {
		ser.recordEvent(e)
	}
}

// eventFeedStringLocked describes the event information that was recorded by
// storeEventReader. The formatting is appropriate to paste into this test if as
// a new expected value.
func (ser *storeEventReader) eventFeedStringLocked() string {
	var response string
	for id, feed := range ser.perStoreFeeds {
		response += fmt.Sprintf("proto.StoreID(%d): []string{\n", int64(id))
		for _, evt := range feed {
			response += fmt.Sprintf("\t\t\"%s\",\n", evt)
		}
		response += fmt.Sprintf("},\n")
	}
	return response
}

// updateCountStringLocked describes the update counts that were recorded by
// storeEventReader.  The formatting is appropriate to paste into this test if
// as a new expected value.
func (ser *storeEventReader) updateCountStringLocked() string {
	var response string
	for id, c := range ser.perStoreUpdateCount {
		response += fmt.Sprintf("proto.StoreID(%d): %d,\n", int64(id), c)
	}
	return response
}

func checkMatchLocked(patternMap, lineMap map[proto.StoreID][]string) bool {
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
			if match, err := regexp.Match(patterns[i], []byte(lines[i])); !match || err != nil {
				log.Errorf("%d: %s did not match %s: %v", i, patterns[i], lines[i], err)
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

	// Create a multiTestContext which publishes all store events to the given
	// feed.
	feed := &util.Feed{}
	mtc := &multiTestContext{
		feed: feed,
	}

	// Start reading events from the feed before starting the stores.
	ser := &storeEventReader{
		recordUpdateDetail: false,
	}
	readStopper := util.NewStopper()
	sub := feed.Subscribe()
	readStopper.RunWorker(func() {
		ser.readEvents(sub)
	})

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

	// Establish expected values for the store event recorder.
	expected := map[proto.StoreID][]string{
		proto.StoreID(1): {
			"StartStore",
			"BeginScanRanges",
			"RegisterRange scan=true, rid=1, live=.*",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origKey=316, newKey=15",
			"SplitRange origId=2, newId=3, origKey=15, newKey=0",
			"MergeRange rid=2, subId=3, key=15, subKey=0",
		},
		proto.StoreID(2): {
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"RegisterRange scan=false, rid=1, live=.*",
			"SplitRange origId=1, newId=2, origKey=316, newKey=15",
			"SplitRange origId=2, newId=3, origKey=15, newKey=0",
			"MergeRange rid=2, subId=3, key=15, subKey=0",
		},
		proto.StoreID(3): {
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"RegisterRange scan=false, rid=1, live=.*",
			"SplitRange origId=1, newId=2, origKey=316, newKey=15",
			"SplitRange origId=2, newId=3, origKey=15, newKey=0",
			"MergeRange rid=2, subId=3, key=15, subKey=0",
		},
	}
	expectedUpdateCount := map[proto.StoreID]int{
		proto.StoreID(1): 37,
		proto.StoreID(2): 32,
		proto.StoreID(3): 28,
	}
	expectedTotalCount := map[proto.StoreID]int{}
	for k := range expected {
		expectedTotalCount[k] = len(expected[k]) + expectedUpdateCount[k]
	}

	// Wait for expected total counts to match.
	util.SucceedsWithin(t, time.Second, func() error {
		ser.Lock()
		defer ser.Unlock()
		if a, e := ser.perStoreTotalCount, expectedTotalCount; !reflect.DeepEqual(a, e) {
			return util.Errorf(
				"Total Counts did not match Expected Count: %v != %v.\n"+
					"Event feed information:\n%s\n\n"+
					"Update Count Information:\n%s\n\n",
				a, e, ser.eventFeedStringLocked(), ser.updateCountStringLocked())
		}
		return nil
	})

	// Close feed and readers.
	feed.Close()
	readStopper.Stop()

	// Verify that the events received by the reader are correct.
	ser.Lock()
	defer ser.Unlock()
	if a, e := ser.perStoreFeeds, expected; !checkMatchLocked(e, a) {
		t.Errorf("event feed did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Event feed information:\n%s", ser.eventFeedStringLocked())
	}
	if a, e := ser.perStoreUpdateCount, expectedUpdateCount; !reflect.DeepEqual(a, e) {
		t.Errorf("update counts did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Update count information:\n%s", ser.updateCountStringLocked())
	}
}
