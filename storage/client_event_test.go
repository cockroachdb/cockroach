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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type storeEventReader struct {
	// If true, Update events will be recorded in full detail. If false, only a
	// count of update events will be recorded.
	recordUpdateDetail  bool
	perStoreFeeds       map[proto.StoreID][]string
	perStoreUpdateCount map[proto.StoreID]int
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
	case *storage.AddRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("AddRange rid=%d, live=%d",
			event.Desc.RaftID, event.Stats.LiveBytes)
	case *storage.UpdateRangeEvent:
		if event.Method == proto.InternalResolveIntent {
			// InternalResolveIntent is a best effort call that seems to make
			// this test flaky. Ignore them.
			break
		}
		if ser.recordUpdateDetail {
			sid = event.StoreID
			eventStr = fmt.Sprintf("UpdateRange rid=%d, method=%s, livediff=%d",
				event.Desc.RaftID, event.Method.String(), event.Delta.LiveBytes)
		} else {
			ser.perStoreUpdateCount[event.StoreID]++
		}
	case *storage.RemoveRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("RemoveRange rid=%d, live=%d",
			event.Desc.RaftID, event.Stats.LiveBytes)
	case *storage.SplitRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("SplitRange origId=%d, newId=%d, origLive=%d, newLive=%d, origVal=%d, newVal=%d",
			event.Original.Desc.RaftID, event.New.Desc.RaftID,
			event.Original.Stats.LiveBytes, event.New.Stats.LiveBytes,
			event.Original.Stats.ValBytes, event.New.Stats.ValBytes)
	case *storage.MergeRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("MergeRange rid=%d, subId=%d, live=%d, subLive=%d",
			event.Merged.Desc.RaftID, event.Removed.Desc.RaftID,
			event.Merged.Stats.LiveBytes, event.Removed.Stats.LiveBytes)
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

func (ser *storeEventReader) readEvents(sub *util.Subscription) {
	ser.perStoreFeeds = make(map[proto.StoreID][]string)
	ser.perStoreUpdateCount = make(map[proto.StoreID]int)
	for e := range sub.Events() {
		ser.recordEvent(e)
	}
}

// eventFeedString describes the event information that was recorded by
// storeEventReader. The formatting is appropriate to paste into this test if as
// a new expected value.
func (ser *storeEventReader) eventFeedString() string {
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

// updateCountString describes the update counts that were recorded by
// storeEventReader.  The formatting is appropriate to paste into this test if
// as a new expected value.
func (ser *storeEventReader) updateCountString() string {
	var response string
	for id, c := range ser.perStoreUpdateCount {
		response += fmt.Sprintf("proto.StoreID(%d): %d,\n", int64(id), c)
	}
	return response
}

// TestMultiStoreEventFeed verifies that events on multiple stores are properly
// recieved by a single event reader.
func TestMultiStoreEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)
	engine.EnableMVCCComputeStatsDiagnostics(true)
	defer engine.EnableMVCCComputeStatsDiagnostics(false)

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
	raftID := int64(1)
	mtc.replicateRange(raftID, 0, 1, 2)

	// Add some data in a transaction
	err := mtc.db.RunTransaction(nil, func(txn *client.Txn) error {
		return txn.Run(
			client.Put(proto.Key("a"), []byte("asdf")),
			client.Put(proto.Key("c"), []byte("jkl;")),
		)
	})
	if err != nil {
		t.Fatalf("error putting data to db: %s", err.Error())
	}

	// AdminSplit in between the two ranges.
	err = mtc.db.Run(
		client.Call{
			Args: &proto.AdminSplitRequest{
				RequestHeader: proto.RequestHeader{
					Key: proto.Key("b"),
				},
				SplitKey: proto.Key("b"),
			},
			Reply: &proto.AdminSplitResponse{},
		},
	)
	if err != nil {
		t.Fatalf("error splitting initial: %s", err.Error())
	}

	// AdminSplit an empty range at the end of the second range.
	err = mtc.db.Run(
		client.Call{
			Args: &proto.AdminSplitRequest{
				RequestHeader: proto.RequestHeader{
					Key: proto.Key("z"),
				},
				SplitKey: proto.Key("z"),
			},
			Reply: &proto.AdminSplitResponse{},
		},
	)
	if err != nil {
		t.Fatalf("error splitting second range: %s", err.Error())
	}

	// AdminMerge the empty range back into the second range.
	err = mtc.db.Run(
		client.Call{
			Args: &proto.AdminMergeRequest{
				RequestHeader: proto.RequestHeader{
					Key: proto.Key("c"),
				},
			},
			Reply: &proto.AdminMergeResponse{},
		},
	)
	if err != nil {
		t.Fatalf("error merging final range: %s", err.Error())
	}

	// Add an additional put through the system and wait for all
	// replicas to receive it.
	err = mtc.db.Run(
		client.Increment(proto.Key("aa"), 5),
	)
	if err != nil {
		t.Fatalf("error putting data to db: %s", err.Error())
	}
	util.SucceedsWithin(t, time.Second, func() error {
		for _, eng := range mtc.engines {
			val, err := engine.MVCCGet(eng, proto.Key("aa"), mtc.clock.Now(), true, nil)
			if err != nil {
				return err
			}
			if a, e := val.GetInteger(), int64(5); a != e {
				return util.Errorf("expected aa = %d, got %d", e, a)
			}
		}
		return nil
	})

	// Close feed and wait for reader to receive all events.
	feed.Close()
	readStopper.Stop()

	// Compare events to expected values.
	expected := map[proto.StoreID][]string{
		proto.StoreID(1): []string{
			"StartStore",
			"BeginScanRanges",
			"AddRange rid=1, live=348",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origLive=858, newLive=42, origVal=719, newVal=27",
			"SplitRange origId=2, newId=3, origLive=42, newLive=0, origVal=27, newVal=0",
			"MergeRange rid=2, subId=3, live=42, subLive=0",
		},
		proto.StoreID(2): []string{
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origLive=858, newLive=42, origVal=719, newVal=27",
			"SplitRange origId=2, newId=3, origLive=42, newLive=0, origVal=27, newVal=0",
			"MergeRange rid=2, subId=3, live=42, subLive=0",
		},
		proto.StoreID(3): []string{
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origLive=858, newLive=42, origVal=719, newVal=27",
			"SplitRange origId=2, newId=3, origLive=42, newLive=0, origVal=27, newVal=0",
			"MergeRange rid=2, subId=3, live=42, subLive=0",
		},
	}
	if a, e := ser.perStoreFeeds, expected; !reflect.DeepEqual(a, e) {
		t.Errorf("event feed did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Event feed information:\n%s", ser.eventFeedString())
	}

	expectedUpdateCount := map[proto.StoreID]int{
		proto.StoreID(1): 34,
		proto.StoreID(2): 31,
		proto.StoreID(3): 29,
	}
	if a, e := ser.perStoreUpdateCount, expectedUpdateCount; !reflect.DeepEqual(a, e) {
		t.Errorf("update counts did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Update count information:\n%s", ser.updateCountString())
	}
}
