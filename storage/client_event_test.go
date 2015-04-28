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
	perStoreFeeds map[proto.StoreID][]string
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
		sid = event.StoreID
		eventStr = fmt.Sprintf("UpdateRange rid=%d, live=%d",
			event.Desc.RaftID, event.Diff.LiveBytes)
	case *storage.RemoveRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("RemoveRange rid=%d, live=%d",
			event.Desc.RaftID, event.Stats.LiveBytes)
	case *storage.SplitRangeEvent:
		sid = event.StoreID
		eventStr = fmt.Sprintf("SplitRange origId=%d, newId=%d, origLive=%d, newLive=%d",
			event.Original.Desc.RaftID, event.New.Desc.RaftID,
			event.Original.Stats.LiveBytes, event.New.Stats.LiveBytes)
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
	for e := range sub.Events() {
		ser.recordEvent(e)
	}
}

// printValues is used to assist the debugging of tests which use
// storeEventReader.
func (ser *storeEventReader) printValues(t testing.TB) {
	for id, feed := range ser.perStoreFeeds {
		fmt.Printf("proto.StoreID(%d): []string{\n", int64(id))
		for _, evt := range feed {
			fmt.Printf("\t\t\"%s\",\n", evt)
		}
		fmt.Printf("},\n")
	}
}

// TestMultiStoreEventFeed verifies that events on multiple stores are properly
// recieved by a single event reader.
func TestMultiStoreEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)

	// Create a multiTestContext which publishes all store events to the given
	// feed.
	ser := &storeEventReader{}
	feed := &util.Feed{}
	mtc := &multiTestContext{
		feed: feed,
	}

	// Start reading events from the feed before starting the stores.
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
	<-readStopper.IsStopped()

	// Compare events to expected values.
	expected := map[proto.StoreID][]string{
		proto.StoreID(1): []string{
			"StartStore",
			"BeginScanRanges",
			"AddRange rid=1, live=348",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origLive=938, newLive=42",
			"SplitRange origId=2, newId=3, origLive=42, newLive=0",
			"MergeRange rid=2, subId=3, live=42, subLive=0",
		},
		proto.StoreID(2): []string{
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origLive=938, newLive=42",
			"SplitRange origId=2, newId=3, origLive=42, newLive=0",
			"MergeRange rid=2, subId=3, live=42, subLive=0",
		},
		proto.StoreID(3): []string{
			"StartStore",
			"BeginScanRanges",
			"EndScanRanges",
			"SplitRange origId=1, newId=2, origLive=938, newLive=42",
			"SplitRange origId=2, newId=3, origLive=42, newLive=0",
			"MergeRange rid=2, subId=3, live=42, subLive=0",
		},
	}
	if a, e := ser.perStoreFeeds, expected; !reflect.DeepEqual(a, e) {
		t.Errorf("event feed did not meet expected value: printing actual values to compare with above expectation:\n")
		ser.printValues(t)
	}
}
