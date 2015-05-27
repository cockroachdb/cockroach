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

package status_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// simpleEventConsumer stores every event published to a feed.
type simpleEventConsumer struct {
	sub      *util.Subscription
	received []interface{}
}

func newSimpleEventConsumer(feed *util.Feed) *simpleEventConsumer {
	return &simpleEventConsumer{
		sub: feed.Subscribe(),
	}
}

func (sec *simpleEventConsumer) process() {
	for e := range sec.sub.Events() {
		sec.received = append(sec.received, e)
	}
}

// startConsumerSet starts a NodeEventFeed and a number of associated
// simple consumers.
func startConsumerSet(count int) (*util.Stopper, *util.Feed, []*simpleEventConsumer) {
	stopper := util.NewStopper()
	feed := &util.Feed{}
	consumers := make([]*simpleEventConsumer, count)
	for i := range consumers {
		consumers[i] = newSimpleEventConsumer(feed)
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

func TestNodeEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)

	// A testCase corresponds to a single Store event type. Each case contains a
	// method which publishes a single event to the given storeEventPublisher,
	// and an expected result interface which should match the produced
	// event.
	testCases := []struct {
		name      string
		publishTo func(status.NodeEventFeed)
		expected  interface{}
	}{
		{
			name: "Get",
			publishTo: func(nef status.NodeEventFeed) {
				call := client.Get(proto.Key("abc"))
				nef.CallComplete(call.Args, call.Reply)
			},
			expected: &status.CallSuccessEvent{
				NodeID: proto.NodeID(1),
				Method: proto.Get,
			},
		},
		{
			name: "Put",
			publishTo: func(nef status.NodeEventFeed) {
				call := client.Put(proto.Key("abc"), []byte("def"))
				nef.CallComplete(call.Args, call.Reply)
			},
			expected: &status.CallSuccessEvent{
				NodeID: proto.NodeID(1),
				Method: proto.Put,
			},
		},
		{
			name: "Get Error",
			publishTo: func(nef status.NodeEventFeed) {
				call := client.Get(proto.Key("abc"))
				call.Reply.Header().SetGoError(util.Errorf("error"))
				nef.CallComplete(call.Args, call.Reply)
			},
			expected: &status.CallErrorEvent{
				NodeID: proto.NodeID(1),
				Method: proto.Get,
			},
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
	nodefeed := status.NewNodeEventFeed(proto.NodeID(1), feed)
	for _, tc := range testCases {
		tc.publishTo(nodefeed)
	}
	feed.Close()
	waitForStopper(t, stopper)
	for i, c := range consumers {
		verifyEventSlice(fmt.Sprintf("feed direct consumer %d", i), c.received)
	}
}

// nodeEventReader reads the node-related events off of a feed subscription,
// ignoring other events.
type nodeEventReader struct {
	perNodeFeeds map[proto.NodeID][]string
}

// recordEvent records an events received from the node itself. Each event is
// recorded as a simple string value; this is less exhaustive than a full struct
// comparison, but should be easier to correct if future changes slightly modify
// these values. Events which do not pertain to a Node are ignored.
func (ner *nodeEventReader) recordEvent(event interface{}) {
	var nid proto.NodeID
	eventStr := ""
	switch event := event.(type) {
	case *status.CallSuccessEvent:
		if event.Method == proto.InternalResolveIntent {
			// Ignore this best-effort method.
			break
		}
		nid = event.NodeID
		eventStr = event.Method.String()
	case *status.CallErrorEvent:
		nid = event.NodeID
		eventStr = "failed " + event.Method.String()
	}
	if nid > 0 {
		ner.perNodeFeeds[nid] = append(ner.perNodeFeeds[nid], eventStr)
	}
}

func (ner *nodeEventReader) readEvents(sub *util.Subscription) {
	ner.perNodeFeeds = make(map[proto.NodeID][]string)
	for e := range sub.Events() {
		ner.recordEvent(e)
	}
}

// eventFeedString describes the event information that was recorded by
// nodeEventReader. The formatting is appropriate to paste directly into test as
// a new expected value.
func (ner *nodeEventReader) eventFeedString() string {
	var response string
	for id, feed := range ner.perNodeFeeds {
		response += fmt.Sprintf("proto.NodeID(%d): []string{\n", int64(id))
		for _, evt := range feed {
			response += fmt.Sprintf("\t\t\"%s\",\n", evt)
		}
		response += fmt.Sprintf("},\n")
	}
	return response
}

// TestServerNodeEventFeed verifies that a test server emits Node-specific
// events.
func TestServerNodeEventFeed(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()

	feed := s.EventFeed()

	// Start reading events from the feed before starting the stores.
	readStopper := util.NewStopper()
	ner := &nodeEventReader{}
	sub := feed.Subscribe()
	readStopper.RunWorker(func() {
		ner.readEvents(sub)
	})

	sender, err := client.NewHTTPSender(s.ServingAddr(), testutils.NewTestBaseContext())
	if err != nil {
		t.Fatal(err)
	}
	kv := client.NewKV(nil, sender)
	kv.User = storage.UserRoot

	// Add some data in a transaction
	err = kv.RunTransaction(nil, func(txn *client.Txn) error {
		return txn.Run(
			client.Put(proto.Key("a"), []byte("asdf")),
			client.Put(proto.Key("c"), []byte("jkl;")),
		)
	})
	if err != nil {
		t.Fatalf("error putting data to db: %s", err.Error())
	}

	// Get some data, discarding the result.
	err = kv.Run(
		client.Get(proto.Key("a")),
	)
	if err != nil {
		t.Fatalf("error putting data to db: %s", err.Error())
	}

	// Scan, which should fail.
	err = kv.Run(
		client.Scan(proto.Key("b"), proto.Key("a"), 0),
	)
	if err == nil {
		t.Fatal("expected scan to fail.")
	}

	// Close feed and wait for reader to receive all events.
	feed.Close()
	readStopper.Stop()

	expectedNodeEvents := map[proto.NodeID][]string{
		proto.NodeID(1): {
			"InternalRangeLookup",
			"InternalRangeLookup",
			"InternalRangeLookup",
			"InternalRangeLookup",
			"Put",
			"Put",
			"Put",
			"EndTransaction",
			"Get",
			"failed Scan",
		},
	}
	if a, e := ner.perNodeFeeds, expectedNodeEvents; !reflect.DeepEqual(a, e) {
		t.Errorf("node feed did not match expected value. Actual values have been printed to compare with above expectation.\n")
		t.Logf("Event feed information:\n%s", ner.eventFeedString())
	}
}
