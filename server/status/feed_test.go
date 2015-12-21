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
// Author: Matt Tracy (matt@cockroachlabs.com)

package status_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

const d = time.Second

func wrap(args roachpb.Request) roachpb.BatchRequest {
	var ba roachpb.BatchRequest
	ba.Add(args)
	return ba
}

func TestNodeEventFeed(t *testing.T) {
	defer leaktest.AfterTest(t)

	nodeDesc := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(99),
	}

	// A testCase corresponds to a single Store event type. Each case contains a
	// method which publishes a single event to the given storeEventPublisher,
	// and an expected result interface which should match the produced
	// event.
	testCases := []struct {
		publishTo func(status.NodeEventFeed)
		expected  interface{}
	}{
		{
			publishTo: func(nef status.NodeEventFeed) {
				nef.StartNode(nodeDesc, 100)
			},
			expected: &status.StartNodeEvent{
				Desc:      nodeDesc,
				StartedAt: 100,
			},
		},
		{
			publishTo: func(nef status.NodeEventFeed) {
				nef.CallComplete(wrap(roachpb.NewGet(roachpb.Key("abc"))), 0, nil)
			},
			expected: &status.CallSuccessEvent{
				NodeID: roachpb.NodeID(1),
				Method: roachpb.Get,
			},
		},
		{
			publishTo: func(nef status.NodeEventFeed) {
				nef.CallComplete(wrap(roachpb.NewPut(roachpb.Key("abc"), roachpb.MakeValueFromString("def"))), 0, nil)
			},
			expected: &status.CallSuccessEvent{
				NodeID: roachpb.NodeID(1),
				Method: roachpb.Put,
			},
		},
		{
			publishTo: func(nef status.NodeEventFeed) {
				nef.CallComplete(wrap(roachpb.NewGet(roachpb.Key("abc"))), 0, roachpb.NewError(util.Errorf("error")))
			},
			expected: &status.CallErrorEvent{
				NodeID: roachpb.NodeID(1),
				Method: roachpb.Batch,
			},
		},
		{
			publishTo: func(nef status.NodeEventFeed) {
				nef.CallComplete(wrap(roachpb.NewGet(roachpb.Key("abc"))), time.Minute, &roachpb.Error{
					Detail: &roachpb.ErrorDetail{
						WriteIntent: &roachpb.WriteIntentError{
							Index: &roachpb.ErrPosition{Index: 0},
						},
					},
					Message: "boo",
				})
			},
			expected: &status.CallErrorEvent{
				NodeID:   roachpb.NodeID(1),
				Method:   roachpb.Get,
				Duration: time.Minute,
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

	nodefeed := status.NewNodeEventFeed(roachpb.NodeID(1), feed)
	for _, tc := range testCases {
		tc.publishTo(nodefeed)
	}

	feed.Flush()

	if a, e := events, expectedEvents; !reflect.DeepEqual(a, e) {
		t.Errorf("received incorrect events.\nexpected: %v\nactual: %v", e, a)
	}
}

// nodeEventReader reads the node-related events off of a feed subscription,
// ignoring other events.
type nodeEventReader struct {
	perNodeFeeds map[roachpb.NodeID][]string
}

// recordEvent records an events received from the node itself. Each event is
// recorded as a simple string value; this is less exhaustive than a full struct
// comparison, but should be easier to correct if future changes slightly modify
// these values. Events which do not pertain to a Node are ignored.
func (ner *nodeEventReader) recordEvent(event interface{}) {
	var nid roachpb.NodeID
	eventStr := ""
	switch event := event.(type) {
	case *status.CallSuccessEvent:
		if event.Method == roachpb.ResolveIntent {
			// Ignore this best-effort method.
			break
		}
		if event.Method == roachpb.RangeLookup {
			// Due to a race with the server's status recording system, we
			// can't reliably depend on RangeLookup to occur during the
			// test. Ignore this method.
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

func (ner *nodeEventReader) readEvents(feed *util.Feed) {
	ner.perNodeFeeds = make(map[roachpb.NodeID][]string)
	feed.Subscribe(ner.recordEvent)
}

// eventFeedString describes the event information that was recorded by
// nodeEventReader. The formatting is appropriate to paste directly into test as
// a new expected value.
func (ner *nodeEventReader) eventFeedString() string {
	var response string
	for id, feed := range ner.perNodeFeeds {
		response += fmt.Sprintf("%T(%s): []string{\n", id, id)
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
	defer leaktest.AfterTest(t)
	t.Skip("TODO(tschottdorf): needs update for batches; see comment on CallComplete")
	s := server.StartTestServer(t)

	feed := s.EventFeed()

	// Start reading events from the feed before starting the stores.
	ner := nodeEventReader{}
	ner.readEvents(feed)

	db, err := client.Open(s.Stopper(), fmt.Sprintf("rpcs://%s@%s?certs=%s",
		security.NodeUser,
		s.ServingAddr(),
		security.EmbeddedCertsDir))
	if err != nil {
		t.Fatal(err)
	}

	// Add some data in a transaction
	err = db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		b.Put("a", "asdf")
		b.Put("c", "jkl;")
		return txn.CommitInBatch(b)
	})
	if err != nil {
		t.Fatalf("error putting data to db: %s", err)
	}

	// Get some data, discarding the result.
	if _, err := db.Get("a"); err != nil {
		t.Fatalf("error getting data from db: %s", err)
	}

	// Scan, which should fail.
	if _, err = db.Scan("b", "a", 0); err == nil {
		t.Fatal("expected scan to fail")
	}

	// Close feed and wait for reader to receive all events.
	feed.Flush()
	s.Stop()

	expectedNodeEvents := map[roachpb.NodeID][]string{
		roachpb.NodeID(1): {
			"Put",
			"Put",
			"EndTransaction",
			"Get",
			"failed Scan",
		},
	}

	// TODO(mtracy): This assertion has been made "fuzzy" in order to account
	// for the unpredictably ordered events from an asynchronous background
	// task.  A future commit should disable that background task (status
	// recording) during this test, and exact matching should be restored.
	/*
	   if a, e := ner.perNodeFeeds, expectedNodeEvents; !reflect.DeepEqual(a, e) {
	       t.Errorf("node feed did not match expected value. Actual values have been printed to compare with above expectation.\n")
	       log.Infof("Event feed information:\n%s", ner.eventFeedString())
	   }
	*/

	// The actual results should contain the expected results as an ordered
	// subset.
	passed := true
	for k := range expectedNodeEvents {
		// Maintain an index into the actual and expected feed slices.
		actual, expected := ner.perNodeFeeds[k], expectedNodeEvents[k]
		i, j := 0, 0
		// Advance indexes until one or both slices are exhausted.
		for i < len(expected) && j < len(actual) {
			// If the current expected value matches the current actual value,
			// advance both indexes. Otherwise, advance only the actual index.
			if reflect.DeepEqual(expected[i], actual[j]) {
				i++
			}
			j++
		}
		// Test succeeded if it advanced over every expected event.
		if i != len(expected) {
			passed = false
			break
		}
	}

	if !passed {
		t.Fatalf("received unexpected events: %s", ner.eventFeedString())
	}
}

// TestNodeEventFeedTransactionRestart verifies that calls which indicate a
// transaction restart are counted as successful.
func TestNodeEventFeedTransactionRestart(t *testing.T) {
	defer leaktest.AfterTest(t)

	stopper := stop.NewStopper()
	feed := util.NewFeed(stopper)
	nodeID := roachpb.NodeID(1)
	nodefeed := status.NewNodeEventFeed(nodeID, feed)
	ner := nodeEventReader{}
	ner.readEvents(feed)

	get := wrap(&roachpb.GetRequest{})
	nodefeed.CallComplete(get, d, &roachpb.Error{
		TransactionRestart: roachpb.TransactionRestart_BACKOFF})
	nodefeed.CallComplete(get, d, &roachpb.Error{
		TransactionRestart: roachpb.TransactionRestart_IMMEDIATE})
	nodefeed.CallComplete(wrap(&roachpb.PutRequest{}), d, &roachpb.Error{
		TransactionRestart: roachpb.TransactionRestart_ABORT})
	nodefeed.CallComplete(wrap(&roachpb.PutRequest{}), d, &roachpb.Error{
		Detail: &roachpb.ErrorDetail{
			WriteIntent: &roachpb.WriteIntentError{
				Index: &roachpb.ErrPosition{Index: 0},
			},
		},
		TransactionRestart: roachpb.TransactionRestart_ABORT,
	})

	feed.Flush()
	stopper.Stop()

	exp := []string{
		"Get",
		"Get",
		"failed Batch",
		"failed Put",
	}

	if !reflect.DeepEqual(exp, ner.perNodeFeeds[nodeID]) {
		t.Fatalf("received unexpected events: %s", ner.eventFeedString())
	}
}
