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

package util

import (
	"reflect"
	"testing"
	"time"
)

type testSubscriber struct {
	sub      *Subscription
	received []interface{}
	done     chan struct{}
}

func (ts *testSubscriber) readAll() {
	for e := range ts.sub.Events {
		ts.received = append(ts.received, e)
	}
	close(ts.done)
}

func (ts *testSubscriber) readN(n int) {
	for e := range ts.sub.Events {
		ts.received = append(ts.received, e)
		if len(ts.received) == n {
			break
		}
	}
	close(ts.done)
}

func newSubscriber(feed *Feed) *testSubscriber {
	return &testSubscriber{
		sub:  feed.Subscribe(),
		done: make(chan struct{}),
	}
}

// TestFeed exercises the most basic behavior of a Feed, with a constant set of
// Subscribers.
func TestFeed(t *testing.T) {
	var (
		numSubs  = 10
		events   = []interface{}{1, 1, 2, 3, 5, 8, 13, 21, 34}
		testSubs = make([]*testSubscriber, 0, numSubs)
		stopper  = NewStopper()
	)

	feed := StartFeed(stopper)

	// Add a number of subscribers.
	for i := 0; i < numSubs; i++ {
		ts := newSubscriber(feed)
		testSubs = append(testSubs, ts)
		stopper.RunWorker(ts.readAll)
	}
	// Publish every event to the Feed in order.
	for _, e := range events {
		feed.Publish(e)
	}
	stopper.Stop()

	// Wait for stopper to finish, meaning all publishers have ceased.
	select {
	case <-stopper.IsStopped():
	case <-time.After(5 * time.Second):
		t.Fatalf("stopper failed to complete after 5 seconds.")
	}

	// Verify that all results are received by all Subscribers in order.
	for i, ts := range testSubs {
		if a, e := ts.received, events; !reflect.DeepEqual(a, e) {
			t.Errorf("subscriber %d received incorrect events: %v", i, a)
		}
	}
}

// TestFeedUnsubscription tests that Subscriptions are able to Unsubscribe.
func TestFeedUnsubscription(t *testing.T) {
	var (
		events   = []interface{}{1, 1, 2, 3, 5, 8, 13, 21, 34}
		testSubs = make([]*testSubscriber, len(events))
		stopper  = NewStopper()
	)

	feed := StartFeed(stopper)

	// The strategy is to Publish every event twice. In the first pass, a
	// subscriber is added before each event is published. In the second pass, a
	// Subscriber is unsubscribed before each publish. The result should be that
	// every Subscriber gets every value, but their individual 'received' arrays
	// will be a rotation of the original events array.
	for i, e := range events {
		ts := newSubscriber(feed)
		testSubs[i] = ts
		stopper.RunWorker(func() {
			ts.readN(len(events))
		})
		feed.Publish(e)
	}
	for i, e := range events {
		select {
		case <-testSubs[i].done:
		case <-time.After(5 * time.Second):
			t.Logf("subscriber %d failed to complete in 5 seconds", i)
		}
		testSubs[i].sub.Unsubscribe()
		feed.Publish(e)
	}
	stopper.Stop()

	// Wait for stopper to finish, meaning all publishers have ceased.
	select {
	case <-stopper.IsStopped():
	case <-time.After(5 * time.Second):
		t.Fatalf("stopper failed to complete after 5 seconds.")
	}

	// Verify that all results are received by all Subscribers. Each subscriber
	// will have received the results in a unique but predictable order.
	for i, ts := range testSubs {
		expected := make([]interface{}, 0, len(ts.received))
		expected = append(expected, events[i:]...)
		expected = append(expected, events[:i]...)
		if a, e := ts.received, expected; !reflect.DeepEqual(a, e) {
			t.Errorf("subscriber %d received incorrect events %v, expected %v", i, a, e)
		}
	}
}

// TestMultipleFeed verifies that feeds are separate from each other. This is
// important to verify because of the shared pool of feedEntry objects used in
// their implementation.
func TestMultipleFeed(t *testing.T) {
	var (
		numSubs      = 10
		intEvents    = []interface{}{1, 1, 2, 3, 5, 8, 13, 21, 34}
		stringEvents = []interface{}{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}
		intSubs      = make([]*testSubscriber, 0, numSubs)
		stringSubs   = make([]*testSubscriber, 0, numSubs)
		stopper      = NewStopper()
	)

	intFeed := StartFeed(stopper)
	stringFeed := StartFeed(stopper)

	for i := 0; i < numSubs; i++ {
		// int subscriber
		ts := newSubscriber(intFeed)
		intSubs = append(intSubs, ts)
		stopper.RunWorker(ts.readAll)

		// string subscriber
		ts = newSubscriber(stringFeed)
		stringSubs = append(stringSubs, ts)
		stopper.RunWorker(ts.readAll)
	}

	for i := 0; i < len(intEvents); i++ {
		intFeed.Publish(intEvents[i])
		stringFeed.Publish(stringEvents[i])
	}
	stopper.Stop()

	// Wait for stopper to finish, meaning all publishers have ceased.
	select {
	case <-stopper.IsStopped():
	case <-time.After(5 * time.Second):
		t.Fatalf("stopper failed to complete after 5 seconds.")
	}

	for i, ts := range intSubs {
		if a, e := ts.received, intEvents; !reflect.DeepEqual(a, e) {
			t.Errorf("int subscriber %d received incorrect events %v, expected %v", i, a, e)
		}
	}
	for i, ts := range stringSubs {
		if a, e := ts.received, stringEvents; !reflect.DeepEqual(a, e) {
			t.Errorf("int subscriber %d received incorrect events %v, expected %v", i, a, e)
		}
	}
}
