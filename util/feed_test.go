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

	"github.com/cockroachdb/cockroach/util/stop"
)

// TestFeed exercises the most basic behavior of a Feed.
func TestFeed(t *testing.T) {
	numSubs := 10
	events := []interface{}{1, 1, 2, 3, 5, 8, 13, 21, 34}
	testSubs := make([][]interface{}, numSubs)
	stopper := stop.NewStopper()

	feed := NewFeed(stopper)

	// Add a number of subscribers.
	for i := 0; i < numSubs; i++ {
		idx := i
		feed.Subscribe(func(event interface{}) {
			testSubs[idx] = append(testSubs[idx], event)
		})
	}
	// Publish every event to the Feed in order.
	for _, e := range events {
		feed.Publish(e)
	}

	feed.Flush()
	stopper.Stop()

	// Verify that all results are received by all Subscribers in order.
	for i, ts := range testSubs {
		if a, e := ts, events; !reflect.DeepEqual(a, e) {
			t.Errorf("subscriber %d received incorrect events: %v", i, a)
		}
	}
}

// TestMultipleFeed verifies that feeds are separate from each other. This is
// important to verify because of the shared pool of feedEntry objects used in
// their implementation.
func TestMultipleFeed(t *testing.T) {
	numSubs := 10
	intEvents := []interface{}{1, 1, 2, 3, 5, 8, 13, 21, 34}
	stringEvents := []interface{}{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}
	intSubs := make([][]interface{}, numSubs)
	stringSubs := make([][]interface{}, numSubs)
	stopper := stop.NewStopper()

	intFeed := NewFeed(stopper)
	stringFeed := NewFeed(stopper)

	for i := 0; i < numSubs; i++ {
		// int subscriber
		idx := i
		intFeed.Subscribe(func(event interface{}) {
			intSubs[idx] = append(intSubs[idx], event)
		})

		// string subscriber
		stringFeed.Subscribe(func(event interface{}) {
			stringSubs[idx] = append(stringSubs[idx], event)
		})
	}

	for i := 0; i < len(intEvents); i++ {
		intFeed.Publish(intEvents[i])
		stringFeed.Publish(stringEvents[i])
	}

	intFeed.Flush()
	stringFeed.Flush()
	stopper.Stop()

	for i, ts := range intSubs {
		if a, e := ts, intEvents; !reflect.DeepEqual(a, e) {
			t.Errorf("int subscriber %d received incorrect events %v, expected %v", i, a, e)
		}
	}
	for i, ts := range stringSubs {
		if a, e := ts, stringEvents; !reflect.DeepEqual(a, e) {
			t.Errorf("int subscriber %d received incorrect events %v, expected %v", i, a, e)
		}
	}
}
