// Copyright 2014 The Cockroach Authors.
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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package storage

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestCapacityGossipUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)
	sf := newStoreFinder(nil)
	key := "testkey"

	// Order and value of contentsChanged shouldn't matter.
	sf.capacityGossipUpdate(key, true)
	sf.capacityGossipUpdate(key, false)

	expectedKeys := stringSet{key: struct{}{}}
	sf.finderMu.Lock()
	actualKeys := sf.capacityKeys
	sf.finderMu.Unlock()

	if !reflect.DeepEqual(expectedKeys, actualKeys) {
		t.Errorf("expected to fetch %+v, instead %+v", expectedKeys, actualKeys)
	}
}

func TestStoreFinder(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _ := createTestStore(t)
	defer s.Stop()
	required := []string{"ssd", "dc"}
	// Nothing yet.
	if stores, _ := s.findStores(proto.Attributes{Attrs: required}); stores != nil {
		t.Errorf("expected no stores, instead %+v", stores)
	}

	matchingStore := StoreDescriptor{
		Attrs: proto.Attributes{Attrs: required},
	}
	supersetStore := StoreDescriptor{
		Attrs: proto.Attributes{Attrs: append(required, "db")},
	}
	unmatchingStore := StoreDescriptor{
		Attrs: proto.Attributes{Attrs: []string{"ssd", "otherdc"}},
	}
	emptyStore := StoreDescriptor{Attrs: proto.Attributes{}}

	// Explicitly add keys rather than registering a gossip callback to avoid
	// waiting for the goroutine callback to finish.
	s.capacityKeys = stringSet{
		"k1": struct{}{},
		"k2": struct{}{},
		"k3": struct{}{},
		"k4": struct{}{},
	}
	s.gossip.AddInfo("k1", matchingStore, time.Hour)
	s.gossip.AddInfo("k2", supersetStore, time.Hour)
	s.gossip.AddInfo("k3", unmatchingStore, time.Hour)
	s.gossip.AddInfo("k4", emptyStore, time.Hour)

	expected := []string{matchingStore.Attrs.SortedString(), supersetStore.Attrs.SortedString()}
	stores, err := s.findStores(proto.Attributes{Attrs: required})
	if err != nil {
		t.Errorf("expected no err, got %s", err)
	}
	var actual []string
	for _, store := range stores {
		actual = append(actual, store.Attrs.SortedString())
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected %+v Attrs, instead %+v", expected, actual)
	}
}

// TestStoreFinderGarbageCollection ensures removal of capacity gossip keys in
// the map, if their gossip does not exist when we try to retrieve them.
func TestStoreFinderGarbageCollection(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, _ := createTestStore(t)
	defer s.Stop()

	s.capacityKeys = stringSet{
		"key0": struct{}{},
		"key1": struct{}{},
	}
	required := []string{}

	// No gossip added for either key, so they should be removed.
	stores, err := s.findStores(proto.Attributes{Attrs: required})
	if err != nil {
		t.Errorf("unexpected error retrieving stores %s", err)
	} else if len(stores) != 0 {
		t.Errorf("expected no stores found, instead %+v", stores)
	}

	if len(s.capacityKeys) != 0 {
		t.Errorf("expected keys to be cleared, instead are %+v",
			s.capacityKeys)
	}
}
