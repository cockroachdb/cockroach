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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestLeaderCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	lc := newLeaderCache(3)
	if r := lc.Lookup(12); r.StoreID != 0 {
		t.Fatalf("lookup of missing key returned replica: %v", r)
	}
	replica := roachpb.ReplicaDescriptor{StoreID: 1}
	lc.Update(5, replica)
	if r := lc.Lookup(5); r.StoreID != 1 {
		t.Errorf("expected %v, got %v", replica, r)
	}
	newReplica := roachpb.ReplicaDescriptor{StoreID: 7}
	lc.Update(5, newReplica)
	r := lc.Lookup(5)
	if r.StoreID != 7 {
		t.Errorf("expected %v, got %v", newReplica, r)
	}
	lc.Update(5, roachpb.ReplicaDescriptor{})
	r = lc.Lookup(5)
	if r.StoreID != 0 {
		t.Fatalf("evicted leader returned: %v", r)
	}

	for i := 10; i < 20; i++ {
		lc.Update(roachpb.RangeID(i), replica)
	}
	if lc.Lookup(16).StoreID != 0 || lc.Lookup(17).StoreID == 0 {
		t.Errorf("unexpected policy used in cache")
	}
}
