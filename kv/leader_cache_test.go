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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
)

func TestLeaderCache(t *testing.T) {
	lc := kv.NewLeaderCache(3)
	if r := lc.Lookup(12); r != nil {
		t.Fatalf("lookup of missing key returned replica: %v", r)
	}
	replica := &proto.Replica{StoreID: 1}
	lc.EvictOrUpdate(5, replica)
	if r := lc.Lookup(5); !reflect.DeepEqual(replica, r) {
		t.Errorf("expected %v, got %v", replica, r)
	}
	newReplica := &proto.Replica{StoreID: 7}
	lc.EvictOrUpdate(5, newReplica)
	r := lc.Lookup(5)
	if !reflect.DeepEqual(newReplica, r) {
		t.Errorf("expected %v, got %v", newReplica, r)
	}
	rCopy := *r
	// Updating with the same replica (or a copy thereof) should
	// throw the entry out.
	lc.EvictOrUpdate(5, &rCopy)
	if r := lc.Lookup(5); r != nil {
		t.Fatalf("unexpected leader cache hit: %v", r)
	}
	for i := 10; i < 20; i++ {
		lc.EvictOrUpdate(proto.RaftID(i), &rCopy)
	}
	if lc.Lookup(16) != nil || lc.Lookup(17) == nil {
		t.Errorf("unexpected policy used in cache")
	}
}
