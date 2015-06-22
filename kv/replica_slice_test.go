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

package kv

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func verifyOrdering(attrs []string, replicas replicaSlice, prefixLen int) bool {
	prevMatchIndex := len(attrs)
	for i, replica := range replicas {
		matchIndex := -1

		for j, attr := range attrs {
			if j >= len(replica.attrs()) || replica.attrs()[j] != attr {
				break
			}
			matchIndex = j
		}
		if matchIndex != -1 && matchIndex > prevMatchIndex {
			return false
		}
		if i == 0 && matchIndex+1 != prefixLen {
			return false
		}
		prevMatchIndex = matchIndex
	}
	return true
}

func TestReplicaSetSortByCommonAttributePrefix(t *testing.T) {
	defer leaktest.AfterTest(t)
	replicaAttrs := [][]string{
		{"us-west-1a", "gpu"},
		{"us-east-1a", "pdu1", "gpu"},
		{"us-east-1a", "pdu1", "fio"},
		{"breaker", "us-east-1a", "pdu1", "fio"},
		{""},
		{"us-west-1a", "pdu1", "fio"},
		{"us-west-1a", "pdu1", "fio", "aux"},
	}
	attrs := [][]string{
		{"us-carl"},
		{"us-west-1a", "pdu1", "fio"},
		{"us-west-1a"},
		{"", "pdu1", "fio"},
	}

	for i, attr := range attrs {
		rs := replicaSlice{}
		for _, c := range replicaAttrs {
			rs = append(rs, replicaInfo{
				NodeDesc: proto.NodeDescriptor{
					Attrs: proto.Attributes{Attrs: c},
				},
			})
		}
		prefixLen := rs.SortByCommonAttributePrefix(attr)
		if !verifyOrdering(attr, rs, prefixLen) {
			t.Errorf("%d: attributes not ordered by %s or prefix length %d incorrect:\n%v", i, attr, prefixLen, rs)
		}
	}
}

func getStores(rs replicaSlice) (r []proto.StoreID) {
	for i := range rs {
		r = append(r, rs[i].StoreID)
	}
	return
}

func TestReplicaSetMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)
	rs := replicaSlice(nil)
	for i := 0; i < 5; i++ {
		rs = append(rs, replicaInfo{Replica: proto.Replica{StoreID: proto.StoreID(i + 1)}})
	}
	rs.MoveToFront(0)
	exp := []proto.StoreID{1, 2, 3, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(2)
	exp = []proto.StoreID{3, 1, 2, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(4)
	exp = []proto.StoreID{5, 3, 1, 2, 4}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
}
