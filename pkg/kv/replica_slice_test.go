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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func verifyOrdering(attrs []string, replicas ReplicaSlice, prefixLen int) bool {
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
	defer leaktest.AfterTest(t)()
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
		rs := ReplicaSlice{}
		for _, c := range replicaAttrs {
			rs = append(rs, ReplicaInfo{
				NodeDesc: &roachpb.NodeDescriptor{
					Attrs: roachpb.Attributes{Attrs: c},
				},
			})
		}
		prefixLen := rs.SortByCommonAttributePrefix(attr)
		if !verifyOrdering(attr, rs, prefixLen) {
			t.Errorf("%d: attributes not ordered by %s or prefix length %d incorrect:\n%v", i, attr, prefixLen, rs)
		}
	}
}

func getStores(rs ReplicaSlice) (r []roachpb.StoreID) {
	for i := range rs {
		r = append(r, rs[i].StoreID)
	}
	return
}

func createReplicaSlice() ReplicaSlice {
	rs := ReplicaSlice(nil)
	for i := 0; i < 5; i++ {
		rs = append(rs, ReplicaInfo{ReplicaDescriptor: roachpb.ReplicaDescriptor{StoreID: roachpb.StoreID(i + 1)}})
	}
	return rs
}

func TestReplicaSetMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rs := createReplicaSlice()
	rs.MoveToFront(0)
	exp := []roachpb.StoreID{1, 2, 3, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(2)
	exp = []roachpb.StoreID{3, 1, 2, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(4)
	exp = []roachpb.StoreID{5, 3, 1, 2, 4}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
}

// TestMoveLocalReplicaToFront verifies that OptimizeReplicaOrder correctly
// move the local replica to the front.
func TestMoveLocalReplicaToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCase := []struct {
		slice         ReplicaSlice
		localNodeDesc roachpb.NodeDescriptor
	}{
		{
			// No attribute prefix
			slice: ReplicaSlice{
				ReplicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 2},
				},
				ReplicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 3},
				},
				ReplicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 1},
				},
			},
			localNodeDesc: roachpb.NodeDescriptor{NodeID: 1},
		},
		{
			// Sort replicas by attribute
			slice: ReplicaSlice{
				ReplicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 2, Attrs: roachpb.Attributes{Attrs: []string{"ad"}}},
				},
				ReplicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 3, Attrs: roachpb.Attributes{Attrs: []string{"ab", "c"}}},
				},
				ReplicaInfo{
					ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1},
					NodeDesc:          &roachpb.NodeDescriptor{NodeID: 1, Attrs: roachpb.Attributes{Attrs: []string{"ab"}}},
				},
			},
			localNodeDesc: roachpb.NodeDescriptor{NodeID: 1, Attrs: roachpb.Attributes{Attrs: []string{"ab"}}},
		},
	}
	for _, test := range testCase {
		test.slice.OptimizeReplicaOrder(&test.localNodeDesc)
		if s := test.slice[0]; s.NodeID != test.localNodeDesc.NodeID {
			t.Errorf("unexpected header, wanted nodeid = %d, got %d", test.localNodeDesc.NodeID, s.NodeID)
		}
	}

}
