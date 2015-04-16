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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"bytes"
	"reflect"
	"testing"
)

func TestAttributesIsSubset(t *testing.T) {
	a := Attributes{Attrs: []string{"a", "b", "c"}}
	b := Attributes{Attrs: []string{"a", "b"}}
	c := Attributes{Attrs: []string{"a"}}
	if !b.IsSubset(a) {
		t.Errorf("expected %+v to be a subset of %+v", b, a)
	}
	if !c.IsSubset(a) {
		t.Errorf("expected %+v to be a subset of %+v", c, a)
	}
	if !c.IsSubset(b) {
		t.Errorf("expected %+v to be a subset of %+v", c, b)
	}
	if a.IsSubset(b) {
		t.Errorf("%+v should not be a subset of %+v", a, b)
	}
	if a.IsSubset(c) {
		t.Errorf("%+v should not be a subset of %+v", a, c)
	}
	if b.IsSubset(c) {
		t.Errorf("%+v should not be a subset of %+v", b, c)
	}
}

func TestAttributesSortedString(t *testing.T) {
	a := Attributes{Attrs: []string{"a", "b", "c"}}
	if a.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", a, a.SortedString())
	}
	b := Attributes{Attrs: []string{"c", "a", "b"}}
	if b.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", b, b.SortedString())
	}
	// Duplicates.
	c := Attributes{Attrs: []string{"c", "c", "a", "a", "b", "b"}}
	if c.SortedString() != "a,b,c" {
		t.Errorf("sorted string of %+v (%s) != \"a,b,c\"", c, c.SortedString())
	}
}

func TestRangeDescriptorFindReplica(t *testing.T) {
	desc := RangeDescriptor{
		Replicas: []Replica{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}
	for i, r := range desc.Replicas {
		if i2, r2 := desc.FindReplica(r.StoreID); r2.NodeID != r.NodeID || i2 != i {
			t.Errorf("%d: expected to find node %d for store %d; got %d", i, r.NodeID, r.StoreID, r2.StoreID)
		}
	}
}

func TestRangeDescriptorMissingReplica(t *testing.T) {
	desc := RangeDescriptor{}
	i, r := desc.FindReplica(0)
	if i >= 0 || r != nil {
		t.Fatalf("unexpected return (%s, %s) on missing replica", i, r)
	}
}

// TestRangeDescriptorContains verifies methods to check whether a key
// or key range is contained within the range.
func TestRangeDescriptorContains(t *testing.T) {
	desc := RangeDescriptor{}
	desc.StartKey = []byte("a")
	desc.EndKey = []byte("b")

	testData := []struct {
		start, end []byte
		contains   bool
	}{
		// Single keys.
		{[]byte("a"), []byte("a"), true},
		{[]byte("a"), nil, true},
		{[]byte("aa"), []byte("aa"), true},
		{[]byte("`"), []byte("`"), false},
		{[]byte("b"), []byte("b"), false},
		{[]byte("b"), nil, false},
		{[]byte("c"), []byte("c"), false},
		// Key ranges.
		{[]byte("a"), []byte("b"), true},
		{[]byte("a"), []byte("aa"), true},
		{[]byte("aa"), []byte("b"), true},
		{[]byte("0"), []byte("9"), false},
		{[]byte("`"), []byte("a"), false},
		{[]byte("b"), []byte("bb"), false},
		{[]byte("0"), []byte("bb"), false},
		{[]byte("aa"), []byte("bb"), false},
		{[]byte("b"), []byte("a"), false},
	}
	for _, test := range testData {
		if bytes.Compare(test.start, test.end) == 0 {
			if desc.ContainsKey(test.start) != test.contains {
				t.Errorf("expected key %q within range", test.start)
			}
		} else {
			if desc.ContainsKeyRange(test.start, test.end) != test.contains {
				t.Errorf("expected key range %q-%q within range", test.start, test.end)
			}
		}
	}
}

func TestPermConfig(t *testing.T) {
	p := &PermConfig{
		Read:  []string{"foo", "bar", "baz"},
		Write: []string{"foo", "baz"},
	}
	for _, u := range p.Read {
		if !p.CanRead(u) {
			t.Errorf("expected read permission for %q", u)
		}
	}
	if p.CanRead("bad") {
		t.Errorf("unexpected read access for user \"bad\"")
	}
	for _, u := range p.Write {
		if !p.CanWrite(u) {
			t.Errorf("expected read permission for %q", u)
		}
	}
	if p.CanWrite("bar") {
		t.Errorf("unexpected read access for user \"bar\"")
	}
}

func verifyOrdering(attrs []string, rs ReplicaSlice, prefixLen int) bool {
	prevMatchIndex := len(attrs)
	for i := range rs {
		matchIndex := -1
		for j := range attrs {
			if j >= len(rs[i].Attrs.Attrs) || rs[i].Attrs.Attrs[j] != attrs[j] {
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
	replicaAttrs := [][]string{
		[]string{"us-west-1a", "gpu"},
		[]string{"us-east-1a", "pdu1", "gpu"},
		[]string{"us-east-1a", "pdu1", "fio"},
		[]string{"breaker", "us-east-1a", "pdu1", "fio"},
		[]string{""},
		[]string{"us-west-1a", "pdu1", "fio"},
		[]string{"us-west-1a", "pdu1", "fio", "aux"},
	}
	attrs := [][]string{
		[]string{"us-carl"},
		[]string{"us-west-1a", "pdu1", "fio"},
		[]string{"us-west-1a"},
		[]string{"", "pdu1", "fio"},
	}

	for i, attr := range attrs {
		rs := ReplicaSlice{}
		for _, c := range replicaAttrs {
			rs = append(rs, Replica{Attrs: Attributes{Attrs: c}})
		}
		prefixLen := rs.SortByCommonAttributePrefix(attr)
		if !verifyOrdering(attr, rs, prefixLen) {
			t.Errorf("%d: attributes not ordered by %s or prefix length %d incorrect:\n%v", i, attr, prefixLen, rs)
		}
	}

}

func getStores(rs ReplicaSlice) (r []StoreID) {
	for i := range rs {
		r = append(r, rs[i].StoreID)
	}
	return
}

func TestReplicaSetMoveToFront(t *testing.T) {
	rs := ReplicaSlice(nil)
	for i := 0; i < 5; i++ {
		rs = append(rs, Replica{StoreID: StoreID(i + 1)})
	}
	rs.MoveToFront(0)
	exp := []StoreID{1, 2, 3, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(2)
	exp = []StoreID{3, 1, 2, 4, 5}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
	rs.MoveToFront(4)
	exp = []StoreID{5, 3, 1, 2, 4}
	if stores := getStores(rs); !reflect.DeepEqual(stores, exp) {
		t.Errorf("expected order %s, got %s", exp, stores)
	}
}
