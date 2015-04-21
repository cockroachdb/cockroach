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

package gossip

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
)

// testAddr is a fake net.Addr replacement for unittesting.
type testAddr string

func (t testAddr) Network() string {
	return "test"
}
func (t testAddr) String() string {
	return string(t)
}

var emptyAddr = testAddr("<test-addr>")

func TestPrefix(t *testing.T) {
	prefixes := []struct{ Key, Prefix string }{
		{"a", ""},
		{"a.b", "a"},
		{"a.b.c", "a.b"},
		{"a.b.ccc", "a.b"},
		{"a.b..ccc", "a.b."},
	}

	for _, pi := range prefixes {
		if p := infoPrefix(pi.Key); p != pi.Prefix {
			t.Errorf("infoPrefix(%s) = %s != %s", pi.Key, p, pi.Prefix)
		}
	}
}

func TestSort(t *testing.T) {
	infos := infoSlice{
		{"a", 3.0, 0, 0, 0, 0, 0, 0},
		{"b", 1.0, 0, 0, 0, 0, 0, 0},
		{"c", 2.1, 0, 0, 0, 0, 0, 0},
		{"d", 2.0, 0, 0, 0, 0, 0, 0},
		{"e", -1.0, 0, 0, 0, 0, 0, 0},
	}

	// Verify forward sort.
	sort.Sort(infos)
	last := &info{"last", -math.MaxFloat64, 0, 0, 0, 0, 0, 0}
	for _, i := range infos {
		if i.less(last) {
			t.Errorf("info val %v not increasing", i.Val)
		}
		last.Val = i.Val
	}

	// Verify reverse sort.
	sort.Sort(sort.Reverse(infos))
	last = &info{"last", math.MaxFloat64, 0, 0, 0, 0, 0, 0}
	for _, i := range infos {
		if !i.less(last) {
			t.Errorf("info val %v not decreasing", i.Val)
		}
		last.Val = i.Val
	}
}

func TestExpired(t *testing.T) {
	now := time.Now().UnixNano()
	i := info{"a", float64(1), now, now + int64(time.Millisecond), 0, 0, 0, 0}
	if i.expired(now) {
		t.Error("premature expiration")
	}
	if !i.expired(now + int64(time.Millisecond)) {
		t.Error("info should have expired")
	}
}

func TestIsFresh(t *testing.T) {
	const seq = 10
	now := time.Now().UnixNano()
	node1 := proto.NodeID(1)
	node2 := proto.NodeID(2)
	node3 := proto.NodeID(3)
	i := info{"a", float64(1), now, now + int64(time.Millisecond), 0, node1, node2, seq}
	if !i.isFresh(node3, seq-1) {
		t.Error("info should be fresh:", i)
	}
	if i.isFresh(node3, seq+1) {
		t.Error("info should not be fresh:", i)
	}
	if i.isFresh(node1, seq-1) {
		t.Error("info should not be fresh:", i)
	}
	if i.isFresh(node2, seq-1) {
		t.Error("info should not be fresh:", i)
	}
	// Using node 0 will always yield fresh data.
	if !i.isFresh(0, 0) {
		t.Error("info should be fresh from node0:", i)
	}
}
