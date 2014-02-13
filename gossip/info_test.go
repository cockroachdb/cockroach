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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package gossip

import (
	"math"
	"sort"
	"testing"
)

func TestPrefix(t *testing.T) {
	prefixes := []struct{ Key, Prefix string }{
		{"a", ""},
		{"a.b", "a"},
		{"a.b.c", "a.b"},
		{"a.b.ccc", "a.b"},
		{"a.b..ccc", "a.b."},
	}

	for _, pi := range prefixes {
		if p := InfoPrefix(pi.Key); p != pi.Prefix {
			t.Errorf("InfoPrefix(%s) = %s != %s", pi.Key, p, pi.Prefix)
		}
	}
}

func TestSort(t *testing.T) {
	infos := InfoArray{
		{"a", Float64Value(3.0), 0, 0, 0, "", 0},
		{"b", Float64Value(1.0), 0, 0, 0, "", 0},
		{"c", Float64Value(2.1), 0, 0, 0, "", 0},
		{"d", Float64Value(2.0), 0, 0, 0, "", 0},
		{"e", Float64Value(-1.0), 0, 0, 0, "", 0},
	}

	// Verify forward sort.
	sort.Sort(infos)
	last := Float64Value(-math.MaxFloat64)
	for _, info := range infos {
		if info.Val.Less(last) {
			t.Errorf("info val %f not increasing", info.Val)
		}
		last = info.Val.(Float64Value)
	}

	// Verify reverse sort.
	sort.Sort(sort.Reverse(infos))
	last = Float64Value(math.MaxFloat64)
	for _, info := range infos {
		if !info.Val.Less(last) {
			t.Errorf("info val %f not decreasing", info.Val)
		}
		last = info.Val.(Float64Value)
	}
}
