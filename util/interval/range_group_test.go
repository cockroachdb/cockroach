// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package interval

import (
	"bytes"
	"crypto/rand"
	"testing"

	_ "github.com/cockroachdb/cockroach/util/log" // for flags
)

func TestRangeListSingleRangeAndClear(t *testing.T) {
	testRangeGroupSingleRangeAndClear(t, NewRangeList())
}

func TestRangeTreeSingleRangeAndClear(t *testing.T) {
	testRangeGroupSingleRangeAndClear(t, NewRangeTree())
}

func testRangeGroupSingleRangeAndClear(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added 1 range to range group, wanted len = 1; got len = %d", l)
	}

	rg.Clear()
	if l := rg.Len(); l != 0 {
		t.Errorf("cleared range group, wanted len = 0; got len = %d", l)
	}
}

func TestRangeListTwoRangesBefore(t *testing.T) {
	testRangeGroupTwoRangesBefore(t, NewRangeList())
}

func TestRangeTreeTwoRangesBefore(t *testing.T) {
	testRangeGroupTwoRangesBefore(t, NewRangeTree())
}

func testRangeGroupTwoRangesBefore(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 2 {
		t.Errorf("added 2 disjoint ranges to range group, wanted len = 2; got len = %d", l)
	}
}

func TestRangeListTwoRangesAfter(t *testing.T) {
	testRangeGroupTwoRangesAfter(t, NewRangeList())
}

func TestRangeTreeTwoRangesAfter(t *testing.T) {
	testRangeGroupTwoRangesAfter(t, NewRangeTree())
}

func testRangeGroupTwoRangesAfter(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 2 {
		t.Errorf("added 2 disjoint ranges to range group, wanted len = 2; got len = %d", l)
	}
}

func TestRangeListTwoRangesMergeForward(t *testing.T) {
	testRangeGroupTwoRangesMergeForward(t, NewRangeList())
}

func TestRangeTreeTwoRangesMergeForward(t *testing.T) {
	testRangeGroupTwoRangesMergeForward(t, NewRangeTree())
}

func testRangeGroupTwoRangesMergeForward(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListTwoRangesMergeBackwards(t *testing.T) {
	testRangeGroupTwoRangesMergeBackwards(t, NewRangeList())
}

func TestRangeTreeTwoRangesMergeBackwards(t *testing.T) {
	testRangeGroupTwoRangesMergeBackwards(t, NewRangeTree())
}

func testRangeGroupTwoRangesMergeBackwards(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListTwoRangesWithin(t *testing.T) {
	testRangeGroupTwoRangesWithin(t, NewRangeList())
}

func TestRangeTreeTwoRangesWithin(t *testing.T) {
	testRangeGroupTwoRangesWithin(t, NewRangeTree())
}

func testRangeGroupTwoRangesWithin(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x02}, End: []byte{0x03}}); added {
		t.Errorf("added fully contained range to range group, wanted false added flag; got true")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListTwoRangesSurround(t *testing.T) {
	testRangeGroupTwoRangesSurround(t, NewRangeList())
}

func TestRangeTreeTwoRangesSurround(t *testing.T) {
	testRangeGroupTwoRangesSurround(t, NewRangeTree())
}

func testRangeGroupTwoRangesSurround(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x02}, End: []byte{0x03}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListThreeRangesMergeInOrder(t *testing.T) {
	testRangeGroupThreeRangesMergeInOrder(t, NewRangeList())
}

func TestRangeTreeThreeRangesMergeInOrder(t *testing.T) {
	testRangeGroupThreeRangesMergeInOrder(t, NewRangeTree())
}

func testRangeGroupThreeRangesMergeInOrder(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x04}, End: []byte{0x06}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added three overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListThreeRangesMergeOutOfOrder(t *testing.T) {
	testRangeGroupThreeRangesMergeOutOfOrder(t, NewRangeList())
}

func TestRangeTreeThreeRangesMergeOutOfOrder(t *testing.T) {
	testRangeGroupThreeRangesMergeOutOfOrder(t, NewRangeTree())
}

func testRangeGroupThreeRangesMergeOutOfOrder(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x04}, End: []byte{0x06}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added three overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListThreeRangesMergeReverseOrder(t *testing.T) {
	testRangeGroupThreeRangesMergeReverseOrder(t, NewRangeList())
}

func TestRangeTreeThreeRangesMergeReverseOrder(t *testing.T) {
	testRangeGroupThreeRangesMergeReverseOrder(t, NewRangeTree())
}

func testRangeGroupThreeRangesMergeReverseOrder(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x04}, End: []byte{0x06}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added three overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListThreeRangesSuperset(t *testing.T) {
	testRangeGroupThreeRangesSuperset(t, NewRangeList())
}

func TestRangeTreeThreeRangesSuperset(t *testing.T) {
	testRangeGroupThreeRangesSuperset(t, NewRangeTree())
}

func testRangeGroupThreeRangesSuperset(t *testing.T, rg RangeGroup) {
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x04}, End: []byte{0x06}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}
	if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x06}}); !added {
		t.Errorf("added new range to range group, wanted true added flag; got false")
	}

	if l := rg.Len(); l != 1 {
		t.Errorf("added three overlapping ranges to range group, wanted len = 1; got len = %d", l)
	}
}

func TestRangeListAndRangeGroupIdentical(t *testing.T) {
	const trials = 5
	for tr := 0; tr < trials; tr++ {
		rl := NewRangeList()
		rt := NewRangeTree()

		const iters = 20
		for i := 0; i < iters; i++ {
			r := getRandomRange(t, 3)
			listAdded := rl.Add(r)
			treeAdded := rt.Add(r)
			if listAdded != treeAdded {
				t.Errorf("expected adding %s to RangeList and RangeTree to produce the same result; got %t from RangeList and %t from RangeTree", r, listAdded, treeAdded)
			}

			listLen := rl.Len()
			treeLen := rt.Len()
			if listLen != treeLen {
				t.Errorf("expected RangeList and RangeTree to have the same length; got %d from RangeList and %d from RangeTree", listLen, treeLen)
			}
		}
	}
}

func getRandomRange(t *testing.T, n int) Range {
	s1 := getRandomByteSlice(t, n)
	s2 := getRandomByteSlice(t, n)
	cmp := bytes.Compare(s1, s2)
	for cmp == 0 {
		s2 = getRandomByteSlice(t, n)
		cmp = bytes.Compare(s1, s2)
	}
	if cmp < 0 {
		return Range{Start: s1, End: s2}
	}
	return Range{Start: s2, End: s1}
}

func getRandomByteSlice(t *testing.T, n int) []byte {
	s := make([]byte, n)
	_, err := rand.Read(s)
	if err != nil {
		t.Fatalf("could not create random byte slice: %v", err)
	}
	return s
}
