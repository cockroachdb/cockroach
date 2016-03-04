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
	"reflect"
	"testing"

	_ "github.com/cockroachdb/cockroach/util/log" // for flags
)

func TestRangeListAddSingleRangeAndClear(t *testing.T) {
	testRangeGroupAddSingleRangeAndClear(t, NewRangeList())
}

func TestRangeTreeAddSingleRangeAndClear(t *testing.T) {
	testRangeGroupAddSingleRangeAndClear(t, NewRangeTree())
}

func testRangeGroupAddSingleRangeAndClear(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddTwoRangesBefore(t *testing.T) {
	testRangeGroupAddTwoRangesBefore(t, NewRangeList())
}

func TestRangeTreeAddTwoRangesBefore(t *testing.T) {
	testRangeGroupAddTwoRangesBefore(t, NewRangeTree())
}

func testRangeGroupAddTwoRangesBefore(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddTwoRangesAfter(t *testing.T) {
	testRangeGroupAddTwoRangesAfter(t, NewRangeList())
}

func TestRangeTreeAddTwoRangesAfter(t *testing.T) {
	testRangeGroupAddTwoRangesAfter(t, NewRangeTree())
}

func testRangeGroupAddTwoRangesAfter(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddTwoRangesMergeForward(t *testing.T) {
	testRangeGroupAddTwoRangesMergeForward(t, NewRangeList())
}

func TestRangeTreeAddTwoRangesMergeForward(t *testing.T) {
	testRangeGroupAddTwoRangesMergeForward(t, NewRangeTree())
}

func testRangeGroupAddTwoRangesMergeForward(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddTwoRangesMergeBackwards(t *testing.T) {
	testRangeGroupAddTwoRangesMergeBackwards(t, NewRangeList())
}

func TestRangeTreeAddTwoRangesMergeBackwards(t *testing.T) {
	testRangeGroupAddTwoRangesMergeBackwards(t, NewRangeTree())
}

func testRangeGroupAddTwoRangesMergeBackwards(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddTwoRangesWithin(t *testing.T) {
	testRangeGroupAddTwoRangesWithin(t, NewRangeList())
}

func TestRangeTreeAddTwoRangesWithin(t *testing.T) {
	testRangeGroupAddTwoRangesWithin(t, NewRangeTree())
}

func testRangeGroupAddTwoRangesWithin(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddTwoRangesSurround(t *testing.T) {
	testRangeGroupAddTwoRangesSurround(t, NewRangeList())
}

func TestRangeTreeAddTwoRangesSurround(t *testing.T) {
	testRangeGroupAddTwoRangesSurround(t, NewRangeTree())
}

func testRangeGroupAddTwoRangesSurround(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddThreeRangesMergeInOrder(t *testing.T) {
	testRangeGroupAddThreeRangesMergeInOrder(t, NewRangeList())
}

func TestRangeTreeAddThreeRangesMergeInOrder(t *testing.T) {
	testRangeGroupAddThreeRangesMergeInOrder(t, NewRangeTree())
}

func testRangeGroupAddThreeRangesMergeInOrder(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddThreeRangesMergeOutOfOrder(t *testing.T) {
	testRangeGroupAddThreeRangesMergeOutOfOrder(t, NewRangeList())
}

func TestRangeTreeAddThreeRangesMergeOutOfOrder(t *testing.T) {
	testRangeGroupAddThreeRangesMergeOutOfOrder(t, NewRangeTree())
}

func testRangeGroupAddThreeRangesMergeOutOfOrder(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddThreeRangesMergeReverseOrder(t *testing.T) {
	testRangeGroupAddThreeRangesMergeReverseOrder(t, NewRangeList())
}

func TestRangeTreeAddThreeRangesMergeReverseOrder(t *testing.T) {
	testRangeGroupAddThreeRangesMergeReverseOrder(t, NewRangeTree())
}

func testRangeGroupAddThreeRangesMergeReverseOrder(t *testing.T, rg RangeGroup) {
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

func TestRangeListAddThreeRangesSuperset(t *testing.T) {
	testRangeGroupAddThreeRangesSuperset(t, NewRangeList())
}

func TestRangeTreeAddThreeRangesSuperset(t *testing.T) {
	testRangeGroupAddThreeRangesSuperset(t, NewRangeTree())
}

func testRangeGroupAddThreeRangesSuperset(t *testing.T, rg RangeGroup) {
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

func TestRangeListSubRanges(t *testing.T) {
	testRangeGroupSubRanges(t, NewRangeList())
}

func TestRangeTreeSubRanges(t *testing.T) {
	testRangeGroupSubRanges(t, NewRangeTree())
}

func testRangeGroupSubRanges(t *testing.T, rg RangeGroup) {
	oneRange := []Range{{Start: []byte{0x01}, End: []byte{0x05}}}
	twoRanges := []Range{
		{Start: []byte{0x01}, End: []byte{0x05}},
		{Start: []byte{0x07}, End: []byte{0x0f}},
	}

	tests := []struct {
		add []Range
		sub Range
		rem bool
		res []Range
	}{
		{
			add: []Range{},
			sub: Range{Start: []byte{0x10}, End: []byte{0x11}},
			rem: false,
			res: []Range{},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x10}, End: []byte{0x11}},
			rem: false,
			res: []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x01}, End: []byte{0x05}},
			rem: true,
			res: []Range{},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x00}, End: []byte{0x06}},
			rem: true,
			res: []Range{},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x04}, End: []byte{0x06}},
			rem: true,
			res: []Range{{Start: []byte{0x01}, End: []byte{0x04}}},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x05}, End: []byte{0x06}},
			rem: false,
			res: []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x01}, End: []byte{0x03}},
			rem: true,
			res: []Range{{Start: []byte{0x03}, End: []byte{0x05}}},
		},
		{
			add: oneRange,
			sub: Range{Start: []byte{0x02}, End: []byte{0x04}},
			rem: true,
			res: []Range{
				{Start: []byte{0x01}, End: []byte{0x02}},
				{Start: []byte{0x04}, End: []byte{0x05}},
			},
		},
		{
			add: twoRanges,
			sub: Range{Start: []byte{0x10}, End: []byte{0x11}},
			rem: false,
			res: []Range{
				{Start: []byte{0x01}, End: []byte{0x05}},
				{Start: []byte{0x07}, End: []byte{0x0f}},
			},
		},
		{
			add: twoRanges,
			sub: Range{Start: []byte{0x01}, End: []byte{0x04}},
			rem: true,
			res: []Range{
				{Start: []byte{0x04}, End: []byte{0x05}},
				{Start: []byte{0x07}, End: []byte{0x0f}},
			},
		},
		{
			add: twoRanges,
			sub: Range{Start: []byte{0x01}, End: []byte{0x05}},
			rem: true,
			res: []Range{{Start: []byte{0x07}, End: []byte{0x0f}}},
		},
		{
			add: twoRanges,
			sub: Range{Start: []byte{0x03}, End: []byte{0x09}},
			rem: true,
			res: []Range{
				{Start: []byte{0x01}, End: []byte{0x03}},
				{Start: []byte{0x09}, End: []byte{0x0f}},
			},
		},
		{
			add: twoRanges,
			sub: Range{Start: []byte{0x03}, End: []byte{0xff}},
			rem: true,
			res: []Range{
				{Start: []byte{0x01}, End: []byte{0x03}},
			},
		},
		{
			add: twoRanges,
			sub: Range{Start: []byte{0x00}, End: []byte{0x09}},
			rem: true,
			res: []Range{
				{Start: []byte{0x09}, End: []byte{0x0f}},
			},
		},
	}

	for _, test := range tests {
		rg.Clear()

		for _, r := range test.add {
			if added := rg.Add(r); !added {
				t.Errorf("added new range %v to empty range group, wanted true added flag; got false", r)
			}
		}
		str := rg.String()

		if rem := rg.Sub(test.sub); rem != test.rem {
			t.Errorf("subtracted %v from range group %s, wanted %t removed flag; got %t", test.sub, str, test.rem, rem)
		}

		if l, el := rg.Len(), len(test.res); l != el {
			t.Errorf("subtracted %v from range group %s, wanted %b remaining ranges; got %b", test.sub, str, el, l)
		} else {
			ranges := make([]Range, 0, l)
			if err := rg.ForEach(func(r Range) error {
				ranges = append(ranges, r)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(ranges, test.res) {
				t.Errorf("subtracted %v from range group %s, wanted the remaining ranges %v; got %v", test.sub, str, test.res, ranges)
			}
		}
	}
}

func TestRangeListOverlapsRange(t *testing.T) {
	testRangeGroupOverlapsRange(t, NewRangeList())
}

func TestRangeTreeOverlapsRange(t *testing.T) {
	testRangeGroupOverlapsRange(t, NewRangeTree())
}

func testRangeGroupOverlapsRange(t *testing.T, rg RangeGroup) {
	rg.Clear()
	for _, r := range []Range{
		{Start: []byte{0x01}, End: []byte{0x05}},
		{Start: []byte{0x07}, End: []byte{0x0f}},
	} {
		if added := rg.Add(r); !added {
			t.Errorf("added new range %v to empty range group, wanted true added flag; got false", r)
		}
	}

	tests := []struct {
		r    Range
		want bool
	}{
		{
			r:    Range{Start: []byte{0x0a}, End: []byte{0x0b}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x01}, End: []byte{0x04}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x04}, End: []byte{0x07}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x05}, End: []byte{0x07}},
			want: false,
		},
		{
			r:    Range{Start: []byte{0x05}, End: []byte{0x08}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x01}, End: []byte{0x0f}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x10}, End: []byte{0x11}},
			want: false,
		},
	}

	for _, test := range tests {
		if enc := rg.Overlaps(test.r); enc != test.want {
			t.Errorf("testing if range group %v overlaps range %v, wanted %t; got %t", rg, test.r, test.want, enc)
		}
	}
}

func TestRangeListEnclosesRange(t *testing.T) {
	testRangeGroupEnclosesRange(t, NewRangeList())
}

func TestRangeTreeEnclosesRange(t *testing.T) {
	testRangeGroupEnclosesRange(t, NewRangeTree())
}

func testRangeGroupEnclosesRange(t *testing.T, rg RangeGroup) {
	rg.Clear()
	for _, r := range []Range{
		{Start: []byte{0x01}, End: []byte{0x05}},
		{Start: []byte{0x07}, End: []byte{0x0f}},
	} {
		if added := rg.Add(r); !added {
			t.Errorf("added new range %v to empty range group, wanted true added flag; got false", r)
		}
	}

	tests := []struct {
		r    Range
		want bool
	}{
		{
			r:    Range{Start: []byte{0x10}, End: []byte{0x11}},
			want: false,
		},
		{
			r:    Range{Start: []byte{0x01}, End: []byte{0x04}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x01}, End: []byte{0x05}},
			want: true,
		},
		{
			r:    Range{Start: []byte{0x01}, End: []byte{0x0f}},
			want: false,
		},
		{
			r:    Range{Start: []byte{0x07}, End: []byte{0x0f}},
			want: true,
		},
	}

	for _, test := range tests {
		if enc := rg.Encloses(test.r); enc != test.want {
			t.Errorf("testing if range group %v encloses range %v, wanted %t; got %t", rg, test.r, test.want, enc)
		}
	}
}

func TestRangeListStringer(t *testing.T) {
	testRangeGroupStringer(t, NewRangeList())
}

func TestRangeTreeStringer(t *testing.T) {
	testRangeGroupStringer(t, NewRangeTree())
}

func testRangeGroupStringer(t *testing.T, rg RangeGroup) {
	tests := []struct {
		rngs []Range
		str  string
	}{
		{
			rngs: []Range{},
			str:  "[]",
		},
		{
			rngs: []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
			str:  "[[01-05)]",
		},
		{
			rngs: []Range{
				{Start: []byte{0x01}, End: []byte{0x05}},
				{Start: []byte{0x09}, End: []byte{0xff}},
			},
			str: "[[01-05) [09-ff)]",
		},
	}

	for _, test := range tests {
		rg.Clear()
		for _, r := range test.rngs {
			rg.Add(r)
		}
		if str := rg.String(); str != test.str {
			t.Errorf("added new ranges %v to range group, wanted string value %s; got %s", test.rngs, test.str, str)
		}
	}
}

func TestRangeListAndRangeGroupIdentical(t *testing.T) {
	const trials = 5
	for tr := 0; tr < trials; tr++ {
		rl := NewRangeList()
		rt := NewRangeTree()

		const iters = 50
		const nBytes = 2
		for i := 0; i < iters; i++ {
			lStr := rl.String()
			tStr := rt.String()
			if lStr != tStr {
				t.Errorf("expected string value for RangeList and RangeTree to be the same; got %s for RangeList and %s for RangeTree", lStr, tStr)
			}

			ar := getRandomRange(t, nBytes)
			listAdded := rl.Add(ar)
			treeAdded := rt.Add(ar)
			if listAdded != treeAdded {
				t.Errorf("expected adding %s to RangeList %v and RangeTree %v to produce the same result; got %t from RangeList and %t from RangeTree", ar, rl, rt, listAdded, treeAdded)
			}

			sr := getRandomRange(t, nBytes)
			listSubtracted := rl.Sub(sr)
			treeSubtracted := rt.Sub(sr)
			if listSubtracted != treeSubtracted {
				t.Errorf("expected subtracting %s from RangeList %v and RangeTree %v to produce the same result; got %t from RangeList and %t from RangeTree", sr, rl, rt, listSubtracted, treeSubtracted)
			}

			or := getRandomRange(t, nBytes)
			listOverlaps := rl.Overlaps(or)
			treeOverlaps := rt.Overlaps(or)
			if listOverlaps != treeOverlaps {
				t.Errorf("expected RangeList %v and RangeTree %v to return the same overlapping state for %v; got %t from RangeList and %t from RangeTree", rl, rt, or, listOverlaps, treeOverlaps)
			}

			er := getRandomRange(t, nBytes)
			listEncloses := rl.Encloses(er)
			treeEncloses := rt.Encloses(er)
			if listEncloses != treeEncloses {
				t.Errorf("expected RangeList %v and RangeTree %v to return the same enclosing state for %v; got %t from RangeList and %t from RangeTree", rl, rt, er, listEncloses, treeEncloses)
			}

			listLen := rl.Len()
			treeLen := rt.Len()
			if listLen != treeLen {
				t.Errorf("expected RangeList and RangeTree to have the same length; got %d from RangeList and %d from RangeTree", listLen, treeLen)
			}

			listRngs := make([]Range, 0, rl.Len())
			treeRngs := make([]Range, 0, rt.Len())
			if err := rl.ForEach(func(r Range) error {
				listRngs = append(listRngs, r)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := rt.ForEach(func(r Range) error {
				treeRngs = append(treeRngs, r)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(listRngs, treeRngs) {
				t.Fatalf(`expected RangeList and RangeTree to contain the same ranges; started with %s, added %v, and subtracted %v to get %v for RangeList and %v for RangeTree`, lStr, ar, sr, rl, rt)
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

func BenchmarkRangeList(b *testing.B) {
	benchmarkRangeGroup(b, NewRangeList())
}

func BenchmarkRangeTree(b *testing.B) {
	benchmarkRangeGroup(b, NewRangeTree())
}

func benchmarkRangeGroup(b *testing.B, rg RangeGroup) {
	for i := 0; i < b.N; i++ {
		rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}})
		rg.Add(Range{Start: []byte{0x04}, End: []byte{0x06}})
		rg.Add(Range{Start: []byte{0x00}, End: []byte{0x02}})
		rg.Add(Range{Start: []byte{0x01}, End: []byte{0x06}})
		rg.Add(Range{Start: []byte{0x05}, End: []byte{0x15}})
		rg.Add(Range{Start: []byte{0x25}, End: []byte{0x30}})
		rg.Clear()
	}
}
