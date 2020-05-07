// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package interval

import (
	"bytes"
	"crypto/rand"
	"reflect"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/cockroachdb/errors"
)

func forEachRangeGroupImpl(t *testing.T, fn func(t *testing.T, rg RangeGroup)) {
	for _, constr := range []func() RangeGroup{
		NewRangeList,
		NewRangeTree,
	} {
		rg := constr()
		name := reflect.TypeOf(rg).Elem().Name()
		t.Run(name, func(t *testing.T) {
			fn(t, rg)
		})
	}
}

func TestRangeGroupAddSingleRangeAndClear(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupAddTwoRangesBefore(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}
		if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}

		if l := rg.Len(); l != 2 {
			t.Errorf("added 2 disjoint ranges to range group, wanted len = 2; got len = %d", l)
		}
	})
}

func TestRangeGroupAddTwoRangesAfter(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x02}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}
		if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}

		if l := rg.Len(); l != 2 {
			t.Errorf("added 2 disjoint ranges to range group, wanted len = 2; got len = %d", l)
		}
	})
}

func TestRangeGroupAddTwoRangesMergeForward(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}
		if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}

		if l := rg.Len(); l != 1 {
			t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
		}
	})
}

func TestRangeGroupAddTwoRangesMergeBackwards(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		if added := rg.Add(Range{Start: []byte{0x03}, End: []byte{0x04}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}
		if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x03}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}

		if l := rg.Len(); l != 1 {
			t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
		}
	})
}

func TestRangeGroupAddTwoRangesWithin(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x04}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}
		if added := rg.Add(Range{Start: []byte{0x02}, End: []byte{0x03}}); added {
			t.Errorf("added fully contained range to range group, wanted false added flag; got true")
		}

		if l := rg.Len(); l != 1 {
			t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
		}
	})
}

func TestRangeGroupAddTwoRangesSurround(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		if added := rg.Add(Range{Start: []byte{0x02}, End: []byte{0x03}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}
		if added := rg.Add(Range{Start: []byte{0x01}, End: []byte{0x04}}); !added {
			t.Errorf("added new range to range group, wanted true added flag; got false")
		}

		if l := rg.Len(); l != 1 {
			t.Errorf("added 2 overlapping ranges to range group, wanted len = 1; got len = %d", l)
		}
	})
}

func TestRangeGroupAddThreeRangesMergeInOrder(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupAddThreeRangesMergeOutOfOrder(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupAddThreeRangesMergeReverseOrder(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupAddThreeRangesSuperset(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupSubRanges(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupOverlapsRange(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
				r:    Range{Start: []byte{0x06}, End: []byte{0x07}},
				want: false,
			},
			{
				r:    Range{Start: []byte{0x05}, End: []byte{0x07}},
				want: false,
			},
			{
				r:    Range{Start: []byte{0x05}, End: []byte{0x06}},
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
	})
}

func TestRangeGroupEnclosesRange(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
	})
}

func TestRangeGroupForEach(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		errToThrow := errors.New("this error should be thrown")
		dontErr := -1
		tests := []struct {
			rngs     []Range
			errAfter int // after this many iterations, throw error. -1 to ignore.
		}{
			{
				rngs:     []Range{},
				errAfter: dontErr,
			},
			{
				rngs:     []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
				errAfter: dontErr,
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0xff}},
				},
				errAfter: dontErr,
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0x10}},
					{Start: []byte{0x1a}, End: []byte{0x1c}},
					{Start: []byte{0x44}, End: []byte{0xf0}},
					{Start: []byte{0xf1}, End: []byte{0xff}},
				},
				errAfter: dontErr,
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0x10}},
					{Start: []byte{0x1a}, End: []byte{0x1c}},
					{Start: []byte{0x44}, End: []byte{0xf0}},
					{Start: []byte{0xf1}, End: []byte{0xff}},
				},
				errAfter: 1,
			},
		}

		for _, test := range tests {
			rg.Clear()
			for _, r := range test.rngs {
				rg.Add(r)
			}

			n := 0
			acc := []Range{}
			throwingErr := test.errAfter != -1
			errSaw := rg.ForEach(func(r Range) error {
				if throwingErr && n >= test.errAfter {
					return errToThrow
				}
				n++
				acc = append(acc, r)
				return nil
			})

			expRngs := test.rngs
			if throwingErr {
				expRngs = test.rngs[:test.errAfter]
				if !errors.Is(errSaw, errToThrow) {
					t.Errorf("expected error %v from RangeGroup.ForEach, found %v", errToThrow, errSaw)
				}
			} else {
				if errSaw != nil {
					t.Errorf("no error expected from RangeGroup.ForEach, found %v", errSaw)
				}
			}
			if !reflect.DeepEqual(acc, expRngs) {
				t.Errorf("expected to accumulate Ranges %v, found %v", expRngs, acc)
			}
		}
	})
}

func TestRangeGroupIterator(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
		tests := []struct {
			rngs []Range
		}{
			{
				rngs: []Range{},
			},
			{
				rngs: []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0xff}},
				},
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0x10}},
					{Start: []byte{0x1a}, End: []byte{0x1c}},
					{Start: []byte{0x44}, End: []byte{0xf0}},
					{Start: []byte{0xf1}, End: []byte{0xff}},
				},
			},
		}

		for _, test := range tests {
			rg.Clear()
			for _, r := range test.rngs {
				rg.Add(r)
			}

			acc := []Range{}
			it := rg.Iterator()
			for r, ok := it.Next(); ok; r, ok = it.Next() {
				acc = append(acc, r)
			}

			if !reflect.DeepEqual(acc, test.rngs) {
				t.Errorf("expected to accumulate Ranges %v, found %v", test.rngs, acc)
			}
		}
	})
}

func TestRangeGroupStringer(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg RangeGroup) {
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
				str:  "[{01-05}]",
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0xff}},
				},
				str: "[{01-05} {09-ff}]",
			},
			{
				rngs: []Range{
					{Start: []byte{0x01}, End: []byte{0x05}},
					{Start: []byte{0x09}, End: []byte{0xf0}},
					{Start: []byte{0xf1}, End: []byte{0xff}},
				},
				str: "[{01-05} {09-f0} {f1-ff}]",
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
	})
}

func TestRangeGroupsOverlap(t *testing.T) {
	forEachRangeGroupImpl(t, func(t *testing.T, rg1 RangeGroup) {
		forEachRangeGroupImpl(t, func(t *testing.T, rg2 RangeGroup) {
			tests := []struct {
				rngs1   []Range
				rngs2   []Range
				overlap bool
			}{
				{
					rngs1:   []Range{},
					rngs2:   []Range{},
					overlap: false,
				},
				{
					rngs1:   []Range{},
					rngs2:   []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
					overlap: false,
				},
				{
					rngs1:   []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
					rngs2:   []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
					overlap: true,
				},
				{
					rngs1:   []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
					rngs2:   []Range{{Start: []byte{0x05}, End: []byte{0x08}}},
					overlap: false,
				},
				{
					rngs1:   []Range{{Start: []byte{0x01}, End: []byte{0x05}}},
					rngs2:   []Range{{Start: []byte{0x04}, End: []byte{0x08}}},
					overlap: true,
				},
				{
					rngs1: []Range{
						{Start: []byte{0x01}, End: []byte{0x05}},
						{Start: []byte{0x09}, End: []byte{0xf0}},
					},
					rngs2:   []Range{{Start: []byte{0xf1}, End: []byte{0xff}}},
					overlap: false,
				},
				{
					rngs1: []Range{
						{Start: []byte{0x01}, End: []byte{0x05}},
						{Start: []byte{0x09}, End: []byte{0xf0}},
					},
					rngs2:   []Range{{Start: []byte{0xe0}, End: []byte{0xff}}},
					overlap: true,
				},
				{
					rngs1: []Range{
						{Start: []byte{0x01}, End: []byte{0x05}},
						{Start: []byte{0x09}, End: []byte{0x10}},
						{Start: []byte{0x1a}, End: []byte{0x1c}},
						{Start: []byte{0x44}, End: []byte{0xf0}},
						{Start: []byte{0xf1}, End: []byte{0xff}},
					},
					rngs2:   []Range{{Start: []byte{0x11}, End: []byte{0x19}}},
					overlap: false,
				},
				{
					rngs1: []Range{
						{Start: []byte{0x01}, End: []byte{0x05}},
						{Start: []byte{0x09}, End: []byte{0x10}},
						{Start: []byte{0x1a}, End: []byte{0x1c}},
						{Start: []byte{0x44}, End: []byte{0xf0}},
						{Start: []byte{0xf1}, End: []byte{0xff}},
					},
					rngs2:   []Range{{Start: []byte{0x11}, End: []byte{0xff}}},
					overlap: true,
				},
			}

			for _, test := range tests {
				for _, swap := range []bool{false, true} {
					rg1.Clear()
					rg2.Clear()

					rngs1, rngs2 := test.rngs1, test.rngs2
					if swap {
						rngs1, rngs2 = rngs2, rngs1
					}

					for _, r := range rngs1 {
						rg1.Add(r)
					}
					for _, r := range rngs2 {
						rg2.Add(r)
					}

					overlap := RangeGroupsOverlap(rg1, rg2)
					if e, a := test.overlap, overlap; a != e {
						t.Errorf("expected RangeGroupsOverlap(%s, %s) = %t, found %t", rg1, rg2, e, a)
					}
				}
			}
		})
	})
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
				t.Fatalf("expected RangeList and RangeTree to have the same length; got %d from RangeList and %d from RangeTree", listLen, treeLen)
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
				t.Fatalf("expected RangeList and RangeTree to contain the same ranges:\nstarted with %s\nadded %v\nsubtracted %v\n\ngot:\nRangeList: %v\nRangeTree: %v", lStr, ar, sr, rl, rt)
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
