// Copyright 2018 The Cockroach Authors.
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

package tpcc

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func partitionerTestName(total, active, parts int) string {
	return fmt.Sprintf("total=%d,active=%d,parts=%d", total, active, parts)
}

func TestPartitioner(t *testing.T) {
	tests := []struct {
		total  int
		active int
		parts  int

		expPartBounds   []int
		expPartElems    [][]int
		expPartElemsMap map[int]int
		expTotalElems   []int
	}{
		{
			total:           10,
			active:          10,
			parts:           1,
			expPartBounds:   []int{0, 10},
			expPartElems:    [][]int{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			expPartElemsMap: map[int]int{0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0},
			expTotalElems:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			total:           10,
			active:          7,
			parts:           1,
			expPartBounds:   []int{0, 10},
			expPartElems:    [][]int{{0, 1, 2, 3, 4, 5, 6}},
			expPartElemsMap: map[int]int{0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0},
			expTotalElems:   []int{0, 1, 2, 3, 4, 5, 6},
		},
		{
			total:           10,
			active:          10,
			parts:           2,
			expPartBounds:   []int{0, 5, 10},
			expPartElems:    [][]int{{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}},
			expPartElemsMap: map[int]int{0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1},
			expTotalElems:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			total:           10,
			active:          3,
			parts:           2,
			expPartBounds:   []int{0, 5, 10},
			expPartElems:    [][]int{{0}, {5, 6}},
			expPartElemsMap: map[int]int{0: 0, 5: 1, 6: 1},
			expTotalElems:   []int{0, 5, 6},
		},
		{
			total:           10,
			active:          10,
			parts:           3,
			expPartBounds:   []int{0, 3, 6, 10},
			expPartElems:    [][]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8, 9}},
			expPartElemsMap: map[int]int{0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2, 9: 2},
			expTotalElems:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			total:           10,
			active:          5,
			parts:           3,
			expPartBounds:   []int{0, 3, 6, 10},
			expPartElems:    [][]int{{0}, {3, 4}, {6, 7}},
			expPartElemsMap: map[int]int{0: 0, 3: 1, 4: 1, 6: 2, 7: 2},
			expTotalElems:   []int{0, 3, 4, 6, 7},
		},
		{
			total:           10,
			active:          10,
			parts:           10,
			expPartBounds:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expPartElems:    [][]int{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}},
			expPartElemsMap: map[int]int{0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9},
			expTotalElems:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			total:           10,
			active:          6,
			parts:           10,
			expPartBounds:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expPartElems:    [][]int{{}, {1}, {}, {3}, {4}, {}, {6}, {}, {8}, {9}},
			expPartElemsMap: map[int]int{1: 1, 3: 3, 4: 4, 6: 6, 8: 8, 9: 9},
			expTotalElems:   []int{1, 3, 4, 6, 8, 9},
		},
		{
			total:           10,
			active:          1,
			parts:           10,
			expPartBounds:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expPartElems:    [][]int{{}, {}, {}, {}, {}, {}, {}, {}, {}, {9}},
			expPartElemsMap: map[int]int{9: 9},
			expTotalElems:   []int{9},
		},
	}
	for _, tc := range tests {
		name := partitionerTestName(tc.total, tc.active, tc.parts)
		t.Run(name, func(t *testing.T) {
			p, err := makePartitioner(tc.total, tc.active, tc.parts)
			if err != nil {
				t.Errorf("got error %v", err)
			}
			if !reflect.DeepEqual(p.partBounds, tc.expPartBounds) {
				t.Errorf("expected partition boundaries %v, got %v", tc.expPartBounds, p.partBounds)
			}
			if !reflect.DeepEqual(p.partElems, tc.expPartElems) {
				t.Errorf("expected partition elements %v, got %v", tc.expPartElems, p.partElems)
			}
			if !reflect.DeepEqual(p.partElemsMap, tc.expPartElemsMap) {
				t.Errorf("expected partition element reverse mapping %v, got %v", tc.expPartElemsMap, p.partElemsMap)
			}
			if !reflect.DeepEqual(p.totalElems, tc.expTotalElems) {
				t.Errorf("expected total elements %v, got %v", tc.expTotalElems, p.totalElems)
			}
		})
	}
}

func TestPartitionerError(t *testing.T) {
	tests := []struct {
		total  int
		active int
		parts  int

		expError string
	}{
		{total: 0, active: 9, parts: 3, expError: "total must be positive; 0"},
		{total: 9, active: 0, parts: 3, expError: "active must be positive; 0"},
		{total: 9, active: 9, parts: 0, expError: "parts must be positive; 0"},
		{total: -1, active: 9, parts: 3, expError: "total must be positive; -1"},
		{total: 9, active: -2, parts: 3, expError: "active must be positive; -2"},
		{total: 9, active: 9, parts: -3, expError: "parts must be positive; -3"},
		{total: 8, active: 9, parts: 3, expError: "active > total; 9 > 8"},
		{total: 3, active: 3, parts: 4, expError: "parts > total; 4 > 3"},
	}
	for _, tc := range tests {
		name := partitionerTestName(tc.total, tc.active, tc.parts)
		t.Run(name, func(t *testing.T) {
			p, err := makePartitioner(tc.total, tc.active, tc.parts)
			if p != nil {
				t.Errorf("expected nil partitioner, got %+v", p)
			}
			if !testutils.IsError(err, tc.expError) {
				t.Errorf("expected error %q, got %v", tc.expError, err)
			}
		})
	}
}
