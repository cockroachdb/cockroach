// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestPermuteValuesColumns(t *testing.T) {
	testCases := []struct {
		cols     opt.ColList
		wanted   opt.ColList
		rows     []string
		expected []string
	}{
		{ // 0
			cols:   opt.ColList{1, 2, 3},
			wanted: opt.ColList{1, 2, 3},
			rows: []string{
				"a1 a2 a3",
				"b1 b2 b3",
			},
			expected: []string{
				"a1 a2 a3",
				"b1 b2 b3",
			},
		},
		{ // 1
			cols:   opt.ColList{1, 2, 3},
			wanted: opt.ColList{1, 3, 2},
			rows: []string{
				"a1 a2 a3",
				"b1 b2 b3",
			},
			expected: []string{
				"a1 a3 a2",
				"b1 b3 b2",
			},
		},
		{ // 2
			cols:   opt.ColList{1, 2, 3},
			wanted: opt.ColList{3, 1, 2},
			rows: []string{
				"a1 a2 a3",
			},
			expected: []string{
				"a3 a1 a2",
			},
		},
		{ // 3
			cols:     opt.ColList{1, 2, 3},
			wanted:   opt.ColList{1, 3, 2},
			rows:     nil,
			expected: nil,
		},
		{ // 4
			cols:   opt.ColList{1, 2},
			wanted: opt.ColList{2, 1},
			rows: []string{
				"a1 a2",
				"b1 b2",
				"c1 c2",
			},
			expected: []string{
				"a2 a1",
				"b2 b1",
				"c2 c1",
			},
		},
		{ // 5
			cols:   opt.ColList{1, 2},
			wanted: opt.ColList{2, 1, 3},
			rows: []string{
				"a1 a2",
				"b1 b2",
			},
			expected: []string{"fail"},
		},
		{ // 6
			cols:   opt.ColList{1, 2, 3},
			wanted: opt.ColList{2, 2, 3},
			rows: []string{
				"a1 a2 a3",
				"b1 b2 b3",
			},
			expected: []string{"fail"},
		},
		{ // 7
			cols:     opt.ColList{1, 2, 3},
			wanted:   opt.ColList{1, 3},
			rows:     []string{},
			expected: []string{"fail"},
		},
		{ // 8
			cols:   opt.ColList{1, 2, 2, 3},
			wanted: opt.ColList{2, 1, 3, 2},
			rows: []string{
				"a1 a2 a2 a3",
				"b1 b2 b2 b3",
			},
			expected: []string{
				"a2 a1 a3 a2",
				"b2 b1 b3 b2",
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			rows := make([][]tree.TypedExpr, len(tc.rows))
			for i := range tc.rows {
				for _, str := range strings.Split(tc.rows[i], " ") {
					rows[i] = append(rows[i], tree.NewDString(str))
				}
			}
			var result []string
			if permuteValuesColumns(tc.cols, tc.wanted, rows) {
				for i, row := range rows {
					strs := make([]string, len(rows[i]))
					for j := range row {
						strs[j] = string(*row[j].(*tree.DString))
					}
					result = append(result, strings.Join(strs, " "))
				}
			} else {
				result = []string{"fail"}
			}
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %#v, got %#v", tc.expected, result)
			}
		})
	}
}
