// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func TestJoinCardinality(t *testing.T) {
	c := func(min, max uint32) props.Cardinality {
		return props.Cardinality{Min: min, Max: max}
	}

	type testCase struct {
		left     props.Cardinality
		right    props.Cardinality
		expected props.Cardinality
	}

	testCaseGroups := []struct {
		joinType  opt.Operator
		filter    string // "true", "false", or "other"
		testCases []testCase
	}{
		{ // Inner join, true filter.
			joinType: opt.InnerJoinOp,
			filter:   "true",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(25, 100)},
			},
		},

		{ // Inner join, false filter.
			joinType: opt.InnerJoinOp,
			filter:   "false",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 0)},
				{left: c(5, 10), right: c(0, 10), expected: c(0, 0)},
				{left: c(0, 10), right: c(5, 10), expected: c(0, 0)},
				{left: c(5, 10), right: c(5, 10), expected: c(0, 0)},
			},
		},

		{ // Inner join, other filter.
			joinType: opt.InnerJoinOp,
			filter:   "other",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(0, 100)},
			},
		},

		{ // Left join, true filter.
			joinType: opt.LeftJoinOp,
			filter:   "true",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(5, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(25, 100)},
			},
		},

		{ // Left join, false filter.
			joinType: opt.LeftJoinOp,
			filter:   "false",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 10)},
				{left: c(5, 10), right: c(0, 10), expected: c(5, 10)},
				{left: c(0, 10), right: c(5, 10), expected: c(0, 10)},
				{left: c(5, 10), right: c(5, 10), expected: c(5, 10)},
			},
		},

		{ // Left join, other filter.
			joinType: opt.LeftJoinOp,
			filter:   "other",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(5, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(5, 100)},
			},
		},

		{ // Right join, true filter.
			joinType: opt.RightJoinOp,
			filter:   "true",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(5, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(25, 100)},
			},
		},

		{ // Right join, false filter.
			joinType: opt.RightJoinOp,
			filter:   "false",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 10)},
				{left: c(5, 10), right: c(0, 10), expected: c(0, 10)},
				{left: c(0, 10), right: c(5, 10), expected: c(5, 10)},
				{left: c(5, 10), right: c(5, 10), expected: c(5, 10)},
			},
		},

		{ // Right join, other filter.
			joinType: opt.RightJoinOp,
			filter:   "other",
			testCases: []testCase{
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(5, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(5, 100)},
			},
		},

		{ // Full join, true filter.
			joinType: opt.FullJoinOp,
			filter:   "true",
			testCases: []testCase{
				{left: c(0, 1), right: c(0, 1), expected: c(0, 2)},
				{left: c(1, 1), right: c(1, 1), expected: c(1, 2)},
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(5, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(5, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(25, 100)},
				{left: c(7, 10), right: c(8, 10), expected: c(56, 100)},
				{left: c(8, 10), right: c(7, 10), expected: c(56, 100)},
			},
		},

		{ // Full join, false filter.
			joinType: opt.FullJoinOp,
			filter:   "false",
			testCases: []testCase{
				{left: c(0, 1), right: c(0, 1), expected: c(0, 2)},
				{left: c(1, 1), right: c(1, 1), expected: c(2, 2)},
				{left: c(2, 5), right: c(3, 8), expected: c(5, 13)},
				{left: c(0, 10), right: c(0, 10), expected: c(0, 20)},
				{left: c(5, 10), right: c(0, 10), expected: c(5, 20)},
				{left: c(0, 10), right: c(5, 10), expected: c(5, 20)},
				{left: c(5, 10), right: c(5, 10), expected: c(10, 20)},
				{left: c(7, 10), right: c(8, 10), expected: c(15, 20)},
				{left: c(8, 10), right: c(7, 10), expected: c(15, 20)},
			},
		},

		{ // Full join, other filter.
			joinType: opt.FullJoinOp,
			filter:   "other",
			testCases: []testCase{
				{left: c(0, 1), right: c(0, 1), expected: c(0, 2)},
				{left: c(1, 1), right: c(1, 1), expected: c(1, 2)},
				{left: c(2, 5), right: c(3, 8), expected: c(3, 40)},
				{left: c(0, 10), right: c(0, 10), expected: c(0, 100)},
				{left: c(5, 10), right: c(0, 10), expected: c(5, 100)},
				{left: c(0, 10), right: c(5, 10), expected: c(5, 100)},
				{left: c(5, 10), right: c(5, 10), expected: c(5, 100)},
				{left: c(7, 10), right: c(8, 10), expected: c(8, 100)},
				{left: c(8, 10), right: c(7, 10), expected: c(8, 100)},
			},
		},
	}

	for _, group := range testCaseGroups {
		t.Run(fmt.Sprintf("%s/%s", group.joinType, group.filter), func(t *testing.T) {
			for i, tc := range group.testCases {
				t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
					h := &joinPropsHelper{}
					h.joinType = group.joinType
					h.leftProps = &props.Relational{Cardinality: tc.left}
					h.rightProps = &props.Relational{Cardinality: tc.right}
					h.filterIsTrue = (group.filter == "true")
					h.filterIsFalse = (group.filter == "false")

					res := h.cardinality()
					if res != tc.expected {
						t.Errorf(
							"left=%s right=%s: expected %s, got %s\n", tc.left, tc.right, tc.expected, res,
						)
					}
				})
			}
		})
	}
}
