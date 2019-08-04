// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type mjTestInitializer interface {
	initWithBatchSize(outBatchSize uint16)
}

type mjTestCase struct {
	description     string
	joinType        sqlbase.JoinType
	leftTuples      []tuple
	leftTypes       []types.T
	leftOutCols     []uint32
	leftEqCols      []uint32
	leftDirections  []distsqlpb.Ordering_Column_Direction
	rightTuples     []tuple
	rightTypes      []types.T
	rightOutCols    []uint32
	rightEqCols     []uint32
	rightDirections []distsqlpb.Ordering_Column_Direction
	expected        []tuple
	expectedOutCols []int
	outputBatchSize uint16
}

func (tc *mjTestCase) Init() {
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = coldata.BatchSize
	}

	if len(tc.leftDirections) == 0 {
		tc.leftDirections = make([]distsqlpb.Ordering_Column_Direction, len(tc.leftTypes))
		for i := range tc.leftDirections {
			tc.leftDirections[i] = distsqlpb.Ordering_Column_ASC
		}
	}

	if len(tc.rightDirections) == 0 {
		tc.rightDirections = make([]distsqlpb.Ordering_Column_Direction, len(tc.rightTypes))
		for i := range tc.rightDirections {
			tc.rightDirections[i] = distsqlpb.Ordering_Column_ASC
		}
	}
}

func TestMergeJoiner(t *testing.T) {
	tcs := []mjTestCase{
		{
			description:     "basic test",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, no out cols",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{}, {}, {}, {}},
			expectedOutCols: []int{},
		},
		{
			description:     "basic test, out col on left",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, out col on right",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {2}, {3}, {4}},
			expectedOutCols: []int{1},
		},
		{
			description:     "basic test, L missing",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, R missing",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, L duplicate",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, R duplicate",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, R duplicate 2",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}},
			rightTuples:     tuples{{1}, {1}, {2}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {2}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, L+R duplicates",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic test, L+R duplicate, multiple runs",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {2}, {2}, {3}, {4}},
			rightTuples:     tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {2}, {2}, {2}, {3}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "cross product test, batch size = 1024 (col.BatchSize)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
		},
		{
			description:     "cross product test, batch size = 4 (small even)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: 4,
		},
		{
			description:     "cross product test, batch size = 3 (small odd)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: 3,
		},
		{
			description:     "cross product test, batch size = 1 (unit)",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expectedOutCols: []int{0},
			outputBatchSize: 1,
		},
		{
			description:     "multi output column test, basic",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "multi output column test, batch size = 1",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: 1,
		},
		{
			description:     "multi output column test, test output coldata projection",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			expectedOutCols: []int{0, 2},
		},
		{
			description:     "multi output column test, test output coldata projection",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{1},
			rightOutCols:    []uint32{1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{10, 11}, {20, 12}, {30, 13}, {40, 14}},
			expectedOutCols: []int{1, 3},
		},
		{
			description:     "multi output column test, L run",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "multi output column test, L run, batch size = 1",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: 1,
		},
		{
			description:     "multi output column test, R run",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "multi output column test, R run, batch size = 1",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
			outputBatchSize: 1,
		},
		{
			description:     "logic test",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{-1, -1}, {0, 4}, {2, 1}, {3, 4}, {5, 4}},
			rightTuples:     tuples{{0, 5}, {1, 3}, {3, 2}, {4, 6}},
			leftOutCols:     []uint32{1},
			rightOutCols:    []uint32{1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{5, 4}, {2, 4}},
			expectedOutCols: []int{3, 1},
		},
		{
			description:  "multi output column test, batch size = 1 and runs (to test saved output), reordered out columns",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1, 0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{10, 1, 11, 1},
				{20, 2, 12, 2},
				{30, 3, 13, 3},
				{40, 4, 14, 4},
			},
			expectedOutCols: []int{1, 0, 3, 2},
			outputBatchSize: 1,
		},
		{
			description:  "multi output column test, batch size = 1 and runs (to test saved output), reordered out columns that dont start at 0",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{10, 1, 11},
				{20, 2, 12},
				{30, 3, 13},
				{40, 4, 14},
			},
			expectedOutCols: []int{1, 0, 3},
			outputBatchSize: 1,
		},
		{
			description:  "equality column is correctly indexed",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{10, 1}, {10, 1}, {10, 1}, {20, 2}, {30, 3}, {40, 4}},
			rightTuples:  tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{1},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{1, 10, 11},
				{1, 10, 11},
				{1, 10, 11},
				{1, 10, 11},
				{1, 10, 11},
				{1, 10, 11},
				{2, 20, 12},
				{3, 30, 13},
				{4, 40, 14},
			},
			expectedOutCols: []int{1, 0, 3},
		},
		{
			description:  "multi column equality basic test",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 10}, {2, 20}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{1, 10, 1, 10},
				{2, 20, 2, 20},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:  "multi column equality runs",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 10}, {1, 10}, {2, 20}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{2, 20, 2, 20},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:  "multi column non-consecutive equality cols",
			leftTypes:    []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64, types.Int64},
			leftTuples:   tuples{{1, 123, 1}, {1, 234, 10}},
			rightTuples:  tuples{{1, 1, 345}, {1, 10, 456}},
			leftOutCols:  []uint32{0, 2, 1},
			rightOutCols: []uint32{0, 2, 1},
			leftEqCols:   []uint32{0, 2},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{1, 123, 1, 1, 1, 345},
				{1, 234, 10, 1, 10, 456},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:  "multi column equality: new batch ends run",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 1}, {1, 1}, {3, 3}, {4, 3}},
			rightTuples:  tuples{{1, 1}, {1, 2}, {3, 3}, {3, 3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{1, 1, 1, 1},
				{1, 1, 1, 1},
				{3, 3, 3, 3},
				{3, 3, 3, 3},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:  "multi column equality: reordered eq columns",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 1}, {1, 1}, {3, 3}, {4, 3}},
			rightTuples:  tuples{{1, 1}, {1, 2}, {3, 3}, {3, 3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{1, 0},
			expected: tuples{
				{1, 1, 1, 1},
				{1, 1, 1, 1},
				{3, 3, 3, 3},
				{3, 3, 3, 3},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:  "cross batch, distinct group",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{1, 2}, {1, 2}, {1, 2}, {2, 2}},
			rightTuples:  tuples{{1, 2}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{1, 2, 1, 2},
				{1, 2, 1, 2},
				{1, 2, 1, 2},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:  "templating basic test",
			leftTypes:    []types.T{types.Bool, types.Int16, types.Float64},
			rightTypes:   []types.T{types.Bool, types.Int16, types.Float64},
			leftTuples:   tuples{{true, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{true, int16(10), 1.2}, {false, int16(20), 2.2}, {true, int16(30), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1, 2},
			rightEqCols:  []uint32{0, 1, 2},
			expected: tuples{
				{true, 10, 1.2, true, 10, 1.2},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:  "templating cross product test",
			leftTypes:    []types.T{types.Bool, types.Int16, types.Float64},
			rightTypes:   []types.T{types.Bool, types.Int16, types.Float64},
			leftTuples:   tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{false, int16(10), 1.2}, {true, int16(20), 2.3}, {true, int16(20), 2.4}, {true, int16(31), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{false, 10, 1.2, false, 10, 1.2},
				{true, 20, 2.2, true, 20, 2.3},
				{true, 20, 2.2, true, 20, 2.4},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:  "templating cross product test, output batch size 1",
			leftTypes:    []types.T{types.Bool, types.Int16, types.Float64},
			rightTypes:   []types.T{types.Bool, types.Int16, types.Float64},
			leftTuples:   tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{false, int16(10), 1.2}, {true, int16(20), 2.3}, {true, int16(20), 2.4}, {true, int16(31), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{false, 10, 1.2, false, 10, 1.2},
				{true, 20, 2.2, true, 20, 2.3},
				{true, 20, 2.2, true, 20, 2.4},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
			outputBatchSize: 1,
		},
		{
			description:  "templating cross product test, output batch size 2",
			leftTypes:    []types.T{types.Bool, types.Int16, types.Float64},
			rightTypes:   []types.T{types.Bool, types.Int16, types.Float64},
			leftTuples:   tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{false, int16(10), 1.2}, {true, int16(20), 2.3}, {true, int16(20), 2.4}, {true, int16(31), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{false, 10, 1.2, false, 10, 1.2},
				{true, 20, 2.2, true, 20, 2.3},
				{true, 20, 2.2, true, 20, 2.4},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
			outputBatchSize: 2,
		},
		{
			description:  "templating reordered eq columns",
			leftTypes:    []types.T{types.Bool, types.Int16, types.Float64},
			rightTypes:   []types.T{types.Bool, types.Int16, types.Float64},
			leftTuples:   tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{false, int16(10), 1.2}, {true, int16(20), 2.3}, {true, int16(20), 2.4}, {true, int16(31), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{1, 0},
			rightEqCols:  []uint32{1, 0},
			expected: tuples{
				{false, 10, 1.2, false, 10, 1.2},
				{true, 20, 2.2, true, 20, 2.3},
				{true, 20, 2.2, true, 20, 2.4},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:  "templating reordered eq columns non symmetrical",
			leftTypes:    []types.T{types.Bool, types.Int16, types.Float64},
			rightTypes:   []types.T{types.Int16, types.Float64, types.Bool},
			leftTuples:   tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{int16(10), 1.2, false}, {int16(20), 2.2, true}, {int16(21), 2.2, true}, {int16(30), 3.2, false}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{2, 0},
			rightEqCols:  []uint32{1, 2},
			expected: tuples{
				{false, 10, 1.2, 10, 1.2, false},
				{true, 20, 2.2, 20, 2.2, true},
				{true, 20, 2.2, 21, 2.2, true},
			},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:  "null handling",
			leftTypes:    []types.T{types.Int64},
			rightTypes:   []types.T{types.Int64},
			leftTuples:   tuples{{nil}, {0}},
			rightTuples:  tuples{{nil}, {0}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{0, 0},
			},
			expectedOutCols: []int{0, 1},
		},
		{
			description:  "null handling multi column, nulls on left",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{nil, 0}, {0, nil}},
			rightTuples:  tuples{{nil, nil}, {0, 1}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{0, nil, 0, 1},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:  "null handling multi column, nulls on right",
			leftTypes:    []types.T{types.Int64, types.Int64},
			rightTypes:   []types.T{types.Int64, types.Int64},
			leftTuples:   tuples{{nil, 0}, {0, 1}},
			rightTuples:  tuples{{nil, nil}, {0, nil}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{0, 1, 0, nil},
			},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "desc test",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{4}, {3}, {2}, {1}},
			rightTuples:     tuples{{4}, {2}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{4, 4}, {2, 2}, {1, 1}},
			expectedOutCols: []int{0, 1},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
		},
		{
			description:     "desc nulls test",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{4}, {3}, {nil}, {1}},
			rightTuples:     tuples{{4}, {nil}, {2}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{4, 4}, {1, 1}},
			expectedOutCols: []int{0, 1},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
		},
		{
			description:     "desc nulls test end on 0",
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{9}, {9}, {8}, {0}, {nil}},
			rightTuples:     tuples{{9}, {9}, {8}, {0}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{9, 9}, {9, 9}, {9, 9}, {9, 9}, {8, 8}, {0, 0}},
			expectedOutCols: []int{0, 1},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
		},
		{
			description:     "non-equality columns with nulls",
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, nil}, {2, 2}, {2, 2}, {3, nil}, {4, nil}},
			rightTuples:     tuples{{1, 1}, {2, nil}, {2, nil}, {3, nil}, {4, 4}, {4, 4}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, nil, 1, 1}, {2, 2, 2, nil}, {2, 2, 2, nil}, {2, 2, 2, nil}, {2, 2, 2, nil}, {3, nil, 3, nil}, {4, nil, 4, 4}, {4, nil, 4, 4}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "basic LEFT OUTER JOIN test, L and R exhausted at the same time",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:     tuples{{0}, {2}, {3}, {4}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, nil}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic LEFT OUTER JOIN test, R exhausted first",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, nil}, {1, nil}, {3, 3}, {5, nil}, {6, nil}, {7, nil}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic LEFT OUTER JOIN test, L exhausted first",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{3, 3}, {5, nil}, {6, 6}, {7, nil}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "multi output column LEFT OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:     tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, nil}, {2, 20, nil, nil}, {3, nil, 3, 13}, {4, 40, 4, 14}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "null in equality column LEFT OUTER JOIN",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:     tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {3, 3, 3}},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "multi equality column LEFT OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:     tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{nil, nil, nil, nil}, {nil, 10, nil, nil}, {1, nil, nil, nil}, {1, 10, nil, nil}, {2, 20, 2, 20}, {4, 40, nil, nil}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "multi equality column (long runs on left) LEFT OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:     tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{1, 9, nil, nil}, {1, 10, nil, nil}, {1, 10, nil, nil}, {1, 11, 1, 11}, {1, 11, 1, 11}, {2, 20, nil, nil}, {2, 20, nil, nil}, {2, 21, 2, 21}, {2, 22, nil, nil}, {2, 22, nil, nil}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "3 equality column LEFT OUTER JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{2, 3, 1, nil, nil, nil}, {2, nil, 1, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:     "3 equality column LEFT OUTER JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_ASC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{2, 3, 1, nil, nil, nil}, {2, nil, 1, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:     "single column DESC with nulls on the left LEFT OUTER JOIN",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     tuples{{1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic RIGHT OUTER JOIN test, L and R exhausted at the same time",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{-1}, {2}, {3}, {4}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic RIGHT OUTER JOIN test, R exhausted first",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, 2}, {3, 3}, {nil, 4}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic RIGHT OUTER JOIN test, L exhausted first",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, 2}, {3, 3}, {nil, 4}, {6, 6}, {nil, 8}, {nil, 9}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "multi output column RIGHT OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, nil}, {3, 13}, {4, 14}},
			rightTuples:     tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, nil, 1, 10}, {nil, nil, 2, 20}, {3, 13, 3, nil}, {4, 14, 4, 40}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "null in equality column RIGHT OUTER JOIN",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			rightTuples:     tuples{{nil}, {nil}, {1}, {3}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {3, 3, 3}},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "multi equality column RIGHT OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			rightTuples:     tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{nil, nil, nil, nil}, {nil, nil, nil, 10}, {nil, nil, 1, nil}, {nil, nil, 1, 10}, {2, 20, 2, 20}, {nil, nil, 4, 40}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "multi equality column (long runs on right) RIGHT OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			rightTuples:     tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{nil, nil, 1, 9}, {nil, nil, 1, 10}, {nil, nil, 1, 10}, {1, 11, 1, 11}, {1, 11, 1, 11}, {nil, nil, 2, 20}, {nil, nil, 2, 20}, {2, 21, 2, 21}, {nil, nil, 2, 22}, {nil, nil, 2, 22}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "3 equality column RIGHT OUTER JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, nil, nil, nil, 1, 3}},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:     "3 equality column RIGHT OUTER JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_ASC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, nil, nil, nil, 1, 3}},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:     "single column DESC with nulls on the right RIGHT OUTER JOIN",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}},
			rightTuples:     tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic FULL OUTER JOIN test, L and R exhausted at the same time",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{-1}, {2}, {3}, {4}, {4}},
			rightTuples:     tuples{{1}, {2}, {3}, {4}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{-1, nil}, {nil, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic FULL OUTER JOIN test, R exhausted first",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, nil}, {1, nil}, {nil, 2}, {3, 3}, {nil, 4}, {5, nil}, {6, nil}, {7, nil}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic FULL OUTER JOIN test, L exhausted first",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, 2}, {3, 3}, {nil, 4}, {5, nil}, {6, 6}, {7, nil}, {nil, 8}, {nil, 9}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "multi output column FULL OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, nil}, {3, 13}, {4, 14}},
			rightTuples:     tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, nil, 1, 10}, {nil, nil, 2, 20}, {3, 13, 3, nil}, {4, 14, 4, 40}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "null in equality column FULL OUTER JOIN",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			rightTuples:     tuples{{nil}, {nil}, {1}, {3}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil, 1, nil}, {nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {2, 2, nil}, {3, 3, 3}},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "multi equality column FULL OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			rightTuples:     tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{nil, nil, nil, nil}, {nil, nil, nil, nil}, {nil, 10, nil, nil}, {nil, nil, nil, 10}, {1, nil, nil, nil}, {1, nil, nil, nil}, {nil, nil, 1, nil}, {nil, nil, 1, 10}, {2, 20, 2, 20}, {3, 30, nil, nil}, {nil, nil, 4, 40}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "multi equality column (long runs on right) FULL OUTER JOIN test with nulls",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			rightTuples:     tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{1, 8, nil, nil}, {nil, nil, 1, 9}, {nil, nil, 1, 10}, {nil, nil, 1, 10}, {1, 11, 1, 11}, {1, 11, 1, 11}, {nil, nil, 2, 20}, {nil, nil, 2, 20}, {2, 21, 2, 21}, {nil, nil, 2, 22}, {nil, nil, 2, 22}, {2, 23, nil, nil}},
			expectedOutCols: []int{0, 1, 2, 3},
		},
		{
			description:     "3 equality column FULL OUTER JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{4, 3, 3, nil, nil, nil}, {nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, 2, nil, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}, {nil, nil, nil, nil, 1, 3}},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:     "3 equality column FULL OUTER JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_ASC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{4, 3, 3, nil, nil, nil}, {nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, 2, nil, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}, {nil, nil, nil, nil, 1, 3}},
			expectedOutCols: []int{0, 1, 2, 3, 4, 5},
		},
		{
			description:     "single column DESC with nulls on the right FULL OUTER JOIN",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}},
			rightTuples:     tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "basic LEFT SEMI JOIN test, L and R exhausted at the same time",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:     tuples{{-1}, {2}, {3}, {4}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{2}, {3}, {4}, {4}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic LEFT SEMI JOIN test, R exhausted first",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{3}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic LEFT SEMI JOIN test, L exhausted first",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {3}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{3}, {6}},
			expectedOutCols: []int{0},
		},
		{
			description:     "multi output column LEFT SEMI JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:     tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10}, {3, nil}, {4, 40}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "null in equality column LEFT SEMI JOIN",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:     tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {3}},
			expectedOutCols: []int{0},
		},
		{
			description:     "multi equality column LEFT SEMI JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:     tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{2, 20}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "multi equality column (long runs on left) LEFT SEMI JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {1, 11}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:     tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{1, 11}, {1, 11}, {1, 11}, {2, 21}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "3 equality column LEFT SEMI JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "3 equality column LEFT SEMI JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_ASC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "single column DESC with nulls on the left LEFT SEMI JOIN",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     tuples{{1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic LEFT ANTI JOIN test, L and R exhausted at the same time",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:     tuples{{-1}, {2}, {4}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {3}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic LEFT ANTI JOIN test, R exhausted first",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {4}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {5}, {6}, {7}},
			expectedOutCols: []int{0},
		},
		{
			description:     "basic LEFT ANTI JOIN test, L exhausted first",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftTuples:      tuples{{3}, {5}, {6}, {7}},
			rightTuples:     tuples{{2}, {3}, {3}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{5}, {7}},
			expectedOutCols: []int{0},
		},
		{
			description:     "multi output column LEFT ANTI JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:     tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{2, 20}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "null in equality column LEFT ANTI JOIN",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:     tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil}, {nil}},
			expectedOutCols: []int{0},
		},
		{
			description:     "multi equality column LEFT ANTI JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:     tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {4, 40}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "multi equality column (long runs on left) LEFT ANTI JOIN test with nulls",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64},
			leftTuples:      tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {1, 11}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:     tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1},
			rightEqCols:     []uint32{0, 1},
			expected:        tuples{{1, 9}, {1, 10}, {1, 10}, {2, 20}, {2, 20}, {2, 22}, {2, 22}},
			expectedOutCols: []int{0, 1},
		},
		{
			description:     "3 equality column LEFT ANTI JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "3 equality column LEFT ANTI JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64, types.Int64, types.Int64},
			rightTypes:      []types.T{types.Int64, types.Int64, types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_ASC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC, distsqlpb.Ordering_Column_DESC, distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			expectedOutCols: []int{0, 1, 2},
		},
		{
			description:     "single column DESC with nulls on the left LEFT ANTI JOIN",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []types.T{types.Int64},
			rightTypes:      []types.T{types.Int64},
			leftDirections:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			rightDirections: []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     tuples{{1}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil}, {nil}, {nil}},
			expectedOutCols: []int{0},
		},
	}

	for _, tc := range tcs {
		tc.Init()
		lOrderings := make([]distsqlpb.Ordering_Column, len(tc.leftEqCols))
		for i := range tc.leftEqCols {
			lOrderings[i] = distsqlpb.Ordering_Column{
				ColIdx:    tc.leftEqCols[i],
				Direction: tc.leftDirections[i],
			}
		}

		rOrderings := make([]distsqlpb.Ordering_Column, len(tc.rightEqCols))
		for i := range tc.rightEqCols {
			rOrderings[i] = distsqlpb.Ordering_Column{
				ColIdx:    tc.rightEqCols[i],
				Direction: tc.rightDirections[i],
			}
		}

		// We use a custom verifier function so that we can get the merge join op
		// to use a custom output batch size per test, to exercise more cases.
		var mergeJoinVerifier verifier = func(output *opTestOutput) error {
			if mj, ok := output.input.(mjTestInitializer); ok {
				mj.initWithBatchSize(tc.outputBatchSize)
			} else {
				t.Fatalf("unexpectedly merge joiner doesn't implement mjTestInitializer")
			}
			verify := output.Verify
			if _, isFullOuter := output.input.(*mergeJoinFullOuterOp); isFullOuter {
				// FULL OUTER JOIN doesn't guarantee any ordering on its output (since
				// it is ambiguous), so we're comparing the outputs as sets.
				verify = output.VerifyAnyOrder
			}

			return verify()
		}

		runTests(t, []tuples{tc.leftTuples, tc.rightTuples}, tc.expected, mergeJoinVerifier,
			tc.expectedOutCols, func(input []Operator) (Operator, error) {
				return NewMergeJoinOp(tc.joinType, input[0], input[1], tc.leftOutCols,
					tc.rightOutCols, tc.leftTypes, tc.rightTypes, lOrderings, rOrderings)
			})
	}
}

// TestMergeJoinerMultiBatch creates one long input of a 1:1 join, and keeps
// track of the expected output to make sure the join output is batched
// correctly.
func TestMergeJoinerMultiBatch(t *testing.T) {
	ctx := context.Background()
	for _, numInputBatches := range []int{1, 2, 16} {
		for _, outBatchSize := range []uint16{1, 16, coldata.BatchSize} {
			t.Run(fmt.Sprintf("numInputBatches=%d", numInputBatches),
				func(t *testing.T) {
					nTuples := coldata.BatchSize * numInputBatches
					typs := []types.T{types.Int64}
					cols := []coldata.Vec{coldata.NewMemColumn(typs[0], nTuples)}
					groups := cols[0].Int64()
					for i := range groups {
						groups[i] = int64(i)
					}

					leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
					rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

					a, err := NewMergeJoinOp(
						sqlbase.InnerJoin,
						leftSource,
						rightSource,
						[]uint32{0},
						[]uint32{0},
						typs,
						typs,
						[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
						[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
					)
					if err != nil {
						t.Fatal("error in merge join op constructor", err)
					}

					a.(*mergeJoinInnerOp).initWithBatchSize(outBatchSize)

					i := 0
					count := 0
					// Keep track of the last comparison value.
					expVal := int64(0)
					for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
						count += int(b.Length())
						outCol := b.ColVec(0).Int64()
						for j := int64(0); j < int64(b.Length()); j++ {
							outVal := outCol[j]
							if outVal != expVal {
								t.Fatalf("found val %d, expected %d, idx %d of batch %d",
									outVal, expVal, j, i)
							}
							expVal++
						}
						i++
					}
					if count != nTuples {
						t.Fatalf("found count %d, expected count %d", count, nTuples)
					}
				})
		}
	}
}

// TestMergeJoinerMultiBatchRuns creates one long input of a n:n join, and
// keeps track of the expected count to make sure the join output is batched
// correctly.
func TestMergeJoinerMultiBatchRuns(t *testing.T) {
	ctx := context.Background()
	for _, groupSize := range []int{coldata.BatchSize / 8, coldata.BatchSize / 4, coldata.BatchSize / 2} {
		for _, numInputBatches := range []int{1, 2, 16} {
			t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(t *testing.T) {
					nTuples := coldata.BatchSize * numInputBatches
					typs := []types.T{types.Int64, types.Int64}
					cols := []coldata.Vec{coldata.NewMemColumn(typs[0], nTuples), coldata.NewMemColumn(typs[1], nTuples)}
					for i := range cols[0].Int64() {
						cols[0].Int64()[i] = int64(i / groupSize)
						cols[1].Int64()[i] = int64(i / groupSize)
					}

					leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
					rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

					a, err := NewMergeJoinOp(
						sqlbase.InnerJoin,
						leftSource,
						rightSource,
						[]uint32{0},
						[]uint32{0},
						typs,
						typs,
						[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}, {ColIdx: 1, Direction: distsqlpb.Ordering_Column_ASC}},
						[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}, {ColIdx: 1, Direction: distsqlpb.Ordering_Column_ASC}},
					)
					if err != nil {
						t.Fatal("error in merge join op constructor", err)
					}

					a.(*mergeJoinInnerOp).Init()

					i := 0
					count := 0
					// Keep track of the last comparison value.
					lastVal := int64(0)
					for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
						count += int(b.Length())
						outCol := b.ColVec(0).Int64()
						for j := int64(0); j < int64(b.Length()); j++ {
							outVal := outCol[j]
							expVal := lastVal / int64(groupSize*groupSize)
							if outVal != expVal {
								t.Fatalf("found val %d, expected %d, idx %d of batch %d",
									outVal, expVal, j, i)
							}
							lastVal++
						}
						i++
					}

					if count != groupSize*coldata.BatchSize*numInputBatches {
						t.Fatalf("found count %d, expected count %d",
							count, groupSize*coldata.BatchSize*numInputBatches)
					}
				})
		}
	}
}

// TestMergeJoinerLongMultiBatchCount creates one long input of a 1:1 join, and
// keeps track of the expected count to make sure the join output is batched
// correctly.
func TestMergeJoinerLongMultiBatchCount(t *testing.T) {
	ctx := context.Background()
	for _, groupSize := range []int{1, 2, coldata.BatchSize / 4, coldata.BatchSize / 2} {
		for _, numInputBatches := range []int{1, 2, 16} {
			for _, outBatchSize := range []uint16{1, 16, coldata.BatchSize} {
				t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
					func(t *testing.T) {
						nTuples := coldata.BatchSize * numInputBatches
						typs := []types.T{types.Int64}
						cols := []coldata.Vec{coldata.NewMemColumn(typs[0], nTuples)}
						groups := cols[0].Int64()
						for i := range groups {
							groups[i] = int64(i)
						}

						leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
						rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

						a, err := NewMergeJoinOp(
							sqlbase.InnerJoin,
							leftSource,
							rightSource,
							[]uint32{},
							[]uint32{},
							typs,
							typs,
							[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
							[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
						)
						if err != nil {
							t.Fatal("error in merge join op constructor", err)
						}

						a.(*mergeJoinInnerOp).initWithBatchSize(outBatchSize)

						count := 0
						for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
							count += int(b.Length())
						}
						if count != nTuples {
							t.Fatalf("found count %d, expected count %d",
								count, nTuples)
						}
					})
			}
		}
	}
}

// TestMergeJoinerMultiBatchCountRuns creates one long input of a n:n join, and
// keeps track of the expected count to make sure the join output is batched
// correctly.
func TestMergeJoinerMultiBatchCountRuns(t *testing.T) {
	ctx := context.Background()
	for _, groupSize := range []int{coldata.BatchSize / 8, coldata.BatchSize / 4, coldata.BatchSize / 2} {
		for _, numInputBatches := range []int{1, 2, 16} {
			t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(t *testing.T) {
					nTuples := coldata.BatchSize * numInputBatches
					typs := []types.T{types.Int64}
					cols := []coldata.Vec{coldata.NewMemColumn(typs[0], nTuples)}
					groups := cols[0].Int64()
					for i := range groups {
						groups[i] = int64(i / groupSize)
					}

					leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
					rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

					a, err := NewMergeJoinOp(
						sqlbase.InnerJoin,
						leftSource,
						rightSource,
						[]uint32{},
						[]uint32{},
						typs,
						typs,
						[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
						[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
					)
					if err != nil {
						t.Fatal("error in merge join op constructor", err)
					}

					a.(*mergeJoinInnerOp).Init()

					count := 0
					for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
						count += int(b.Length())
					}
					if count != groupSize*coldata.BatchSize*numInputBatches {
						t.Fatalf("found count %d, expected count %d",
							count, groupSize*coldata.BatchSize*numInputBatches)
					}
				})
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type expectedGroup struct {
	val         int64
	cardinality int
}

func newBatchesOfRandIntRows(
	nTuples int, typs []types.T, maxRunLength int64, skipValues bool, randomIncrement int64,
) ([]coldata.Vec, []coldata.Vec, []expectedGroup) {
	rng, _ := randutil.NewPseudoRand()
	lCols := []coldata.Vec{coldata.NewMemColumn(typs[0], nTuples)}
	lCol := lCols[0].Int64()
	rCols := []coldata.Vec{coldata.NewMemColumn(typs[0], nTuples)}
	rCol := rCols[0].Int64()
	exp := make([]expectedGroup, nTuples)
	val := int64(0)
	lIdx, rIdx := 0, 0
	i := 0
	for lIdx < nTuples && rIdx < nTuples {
		// Randomly increment the value to write to the group.
		val += 1 + randomIncrement*rng.Int63n(256)
		// Determine whether or not to create a group.
		if skipValues && rng.Int63n(4) == 0 {
			lCol[lIdx] = val
			lIdx++
			// Randomly increment the value to write to the group.
			val += 1 + rng.Int63n(16)
			rCol[rIdx] = val
			rIdx++
		} else {
			lGroupSize := min(int(rng.Int63n(maxRunLength)+1), nTuples-lIdx)
			rGroupSize := min(int(rng.Int63n(maxRunLength)+1), nTuples-rIdx)

			for j := 0; j < lGroupSize; j++ {
				lCol[lIdx] = val
				lIdx++
			}

			for j := 0; j < rGroupSize; j++ {
				rCol[rIdx] = val
				rIdx++
			}

			exp[i] = expectedGroup{val, lGroupSize * rGroupSize}
			i++
		}
	}

	if lIdx < nTuples {
		for lIdx < nTuples {
			lCol[lIdx] = val + 1
			lIdx++
		}
	}

	if rIdx < nTuples {
		for rIdx < nTuples {
			rCol[rIdx] = val + 1
			rIdx++
		}
	}

	return lCols, rCols, exp
}

func TestMergeJoinerRandomized(t *testing.T) {
	ctx := context.Background()
	for _, numInputBatches := range []int{1, 2, 16, 256} {
		for _, maxRunLength := range []int64{2, 3, 100} {
			for _, skipValues := range []bool{false, true} {
				for _, randomIncrement := range []int64{0, 1} {
					t.Run(fmt.Sprintf("numInputBatches=%dmaxRunLength=%dskipValues=%trandomIncrement=%d", numInputBatches, maxRunLength, skipValues, randomIncrement),
						func(t *testing.T) {
							nTuples := coldata.BatchSize * numInputBatches
							typs := []types.T{types.Int64}
							lCols, rCols, exp := newBatchesOfRandIntRows(nTuples, typs, maxRunLength, skipValues, randomIncrement)
							leftSource := newChunkingBatchSource(typs, lCols, uint64(nTuples))
							rightSource := newChunkingBatchSource(typs, rCols, uint64(nTuples))

							a, err := NewMergeJoinOp(
								sqlbase.InnerJoin,
								leftSource,
								rightSource,
								[]uint32{0},
								[]uint32{0},
								typs,
								typs,
								[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
								[]distsqlpb.Ordering_Column{{ColIdx: 0, Direction: distsqlpb.Ordering_Column_ASC}},
							)

							if err != nil {
								t.Fatal("error in merge join op constructor", err)
							}

							a.(*mergeJoinInnerOp).Init()

							i := 0
							count := 0
							cpIdx := 0
							for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
								count += int(b.Length())
								outCol := b.ColVec(0).Int64()
								for j := 0; j < int(b.Length()); j++ {
									outVal := outCol[j]

									if exp[cpIdx].cardinality == 0 {
										cpIdx++
									}
									expVal := exp[cpIdx].val
									exp[cpIdx].cardinality--
									if expVal != outVal {
										t.Fatalf("found val %d, expected %d, idx %d of batch %d",
											outVal, expVal, j, i)
									}
								}
								i++
							}
						})
				}
			}
		}
	}
}

func newBatchOfIntRows(nCols int, batch coldata.Batch) coldata.Batch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize; i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(coldata.BatchSize)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.Nulls().UnsetNulls()
	}
	return batch
}

func newBatchOfRepeatedIntRows(nCols int, batch coldata.Batch, numRepeats int) coldata.Batch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize; i++ {
			col[i] = int64((i + 1) / numRepeats)
		}
	}

	batch.SetLength(coldata.BatchSize)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.Nulls().UnsetNulls()
	}
	return batch
}

func BenchmarkMergeJoiner(b *testing.B) {
	ctx := context.Background()
	nCols := 4
	sourceTypes := make([]types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int64
	}

	batch := coldata.NewMemBatch(sourceTypes)

	// 1:1 join.
	for _, nBatches := range []int{1, 4, 16, 1024} {
		b.Run(fmt.Sprintf("rows=%d", nBatches*coldata.BatchSize), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * coldata.BatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)

				s := mergeJoinInnerOp{
					mergeJoinBase{
						left: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{0, 1},
							sourceTypes: sourceTypes,
							source:      leftSource,
							directions:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC},
						},

						right: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{2, 3},
							sourceTypes: sourceTypes,
							source:      rightSource,
							directions:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC},
						},
					},
				}

				s.Init()

				b.StartTimer()
				for b := s.Next(ctx); b.Length() != 0; b = s.Next(ctx) {
				}
				b.StopTimer()
			}
		})
	}

	// Groups on left side.
	for _, nBatches := range []int{1, 4, 16, 1024} {
		b.Run(fmt.Sprintf("oneSideRepeat-rows=%d", nBatches*coldata.BatchSize), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * coldata.BatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)

				s := mergeJoinInnerOp{
					mergeJoinBase{
						left: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{0, 1},
							sourceTypes: sourceTypes,
							source:      leftSource,
							directions:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC},
						},

						right: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{2, 3},
							sourceTypes: sourceTypes,
							source:      rightSource,
							directions:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC},
						},
					},
				}
				s.Init()

				b.StartTimer()
				for b := s.Next(ctx); b.Length() != 0; b = s.Next(ctx) {
				}
				b.StopTimer()
			}
		})
	}

	// Groups on both sides.
	for _, nBatches := range []int{1, 4, 16, 32} {
		numRepeats := nBatches
		b.Run(fmt.Sprintf("bothSidesRepeat-rows=%d", nBatches*coldata.BatchSize), func(b *testing.B) {

			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * coldata.BatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, numRepeats), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, numRepeats), nBatches)

				s := mergeJoinInnerOp{
					mergeJoinBase{
						left: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{0, 1},
							sourceTypes: sourceTypes,
							source:      leftSource,
							directions:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC},
						},

						right: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{2, 3},
							sourceTypes: sourceTypes,
							source:      rightSource,
							directions:  []distsqlpb.Ordering_Column_Direction{distsqlpb.Ordering_Column_ASC},
						},
					},
				}

				s.Init()

				b.StartTimer()
				for b := s.Next(ctx); b.Length() != 0; b = s.Next(ctx) {
				}
				b.StopTimer()
			}
		})
	}

}
