// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMergeJoiner(t *testing.T) {
	tcs := []struct {
		description     string
		leftTuples      []tuple
		leftTypes       []types.T
		leftOutCols     []uint32
		leftEqCols      []uint32
		rightTuples     []tuple
		rightTypes      []types.T
		rightOutCols    []uint32
		rightEqCols     []uint32
		expected        []tuple
		expectedOutCols []int
		outputBatchSize uint16
	}{
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
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
			outputBatchSize: coldata.BatchSize,
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.leftTuples, tc.rightTuples}, func(t *testing.T, input []Operator) {
			s := NewMergeJoinOp(input[0], input[1], tc.leftOutCols, tc.rightOutCols, tc.leftTypes, tc.rightTypes, tc.leftEqCols, tc.rightEqCols)

			out := newOpTestOutput(s, tc.expectedOutCols, tc.expected)
			s.(*mergeJoinOp).initWithBatchSize(tc.outputBatchSize)

			if err := out.Verify(); err != nil {
				t.Fatalf("Test case description: '%s'\n%v", tc.description, err)
			}
		})
	}
}

// TestMergeJoinerMultiBatch creates one long input of a 1:1 join, and keeps track of the expected
// output to make sure the join output is batched correctly.
func TestMergeJoinerMultiBatch(t *testing.T) {
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

						a := NewMergeJoinOp(
							leftSource,
							rightSource,
							[]uint32{0},
							[]uint32{0},
							typs,
							typs,
							[]uint32{0},
							[]uint32{0},
						)

						a.(*mergeJoinOp).initWithBatchSize(outBatchSize)

						i := 0
						count := 0
						// Keep track of the last comparison value.
						lastVal := int64(0)
						for b := a.Next(); b.Length() != 0; b = a.Next() {
							count += int(b.Length())
							outCol := b.ColVec(0).Int64()
							for j := int64(0); j < int64(b.Length()); j++ {
								outVal := outCol[j]
								expVal := lastVal
								if outVal != expVal {
									t.Fatalf("Found val %d, expected %d, idx %d of batch %d", outVal, expVal, j, i)
								}
								lastVal++
							}
							i++
						}
						if count != nTuples {
							t.Fatalf("Found count %d, expected count %d", count, nTuples)
						}
					})
			}
		}
	}
}

// TestMergeJoinerMultiBatchRuns creates one long input of a n:n join, and keeps track of the expected
// count to make sure the join output is batched correctly.
func TestMergeJoinerMultiBatchRuns(t *testing.T) {
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

					a := NewMergeJoinOp(
						leftSource,
						rightSource,
						[]uint32{0},
						[]uint32{0},
						typs,
						typs,
						[]uint32{0, 1},
						[]uint32{0, 1},
					)

					a.(*mergeJoinOp).Init()

					i := 0
					count := 0
					// Keep track of the last comparison value.
					lastVal := int64(0)
					for b := a.Next(); b.Length() != 0; b = a.Next() {
						count += int(b.Length())
						outCol := b.ColVec(0).Int64()
						for j := int64(0); j < int64(b.Length()); j++ {
							outVal := outCol[j]
							expVal := lastVal / int64(groupSize*groupSize)
							if outVal != expVal {
								t.Fatalf("Found val %d, expected %d, idx %d of batch %d", outVal, expVal, j, i)
							}
							lastVal++
						}
						i++
					}

					if count != groupSize*coldata.BatchSize*numInputBatches {
						t.Fatalf("Found count %d, expected count %d", count, groupSize*coldata.BatchSize*numInputBatches)
					}
				})
		}
	}
}

// TestMergeJoinerLongMultiBatchCount creates one long input of a 1:1 join, and keeps track of the expected
// count to make sure the join output is batched correctly.
func TestMergeJoinerLongMultiBatchCount(t *testing.T) {
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

						a := NewMergeJoinOp(
							leftSource,
							rightSource,
							[]uint32{},
							[]uint32{},
							typs,
							typs,
							[]uint32{0},
							[]uint32{0},
						)

						a.(*mergeJoinOp).initWithBatchSize(outBatchSize)

						count := 0
						for b := a.Next(); b.Length() != 0; b = a.Next() {
							count += int(b.Length())
						}
						if count != nTuples {
							t.Fatalf("Found count %d, expected count %d", count, nTuples)
						}
					})
			}
		}
	}
}

// TestMergeJoinerMultiBatchCountRuns creates one long input of a n:n join, and keeps track of the expected
// count to make sure the join output is batched correctly.
func TestMergeJoinerMultiBatchCountRuns(t *testing.T) {
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

					a := NewMergeJoinOp(
						leftSource,
						rightSource,
						[]uint32{},
						[]uint32{},
						typs,
						typs,
						[]uint32{0},
						[]uint32{0},
					)

					a.(*mergeJoinOp).Init()

					count := 0
					for b := a.Next(); b.Length() != 0; b = a.Next() {
						count += int(b.Length())
					}
					if count != groupSize*coldata.BatchSize*numInputBatches {
						t.Fatalf("Found count %d, expected count %d", count, groupSize*coldata.BatchSize*numInputBatches)
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

							a := NewMergeJoinOp(
								leftSource,
								rightSource,
								[]uint32{0},
								[]uint32{0},
								typs,
								typs,
								[]uint32{0},
								[]uint32{0},
							)

							a.(*mergeJoinOp).Init()

							i := 0
							count := 0
							cpIdx := 0
							for b := a.Next(); b.Length() != 0; b = a.Next() {
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
										t.Fatalf("Found val %d, expected %d, idx %d of batch %d", outVal, expVal, j, i)
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
		vec.UnsetNulls()
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
		vec.UnsetNulls()
	}
	return batch
}

func BenchmarkMergeJoiner(b *testing.B) {
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

				s := mergeJoinOp{
					left: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{0, 1},
						sourceTypes: sourceTypes,
						source:      leftSource,
					},

					right: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{2, 3},
						sourceTypes: sourceTypes,
						source:      rightSource,
					},
				}

				s.Init()

				b.StartTimer()
				for b := s.Next(); b.Length() != 0; b = s.Next() {
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

				s := mergeJoinOp{
					left: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{0, 1},
						sourceTypes: sourceTypes,
						source:      leftSource,
					},

					right: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{2, 3},
						sourceTypes: sourceTypes,
						source:      rightSource,
					},
				}

				s.Init()

				b.StartTimer()
				for b := s.Next(); b.Length() != 0; b = s.Next() {
				}
				b.StopTimer()
			}
		})
	}

	// Groups on both sides.
	for _, nBatches := range []int{1, 4, 16, 32} {
		b.Run(fmt.Sprintf("bothSidesRepeat-rows=%d", nBatches*coldata.BatchSize), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * coldata.BatchSize * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)

				s := mergeJoinOp{
					left: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{0, 1},
						sourceTypes: sourceTypes,
						source:      leftSource,
					},

					right: mergeJoinInput{
						eqCols:      []uint32{0},
						outCols:     []uint32{2, 3},
						sourceTypes: sourceTypes,
						source:      rightSource,
					},
				}

				s.Init()

				b.StartTimer()
				for b := s.Next(); b.Length() != 0; b = s.Next() {
				}
				b.StopTimer()
			}
		})
	}

}
