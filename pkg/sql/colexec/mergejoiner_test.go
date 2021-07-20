// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func createSpecForMergeJoiner(tc *joinTestCase) *execinfrapb.ProcessorSpec {
	leftOrdering := execinfrapb.Ordering{}
	for i, eqCol := range tc.leftEqCols {
		leftOrdering.Columns = append(
			leftOrdering.Columns,
			execinfrapb.Ordering_Column{
				ColIdx:    eqCol,
				Direction: tc.leftDirections[i],
			},
		)
	}
	rightOrdering := execinfrapb.Ordering{}
	for i, eqCol := range tc.rightEqCols {
		rightOrdering.Columns = append(
			rightOrdering.Columns,
			execinfrapb.Ordering_Column{
				ColIdx:    eqCol,
				Direction: tc.rightDirections[i],
			},
		)
	}
	mjSpec := &execinfrapb.MergeJoinerSpec{
		LeftOrdering:  leftOrdering,
		RightOrdering: rightOrdering,
		OnExpr:        tc.onExpr,
		Type:          tc.joinType,
	}
	projection := make([]uint32, 0, len(tc.leftOutCols)+len(tc.rightOutCols))
	projection = append(projection, tc.leftOutCols...)
	rColOffset := uint32(len(tc.leftTypes))
	if !tc.joinType.ShouldIncludeLeftColsInOutput() {
		rColOffset = 0
	}
	for _, outCol := range tc.rightOutCols {
		projection = append(projection, rColOffset+outCol)
	}
	var resultTypes []*types.T
	for _, i := range projection {
		if int(i) < len(tc.leftTypes) {
			resultTypes = append(resultTypes, tc.leftTypes[i])
		} else {
			resultTypes = append(resultTypes, tc.rightTypes[i-rColOffset])
		}
	}
	return &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{
			{ColumnTypes: tc.leftTypes},
			{ColumnTypes: tc.rightTypes},
		},
		Core: execinfrapb.ProcessorCoreUnion{
			MergeJoiner: mjSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: projection,
		},
		ResultTypes: resultTypes,
	}
}

func getMJTestCases() []*joinTestCase {
	mjTestCases := []*joinTestCase{
		{
			description:  "basic test",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, no out cols",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{}, {}, {}, {}},
		},
		{
			description:  "basic test, out col on left",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, out col on right",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, L missing",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {3}, {4}},
		},
		{
			description:  "basic test, R missing",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {3}, {4}},
		},
		{
			description:  "basic test, L duplicate",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, R duplicate",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, R duplicate 2",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}},
			rightTuples:  colexectestutils.Tuples{{1}, {1}, {2}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {2}},
		},
		{
			description:  "basic test, L+R duplicates",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {1}, {1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, L+R duplicate, multiple runs",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {2}, {2}, {3}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {2}, {2}, {2}, {3}, {4}},
		},
		{
			description:  "cross product test",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {1}, {1}},
			rightTuples:  colexectestutils.Tuples{{1}, {1}, {1}, {1}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
		},
		{
			description:  "multi output column test",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:  "multi output column test, test output coldata projection",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
		},
		{
			description:  "multi output column test, test output coldata projection",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{10, 11}, {20, 12}, {30, 13}, {40, 14}},
		},
		{
			description:  "multi output column test, L run",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:  "multi output column test, R run",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:  "logic test",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{-1, -1}, {0, 4}, {2, 1}, {3, 4}, {5, 4}},
			rightTuples:  colexectestutils.Tuples{{0, 5}, {1, 3}, {3, 2}, {4, 6}},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{4, 5}, {4, 2}},
		},
		{
			description:  "multi output column test, runs (to test saved output), reordered out columns",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1, 0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
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
		},
		{
			description:  "multi output column test, runs (to test saved output), reordered out columns that dont start at 0",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
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
		},
		{
			description:  "equality column is correctly indexed",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{10, 1}, {10, 1}, {10, 1}, {20, 2}, {30, 3}, {40, 4}},
			rightTuples:  colexectestutils.Tuples{{1, 11}, {1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1, 0},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{1},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
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
		},
		{
			description:  "multi column equality basic test",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 10}, {2, 20}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: colexectestutils.Tuples{
				{1, 10, 1, 10},
				{2, 20, 2, 20},
			},
		},
		{
			description:  "multi column equality runs",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {1, 10}, {1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, 10}, {1, 10}, {2, 20}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: colexectestutils.Tuples{
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{1, 10, 1, 10},
				{2, 20, 2, 20},
			},
		},
		{
			description:  "multi column non-consecutive equality cols",
			leftTypes:    []*types.T{types.Int, types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 123, 1}, {1, 234, 10}},
			rightTuples:  colexectestutils.Tuples{{1, 1, 345}, {1, 10, 456}},
			leftOutCols:  []uint32{0, 2, 1},
			rightOutCols: []uint32{0, 2, 1},
			leftEqCols:   []uint32{0, 2},
			rightEqCols:  []uint32{0, 1},
			expected: colexectestutils.Tuples{
				{1, 1, 123, 1, 345, 1},
				{1, 10, 234, 1, 456, 10},
			},
		},
		{
			description:  "multi column equality: new batch ends run",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 1}, {1, 1}, {3, 3}, {4, 3}},
			rightTuples:  colexectestutils.Tuples{{1, 1}, {1, 2}, {3, 3}, {3, 3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: colexectestutils.Tuples{
				{1, 1, 1, 1},
				{1, 1, 1, 1},
				{3, 3, 3, 3},
				{3, 3, 3, 3},
			},
		},
		{
			description:  "multi column equality: reordered eq columns",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 1}, {1, 1}, {3, 3}, {4, 3}},
			rightTuples:  colexectestutils.Tuples{{1, 1}, {1, 2}, {3, 3}, {3, 3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{1, 0},
			expected: colexectestutils.Tuples{
				{1, 1, 1, 1},
				{1, 1, 1, 1},
				{3, 3, 3, 3},
				{3, 3, 3, 3},
			},
		},
		{
			description:  "cross batch, distinct group",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 2}, {1, 2}, {1, 2}, {2, 2}},
			rightTuples:  colexectestutils.Tuples{{1, 2}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: colexectestutils.Tuples{
				{1, 2, 1, 2},
				{1, 2, 1, 2},
				{1, 2, 1, 2},
			},
		},
		{
			description:  "templating basic test",
			leftTypes:    []*types.T{types.Bool, types.Int2, types.Float},
			rightTypes:   []*types.T{types.Bool, types.Int2, types.Float},
			leftTuples:   colexectestutils.Tuples{{true, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  colexectestutils.Tuples{{true, int16(10), 1.2}, {false, int16(20), 2.2}, {true, int16(30), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1, 2},
			rightEqCols:  []uint32{0, 1, 2},
			expected: colexectestutils.Tuples{
				{true, 10, 1.2, true, 10, 1.2},
			},
		},
		{
			description:  "templating cross product test",
			leftTypes:    []*types.T{types.Bool, types.Int2, types.Float},
			rightTypes:   []*types.T{types.Bool, types.Int2, types.Float},
			leftTuples:   colexectestutils.Tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  colexectestutils.Tuples{{false, int16(10), 1.2}, {true, int16(20), 2.3}, {true, int16(20), 2.4}, {true, int16(31), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected: colexectestutils.Tuples{
				{false, 10, 1.2, false, 10, 1.2},
				{true, 20, 2.2, true, 20, 2.3},
				{true, 20, 2.2, true, 20, 2.4},
			},
		},
		{
			description:  "templating reordered eq columns",
			leftTypes:    []*types.T{types.Bool, types.Int2, types.Float},
			rightTypes:   []*types.T{types.Bool, types.Int2, types.Float},
			leftTuples:   colexectestutils.Tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  colexectestutils.Tuples{{false, int16(10), 1.2}, {true, int16(20), 2.3}, {true, int16(20), 2.4}, {true, int16(31), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{1, 0},
			rightEqCols:  []uint32{1, 0},
			expected: colexectestutils.Tuples{
				{false, 10, 1.2, false, 10, 1.2},
				{true, 20, 2.2, true, 20, 2.3},
				{true, 20, 2.2, true, 20, 2.4},
			},
		},
		{
			description:  "templating reordered eq columns non symmetrical",
			leftTypes:    []*types.T{types.Bool, types.Int2, types.Float},
			rightTypes:   []*types.T{types.Int2, types.Float, types.Bool},
			leftTuples:   colexectestutils.Tuples{{false, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  colexectestutils.Tuples{{int16(10), 1.2, false}, {int16(20), 2.2, true}, {int16(21), 2.2, true}, {int16(30), 3.2, false}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{2, 0},
			rightEqCols:  []uint32{1, 2},
			expected: colexectestutils.Tuples{
				{false, 10, 1.2, 10, 1.2, false},
				{true, 20, 2.2, 20, 2.2, true},
				{true, 20, 2.2, 21, 2.2, true},
			},
		},
		{
			description:  "null handling",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{nil}, {0}},
			rightTuples:  colexectestutils.Tuples{{nil}, {0}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
				{0, 0},
			},
		},
		{
			description:  "null handling multi column, nulls on left",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 0}, {0, nil}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {0, 1}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
				{0, nil, 0, 1},
			},
		},
		{
			description:  "null handling multi column, nulls on right",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 0}, {0, 1}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {0, nil}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
				{0, 1, 0, nil},
			},
		},
		{
			description:  "desc test",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{4}, {3}, {2}, {1}},
			rightTuples:  colexectestutils.Tuples{{4}, {2}, {1}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{4, 4}, {2, 2}, {1, 1}},

			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
		},
		{
			description:  "desc nulls test",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{4}, {3}, {nil}, {1}},
			rightTuples:  colexectestutils.Tuples{{4}, {nil}, {2}, {1}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{4, 4}, {1, 1}},

			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
		},
		{
			description:  "desc nulls test end on 0",
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{9}, {9}, {8}, {0}, {nil}},
			rightTuples:  colexectestutils.Tuples{{9}, {9}, {8}, {0}, {nil}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{9, 9}, {9, 9}, {9, 9}, {9, 9}, {8, 8}, {0, 0}},

			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
		},
		{
			description:  "non-equality columns with nulls",
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, nil}, {2, 2}, {2, 2}, {3, nil}, {4, nil}},
			rightTuples:  colexectestutils.Tuples{{1, 1}, {2, nil}, {2, nil}, {3, nil}, {4, 4}, {4, 4}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, nil, 1, 1}, {2, 2, 2, nil}, {2, 2, 2, nil}, {2, 2, 2, nil}, {2, 2, 2, nil}, {3, nil, 3, nil}, {4, nil, 4, 4}, {4, nil, 4, 4}},
		},
		{
			description:  "basic LEFT OUTER JOIN test, L and R exhausted at the same time",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:  colexectestutils.Tuples{{0}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, nil}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
		},
		{
			description:  "basic LEFT OUTER JOIN test, R exhausted first",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, nil}, {1, nil}, {3, 3}, {5, nil}, {6, nil}, {7, nil}},
		},
		{
			description:  "basic LEFT OUTER JOIN test, L exhausted first",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{3, 3}, {5, nil}, {6, 6}, {7, nil}},
		},
		{
			description:  "multi output column LEFT OUTER JOIN test with nulls",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, 10, 1, nil}, {2, 20, nil, nil}, {3, nil, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:  "null in equality column LEFT OUTER JOIN",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:  colexectestutils.Tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {3, 3, 3}},
		},
		{
			description:  "multi equality column LEFT OUTER JOIN test with nulls",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{nil, nil, nil, nil}, {nil, 10, nil, nil}, {1, nil, nil, nil}, {1, 10, nil, nil}, {2, 20, 2, 20}, {4, 40, nil, nil}},
		},
		{
			description:  "multi equality column (long runs on left) LEFT OUTER JOIN test with nulls",
			joinType:     descpb.LeftOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:  colexectestutils.Tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{1, 9, nil, nil}, {1, 10, nil, nil}, {1, 10, nil, nil}, {1, 11, 1, 11}, {1, 11, 1, 11}, {2, 20, nil, nil}, {2, 20, nil, nil}, {2, 21, 2, 21}, {2, 22, nil, nil}, {2, 22, nil, nil}},
		},
		{
			description:     "3 equality column LEFT OUTER JOIN test with nulls DESC ordering",
			joinType:        descpb.LeftOuterJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        colexectestutils.Tuples{{2, 3, 1, nil, nil, nil}, {2, nil, 1, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}},
		},
		{
			description:     "3 equality column LEFT OUTER JOIN test with nulls mixed ordering",
			joinType:        descpb.LeftOuterJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        colexectestutils.Tuples{{2, 3, 1, nil, nil, nil}, {2, nil, 1, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}},
		},
		{
			description:     "single column DESC with nulls on the left LEFT OUTER JOIN",
			joinType:        descpb.LeftOuterJoin,
			leftTypes:       []*types.T{types.Int},
			rightTypes:      []*types.T{types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     colexectestutils.Tuples{{1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        colexectestutils.Tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
		},
		{
			description:  "basic RIGHT OUTER JOIN test, L and R exhausted at the same time",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{-1}, {2}, {3}, {4}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
		},
		{
			description:  "basic RIGHT OUTER JOIN test, R exhausted first",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, 2}, {3, 3}, {nil, 4}},
		},
		{
			description:  "basic RIGHT OUTER JOIN test, L exhausted first",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, 2}, {3, 3}, {nil, 4}, {6, 6}, {nil, 8}, {nil, 9}},
		},
		{
			description:  "multi output column RIGHT OUTER JOIN test with nulls",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			rightTuples:  colexectestutils.Tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, nil, 1, 10}, {nil, nil, 2, 20}, {3, 13, 3, nil}, {4, 14, 4, 40}},
		},
		{
			description:  "null in equality column RIGHT OUTER JOIN",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			rightTuples:  colexectestutils.Tuples{{nil}, {nil}, {1}, {3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {3, 3, 3}},
		},
		{
			description:  "multi equality column RIGHT OUTER JOIN test with nulls",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{nil, nil, nil, nil}, {nil, nil, nil, 10}, {nil, nil, 1, nil}, {nil, nil, 1, 10}, {2, 20, 2, 20}, {nil, nil, 4, 40}},
		},
		{
			description:  "multi equality column (long runs on right) RIGHT OUTER JOIN test with nulls",
			joinType:     descpb.RightOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			rightTuples:  colexectestutils.Tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{nil, nil, 1, 9}, {nil, nil, 1, 10}, {nil, nil, 1, 10}, {1, 11, 1, 11}, {1, 11, 1, 11}, {nil, nil, 2, 20}, {nil, nil, 2, 20}, {2, 21, 2, 21}, {nil, nil, 2, 22}, {nil, nil, 2, 22}},
		},
		{
			description:     "3 equality column RIGHT OUTER JOIN test with nulls DESC ordering",
			joinType:        descpb.RightOuterJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        colexectestutils.Tuples{{nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "3 equality column RIGHT OUTER JOIN test with nulls mixed ordering",
			joinType:        descpb.RightOuterJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        colexectestutils.Tuples{{nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "single column DESC with nulls on the right RIGHT OUTER JOIN",
			joinType:        descpb.RightOuterJoin,
			leftTypes:       []*types.T{types.Int},
			rightTypes:      []*types.T{types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{1}},
			rightTuples:     colexectestutils.Tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        colexectestutils.Tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
		},
		{
			description:  "basic FULL OUTER JOIN test, L and R exhausted at the same time",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{-1}, {2}, {3}, {4}, {4}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{-1, nil}, {nil, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
		},
		{
			description:  "basic FULL OUTER JOIN test, R exhausted first",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, nil}, {1, nil}, {nil, 2}, {3, 3}, {nil, 4}, {5, nil}, {6, nil}, {7, nil}},
		},
		{
			description:  "basic FULL OUTER JOIN test, L exhausted first",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, 2}, {3, 3}, {nil, 4}, {5, nil}, {6, 6}, {7, nil}, {nil, 8}, {nil, 9}},
		},
		{
			description:  "multi output column FULL OUTER JOIN test with nulls",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			rightTuples:  colexectestutils.Tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, nil, 1, 10}, {nil, nil, 2, 20}, {3, 13, 3, nil}, {4, 14, 4, 40}},
		},
		{
			description:  "null in equality column FULL OUTER JOIN",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			rightTuples:  colexectestutils.Tuples{{nil}, {nil}, {1}, {3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, 1, nil}, {nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {2, 2, nil}, {3, 3, 3}},
		},
		{
			description:  "multi equality column FULL OUTER JOIN test with nulls",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{nil, nil, nil, nil}, {nil, nil, nil, nil}, {nil, 10, nil, nil}, {nil, nil, nil, 10}, {1, nil, nil, nil}, {1, nil, nil, nil}, {nil, nil, 1, nil}, {nil, nil, 1, 10}, {2, 20, 2, 20}, {3, 30, nil, nil}, {nil, nil, 4, 40}},
		},
		{
			description:  "multi equality column (long runs on right) FULL OUTER JOIN test with nulls",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			rightTuples:  colexectestutils.Tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{1, 8, nil, nil}, {nil, nil, 1, 9}, {nil, nil, 1, 10}, {nil, nil, 1, 10}, {1, 11, 1, 11}, {1, 11, 1, 11}, {nil, nil, 2, 20}, {nil, nil, 2, 20}, {2, 21, 2, 21}, {nil, nil, 2, 22}, {nil, nil, 2, 22}, {2, 23, nil, nil}},
		},
		{
			description:     "3 equality column FULL OUTER JOIN test with nulls DESC ordering",
			joinType:        descpb.FullOuterJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        colexectestutils.Tuples{{4, 3, 3, nil, nil, nil}, {nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, 2, nil, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "3 equality column FULL OUTER JOIN test with nulls mixed ordering",
			joinType:        descpb.FullOuterJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        colexectestutils.Tuples{{4, 3, 3, nil, nil, nil}, {nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, 2, nil, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "single column DESC with nulls on the right FULL OUTER JOIN",
			joinType:        descpb.FullOuterJoin,
			leftTypes:       []*types.T{types.Int},
			rightTypes:      []*types.T{types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{1}},
			rightTuples:     colexectestutils.Tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        colexectestutils.Tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
		},
		{
			description:  "FULL OUTER JOIN test with nulls and Bytes",
			joinType:     descpb.FullOuterJoin,
			leftTypes:    []*types.T{types.Int, types.Bytes},
			rightTypes:   []*types.T{types.Int, types.Bytes},
			leftTuples:   colexectestutils.Tuples{{nil, "0"}, {1, "10"}, {2, "20"}, {3, nil}, {4, "40"}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, "13"}, {4, nil}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil, "0", nil}, {1, "10", nil}, {2, "20", nil}, {3, nil, "13"}, {4, "40", nil}},
		},
		{
			description:  "basic LEFT SEMI JOIN test, L and R exhausted at the same time",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:  colexectestutils.Tuples{{-1}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{2}, {3}, {4}, {4}},
		},
		{
			description:  "basic LEFT SEMI JOIN test, R exhausted first",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{3}},
		},
		{
			description:  "basic LEFT SEMI JOIN test, L exhausted first",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {3}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{3}, {6}},
		},
		{
			description:  "multi output column LEFT SEMI JOIN test with nulls",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1, 10}, {3, nil}, {4, 40}},
		},
		{
			description:  "null in equality column LEFT SEMI JOIN",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:  colexectestutils.Tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {3}},
		},
		{
			description:  "multi equality column LEFT SEMI JOIN test with nulls",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{2, 20}},
		},
		{
			description:  "multi equality column (long runs on left) LEFT SEMI JOIN test with nulls",
			joinType:     descpb.LeftSemiJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {1, 11}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:  colexectestutils.Tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{1, 11}, {1, 11}, {1, 11}, {2, 21}},
		},
		{
			description:     "3 equality column LEFT SEMI JOIN test with nulls DESC ordering",
			joinType:        descpb.LeftSemiJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        colexectestutils.Tuples{},
			// The expected output here is empty, so will it be during the all nulls
			// injection, so we want to skip that.
			skipAllNullsInjection: true,
		},
		{
			description:     "3 equality column LEFT SEMI JOIN test with nulls mixed ordering",
			joinType:        descpb.LeftSemiJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        colexectestutils.Tuples{},
			// The expected output here is empty, so will it be during the all nulls
			// injection, so we want to skip that.
			skipAllNullsInjection: true,
		},
		{
			description:     "single column DESC with nulls on the left LEFT SEMI JOIN",
			joinType:        descpb.LeftSemiJoin,
			leftTypes:       []*types.T{types.Int},
			rightTypes:      []*types.T{types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     colexectestutils.Tuples{{1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        colexectestutils.Tuples{{1}, {1}, {1}},
		},
		{
			description:  "basic LEFT ANTI JOIN test, L and R exhausted at the same time",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:  colexectestutils.Tuples{{-1}, {2}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {3}},
		},
		{
			description:  "basic LEFT ANTI JOIN test, R exhausted first",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {1}, {5}, {6}, {7}},
		},
		{
			description:  "basic LEFT ANTI JOIN test, L exhausted first",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{3}, {5}, {6}, {7}},
			rightTuples:  colexectestutils.Tuples{{2}, {3}, {3}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{5}, {7}},
		},
		{
			description:  "multi output column LEFT ANTI JOIN test with nulls",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{2, 20}},
		},
		{
			description:  "null in equality column LEFT ANTI JOIN",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:  colexectestutils.Tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil}, {nil}},
		},
		{
			description:  "multi equality column LEFT ANTI JOIN test with nulls",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {4, 40}},
		},
		{
			description:  "multi equality column (long runs on left) LEFT ANTI JOIN test with nulls",
			joinType:     descpb.LeftAntiJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {1, 11}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:  colexectestutils.Tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     colexectestutils.Tuples{{1, 9}, {1, 10}, {1, 10}, {2, 20}, {2, 20}, {2, 22}, {2, 22}},
		},
		{
			description:     "3 equality column LEFT ANTI JOIN test with nulls DESC ordering",
			joinType:        descpb.LeftAntiJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
		},
		{
			description:     "3 equality column LEFT ANTI JOIN test with nulls mixed ordering",
			joinType:        descpb.LeftAntiJoin,
			leftTypes:       []*types.T{types.Int, types.Int, types.Int},
			rightTypes:      []*types.T{types.Int, types.Int, types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     colexectestutils.Tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        colexectestutils.Tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
		},
		{
			description:     "single column DESC with nulls on the left LEFT ANTI JOIN",
			joinType:        descpb.LeftAntiJoin,
			leftTypes:       []*types.T{types.Int},
			rightTypes:      []*types.T{types.Int},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      colexectestutils.Tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     colexectestutils.Tuples{{1}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        colexectestutils.Tuples{{nil}, {nil}, {nil}},
		},
		{
			description:  "INNER JOIN test with ON expression (filter only on left)",
			joinType:     descpb.InnerJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@1 < 4"},
			expected:     colexectestutils.Tuples{{1, 10}, {3, nil}},
		},
		{
			description:  "INNER JOIN test with ON expression (filter only on right)",
			joinType:     descpb.InnerJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@4 < 14"},
			expected:     colexectestutils.Tuples{{3, nil}},
		},
		{
			description:  "INNER JOIN test with ON expression (filter on both)",
			joinType:     descpb.InnerJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  colexectestutils.Tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@2 + @3 < 50"},
			expected:     colexectestutils.Tuples{{1, 10}, {4, 40}},
		},
		{
			description:  "INTERSECT ALL join basic",
			joinType:     descpb.IntersectAllJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {2}, {2}, {3}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {2}, {3}, {3}, {3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {2}, {2}, {3}},
		},
		{
			description:  "INTERSECT ALL join with mixed ordering with NULLs",
			joinType:     descpb.IntersectAllJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{4, nil}, {4, 1}, {1, nil}, {1, 2}, {0, 2}, {0, 3}, {nil, 1}, {nil, 2}, {nil, 2}, {nil, 3}},
			rightTuples:  colexectestutils.Tuples{{3, 2}, {2, 1}, {2, 2}, {2, 3}, {1, nil}, {1, 1}, {1, 1}, {0, 1}, {0, 2}, {nil, 2}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			leftDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC,
			},
			rightEqCols: []uint32{0, 1},
			rightDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC,
			},
			expected: colexectestutils.Tuples{{1, nil}, {0, 2}, {nil, 2}},
		},
		{
			description:  "INTERSECT ALL join with mixed ordering with NULLs on the left",
			joinType:     descpb.IntersectAllJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 3}, {nil, nil}, {9, 6}, {9, 0}, {9, nil}},
			rightTuples:  colexectestutils.Tuples{{0, 5}, {0, 4}, {8, 8}, {8, 6}, {9, 0}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			leftDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC,
			},
			rightEqCols: []uint32{0, 1},
			rightDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC,
			},
			expected: colexectestutils.Tuples{{9, 0}},
		},
		{
			description:  "INTERSECT ALL join on booleans",
			joinType:     descpb.IntersectAllJoin,
			leftTypes:    []*types.T{types.Bool},
			rightTypes:   []*types.T{types.Bool},
			leftTuples:   colexectestutils.Tuples{{nil}, {nil}, {false}, {false}, {true}, {true}, {true}},
			rightTuples:  colexectestutils.Tuples{{nil}, {false}, {false}, {false}, {false}, {true}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil}, {false}, {false}, {true}},
		},
		{
			description:  "EXCEPT ALL join basic",
			joinType:     descpb.ExceptAllJoin,
			leftTypes:    []*types.T{types.Int},
			rightTypes:   []*types.T{types.Int},
			leftTuples:   colexectestutils.Tuples{{1}, {1}, {2}, {2}, {2}, {3}, {3}},
			rightTuples:  colexectestutils.Tuples{{1}, {2}, {3}, {3}, {3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{1}, {2}, {2}},
		},
		{
			description:  "EXCEPT ALL join with mixed ordering with NULLs",
			joinType:     descpb.ExceptAllJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{4, nil}, {4, 1}, {1, nil}, {1, 2}, {0, 2}, {0, 3}, {nil, 1}, {nil, 2}, {nil, 2}, {nil, 3}},
			rightTuples:  colexectestutils.Tuples{{3, 2}, {2, 1}, {2, 2}, {2, 3}, {1, nil}, {1, 1}, {1, 1}, {0, 1}, {0, 2}, {nil, 2}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			leftDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC,
			},
			rightEqCols: []uint32{0, 1},
			rightDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC,
			},
			expected: colexectestutils.Tuples{{4, nil}, {4, 1}, {1, 2}, {0, 3}, {nil, 1}, {nil, 2}, {nil, 3}},
		},
		{
			description:  "EXCEPT ALL join with mixed ordering with NULLs on the left",
			joinType:     descpb.ExceptAllJoin,
			leftTypes:    []*types.T{types.Int, types.Int},
			rightTypes:   []*types.T{types.Int, types.Int},
			leftTuples:   colexectestutils.Tuples{{nil, 3}, {nil, nil}, {9, 6}, {9, 0}, {9, nil}},
			rightTuples:  colexectestutils.Tuples{{0, 5}, {0, 4}, {8, 8}, {8, 6}, {9, 0}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			leftDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC,
			},
			rightEqCols: []uint32{0, 1},
			rightDirections: []execinfrapb.Ordering_Column_Direction{
				execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC,
			},
			expected: colexectestutils.Tuples{{nil, 3}, {nil, nil}, {9, 6}, {9, nil}},
		},
		{
			description:  "EXCEPT ALL join on booleans",
			joinType:     descpb.ExceptAllJoin,
			leftTypes:    []*types.T{types.Bool},
			rightTypes:   []*types.T{types.Bool},
			leftTuples:   colexectestutils.Tuples{{nil}, {nil}, {false}, {false}, {true}, {true}, {true}},
			rightTuples:  colexectestutils.Tuples{{nil}, {false}, {false}, {false}, {false}, {true}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     colexectestutils.Tuples{{nil}, {true}, {true}},
		},
		{
			description: "FULL OUTER join on mixed-type equality columns",
			joinType:    descpb.FullOuterJoin,
			leftTuples: colexectestutils.Tuples{
				{8398534516657654136},
				{932352552525192296},
			},
			leftTypes:   []*types.T{types.Int},
			leftOutCols: []uint32{0},
			leftEqCols:  []uint32{0},
			rightTuples: colexectestutils.Tuples{
				{-20041},
				{23918},
			},
			rightTypes:   []*types.T{types.Int2},
			rightOutCols: []uint32{0},
			rightEqCols:  []uint32{0},
			expected: colexectestutils.Tuples{
				{8398534516657654136, nil},
				{932352552525192296, nil},
				{nil, -20041},
				{nil, 23918},
			},
		},
		{
			description:       "LEFT ANTI join when right eq cols are key",
			joinType:          descpb.LeftAntiJoin,
			leftTypes:         []*types.T{types.Int},
			rightTypes:        []*types.T{types.Int},
			leftTuples:        colexectestutils.Tuples{{0}, {0}, {1}, {2}},
			rightTuples:       colexectestutils.Tuples{{0}, {2}},
			leftEqCols:        []uint32{0},
			rightEqCols:       []uint32{0},
			leftOutCols:       []uint32{0},
			rightOutCols:      []uint32{},
			rightEqColsAreKey: true,
			expected:          colexectestutils.Tuples{{1}},
		},
		{
			description:       "INTERSECT ALL join when right eq cols are key",
			joinType:          descpb.IntersectAllJoin,
			leftTypes:         []*types.T{types.Int},
			rightTypes:        []*types.T{types.Int},
			leftTuples:        colexectestutils.Tuples{{1}, {1}, {2}, {2}, {2}, {4}, {4}},
			rightTuples:       colexectestutils.Tuples{{1}, {2}, {3}},
			leftEqCols:        []uint32{0},
			rightEqCols:       []uint32{0},
			leftOutCols:       []uint32{0},
			rightEqColsAreKey: true,
			expected:          colexectestutils.Tuples{{1}, {2}},
		},
		{
			description:       "EXCEPT ALL join when right eq cols are key",
			joinType:          descpb.ExceptAllJoin,
			leftTypes:         []*types.T{types.Int},
			rightTypes:        []*types.T{types.Int},
			leftTuples:        colexectestutils.Tuples{{1}, {1}, {2}, {2}, {2}, {3}, {3}},
			rightTuples:       colexectestutils.Tuples{{1}, {2}, {3}},
			leftEqCols:        []uint32{0},
			rightEqCols:       []uint32{0},
			leftOutCols:       []uint32{0},
			rightEqColsAreKey: true,
			expected:          colexectestutils.Tuples{{1}, {2}, {2}, {3}},
		},
		{
			description: "LEFT ANTI join with mixed types",
			joinType:    descpb.LeftAntiJoin,
			leftTypes:   []*types.T{types.Int, types.Int2},
			rightTypes:  []*types.T{types.Int4, types.Int},
			leftTuples:  colexectestutils.Tuples{{0, int16(0)}, {1, int16(0)}, {1, int16(1)}, {1, int16(2)}},
			rightTuples: colexectestutils.Tuples{{int32(0), 0}, {int32(0), 1}, {int32(1), 1}},
			leftEqCols:  []uint32{0, 1},
			rightEqCols: []uint32{0, 1},
			leftOutCols: []uint32{0, 1},
			expected:    colexectestutils.Tuples{{1, int16(0)}, {1, int16(2)}},
		},
		{
			description:       "LEFT SEMI join when eq cols are key on both sides",
			joinType:          descpb.LeftSemiJoin,
			leftTypes:         []*types.T{types.Int},
			rightTypes:        []*types.T{types.Int},
			leftTuples:        colexectestutils.Tuples{{1}, {2}, {4}},
			rightTuples:       colexectestutils.Tuples{{0}, {2}, {3}, {4}},
			leftEqCols:        []uint32{0},
			rightEqCols:       []uint32{0},
			leftOutCols:       []uint32{0},
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,
			expected:          colexectestutils.Tuples{{2}, {4}},
		},
		{
			description:       "LEFT ANTI join when eq cols are key on both sides",
			joinType:          descpb.LeftAntiJoin,
			leftTypes:         []*types.T{types.Int},
			rightTypes:        []*types.T{types.Int},
			leftTuples:        colexectestutils.Tuples{{1}, {2}, {4}},
			rightTuples:       colexectestutils.Tuples{{0}, {2}, {3}, {4}},
			leftEqCols:        []uint32{0},
			rightEqCols:       []uint32{0},
			leftOutCols:       []uint32{0},
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,
			expected:          colexectestutils.Tuples{{1}},
		},
	}
	return withMirrors(mjTestCases)
}
func TestMergeJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
		DiskMonitor: testDiskMonitor,
	}
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	var (
		accounts []*mon.BoundAccount
		monitors []*mon.BytesMonitor
	)
	for _, tc := range getMJTestCases() {
		for _, tc := range tc.mutateTypes() {
			tc.init()
			verifier := colexectestutils.OrderedVerifier
			if tc.joinType == descpb.FullOuterJoin {
				// FULL OUTER JOIN doesn't guarantee any ordering on its output
				// (since it is ambiguous), so we're comparing the outputs as
				// sets.
				verifier = colexectestutils.UnorderedVerifier
			}
			var runner colexectestutils.TestRunner
			if tc.skipAllNullsInjection {
				// We're omitting all nulls injection test. See comments for each such
				// test case.
				runner = colexectestutils.RunTestsWithoutAllNullsInjection
			} else {
				runner = colexectestutils.RunTestsWithTyps
			}
			// We test all cases with the default memory limit (regular scenario) and a
			// limit of 1 byte (to force the buffered groups to spill to disk).
			for _, memoryLimit := range []int64{1, execinfra.DefaultMemoryLimit} {
				log.Infof(context.Background(), "MemoryLimit=%s/%s", humanizeutil.IBytes(memoryLimit), tc.description)
				runner(t, testAllocator, []colexectestutils.Tuples{tc.leftTuples, tc.rightTuples},
					[][]*types.T{tc.leftTypes, tc.rightTypes},
					tc.expected, verifier,
					func(sources []colexecop.Operator) (colexecop.Operator, error) {
						spec := createSpecForMergeJoiner(tc)
						args := &colexecargs.NewColOperatorArgs{
							Spec:                spec,
							Inputs:              colexectestutils.MakeInputs(sources),
							StreamingMemAccount: testMemAcc,
							DiskQueueCfg:        queueCfg,
							FDSemaphore:         colexecop.NewTestingSemaphore(mjFDLimit),
						}
						flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = memoryLimit
						result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
						if err != nil {
							return nil, err
						}
						accounts = append(accounts, result.OpAccounts...)
						monitors = append(monitors, result.OpMonitors...)
						return result.Root, nil
					})
			}
		}
	}
	for _, acc := range accounts {
		acc.Close(ctx)
	}
	for _, mon := range monitors {
		mon.Stop(ctx)
	}
}

// Merge joiner will be using two spillingQueues, and each of them will use
// 2 file descriptors.
const mjFDLimit = 4

// TestFullOuterMergeJoinWithMaximumNumberOfGroups will create two input
// sources such that the left one contains rows with even numbers 0, 2, 4, ...
// while the right contains one rows with odd numbers 1, 3, 5, ... The test
// will perform FULL OUTER JOIN. Such setup will create the maximum number of
// groups per batch.
func TestFullOuterMergeJoinWithMaximumNumberOfGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	nTuples := coldata.BatchSize() * 4
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	typs := []*types.T{types.Int}
	colsLeft := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
	colsRight := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
	groupsLeft := colsLeft[0].Int64()
	groupsRight := colsRight[0].Int64()
	for i := range groupsLeft {
		groupsLeft[i] = int64(i * 2)
		groupsRight[i] = int64(i*2 + 1)
	}
	leftSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, colsLeft, nTuples)
	rightSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, colsRight, nTuples)
	a, err := colexecjoin.NewMergeJoinOp(
		testAllocator, execinfra.DefaultMemoryLimit, queueCfg,
		colexecop.NewTestingSemaphore(mjFDLimit), descpb.FullOuterJoin,
		leftSource, rightSource, typs, typs,
		[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
		[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
		testDiskAcc,
	)
	if err != nil {
		t.Fatal("error in merge join op constructor", err)
	}
	a.Init(ctx)
	i, count, expVal := 0, 0, int64(0)
	for b := a.Next(); b.Length() != 0; b = a.Next() {
		count += b.Length()
		leftOutCol := b.ColVec(0).Int64()
		leftNulls := b.ColVec(0).Nulls()
		rightOutCol := b.ColVec(1).Int64()
		rightNulls := b.ColVec(1).Nulls()
		for j := 0; j < b.Length(); j++ {
			leftVal := leftOutCol[j]
			leftNull := leftNulls.NullAt(j)
			rightVal := rightOutCol[j]
			rightNull := rightNulls.NullAt(j)
			if expVal%2 == 0 {
				// It is an even-numbered row, so the left value should contain
				// expVal and the right value should be NULL.
				if leftVal != expVal || leftNull || !rightNull {
					t.Fatalf("found left = %d, left NULL? = %t, right NULL? = %t, "+
						"expected left = %d, left NULL? = false, right NULL? = true, idx %d of batch %d",
						leftVal, leftNull, rightNull, expVal, j, i)
				}
			} else {
				// It is an odd-numbered row, so the right value should contain
				// expVal and the left value should be NULL.
				if rightVal != expVal || rightNull || !leftNull {
					t.Fatalf("found right = %d, right NULL? = %t, left NULL? = %t, "+
						"expected right = %d, right NULL? = false, left NULL? = true, idx %d of batch %d",
						rightVal, rightNull, leftNull, expVal, j, i)
				}
			}
			expVal++
		}
		i++
	}
	if count != 2*nTuples {
		t.Fatalf("found count %d, expected count %d", count, 2*nTuples)
	}
}

// TestMergeJoinerMultiBatch creates one long input of a 1:1 join, and keeps
// track of the expected output to make sure the join output is batched
// correctly.
func TestMergeJoinerMultiBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	for _, numInputBatches := range []int{1, 2, 16} {
		t.Run(fmt.Sprintf("numInputBatches=%d", numInputBatches),
			func(t *testing.T) {
				nTuples := coldata.BatchSize() * numInputBatches
				typs := []*types.T{types.Int}
				cols := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
				groups := cols[0].Int64()
				for i := range groups {
					groups[i] = int64(i)
				}
				leftSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
				rightSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
				a, err := colexecjoin.NewMergeJoinOp(
					testAllocator, execinfra.DefaultMemoryLimit,
					queueCfg, colexecop.NewTestingSemaphore(mjFDLimit), descpb.InnerJoin,
					leftSource, rightSource, typs, typs,
					[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
					[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
					testDiskAcc,
				)
				if err != nil {
					t.Fatal("error in merge join op constructor", err)
				}
				a.Init(ctx)
				i := 0
				count := 0
				// Keep track of the last comparison value.
				expVal := int64(0)
				for b := a.Next(); b.Length() != 0; b = a.Next() {
					count += b.Length()
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

// TestMergeJoinerMultiBatchRuns creates one long input of a n:n join, and
// keeps track of the expected count to make sure the join output is batched
// correctly.
func TestMergeJoinerMultiBatchRuns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	for _, groupSize := range []int{coldata.BatchSize() / 8, coldata.BatchSize() / 4, coldata.BatchSize() / 2} {
		if groupSize == 0 {
			// We might be varying coldata.BatchSize() so that when it is divided by
			// 4, groupSize is 0. We want to skip such configuration.
			continue
		}
		for _, numInputBatches := range []int{1, 2, 16} {
			t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(t *testing.T) {
					nTuples := coldata.BatchSize() * numInputBatches
					// There will be nTuples/groupSize "full" groups - i.e. groups of
					// groupSize. Each of these "full" groups will produce groupSize^2
					// tuples. The last group might be not full and will consist of
					// nTuples % groupSize tuples. That group will produce
					// lastGroupSize^2 tuples.
					// Note that the math will still be correct in case when nTuples is
					// divisible by groupSize - all the groups will be full and "last"
					// group will be of size 0.
					lastGroupSize := nTuples % groupSize
					expCount := nTuples/groupSize*(groupSize*groupSize) + lastGroupSize*lastGroupSize
					typs := []*types.T{types.Int, types.Int}
					cols := []coldata.Vec{
						testAllocator.NewMemColumn(typs[0], nTuples),
						testAllocator.NewMemColumn(typs[1], nTuples),
					}
					for i := range cols[0].Int64() {
						cols[0].Int64()[i] = int64(i / groupSize)
						cols[1].Int64()[i] = int64(i / groupSize)
					}
					leftSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
					rightSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
					a, err := colexecjoin.NewMergeJoinOp(
						testAllocator, execinfra.DefaultMemoryLimit,
						queueCfg, colexecop.NewTestingSemaphore(mjFDLimit), descpb.InnerJoin,
						leftSource, rightSource, typs, typs,
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_ASC}},
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_ASC}},
						testDiskAcc,
					)
					if err != nil {
						t.Fatal("error in merge join op constructor", err)
					}
					a.Init(ctx)
					i := 0
					count := 0
					// Keep track of the last comparison value.
					lastVal := int64(0)
					for b := a.Next(); b.Length() != 0; b = a.Next() {
						count += b.Length()
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

					if count != expCount {
						t.Fatalf("found count %d, expected count %d",
							count, expCount)
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
	nTuples int, maxRunLength int64, skipValues bool, randomIncrement int64,
) ([]coldata.Vec, []coldata.Vec, []expectedGroup) {
	rng, _ := randutil.NewPseudoRand()
	lCols := []coldata.Vec{testAllocator.NewMemColumn(types.Int, nTuples)}
	lCol := lCols[0].Int64()
	rCols := []coldata.Vec{testAllocator.NewMemColumn(types.Int, nTuples)}
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()
	for _, numInputBatches := range []int{1, 2, 16, 256} {
		for _, maxRunLength := range []int64{2, 3, 100} {
			for _, skipValues := range []bool{false, true} {
				for _, randomIncrement := range []int64{0, 1} {
					log.Infof(ctx, "numInputBatches=%d/maxRunLength=%d/skipValues=%t/randomIncrement=%d", numInputBatches, maxRunLength, skipValues, randomIncrement)
					nTuples := coldata.BatchSize() * numInputBatches
					typs := []*types.T{types.Int}
					lCols, rCols, exp := newBatchesOfRandIntRows(nTuples, maxRunLength, skipValues, randomIncrement)
					leftSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, lCols, nTuples)
					rightSource := colexectestutils.NewChunkingBatchSource(testAllocator, typs, rCols, nTuples)

					a, err := colexecjoin.NewMergeJoinOp(
						testAllocator, execinfra.DefaultMemoryLimit,
						queueCfg, colexecop.NewTestingSemaphore(mjFDLimit), descpb.InnerJoin,
						leftSource, rightSource, typs, typs,
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
						testDiskAcc,
					)
					if err != nil {
						t.Fatal("error in merge join op constructor", err)
					}
					a.Init(ctx)
					i := 0
					count := 0
					cpIdx := 0
					for b := a.Next(); b.Length() != 0; b = a.Next() {
						count += b.Length()
						outCol := b.ColVec(0).Int64()
						for j := 0; j < b.Length(); j++ {
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
				}
			}
		}
	}
}
