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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type mjTestCase struct {
	description           string
	joinType              sqlbase.JoinType
	leftTuples            []tuple
	leftTypes             []coltypes.T
	leftOutCols           []uint32
	leftEqCols            []uint32
	leftDirections        []execinfrapb.Ordering_Column_Direction
	rightTuples           []tuple
	rightTypes            []coltypes.T
	rightOutCols          []uint32
	rightEqCols           []uint32
	rightDirections       []execinfrapb.Ordering_Column_Direction
	expected              []tuple
	outputBatchSize       uint16
	skipAllNullsInjection bool
	onExpr                execinfrapb.Expression
}

func (tc *mjTestCase) Init() {
	if tc.outputBatchSize == 0 {
		tc.outputBatchSize = coldata.BatchSize()
	}

	if len(tc.leftDirections) == 0 {
		tc.leftDirections = make([]execinfrapb.Ordering_Column_Direction, len(tc.leftTypes))
		for i := range tc.leftDirections {
			tc.leftDirections[i] = execinfrapb.Ordering_Column_ASC
		}
	}

	if len(tc.rightDirections) == 0 {
		tc.rightDirections = make([]execinfrapb.Ordering_Column_Direction, len(tc.rightTypes))
		for i := range tc.rightDirections {
			tc.rightDirections[i] = execinfrapb.Ordering_Column_ASC
		}
	}
}

func createSpecForMergeJoiner(tc mjTestCase) *execinfrapb.ProcessorSpec {
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
	for _, outCol := range tc.rightOutCols {
		projection = append(projection, rColOffset+outCol)
	}
	return &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{
			{ColumnTypes: typeconv.ToColumnTypes(tc.leftTypes)},
			{ColumnTypes: typeconv.ToColumnTypes(tc.rightTypes)},
		},
		Core: execinfrapb.ProcessorCoreUnion{
			MergeJoiner: mjSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: projection,
		},
	}
}

func TestMergeJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: st},
	}

	tcs := []mjTestCase{
		{
			description:  "basic test",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, no out cols",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{}, {}, {}, {}},
		},
		{
			description:  "basic test, out col on left",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, out col on right",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, L missing",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {3}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {3}, {4}},
		},
		{
			description:  "basic test, R missing",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {3}, {4}},
		},
		{
			description:  "basic test, L duplicate",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, R duplicate",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, R duplicate 2",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}},
			rightTuples:  tuples{{1}, {1}, {2}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {2}},
		},
		{
			description:  "basic test, L+R duplicates",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {1}, {1}, {2}, {3}, {4}},
		},
		{
			description:  "basic test, L+R duplicate, multiple runs",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {2}, {2}, {3}, {4}},
			rightTuples:  tuples{{1}, {1}, {2}, {3}, {4}},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {2}, {2}, {2}, {3}, {4}},
		},
		{
			description:  "cross product test, batch size = col.BatchSize()",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {1}, {1}},
			rightTuples:  tuples{{1}, {1}, {1}, {1}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
		},
		{
			description:     "cross product test, batch size = 4 (small even)",
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			outputBatchSize: 4,
		},
		{
			description:     "cross product test, batch size = 3 (small odd)",
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			outputBatchSize: 3,
		},
		{
			description:     "cross product test, batch size = 1 (unit)",
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftTuples:      tuples{{1}, {1}, {1}, {1}},
			rightTuples:     tuples{{1}, {1}, {1}, {1}},
			leftOutCols:     []uint32{},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			outputBatchSize: 1,
		},
		{
			description:  "multi output column test, basic",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:     "multi output column test, batch size = 1",
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			outputBatchSize: 1,
		},
		{
			description:  "multi output column test, test output coldata projection",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
		},
		{
			description:  "multi output column test, test output coldata projection",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{10, 11}, {20, 12}, {30, 13}, {40, 14}},
		},
		{
			description:  "multi output column test, L run",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:     "multi output column test, L run, batch size = 1",
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {2, 21}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {2, 20, 2, 12}, {2, 21, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			outputBatchSize: 1,
		},
		{
			description:  "multi output column test, R run",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:  tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:     "multi output column test, R run, batch size = 1",
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:      tuples{{1, 10}, {2, 20}, {3, 30}, {4, 40}},
			rightTuples:     tuples{{1, 11}, {1, 111}, {2, 12}, {3, 13}, {4, 14}},
			leftOutCols:     []uint32{0, 1},
			rightOutCols:    []uint32{0, 1},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 10, 1, 11}, {1, 10, 1, 111}, {2, 20, 2, 12}, {3, 30, 3, 13}, {4, 40, 4, 14}},
			outputBatchSize: 1,
		},
		{
			description:  "logic test",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{-1, -1}, {0, 4}, {2, 1}, {3, 4}, {5, 4}},
			rightTuples:  tuples{{0, 5}, {1, 3}, {3, 2}, {4, 6}},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{4, 5}, {4, 2}},
		},
		{
			description:  "multi output column test, batch size = 1 and runs (to test saved output), reordered out columns",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
			outputBatchSize: 1,
		},
		{
			description:  "multi output column test, batch size = 1 and runs (to test saved output), reordered out columns that dont start at 0",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
			outputBatchSize: 1,
		},
		{
			description:  "equality column is correctly indexed",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
		},
		{
			description:  "multi column equality basic test",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
		},
		{
			description:  "multi column equality runs",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
		},
		{
			description:  "multi column non-consecutive equality cols",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 123, 1}, {1, 234, 10}},
			rightTuples:  tuples{{1, 1, 345}, {1, 10, 456}},
			leftOutCols:  []uint32{0, 2, 1},
			rightOutCols: []uint32{0, 2, 1},
			leftEqCols:   []uint32{0, 2},
			rightEqCols:  []uint32{0, 1},
			expected: tuples{
				{1, 1, 123, 1, 345, 1},
				{1, 10, 234, 1, 456, 10},
			},
		},
		{
			description:  "multi column equality: new batch ends run",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
		},
		{
			description:  "multi column equality: reordered eq columns",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
		},
		{
			description:  "cross batch, distinct group",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
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
		},
		{
			description:  "templating basic test",
			leftTypes:    []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			rightTypes:   []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			leftTuples:   tuples{{true, int16(10), 1.2}, {true, int16(20), 2.2}, {true, int16(30), 3.2}},
			rightTuples:  tuples{{true, int16(10), 1.2}, {false, int16(20), 2.2}, {true, int16(30), 3.9}},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{0, 1, 2},
			leftEqCols:   []uint32{0, 1, 2},
			rightEqCols:  []uint32{0, 1, 2},
			expected: tuples{
				{true, 10, 1.2, true, 10, 1.2},
			},
		},
		{
			description:  "templating cross product test",
			leftTypes:    []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			rightTypes:   []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
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
		},
		{
			description:  "templating cross product test, output batch size 1",
			leftTypes:    []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			rightTypes:   []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
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
			outputBatchSize: 1,
		},
		{
			description:  "templating cross product test, output batch size 2",
			leftTypes:    []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			rightTypes:   []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
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
			outputBatchSize: 2,
		},
		{
			description:  "templating reordered eq columns",
			leftTypes:    []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			rightTypes:   []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
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
		},
		{
			description:  "templating reordered eq columns non symmetrical",
			leftTypes:    []coltypes.T{coltypes.Bool, coltypes.Int16, coltypes.Float64},
			rightTypes:   []coltypes.T{coltypes.Int16, coltypes.Float64, coltypes.Bool},
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
		},
		{
			description:  "null handling",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{nil}, {0}},
			rightTuples:  tuples{{nil}, {0}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{0, 0},
			},
		},
		{
			description:  "null handling multi column, nulls on left",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {0, nil}},
			rightTuples:  tuples{{nil, nil}, {0, 1}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{0, nil, 0, 1},
			},
		},
		{
			description:  "null handling multi column, nulls on right",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {0, 1}},
			rightTuples:  tuples{{nil, nil}, {0, nil}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected: tuples{
				{0, 1, 0, nil},
			},
		},
		{
			description:  "desc test",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{4}, {3}, {2}, {1}},
			rightTuples:  tuples{{4}, {2}, {1}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{4, 4}, {2, 2}, {1, 1}},

			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
		},
		{
			description:  "desc nulls test",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{4}, {3}, {nil}, {1}},
			rightTuples:  tuples{{4}, {nil}, {2}, {1}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{4, 4}, {1, 1}},

			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
		},
		{
			description:  "desc nulls test end on 0",
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{9}, {9}, {8}, {0}, {nil}},
			rightTuples:  tuples{{9}, {9}, {8}, {0}, {nil}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{9, 9}, {9, 9}, {9, 9}, {9, 9}, {8, 8}, {0, 0}},

			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
		},
		{
			description:  "non-equality columns with nulls",
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, nil}, {2, 2}, {2, 2}, {3, nil}, {4, nil}},
			rightTuples:  tuples{{1, 1}, {2, nil}, {2, nil}, {3, nil}, {4, 4}, {4, 4}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, nil, 1, 1}, {2, 2, 2, nil}, {2, 2, 2, nil}, {2, 2, 2, nil}, {2, 2, 2, nil}, {3, nil, 3, nil}, {4, nil, 4, 4}, {4, nil, 4, 4}},
		},
		{
			description:  "basic LEFT OUTER JOIN test, L and R exhausted at the same time",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:  tuples{{0}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, nil}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
		},
		{
			description:  "basic LEFT OUTER JOIN test, R exhausted first",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, nil}, {1, nil}, {3, 3}, {5, nil}, {6, nil}, {7, nil}},
		},
		{
			description:  "basic LEFT OUTER JOIN test, L exhausted first",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{3, 3}, {5, nil}, {6, 6}, {7, nil}},
		},
		{
			description:  "multi output column LEFT OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, 10, 1, nil}, {2, 20, nil, nil}, {3, nil, 3, 13}, {4, 40, 4, 14}},
		},
		{
			description:  "null in equality column LEFT OUTER JOIN",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:  tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {3, 3, 3}},
		},
		{
			description:  "multi equality column LEFT OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:  tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{nil, nil, nil, nil}, {nil, 10, nil, nil}, {1, nil, nil, nil}, {1, 10, nil, nil}, {2, 20, 2, 20}, {4, 40, nil, nil}},
		},
		{
			description:  "multi equality column (long runs on left) LEFT OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:  tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{1, 9, nil, nil}, {1, 10, nil, nil}, {1, 10, nil, nil}, {1, 11, 1, 11}, {1, 11, 1, 11}, {2, 20, nil, nil}, {2, 20, nil, nil}, {2, 21, 2, 21}, {2, 22, nil, nil}, {2, 22, nil, nil}},
		},
		{
			description:     "3 equality column LEFT OUTER JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{2, 3, 1, nil, nil, nil}, {2, nil, 1, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}},
		},
		{
			description:     "3 equality column LEFT OUTER JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{2, 3, 1, nil, nil, nil}, {2, nil, 1, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}},
		},
		{
			description:     "single column DESC with nulls on the left LEFT OUTER JOIN",
			joinType:        sqlbase.JoinType_LEFT_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     tuples{{1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
		},
		{
			description:  "basic RIGHT OUTER JOIN test, L and R exhausted at the same time",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{-1}, {2}, {3}, {4}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
		},
		{
			description:  "basic RIGHT OUTER JOIN test, R exhausted first",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, 2}, {3, 3}, {nil, 4}},
		},
		{
			description:  "basic RIGHT OUTER JOIN test, L exhausted first",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, 2}, {3, 3}, {nil, 4}, {6, 6}, {nil, 8}, {nil, 9}},
		},
		{
			description:  "multi output column RIGHT OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, nil}, {3, 13}, {4, 14}},
			rightTuples:  tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, nil, 1, 10}, {nil, nil, 2, 20}, {3, 13, 3, nil}, {4, 14, 4, 40}},
		},
		{
			description:  "null in equality column RIGHT OUTER JOIN",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			rightTuples:  tuples{{nil}, {nil}, {1}, {3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {3, 3, 3}},
		},
		{
			description:  "multi equality column RIGHT OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			rightTuples:  tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{nil, nil, nil, nil}, {nil, nil, nil, 10}, {nil, nil, 1, nil}, {nil, nil, 1, 10}, {2, 20, 2, 20}, {nil, nil, 4, 40}},
		},
		{
			description:  "multi equality column (long runs on right) RIGHT OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			rightTuples:  tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{nil, nil, 1, 9}, {nil, nil, 1, 10}, {nil, nil, 1, 10}, {1, 11, 1, 11}, {1, 11, 1, 11}, {nil, nil, 2, 20}, {nil, nil, 2, 20}, {2, 21, 2, 21}, {nil, nil, 2, 22}, {nil, nil, 2, 22}},
		},
		{
			description:     "3 equality column RIGHT OUTER JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "3 equality column RIGHT OUTER JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "single column DESC with nulls on the right RIGHT OUTER JOIN",
			joinType:        sqlbase.JoinType_RIGHT_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}},
			rightTuples:     tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
		},
		{
			description:  "basic FULL OUTER JOIN test, L and R exhausted at the same time",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{-1}, {2}, {3}, {4}, {4}},
			rightTuples:  tuples{{1}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{-1, nil}, {nil, 1}, {2, 2}, {3, 3}, {4, 4}, {4, 4}, {4, 4}, {4, 4}},
		},
		{
			description:  "basic FULL OUTER JOIN test, R exhausted first",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, nil}, {1, nil}, {nil, 2}, {3, 3}, {nil, 4}, {5, nil}, {6, nil}, {7, nil}},
		},
		{
			description:  "basic FULL OUTER JOIN test, L exhausted first",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, 2}, {3, 3}, {nil, 4}, {5, nil}, {6, 6}, {7, nil}, {nil, 8}, {nil, 9}},
		},
		{
			description:  "multi output column FULL OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, nil}, {3, 13}, {4, 14}},
			rightTuples:  tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, nil, 1, 10}, {nil, nil, 2, 20}, {3, 13, 3, nil}, {4, 14, 4, 40}},
		},
		{
			description:  "null in equality column FULL OUTER JOIN",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			rightTuples:  tuples{{nil}, {nil}, {1}, {3}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, 1, nil}, {nil, nil, nil}, {nil, nil, nil}, {1, 1, 1}, {2, 2, nil}, {3, 3, 3}},
		},
		{
			description:  "multi equality column FULL OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			rightTuples:  tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{nil, nil, nil, nil}, {nil, nil, nil, nil}, {nil, 10, nil, nil}, {nil, nil, nil, 10}, {1, nil, nil, nil}, {1, nil, nil, nil}, {nil, nil, 1, nil}, {nil, nil, 1, 10}, {2, 20, 2, 20}, {3, 30, nil, nil}, {nil, nil, 4, 40}},
		},
		{
			description:  "multi equality column (long runs on right) FULL OUTER JOIN test with nulls",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			rightTuples:  tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{0, 1},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{1, 8, nil, nil}, {nil, nil, 1, 9}, {nil, nil, 1, 10}, {nil, nil, 1, 10}, {1, 11, 1, 11}, {1, 11, 1, 11}, {nil, nil, 2, 20}, {nil, nil, 2, 20}, {2, 21, 2, 21}, {nil, nil, 2, 22}, {nil, nil, 2, 22}, {2, 23, nil, nil}},
		},
		{
			description:     "3 equality column FULL OUTER JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{4, 3, 3, nil, nil, nil}, {nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, 2, nil, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "3 equality column FULL OUTER JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			rightTuples:     tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{0, 1, 2},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{4, 3, 3, nil, nil, nil}, {nil, nil, nil, 2, 3, 1}, {nil, nil, nil, 2, nil, 1}, {nil, 2, nil, nil, nil, nil}, {nil, 1, 3, nil, nil, nil}, {nil, nil, nil, nil, 1, 3}},
		},
		{
			description:     "single column DESC with nulls on the right FULL OUTER JOIN",
			joinType:        sqlbase.JoinType_FULL_OUTER,
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}},
			rightTuples:     tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{0},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1, 1}, {1, 1}, {1, 1}, {nil, nil}, {nil, nil}, {nil, nil}},
		},
		{
			description:  "FULL OUTER JOIN test with nulls and Bytes",
			joinType:     sqlbase.JoinType_FULL_OUTER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Bytes},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Bytes},
			leftTuples:   tuples{{nil, "0"}, {1, "10"}, {2, "20"}, {3, nil}, {4, "40"}},
			rightTuples:  tuples{{1, nil}, {3, "13"}, {4, nil}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{1},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil, "0", nil}, {1, "10", nil}, {2, "20", nil}, {3, nil, "13"}, {4, "40", nil}},
		},
		{
			description:  "basic LEFT SEMI JOIN test, L and R exhausted at the same time",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:  tuples{{-1}, {2}, {3}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{2}, {3}, {4}, {4}},
		},
		{
			description:  "basic LEFT SEMI JOIN test, R exhausted first",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{3}},
		},
		{
			description:  "basic LEFT SEMI JOIN test, L exhausted first",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {3}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{3}, {6}},
		},
		{
			description:  "multi output column LEFT SEMI JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1, 10}, {3, nil}, {4, 40}},
		},
		{
			description:  "null in equality column LEFT SEMI JOIN",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:  tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {3}},
		},
		{
			description:  "multi equality column LEFT SEMI JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:  tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{2, 20}},
		},
		{
			description:  "multi equality column (long runs on left) LEFT SEMI JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {1, 11}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:  tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{1, 11}, {1, 11}, {1, 11}, {2, 21}},
		},
		{
			description:     "3 equality column LEFT SEMI JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{},
			// The expected output here is empty, so will it be during the all nulls
			// injection, so we want to skip that.
			skipAllNullsInjection: true,
		},
		{
			description:     "3 equality column LEFT SEMI JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{},
			// The expected output here is empty, so will it be during the all nulls
			// injection, so we want to skip that.
			skipAllNullsInjection: true,
		},
		{
			description:     "single column DESC with nulls on the left LEFT SEMI JOIN",
			joinType:        sqlbase.JoinType_LEFT_SEMI,
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     tuples{{1}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{1}, {1}, {1}},
		},
		{
			description:  "basic LEFT ANTI JOIN test, L and R exhausted at the same time",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {2}, {3}, {4}, {4}},
			rightTuples:  tuples{{-1}, {2}, {4}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {3}},
		},
		{
			description:  "basic LEFT ANTI JOIN test, R exhausted first",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{1}, {1}, {3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {4}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{1}, {1}, {5}, {6}, {7}},
		},
		{
			description:  "basic LEFT ANTI JOIN test, L exhausted first",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64},
			leftTuples:   tuples{{3}, {5}, {6}, {7}},
			rightTuples:  tuples{{2}, {3}, {3}, {3}, {4}, {6}, {8}, {9}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{5}, {7}},
		},
		{
			description:  "multi output column LEFT ANTI JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{2, 20}},
		},
		{
			description:  "null in equality column LEFT ANTI JOIN",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil}, {nil}, {1}, {3}},
			rightTuples:  tuples{{nil, 1}, {1, 1}, {2, 2}, {3, 3}},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			expected:     tuples{{nil}, {nil}},
		},
		{
			description:  "multi equality column LEFT ANTI JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {2, 20}, {4, 40}},
			rightTuples:  tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, nil}, {2, 20}, {3, 30}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{nil, nil}, {nil, 10}, {1, nil}, {1, 10}, {4, 40}},
		},
		{
			description:  "multi equality column (long runs on left) LEFT ANTI JOIN test with nulls",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{1, 9}, {1, 10}, {1, 10}, {1, 11}, {1, 11}, {1, 11}, {2, 20}, {2, 20}, {2, 21}, {2, 22}, {2, 22}},
			rightTuples:  tuples{{1, 8}, {1, 11}, {1, 11}, {2, 21}, {2, 23}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			expected:     tuples{{1, 9}, {1, 10}, {1, 10}, {2, 20}, {2, 20}, {2, 22}, {2, 22}},
		},
		{
			description:     "3 equality column LEFT ANTI JOIN test with nulls DESC ordering",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{0, 1, 2},
			expected:        tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
		},
		{
			description:     "3 equality column LEFT ANTI JOIN test with nulls mixed ordering",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_ASC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC, execinfrapb.Ordering_Column_DESC, execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
			rightTuples:     tuples{{4, 3, 3}, {nil, 2, nil}, {nil, 1, 3}},
			leftOutCols:     []uint32{0, 1, 2},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0, 1, 2},
			rightEqCols:     []uint32{1, 2, 0},
			expected:        tuples{{2, 3, 1}, {2, nil, 1}, {nil, 1, 3}},
		},
		{
			description:     "single column DESC with nulls on the left LEFT ANTI JOIN",
			joinType:        sqlbase.JoinType_LEFT_ANTI,
			leftTypes:       []coltypes.T{coltypes.Int64},
			rightTypes:      []coltypes.T{coltypes.Int64},
			leftDirections:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			rightDirections: []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_DESC},
			leftTuples:      tuples{{1}, {1}, {1}, {nil}, {nil}, {nil}},
			rightTuples:     tuples{{1}, {nil}},
			leftOutCols:     []uint32{0},
			rightOutCols:    []uint32{},
			leftEqCols:      []uint32{0},
			rightEqCols:     []uint32{0},
			expected:        tuples{{nil}, {nil}, {nil}},
		},
		{
			description:  "INNER JOIN test with ON expression (filter only on left)",
			joinType:     sqlbase.JoinType_INNER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@1 < 4"},
			expected:     tuples{{1, 10}, {3, nil}},
		},
		{
			description:  "INNER JOIN test with ON expression (filter only on right)",
			joinType:     sqlbase.JoinType_INNER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@4 < 14"},
			expected:     tuples{{3, nil}},
		},
		{
			description:  "INNER JOIN test with ON expression (filter on both)",
			joinType:     sqlbase.JoinType_INNER,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@2 + @3 < 50"},
			expected:     tuples{{1, 10}, {4, 40}},
		},
		{
			description:  "LEFT SEMI JOIN test with ON expression (filter only on left)",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@1 < 4"},
			expected:     tuples{{1, 10}, {3, nil}},
		},
		{
			description:  "LEFT SEMI JOIN test with ON expression (filter only on right)",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@4 < 14"},
			expected:     tuples{{3, nil}},
		},
		{
			description:  "LEFT SEMI JOIN test with ON expression (filter on both)",
			joinType:     sqlbase.JoinType_LEFT_SEMI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@2 + @3 < 50"},
			expected:     tuples{{1, 10}, {4, 40}},
		},
		{
			description:  "LEFT ANTI JOIN test with ON expression (filter only on left)",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@1 < 4"},
			expected:     tuples{{nil, 0}, {2, 20}, {4, 40}},
		},
		{
			description:  "LEFT ANTI JOIN test with ON expression (filter only on right)",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@4 < 14"},
			expected:     tuples{{nil, 0}, {1, 10}, {2, 20}, {4, 40}},
		},
		{
			description:  "LEFT ANTI JOIN test with ON expression (filter on both)",
			joinType:     sqlbase.JoinType_LEFT_ANTI,
			leftTypes:    []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes:   []coltypes.T{coltypes.Int64, coltypes.Int64},
			leftTuples:   tuples{{nil, 0}, {1, 10}, {2, 20}, {3, nil}, {4, 40}},
			rightTuples:  tuples{{1, nil}, {3, 13}, {4, 14}},
			leftOutCols:  []uint32{0, 1},
			rightOutCols: []uint32{},
			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			onExpr:       execinfrapb.Expression{Expr: "@2 + @3 < 50"},
			expected:     tuples{{nil, 0}, {2, 20}, {3, nil}},
		},
	}

	for _, tc := range tcs {
		tc.Init()

		// We use a custom verifier function so that we can get the merge join op
		// to use a custom output batch size per test, to exercise more cases.
		var mergeJoinVerifier verifier = func(output *opTestOutput) error {
			if mj, ok := output.input.(variableOutputBatchSizeInitializer); ok {
				mj.initWithOutputBatchSize(tc.outputBatchSize)
			} else {
				// When we have an inner join with ON expression, a filter operator
				// will be put on top of the merge join, so to make life easier, we'll
				// just ignore the requested output batch size.
				output.input.Init()
			}
			verify := output.Verify
			if _, isFullOuter := output.input.(*mergeJoinFullOuterOp); isFullOuter {
				// FULL OUTER JOIN doesn't guarantee any ordering on its output (since
				// it is ambiguous), so we're comparing the outputs as sets.
				verify = output.VerifyAnyOrder
			}

			return verify()
		}

		var runner testRunner
		if tc.skipAllNullsInjection {
			// We're omitting all nulls injection test. See comments for each such
			// test case.
			runner = runTestsWithoutAllNullsInjection
		} else {
			runner = runTestsWithTyps
		}
		runner(t, []tuples{tc.leftTuples, tc.rightTuples}, nil /* typs */, tc.expected, mergeJoinVerifier,
			func(input []Operator) (Operator, error) {
				spec := createSpecForMergeJoiner(tc)
				args := NewColOperatorArgs{
					Spec:                               spec,
					Inputs:                             input,
					StreamingMemAccount:                testMemAcc,
					UseStreamingMemAccountForBuffering: true,
				}
				result, err := NewColOperator(ctx, flowCtx, args)
				if err != nil {
					return nil, err
				}
				return result.Op, nil
			})
	}
}

// TestFullOuterMergeJoinWithMaximumNumberOfGroups will create two input
// sources such that the left one contains rows with even numbers 0, 2, 4, ...
// while the right contains one rows with odd numbers 1, 3, 5, ... The test
// will perform FULL OUTER JOIN. Such setup will create the maximum number of
// groups per batch.
func TestFullOuterMergeJoinWithMaximumNumberOfGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	nTuples := int(coldata.BatchSize()) * 4
	for _, outBatchSize := range []uint16{1, 16, coldata.BatchSize() - 1, coldata.BatchSize(), coldata.BatchSize() + 1} {
		t.Run(fmt.Sprintf("outBatchSize=%d", outBatchSize),
			func(t *testing.T) {
				typs := []coltypes.T{coltypes.Int64}
				colsLeft := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
				colsRight := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
				groupsLeft := colsLeft[0].Int64()
				groupsRight := colsRight[0].Int64()
				for i := range groupsLeft {
					groupsLeft[i] = int64(i * 2)
					groupsRight[i] = int64(i*2 + 1)
				}
				leftSource := newChunkingBatchSource(typs, colsLeft, uint64(nTuples))
				rightSource := newChunkingBatchSource(typs, colsRight, uint64(nTuples))
				a, err := NewMergeJoinOp(
					testAllocator,
					sqlbase.FullOuterJoin,
					leftSource,
					rightSource,
					[]uint32{0},
					[]uint32{0},
					typs,
					typs,
					[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
					[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
					nil,   /* filterConstructor */
					false, /* filterOnlyOnLeft */
				)
				if err != nil {
					t.Fatal("error in merge join op constructor", err)
				}
				a.(*mergeJoinFullOuterOp).initWithOutputBatchSize(outBatchSize)
				i, count, expVal := 0, 0, int64(0)
				for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
					count += int(b.Length())
					leftOutCol := b.ColVec(0).Int64()
					leftNulls := b.ColVec(0).Nulls()
					rightOutCol := b.ColVec(1).Int64()
					rightNulls := b.ColVec(1).Nulls()
					for j := uint16(0); j < b.Length(); j++ {
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
			})
	}
}

// TestMergeJoinerMultiBatch creates one long input of a 1:1 join, and keeps
// track of the expected output to make sure the join output is batched
// correctly.
func TestMergeJoinerMultiBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	for _, numInputBatches := range []int{1, 2, 16} {
		for _, outBatchSize := range []uint16{1, 16, coldata.BatchSize()} {
			t.Run(fmt.Sprintf("numInputBatches=%d", numInputBatches),
				func(t *testing.T) {
					nTuples := int(coldata.BatchSize()) * numInputBatches
					typs := []coltypes.T{coltypes.Int64}
					cols := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
					groups := cols[0].Int64()
					for i := range groups {
						groups[i] = int64(i)
					}

					leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
					rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

					a, err := NewMergeJoinOp(
						testAllocator,
						sqlbase.InnerJoin,
						leftSource,
						rightSource,
						[]uint32{0},
						[]uint32{0},
						typs,
						typs,
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
						nil,   /* filterConstructor */
						false, /* filterOnlyOnLeft */
					)
					if err != nil {
						t.Fatal("error in merge join op constructor", err)
					}

					a.(*mergeJoinInnerOp).initWithOutputBatchSize(outBatchSize)

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
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	for _, groupSize := range []int{int(coldata.BatchSize()) / 8, int(coldata.BatchSize()) / 4, int(coldata.BatchSize()) / 2} {
		if groupSize == 0 {
			// We might be varying coldata.BatchSize() so that when it is divided by
			// 4, groupSize is 0. We want to skip such configuration.
			continue
		}
		for _, numInputBatches := range []int{1, 2, 16} {
			t.Run(fmt.Sprintf("groupSize=%d/numInputBatches=%d", groupSize, numInputBatches),
				func(t *testing.T) {
					nTuples := int(coldata.BatchSize()) * numInputBatches
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
					typs := []coltypes.T{coltypes.Int64, coltypes.Int64}
					cols := []coldata.Vec{
						testAllocator.NewMemColumn(typs[0], nTuples),
						testAllocator.NewMemColumn(typs[1], nTuples),
					}
					for i := range cols[0].Int64() {
						cols[0].Int64()[i] = int64(i / groupSize)
						cols[1].Int64()[i] = int64(i / groupSize)
					}

					leftSource := newChunkingBatchSource(typs, cols, uint64(nTuples))
					rightSource := newChunkingBatchSource(typs, cols, uint64(nTuples))

					a, err := NewMergeJoinOp(
						testAllocator,
						sqlbase.InnerJoin,
						leftSource,
						rightSource,
						[]uint32{0},
						[]uint32{0},
						typs,
						typs,
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_ASC}},
						[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_ASC}},
						nil,   /* filterConstructor */
						false, /* filterOnlyOnLeft */
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
	nTuples int, typs []coltypes.T, maxRunLength int64, skipValues bool, randomIncrement int64,
) ([]coldata.Vec, []coldata.Vec, []expectedGroup) {
	rng, _ := randutil.NewPseudoRand()
	lCols := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
	lCol := lCols[0].Int64()
	rCols := []coldata.Vec{testAllocator.NewMemColumn(typs[0], nTuples)}
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
	ctx := context.Background()
	for _, numInputBatches := range []int{1, 2, 16, 256} {
		for _, maxRunLength := range []int64{2, 3, 100} {
			for _, skipValues := range []bool{false, true} {
				for _, randomIncrement := range []int64{0, 1} {
					t.Run(fmt.Sprintf("numInputBatches=%dmaxRunLength=%dskipValues=%trandomIncrement=%d", numInputBatches, maxRunLength, skipValues, randomIncrement),
						func(t *testing.T) {
							nTuples := int(coldata.BatchSize()) * numInputBatches
							typs := []coltypes.T{coltypes.Int64}
							lCols, rCols, exp := newBatchesOfRandIntRows(nTuples, typs, maxRunLength, skipValues, randomIncrement)
							leftSource := newChunkingBatchSource(typs, lCols, uint64(nTuples))
							rightSource := newChunkingBatchSource(typs, rCols, uint64(nTuples))

							a, err := NewMergeJoinOp(
								testAllocator,
								sqlbase.InnerJoin,
								leftSource,
								rightSource,
								[]uint32{0},
								[]uint32{0},
								typs,
								typs,
								[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
								[]execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC}},
								nil,   /* filterConstructor */
								false, /* filterOnlyOnLeft */
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
		for i := 0; i < int(coldata.BatchSize()); i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(coldata.BatchSize())

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.Nulls().UnsetNulls()
	}
	return batch
}

func newBatchOfRepeatedIntRows(nCols int, batch coldata.Batch, numRepeats int) coldata.Batch {
	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < int(coldata.BatchSize()); i++ {
			col[i] = int64((i + 1) / numRepeats)
		}
	}

	batch.SetLength(coldata.BatchSize())

	for colIdx := 0; colIdx < nCols; colIdx++ {
		vec := batch.ColVec(colIdx)
		vec.Nulls().UnsetNulls()
	}
	return batch
}

func BenchmarkMergeJoiner(b *testing.B) {
	ctx := context.Background()
	nCols := 4
	sourceTypes := make([]coltypes.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = coltypes.Int64
	}

	batch := testAllocator.NewMemBatch(sourceTypes)

	// 1:1 join.
	for _, nBatches := range []int{1, 4, 16, 1024} {
		b.Run(fmt.Sprintf("rows=%d", nBatches*int(coldata.BatchSize())), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)

				s := mergeJoinInnerOp{
					mergeJoinBase{
						allocator: testAllocator,
						left: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{0, 1},
							sourceTypes: sourceTypes,
							source:      leftSource,
							directions:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC},
						},

						right: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{2, 3},
							sourceTypes: sourceTypes,
							source:      rightSource,
							directions:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC},
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
		b.Run(fmt.Sprintf("oneSideRepeat-rows=%d", nBatches*int(coldata.BatchSize())), func(b *testing.B) {
			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, nBatches), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfIntRows(nCols, batch), nBatches)

				s := mergeJoinInnerOp{
					mergeJoinBase{
						allocator: testAllocator,
						left: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{0, 1},
							sourceTypes: sourceTypes,
							source:      leftSource,
							directions:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC},
						},

						right: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{2, 3},
							sourceTypes: sourceTypes,
							source:      rightSource,
							directions:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC},
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
		b.Run(fmt.Sprintf("bothSidesRepeat-rows=%d", nBatches*int(coldata.BatchSize())), func(b *testing.B) {

			// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
			// batch) * nCols (number of columns / row) * 2 (number of sources).
			b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				leftSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, numRepeats), nBatches)
				rightSource := newFiniteBatchSource(newBatchOfRepeatedIntRows(nCols, batch, numRepeats), nBatches)

				s := mergeJoinInnerOp{
					mergeJoinBase{
						allocator: testAllocator,
						left: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{0, 1},
							sourceTypes: sourceTypes,
							source:      leftSource,
							directions:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC},
						},

						right: mergeJoinInput{
							eqCols:      []uint32{0},
							outCols:     []uint32{2, 3},
							sourceTypes: sourceTypes,
							source:      rightSource,
							directions:  []execinfrapb.Ordering_Column_Direction{execinfrapb.Ordering_Column_ASC},
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
