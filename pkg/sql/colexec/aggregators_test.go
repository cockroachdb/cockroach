// Copyright 2018 The Cockroach Authors.
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
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/stretchr/testify/require"
)

type aggregatorTestCase struct {
	name      string
	typs      []*types.T
	input     colexectestutils.Tuples
	groupCols []uint32
	aggCols   [][]uint32
	aggFns    []execinfrapb.AggregatorSpec_Func
	expected  colexectestutils.Tuples

	constArguments [][]execinfrapb.Expression
	// spec will be populated during init().
	spec           *execinfrapb.AggregatorSpec
	aggDistinct    []bool
	aggFilter      []int
	unorderedInput bool
	orderedCols    []uint32

	// convToDecimal will convert any float64s to apd.Decimals. If a string is
	// encountered, a best effort is made to convert that string to an
	// apd.Decimal.
	convToDecimal bool
}

type ordering int64

const (
	ordered ordering = iota
	partial
	unordered
)

// aggType is a helper struct that allows tests to test both the ordered and
// hash aggregators at the same time.
type aggType struct {
	new   func(*colexecagg.NewAggregatorArgs) colexecop.ResettableOperator
	name  string
	order ordering
}

var aggTypesWithPartial = []aggType{
	{
		// This is a wrapper around NewHashAggregator so its signature is
		// compatible with NewOrderedAggregator.
		new: func(args *colexecagg.NewAggregatorArgs) colexecop.ResettableOperator {
			return NewHashAggregator(args, nil /* newSpillingQueueArgs */, testAllocator, math.MaxInt64)
		},
		name:  "hash",
		order: unordered,
	},
	{
		new:   NewOrderedAggregator,
		name:  "ordered",
		order: ordered,
	},
	{
		// This is a wrapper around NewHashAggregator so its signature is
		// compatible with NewOrderedAggregator.
		new: func(args *colexecagg.NewAggregatorArgs) colexecop.ResettableOperator {
			return NewHashAggregator(args, nil /* newSpillingQueueArgs */, testAllocator, math.MaxInt64)
		},
		name:  "hash-partial-order",
		order: partial,
	},
}

var aggTypes = aggTypesWithPartial[:2]

func (tc *aggregatorTestCase) init() error {
	if tc.convToDecimal {
		for _, tuples := range []colexectestutils.Tuples{tc.input, tc.expected} {
			for _, tuple := range tuples {
				for i, e := range tuple {
					switch v := e.(type) {
					case float64:
						d := &apd.Decimal{}
						d, err := d.SetFloat64(v)
						if err != nil {
							return err
						}
						tuple[i] = *d
					case string:
						d := &apd.Decimal{}
						d, _, err := d.SetString(v)
						if err != nil {
							// If there was an error converting the string to decimal, just
							// leave the datum as is.
							continue
						}
						tuple[i] = *d
					}
				}
			}
		}
	}
	aggregations := make([]execinfrapb.AggregatorSpec_Aggregation, len(tc.aggFns))
	for i, aggFn := range tc.aggFns {
		aggregations[i].Func = aggFn
		aggregations[i].ColIdx = tc.aggCols[i]
		if tc.constArguments != nil {
			aggregations[i].Arguments = tc.constArguments[i]
		}
		if tc.aggDistinct != nil {
			aggregations[i].Distinct = tc.aggDistinct[i]
		}
		if tc.aggFilter != nil && tc.aggFilter[i] != tree.NoColumnIdx {
			filterColIdx := uint32(tc.aggFilter[i])
			aggregations[i].FilterColIdx = &filterColIdx
		}
	}
	tc.spec = &execinfrapb.AggregatorSpec{
		GroupCols:    tc.groupCols,
		Aggregations: aggregations,
	}
	if !tc.unorderedInput {
		var outputOrderCols []uint32
		if len(tc.orderedCols) == 0 {
			outputOrderCols = tc.spec.GroupCols
		} else {
			outputOrderCols = tc.orderedCols
			tc.spec.OrderedGroupCols = tc.orderedCols
		}
		// If input grouping columns have an ordering, then we'll require the
		// output to also have the same ordering.
		outputOrdering := execinfrapb.Ordering{Columns: make([]execinfrapb.Ordering_Column, len(outputOrderCols))}
		for i, col := range outputOrderCols {
			outputOrdering.Columns[i].ColIdx = col
		}
		tc.spec.OutputOrdering = outputOrdering
	}
	return nil
}

var aggregatorsTestCases = []aggregatorTestCase{
	{
		name: "OneTuple",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 1},
		},
	},
	{
		name: "OneGroup",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 1},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 2},
		},
	},
	{
		name: "MultiGroup",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 0},
			{0, 1},
			{1, 4},
			{2, 5},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 2},
			{1, 4},
			{2, 5},
		},
	},
	{
		name: "CarryBetweenInputBatches",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 2},
			{0, 3},
			{1, 4},
			{1, 5},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 6},
			{1, 9},
		},
	},
	{
		name: "CarryBetweenOutputBatches",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 2},
			{0, 3},
			{0, 4},
			{1, 5},
			{2, 6},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 10},
			{1, 5},
			{2, 6},
		},
	},
	{
		name: "CarryBetweenInputAndOutputBatches",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 1},
			{1, 2},
			{2, 3},
			{2, 3},
			{3, 4},
			{3, 4},
			{4, 5},
			{5, 6},
			{6, 7},
			{7, 8},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 2},
			{1, 2},
			{2, 6},
			{3, 8},
			{4, 5},
			{5, 6},
			{6, 7},
			{7, 8},
		},
	},
	{
		name: "NoGroupingCols",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 2},
			{0, 3},
			{0, 4},
		},
		groupCols: []uint32{},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 10},
		},
	},
	{
		name: "UnorderedWithNullsInGroupingCol",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{nil, 1},
			{4, 42},
			{nil, 2},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{nil, 3},
			{4, 42},
		},
		unorderedInput: true,
	},
	{
		name: "CountRows",
		typs: types.OneIntCol,
		input: colexectestutils.Tuples{
			{1},
			{2},
			{1},
			{nil},
			{3},
			{1},
			{3},
			{4},
			{1},
			{nil},
			{2},
			{4},
			{2},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.CountRows,
		},
		expected: colexectestutils.Tuples{
			{nil, 2},
			{1, 4},
			{2, 3},
			{3, 2},
			{4, 2},
		},
		unorderedInput: true,
	},
	{
		name: "OutputOrder",
		typs: types.ThreeIntCols,
		input: colexectestutils.Tuples{
			{0, 1, 2},
			{0, 1, 2},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {2}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 4, 2},
		},
	},
	{
		name: "SumMultiType",
		typs: []*types.T{types.Int, types.Int, types.Decimal},
		input: colexectestutils.Tuples{
			{0, 1, 1.3},
			{0, 1, 1.6},
			{0, 1, 0.5},
			{1, 1, 1.2},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {2}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.Sum,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 3.4, 3},
			{1, 1.2, 1},
		},
		convToDecimal: true,
	},
	{
		name: "AvgSumSingleInputBatch",
		typs: []*types.T{types.Int, types.Decimal},
		input: colexectestutils.Tuples{
			{0, 1.1},
			{0, 1.2},
			{0, 2.3},
			{1, 6.21},
			{1, 2.43},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.Avg,
			execinfrapb.Sum,
		},
		expected: colexectestutils.Tuples{
			{0, "1.5333333333333333333", 4.6},
			{1, "4.3200000000000000000", 8.64},
		},
		convToDecimal: true,
	},
	{
		name: "BoolAndOrBatch",
		typs: []*types.T{types.Int, types.Bool},
		input: colexectestutils.Tuples{
			{0, true},
			{1, false},
			{2, true},
			{2, false},
			{3, true},
			{3, true},
			{4, false},
			{4, false},
			{5, false},
			{5, nil},
			{6, nil},
			{6, true},
			{7, nil},
			{7, false},
			{7, true},
			{8, nil},
			{8, nil},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.BoolAnd,
			execinfrapb.BoolOr,
		},
		expected: colexectestutils.Tuples{
			{0, true, true},
			{1, false, false},
			{2, false, true},
			{3, true, true},
			{4, false, false},
			{5, false, false},
			{6, true, true},
			{7, false, true},
			{8, nil, nil},
		},
	},
	{
		name: "MultiGroupColsWithPointerTypes",
		typs: []*types.T{types.Int, types.Decimal, types.Bytes, types.Decimal},
		input: colexectestutils.Tuples{
			{2, 1.0, "1.0", 2.0},
			{2, 1.0, "1.0", 4.0},
			{2, 2.0, "2.0", 6.0},
		},
		groupCols: []uint32{0, 1, 2},
		aggCols:   [][]uint32{{0}, {1}, {2}, {3}, {3}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.AnyNotNull,
			execinfrapb.Min,
			execinfrapb.Sum,
		},
		expected: colexectestutils.Tuples{
			{2, 1.0, "1.0", 2.0, 6.0},
			{2, 2.0, "2.0", 6.0, 6.0},
		},
	},
	{
		name: "GroupOnTimeTZColumns",
		typs: []*types.T{types.TimeTZ, types.Int},
		input: colexectestutils.Tuples{
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 0), -1},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 1), 1},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 1), 2},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 2), 10},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 2), 11},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 3), 100},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 3), 101},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 4), 102},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 0), -1},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 1), 3},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 2), 21},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 3), 201},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 4), 102},
		},
	},
	{
		name: "AVG on all types",
		typs: []*types.T{types.Int, types.Int2, types.Int4, types.Int, types.Decimal, types.Float, types.Interval},
		input: colexectestutils.Tuples{
			{0, nil, 1, 1, 1.0, 1.0, duration.MakeDuration(1, 1, 1)},
			{0, 1, nil, 2, 2.0, 2.0, duration.MakeDuration(2, 2, 2)},
			{0, 2, 2, nil, 3.0, 3.0, duration.MakeDuration(3, 3, 3)},
			{0, 3, 3, 3, nil, 4.0, duration.MakeDuration(4, 4, 4)},
			{0, 4, 4, 4, 4.0, nil, duration.MakeDuration(5, 5, 5)},
			{0, 5, 5, 5, 5.0, 5.0, nil},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}, {2}, {3}, {4}, {5}, {6}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.Avg,
			execinfrapb.Avg,
			execinfrapb.Avg,
			execinfrapb.Avg,
			execinfrapb.Avg,
			execinfrapb.Avg,
		},
		expected: colexectestutils.Tuples{
			{0, 3.0, 3.0, 3.0, 3.0, 3.0, duration.MakeDuration(3, 3, 3)},
		},
	},
	{
		name: "ConcatAgg",
		typs: []*types.T{types.Int, types.Bytes},
		input: colexectestutils.Tuples{
			{1, "1"},
			{1, "2"},
			{1, "3"},
			{2, nil},
			{2, "1"},
			{2, "2"},
			{3, "1"},
			{3, nil},
			{3, "2"},
			{4, nil},
			{4, nil},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.ConcatAgg,
		},
		expected: colexectestutils.Tuples{
			{1, "123"},
			{2, "12"},
			{3, "12"},
			{4, nil},
		},
	},
	{
		name: "All",
		typs: []*types.T{types.Int, types.Decimal, types.Int, types.Bool, types.Bytes},
		input: colexectestutils.Tuples{
			{0, 3.1, 2, true, "zero"},
			{0, 1.1, 3, false, "zero"},
			{1, 1.1, 1, false, "one"},
			{1, 4.1, 0, false, "one"},
			{2, 1.1, 1, true, "two"},
			{3, 4.1, 0, false, "three"},
			{3, 5.1, 0, true, "three"},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {}, {1}, {1}, {1}, {2}, {2}, {2}, {3}, {3}, {4}, {4}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.CountRows,
			execinfrapb.Avg,
			execinfrapb.Count,
			execinfrapb.Sum,
			execinfrapb.SumInt,
			execinfrapb.Min,
			execinfrapb.Max,
			execinfrapb.BoolAnd,
			execinfrapb.BoolOr,
			execinfrapb.AnyNotNull,
			execinfrapb.ConcatAgg,
		},
		expected: colexectestutils.Tuples{
			{0, 2, "2.1000000000000000000", 2, 4.2, 5, 2, 3, false, true, "zero", "zerozero"},
			{1, 2, "2.6000000000000000000", 2, 5.2, 1, 0, 1, false, false, "one", "oneone"},
			{2, 1, "1.1000000000000000000", 1, 1.1, 1, 1, 1, true, true, "two", "two"},
			{3, 2, "4.6000000000000000000", 2, 9.2, 0, 0, 0, false, true, "three", "threethree"},
		},
		convToDecimal: true,
	},
	{
		name: "NullHandling",
		typs: []*types.T{types.Int, types.Decimal, types.Int, types.Bool, types.Bytes},
		input: colexectestutils.Tuples{
			{nil, 1.1, 4, true, "a"},
			{0, nil, nil, nil, nil},
			{0, 3.1, 5, nil, "b"},
			{1, nil, nil, nil, nil},
			{1, nil, nil, false, nil},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {}, {1}, {1}, {1}, {1}, {2}, {2}, {2}, {3}, {3}, {4}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.CountRows,
			execinfrapb.AnyNotNull,
			execinfrapb.Count,
			execinfrapb.Sum,
			execinfrapb.Avg,
			execinfrapb.SumInt,
			execinfrapb.Min,
			execinfrapb.Max,
			execinfrapb.BoolAnd,
			execinfrapb.BoolOr,
			execinfrapb.ConcatAgg,
		},
		expected: colexectestutils.Tuples{
			{nil, 1, 1.1, 1, 1.1, "1.1000000000000000000", 4, 4, 4, true, true, "a"},
			{0, 2, 3.1, 1, 3.1, "3.1000000000000000000", 5, 5, 5, nil, nil, "b"},
			{1, 2, nil, 0, nil, nil, nil, nil, nil, false, false, nil},
		},
		convToDecimal: true,
	},
	{
		name: "DistinctAggregation",
		typs: types.TwoIntCols,
		input: colexectestutils.Tuples{
			{0, 1},
			{0, 2},
			{0, 2},
			{0, nil},
			{0, 1},
			{0, nil},
			{1, 1},
			{1, 2},
			{1, 2},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}, {1}, {1}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.Count,
			execinfrapb.Count,
			execinfrapb.SumInt,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 4, 2, 6, 3},
			{1, 3, 2, 5, 3},
		},
		aggDistinct: []bool{false, false, true, false, true},
	},
	{
		name: "FilteringAggregation",
		typs: []*types.T{types.Int, types.Int, types.Bool},
		input: colexectestutils.Tuples{
			{0, 1, false},
			{0, 2, true},
			{0, 2, true},
			{0, nil, nil},
			{0, 1, nil},
			{0, nil, true},
			{1, 1, true},
			{1, 2, nil},
			{1, 2, true},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.CountRows,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 3, 4},
			{1, 2, 3},
		},
		aggFilter: []int{tree.NoColumnIdx, 2, 2},
	},
	{
		name: "AllGroupsFilteredOut",
		typs: []*types.T{types.Int, types.Int, types.Bool},
		input: colexectestutils.Tuples{
			{0, 1, false},
			{0, nil, nil},
			{0, 2, false},
			{1, 1, true},
			{1, 2, nil},
			{1, 2, true},
			{2, 1, false},
			{2, nil, nil},
			{2, 2, nil},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.CountRows,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 0, nil},
			{1, 2, 3},
			{2, 0, nil},
		},
		aggFilter: []int{tree.NoColumnIdx, 2, 2},
	},
	{
		name: "DistinctFilteringAggregation",
		typs: []*types.T{types.Int, types.Int, types.Bool},
		input: colexectestutils.Tuples{
			{0, 1, false},
			{0, 2, true},
			{0, 2, true},
			{0, nil, nil},
			{0, 1, nil},
			{0, nil, true},
			{1, 1, true},
			{1, 2, nil},
			{1, 2, true},
		},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{0}, {1}, {1}, {1}, {1}, {1}, {1}},
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AnyNotNull,
			execinfrapb.Count,
			execinfrapb.Count,
			execinfrapb.Count,
			execinfrapb.SumInt,
			execinfrapb.SumInt,
			execinfrapb.SumInt,
		},
		expected: colexectestutils.Tuples{
			{0, 2, 2, 1, 4, 3, 2},
			{1, 2, 2, 2, 3, 3, 3},
		},
		aggDistinct: []bool{false, false, true, true, false, true, true},
		aggFilter:   []int{tree.NoColumnIdx, 2, tree.NoColumnIdx, 2, 2, tree.NoColumnIdx, 2},
	},
}

func init() {
	for i := range aggregatorsTestCases {
		if err := aggregatorsTestCases[i].init(); err != nil {
			colexecerror.InternalError(err)
		}
	}
}

func TestAggregators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	ctx := context.Background()
	for _, tc := range aggregatorsTestCases {
		constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
			&evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
		)
		require.NoError(t, err)
		for _, agg := range aggTypes {
			if tc.unorderedInput && agg.order == ordered {
				// This test case has unordered input, so we skip ordered
				// aggregator.
				continue
			}
			if agg.order == ordered && tc.aggFilter != nil {
				// Filtering aggregation is only supported with hash aggregator.
				continue
			}
			log.Infof(ctx, "%s/%s", tc.name, agg.name)
			verifier := colexectestutils.OrderedVerifier
			if tc.unorderedInput {
				verifier = colexectestutils.UnorderedVerifier
			}
			colexectestutils.RunTestsWithTyps(t, testAllocator, []colexectestutils.Tuples{tc.input}, [][]*types.T{tc.typs}, tc.expected, verifier,
				func(input []colexecop.Operator) (colexecop.Operator, error) {
					return agg.new(&colexecagg.NewAggregatorArgs{
						Allocator:      testAllocator,
						MemAccount:     testMemAcc,
						Input:          input[0],
						InputTypes:     tc.typs,
						Spec:           tc.spec,
						EvalCtx:        &evalCtx,
						Constructors:   constructors,
						ConstArguments: constArguments,
						OutputTypes:    outputTypes,
					}), nil
				})
		}
	}
}

func TestAggregatorRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	// This test aggregates random inputs, keeping track of the expected results
	// to make sure the aggregations are correct.
	rng, _ := randutil.NewTestRand()
	for _, groupSize := range []int{1, 2, coldata.BatchSize() / 4, coldata.BatchSize() / 2} {
		if groupSize == 0 {
			// We might be varying coldata.BatchSize() so that when it is divided by
			// 4, groupSize is 0. We want to skip such configuration.
			continue
		}
		for _, numInputBatches := range []int{1, 2, 64} {
			for _, hasNulls := range []bool{true, false} {
				for _, agg := range aggTypesWithPartial {
					log.Infof(context.Background(), "%s/groupSize=%d/numInputBatches=%d/hasNulls=%t", agg.name, groupSize, numInputBatches, hasNulls)
					nTuples := coldata.BatchSize() * numInputBatches
					typs := []*types.T{types.Int, types.Float}
					cols := []coldata.Vec{
						testAllocator.NewMemColumn(typs[0], nTuples),
						testAllocator.NewMemColumn(typs[1], nTuples),
					}
					if agg.order == partial {
						typs = append(typs, types.Int)
						cols = append(cols, testAllocator.NewMemColumn(typs[2], nTuples))
					}
					groups, aggCol, aggColNulls := cols[0].Int64(), cols[1].Float64(), cols[1].Nulls()
					expectedTuples := colexectestutils.Tuples{}

					var expRowCounts, expCounts []int64
					var expSums, expMins, expMaxs []float64
					// SUM, MIN, MAX, and AVG aggregators can output null.
					var expNulls []bool
					curGroup := -1
					for i := range groups {
						if i%groupSize == 0 {
							if curGroup != -1 {
								if expNulls[curGroup] {
									expectedTuples = append(expectedTuples, colexectestutils.Tuple{
										expRowCounts[curGroup], expCounts[curGroup], nil, nil, nil, nil,
									})
								} else {
									expectedTuples = append(expectedTuples, colexectestutils.Tuple{
										expRowCounts[curGroup], expCounts[curGroup], expSums[curGroup], expMins[curGroup], expMaxs[curGroup], expSums[curGroup] / float64(expCounts[curGroup]),
									})
								}
							}
							expRowCounts = append(expRowCounts, 0)
							expCounts = append(expCounts, 0)
							expSums = append(expSums, 0)
							expMins = append(expMins, 2048)
							expMaxs = append(expMaxs, -2048)
							expNulls = append(expNulls, true)
							curGroup++
						}
						// Keep the inputs small so they are a realistic size. Using a
						// large range is not realistic and makes decimal operations
						// slower.
						aggCol[i] = 2048 * (rng.Float64() - 0.5)

						// NULL values contribute to the row count, so we're updating
						// the row counts outside of the if block.
						expRowCounts[curGroup]++
						if hasNulls && rng.Float64() < nullProbability {
							aggColNulls.SetNull(i)
						} else {
							expNulls[curGroup] = false
							expCounts[curGroup]++
							expSums[curGroup] += aggCol[i]
							expMins[curGroup] = min64(aggCol[i], expMins[curGroup])
							expMaxs[curGroup] = max64(aggCol[i], expMaxs[curGroup])
						}
						groups[i] = int64(curGroup)
					}
					// Add result for last group.
					if expNulls[curGroup] {
						expectedTuples = append(expectedTuples, colexectestutils.Tuple{
							expRowCounts[curGroup], expCounts[curGroup], nil, nil, nil, nil,
						})
					} else {
						expectedTuples = append(expectedTuples, colexectestutils.Tuple{
							expRowCounts[curGroup], expCounts[curGroup], expSums[curGroup], expMins[curGroup], expMaxs[curGroup], expSums[curGroup] / float64(expCounts[curGroup]),
						})
					}

					source := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, nTuples)
					tc := aggregatorTestCase{
						typs:      typs,
						groupCols: []uint32{0},
						aggCols:   [][]uint32{{}, {1}, {1}, {1}, {1}, {1}},
						aggFns: []execinfrapb.AggregatorSpec_Func{
							execinfrapb.CountRows,
							execinfrapb.Count,
							execinfrapb.Sum,
							execinfrapb.Min,
							execinfrapb.Max,
							execinfrapb.Avg,
						},
					}
					if agg.order == partial {
						tc.groupCols = []uint32{0, 2}
						tc.orderedCols = []uint32{0}
					}
					require.NoError(t, tc.init())
					constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
						&evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
					)
					require.NoError(t, err)
					a := agg.new(&colexecagg.NewAggregatorArgs{
						Allocator:      testAllocator,
						MemAccount:     testMemAcc,
						Input:          source,
						InputTypes:     tc.typs,
						Spec:           tc.spec,
						EvalCtx:        &evalCtx,
						Constructors:   constructors,
						ConstArguments: constArguments,
						OutputTypes:    outputTypes,
					})
					a.Init(context.Background())

					testOutput := colexectestutils.NewOpTestOutput(a, expectedTuples)

					if agg.order == ordered {
						err = testOutput.Verify()
					} else if agg.order == partial {
						err = testOutput.VerifyPartialOrder()
					} else {
						err = testOutput.VerifyAnyOrder()
					}

					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

// benchmarkAggregateFunction runs aggregator microbenchmarks. numGroupCol is
// the number of grouping columns. groupSize is the number of tuples to target
// in each distinct aggregation group. chunkSize is the number of tuples to
// target in each distinct partially ordered group column, and is intended for
// use with partial order. Limit is the number of rows to retrieve from the
// aggregation function before ending the microbenchmark.
func benchmarkAggregateFunction(
	b *testing.B,
	agg aggType,
	aggFn execinfrapb.AggregatorSpec_Func,
	aggInputTypes []*types.T,
	numGroupCol int,
	groupSize int,
	distinctProb float64,
	numInputRows int,
	chunkSize int,
	limit int,
) {
	defer log.Scope(b).Close(b)
	if groupSize > numInputRows {
		// In this case all tuples will be part of the same group, and we have
		// likely already benchmarked such scenario with this value of
		// numInputRows, so we short-circuit.
		return
	}
	if numGroupCol < 1 {
		// We should always have at least one group column.
		return
	}
	if agg.order == partial {
		if chunkSize > numInputRows || groupSize > chunkSize {
			return
		}
	}
	rng, _ := randutil.NewTestRand()
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)
	aggMemAcc := evalCtx.Mon.MakeBoundAccount()
	defer aggMemAcc.Close(ctx)
	evalCtx.SingleDatumAggMemAccount = &aggMemAcc
	const bytesFixedLength = 8
	typs := []*types.T{types.Int}
	groupCols := []uint32{0}
	for g := 1; g < numGroupCol; g++ {
		typs = append(typs, types.Int)
		groupCols = append(groupCols, uint32(g))
	}
	typs = append(typs, aggInputTypes...)
	cols := make([]coldata.Vec, len(typs))
	for i := range typs {
		cols[i] = testAllocator.NewMemColumn(typs[i], numInputRows)
	}
	groups := cols[0].Int64()
	if agg.order == ordered {
		curGroup := -1
		for i := 0; i < numInputRows; i++ {
			if i%groupSize == 0 {
				curGroup++
			}
			groups[i] = int64(curGroup)
		}
	} else if agg.order == unordered {
		numGroups := numInputRows / groupSize
		for i := 0; i < numInputRows; i++ {
			groups[i] = int64(rng.Intn(numGroups))
		}
	} else {
		// partial order.
		chunks := cols[0].Int64()
		groups = cols[1].Int64()
		curChunk := -1
		numGroups := chunkSize / groupSize
		for i := 0; i < numInputRows; i++ {
			if i%chunkSize == 0 {
				curChunk++
			}
			chunks[i] = int64(curChunk)
			groups[i] = int64(rng.Intn(numGroups))
		}
	}
	for _, col := range cols[numGroupCol:] {
		coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
			Rand:             rng,
			Vec:              col,
			N:                numInputRows,
			NullProbability:  0,
			BytesFixedLength: bytesFixedLength,
		})
	}
	if aggFn == execinfrapb.SumInt {
		// Integer summation of random Int64 values can lead
		// to overflow, and we will panic. To go around it, we
		// restrict the range of values.
		vals := cols[numGroupCol].Int64()
		for i := range vals {
			vals[i] = vals[i] % 1024
		}
	}
	source := colexectestutils.NewChunkingBatchSource(testAllocator, typs, cols, numInputRows)

	aggCols := make([]uint32, len(aggInputTypes))
	for i := range aggCols {
		aggCols[i] = uint32(numGroupCol + i)
	}
	tc := aggregatorTestCase{
		typs:           typs,
		groupCols:      groupCols,
		aggCols:        [][]uint32{aggCols},
		aggFns:         []execinfrapb.AggregatorSpec_Func{aggFn},
		unorderedInput: agg.order == unordered,
	}
	if distinctProb > 0 {
		if !typs[0].Identical(types.Int) {
			skip.IgnoreLint(b, "benchmarking distinct aggregation is supported only on an INT argument")
		}
		tc.aggDistinct = []bool{true}
		distinctModulo := int64(1.0 / distinctProb)
		vals := cols[1].Int64()
		for i := range vals {
			vals[i] = vals[i] % distinctModulo
		}
	}
	if agg.order == partial {
		tc.orderedCols = []uint32{0}
	}
	require.NoError(b, tc.init())
	constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
		&evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
	)
	require.NoError(b, err)
	fName := execinfrapb.AggregatorSpec_Func_name[int32(aggFn)]
	// Only count the aggregation columns.
	var argumentsSize int
	if len(aggInputTypes) > 0 {
		for _, typ := range aggInputTypes {
			if typ.Identical(types.Bool) {
				argumentsSize++
			} else {
				argumentsSize += 8
			}
		}
	} else {
		// For COUNT_ROWS we'll just use 8 bytes.
		argumentsSize = 8
	}
	var inputTypesString string
	switch len(aggInputTypes) {
	case 1:
		// Override the string so that the name of the benchmark was the same
		// as in pre-20.2 releases (which allows us to compare against old
		// numbers).
		inputTypesString = aggInputTypes[0].String()
	default:
		inputTypesString = fmt.Sprintf("%s", aggInputTypes)
	}
	distinctProbString := ""
	if distinctProb > 0 {
		distinctProbString = fmt.Sprintf("/distinctProb=%.2f", distinctProb)
	}
	b.Run(fmt.Sprintf(
		"%s/%s/%s/groupSize=%d%s/numInputRows=%d",
		fName, agg.name, inputTypesString, groupSize, distinctProbString, numInputRows),
		func(b *testing.B) {
			b.SetBytes(int64(argumentsSize * numInputRows))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a := agg.new(&colexecagg.NewAggregatorArgs{
					Allocator:      testAllocator,
					MemAccount:     testMemAcc,
					Input:          source,
					InputTypes:     tc.typs,
					Spec:           tc.spec,
					EvalCtx:        &evalCtx,
					Constructors:   constructors,
					ConstArguments: constArguments,
					OutputTypes:    outputTypes,
				})
				a.Init(ctx)
				// Exhaust aggregator until all batches have been read or limit, if
				// non-zero, is reached.
				tupleCount := 0
				for b := a.Next(); b.Length() != 0; b = a.Next() {
					tupleCount += b.Length()
					if limit > 0 && tupleCount >= limit {
						break
					}
				}
				if err = a.(colexecop.Closer).Close(ctx); err != nil {
					b.Fatal(err)
				}
				source.Reset(ctx)
			}
		},
	)
}

// BenchmarkAggregator runs the benchmark both aggregators with diverse data
// source parameters but using a single aggregate function. The goal of this
// benchmark is measuring the performance of the aggregators themselves
// depending on the parameters of the input.
func BenchmarkAggregator(b *testing.B) {
	numRows := []int{1, 32, coldata.BatchSize(), 32 * coldata.BatchSize(), 1024 * coldata.BatchSize()}
	groupSizes := []int{1, 2, 32, 128, coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{32, 32 * coldata.BatchSize()}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	for _, aggFn := range []execinfrapb.AggregatorSpec_Func{
		// We choose any_not_null aggregate function because it is the simplest
		// possible and, thus, its Compute function call will have the least
		// impact when benchmarking the aggregator logic.
		execinfrapb.AnyNotNull,
		// min aggregate function has been used before transitioning to
		// any_not_null in 22.1 cycle. It is kept so that we could use it for
		// comparison of 22.1 against 21.2.
		// TODO(yuzefovich): use only any_not_null in 22.2 (#75106).
		execinfrapb.Min,
	} {
		for _, agg := range aggTypes {
			for _, numInputRows := range numRows {
				for _, groupSize := range groupSizes {
					benchmarkAggregateFunction(
						b, agg, aggFn, []*types.T{types.Int}, 1, /* numGroupCol */
						groupSize, 0 /* distinctProb */, numInputRows,
						0 /* chunkSize */, 0 /* limit */)
				}
			}
		}
	}
}

// BenchmarkAllOptimizedAggregateFunctions runs the benchmark of all optimized
// aggregate functions in 4 configurations (hash vs ordered, and small groups
// vs big groups). Such configurations were chosen since they provide good
// enough signal on the speeds of aggregate functions. For more diverse
// configurations look at BenchmarkAggregator.
func BenchmarkAllOptimizedAggregateFunctions(b *testing.B) {
	var numInputRows = 32 * coldata.BatchSize()
	numFnsToRun := len(execinfrapb.AggregatorSpec_Func_name)
	if testing.Short() {
		numFnsToRun = 1
	}
	for aggFnNumber := 0; aggFnNumber < numFnsToRun; aggFnNumber++ {
		aggFn := execinfrapb.AggregatorSpec_Func(aggFnNumber)
		if !colexecagg.IsAggOptimized(aggFn) {
			continue
		}
		for _, agg := range aggTypes {
			var aggInputTypes []*types.T
			switch aggFn {
			case execinfrapb.BoolAnd, execinfrapb.BoolOr:
				aggInputTypes = []*types.T{types.Bool}
			case execinfrapb.ConcatAgg:
				aggInputTypes = []*types.T{types.Bytes}
			case execinfrapb.CountRows:
			default:
				aggInputTypes = []*types.T{types.Int}
			}
			for _, groupSize := range []int{1, coldata.BatchSize()} {
				benchmarkAggregateFunction(b, agg, aggFn, aggInputTypes,
					1 /* numGroupCol */, groupSize,
					0 /* distinctProb */, numInputRows,
					0 /* chunkSize */, 0 /* limit */)
			}
		}
	}
}

func BenchmarkDistinctAggregation(b *testing.B) {
	aggFn := execinfrapb.Count
	for _, agg := range aggTypes {
		for _, numInputRows := range []int{32, 32 * coldata.BatchSize()} {
			for _, groupSize := range []int{1, 2, 32, 128, coldata.BatchSize()} {
				for _, distinctProb := range []float64{0.01, 0.1, 1.0} {
					distinctModulo := int(1.0 / distinctProb)
					if (groupSize == 1 && distinctProb != 1.0) || float64(groupSize)/float64(distinctModulo) < 0.1 {
						// We have a such combination of groupSize and distinctProb
						// parameters that we will be very unlikely to satisfy them
						// (for example, with groupSize=1 and distinctProb=0.01,
						// every value will be distinct within the group), so we
						// skip such configuration.
						continue
					}
					benchmarkAggregateFunction(b, agg, aggFn, []*types.T{types.Int},
						1 /* numGroupCol */, groupSize,
						0 /* distinctProb */, numInputRows,
						0 /* chunkSize */, 0 /* limit */)
				}
			}
		}
	}
}

func min64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
