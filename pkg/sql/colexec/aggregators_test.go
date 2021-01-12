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
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
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

// TODO(yuzefovich): introduce nicer aliases for the protobuf generated ones for
// the aggregate functions and use those throughout the codebase.

var (
	defaultGroupCols = []uint32{0}
	defaultAggCols   = [][]uint32{{1}}
	defaultAggFns    = []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM_INT}
	defaultTyps      = []*types.T{types.Int, types.Int}
)

type aggregatorTestCase struct {
	// typs, aggFns, groupCols, and aggCols will be set to their default
	// values before running a test if nil.
	typs           []*types.T
	aggFns         []execinfrapb.AggregatorSpec_Func
	groupCols      []uint32
	aggCols        [][]uint32
	constArguments [][]execinfrapb.Expression
	// spec will be populated during init().
	spec           *execinfrapb.AggregatorSpec
	aggDistinct    []bool
	aggFilter      []int
	input          tuples
	unorderedInput bool
	expected       tuples
	name           string

	// convToDecimal will convert any float64s to apd.Decimals. If a string is
	// encountered, a best effort is made to convert that string to an
	// apd.Decimal.
	convToDecimal bool
}

// aggType is a helper struct that allows tests to test both the ordered and
// hash aggregators at the same time.
type aggType struct {
	new  func(*colexecagg.NewAggregatorArgs) (ResettableOperator, error)
	name string
}

var aggTypes = []aggType{
	{
		// This is a wrapper around NewHashAggregator so its signature is
		// compatible with NewOrderedAggregator.
		new: func(args *colexecagg.NewAggregatorArgs) (ResettableOperator, error) {
			return NewHashAggregator(args, nil /* newSpillingQueueArgs */)
		},
		name: "hash",
	},
	{
		new:  NewOrderedAggregator,
		name: "ordered",
	},
}

func (tc *aggregatorTestCase) init() error {
	if tc.convToDecimal {
		for _, tuples := range []tuples{tc.input, tc.expected} {
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
	if tc.groupCols == nil {
		tc.groupCols = defaultGroupCols
	}
	if tc.aggFns == nil {
		tc.aggFns = defaultAggFns
	}
	if tc.aggCols == nil {
		tc.aggCols = defaultAggCols
	}
	if tc.typs == nil {
		tc.typs = defaultTyps
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
	return nil
}

var aggregatorsTestCases = []aggregatorTestCase{
	{
		input: tuples{
			{0, 1},
		},
		expected: tuples{
			{1},
		},
		name: "OneTuple",
	},
	{
		input: tuples{
			{0, 1},
			{0, 1},
		},
		expected: tuples{
			{2},
		},
		name: "OneGroup",
	},
	{
		input: tuples{
			{0, 1},
			{0, 0},
			{0, 1},
			{1, 4},
			{2, 5},
		},
		expected: tuples{
			{2},
			{4},
			{5},
		},
		name: "MultiGroup",
	},
	{
		input: tuples{
			{0, 1},
			{0, 2},
			{0, 3},
			{1, 4},
			{1, 5},
		},
		expected: tuples{
			{6},
			{9},
		},
		name: "CarryBetweenInputBatches",
	},
	{
		input: tuples{
			{0, 1},
			{0, 2},
			{0, 3},
			{0, 4},
			{1, 5},
			{2, 6},
		},
		expected: tuples{
			{10},
			{5},
			{6},
		},
		name: "CarryBetweenOutputBatches",
	},
	{
		input: tuples{
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
		expected: tuples{
			{2},
			{2},
			{6},
			{8},
			{5},
			{6},
			{7},
			{8},
		},
		name: "CarryBetweenInputAndOutputBatches",
	},
	{
		input: tuples{
			{0, 1},
			{0, 2},
			{0, 3},
			{0, 4},
		},
		expected: tuples{
			{10},
		},
		name:      "NoGroupingCols",
		groupCols: []uint32{},
	},
	{
		input: tuples{
			{1, 0, 0},
			{2, 0, 0},
			{3, 0, 0},
			{4, 0, 0},
		},
		expected: tuples{
			{10},
		},
		name:      "UnusedInputColumns",
		typs:      []*types.T{types.Int, types.Int, types.Int},
		groupCols: []uint32{1, 2},
		aggCols:   [][]uint32{{0}},
	},
	{
		input: tuples{
			{nil, 1},
			{4, 42},
			{nil, 2},
		},
		expected: tuples{
			{3},
			{42},
		},
		name:           "UnorderedWithNullsInGroupingCol",
		unorderedInput: true,
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_COUNT_ROWS,
		},
		aggCols:        [][]uint32{{0}, {}},
		typs:           []*types.T{types.Int},
		unorderedInput: true,
		input: tuples{
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
		expected: tuples{
			{nil, 2},
			{1, 4},
			{2, 3},
			{3, 2},
			{4, 2},
		},
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM_INT, execinfrapb.AggregatorSpec_SUM_INT},
		aggCols: [][]uint32{
			{2}, {1},
		},
		input: tuples{
			{0, 1, 2},
			{0, 1, 2},
		},
		typs: []*types.T{types.Int, types.Int, types.Int},
		expected: tuples{
			{4, 2},
		},
		name: "OutputOrder",
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM_INT},
		aggCols: [][]uint32{
			{2}, {1},
		},
		input: tuples{
			{0, 1, 1.3},
			{0, 1, 1.6},
			{0, 1, 0.5},
			{1, 1, 1.2},
		},
		typs: []*types.T{types.Int, types.Int, types.Decimal},
		expected: tuples{
			{3.4, 3},
			{1.2, 1},
		},
		name:          "SumMultiType",
		convToDecimal: true,
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_SUM},
		aggCols: [][]uint32{
			{1}, {1},
		},
		input: tuples{
			{0, 1.1},
			{0, 1.2},
			{0, 2.3},
			{1, 6.21},
			{1, 2.43},
		},
		typs: []*types.T{types.Int, types.Decimal},
		expected: tuples{
			{"1.5333333333333333333", 4.6},
			{4.32, 8.64},
		},
		name:          "AvgSumSingleInputBatch",
		convToDecimal: true,
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_BOOL_AND,
			execinfrapb.AggregatorSpec_BOOL_OR,
		},
		aggCols: [][]uint32{
			{1}, {1},
		},
		input: tuples{
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
		typs: []*types.T{types.Int, types.Bool},
		expected: tuples{
			{true, true},
			{false, false},
			{false, true},
			{true, true},
			{false, false},
			{false, false},
			{true, true},
			{false, true},
			{nil, nil},
		},
		name: "BoolAndOrBatch",
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_MIN,
			execinfrapb.AggregatorSpec_SUM,
		},
		input: tuples{
			{2, 1.0, "1.0", 2.0},
			{2, 1.0, "1.0", 4.0},
			{2, 2.0, "2.0", 6.0},
		},
		expected: tuples{
			{2, 1.0, "1.0", 2.0, 6.0},
			{2, 2.0, "2.0", 6.0, 6.0},
		},
		typs:      []*types.T{types.Int, types.Decimal, types.Bytes, types.Decimal},
		name:      "MultiGroupColsWithPointerTypes",
		groupCols: []uint32{0, 1, 2},
		aggCols: [][]uint32{
			{0}, {1}, {2}, {3}, {3},
		},
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_SUM_INT,
		},
		input: tuples{
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 0), -1},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 1), 1},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 1), 2},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 2), 10},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 2), 11},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 3), 100},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 3), 101},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 4), 102},
		},
		expected: tuples{
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 0), -1},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 1), 3},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 2), 21},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 3), 201},
			{tree.NewDTimeTZFromOffset(timeofday.FromInt(0), 4), 102},
		},
		typs:      []*types.T{types.TimeTZ, types.Int},
		name:      "GroupOnTimeTZColumns",
		groupCols: []uint32{0},
		aggCols: [][]uint32{
			{0}, {1},
		},
	},
	{
		input: tuples{
			{0, nil, 1, 1, 1.0, 1.0, duration.MakeDuration(1, 1, 1)},
			{0, 1, nil, 2, 2.0, 2.0, duration.MakeDuration(2, 2, 2)},
			{0, 2, 2, nil, 3.0, 3.0, duration.MakeDuration(3, 3, 3)},
			{0, 3, 3, 3, nil, 4.0, duration.MakeDuration(4, 4, 4)},
			{0, 4, 4, 4, 4.0, nil, duration.MakeDuration(5, 5, 5)},
			{0, 5, 5, 5, 5.0, 5.0, nil},
		},
		expected: tuples{
			{3.0, 3.0, 3.0, 3.0, 3.0, duration.MakeDuration(3, 3, 3)},
		},
		typs:    []*types.T{types.Int, types.Int2, types.Int4, types.Int, types.Decimal, types.Float, types.Interval},
		aggFns:  []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_AVG, execinfrapb.AggregatorSpec_AVG},
		aggCols: [][]uint32{{1}, {2}, {3}, {4}, {5}, {6}},
		name:    "AVG on all types",
	},
	{
		input: tuples{
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
		expected: tuples{
			{"123"},
			{"12"},
			{"12"},
			{nil},
		},
		typs:      []*types.T{types.Int, types.Bytes},
		aggFns:    []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_CONCAT_AGG},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{{1}},
	},
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_AVG,
			execinfrapb.AggregatorSpec_COUNT_ROWS,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_SUM,
			execinfrapb.AggregatorSpec_SUM_INT,
			execinfrapb.AggregatorSpec_MIN,
			execinfrapb.AggregatorSpec_MAX,
			execinfrapb.AggregatorSpec_BOOL_AND,
			execinfrapb.AggregatorSpec_BOOL_OR,
			execinfrapb.AggregatorSpec_CONCAT_AGG,
		},
		aggCols: [][]uint32{{0}, {4}, {1}, {}, {1}, {1}, {2}, {2}, {2}, {3}, {3}, {4}},
		typs:    []*types.T{types.Int, types.Decimal, types.Int, types.Bool, types.Bytes},
		input: tuples{
			{0, 3.1, 2, true, "zero"},
			{0, 1.1, 3, false, "zero"},
			{1, 1.1, 1, false, "one"},
			{1, 4.1, 0, false, "one"},
			{2, 1.1, 1, true, "two"},
			{3, 4.1, 0, false, "three"},
			{3, 5.1, 0, true, "three"},
		},
		expected: tuples{
			{0, "zero", 2.1, 2, 2, 4.2, 5, 2, 3, false, true, "zerozero"},
			{1, "one", 2.6, 2, 2, 5.2, 1, 0, 1, false, false, "oneone"},
			{2, "two", 1.1, 1, 1, 1.1, 1, 1, 1, true, true, "two"},
			{3, "three", 4.6, 2, 2, 9.2, 0, 0, 0, false, true, "threethree"},
		},
		convToDecimal: true,
	},

	// Test case for null handling.
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_COUNT_ROWS,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_SUM,
			execinfrapb.AggregatorSpec_SUM_INT,
			execinfrapb.AggregatorSpec_MIN,
			execinfrapb.AggregatorSpec_MAX,
			execinfrapb.AggregatorSpec_AVG,
			execinfrapb.AggregatorSpec_BOOL_AND,
			execinfrapb.AggregatorSpec_BOOL_OR,
			execinfrapb.AggregatorSpec_CONCAT_AGG,
		},
		aggCols: [][]uint32{{0}, {1}, {}, {1}, {1}, {2}, {2}, {2}, {1}, {3}, {3}, {4}},
		typs:    []*types.T{types.Int, types.Decimal, types.Int, types.Bool, types.Bytes},
		input: tuples{
			{nil, 1.1, 4, true, "a"},
			{0, nil, nil, nil, nil},
			{0, 3.1, 5, nil, "b"},
			{1, nil, nil, nil, nil},
			{1, nil, nil, false, nil},
		},
		expected: tuples{
			{nil, 1.1, 1, 1, 1.1, 4, 4, 4, 1.1, true, true, "a"},
			{0, 3.1, 2, 1, 3.1, 5, 5, 5, 3.1, nil, nil, "b"},
			{1, nil, 2, 0, nil, nil, nil, nil, nil, false, false, nil},
		},
		convToDecimal: true,
	},

	// Test DISTINCT aggregation.
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_SUM_INT,
			execinfrapb.AggregatorSpec_SUM_INT,
		},
		aggCols:     [][]uint32{{0}, {1}, {1}, {1}, {1}},
		aggDistinct: []bool{false, false, true, false, true},
		typs:        []*types.T{types.Int, types.Int},
		input: tuples{
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
		expected: tuples{
			{0, 4, 2, 6, 3},
			{1, 3, 2, 5, 3},
		},
	},

	// Test aggregation with FILTERs.
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_COUNT_ROWS,
			execinfrapb.AggregatorSpec_SUM_INT,
		},
		aggCols:   [][]uint32{{0}, {}, {1}},
		aggFilter: []int{tree.NoColumnIdx, 2, 2},
		typs:      []*types.T{types.Int, types.Int, types.Bool},
		input: tuples{
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
		expected: tuples{
			{0, 3, 4},
			{1, 2, 3},
		},
	},

	// Test aggregation with FILTERs when the whole groups are filtered out.
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_COUNT_ROWS,
			execinfrapb.AggregatorSpec_SUM_INT,
		},
		aggCols:   [][]uint32{{0}, {}, {1}},
		aggFilter: []int{tree.NoColumnIdx, 2, 2},
		typs:      []*types.T{types.Int, types.Int, types.Bool},
		input: tuples{
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
		expected: tuples{
			{0, 0, nil},
			{1, 2, 3},
			{2, 0, nil},
		},
	},

	// Test aggregation with FILTERs and DISTINCTs intertwined.
	{
		aggFns: []execinfrapb.AggregatorSpec_Func{
			execinfrapb.AggregatorSpec_ANY_NOT_NULL,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_COUNT,
			execinfrapb.AggregatorSpec_SUM_INT,
			execinfrapb.AggregatorSpec_SUM_INT,
			execinfrapb.AggregatorSpec_SUM_INT,
		},
		aggCols:     [][]uint32{{0}, {1}, {1}, {1}, {1}, {1}, {1}},
		aggDistinct: []bool{false, false, true, true, false, true, true},
		aggFilter:   []int{tree.NoColumnIdx, 2, tree.NoColumnIdx, 2, 2, tree.NoColumnIdx, 2},
		typs:        []*types.T{types.Int, types.Int, types.Bool},
		input: tuples{
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
		expected: tuples{
			{0, 2, 2, 1, 4, 3, 2},
			{1, 2, 2, 2, 3, 3, 3},
		},
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
			if tc.unorderedInput && agg.name == "ordered" {
				// This test case has unordered input, so we skip ordered
				// aggregator.
				continue
			}
			if agg.name != "hash" && tc.aggFilter != nil {
				// Filtering aggregation is only supported with hash aggregator.
				continue
			}
			log.Infof(ctx, "%s/%s", tc.name, agg.name)
			verifier := orderedVerifier
			if agg.name == "hash" {
				verifier = unorderedVerifier
			}
			runTestsWithTyps(t, []tuples{tc.input}, [][]*types.T{tc.typs}, tc.expected, verifier,
				func(input []colexecbase.Operator) (colexecbase.Operator, error) {
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
					})
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
	rng, _ := randutil.NewPseudoRand()
	for _, groupSize := range []int{1, 2, coldata.BatchSize() / 4, coldata.BatchSize() / 2} {
		if groupSize == 0 {
			// We might be varying coldata.BatchSize() so that when it is divided by
			// 4, groupSize is 0. We want to skip such configuration.
			continue
		}
		for _, numInputBatches := range []int{1, 2, 64} {
			for _, hasNulls := range []bool{true, false} {
				for _, agg := range aggTypes {
					log.Infof(context.Background(), "%s/groupSize=%d/numInputBatches=%d/hasNulls=%t", agg.name, groupSize, numInputBatches, hasNulls)
					nTuples := coldata.BatchSize() * numInputBatches
					typs := []*types.T{types.Int, types.Float}
					cols := []coldata.Vec{
						testAllocator.NewMemColumn(typs[0], nTuples),
						testAllocator.NewMemColumn(typs[1], nTuples),
					}
					groups, aggCol, aggColNulls := cols[0].Int64(), cols[1].Float64(), cols[1].Nulls()
					expectedTuples := tuples{}

					var expRowCounts, expCounts []int64
					var expSums, expMins, expMaxs []float64
					// SUM, MIN, MAX, and AVG aggregators can output null.
					var expNulls []bool
					curGroup := -1
					for i := range groups {
						if i%groupSize == 0 {
							if curGroup != -1 {
								if expNulls[curGroup] {
									expectedTuples = append(expectedTuples, tuple{
										expRowCounts[curGroup], expCounts[curGroup], nil, nil, nil, nil,
									})
								} else {
									expectedTuples = append(expectedTuples, tuple{
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
						expectedTuples = append(expectedTuples, tuple{
							expRowCounts[curGroup], expCounts[curGroup], nil, nil, nil, nil,
						})
					} else {
						expectedTuples = append(expectedTuples, tuple{
							expRowCounts[curGroup], expCounts[curGroup], expSums[curGroup], expMins[curGroup], expMaxs[curGroup], expSums[curGroup] / float64(expCounts[curGroup]),
						})
					}

					source := newChunkingBatchSource(typs, cols, nTuples)
					tc := aggregatorTestCase{
						typs: typs,
						aggFns: []execinfrapb.AggregatorSpec_Func{
							execinfrapb.AggregatorSpec_COUNT_ROWS,
							execinfrapb.AggregatorSpec_COUNT,
							execinfrapb.AggregatorSpec_SUM,
							execinfrapb.AggregatorSpec_MIN,
							execinfrapb.AggregatorSpec_MAX,
							execinfrapb.AggregatorSpec_AVG,
						},
						groupCols: []uint32{0},
						aggCols:   [][]uint32{{}, {1}, {1}, {1}, {1}, {1}},
					}
					require.NoError(t, tc.init())
					constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
						&evalCtx, nil /* semaCtx */, tc.spec.Aggregations, tc.typs,
					)
					require.NoError(t, err)
					a, err := agg.new(&colexecagg.NewAggregatorArgs{
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
					if err != nil {
						t.Fatal(err)
					}
					a.Init()

					testOutput := newOpTestOutput(a, expectedTuples)
					if strings.Contains(agg.name, "hash") {
						err = testOutput.VerifyAnyOrder()
					} else {
						err = testOutput.Verify()
					}

					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

func benchmarkAggregateFunction(
	b *testing.B,
	agg aggType,
	aggFn execinfrapb.AggregatorSpec_Func,
	aggInputTypes []*types.T,
	groupSize int,
	distinctProb float64,
	numInputRows int,
) {
	if groupSize > numInputRows {
		// In this case all tuples will be part of the same group, and we have
		// likely already benchmarked such scenario with this value of
		// numInputRows, so we short-circuit.
		return
	}
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)
	aggMemAcc := evalCtx.Mon.MakeBoundAccount()
	defer aggMemAcc.Close(ctx)
	evalCtx.SingleDatumAggMemAccount = &aggMemAcc
	const bytesFixedLength = 8
	typs := append([]*types.T{types.Int}, aggInputTypes...)
	cols := make([]coldata.Vec, len(typs))
	for i := range typs {
		cols[i] = testAllocator.NewMemColumn(typs[i], numInputRows)
	}
	groups := cols[0].Int64()
	if agg.name == "hash" {
		numGroups := numInputRows / groupSize
		for i := 0; i < numInputRows; i++ {
			groups[i] = int64(rng.Intn(numGroups))
		}
	} else {
		curGroup := -1
		for i := 0; i < numInputRows; i++ {
			if groupSize == 1 || i%groupSize == 0 {
				curGroup++
			}
			groups[i] = int64(curGroup)
		}
	}
	for _, col := range cols[1:] {
		coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
			Rand:             rng,
			Vec:              col,
			N:                numInputRows,
			NullProbability:  0,
			BytesFixedLength: bytesFixedLength,
		})
	}
	if aggFn == execinfrapb.AggregatorSpec_SUM_INT {
		// Integer summation of random Int64 values can lead
		// to overflow, and we will panic. To go around it, we
		// restrict the range of values.
		vals := cols[1].Int64()
		for i := range vals {
			vals[i] = vals[i] % 1024
		}
	}
	source := newChunkingBatchSource(typs, cols, numInputRows)

	aggCols := make([]uint32, len(aggInputTypes))
	for i := range aggCols {
		aggCols[i] = uint32(i + 1)
	}
	tc := aggregatorTestCase{
		typs:      typs,
		aggFns:    []execinfrapb.AggregatorSpec_Func{aggFn},
		groupCols: []uint32{0},
		aggCols:   [][]uint32{aggCols},
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
				a, err := agg.new(&colexecagg.NewAggregatorArgs{
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
				if err != nil {
					b.Fatal(err)
				}
				a.Init()
				// Exhaust aggregator until all batches have been read.
				for b := a.Next(ctx); b.Length() != 0; b = a.Next(ctx) {
				}
				if err = a.(colexecbase.Closer).Close(ctx); err != nil {
					b.Fatal(err)
				}
				source.reset(ctx)
			}
		},
	)
}

// BenchmarkAggregator runs the benchmark both aggregators with diverse data
// source parameters but using a single aggregate function. The goal of this
// benchmark is measuring the performance of the aggregators themselves
// depending on the parameters of the input.
func BenchmarkAggregator(b *testing.B) {
	aggFn := execinfrapb.AggregatorSpec_MIN
	numRows := []int{1, 32, coldata.BatchSize(), 32 * coldata.BatchSize(), 1024 * coldata.BatchSize()}
	groupSizes := []int{1, 2, 32, 128, coldata.BatchSize()}
	if testing.Short() {
		numRows = []int{32, 32 * coldata.BatchSize()}
		groupSizes = []int{1, coldata.BatchSize()}
	}
	for _, agg := range aggTypes {
		for _, numInputRows := range numRows {
			for _, groupSize := range groupSizes {
				benchmarkAggregateFunction(
					b, agg, aggFn, []*types.T{types.Int}, groupSize,
					0 /* distinctProb */, numInputRows,
				)
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
			case execinfrapb.AggregatorSpec_BOOL_AND, execinfrapb.AggregatorSpec_BOOL_OR:
				aggInputTypes = []*types.T{types.Bool}
			case execinfrapb.AggregatorSpec_CONCAT_AGG:
				aggInputTypes = []*types.T{types.Bytes}
			case execinfrapb.AggregatorSpec_COUNT_ROWS:
			default:
				aggInputTypes = []*types.T{types.Int}
			}
			for _, groupSize := range []int{1, coldata.BatchSize()} {
				benchmarkAggregateFunction(
					b, agg, aggFn, aggInputTypes, groupSize,
					0 /* distinctProb */, numInputRows,
				)
			}
		}
	}
}

func BenchmarkDistinctAggregation(b *testing.B) {
	aggFn := execinfrapb.AggregatorSpec_COUNT
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
					benchmarkAggregateFunction(
						b, agg, aggFn, []*types.T{types.Int}, groupSize, distinctProb, numInputRows,
					)
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
