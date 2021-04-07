// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type aggTestSpec struct {
	// The name of the aggregate function.
	fname    string
	distinct bool
	// The column indices of the arguments to the function.
	colIdx       []uint32
	filterColIdx *uint32
}

func aggregations(aggTestSpecs []aggTestSpec) []execinfrapb.AggregatorSpec_Aggregation {
	agg := make([]execinfrapb.AggregatorSpec_Aggregation, len(aggTestSpecs))
	for i, spec := range aggTestSpecs {
		agg[i].Func = execinfrapb.AggregatorSpec_Func(execinfrapb.AggregatorSpec_Func_value[spec.fname])
		agg[i].Distinct = spec.distinct
		agg[i].ColIdx = spec.colIdx
		agg[i].FilterColIdx = spec.filterColIdx
	}
	return agg
}

// TODO(irfansharif): Add tests to verify the following aggregation functions:
//      AVG
//      BOOL_AND
//      BOOL_OR
//      CONCAT_AGG
//      JSON_AGG
//      JSONB_AGG
//      STDDEV
//      VARIANCE
func TestAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		col0              = []uint32{0}
		col1              = []uint32{1}
		col2              = []uint32{2}
		filterCol1 uint32 = 1
		filterCol3 uint32 = 3
	)

	testCases := []ProcessorTestCase{
		{
			// SELECT min(@0), max(@0), count(@0), avg(@0), sum(@0), stddev(@0),
			// variance(@0) GROUP BY [] (no rows).
			Name: "MinMaxCountAvgSumStddevGroupByNoneNoRows",
			Input: ProcessorTestCaseRows{
				Rows:  [][]interface{}{},
				Types: types.MakeIntCols(1),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{nil, nil, 0, nil, nil, nil, nil},
				},
				Types: []*types.T{types.Int, types.Int, types.Int, types.Decimal, types.Decimal, types.Decimal, types.Decimal},
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					Aggregations: aggregations([]aggTestSpec{
						{fname: "MIN", colIdx: col0},
						{fname: "MAX", colIdx: col0},
						{fname: "COUNT", colIdx: col0},
						{fname: "AVG", colIdx: col0},
						{fname: "SUM", colIdx: col0},
						{fname: "STDDEV", colIdx: col0},
						{fname: "VARIANCE", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT @2, count(@1), GROUP BY @2.
			Name: "CountGroupByWithNull",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, 2},
					{3, nil},
					{6, 2},
					{7, 2},
					{8, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{nil, 1},
					{4, 1},
					{2, 3},
				},
				Types: types.MakeIntCols(2),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					GroupCols: col1,
					Aggregations: aggregations([]aggTestSpec{
						{fname: "ANY_NOT_NULL", colIdx: col1},
						{fname: "COUNT", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT @2, count(@1), GROUP BY @2.
			Name: "CountGroupBy",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, 2},
					{3, 4},
					{6, 2},
					{7, 2},
					{8, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{4, 2},
					{2, 3},
				},
				Types: types.MakeIntCols(2),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					GroupCols: col1,
					Aggregations: aggregations([]aggTestSpec{
						{fname: "ANY_NOT_NULL", colIdx: col1},
						{fname: "COUNT", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT @2, count(@1), GROUP BY @2 (ordering: @2+).
			Name: "CountGroupByOrderBy",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, 2},
					{6, 2},
					{7, 2},
					{3, 4},
					{8, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{2, 3},
					{4, 2},
				},
				Types: types.MakeIntCols(2),
			},
			DisableSort: true,
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					OrderedGroupCols: col1,
					GroupCols:        col1,
					Aggregations: aggregations([]aggTestSpec{
						{fname: "ANY_NOT_NULL", colIdx: col1},
						{fname: "COUNT", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT @2, sum(@1), GROUP BY @2.
			Name: "SumGroupBy",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, 2},
					{3, 4},
					{6, 2},
					{7, 2},
					{8, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{2, 14},
					{4, 11},
				},
				Types: []*types.T{types.Int, types.Decimal},
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					GroupCols: col1,
					Aggregations: aggregations([]aggTestSpec{
						{fname: "ANY_NOT_NULL", colIdx: col1},
						{fname: "SUM", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT @2, sum(@1), GROUP BY @2 (ordering: @2+).
			Name: "SumGroupByOrderBy",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, 2},
					{6, 2},
					{7, 2},
					{8, 4},
					{3, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{2, 14},
					{4, 11},
				},
				Types: []*types.T{types.Int, types.Decimal},
			},
			DisableSort: true,
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					GroupCols:        col1,
					OrderedGroupCols: col1,
					Aggregations: aggregations([]aggTestSpec{
						{fname: "ANY_NOT_NULL", colIdx: col1},
						{fname: "SUM", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT count(@1), sum(@1), GROUP BY [] (empty group key).
			Name: "CountSumGroupByNone",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, 2},
					{1, 4},
					{3, 2},
					{4, 2},
					{5, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{5, 14},
				},
				Types: []*types.T{types.Int, types.Decimal},
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					Aggregations: aggregations([]aggTestSpec{
						{fname: "COUNT", colIdx: col0},
						{fname: "SUM", colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT SUM DISTINCT (@1), GROUP BY [] (empty group key).
			Name: "SumdistinctGroupByNone",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{2},
					{4},
					{2},
					{2},
					{4},
				},
				Types: types.MakeIntCols(1),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{6},
				},
				Types: types.MakeIntCols(1),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					Aggregations: aggregations([]aggTestSpec{
						{fname: "SUM", distinct: true, colIdx: col0},
					}),
				},
			},
		},
		{
			// SELECT (@1), GROUP BY [] (empty group key).
			Name: "GroupByNone",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1},
					{1},
					{1},
				},
				Types: types.MakeIntCols(1),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1},
				},
				Types: types.MakeIntCols(1),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					Aggregations: aggregations([]aggTestSpec{
						{fname: "ANY_NOT_NULL", colIdx: col0},
					}),
				},
			},
		},
		{
			Name: "MaxMinCountCountdistinctGroupByNone",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{2, 2},
					{1, 4},
					{3, 2},
					{4, 2},
					{5, 4},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{5, 2, 5, 2},
				},
				Types: types.MakeIntCols(4),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					Aggregations: aggregations([]aggTestSpec{
						{fname: "MAX", colIdx: col0},
						{fname: "MIN", colIdx: col1},
						{fname: "COUNT", colIdx: col1},
						{fname: "COUNT", distinct: true, colIdx: col1},
					}),
				},
			},
		},
		{
			Name: "MaxfilterCountfilterCountrowsfilter",
			Input: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{1, true, 1, true},
					{5, false, 1, false},
					{2, true, 1, nil},
					{3, nil, 1, true},
					{2, true, 1, true},
				},
				Types: []*types.T{types.Int, types.Bool, types.Int, types.Bool},
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{2, 3, 3},
				},
				Types: types.MakeIntCols(3),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				Aggregator: &execinfrapb.AggregatorSpec{
					Aggregations: aggregations([]aggTestSpec{
						{fname: "MAX", colIdx: col0, filterColIdx: &filterCol1},
						{fname: "COUNT", colIdx: col2, filterColIdx: &filterCol3},
						{fname: "COUNT_ROWS", filterColIdx: &filterCol3},
					}),
				},
			},
		},
	}

	ctx := context.Background()
	test := MakeProcessorTest(DefaultProcessorTestConfig())
	test.RunTestCases(ctx, t, testCases)
	test.Close(ctx)
}

func BenchmarkAggregation(b *testing.B) {
	defer log.Scope(b).Close(b)
	const numCols = 1
	const numRows = 1000

	aggFuncs := []execinfrapb.AggregatorSpec_Func{
		execinfrapb.AnyNotNull,
		execinfrapb.Avg,
		execinfrapb.Count,
		execinfrapb.Max,
		execinfrapb.Min,
		execinfrapb.Stddev,
		execinfrapb.Sum,
		execinfrapb.SumInt,
		execinfrapb.Variance,
		execinfrapb.XorAgg,
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	for _, aggFunc := range aggFuncs {
		b.Run(aggFunc.String(), func(b *testing.B) {
			spec := &execinfrapb.AggregatorSpec{
				Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
					{
						Func:   aggFunc,
						ColIdx: []uint32{0},
					},
				},
			}
			post := &execinfrapb.PostProcessSpec{}
			disposer := &rowDisposer{}
			input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newAggregator(flowCtx, 0 /* processorID */, spec, input, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
			b.StopTimer()
		})
	}
}

func BenchmarkCountRows(b *testing.B) {
	defer log.Scope(b).Close(b)
	spec := &execinfrapb.AggregatorSpec{
		Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
			{
				Func: execinfrapb.CountRows,
			},
		},
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &rowDisposer{}
	const numCols = 1
	const numRows = 100000
	input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(numRows, numCols))

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	b.SetBytes(int64(8 * numRows * numCols))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := newAggregator(flowCtx, 0 /* processorID */, spec, input, post, disposer)
		if err != nil {
			b.Fatal(err)
		}
		d.Run(context.Background())
		input.Reset()
	}
}

func BenchmarkGrouping(b *testing.B) {
	defer log.Scope(b).Close(b)
	const numCols = 1
	const numRows = 1000

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	spec := &execinfrapb.AggregatorSpec{
		GroupCols: []uint32{0},
	}
	post := &execinfrapb.PostProcessSpec{}
	disposer := &rowDisposer{}
	input := execinfra.NewRepeatableRowSource(types.OneIntCol, randgen.MakeIntRows(numRows, numCols))

	b.SetBytes(int64(8 * numRows * numCols))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := newAggregator(flowCtx, 0 /* processorID */, spec, input, post, disposer)
		if err != nil {
			b.Fatal(err)
		}
		d.Run(context.Background())
		input.Reset()
	}
	b.StopTimer()
}

func benchmarkAggregationWithGrouping(b *testing.B, numOrderedCols int) {
	defer log.Scope(b).Close(b)
	const numCols = 3
	const groupSize = 10
	var groupedCols = [2]int{0, 1}
	var allOrderedGroupCols = [2]uint32{0, 1}

	aggFuncs := []execinfrapb.AggregatorSpec_Func{
		execinfrapb.AnyNotNull,
		execinfrapb.Avg,
		execinfrapb.Count,
		execinfrapb.Max,
		execinfrapb.Min,
		execinfrapb.Stddev,
		execinfrapb.Sum,
		execinfrapb.SumInt,
		execinfrapb.Variance,
		execinfrapb.XorAgg,
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	for _, aggFunc := range aggFuncs {
		b.Run(aggFunc.String(), func(b *testing.B) {
			spec := &execinfrapb.AggregatorSpec{
				GroupCols: []uint32{0, 1},
				Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
					{
						Func:   aggFunc,
						ColIdx: []uint32{2},
					},
				},
			}
			spec.OrderedGroupCols = allOrderedGroupCols[:numOrderedCols]
			post := &execinfrapb.PostProcessSpec{}
			disposer := &rowDisposer{}
			input := execinfra.NewRepeatableRowSource(types.ThreeIntCols, makeGroupedIntRows(groupSize, numCols, groupedCols[:]))

			b.SetBytes(int64(8 * intPow(groupSize, len(groupedCols)+1) * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newAggregator(flowCtx, 0 /* processorID */, spec, input, post, disposer)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
			b.StopTimer()
		})
	}
}

func BenchmarkOrderedAggregation(b *testing.B) {
	benchmarkAggregationWithGrouping(b, 2 /* numOrderedCols */)
}

func BenchmarkPartiallyOrderedAggregation(b *testing.B) {
	benchmarkAggregationWithGrouping(b, 1 /* numOrderedCols */)
}

func BenchmarkUnorderedAggregation(b *testing.B) {
	benchmarkAggregationWithGrouping(b, 0 /* numOrderedCols */)
}

func intPow(a, b int) int {
	return int(math.Pow(float64(a), float64(b)))
}

// makeGroupedIntRows constructs a (groupSize**(len(groupedCols)+1)) x numCols
// table, where columns in groupedCols are sorted in ascending order with column
// priority defined by their position in groupedCols. If used in an aggregation
// where groupedCols are the GROUP BY columns, each group will have a size of
// groupSize. To make the input more interesting for aggregation, group columns
// are repeated.
//
// Examples:
// makeGroupedIntRows(2, 2, []int{1, 0}) ->
// [0 0]
// [0 0]
// [1 0]
// [1 0]
// [0 1]
// [0 1]
// [1 1]
// [1 1]
func makeGroupedIntRows(groupSize, numCols int, groupedCols []int) rowenc.EncDatumRows {
	numRows := intPow(groupSize, len(groupedCols)+1)
	rows := make(rowenc.EncDatumRows, numRows)

	groupColSet := util.MakeFastIntSet(groupedCols...)
	getGroupedColVal := func(rowIdx, colIdx int) int {
		rank := -1
		for i, c := range groupedCols {
			if colIdx == c {
				rank = len(groupedCols) - i
				break
			}
		}
		if rank == -1 {
			panic("provided colIdx is not a group column")
		}
		return (rowIdx % intPow(groupSize, rank+1)) / intPow(groupSize, rank)
	}

	for i := range rows {
		rows[i] = make(rowenc.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			if groupColSet.Contains(j) {
				rows[i][j] = rowenc.DatumToEncDatum(
					types.Int, tree.NewDInt(tree.DInt(getGroupedColVal(i, j))))
			} else {
				rows[i][j] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i+j)))
			}
		}
	}
	return rows
}
