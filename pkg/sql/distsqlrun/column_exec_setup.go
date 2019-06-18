// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/vecbuiltins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
)

func checkNumIn(inputs []exec.Operator, numIn int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// wrapRowSource, given an input exec.Operator, integrates toWrap into a
// columnar execution flow and returns toWrap's output as an exec.Operator.
func wrapRowSource(
	flowCtx *FlowCtx,
	input exec.Operator,
	inputTypes []semtypes.T,
	newToWrap func(RowSource) (RowSource, error),
) (exec.Operator, error) {
	var (
		toWrapInput RowSource
		// TODO(asubiotto): Plumb proper processorIDs once we have stats.
		processorID int32
	)
	// Optimization: if the input is a columnarizer, its input is necessarily a
	// RowSource, so remove the unnecessary conversion.
	if c, ok := input.(*columnarizer); ok {
		// TODO(asubiotto): We might need to do some extra work to remove references
		// to this operator (e.g. streamIDToOp).
		toWrapInput = c.input
	} else {
		outputToInputColIdx := make([]int, len(inputTypes))
		for i := range outputToInputColIdx {
			outputToInputColIdx[i] = i
		}
		var err error
		toWrapInput, err = newMaterializer(
			flowCtx,
			processorID,
			input,
			inputTypes,
			outputToInputColIdx,
			&distsqlpb.PostProcessSpec{},
			nil, /* output */
			nil, /* metadataSourcesQueue */
			nil, /* outputStatsToTrace */
		)
		if err != nil {
			return nil, err
		}
	}

	toWrap, err := newToWrap(toWrapInput)
	if err != nil {
		return nil, err
	}

	return newColumnarizer(flowCtx, processorID, toWrap)
}

func newColOperator(
	ctx context.Context, flowCtx *FlowCtx, spec *distsqlpb.ProcessorSpec, inputs []exec.Operator,
) (exec.Operator, error) {
	core := &spec.Core
	post := &spec.Post
	var err error
	var op exec.Operator

	// Planning additional operators for the PostProcessSpec (filters and render
	// expressions) requires knowing the operator's output column types. Currently
	// this must be set for any core spec which might require post-processing. In
	// the future we may want to make these column types part of the Operator
	// interface.
	var columnTypes []semtypes.T

	switch {
	case core.Noop != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		op = exec.NewNoop(inputs[0])
		columnTypes = spec.Input[0].ColumnTypes
	case core.TableReader != nil:
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		op, err = newColBatchScan(flowCtx, core.TableReader, post)
		// We want to check for cancellation once per input batch, and wrapping
		// only colBatchScan with an exec.CancelChecker allows us to do just that.
		// It's sufficient for most of the operators since they are extremely fast.
		// However, some of the long-running operators (for example, sorter) are
		// still responsible for doing the cancellation check on their own while
		// performing long operations.
		op = exec.NewCancelChecker(op)
		returnMutations := core.TableReader.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
		columnTypes = core.TableReader.Table.ColumnTypesWithMutations(returnMutations)
	case core.Aggregator != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		aggSpec := core.Aggregator
		if len(aggSpec.GroupCols) == 0 &&
			len(aggSpec.Aggregations) == 1 &&
			aggSpec.Aggregations[0].FilterColIdx == nil &&
			aggSpec.Aggregations[0].Func == distsqlpb.AggregatorSpec_COUNT_ROWS &&
			!aggSpec.Aggregations[0].Distinct {
			return exec.NewCountOp(inputs[0]), nil
		}

		var groupCols, orderedCols util.FastIntSet

		for _, col := range aggSpec.OrderedGroupCols {
			orderedCols.Add(int(col))
		}

		needHash := false
		for _, col := range aggSpec.GroupCols {
			if !orderedCols.Contains(int(col)) {
				needHash = true
			}
			groupCols.Add(int(col))
		}
		if !orderedCols.SubsetOf(groupCols) {
			return nil, errors.AssertionFailedf("ordered cols must be a subset of grouping cols")
		}

		aggTyps := make([][]semtypes.T, len(aggSpec.Aggregations))
		aggCols := make([][]uint32, len(aggSpec.Aggregations))
		aggFns := make([]distsqlpb.AggregatorSpec_Func, len(aggSpec.Aggregations))
		columnTypes = make([]semtypes.T, len(aggSpec.Aggregations))
		for i, agg := range aggSpec.Aggregations {
			if agg.Distinct {
				return nil, errors.Newf("distinct aggregation not supported")
			}
			if agg.FilterColIdx != nil {
				return nil, errors.Newf("filtering aggregation not supported")
			}
			if len(agg.Arguments) > 0 {
				return nil, errors.Newf("aggregates with arguments not supported")
			}
			aggTyps[i] = make([]semtypes.T, len(agg.ColIdx))
			for j, colIdx := range agg.ColIdx {
				aggTyps[i][j] = spec.Input[0].ColumnTypes[colIdx]
			}
			aggCols[i] = agg.ColIdx
			aggFns[i] = agg.Func
			switch agg.Func {
			case distsqlpb.AggregatorSpec_SUM:
				switch aggTyps[i][0].Family() {
				case semtypes.IntFamily:
					// TODO(alfonso): plan ordinary SUM on integer types by casting to DECIMAL
					// at the end, mod issues with overflow. Perhaps to avoid the overflow
					// issues, at first, we could plan SUM for all types besides Int64.
					return nil, errors.Newf("sum on int cols not supported (use sum_int)")
				}
			}
			_, retType, err := GetAggregateInfo(agg.Func, aggTyps[i]...)
			if err != nil {
				return nil, err
			}
			columnTypes[i] = *retType
		}
		if needHash {
			op, err = exec.NewHashAggregator(
				inputs[0], conv.FromColumnTypes(spec.Input[0].ColumnTypes), aggFns, aggSpec.GroupCols, aggCols,
			)
		} else {
			op, err = exec.NewOrderedAggregator(
				inputs[0], conv.FromColumnTypes(spec.Input[0].ColumnTypes), aggFns, aggSpec.GroupCols, aggCols,
			)
		}

	case core.Distinct != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}

		var distinctCols, orderedCols util.FastIntSet

		for _, col := range core.Distinct.OrderedColumns {
			orderedCols.Add(int(col))
		}
		for _, col := range core.Distinct.DistinctColumns {
			if !orderedCols.Contains(int(col)) {
				return nil, errors.Newf("unsorted distinct not supported")
			}
			distinctCols.Add(int(col))
		}
		if !orderedCols.SubsetOf(distinctCols) {
			return nil, errors.AssertionFailedf("ordered cols must be a subset of distinct cols")
		}

		columnTypes = spec.Input[0].ColumnTypes
		typs := conv.FromColumnTypes(columnTypes)
		op, err = exec.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)

	case core.HashJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return nil, err
		}

		if !core.HashJoiner.OnExpr.Empty() {
			return nil, errors.Newf("can't plan hash join with on expressions")
		}

		leftTypes := conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		rightTypes := conv.FromColumnTypes(spec.Input[1].ColumnTypes)

		columnTypes = make([]semtypes.T, len(leftTypes)+len(rightTypes))
		copy(columnTypes, spec.Input[0].ColumnTypes)
		copy(columnTypes[len(leftTypes):], spec.Input[1].ColumnTypes)

		nLeftCols := uint32(len(leftTypes))
		nRightCols := uint32(len(rightTypes))

		leftOutCols := make([]uint32, 0)
		rightOutCols := make([]uint32, 0)

		if post.Projection {
			for _, col := range post.OutputColumns {
				if col < nLeftCols {
					leftOutCols = append(leftOutCols, col)
				} else {
					rightOutCols = append(rightOutCols, col-nLeftCols)
				}
			}
		} else {
			for i := uint32(0); i < nLeftCols; i++ {
				leftOutCols = append(leftOutCols, i)
			}

			for i := uint32(0); i < nRightCols; i++ {
				rightOutCols = append(rightOutCols, i)
			}
		}

		op, err = exec.NewEqHashJoinerOp(
			inputs[0],
			inputs[1],
			core.HashJoiner.LeftEqColumns,
			core.HashJoiner.RightEqColumns,
			leftOutCols,
			rightOutCols,
			leftTypes,
			rightTypes,
			core.HashJoiner.RightEqColumnsAreKey,
			core.HashJoiner.LeftEqColumnsAreKey || core.HashJoiner.RightEqColumnsAreKey,
			core.HashJoiner.Type,
		)

	case core.MergeJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return nil, err
		}

		if !core.MergeJoiner.OnExpr.Empty() {
			return nil, errors.Newf("can't plan merge join with on expressions")
		}
		if core.MergeJoiner.Type != sqlbase.InnerJoin {
			return nil, errors.Newf("can plan only inner merge join")
		}

		leftTypes := conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		rightTypes := conv.FromColumnTypes(spec.Input[1].ColumnTypes)

		nLeftCols := uint32(len(leftTypes))
		nRightCols := uint32(len(rightTypes))

		leftOutCols := make([]uint32, 0, nLeftCols)
		rightOutCols := make([]uint32, 0, nRightCols)

		if post.Projection {
			for _, col := range post.OutputColumns {
				if col < nLeftCols {
					leftOutCols = append(leftOutCols, col)
				} else {
					rightOutCols = append(rightOutCols, col-nLeftCols)
				}
			}
		} else {
			for i := uint32(0); i < nLeftCols; i++ {
				leftOutCols = append(leftOutCols, i)
			}

			for i := uint32(0); i < nRightCols; i++ {
				rightOutCols = append(rightOutCols, i)
			}
		}

		op, err = exec.NewMergeJoinOp(
			inputs[0],
			inputs[1],
			leftOutCols,
			rightOutCols,
			leftTypes,
			rightTypes,
			core.MergeJoiner.LeftOrdering.Columns,
			core.MergeJoiner.RightOrdering.Columns,
		)

		columnTypes = make([]semtypes.T, nLeftCols+nRightCols)
		copy(columnTypes, spec.Input[0].ColumnTypes)
		copy(columnTypes[nLeftCols:], spec.Input[1].ColumnTypes)

	case core.JoinReader != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}

		op, err = wrapRowSource(flowCtx, inputs[0], spec.Input[0].ColumnTypes, func(input RowSource) (RowSource, error) {
			var (
				jr  RowSource
				err error
			)
			// The lookup and index joiners need to be passed the post-process specs,
			// since they inspect them to figure out information about needed columns.
			// This means that we'll let those processors do any renders or filters,
			// which isn't ideal. We could improve this.
			if len(core.JoinReader.LookupColumns) == 0 {
				jr, err = newIndexJoiner(
					flowCtx, spec.ProcessorID, core.JoinReader, input, post, nil, /* output */
				)
			} else {
				jr, err = newJoinReader(
					flowCtx, spec.ProcessorID, core.JoinReader, input, post, nil, /* output */
				)
			}
			post = &distsqlpb.PostProcessSpec{}
			if err != nil {
				return nil, err
			}
			columnTypes = jr.OutputTypes()
			return jr, nil
		})

	case core.Sorter != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		input := inputs[0]
		inputTypes := conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		orderingCols := core.Sorter.OutputOrdering.Columns
		matchLen := core.Sorter.OrderingMatchLen
		if matchLen > 0 {
			// The input is already partially ordered. Use a chunks sorter to avoid
			// loading all the rows into memory.
			op, err = exec.NewSortChunks(input, inputTypes, orderingCols, int(matchLen))
		} else if post.Limit != 0 && post.Filter.Empty() && post.Limit+post.Offset < math.MaxUint16 {
			// There is a limit specified with no post-process filter, so we know
			// exactly how many rows the sorter should output. Choose a top K sorter,
			// which uses a heap to avoid storing more rows than necessary.
			k := uint16(post.Limit + post.Offset)
			op = exec.NewTopKSorter(input, inputTypes, orderingCols, k)
		} else {
			// No optimizations possible. Default to the standard sort operator.
			op, err = exec.NewSorter(input, inputTypes, orderingCols)
		}
		columnTypes = spec.Input[0].ColumnTypes

	case core.Windower != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		if len(core.Windower.WindowFns) != 1 {
			return nil, errors.Newf("only a single window function is currently supported")
		}
		wf := core.Windower.WindowFns[0]
		if wf.Frame != nil {
			return nil, errors.Newf("window functions with window frames are not supported")
		}
		if wf.Func.AggregateFunc != nil {
			return nil, errors.Newf("aggregate functions used as window functions are not supported")
		}

		input := inputs[0]
		typs := conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		var orderingCols []uint32
		tempPartitionColOffset, partitionColIdx := 0, -1
		if len(core.Windower.PartitionBy) > 0 {
			// TODO(yuzefovich): add support for hashing partitioner (probably by
			// leveraging hash routers once we can distribute). The decision about
			// which kind of partitioner to use should come from the optimizer.
			input, orderingCols, err = exec.NewWindowSortingPartitioner(input, typs, core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx))
			tempPartitionColOffset, partitionColIdx = 1, int(wf.OutputColIdx)
		} else {
			if len(wf.Ordering.Columns) > 0 {
				input, err = exec.NewSorter(input, typs, wf.Ordering.Columns)
			}
			orderingCols = make([]uint32, len(wf.Ordering.Columns))
			for i, col := range wf.Ordering.Columns {
				orderingCols[i] = col.ColIdx
			}
		}
		if err != nil {
			return nil, err
		}

		switch *wf.Func.WindowFunc {
		case distsqlpb.WindowerSpec_ROW_NUMBER:
			op = vecbuiltins.NewRowNumberOperator(input, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case distsqlpb.WindowerSpec_RANK:
			op, err = vecbuiltins.NewRankOperator(input, typs, false /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case distsqlpb.WindowerSpec_DENSE_RANK:
			op, err = vecbuiltins.NewRankOperator(input, typs, true /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		default:
			return nil, errors.Newf("window function %s is not supported", wf.String())
		}

		if partitionColIdx != -1 {
			// Window partitioner will append a temporary column to the batch which
			// we want to project out.
			projection := make([]uint32, 0, wf.OutputColIdx+1)
			for i := uint32(0); i < wf.OutputColIdx; i++ {
				projection = append(projection, i)
			}
			projection = append(projection, wf.OutputColIdx+1)
			op = exec.NewSimpleProjectOp(op, projection)
		}

		columnTypes = append(spec.Input[0].ColumnTypes, *semtypes.Int)

	default:
		return nil, errors.Newf("unsupported processor core %s", core)
	}
	log.VEventf(ctx, 1, "Made op %T\n", op)

	if err != nil {
		return nil, err
	}

	if columnTypes == nil {
		return nil, errors.AssertionFailedf("output columnTypes unset after planning %T", op)
	}

	if !post.Filter.Empty() {
		var helper exprHelper
		err := helper.init(post.Filter, columnTypes, flowCtx.EvalCtx)
		if err != nil {
			return nil, err
		}
		var filterColumnTypes []semtypes.T
		op, _, filterColumnTypes, err = planSelectionOperators(
			flowCtx.NewEvalCtx(), helper.expr, columnTypes, op)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to columnarize filter expression %q", post.Filter.Expr)
		}
		if len(filterColumnTypes) > len(columnTypes) {
			// Additional columns were appended to store projection results while
			// evaluating the filter. Project them away.
			var outputColumns []uint32
			for i := range columnTypes {
				outputColumns = append(outputColumns, uint32(i))
			}
			op = exec.NewSimpleProjectOp(op, outputColumns)
		}
	}
	if post.Projection {
		op = exec.NewSimpleProjectOp(op, post.OutputColumns)
	} else if post.RenderExprs != nil {
		var renderedCols []uint32
		for _, expr := range post.RenderExprs {
			var helper exprHelper
			err := helper.init(expr, columnTypes, flowCtx.EvalCtx)
			if err != nil {
				return nil, err
			}
			var outputIdx int
			op, outputIdx, columnTypes, err = planProjectionOperators(
				flowCtx.NewEvalCtx(), helper.expr, columnTypes, op)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return nil, errors.AssertionFailedf("missing outputIdx")
			}
			renderedCols = append(renderedCols, uint32(outputIdx))
		}
		op = exec.NewSimpleProjectOp(op, renderedCols)
	}
	if post.Offset != 0 {
		op = exec.NewOffsetOp(op, post.Offset)
	}
	if post.Limit != 0 {
		op = exec.NewLimitOp(op, post.Limit)
	}
	return op, nil
}

func planSelectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []semtypes.T, input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.T, err error) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return exec.NewBoolVecToSelOp(input, t.Idx), -1, columnTypes, nil
	case *tree.AndExpr:
		leftOp, _, ct, err := planSelectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		return planSelectionOperators(ctx, t.TypedRight(), ct, leftOp)
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, err := planProjectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		typ := &ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			if t.Operator == tree.Like || t.Operator == tree.NotLike {
				negate := t.Operator == tree.NotLike
				op, err := exec.GetLikeOperator(
					ctx, leftOp, leftIdx, string(tree.MustBeDString(constArg)), negate)
				return op, resultIdx, ct, err
			}
			op, err := exec.GetSelectionConstOperator(typ, cmpOp, leftOp, leftIdx, constArg)
			return op, resultIdx, ct, err
		}
		rightOp, rightIdx, ct, err := planProjectionOperators(ctx, t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		if !ct[leftIdx].Identical(&ct[rightIdx]) {
			err = errors.Errorf(
				"comparison between %s and %s is unhandled", ct[leftIdx].Family(),
				ct[rightIdx].Family())
			return nil, resultIdx, ct, err
		}
		op, err := exec.GetSelectionOperator(typ, cmpOp, rightOp, leftIdx, rightIdx)
		return op, resultIdx, ct, err
	default:
		return nil, resultIdx, nil, errors.Errorf("unhandled selection expression type: %s", reflect.TypeOf(t))
	}
}

// planProjectionOperators plans a chain of operators to execute the provided
// expression. It returns the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planProjectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []semtypes.T, input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.T, err error) {
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, nil
	case *tree.ComparisonExpr:
		return planProjectionExpr(ctx, t.Operator, t.TypedLeft(), t.TypedRight(), columnTypes, input)
	case *tree.BinaryExpr:
		return planProjectionExpr(ctx, t.Operator, t.TypedLeft(), t.TypedRight(), columnTypes, input)
	case tree.Datum:
		datumType := t.ResolvedType()
		ct := columnTypes
		resultIdx = len(ct)
		ct = append(ct, *datumType)
		if datumType.Family() == semtypes.UnknownFamily {
			return exec.NewConstNullOp(input, resultIdx), resultIdx, ct, nil
		}
		typ := conv.FromColumnType(datumType)
		constVal, err := conv.GetDatumToPhysicalFn(datumType)(t)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		op, err := exec.NewConstOp(input, typ, constVal, resultIdx)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		return op, resultIdx, ct, nil
	default:
		return nil, resultIdx, nil, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func planProjectionExpr(
	ctx *tree.EvalContext,
	binOp tree.Operator,
	left, right tree.TypedExpr,
	columnTypes []semtypes.T,
	input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.T, err error) {
	resultIdx = -1
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	lConstArg, lConst := left.(tree.Datum)
	if lConst {
		// Case one: The left is constant.
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for non-commutative
		// operators such as - and /, though, so we still need this case.
		var rightOp exec.Operator
		var rightIdx int
		rightOp, rightIdx, ct, err = planProjectionOperators(ctx, right, columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		resultIdx = len(ct)
		typ := &ct[rightIdx]
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = exec.GetProjectionLConstOperator(typ, binOp, rightOp, rightIdx, lConstArg, resultIdx)
		ct = append(ct, *typ)
		return op, resultIdx, ct, err
	}
	leftOp, leftIdx, ct, err := planProjectionOperators(ctx, left, columnTypes, input)
	if err != nil {
		return nil, resultIdx, ct, err
	}
	typ := &ct[leftIdx]
	if rConstArg, rConst := right.(tree.Datum); rConst {
		// Case 2: The right is constant.
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		resultIdx = len(ct)
		op, err = exec.GetProjectionRConstOperator(typ, binOp, leftOp, leftIdx, rConstArg, resultIdx)
		ct = append(ct, *typ)
		return op, resultIdx, ct, err
	}
	// Case 3: neither are constant.
	rightOp, rightIdx, ct, err := planProjectionOperators(ctx, right, ct, leftOp)
	if err != nil {
		return nil, resultIdx, nil, err
	}
	if !ct[leftIdx].Identical(&ct[rightIdx]) {
		err = errors.Errorf(
			"projection on %s and %s is unhandled", ct[leftIdx].Family(),
			ct[rightIdx].Family())
		return nil, resultIdx, ct, err
	}
	resultIdx = len(ct)
	op, err = exec.GetProjectionOperator(typ, binOp, rightOp, leftIdx, rightIdx, resultIdx)
	ct = append(ct, *typ)
	return op, resultIdx, ct, err
}

// wrapWithVectorizedStatsCollector creates a new exec.VectorizedStatsCollector
// that wraps op and connects the newly created wrapper with those
// corresponding to operators in inputs (the latter must have already been
// wrapped).
func wrapWithVectorizedStatsCollector(
	op exec.Operator, inputs []exec.Operator, pspec *distsqlpb.ProcessorSpec,
) (*exec.VectorizedStatsCollector, error) {
	inputWatch := timeutil.NewStopWatch()
	vsc := exec.NewVectorizedStatsCollector(op, pspec.ProcessorID, len(inputs) == 0, inputWatch)
	for _, input := range inputs {
		sc, ok := input.(*exec.VectorizedStatsCollector)
		if !ok {
			return nil, errors.New("unexpectedly an input is not collecting stats")
		}
		sc.SetOutputWatch(inputWatch)
	}
	return vsc, nil
}

func (f *Flow) setupVectorized(ctx context.Context) error {
	f.processors = make([]Processor, 1)

	streamIDToInputOp := make(map[distsqlpb.StreamID]exec.Operator)
	streamIDToSpecIdx := make(map[distsqlpb.StreamID]int)
	// queue is a queue of indices into f.spec.Processors, for topologically
	// ordered processing.
	queue := make([]int, 0, len(f.spec.Processors))
	for i := range f.spec.Processors {
		if len(f.spec.Processors[i].Input) == 0 {
			// Queue all procs with no inputs.
			queue = append(queue, i)
		}
		for j := range f.spec.Processors[i].Input {
			input := &f.spec.Processors[i].Input[j]
			for k := range input.Streams {
				if input.Streams[k].Type == distsqlpb.StreamEndpointSpec_LOCAL {
					id := input.Streams[k].StreamID
					streamIDToSpecIdx[id] = i
				} else {
					return errors.Errorf("unsupported input stream type %s", input.Streams[k].Type)
				}
			}
		}
	}

	inputs := make([]exec.Operator, 0, 2)
	metadataSourcesQueue := make([]distsqlpb.MetadataSource, 0, 1)

	recordingStats := false
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		recordingStats = true
	}
	vectorizedStatsCollectorsQueue := make([]*exec.VectorizedStatsCollector, 0, 2)
	procIDs := make([]int32, 0, 2)

	for len(queue) > 0 {
		pspec := &f.spec.Processors[queue[0]]
		queue = queue[1:]
		if len(pspec.Output) > 1 {
			return errors.Errorf("unsupported multi-output proc (%d outputs)", len(pspec.Output))
		}
		output := pspec.Output[0]
		if output.Type != distsqlpb.OutputRouterSpec_PASS_THROUGH {
			return errors.Errorf("unsupported routed proc %s", output.Type)
		}
		if len(output.Streams) != 1 {
			return errors.Errorf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
		}
		inputs = inputs[:0]
		for i := range pspec.Input {
			input := &pspec.Input[i]
			if len(input.Streams) > 1 {
				return errors.Errorf("unsupported multi inputstream proc (%d streams)", len(input.Streams))
			}
			inputStream := &input.Streams[0]
			if inputStream.Type != distsqlpb.StreamEndpointSpec_LOCAL {
				return errors.Errorf("unsupported input stream type %s", inputStream.Type)
			}
			inputs = append(inputs, streamIDToInputOp[inputStream.StreamID])
		}

		op, err := newColOperator(ctx, &f.FlowCtx, pspec, inputs)
		if err != nil {
			return err
		}
		if metaSource, ok := op.(distsqlpb.MetadataSource); ok {
			metadataSourcesQueue = append(metadataSourcesQueue, metaSource)
		}
		if recordingStats {
			vsc, err := wrapWithVectorizedStatsCollector(op, inputs, pspec)
			if err != nil {
				return err
			}
			vectorizedStatsCollectorsQueue = append(vectorizedStatsCollectorsQueue, vsc)
			procIDs = append(procIDs, pspec.ProcessorID)
			op = vsc
		}

		outputStream := output.Streams[0]
		switch outputStream.Type {
		case distsqlpb.StreamEndpointSpec_LOCAL:
		case distsqlpb.StreamEndpointSpec_SYNC_RESPONSE:
			// Make the materializer, which will write to the given receiver.
			columnTypes := f.syncFlowConsumer.Types()
			outputToInputColIdx := make([]int, len(columnTypes))
			for i := range outputToInputColIdx {
				outputToInputColIdx[i] = i
			}
			var outputStatsToTrace func()
			if recordingStats {
				vectorizedStatsCollectors := vectorizedStatsCollectorsQueue
				outputStatsToTrace = func() {
					spansByProcID := make(map[int32]opentracing.Span)
					for _, pid := range procIDs {
						// We're creating a new span for every processor setting the
						// appropriate tag so that it is displayed correctly on the flow
						// diagram.
						// TODO(yuzefovich): these spans are created and finished right
						// away which is not the way they are supposed to be used, so this
						// should be fixed.
						_, spansByProcID[pid] = tracing.ChildSpan(ctx, fmt.Sprintf("operator for processor %d", pid))
						spansByProcID[pid].SetTag(distsqlpb.ProcessorIDTagKey, pid)
					}
					for _, vsc := range vectorizedStatsCollectors {
						// TODO(yuzefovich): I'm not sure whether there are cases when
						// multiple operators correspond to a single processor. We might
						// need to do some aggregation here in that case.
						vsc.FinalizeStats()
						if f.FlowCtx.testingKnobs.DeterministicStats {
							vsc.VectorizedStats.Time = 0
						}
						tracing.SetSpanStats(spansByProcID[vsc.ID], &vsc.VectorizedStats)
					}
					for _, sp := range spansByProcID {
						sp.Finish()
					}
				}
			}
			proc, err := newMaterializer(
				&f.FlowCtx,
				pspec.ProcessorID,
				op,
				columnTypes,
				outputToInputColIdx,
				&distsqlpb.PostProcessSpec{},
				f.syncFlowConsumer,
				metadataSourcesQueue,
				outputStatsToTrace,
			)
			if err != nil {
				return err
			}
			metadataSourcesQueue = metadataSourcesQueue[:0]
			vectorizedStatsCollectorsQueue = vectorizedStatsCollectorsQueue[:0]
			f.processors[0] = proc
		default:
			return errors.Errorf("unsupported output stream type %s", outputStream.Type)
		}

		streamIDToInputOp[outputStream.StreamID] = op

		// Now queue all outputs from this op whose inputs are already all
		// populated.
	NEXTOUTPUT:
		for i := range pspec.Output {
			for j := range pspec.Output[i].Streams {
				stream := &pspec.Output[i].Streams[j]
				if stream.Type != distsqlpb.StreamEndpointSpec_LOCAL {
					continue
				}
				procIdx, ok := streamIDToSpecIdx[stream.StreamID]
				if !ok {
					return errors.Errorf("Couldn't find stream %d", stream.StreamID)
				}
				outputSpec := &f.spec.Processors[procIdx]
				for k := range outputSpec.Input {
					for l := range outputSpec.Input[k].Streams {
						id := outputSpec.Input[k].Streams[l].StreamID
						if _, ok := streamIDToInputOp[id]; !ok {
							continue NEXTOUTPUT
						}
					}
				}
				// We found an input op for every single stream in this output. Queue
				// it for processing.
				queue = append(queue, procIdx)
			}
		}
	}

	if len(metadataSourcesQueue) > 0 {
		panic("Not all metadata sources have been processed.")
	}
	return nil
}
