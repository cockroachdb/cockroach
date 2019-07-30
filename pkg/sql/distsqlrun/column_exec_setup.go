// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/vecbuiltins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
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
	ctx context.Context,
	flowCtx *FlowCtx,
	input exec.Operator,
	inputTypes []semtypes.T,
	newToWrap func(RowSource) (RowSource, error),
) (*columnarizer, error) {
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

	return newColumnarizer(ctx, flowCtx, processorID, toWrap)
}

type newColOperatorResult struct {
	op              exec.Operator
	outputTypes     []types.T
	memUsage        int
	metadataSources []distsqlpb.MetadataSource
}

// newColOperator creates a new columnar operator according to the given spec.
func newColOperator(
	ctx context.Context, flowCtx *FlowCtx, spec *distsqlpb.ProcessorSpec, inputs []exec.Operator,
) (result newColOperatorResult, err error) {
	log.VEventf(ctx, 2, "planning col operator for spec %q", spec)

	core := &spec.Core
	post := &spec.Post

	// Planning additional operators for the PostProcessSpec (filters and render
	// expressions) requires knowing the operator's output column types. Currently
	// this must be set for any core spec which might require post-processing. In
	// the future we may want to make these column types part of the Operator
	// interface.
	var columnTypes []semtypes.T

	switch {
	case core.Noop != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		result.op = exec.NewNoop(inputs[0])
		columnTypes = spec.Input[0].ColumnTypes
	case core.TableReader != nil:
		if err := checkNumIn(inputs, 0); err != nil {
			return result, err
		}
		if core.TableReader.IsCheck {
			return result, errors.Newf("scrub table reader is unsupported in vectorized")
		}
		var scanOp *colBatchScan
		scanOp, err = newColBatchScan(flowCtx, core.TableReader, post)
		result.op = scanOp
		result.metadataSources = append(result.metadataSources, scanOp)
		// We will be wrapping colBatchScan with a cancel checker below, so we
		// need to log its creation separately.
		log.VEventf(ctx, 1, "made op %T\n", result.op)

		// We want to check for cancellation once per input batch, and wrapping
		// only colBatchScan with an exec.CancelChecker allows us to do just that.
		// It's sufficient for most of the operators since they are extremely fast.
		// However, some of the long-running operators (for example, sorter) are
		// still responsible for doing the cancellation check on their own while
		// performing long operations.
		result.op = exec.NewCancelChecker(result.op)
		returnMutations := core.TableReader.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
		columnTypes = core.TableReader.Table.ColumnTypesWithMutations(returnMutations)
	case core.Aggregator != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		aggSpec := core.Aggregator
		if len(aggSpec.Aggregations) == 0 {
			// We can get an aggregator when no aggregate functions are present if
			// HAVING clause is present, for example, with a query as follows:
			// SELECT 1 FROM t HAVING true. In this case, we plan a special operator
			// that outputs a batch of length 1 without actual columns once and then
			// zero-length batches. The actual "data" will be added by projections
			// below.
			// TODO(solon): The distsql plan for this case includes a TableReader, so
			// we end up creating an orphaned colBatchScan. We should avoid that.
			// Ideally the optimizer would not plan a scan in this unusual case.
			result.op, err = exec.NewSingleTupleNoInputOp(), nil
			// We make columnTypes non-nil so that sanity check doesn't panic.
			columnTypes = make([]semtypes.T, 0)
			break
		}
		if len(aggSpec.GroupCols) == 0 &&
			len(aggSpec.Aggregations) == 1 &&
			aggSpec.Aggregations[0].FilterColIdx == nil &&
			aggSpec.Aggregations[0].Func == distsqlpb.AggregatorSpec_COUNT_ROWS &&
			!aggSpec.Aggregations[0].Distinct {
			result.op, err = exec.NewCountOp(inputs[0]), nil
			columnTypes = []semtypes.T{*semtypes.Int}
			break
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
			return result, errors.AssertionFailedf("ordered cols must be a subset of grouping cols")
		}

		aggTyps := make([][]semtypes.T, len(aggSpec.Aggregations))
		aggCols := make([][]uint32, len(aggSpec.Aggregations))
		aggFns := make([]distsqlpb.AggregatorSpec_Func, len(aggSpec.Aggregations))
		columnTypes = make([]semtypes.T, len(aggSpec.Aggregations))
		for i, agg := range aggSpec.Aggregations {
			if agg.Distinct {
				return result, errors.Newf("distinct aggregation not supported")
			}
			if agg.FilterColIdx != nil {
				return result, errors.Newf("filtering aggregation not supported")
			}
			if len(agg.Arguments) > 0 {
				return result, errors.Newf("aggregates with arguments not supported")
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
					return result, errors.Newf("sum on int cols not supported (use sum_int)")
				}
			case distsqlpb.AggregatorSpec_SUM_INT:
				// TODO(yuzefovich): support this case through vectorize.
				if aggTyps[i][0].Width() != 64 {
					return result, errors.Newf("sum_int is only supported on Int64 through vectorized")
				}
			}
			_, retType, err := GetAggregateInfo(agg.Func, aggTyps[i]...)
			if err != nil {
				return result, err
			}
			columnTypes[i] = *retType
		}
		var typs []types.T
		typs, err = conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		if needHash {
			result.op, err = exec.NewHashAggregator(
				inputs[0], typs, aggFns, aggSpec.GroupCols, aggCols, isScalarAggregate(aggSpec),
			)
		} else {
			result.op, err = exec.NewOrderedAggregator(
				inputs[0], typs, aggFns, aggSpec.GroupCols, aggCols, isScalarAggregate(aggSpec),
			)
		}

	case core.Distinct != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}

		var distinctCols, orderedCols util.FastIntSet

		for _, col := range core.Distinct.OrderedColumns {
			orderedCols.Add(int(col))
		}
		for _, col := range core.Distinct.DistinctColumns {
			if !orderedCols.Contains(int(col)) {
				return result, errors.Newf("unsorted distinct not supported")
			}
			distinctCols.Add(int(col))
		}
		if !orderedCols.SubsetOf(distinctCols) {
			return result, errors.AssertionFailedf("ordered cols must be a subset of distinct cols")
		}

		columnTypes = spec.Input[0].ColumnTypes
		var typs []types.T
		typs, err = conv.FromColumnTypes(columnTypes)
		if err != nil {
			return result, err
		}
		result.op, err = exec.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)

	case core.Ordinality != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		columnTypes = append(spec.Input[0].ColumnTypes, *semtypes.Int)
		result.op = exec.NewOrdinalityOp(inputs[0])

	case core.HashJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return result, err
		}

		var leftTypes, rightTypes []types.T
		leftTypes, err = conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		rightTypes, err = conv.FromColumnTypes(spec.Input[1].ColumnTypes)
		if err != nil {
			return result, err
		}

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

		result.op, err = exec.NewEqHashJoinerOp(
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
		if err != nil {
			return result, err
		}

		if !core.HashJoiner.OnExpr.Empty() {
			if core.HashJoiner.Type != sqlbase.JoinType_INNER {
				return result, errors.Newf("can't plan non-inner hash join with on expressions")
			}
			columnTypes, err = result.planFilterExpr(flowCtx, core.HashJoiner.OnExpr, columnTypes)
		}

	case core.MergeJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return result, err
		}

		if core.MergeJoiner.Type.IsSetOpJoin() {
			return result, errors.AssertionFailedf("unexpectedly %s merge join was planned", core.MergeJoiner.Type.String())
		}

		var leftTypes, rightTypes []types.T
		leftTypes, err = conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		rightTypes, err = conv.FromColumnTypes(spec.Input[1].ColumnTypes)
		if err != nil {
			return result, err
		}

		nLeftCols := uint32(len(leftTypes))
		nRightCols := uint32(len(rightTypes))

		leftOutCols := make([]uint32, 0, nLeftCols)
		rightOutCols := make([]uint32, 0, nRightCols)

		if post.Projection {
			for _, col := range post.OutputColumns {
				if col < nLeftCols {
					leftOutCols = append(leftOutCols, col)
				} else if core.MergeJoiner.Type != sqlbase.JoinType_LEFT_SEMI &&
					core.MergeJoiner.Type != sqlbase.JoinType_LEFT_ANTI {
					rightOutCols = append(rightOutCols, col-nLeftCols)
				}
			}
		} else {
			for i := uint32(0); i < nLeftCols; i++ {
				leftOutCols = append(leftOutCols, i)
			}

			if core.MergeJoiner.Type != sqlbase.JoinType_LEFT_SEMI &&
				core.MergeJoiner.Type != sqlbase.JoinType_LEFT_ANTI {
				for i := uint32(0); i < nRightCols; i++ {
					rightOutCols = append(rightOutCols, i)
				}
			}
		}

		result.op, err = exec.NewMergeJoinOp(
			core.MergeJoiner.Type,
			inputs[0],
			inputs[1],
			leftOutCols,
			rightOutCols,
			leftTypes,
			rightTypes,
			core.MergeJoiner.LeftOrdering.Columns,
			core.MergeJoiner.RightOrdering.Columns,
		)
		if err != nil {
			return result, err
		}

		columnTypes = make([]semtypes.T, nLeftCols+nRightCols)
		copy(columnTypes, spec.Input[0].ColumnTypes)
		if core.MergeJoiner.Type != sqlbase.JoinType_LEFT_SEMI &&
			core.MergeJoiner.Type != sqlbase.JoinType_LEFT_ANTI {
			copy(columnTypes[nLeftCols:], spec.Input[1].ColumnTypes)
		} else {
			columnTypes = columnTypes[:nLeftCols]
		}

		if !core.MergeJoiner.OnExpr.Empty() {
			if core.MergeJoiner.Type != sqlbase.JoinType_INNER {
				return result, errors.Errorf("can't plan non-inner merge joins with on expressions")
			}
			columnTypes, err = result.planFilterExpr(flowCtx, core.MergeJoiner.OnExpr, columnTypes)
		}

	case core.JoinReader != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}

		var c *columnarizer
		c, err = wrapRowSource(ctx, flowCtx, inputs[0], spec.Input[0].ColumnTypes, func(input RowSource) (RowSource, error) {
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
		result.op = c
		result.metadataSources = append(result.metadataSources, c)

	case core.Sorter != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		input := inputs[0]
		var inputTypes []types.T
		inputTypes, err = conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		orderingCols := core.Sorter.OutputOrdering.Columns
		matchLen := core.Sorter.OrderingMatchLen
		if matchLen > 0 {
			// The input is already partially ordered. Use a chunks sorter to avoid
			// loading all the rows into memory.
			result.op, err = exec.NewSortChunks(input, inputTypes, orderingCols, int(matchLen))
		} else if post.Limit != 0 && post.Filter.Empty() && post.Limit+post.Offset < math.MaxUint16 {
			// There is a limit specified with no post-process filter, so we know
			// exactly how many rows the sorter should output. Choose a top K sorter,
			// which uses a heap to avoid storing more rows than necessary.
			k := uint16(post.Limit + post.Offset)
			result.op = exec.NewTopKSorter(input, inputTypes, orderingCols, k)
		} else {
			// No optimizations possible. Default to the standard sort operator.
			result.op, err = exec.NewSorter(input, inputTypes, orderingCols)
		}
		columnTypes = spec.Input[0].ColumnTypes

	case core.Windower != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		if len(core.Windower.WindowFns) != 1 {
			return result, errors.Newf("only a single window function is currently supported")
		}
		wf := core.Windower.WindowFns[0]
		if wf.Frame != nil &&
			(wf.Frame.Mode != distsqlpb.WindowerSpec_Frame_RANGE ||
				wf.Frame.Bounds.Start.BoundType != distsqlpb.WindowerSpec_Frame_UNBOUNDED_PRECEDING ||
				(wf.Frame.Bounds.End != nil && wf.Frame.Bounds.End.BoundType != distsqlpb.WindowerSpec_Frame_CURRENT_ROW)) {
			return result, errors.Newf("window functions with non-default window frames are not supported")
		}
		if wf.Func.AggregateFunc != nil {
			return result, errors.Newf("aggregate functions used as window functions are not supported")
		}

		input := inputs[0]
		var typs []types.T
		typs, err = conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		tempPartitionColOffset, partitionColIdx := 0, -1
		if len(core.Windower.PartitionBy) > 0 {
			// TODO(yuzefovich): add support for hashing partitioner (probably by
			// leveraging hash routers once we can distribute). The decision about
			// which kind of partitioner to use should come from the optimizer.
			input, err = exec.NewWindowSortingPartitioner(input, typs, core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx))
			tempPartitionColOffset, partitionColIdx = 1, int(wf.OutputColIdx)
		} else {
			if len(wf.Ordering.Columns) > 0 {
				input, err = exec.NewSorter(input, typs, wf.Ordering.Columns)
			}
		}
		if err != nil {
			return result, err
		}

		orderingCols := make([]uint32, len(wf.Ordering.Columns))
		for i, col := range wf.Ordering.Columns {
			orderingCols[i] = col.ColIdx
		}
		switch *wf.Func.WindowFunc {
		case distsqlpb.WindowerSpec_ROW_NUMBER:
			result.op = vecbuiltins.NewRowNumberOperator(input, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case distsqlpb.WindowerSpec_RANK:
			result.op, err = vecbuiltins.NewRankOperator(input, typs, false /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case distsqlpb.WindowerSpec_DENSE_RANK:
			result.op, err = vecbuiltins.NewRankOperator(input, typs, true /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		default:
			return result, errors.Newf("window function %s is not supported", wf.String())
		}

		if partitionColIdx != -1 {
			// Window partitioner will append a temporary column to the batch which
			// we want to project out.
			projection := make([]uint32, 0, wf.OutputColIdx+1)
			for i := uint32(0); i < wf.OutputColIdx; i++ {
				projection = append(projection, i)
			}
			projection = append(projection, wf.OutputColIdx+1)
			result.op = exec.NewSimpleProjectOp(result.op, int(wf.OutputColIdx+1), projection)
		}

		columnTypes = append(spec.Input[0].ColumnTypes, *semtypes.Int)

	default:
		return result, errors.Newf("unsupported processor core %q", core)
	}

	if err != nil {
		return result, err
	}

	// After constructing the base operator, calculate the memory usage
	// of the operator.
	if sMemOp, ok := result.op.(exec.StaticMemoryOperator); ok {
		result.memUsage += sMemOp.EstimateStaticMemoryUsage()
	}

	log.VEventf(ctx, 1, "made op %T\n", result.op)

	if columnTypes == nil {
		return result, errors.AssertionFailedf("output columnTypes unset after planning %T", result.op)
	}

	if !post.Filter.Empty() {
		if columnTypes, err = result.planFilterExpr(flowCtx, post.Filter, columnTypes); err != nil {
			return result, err
		}
	}
	if post.Projection {
		result.op = exec.NewSimpleProjectOp(result.op, len(columnTypes), post.OutputColumns)
		// Update output columnTypes.
		newTypes := make([]semtypes.T, 0, len(post.OutputColumns))
		for _, j := range post.OutputColumns {
			newTypes = append(newTypes, columnTypes[j])
		}
		columnTypes = newTypes
	} else if post.RenderExprs != nil {
		log.VEventf(ctx, 2, "planning render expressions %+v", post.RenderExprs)
		var renderedCols []uint32
		for _, expr := range post.RenderExprs {
			var (
				helper    exprHelper
				renderMem int
			)
			err := helper.init(expr, columnTypes, flowCtx.EvalCtx)
			if err != nil {
				return result, err
			}
			var outputIdx int
			result.op, outputIdx, columnTypes, renderMem, err = planProjectionOperators(
				flowCtx.NewEvalCtx(), helper.expr, columnTypes, result.op)
			if err != nil {
				return result, errors.Wrapf(err, "unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return result, errors.AssertionFailedf("missing outputIdx")
			}
			result.memUsage += renderMem
			renderedCols = append(renderedCols, uint32(outputIdx))
		}
		result.op = exec.NewSimpleProjectOp(result.op, len(columnTypes), renderedCols)
		newTypes := make([]semtypes.T, 0, len(renderedCols))
		for _, j := range renderedCols {
			newTypes = append(newTypes, columnTypes[j])
		}
		columnTypes = newTypes
	}
	if post.Offset != 0 {
		result.op = exec.NewOffsetOp(result.op, post.Offset)
	}
	if post.Limit != 0 {
		result.op = exec.NewLimitOp(result.op, post.Limit)
	}
	if err != nil {
		return result, err
	}
	result.outputTypes, err = conv.FromColumnTypes(columnTypes)
	return result, err
}

func (r *newColOperatorResult) planFilterExpr(
	flowCtx *FlowCtx, filter distsqlpb.Expression, columnTypes []semtypes.T,
) ([]semtypes.T, error) {
	var (
		helper       exprHelper
		selectionMem int
	)
	err := helper.init(filter, columnTypes, flowCtx.EvalCtx)
	if err != nil {
		return columnTypes, err
	}
	var filterColumnTypes []semtypes.T
	r.op, _, filterColumnTypes, selectionMem, err = planSelectionOperators(flowCtx.NewEvalCtx(), helper.expr, columnTypes, r.op)
	if err != nil {
		return columnTypes, errors.Wrapf(err, "unable to columnarize filter expression %q", filter.Expr)
	}
	r.memUsage += selectionMem
	if len(filterColumnTypes) > len(columnTypes) {
		// Additional columns were appended to store projections while evaluating
		// the filter. Project them away.
		var outputColumns []uint32
		for i := range columnTypes {
			outputColumns = append(outputColumns, uint32(i))
		}
		r.op = exec.NewSimpleProjectOp(r.op, len(filterColumnTypes), outputColumns)
	}
	return columnTypes, nil
}

func planSelectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []semtypes.T, input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.T, memUsed int, err error) {
	if err := assertHomogeneousTypes(expr); err != nil {
		return op, resultIdx, ct, memUsed, err
	}
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return exec.NewBoolVecToSelOp(input, t.Idx), -1, columnTypes, memUsed, nil
	case *tree.AndExpr:
		leftOp, _, ct, memUsage, err := planSelectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsage, err
		}
		return planSelectionOperators(ctx, t.TypedRight(), ct, leftOp)
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, memUsageLeft, err := planProjectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsageLeft, err
		}
		typ := &ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			if t.Operator == tree.Like || t.Operator == tree.NotLike {
				negate := t.Operator == tree.NotLike
				op, err := exec.GetLikeOperator(
					ctx, leftOp, leftIdx, string(tree.MustBeDString(constArg)), negate)
				return op, resultIdx, ct, memUsageLeft, err
			}
			if t.Operator == tree.In || t.Operator == tree.NotIn {
				negate := t.Operator == tree.NotIn
				datumTuple, ok := tree.AsDTuple(constArg)
				if !ok {
					err = errors.Errorf("IN is only supported for constant expressions")
					return nil, resultIdx, ct, memUsed, err
				}
				op, err := exec.GetInOperator(typ, leftOp, leftIdx, datumTuple, negate)
				return op, resultIdx, ct, memUsageLeft, err
			}
			op, err := exec.GetSelectionConstOperator(typ, cmpOp, leftOp, leftIdx, constArg)
			return op, resultIdx, ct, memUsageLeft, err
		}
		rightOp, rightIdx, ct, memUsageRight, err := planProjectionOperators(ctx, t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, ct, memUsageLeft + memUsageRight, err
		}
		op, err := exec.GetSelectionOperator(typ, cmpOp, rightOp, leftIdx, rightIdx)
		return op, resultIdx, ct, memUsageLeft + memUsageRight, err
	default:
		return nil, resultIdx, nil, memUsed, errors.Errorf("unhandled selection expression type: %s", reflect.TypeOf(t))
	}
}

// planProjectionOperators plans a chain of operators to execute the provided
// expression. It returns the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planProjectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []semtypes.T, input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.T, memUsed int, err error) {
	if err := assertHomogeneousTypes(expr); err != nil {
		return op, resultIdx, ct, memUsed, err
	}
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, memUsed, nil
	case *tree.ComparisonExpr:
		return planProjectionExpr(ctx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(), columnTypes, input)
	case *tree.BinaryExpr:
		return planProjectionExpr(ctx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(), columnTypes, input)
	case *tree.FuncExpr:
		var (
			inputCols     []int
			projectionMem int
		)
		ct = columnTypes
		op = input
		for _, e := range t.Exprs {
			var err error
			// TODO(rohany): This could be done better, especially in the case of
			// constant arguments, because the vectorized engine right now
			// creates a new column full of the constant value.
			op, resultIdx, ct, projectionMem, err = planProjectionOperators(ctx, e.(tree.TypedExpr), ct, op)
			if err != nil {
				return nil, resultIdx, nil, memUsed, err
			}
			inputCols = append(inputCols, resultIdx)
			memUsed += projectionMem
		}
		funcOutputType := t.ResolvedType()
		resultIdx = len(ct)
		ct = append(ct, *funcOutputType)
		op = exec.NewBuiltinFunctionOperator(ctx, t, ct, inputCols, resultIdx, op)
		return op, resultIdx, ct, memUsed, nil
	case tree.Datum:
		datumType := t.ResolvedType()
		ct := columnTypes
		resultIdx = len(ct)
		ct = append(ct, *datumType)
		if datumType.Family() == semtypes.UnknownFamily {
			return exec.NewConstNullOp(input, resultIdx), resultIdx, ct, memUsed, nil
		}
		typ := conv.FromColumnType(datumType)
		constVal, err := conv.GetDatumToPhysicalFn(datumType)(t)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		op, err := exec.NewConstOp(input, typ, constVal, resultIdx)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		return op, resultIdx, ct, memUsed, nil
	default:
		return nil, resultIdx, nil, memUsed, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func planProjectionExpr(
	ctx *tree.EvalContext,
	binOp tree.Operator,
	outputType *semtypes.T,
	left, right tree.TypedExpr,
	columnTypes []semtypes.T,
	input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.T, memUsed int, err error) {
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
		rightOp, rightIdx, ct, memUsed, err = planProjectionOperators(ctx, right, columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		resultIdx = len(ct)
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = exec.GetProjectionLConstOperator(&ct[rightIdx], binOp, rightOp, rightIdx, lConstArg, resultIdx)
		ct = append(ct, *outputType)
		if sMem, ok := op.(exec.StaticMemoryOperator); ok {
			memUsed += sMem.EstimateStaticMemoryUsage()
		}
		return op, resultIdx, ct, memUsed, err
	}
	leftOp, leftIdx, ct, leftMem, err := planProjectionOperators(ctx, left, columnTypes, input)
	if err != nil {
		return nil, resultIdx, ct, leftMem, err
	}
	if rConstArg, rConst := right.(tree.Datum); rConst {
		// Case 2: The right is constant.
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		resultIdx = len(ct)
		if binOp == tree.In || binOp == tree.NotIn {
			negate := binOp == tree.NotIn
			datumTuple, ok := tree.AsDTuple(rConstArg)
			if !ok {
				err = errors.Errorf("IN operator supported only on constant expressions")
				return nil, resultIdx, ct, leftMem, err
			}
			op, err = exec.GetInProjectionOperator(&ct[leftIdx], leftOp, leftIdx, resultIdx, datumTuple, negate)
		} else {
			op, err = exec.GetProjectionRConstOperator(&ct[leftIdx], binOp, leftOp, leftIdx, rConstArg, resultIdx)
		}
		ct = append(ct, *outputType)
		if sMem, ok := op.(exec.StaticMemoryOperator); ok {
			memUsed += sMem.EstimateStaticMemoryUsage()
		}
		return op, resultIdx, ct, leftMem + memUsed, err
	}
	// Case 3: neither are constant.
	rightOp, rightIdx, ct, rightMem, err := planProjectionOperators(ctx, right, ct, leftOp)
	if err != nil {
		return nil, resultIdx, nil, leftMem + rightMem, err
	}
	resultIdx = len(ct)
	op, err = exec.GetProjectionOperator(&ct[leftIdx], binOp, rightOp, leftIdx, rightIdx, resultIdx)
	ct = append(ct, *outputType)
	if sMem, ok := op.(exec.StaticMemoryOperator); ok {
		memUsed += sMem.EstimateStaticMemoryUsage()
	}
	return op, resultIdx, ct, leftMem + rightMem + memUsed, err
}

// assertHomogeneousTypes checks that the left and right sides of an expression
// have identical types. (Vectorized execution does not yet handle mixed types.)
// For BinaryExprs, it also checks that the result type matches, since this is
// not the case for certain operations like integer division.
func assertHomogeneousTypes(expr tree.TypedExpr) error {
	switch t := expr.(type) {
	case *tree.BinaryExpr:
		left := t.TypedLeft().ResolvedType()
		right := t.TypedRight().ResolvedType()
		result := t.ResolvedType()
		if !left.Identical(right) {
			return errors.Errorf("BinaryExpr on %s and %s is unhandled", left, right)
		}
		if !left.Identical(result) {
			return errors.Errorf("BinaryExpr on %s with %s result is unhandled", left, result)
		}
	case *tree.ComparisonExpr:
		left := t.TypedLeft().ResolvedType()
		right := t.TypedRight().ResolvedType()

		// Special rules for IN and NOT IN expressions. The type checker
		// handles invalid types for the IN and NOT IN operations at this point,
		// and we allow a comparison between t and t tuple.
		if t.Operator == tree.In || t.Operator == tree.NotIn {
			return nil
		}

		if !left.Identical(right) {
			return errors.Errorf("ComparisonExpr on %s and %s is unhandled", left, right)
		}
	}
	return nil
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

// finishVectorizedStatsCollectors finishes the given stats collectors and
// outputs their stats to the trace contained in the ctx's span.
func finishVectorizedStatsCollectors(
	ctx context.Context,
	deterministicStats bool,
	vectorizedStatsCollectors []*exec.VectorizedStatsCollector,
	procIDs []int32,
) {
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
		if deterministicStats {
			vsc.VectorizedStats.Time = 0
		}
		if vsc.ID < 0 {
			// Ignore stats collectors not associated with a processor.
			continue
		}
		tracing.SetSpanStats(spansByProcID[vsc.ID], &vsc.VectorizedStats)
	}
	for _, sp := range spansByProcID {
		sp.Finish()
	}
}

type runFn func(context.Context, context.CancelFunc)

// flowCreatorHelper contains all the logic needed to add the vectorized
// infrastructure to be run asynchronously as well as to perform some sanity
// checks.
type flowCreatorHelper interface {
	// addStreamEndpoint stores information about an inbound stream.
	addStreamEndpoint(distsqlpb.StreamID, *colrpc.Inbox, *sync.WaitGroup)
	// checkInboundStreamID checks that the provided stream ID has not been seen
	// yet.
	checkInboundStreamID(distsqlpb.StreamID) error
	// accumulateAsyncComponent stores a component (either a router or an outbox)
	// to be run asynchronously.
	accumulateAsyncComponent(runFn)
	// addMaterializer adds a materializer to the flow.
	addMaterializer(*materializer)
}

// vectorizedFlowCreator performs all the setup of vectorized flows. Depending
// on embedded flowCreatorHelper, it can either do the actual setup in order
// to run the flow or do the setup needed to check that the flow is supported
// through the vectorized engine.
type vectorizedFlowCreator struct {
	flowCreatorHelper

	streamIDToInputOp              map[distsqlpb.StreamID]exec.Operator
	recordingStats                 bool
	vectorizedStatsCollectorsQueue []*exec.VectorizedStatsCollector
	procIDs                        []int32
	waitGroup                      *sync.WaitGroup
	syncFlowConsumer               RowReceiver
	nodeDialer                     *nodedialer.Dialer
	flowID                         distsqlpb.FlowID
}

func newVectorizedFlowCreator(
	helper flowCreatorHelper,
	recordingStats bool,
	waitGroup *sync.WaitGroup,
	syncFlowConsumer RowReceiver,
	nodeDialer *nodedialer.Dialer,
	flowID distsqlpb.FlowID,
) *vectorizedFlowCreator {
	return &vectorizedFlowCreator{
		flowCreatorHelper:              helper,
		streamIDToInputOp:              make(map[distsqlpb.StreamID]exec.Operator),
		recordingStats:                 recordingStats,
		vectorizedStatsCollectorsQueue: make([]*exec.VectorizedStatsCollector, 0, 2),
		procIDs:                        make([]int32, 0, 2),
		waitGroup:                      waitGroup,
		syncFlowConsumer:               syncFlowConsumer,
		nodeDialer:                     nodeDialer,
		flowID:                         flowID,
	}
}

func (s *vectorizedFlowCreator) setupRemoteOutputStream(
	op exec.Operator,
	outputTyps []types.T,
	stream *distsqlpb.StreamEndpointSpec,
	metadataSourcesQueue []distsqlpb.MetadataSource,
) error {
	outbox, err := colrpc.NewOutbox(op, outputTyps, metadataSourcesQueue)
	if err != nil {
		return err
	}
	run := func(ctx context.Context, cancelFn context.CancelFunc) {
		outbox.Run(ctx, s.nodeDialer, stream.TargetNodeID, s.flowID, stream.StreamID, cancelFn)
	}
	s.accumulateAsyncComponent(run)
	return nil
}

// setupRouter sets up a vectorized hash router according to the output router
// spec. If the outputs are local, these are added to s.streamIDToInputOp to be
// used as inputs in further planning. metadataSourcesQueue is passed along to
// any outboxes created to be drained, an error is returned if this does not
// happen (i.e. no router outputs are remote).
func (s *vectorizedFlowCreator) setupRouter(
	input exec.Operator,
	outputTyps []types.T,
	output *distsqlpb.OutputRouterSpec,
	metadataSourcesQueue []distsqlpb.MetadataSource,
) error {
	if output.Type == distsqlpb.OutputRouterSpec_PASS_THROUGH {
		// Nothing to do.
		return nil
	}
	if output.Type != distsqlpb.OutputRouterSpec_BY_HASH {
		return errors.Errorf("vectorized output router type %s unsupported", output.Type)
	}

	// TODO(asubiotto): Change hashRouter's hashCols to be uint32s.
	hashCols := make([]int, len(output.HashColumns))
	for i := range hashCols {
		hashCols[i] = int(output.HashColumns[i])
	}
	router, outputs := exec.NewHashRouter(input, outputTyps, hashCols, len(output.Streams))
	runRouter := func(ctx context.Context, _ context.CancelFunc) {
		router.Run(ctx)
	}
	s.accumulateAsyncComponent(runRouter)

	// Append the router to the metadata sources.
	metadataSourcesQueue = append(metadataSourcesQueue, router)

	consumedMetadataSources := false
	for i, op := range outputs {
		stream := &output.Streams[i]
		switch stream.Type {
		case distsqlpb.StreamEndpointSpec_SYNC_RESPONSE:
			return errors.Errorf("unexpected sync response output when setting up router")
		case distsqlpb.StreamEndpointSpec_REMOTE:
			if err := s.setupRemoteOutputStream(op, outputTyps, stream, metadataSourcesQueue); err != nil {
				return err
			}
			// We only need one outbox to drain metadata.
			metadataSourcesQueue = nil
			consumedMetadataSources = true
		case distsqlpb.StreamEndpointSpec_LOCAL:
			if s.recordingStats {
				// Wrap local outputs with vectorized stats collectors when recording
				// stats. This is mostly for compatibility but will provide some useful
				// information (e.g. output stall time).
				var err error
				op, err = wrapWithVectorizedStatsCollector(
					op, nil /* inputs */, &distsqlpb.ProcessorSpec{ProcessorID: -1},
				)
				if err != nil {
					return err
				}
			}
			s.streamIDToInputOp[stream.StreamID] = op
		}
	}

	if !consumedMetadataSources {
		return errors.Errorf("router had no remote outputs so metadata could not be consumed")
	}
	return nil
}

// setupInput sets up one or more input operators (local or remote) and a
// synchronizer to expose these separate streams as one exec.Operator which is
// returned. If s.recordingStats is true, these inputs and synchronizer are
// wrapped in stats collectors if not done so, although these stats are not
// exposed as of yet. Inboxes that are created are also returned as
// []distqlpb.MetadataSource so that any remote metadata can be read through
// calling DrainMeta.
func (s *vectorizedFlowCreator) setupInput(
	input distsqlpb.InputSyncSpec,
) (op exec.Operator, _ []distsqlpb.MetadataSource, memUsed int, _ error) {
	inputStreamOps := make([]exec.Operator, 0, len(input.Streams))
	metaSources := make([]distsqlpb.MetadataSource, 0, len(input.Streams))
	for _, inputStream := range input.Streams {
		switch inputStream.Type {
		case distsqlpb.StreamEndpointSpec_LOCAL:
			inputStreamOps = append(inputStreamOps, s.streamIDToInputOp[inputStream.StreamID])
		case distsqlpb.StreamEndpointSpec_REMOTE:
			// If the input is remote, the input operator does not exist in
			// streamIDToInputOp. Create an inbox.
			if err := s.checkInboundStreamID(inputStream.StreamID); err != nil {
				return nil, nil, memUsed, err
			}
			typs, err := conv.FromColumnTypes(input.ColumnTypes)
			if err != nil {
				return nil, nil, memUsed, err
			}
			inbox, err := colrpc.NewInbox(typs)
			if err != nil {
				return nil, nil, memUsed, err
			}
			s.addStreamEndpoint(inputStream.StreamID, inbox, s.waitGroup)
			metaSources = append(metaSources, inbox)
			op = exec.Operator(inbox)
			memUsed += op.(exec.StaticMemoryOperator).EstimateStaticMemoryUsage()
			if s.recordingStats {
				op, err = wrapWithVectorizedStatsCollector(
					inbox,
					nil, /* inputs */
					// TODO(asubiotto): Vectorized stats collectors currently expect a
					// processor ID. These stats will not be shown until we extend stats
					// collectors to take in a stream ID.
					&distsqlpb.ProcessorSpec{
						ProcessorID: -1,
					},
				)
				if err != nil {
					return nil, nil, memUsed, err
				}
			}
			inputStreamOps = append(inputStreamOps, op)
		default:
			return nil, nil, memUsed, errors.Errorf("unsupported input stream type %s", inputStream.Type)
		}
	}
	op = inputStreamOps[0]
	if len(inputStreamOps) > 1 {
		statsInputs := inputStreamOps
		typs, err := conv.FromColumnTypes(input.ColumnTypes)
		if err != nil {
			return nil, nil, memUsed, err
		}
		if input.Type == distsqlpb.InputSyncSpec_ORDERED {
			op = exec.NewOrderedSynchronizer(
				inputStreamOps, typs, distsqlpb.ConvertToColumnOrdering(input.Ordering),
			)
			memUsed += op.(exec.StaticMemoryOperator).EstimateStaticMemoryUsage()
		} else {
			op = exec.NewUnorderedSynchronizer(inputStreamOps, typs, s.waitGroup)
			// Don't use the unordered synchronizer's inputs for stats collection
			// given that they run concurrently. The stall time will be collected
			// instead.
			statsInputs = nil
		}
		if s.recordingStats {
			// TODO(asubiotto): Once we have IDs for synchronizers, plumb them into
			// this stats collector to display stats.
			var err error
			op, err = wrapWithVectorizedStatsCollector(op, statsInputs, &distsqlpb.ProcessorSpec{ProcessorID: -1})
			if err != nil {
				return nil, nil, memUsed, err
			}
		}
	}
	return op, metaSources, memUsed, nil
}

// setupOutput sets up any necessary infrastructure according to the output
// spec of pspec. It returns an updated queue for metadata sources as well as
// any error if it occurs.
func (s *vectorizedFlowCreator) setupOutput(
	ctx context.Context,
	flowCtx *FlowCtx,
	pspec *distsqlpb.ProcessorSpec,
	op exec.Operator,
	opOutputTypes []types.T,
	metadataSourcesQueue []distsqlpb.MetadataSource,
) ([]distsqlpb.MetadataSource, error) {
	output := &pspec.Output[0]
	if output.Type != distsqlpb.OutputRouterSpec_PASS_THROUGH {
		if err := s.setupRouter(
			op,
			opOutputTypes,
			output,
			// Pass in a copy of the queue to reset metadataSourcesQueue for
			// further appends without overwriting.
			append([]distsqlpb.MetadataSource(nil), metadataSourcesQueue...),
		); err != nil {
			return nil, err
		}
		metadataSourcesQueue = metadataSourcesQueue[:0]
	} else {
		if len(output.Streams) != 1 {
			return nil, errors.Errorf("unsupported multi outputstream proc (%d streams)", len(output.Streams))
		}
		outputStream := &output.Streams[0]
		switch outputStream.Type {
		case distsqlpb.StreamEndpointSpec_LOCAL:
		case distsqlpb.StreamEndpointSpec_REMOTE:
			// Set up an Outbox. Note that we pass in a copy of metadataSourcesQueue
			// so that we can reset it below and keep on writing to it.
			if s.recordingStats {
				// If recording stats, we add a metadata source that will generate all
				// stats data as metadata for the stats collectors created so far.
				vscs := append([]*exec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
				s.vectorizedStatsCollectorsQueue = s.vectorizedStatsCollectorsQueue[:0]
				metadataSourcesQueue = append(
					metadataSourcesQueue,
					distsqlpb.CallbackMetadataSource{
						DrainMetaCb: func(ctx context.Context) []distsqlpb.ProducerMetadata {
							// TODO(asubiotto): Who is responsible for the recording of the
							// parent context?
							// Start a separate recording so that GetRecording will return
							// the recordings for only the child spans containing stats.
							ctx, span := tracing.ChildSpanSeparateRecording(ctx, "")
							finishVectorizedStatsCollectors(ctx, flowCtx.testingKnobs.DeterministicStats, vscs, s.procIDs)
							return []distsqlpb.ProducerMetadata{{TraceData: tracing.GetRecording(span)}}
						},
					},
				)
			}
			if err := s.setupRemoteOutputStream(op, opOutputTypes, outputStream, metadataSourcesQueue); err != nil {
				return nil, err
			}
			metadataSourcesQueue = metadataSourcesQueue[:0]
		case distsqlpb.StreamEndpointSpec_SYNC_RESPONSE:
			if s.syncFlowConsumer == nil {
				return nil, errors.New("syncFlowConsumer unset, unable to create materializer")
			}
			// Make the materializer, which will write to the given receiver.
			columnTypes := s.syncFlowConsumer.Types()
			outputToInputColIdx := make([]int, len(columnTypes))
			for i := range outputToInputColIdx {
				outputToInputColIdx[i] = i
			}
			var outputStatsToTrace func()
			if s.recordingStats {
				// Make a copy given that vectorizedStatsCollectorsQueue is reset and
				// appended to.
				vscq := append([]*exec.VectorizedStatsCollector(nil), s.vectorizedStatsCollectorsQueue...)
				outputStatsToTrace = func() {
					finishVectorizedStatsCollectors(
						ctx, flowCtx.testingKnobs.DeterministicStats, vscq, s.procIDs,
					)
				}
			}
			proc, err := newMaterializer(
				flowCtx,
				pspec.ProcessorID,
				op,
				columnTypes,
				outputToInputColIdx,
				&distsqlpb.PostProcessSpec{},
				s.syncFlowConsumer,
				// Pass in a copy of the queue to reset metadataSourcesQueue for
				// further appends without overwriting.
				append([]distsqlpb.MetadataSource(nil), metadataSourcesQueue...),
				outputStatsToTrace,
			)
			if err != nil {
				return nil, err
			}
			metadataSourcesQueue = metadataSourcesQueue[:0]
			s.vectorizedStatsCollectorsQueue = s.vectorizedStatsCollectorsQueue[:0]
			s.addMaterializer(proc)
		default:
			return nil, errors.Errorf("unsupported output stream type %s", outputStream.Type)
		}
		s.streamIDToInputOp[outputStream.StreamID] = op
	}
	return metadataSourcesQueue, nil
}

func (s *vectorizedFlowCreator) setupFlow(
	ctx context.Context,
	flowCtx *FlowCtx,
	processorSpecs []distsqlpb.ProcessorSpec,
	acc *mon.BoundAccount,
) error {
	streamIDToSpecIdx := make(map[distsqlpb.StreamID]int)
	metadataSourcesQueue := make([]distsqlpb.MetadataSource, 0, 1)
	// queue is a queue of indices into processorSpecs, for topologically
	// ordered processing.
	queue := make([]int, 0, len(processorSpecs))
	for i := range processorSpecs {
		hasLocalInput := false
		for j := range processorSpecs[i].Input {
			input := &processorSpecs[i].Input[j]
			for k := range input.Streams {
				stream := &input.Streams[k]
				streamIDToSpecIdx[stream.StreamID] = i
				if stream.Type != distsqlpb.StreamEndpointSpec_REMOTE {
					hasLocalInput = true
				}
			}
		}
		if hasLocalInput {
			continue
		}
		// Queue all processors with either no inputs or remote inputs.
		queue = append(queue, i)
	}

	inputs := make([]exec.Operator, 0, 2)
	for len(queue) > 0 {
		pspec := &processorSpecs[queue[0]]
		queue = queue[1:]
		if len(pspec.Output) > 1 {
			return errors.Errorf("unsupported multi-output proc (%d outputs)", len(pspec.Output))
		}
		inputs = inputs[:0]
		for i := range pspec.Input {
			input, metadataSources, memUsed, err := s.setupInput(pspec.Input[i])
			if err != nil {
				return err
			}
			if err = acc.Grow(ctx, int64(memUsed)); err != nil {
				return errors.Wrapf(err, "not enough memory to setup vectorized plan")
			}
			metadataSourcesQueue = append(metadataSourcesQueue, metadataSources...)
			inputs = append(inputs, input)
		}

		result, err := newColOperator(ctx, flowCtx, pspec, inputs)
		if err != nil {
			return errors.Wrapf(err, "unable to vectorize execution plan")
		}
		if err = acc.Grow(ctx, int64(result.memUsage)); err != nil {
			return errors.Wrapf(err, "not enough memory to setup vectorized plan")
		}
		metadataSourcesQueue = append(metadataSourcesQueue, result.metadataSources...)

		op := result.op
		if s.recordingStats {
			vsc, err := wrapWithVectorizedStatsCollector(op, inputs, pspec)
			if err != nil {
				return err
			}
			s.vectorizedStatsCollectorsQueue = append(s.vectorizedStatsCollectorsQueue, vsc)
			s.procIDs = append(s.procIDs, pspec.ProcessorID)
			op = vsc
		}

		if metadataSourcesQueue, err = s.setupOutput(
			ctx, flowCtx, pspec, op, result.outputTypes, metadataSourcesQueue,
		); err != nil {
			return err
		}

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
					return errors.Errorf("couldn't find stream %d", stream.StreamID)
				}
				outputSpec := &processorSpecs[procIdx]
				for k := range outputSpec.Input {
					for l := range outputSpec.Input[k].Streams {
						stream := outputSpec.Input[k].Streams[l]
						if stream.Type == distsqlpb.StreamEndpointSpec_REMOTE {
							// Remote streams are not present in streamIDToInputOp. The
							// Inboxes that consume these streams are created at the same time
							// as the operator that needs them, so skip the creation check for
							// this input.
							continue
						}
						if _, ok := s.streamIDToInputOp[stream.StreamID]; !ok {
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
		panic("not all metadata sources have been processed")
	}
	if len(s.vectorizedStatsCollectorsQueue) > 0 {
		panic("not all vectorized stats collectors have been processed")
	}
	return nil
}

// vectorizedFlowCreatorHelper is a flowCreatorHelper that sets up all the
// vectorized infrastructure to be actually run.
type vectorizedFlowCreatorHelper struct {
	f *Flow
}

var _ flowCreatorHelper = &vectorizedFlowCreatorHelper{}

func (r *vectorizedFlowCreatorHelper) addStreamEndpoint(
	streamID distsqlpb.StreamID, inbox *colrpc.Inbox, wg *sync.WaitGroup,
) {
	r.f.inboundStreams[streamID] = &inboundStreamInfo{
		receiver:  vectorizedInboundStreamHandler{inbox},
		waitGroup: wg,
	}
}

func (r *vectorizedFlowCreatorHelper) checkInboundStreamID(sid distsqlpb.StreamID) error {
	return r.f.checkInboundStreamID(sid)
}

func (r *vectorizedFlowCreatorHelper) accumulateAsyncComponent(run runFn) {
	r.f.startables = append(
		r.f.startables,
		startableFn(func(ctx context.Context, wg *sync.WaitGroup, cancelFn context.CancelFunc) {
			if wg != nil {
				wg.Add(1)
			}
			go func() {
				run(ctx, cancelFn)
				if wg != nil {
					wg.Done()
				}
			}()
		}),
	)
}

func (r *vectorizedFlowCreatorHelper) addMaterializer(m *materializer) {
	r.f.processors = make([]Processor, 1)
	r.f.processors[0] = m
}

func (f *Flow) setupVectorizedFlow(ctx context.Context, acc *mon.BoundAccount) error {
	recordingStats := false
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		recordingStats = true
	}
	helper := &vectorizedFlowCreatorHelper{f: f}
	creator := newVectorizedFlowCreator(
		helper, recordingStats, &f.waitGroup, f.syncFlowConsumer, f.nodeDialer, f.id,
	)
	return creator.setupFlow(ctx, &f.FlowCtx, f.spec.Processors, acc)
}

// noopFlowCreatorHelper is a flowCreatorHelper that only performs sanity
// checks.
type noopFlowCreatorHelper struct {
	inboundStreams map[distsqlpb.StreamID]struct{}
}

var _ flowCreatorHelper = &noopFlowCreatorHelper{}

func newNoopFlowCreatorHelper() *noopFlowCreatorHelper {
	return &noopFlowCreatorHelper{
		inboundStreams: make(map[distsqlpb.StreamID]struct{}),
	}
}

func (r *noopFlowCreatorHelper) addStreamEndpoint(
	streamID distsqlpb.StreamID, _ *colrpc.Inbox, _ *sync.WaitGroup,
) {
	r.inboundStreams[streamID] = struct{}{}
}

func (r *noopFlowCreatorHelper) checkInboundStreamID(sid distsqlpb.StreamID) error {
	if _, found := r.inboundStreams[sid]; found {
		return errors.Errorf("inbound stream %d already exists in map", sid)
	}
	return nil
}

func (r *noopFlowCreatorHelper) accumulateAsyncComponent(runFn) {}

func (r *noopFlowCreatorHelper) addMaterializer(m *materializer) {
}

// SupportsVectorized checks whether flow is supported by the vectorized engine
// and returns an error if it isn't. Note that it does so by setting up the
// full flow without running the components asynchronously.
func SupportsVectorized(
	ctx context.Context, flowCtx *FlowCtx, processorSpecs []distsqlpb.ProcessorSpec,
) error {
	creator := newVectorizedFlowCreator(
		newNoopFlowCreatorHelper(),
		false,        /* recordingStats */
		nil,          /* waitGroup */
		&RowBuffer{}, /* syncFlowConsumer */
		nil,          /* nodeDialer */
		distsqlpb.FlowID{},
	)
	// We create an unlimited memory account because we're interested whether the
	// flow is supported via the vectorized engine in general (without paying
	// attention to the memory since it is node-dependent in the distributed
	// case).
	memoryMonitor := mon.MakeMonitor(
		"supports-vectorized",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		flowCtx.Settings,
	)
	memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memoryMonitor.Stop(ctx)
	acc := memoryMonitor.MakeBoundAccount()
	defer acc.Close(ctx)
	return creator.setupFlow(ctx, flowCtx, processorSpecs, &acc)
}
