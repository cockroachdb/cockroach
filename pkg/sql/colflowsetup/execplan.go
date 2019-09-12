// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflowsetup

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/vecbuiltins"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func checkNumIn(inputs []colexec.Operator, numIn int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// wrapRowSource, given an input exec.Operator, integrates toWrap into a
// columnar execution flow and returns toWrap's output as an exec.Operator.
func wrapRowSource(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexec.Operator,
	inputTypes []types.T,
	newToWrap func(execinfra.RowSource) (execinfra.RowSource, error),
) (*Columnarizer, error) {
	var (
		toWrapInput execinfra.RowSource
		// TODO(asubiotto): Plumb proper processorIDs once we have stats.
		processorID int32
	)
	// Optimization: if the input is a Columnarizer, its input is necessarily a
	// distsql.RowSource, so remove the unnecessary conversion.
	if c, ok := input.(*Columnarizer); ok {
		// TODO(asubiotto): We might need to do some extra work to remove references
		// to this operator (e.g. streamIDToOp).
		toWrapInput = c.input
	} else {
		var err error
		toWrapInput, err = NewMaterializer(
			flowCtx,
			processorID,
			input,
			inputTypes,
			&execinfrapb.PostProcessSpec{},
			nil, /* output */
			nil, /* metadataSourcesQueue */
			nil, /* outputStatsToTrace */
			nil, /* cancelFlow */
		)
		if err != nil {
			return nil, err
		}
	}

	toWrap, err := newToWrap(toWrapInput)
	if err != nil {
		return nil, err
	}

	return NewColumnarizer(ctx, flowCtx, processorID, toWrap)
}

// NewColOperatorResult is a helper struct that encompasses all of the return
// values of NewColOperator call.
type NewColOperatorResult struct {
	Op              colexec.Operator
	ColumnTypes     []types.T
	MemUsage        int
	MetadataSources []execinfrapb.MetadataSource
	IsStreaming     bool
}

// NewColOperator creates a new columnar operator according to the given spec.
func NewColOperator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ProcessorSpec,
	inputs []colexec.Operator,
) (result NewColOperatorResult, err error) {
	log.VEventf(ctx, 2, "planning col operator for spec %q", spec)

	core := &spec.Core
	post := &spec.Post

	// By default, we safely assume that an operator is not streaming. Note that
	// projections, renders, filters, limits, offsets as well as all internal
	// operators (like stats collectors and cancel checkers) are streaming, so in
	// order to determine whether the resulting chain of operators is streaming,
	// it is sufficient to look only at the "core" operator.
	result.IsStreaming = false
	switch {
	case core.Noop != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		result.Op, result.IsStreaming = colexec.NewNoop(inputs[0]), true
		result.ColumnTypes = spec.Input[0].ColumnTypes
	case core.TableReader != nil:
		if err := checkNumIn(inputs, 0); err != nil {
			return result, err
		}
		if core.TableReader.IsCheck {
			return result, errors.Newf("scrub table reader is unsupported in vectorized")
		}
		var scanOp *colBatchScan
		scanOp, err = newColBatchScan(flowCtx, core.TableReader, post)
		if err != nil {
			return result, err
		}
		result.Op, result.IsStreaming = scanOp, true
		result.MetadataSources = append(result.MetadataSources, scanOp)
		// colBatchScan is wrapped with a cancel checker below, so we need to
		// account for its static memory usage here. We also need to log its
		// creation separately.
		result.MemUsage += scanOp.EstimateStaticMemoryUsage()
		log.VEventf(ctx, 1, "made op %T\n", result.Op)

		// We want to check for cancellation once per input batch, and wrapping
		// only colBatchScan with an exec.CancelChecker allows us to do just that.
		// It's sufficient for most of the operators since they are extremely fast.
		// However, some of the long-running operators (for example, sorter) are
		// still responsible for doing the cancellation check on their own while
		// performing long operations.
		result.Op = colexec.NewCancelChecker(result.Op)
		returnMutations := core.TableReader.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
		result.ColumnTypes = core.TableReader.Table.ColumnTypesWithMutations(returnMutations)
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
			result.Op, result.IsStreaming, err = colexec.NewSingleTupleNoInputOp(), true, nil
			// We make ColumnTypes non-nil so that sanity check doesn't panic.
			result.ColumnTypes = make([]types.T, 0)
			break
		}
		if len(aggSpec.GroupCols) == 0 &&
			len(aggSpec.Aggregations) == 1 &&
			aggSpec.Aggregations[0].FilterColIdx == nil &&
			aggSpec.Aggregations[0].Func == execinfrapb.AggregatorSpec_COUNT_ROWS &&
			!aggSpec.Aggregations[0].Distinct {
			result.Op, result.IsStreaming, err = colexec.NewCountOp(inputs[0]), true, nil
			result.ColumnTypes = []types.T{*types.Int}
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

		aggTyps := make([][]types.T, len(aggSpec.Aggregations))
		aggCols := make([][]uint32, len(aggSpec.Aggregations))
		aggFns := make([]execinfrapb.AggregatorSpec_Func, len(aggSpec.Aggregations))
		result.ColumnTypes = make([]types.T, len(aggSpec.Aggregations))
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
			aggTyps[i] = make([]types.T, len(agg.ColIdx))
			for j, colIdx := range agg.ColIdx {
				aggTyps[i][j] = spec.Input[0].ColumnTypes[colIdx]
			}
			aggCols[i] = agg.ColIdx
			aggFns[i] = agg.Func
			switch agg.Func {
			case execinfrapb.AggregatorSpec_SUM:
				switch aggTyps[i][0].Family() {
				case types.IntFamily:
					// TODO(alfonso): plan ordinary SUM on integer types by casting to DECIMAL
					// at the end, mod issues with overflow. Perhaps to avoid the overflow
					// issues, at first, we could plan SUM for all types besides Int64.
					return result, errors.Newf("sum on int cols not supported (use sum_int)")
				}
			case execinfrapb.AggregatorSpec_SUM_INT:
				// TODO(yuzefovich): support this case through vectorize.
				if aggTyps[i][0].Width() != 64 {
					return result, errors.Newf("sum_int is only supported on Int64 through vectorized")
				}
			}
			_, retType, err := execinfrapb.GetAggregateInfo(agg.Func, aggTyps[i]...)
			if err != nil {
				return result, err
			}
			result.ColumnTypes[i] = *retType
		}
		var typs []coltypes.T
		typs, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		if needHash {
			result.Op, err = colexec.NewHashAggregator(
				inputs[0], typs, aggFns, aggSpec.GroupCols, aggCols, execinfrapb.IsScalarAggregate(aggSpec),
			)
		} else {
			result.Op, err = colexec.NewOrderedAggregator(
				inputs[0], typs, aggFns, aggSpec.GroupCols, aggCols, execinfrapb.IsScalarAggregate(aggSpec),
			)
			result.IsStreaming = true
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

		result.ColumnTypes = spec.Input[0].ColumnTypes
		var typs []coltypes.T
		typs, err = typeconv.FromColumnTypes(result.ColumnTypes)
		if err != nil {
			return result, err
		}
		result.Op, err = colexec.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)
		result.IsStreaming = true

	case core.Ordinality != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		result.ColumnTypes = append(spec.Input[0].ColumnTypes, *types.Int)
		result.Op, result.IsStreaming = colexec.NewOrdinalityOp(inputs[0]), true

	case core.HashJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return result, err
		}

		var leftTypes, rightTypes []coltypes.T
		leftTypes, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		rightTypes, err = typeconv.FromColumnTypes(spec.Input[1].ColumnTypes)
		if err != nil {
			return result, err
		}

		nLeftCols := uint32(len(leftTypes))
		nRightCols := uint32(len(rightTypes))

		leftOutCols := make([]uint32, 0)
		rightOutCols := make([]uint32, 0)

		// Note that we do not need a special treatment in case of LEFT SEMI and
		// LEFT ANTI joins when setting up outCols because in such cases there will
		// be a projection with post.OutputColumns already projecting out the right
		// side.
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

		var (
			onExpr         *execinfrapb.Expression
			filterPlanning *filterPlanningState
		)
		if !core.HashJoiner.OnExpr.Empty() {
			if core.HashJoiner.Type != sqlbase.JoinType_INNER {
				return result, errors.Newf("can't plan non-inner hash join with on expressions")
			}
			onExpr = &core.HashJoiner.OnExpr
			filterPlanning = newFilterPlanningState(len(leftTypes), len(rightTypes))
			leftOutCols, rightOutCols = filterPlanning.renderAllNeededCols(
				*onExpr, leftOutCols, rightOutCols,
			)
		}

		result.Op, err = colexec.NewEqHashJoinerOp(
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

		result.ColumnTypes = make([]types.T, nLeftCols+nRightCols)
		copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
		if core.HashJoiner.Type != sqlbase.JoinType_LEFT_SEMI {
			// TODO(yuzefovich): update this conditional once LEFT ANTI is supported.
			copy(result.ColumnTypes[nLeftCols:], spec.Input[1].ColumnTypes)
		} else {
			result.ColumnTypes = result.ColumnTypes[:nLeftCols]
		}

		if onExpr != nil {
			filterPlanning.remapIVars(onExpr)
			err = result.planFilterExpr(flowCtx.NewEvalCtx(), *onExpr)
			filterPlanning.projectOutExtraCols(&result, leftOutCols, rightOutCols)
		}

	case core.MergeJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return result, err
		}

		if core.MergeJoiner.Type.IsSetOpJoin() {
			return result, errors.AssertionFailedf("unexpectedly %s merge join was planned", core.MergeJoiner.Type.String())
		}
		// Merge joiner is a streaming operator when equality columns form a key
		// for both of the inputs.
		result.IsStreaming = core.MergeJoiner.LeftEqColumnsAreKey && core.MergeJoiner.RightEqColumnsAreKey

		var leftTypes, rightTypes []coltypes.T
		leftTypes, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		rightTypes, err = typeconv.FromColumnTypes(spec.Input[1].ColumnTypes)
		if err != nil {
			return result, err
		}

		nLeftCols := uint32(len(leftTypes))
		nRightCols := uint32(len(rightTypes))

		leftOutCols := make([]uint32, 0, nLeftCols)
		rightOutCols := make([]uint32, 0, nRightCols)

		// Note that we do not need a special treatment in case of LEFT SEMI and
		// LEFT ANTI joins when setting up outCols because in such cases there will
		// be a projection with post.OutputColumns already projecting out the right
		// side.
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

		var (
			onExpr            *execinfrapb.Expression
			filterPlanning    *filterPlanningState
			filterOnlyOnLeft  bool
			filterConstructor func(colexec.Operator) (colexec.Operator, error)
		)
		if !core.MergeJoiner.OnExpr.Empty() {
			// At the moment, we want to be on the conservative side and not run
			// queries with ON expressions when vectorize=auto, so we say that the
			// merge join is not streaming which will reject running such a query
			// through vectorized engine with 'auto' setting.
			// TODO(yuzefovich): remove this when we're confident in ON expression
			// support.
			result.IsStreaming = false

			onExpr = &core.MergeJoiner.OnExpr
			filterPlanning = newFilterPlanningState(len(leftTypes), len(rightTypes))
			switch core.MergeJoiner.Type {
			case sqlbase.JoinType_INNER:
				leftOutCols, rightOutCols = filterPlanning.renderAllNeededCols(
					*onExpr, leftOutCols, rightOutCols,
				)
			case sqlbase.JoinType_LEFT_SEMI, sqlbase.JoinType_LEFT_ANTI:
				filterOnlyOnLeft = filterPlanning.isFilterOnlyOnLeft(*onExpr)
				filterConstructor = func(op colexec.Operator) (colexec.Operator, error) {
					r := NewColOperatorResult{
						Op:          op,
						ColumnTypes: append(spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes...),
					}
					// We don't need to remap the indexed vars in onExpr because the
					// filter will be run alongside the merge joiner, and it will have
					// access to all of the columns from both sides.
					err := r.planFilterExpr(flowCtx.NewEvalCtx(), *onExpr)
					return r.Op, err
				}
			default:
				return result, errors.Errorf("can only plan INNER, LEFT SEMI, and LEFT ANTI merge joins with ON expressions")
			}
		}

		result.Op, err = colexec.NewMergeJoinOp(
			core.MergeJoiner.Type,
			inputs[0],
			inputs[1],
			leftOutCols,
			rightOutCols,
			leftTypes,
			rightTypes,
			core.MergeJoiner.LeftOrdering.Columns,
			core.MergeJoiner.RightOrdering.Columns,
			filterConstructor,
			filterOnlyOnLeft,
		)
		if err != nil {
			return result, err
		}

		result.ColumnTypes = make([]types.T, nLeftCols+nRightCols)
		copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
		if core.MergeJoiner.Type != sqlbase.JoinType_LEFT_SEMI &&
			core.MergeJoiner.Type != sqlbase.JoinType_LEFT_ANTI {
			copy(result.ColumnTypes[nLeftCols:], spec.Input[1].ColumnTypes)
		} else {
			result.ColumnTypes = result.ColumnTypes[:nLeftCols]
		}

		if onExpr != nil && core.MergeJoiner.Type == sqlbase.JoinType_INNER {
			filterPlanning.remapIVars(onExpr)
			err = result.planFilterExpr(flowCtx.NewEvalCtx(), *onExpr)
			filterPlanning.projectOutExtraCols(&result, leftOutCols, rightOutCols)
		}

	case core.JoinReader != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}

		var c *Columnarizer
		c, err = wrapRowSource(
			ctx,
			flowCtx,
			inputs[0],
			spec.Input[0].ColumnTypes,
			func(input execinfra.RowSource) (execinfra.RowSource, error) {
				var (
					jr  execinfra.RowSource
					err error
				)
				// The lookup and index joiners need to be passed the post-process specs,
				// since they inspect them to figure out information about needed columns.
				// This means that we'll let those processors do any renders or filters,
				// which isn't ideal. We could improve this.
				if len(core.JoinReader.LookupColumns) == 0 {
					jr, err = execinfra.NewIndexJoiner(
						flowCtx, spec.ProcessorID, core.JoinReader, input, post, nil, /* output */
					)
				} else {
					jr, err = execinfra.NewJoinReader(
						flowCtx, spec.ProcessorID, core.JoinReader, input, post, nil, /* output */
					)
				}
				post = &execinfrapb.PostProcessSpec{}
				if err != nil {
					return nil, err
				}
				result.ColumnTypes = jr.OutputTypes()
				return jr, nil
			},
		)
		result.Op, result.IsStreaming = c, true
		result.MetadataSources = append(result.MetadataSources, c)

	case core.Sorter != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		input := inputs[0]
		var inputTypes []coltypes.T
		inputTypes, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		orderingCols := core.Sorter.OutputOrdering.Columns
		matchLen := core.Sorter.OrderingMatchLen
		if matchLen > 0 {
			// The input is already partially ordered. Use a chunks sorter to avoid
			// loading all the rows into memory.
			result.Op, err = colexec.NewSortChunks(input, inputTypes, orderingCols, int(matchLen))
		} else if post.Limit != 0 && post.Filter.Empty() && post.Limit+post.Offset < math.MaxUint16 {
			// There is a limit specified with no post-process filter, so we know
			// exactly how many rows the sorter should output. Choose a top K sorter,
			// which uses a heap to avoid storing more rows than necessary.
			k := uint16(post.Limit + post.Offset)
			result.Op, result.IsStreaming = colexec.NewTopKSorter(input, inputTypes, orderingCols, k), true
		} else {
			// No optimizations possible. Default to the standard sort operator.
			result.Op, err = colexec.NewSorter(input, inputTypes, orderingCols)
		}
		result.ColumnTypes = spec.Input[0].ColumnTypes

	case core.Windower != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		if len(core.Windower.WindowFns) != 1 {
			return result, errors.Newf("only a single window function is currently supported")
		}
		wf := core.Windower.WindowFns[0]
		if wf.Frame != nil &&
			(wf.Frame.Mode != execinfrapb.WindowerSpec_Frame_RANGE ||
				wf.Frame.Bounds.Start.BoundType != execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING ||
				(wf.Frame.Bounds.End != nil && wf.Frame.Bounds.End.BoundType != execinfrapb.WindowerSpec_Frame_CURRENT_ROW)) {
			return result, errors.Newf("window functions with non-default window frames are not supported")
		}
		if wf.Func.AggregateFunc != nil {
			return result, errors.Newf("aggregate functions used as window functions are not supported")
		}

		input := inputs[0]
		var typs []coltypes.T
		typs, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
		if err != nil {
			return result, err
		}
		tempPartitionColOffset, partitionColIdx := 0, -1
		if len(core.Windower.PartitionBy) > 0 {
			// TODO(yuzefovich): add support for hashing partitioner (probably by
			// leveraging hash routers once we can distribute). The decision about
			// which kind of partitioner to use should come from the optimizer.
			input, err = colexec.NewWindowSortingPartitioner(input, typs, core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx))
			tempPartitionColOffset, partitionColIdx = 1, int(wf.OutputColIdx)
		} else {
			if len(wf.Ordering.Columns) > 0 {
				input, err = colexec.NewSorter(input, typs, wf.Ordering.Columns)
			}
			// TODO(yuzefovich): when both PARTITION BY and ORDER BY clauses are
			// omitted, the window function operator is actually streaming.
		}
		if err != nil {
			return result, err
		}

		orderingCols := make([]uint32, len(wf.Ordering.Columns))
		for i, col := range wf.Ordering.Columns {
			orderingCols[i] = col.ColIdx
		}
		switch *wf.Func.WindowFunc {
		case execinfrapb.WindowerSpec_ROW_NUMBER:
			result.Op = vecbuiltins.NewRowNumberOperator(input, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case execinfrapb.WindowerSpec_RANK:
			result.Op, err = vecbuiltins.NewRankOperator(input, typs, false /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case execinfrapb.WindowerSpec_DENSE_RANK:
			result.Op, err = vecbuiltins.NewRankOperator(input, typs, true /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
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
			result.Op = colexec.NewSimpleProjectOp(result.Op, int(wf.OutputColIdx+1), projection)
		}

		result.ColumnTypes = append(spec.Input[0].ColumnTypes, *types.Int)

	default:
		return result, errors.Newf("unsupported processor core %q", core)
	}

	if err != nil {
		return result, err
	}

	// After constructing the base operator, calculate the memory usage
	// of the operator.
	if sMemOp, ok := result.Op.(colexec.StaticMemoryOperator); ok {
		result.MemUsage += sMemOp.EstimateStaticMemoryUsage()
	}

	log.VEventf(ctx, 1, "made op %T\n", result.Op)

	if result.ColumnTypes == nil {
		return result, errors.AssertionFailedf("output columnTypes unset after planning %T", result.Op)
	}

	if !post.Filter.Empty() {
		if err = result.planFilterExpr(flowCtx.NewEvalCtx(), post.Filter); err != nil {
			return result, err
		}
	}
	if post.Projection {
		result.Op = colexec.NewSimpleProjectOp(result.Op, len(result.ColumnTypes), post.OutputColumns)
		// Update output ColumnTypes.
		newTypes := make([]types.T, 0, len(post.OutputColumns))
		for _, j := range post.OutputColumns {
			newTypes = append(newTypes, result.ColumnTypes[j])
		}
		result.ColumnTypes = newTypes
	} else if post.RenderExprs != nil {
		log.VEventf(ctx, 2, "planning render expressions %+v", post.RenderExprs)
		var renderedCols []uint32
		for _, expr := range post.RenderExprs {
			var (
				helper    execinfra.ExprHelper
				renderMem int
			)
			err := helper.Init(expr, result.ColumnTypes, flowCtx.EvalCtx)
			if err != nil {
				return result, err
			}
			var outputIdx int
			result.Op, outputIdx, result.ColumnTypes, renderMem, err = planProjectionOperators(
				flowCtx.NewEvalCtx(), helper.Expr, result.ColumnTypes, result.Op)
			if err != nil {
				return result, errors.Wrapf(err, "unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return result, errors.AssertionFailedf("missing outputIdx")
			}
			result.MemUsage += renderMem
			renderedCols = append(renderedCols, uint32(outputIdx))
		}
		result.Op = colexec.NewSimpleProjectOp(result.Op, len(result.ColumnTypes), renderedCols)
		newTypes := make([]types.T, 0, len(renderedCols))
		for _, j := range renderedCols {
			newTypes = append(newTypes, result.ColumnTypes[j])
		}
		result.ColumnTypes = newTypes
	}
	if post.Offset != 0 {
		result.Op = colexec.NewOffsetOp(result.Op, post.Offset)
	}
	if post.Limit != 0 {
		result.Op = colexec.NewLimitOp(result.Op, post.Limit)
	}
	return result, err
}

type filterPlanningState struct {
	numLeftInputCols  int
	numRightInputCols int
	// indexVarMap will be populated when rendering all needed columns in case
	// when at least one column from either side is used by the filter.
	indexVarMap       []int
	extraLeftOutCols  int
	extraRightOutCols int
}

func newFilterPlanningState(numLeftInputCols, numRightInputCols int) *filterPlanningState {
	return &filterPlanningState{
		numLeftInputCols:  numLeftInputCols,
		numRightInputCols: numRightInputCols,
	}
}

// renderAllNeededCols makes sure that all columns used by filter expression
// will be output. It does so by extracting the indices of all indexed vars
// used in the expression and appending those that are missing from *OutCols
// slices to the slices. Additionally, it populates p.indexVarMap to be used
// later to correctly remap the indexed vars and stores information about how
// many extra columns are added so that those extra columns could be projected
// out after the filter has been run.
// It returns updated leftOutCols and rightOutCols.
func (p *filterPlanningState) renderAllNeededCols(
	filter execinfrapb.Expression, leftOutCols []uint32, rightOutCols []uint32,
) ([]uint32, []uint32) {
	neededColumnsForFilter := findIVarsInRange(
		filter,
		0, /* start */
		p.numLeftInputCols+p.numRightInputCols,
	)
	if len(neededColumnsForFilter) > 0 {
		// At least one column is referenced by the filter expression.
		p.indexVarMap = make([]int, p.numLeftInputCols+p.numRightInputCols)
		for i := range p.indexVarMap {
			p.indexVarMap[i] = -1
		}
		// First, we process only the left side.
		for i, lCol := range leftOutCols {
			p.indexVarMap[lCol] = i
		}
		for _, neededCol := range neededColumnsForFilter {
			if int(neededCol) < p.numLeftInputCols {
				if p.indexVarMap[neededCol] == -1 {
					p.indexVarMap[neededCol] = len(leftOutCols)
					leftOutCols = append(leftOutCols, neededCol)
					p.extraLeftOutCols++
				}
			}
		}
		// Now that we know how many columns from the left will be output, we can
		// process the right side.
		//
		// Here is the explanation of all the indices' dance below:
		//   suppose we have two inputs with three columns in each, the filter
		//   expression as @1 = @4 AND @3 = @5, and leftOutCols = {0} and
		//   rightOutCols = {0} when this method was called. Note that only
		//   ordinals in the expression are counting from 1, everything else is
		//   zero-based.
		// - After we processed the left side above, we have the following state:
		//   neededColumnsForFilter = {0, 2, 3, 4}
		//   leftOutCols = {0, 2}
		//   p.indexVarMap = {0, -1, 1, -1, -1, -1}
		// - We calculate rColOffset = 3 to know which columns for filter are from
		//   the right side as well as to remap those for rightOutCols (the
		//   remapping step is needed because rightOutCols "thinks" only in the
		//   context of the right side).
		// - Next, we add already present rightOutCols to the indexed var map:
		//   rightOutCols = {0}
		//   p.indexVarMap = {0, -1, 1, 2, -1, -1}
		//   Note that we needed to remap the column index, and we could do so only
		//   after the left side has been processed because we need to know how
		//   many columns will be output from the left.
		// - Then, we go through the needed columns for filter slice again, and add
		//   any that are still missing to rightOutCols:
		//   rightOutCols = {0, 1}
		//   p.indexVarMap = {0, -1, 1, 2, 3, -1}
		// - We also stored the fact that we appended 1 extra column for both
		//   inputs, and we will project those out.
		rColOffset := uint32(p.numLeftInputCols)
		for i, rCol := range rightOutCols {
			p.indexVarMap[rCol+rColOffset] = len(leftOutCols) + i
		}
		for _, neededCol := range neededColumnsForFilter {
			if neededCol >= rColOffset {
				if p.indexVarMap[neededCol] == -1 {
					p.indexVarMap[neededCol] = len(rightOutCols) + len(leftOutCols)
					rightOutCols = append(rightOutCols, neededCol-rColOffset)
					p.extraRightOutCols++
				}
			}
		}
	}
	return leftOutCols, rightOutCols
}

// isFilterOnlyOnLeft returns whether the filter expression doesn't use columns
// from the right side.
func (p *filterPlanningState) isFilterOnlyOnLeft(filter execinfrapb.Expression) bool {
	// Find all needed columns for filter only from the right side.
	neededColumnsForFilter := findIVarsInRange(
		filter, p.numLeftInputCols, p.numLeftInputCols+p.numRightInputCols,
	)
	return len(neededColumnsForFilter) == 0
}

// remapIVars remaps tree.IndexedVars in expr using p.indexVarMap. Note that
// expr is modified in-place.
func (p *filterPlanningState) remapIVars(expr *execinfrapb.Expression) {
	if p.indexVarMap == nil {
		// If p.indexVarMap is nil, then there is no remapping to do.
		return
	}
	if expr.LocalExpr != nil {
		expr.LocalExpr = sqlbase.RemapIVarsInTypedExpr(expr.LocalExpr, p.indexVarMap)
	} else {
		// We iterate in the reverse order so that the multiple digit numbers are
		// handled correctly (consider an expression like @1 AND @11).
		for idx := len(p.indexVarMap) - 1; idx >= 0; idx-- {
			if p.indexVarMap[idx] != -1 {
				// We need +1 below because the ordinals are counting from 1.
				expr.Expr = strings.ReplaceAll(
					expr.Expr,
					fmt.Sprintf("@%d", idx+1),
					fmt.Sprintf("@%d", p.indexVarMap[idx]+1),
				)
			}
		}
	}
}

// projectOutExtraCols, possibly, adds a projection to remove all the extra
// columns that were needed by the filter expression.
func (p *filterPlanningState) projectOutExtraCols(
	result *NewColOperatorResult, leftOutCols, rightOutCols []uint32,
) {
	if p.extraLeftOutCols+p.extraRightOutCols > 0 {
		projection := make([]uint32, 0, len(leftOutCols)+len(rightOutCols)-p.extraLeftOutCols-p.extraRightOutCols)
		for i := 0; i < len(leftOutCols)-p.extraLeftOutCols; i++ {
			projection = append(projection, uint32(i))
		}
		for i := 0; i < len(rightOutCols)-p.extraRightOutCols; i++ {
			projection = append(projection, uint32(i+len(leftOutCols)))
		}
		result.Op = colexec.NewSimpleProjectOp(result.Op, len(leftOutCols)+len(rightOutCols), projection)
	}
}

func (r *NewColOperatorResult) planFilterExpr(
	evalCtx *tree.EvalContext, filter execinfrapb.Expression,
) error {
	var (
		helper       execinfra.ExprHelper
		selectionMem int
	)
	err := helper.Init(filter, r.ColumnTypes, evalCtx)
	if err != nil {
		return err
	}
	if helper.Expr == tree.DNull {
		// The filter expression is tree.DNull meaning that it is always false, so
		// we put a zero operator.
		r.Op = colexec.NewZeroOp(r.Op)
		return nil
	}
	var filterColumnTypes []types.T
	r.Op, _, filterColumnTypes, selectionMem, err = planSelectionOperators(evalCtx, helper.Expr, r.ColumnTypes, r.Op)
	if err != nil {
		return errors.Wrapf(err, "unable to columnarize filter expression %q", filter.Expr)
	}
	r.MemUsage += selectionMem
	if len(filterColumnTypes) > len(r.ColumnTypes) {
		// Additional columns were appended to store projections while evaluating
		// the filter. Project them away.
		var outputColumns []uint32
		for i := range r.ColumnTypes {
			outputColumns = append(outputColumns, uint32(i))
		}
		r.Op = colexec.NewSimpleProjectOp(r.Op, len(filterColumnTypes), outputColumns)
	}
	return nil
}

func planSelectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []types.T, input colexec.Operator,
) (op colexec.Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return colexec.NewBoolVecToSelOp(input, t.Idx), -1, columnTypes, memUsed, nil
	case *tree.AndExpr:
		var leftOp, rightOp colexec.Operator
		var memUsedLeft, memUsedRight int
		leftOp, _, ct, memUsedLeft, err = planSelectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		rightOp, resultIdx, ct, memUsedRight, err = planSelectionOperators(
			ctx, t.TypedRight(), ct, leftOp)
		return rightOp, resultIdx, ct, memUsedLeft + memUsedRight, err
	case *tree.OrExpr:
		// OR expressions are handled by converting them to an equivalent CASE
		// statement. Since CASE statements don't have a selection form, plan a
		// projection and then convert the resulting boolean to a selection vector.
		op, resultIdx, ct, memUsed, err = planProjectionOperators(ctx, expr, columnTypes, input)
		op = colexec.NewBoolVecToSelOp(op, resultIdx)
		return op, resultIdx, ct, memUsed, err
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, memUsageLeft, err := planProjectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsageLeft, err
		}
		lTyp := &ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			if t.Operator == tree.Like || t.Operator == tree.NotLike {
				negate := t.Operator == tree.NotLike
				op, err := colexec.GetLikeOperator(
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
				op, err := colexec.GetInOperator(lTyp, leftOp, leftIdx, datumTuple, negate)
				return op, resultIdx, ct, memUsageLeft, err
			}
			op, err := colexec.GetSelectionConstOperator(lTyp, t.TypedRight().ResolvedType(), cmpOp, leftOp, leftIdx, constArg)
			return op, resultIdx, ct, memUsageLeft, err
		}
		rightOp, rightIdx, ct, memUsageRight, err := planProjectionOperators(ctx, t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, ct, memUsageLeft + memUsageRight, err
		}
		op, err := colexec.GetSelectionOperator(lTyp, &ct[rightIdx], cmpOp, rightOp, leftIdx, rightIdx)
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
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []types.T, input colexec.Operator,
) (op colexec.Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, memUsed, nil
	case *tree.ComparisonExpr:
		return planProjectionExpr(ctx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(), columnTypes, input)
	case *tree.BinaryExpr:
		return planProjectionExpr(ctx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(), columnTypes, input)
	case *tree.CastExpr:
		expr := t.Expr.(tree.TypedExpr)
		op, resultIdx, ct, memUsed, err = planProjectionOperators(ctx, expr, columnTypes, input)
		if err != nil {
			return nil, 0, nil, 0, err
		}
		outputIdx := len(ct)
		op, err = colexec.GetCastOperator(op, resultIdx, outputIdx, expr.ResolvedType(), t.Type)
		ct = append(ct, *t.Type)
		if sMem, ok := op.(colexec.StaticMemoryOperator); ok {
			memUsed += sMem.EstimateStaticMemoryUsage()
		}
		return op, outputIdx, ct, memUsed, err
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
		op = colexec.NewBuiltinFunctionOperator(ctx, t, ct, inputCols, resultIdx, op)
		return op, resultIdx, ct, memUsed, nil
	case tree.Datum:
		datumType := t.ResolvedType()
		ct = columnTypes
		resultIdx = len(ct)
		ct = append(ct, *datumType)
		if datumType.Family() == types.UnknownFamily {
			return colexec.NewConstNullOp(input, resultIdx), resultIdx, ct, memUsed, nil
		}
		typ := typeconv.FromColumnType(datumType)
		constVal, err := typeconv.GetDatumToPhysicalFn(datumType)(t)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		op, err := colexec.NewConstOp(input, typ, constVal, resultIdx)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		return op, resultIdx, ct, memUsed, nil
	case *tree.CaseExpr:
		if t.Expr != nil {
			return nil, resultIdx, ct, 0, errors.New("CASE <expr> WHEN expressions unsupported")
		}

		buffer := colexec.NewBufferOp(input)
		caseOps := make([]colexec.Operator, len(t.Whens))
		caseOutputType := typeconv.FromColumnType(t.ResolvedType())
		caseOutputIdx := len(columnTypes)
		ct = append(columnTypes, *t.ResolvedType())
		thenIdxs := make([]int, len(t.Whens)+1)
		for i, when := range t.Whens {
			// The case operator is assembled from n WHEN arms, n THEN arms, and an
			// ELSE arm. Each WHEN arm is a boolean projection. Each THEN arm (and the
			// ELSE arm) is a projection of the type of the CASE expression. We set up
			// each WHEN arm to write its output to a fresh column, and likewise for
			// the THEN arms and the ELSE arm. Each WHEN arm individually acts on the
			// single input batch from the CaseExpr's input and is then transformed
			// into a selection vector, after which the THEN arm runs to create the
			// output just for the tuples that matched the WHEN arm. Each subsequent
			// WHEN arm will use the inverse of the selection vector to avoid running
			// the WHEN projection on tuples that have already been matched by a
			// previous WHEN arm. Finally, after each WHEN arm runs, we copy the
			// results of the WHEN into a single output vector, assembling the final
			// result of the case projection.
			var whenMemUsed, thenMemUsed int
			caseOps[i], resultIdx, ct, whenMemUsed, err = planProjectionOperators(
				ctx, when.Cond.(tree.TypedExpr), ct, buffer)
			if err != nil {
				return nil, resultIdx, ct, 0, err
			}
			// Transform the booleans to a selection vector.
			caseOps[i] = colexec.NewBoolVecToSelOp(caseOps[i], resultIdx)

			// Run the "then" clause on those tuples that were selected.
			caseOps[i], thenIdxs[i], ct, thenMemUsed, err = planProjectionOperators(ctx, when.Val.(tree.TypedExpr), ct,
				caseOps[i])
			if err != nil {
				return nil, resultIdx, ct, 0, err
			}

			memUsed += whenMemUsed + thenMemUsed
		}
		var elseMem int
		var elseOp colexec.Operator
		elseExpr := t.Else
		if elseExpr == nil {
			// If there's no ELSE arm, we write NULLs.
			elseExpr = tree.DNull
		}
		elseOp, thenIdxs[len(t.Whens)], ct, elseMem, err = planProjectionOperators(
			ctx, elseExpr.(tree.TypedExpr), ct, buffer)
		if err != nil {
			return nil, resultIdx, ct, 0, err
		}
		memUsed += elseMem

		op := colexec.NewCaseOp(buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)

		return op, caseOutputIdx, ct, memUsed, nil
	case *tree.AndExpr:
		var leftOp, rightOp colexec.Operator
		var leftIdx, rightIdx, lMemUsed, rMemUsed int
		leftOp, leftIdx, ct, lMemUsed, err = planProjectionOperators(ctx, t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, 0, err
		}
		rightOp, rightIdx, ct, rMemUsed, err = planProjectionOperators(ctx, t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, ct, 0, err
		}
		// Add a new boolean column that ands the two output columns.
		resultIdx = len(ct)
		ct = append(ct, *t.ResolvedType())
		andOp := colexec.NewAndOp(rightOp, leftIdx, rightIdx, resultIdx)
		return andOp, resultIdx, ct, lMemUsed + rMemUsed, nil
	case *tree.OrExpr:
		// Rewrite the OR expression as an equivalent CASE expression.
		// "a OR b" becomes "CASE WHEN a THEN true WHEN b THEN true ELSE false END".
		// This way we can take advantage of the short-circuiting logic built into
		// the CASE operator. (b should not be evaluated if a is true.)
		caseExpr, err := tree.NewTypedCaseExpr(
			nil,
			[]*tree.When{
				{Cond: t.Left, Val: tree.DBoolTrue},
				{Cond: t.Right, Val: tree.DBoolTrue},
			},
			tree.DBoolFalse,
			types.Bool)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		return planProjectionOperators(ctx, caseExpr, columnTypes, input)
	default:
		return nil, resultIdx, nil, memUsed, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func planProjectionExpr(
	ctx *tree.EvalContext,
	binOp tree.Operator,
	outputType *types.T,
	left, right tree.TypedExpr,
	columnTypes []types.T,
	input colexec.Operator,
) (op colexec.Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	resultIdx = -1
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	lConstArg, lConst := left.(tree.Datum)
	if lConst {
		// Case one: The left is constant.
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for non-commutative
		// operators such as - and /, though, so we still need this case.
		var rightOp colexec.Operator
		var rightIdx int
		rightOp, rightIdx, ct, memUsed, err = planProjectionOperators(ctx, right, columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		resultIdx = len(ct)
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = colexec.GetProjectionLConstOperator(left.ResolvedType(), &ct[rightIdx], binOp, rightOp, rightIdx, lConstArg, resultIdx)
		ct = append(ct, *outputType)
		if sMem, ok := op.(colexec.StaticMemoryOperator); ok {
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
		if binOp == tree.Like || binOp == tree.NotLike {
			negate := binOp == tree.NotLike
			op, err = colexec.GetLikeProjectionOperator(
				ctx, leftOp, leftIdx, resultIdx, string(tree.MustBeDString(rConstArg)), negate)
		} else if binOp == tree.In || binOp == tree.NotIn {
			negate := binOp == tree.NotIn
			datumTuple, ok := tree.AsDTuple(rConstArg)
			if !ok {
				err = errors.Errorf("IN operator supported only on constant expressions")
				return nil, resultIdx, ct, leftMem, err
			}
			op, err = colexec.GetInProjectionOperator(&ct[leftIdx], leftOp, leftIdx, resultIdx, datumTuple, negate)
		} else {
			op, err = colexec.GetProjectionRConstOperator(&ct[leftIdx], right.ResolvedType(), binOp, leftOp, leftIdx, rConstArg, resultIdx)
		}
		ct = append(ct, *outputType)
		if sMem, ok := op.(colexec.StaticMemoryOperator); ok {
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
	op, err = colexec.GetProjectionOperator(&ct[leftIdx], &ct[rightIdx], binOp, rightOp, leftIdx, rightIdx, resultIdx)
	ct = append(ct, *outputType)
	if sMem, ok := op.(colexec.StaticMemoryOperator); ok {
		memUsed += sMem.EstimateStaticMemoryUsage()
	}
	return op, resultIdx, ct, leftMem + rightMem + memUsed, err
}
