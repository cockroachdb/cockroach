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
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func checkNumIn(inputs []Operator, numIn int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// wrapRowSource, given an input Operator, integrates toWrap into a columnar
// execution flow and returns toWrap's output as an Operator.
func wrapRowSource(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input Operator,
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
	Op              Operator
	ColumnTypes     []types.T
	MemUsage        int
	MetadataSources []execinfrapb.MetadataSource
	IsStreaming     bool
}

// joinerPlanningState is a helper struct used when creating a hash or merge
// joiner to track the planning state.
type joinerPlanningState struct {
	// postJoinerProjection is the projection that has to be added after a
	// joiner. It is needed because the joiners always output all the requested
	// columns from the left side first followed by the columns from the right
	// side. However, post.OutputColumns projection can have an arbitrary order
	// of columns, and postJoinerProjection behaves as an "adapter" between the
	// output of the joiner and the requested post.OutputColumns projection.
	postJoinerProjection []uint32

	// postFilterPlanning will be set by the operators that handle the
	// projection themselves. This is needed to handle post.Filter correctly so
	// that those operators output all the columns that are used by post.Filter
	// even if some columns are not needed by post.OutputColumns. If it remains
	// unset, then postFilterPlanning will act as a noop.
	postFilterPlanning filterPlanningState
}

// createJoiner adds a new hash or merge join with the argument function
// createJoinOpWithOnExprPlanning distinguishing between the two.
// Note: the passed in 'result' will be modified accordingly.
func createJoiner(
	result *NewColOperatorResult,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ProcessorSpec,
	inputs []Operator,
	planningState *joinerPlanningState,
	joinType sqlbase.JoinType,
	createJoinOpWithOnExprPlanning func(
		result *NewColOperatorResult,
		leftTypes, rightTypes []coltypes.T,
		leftOutCols, rightOutCols []uint32,
	) (*execinfrapb.Expression, filterPlanningState, []uint32, []uint32, error),
) error {
	var err error
	if err = checkNumIn(inputs, 2); err != nil {
		return err
	}

	post := &spec.Post

	var leftTypes, rightTypes []coltypes.T
	leftTypes, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
	if err != nil {
		return err
	}
	rightTypes, err = typeconv.FromColumnTypes(spec.Input[1].ColumnTypes)
	if err != nil {
		return err
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
		// Now that we know how many columns are output from the left side, we
		// can populate the "post-joiner" projection. Consider an example:
		// we have post.OutputColumns = {6, 2, 5, 7, 0, 3} with nLeftCols = 6.
		// We've just populated output columns as follows:
		// leftOutCols = {2, 5, 0, 3} and rightOutCols = {6, 7},
		// and because the joiner always outputs the left columns first, the output
		// will look as {2, 5, 0, 3, 6, 7}, so we need to add an extra projection.
		// The code below will populate postJoinerProjection with
		// {4, 0, 1, 5, 2, 3}.
		// Note that we don't need to pay attention to any filter planning
		// additions since those will be projected out before we will add this
		// "post-joiner" projection.
		var lOutIdx, rOutIdx uint32
		for _, col := range post.OutputColumns {
			if col < nLeftCols {
				planningState.postJoinerProjection = append(planningState.postJoinerProjection, lOutIdx)
				lOutIdx++
			} else {
				planningState.postJoinerProjection = append(planningState.postJoinerProjection, uint32(len(leftOutCols))+rOutIdx)
				rOutIdx++
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

	if !post.Filter.Empty() {
		planningState.postFilterPlanning = makeFilterPlanningState(len(leftTypes), len(rightTypes))
		leftOutCols, rightOutCols, err = planningState.postFilterPlanning.renderAllNeededCols(
			post.Filter, leftOutCols, rightOutCols,
		)
		if err != nil {
			return err
		}
	}

	var (
		onExpr         *execinfrapb.Expression
		onExprPlanning filterPlanningState
	)
	onExpr, onExprPlanning, leftOutCols, rightOutCols, err = createJoinOpWithOnExprPlanning(
		result, leftTypes, rightTypes, leftOutCols, rightOutCols,
	)
	if err != nil {
		return err
	}

	result.setProjectedByJoinerColumnTypes(spec, leftOutCols, rightOutCols)

	if onExpr != nil && joinType == sqlbase.JoinType_INNER {
		err = result.planFilterExpr(flowCtx.NewEvalCtx(), *onExpr, onExprPlanning.indexVarMap)
		onExprPlanning.projectOutExtraCols(result)
	}
	return err
}

// NewColOperator creates a new columnar operator according to the given spec.
func NewColOperator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ProcessorSpec,
	inputs []Operator,
) (result NewColOperatorResult, err error) {
	log.VEventf(ctx, 2, "planning col operator for spec %q", spec)

	core := &spec.Core
	post := &spec.Post

	var planningState joinerPlanningState

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
		result.Op, result.IsStreaming = NewNoop(inputs[0]), true
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
		// only colBatchScan with a CancelChecker allows us to do just that.
		// It's sufficient for most of the operators since they are extremely fast.
		// However, some of the long-running operators (for example, sorter) are
		// still responsible for doing the cancellation check on their own while
		// performing long operations.
		result.Op = NewCancelChecker(result.Op)
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
			result.Op, result.IsStreaming, err = NewSingleTupleNoInputOp(), true, nil
			// We make ColumnTypes non-nil so that sanity check doesn't panic.
			result.ColumnTypes = make([]types.T, 0)
			break
		}
		if len(aggSpec.GroupCols) == 0 &&
			len(aggSpec.Aggregations) == 1 &&
			aggSpec.Aggregations[0].FilterColIdx == nil &&
			aggSpec.Aggregations[0].Func == execinfrapb.AggregatorSpec_COUNT_ROWS &&
			!aggSpec.Aggregations[0].Distinct {
			result.Op, result.IsStreaming, err = NewCountOp(inputs[0]), true, nil
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
			result.Op, err = NewHashAggregator(
				inputs[0], typs, aggFns, aggSpec.GroupCols, aggCols, execinfrapb.IsScalarAggregate(aggSpec),
			)
		} else {
			result.Op, err = NewOrderedAggregator(
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
		result.Op, err = NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)
		result.IsStreaming = true

	case core.Ordinality != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return result, err
		}
		result.ColumnTypes = append(spec.Input[0].ColumnTypes, *types.Int)
		result.Op, result.IsStreaming = NewOrdinalityOp(inputs[0]), true

	case core.HashJoiner != nil:
		createHashJoinerWithOnExprPlanning := func(
			result *NewColOperatorResult,
			leftTypes, rightTypes []coltypes.T,
			leftOutCols, rightOutCols []uint32,
		) (*execinfrapb.Expression, filterPlanningState, []uint32, []uint32, error) {
			var (
				onExpr         *execinfrapb.Expression
				onExprPlanning filterPlanningState
			)
			if !core.HashJoiner.OnExpr.Empty() {
				if core.HashJoiner.Type != sqlbase.JoinType_INNER {
					return onExpr, onExprPlanning, leftOutCols, rightOutCols, errors.Newf("can't plan non-inner hash join with on expressions")
				}
				onExpr = &core.HashJoiner.OnExpr
				onExprPlanning = makeFilterPlanningState(len(leftTypes), len(rightTypes))
				leftOutCols, rightOutCols, err = onExprPlanning.renderAllNeededCols(
					*onExpr, leftOutCols, rightOutCols,
				)
				if err != nil {
					return onExpr, onExprPlanning, leftOutCols, rightOutCols, err
				}
			}

			result.Op, err = NewEqHashJoinerOp(
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
			return onExpr, onExprPlanning, leftOutCols, rightOutCols, err
		}

		err = createJoiner(
			&result, flowCtx, spec, inputs, &planningState, core.HashJoiner.Type,
			createHashJoinerWithOnExprPlanning,
		)

	case core.MergeJoiner != nil:
		if core.MergeJoiner.Type.IsSetOpJoin() {
			return result, errors.AssertionFailedf("unexpectedly %s merge join was planned", core.MergeJoiner.Type.String())
		}
		// Merge joiner is a streaming operator when equality columns form a key
		// for both of the inputs.
		result.IsStreaming = core.MergeJoiner.LeftEqColumnsAreKey && core.MergeJoiner.RightEqColumnsAreKey

		createMergeJoinerWithOnExprPlanning := func(
			result *NewColOperatorResult,
			leftTypes, rightTypes []coltypes.T,
			leftOutCols, rightOutCols []uint32,
		) (*execinfrapb.Expression, filterPlanningState, []uint32, []uint32, error) {
			var (
				onExpr            *execinfrapb.Expression
				onExprPlanning    filterPlanningState
				filterOnlyOnLeft  bool
				filterConstructor func(Operator) (Operator, error)
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
				onExprPlanning = makeFilterPlanningState(len(leftTypes), len(rightTypes))
				switch core.MergeJoiner.Type {
				case sqlbase.JoinType_INNER:
					leftOutCols, rightOutCols, err = onExprPlanning.renderAllNeededCols(
						*onExpr, leftOutCols, rightOutCols,
					)
				case sqlbase.JoinType_LEFT_SEMI, sqlbase.JoinType_LEFT_ANTI:
					filterOnlyOnLeft, err = onExprPlanning.isFilterOnlyOnLeft(*onExpr)
					filterConstructor = func(op Operator) (Operator, error) {
						r := NewColOperatorResult{
							Op:          op,
							ColumnTypes: append(spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes...),
						}
						// We don't need to specify indexVarMap because the filter will be
						// run alongside the merge joiner, and it will have access to all
						// of the columns from both sides.
						err := r.planFilterExpr(flowCtx.NewEvalCtx(), *onExpr, nil /* indexVarMap */)
						return r.Op, err
					}
				default:
					return onExpr, onExprPlanning, leftOutCols, rightOutCols, errors.Errorf("can only plan INNER, LEFT SEMI, and LEFT ANTI merge joins with ON expressions")
				}
			}
			if err != nil {
				return onExpr, onExprPlanning, leftOutCols, rightOutCols, err
			}

			result.Op, err = NewMergeJoinOp(
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
			return onExpr, onExprPlanning, leftOutCols, rightOutCols, err
		}

		err = createJoiner(
			&result, flowCtx, spec, inputs, &planningState, core.MergeJoiner.Type,
			createMergeJoinerWithOnExprPlanning,
		)

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
			result.Op, err = NewSortChunks(input, inputTypes, orderingCols, int(matchLen))
		} else if post.Limit != 0 && post.Filter.Empty() && post.Limit+post.Offset < math.MaxUint16 {
			// There is a limit specified with no post-process filter, so we know
			// exactly how many rows the sorter should output. Choose a top K sorter,
			// which uses a heap to avoid storing more rows than necessary.
			k := uint16(post.Limit + post.Offset)
			result.Op, result.IsStreaming = NewTopKSorter(input, inputTypes, orderingCols, k), true
		} else {
			// No optimizations possible. Default to the standard sort operator.
			result.Op, err = NewSorter(input, inputTypes, orderingCols)
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
			input, err = NewWindowSortingPartitioner(input, typs, core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx))
			tempPartitionColOffset, partitionColIdx = 1, int(wf.OutputColIdx)
		} else {
			if len(wf.Ordering.Columns) > 0 {
				input, err = NewSorter(input, typs, wf.Ordering.Columns)
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
			result.Op = NewRowNumberOperator(input, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case execinfrapb.WindowerSpec_RANK:
			result.Op, err = NewRankOperator(input, typs, false /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
		case execinfrapb.WindowerSpec_DENSE_RANK:
			result.Op, err = NewRankOperator(input, typs, true /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
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
			result.Op = NewSimpleProjectOp(result.Op, int(wf.OutputColIdx+1), projection)
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
	if sMemOp, ok := result.Op.(StaticMemoryOperator); ok {
		result.MemUsage += sMemOp.EstimateStaticMemoryUsage()
	}

	log.VEventf(ctx, 1, "made op %T\n", result.Op)

	// Note: at this point, it is legal for ColumnTypes to be empty (it is
	// legal for empty rows to be passed between processors).

	if !post.Filter.Empty() {
		if err = result.planFilterExpr(flowCtx.NewEvalCtx(), post.Filter, planningState.postFilterPlanning.indexVarMap); err != nil {
			return result, err
		}
		planningState.postFilterPlanning.projectOutExtraCols(&result)
	}
	if post.Projection {
		if len(planningState.postJoinerProjection) > 0 {
			result.addProjection(planningState.postJoinerProjection)
		} else {
			result.addProjection(post.OutputColumns)
		}
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
		result.Op = NewSimpleProjectOp(result.Op, len(result.ColumnTypes), renderedCols)
		newTypes := make([]types.T, 0, len(renderedCols))
		for _, j := range renderedCols {
			newTypes = append(newTypes, result.ColumnTypes[j])
		}
		result.ColumnTypes = newTypes
	}
	if post.Offset != 0 {
		result.Op = NewOffsetOp(result.Op, post.Offset)
	}
	if post.Limit != 0 {
		result.Op = NewLimitOp(result.Op, post.Limit)
	}
	return result, err
}

type filterPlanningState struct {
	numLeftInputCols  int
	numRightInputCols int
	// indexVarMap will be populated when rendering all needed columns in case
	// when at least one column from either side is used by the filter.
	indexVarMap []int
	// originalLeftOutCols and originalRightOutCols are stored so that we can
	// remove all the extra columns that were added to handle the filter.
	originalLeftOutCols  []uint32
	originalRightOutCols []uint32
}

func makeFilterPlanningState(numLeftInputCols, numRightInputCols int) filterPlanningState {
	return filterPlanningState{
		numLeftInputCols:  numLeftInputCols,
		numRightInputCols: numRightInputCols,
	}
}

// renderAllNeededCols makes sure that all columns used by filter expression
// will be output. It does so by extracting the indices of all indexed vars
// used in the expression and appending those that are missing from *OutCols
// slices to the slices. Additionally, it populates p.indexVarMap to be used
// later to correctly remap the indexed vars and stores the original *OutCols
// to be projected after the filter has been run.
// It returns updated leftOutCols and rightOutCols.
// NOTE: projectOutExtraCols must be called after the filter has been run.
func (p *filterPlanningState) renderAllNeededCols(
	filter execinfrapb.Expression, leftOutCols []uint32, rightOutCols []uint32,
) ([]uint32, []uint32, error) {
	neededColumnsForFilter, err := findIVarsInRange(
		filter,
		0, /* start */
		p.numLeftInputCols+p.numRightInputCols,
	)
	if err != nil {
		return nil, nil, errors.Errorf("error parsing filter expression %q: %s", filter, err)
	}
	if len(neededColumnsForFilter) > 0 {
		// Store the original out columns to be restored later.
		p.originalLeftOutCols = leftOutCols
		p.originalRightOutCols = rightOutCols
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
				}
			}
		}
	}
	return leftOutCols, rightOutCols, nil
}

// isFilterOnlyOnLeft returns whether the filter expression doesn't use columns
// from the right side.
func (p *filterPlanningState) isFilterOnlyOnLeft(filter execinfrapb.Expression) (bool, error) {
	// Find all needed columns for filter only from the right side.
	neededColumnsForFilter, err := findIVarsInRange(
		filter, p.numLeftInputCols, p.numLeftInputCols+p.numRightInputCols,
	)
	if err != nil {
		return false, errors.Errorf("error parsing filter expression %q: %s", filter, err)
	}
	return len(neededColumnsForFilter) == 0, nil
}

// projectOutExtraCols, possibly, adds a projection to remove all the extra
// columns that were needed by the filter expression.
// NOTE: result.ColumnTypes is updated if the projection is added.
func (p *filterPlanningState) projectOutExtraCols(result *NewColOperatorResult) {
	if p.indexVarMap == nil {
		// If p.indexVarMap is nil, then this filter planning didn't add any extra
		// columns, so there is nothing to project out.
		return
	}
	projection := make([]uint32, 0, len(p.originalLeftOutCols)+len(p.originalRightOutCols))
	for _, i := range p.originalLeftOutCols {
		projection = append(projection, uint32(p.indexVarMap[i]))
	}
	rColOffset := uint32(p.numLeftInputCols)
	for _, i := range p.originalRightOutCols {
		projection = append(projection, uint32(p.indexVarMap[rColOffset+i]))
	}
	result.Op = NewSimpleProjectOp(result.Op, len(result.ColumnTypes), projection)

	// Update output column types according to the projection.
	newTypes := make([]types.T, 0, len(projection))
	for _, j := range projection {
		newTypes = append(newTypes, result.ColumnTypes[j])
	}
	result.ColumnTypes = newTypes
}

// setProjectedByJoinerColumnTypes sets column types on r according to a
// joiner handled projection.
// NOTE: r.ColumnTypes is updated.
func (r *NewColOperatorResult) setProjectedByJoinerColumnTypes(
	spec *execinfrapb.ProcessorSpec, leftOutCols, rightOutCols []uint32,
) {
	r.ColumnTypes = make([]types.T, 0, len(leftOutCols)+len(rightOutCols))
	for _, leftOutCol := range leftOutCols {
		r.ColumnTypes = append(r.ColumnTypes, spec.Input[0].ColumnTypes[leftOutCol])
	}
	for _, rightOutCol := range rightOutCols {
		r.ColumnTypes = append(r.ColumnTypes, spec.Input[1].ColumnTypes[rightOutCol])
	}
}

func (r *NewColOperatorResult) planFilterExpr(
	evalCtx *tree.EvalContext, filter execinfrapb.Expression, indexVarMap []int,
) error {
	var (
		helper       execinfra.ExprHelper
		selectionMem int
	)
	err := helper.InitWithRemapping(filter, r.ColumnTypes, evalCtx, indexVarMap)
	if err != nil {
		return err
	}
	if helper.Expr == tree.DNull {
		// The filter expression is tree.DNull meaning that it is always false, so
		// we put a zero operator.
		r.Op = NewZeroOp(r.Op)
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
		r.Op = NewSimpleProjectOp(r.Op, len(filterColumnTypes), outputColumns)
	}
	return nil
}

// addProjection adds a simple projection to r (Op and ColumnTypes are updated
// accordingly).
func (r *NewColOperatorResult) addProjection(projection []uint32) {
	r.Op = NewSimpleProjectOp(r.Op, len(r.ColumnTypes), projection)
	// Update output ColumnTypes.
	newTypes := make([]types.T, 0, len(projection))
	for _, j := range projection {
		newTypes = append(newTypes, r.ColumnTypes[j])
	}
	r.ColumnTypes = newTypes
}

func planSelectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []types.T, input Operator,
) (op Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return NewBoolVecToSelOp(input, t.Idx), -1, columnTypes, memUsed, nil
	case *tree.AndExpr:
		// AND expressions are handled by an implicit AND'ing of selection vectors.
		// First we select out the tuples that are true on the left side, and then,
		// only among the matched tuples, we select out the tuples that are true on
		// the right side.
		var leftOp, rightOp Operator
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
		//
		// Rewrite the OR expression as an equivalent CASE expression.
		// "a OR b" becomes "CASE WHEN a THEN true WHEN b THEN true ELSE false END".
		// This way we can take advantage of the short-circuiting logic built into
		// the CASE operator. (b should not be evaluated if a is true.)
		caseExpr, err := tree.NewTypedCaseExpr(
			nil, /* expr */
			[]*tree.When{
				{Cond: t.Left, Val: tree.DBoolTrue},
				{Cond: t.Right, Val: tree.DBoolTrue},
			},
			tree.DBoolFalse,
			types.Bool)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		op, resultIdx, ct, memUsed, err = planProjectionOperators(ctx, caseExpr, columnTypes, input)
		op = NewBoolVecToSelOp(op, resultIdx)
		return op, resultIdx, ct, memUsed, err
	case *tree.CaseExpr:
		op, resultIdx, ct, memUsed, err = planProjectionOperators(ctx, expr, columnTypes, input)
		op = NewBoolVecToSelOp(op, resultIdx)
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
				op, err = GetLikeOperator(
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
				op, err = GetInOperator(lTyp, leftOp, leftIdx, datumTuple, negate)
				return op, resultIdx, ct, memUsageLeft, err
			}
			if t.Operator == tree.IsDistinctFrom || t.Operator == tree.IsNotDistinctFrom {
				if t.Right != tree.DNull {
					err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
					return nil, resultIdx, ct, memUsageLeft, err
				}
				// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we want to
				// negate when IS DISTINCT FROM is used.
				negate := t.Operator == tree.IsDistinctFrom
				op = newIsNullSelOp(leftOp, leftIdx, negate)
				return op, resultIdx, ct, memUsageLeft, err
			}
			op, err := GetSelectionConstOperator(lTyp, t.TypedRight().ResolvedType(), cmpOp, leftOp, leftIdx, constArg)
			return op, resultIdx, ct, memUsageLeft, err
		}
		rightOp, rightIdx, ct, memUsageRight, err := planProjectionOperators(ctx, t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, ct, memUsageLeft + memUsageRight, err
		}
		op, err := GetSelectionOperator(lTyp, &ct[rightIdx], cmpOp, rightOp, leftIdx, rightIdx)
		return op, resultIdx, ct, memUsageLeft + memUsageRight, err
	default:
		return nil, resultIdx, nil, memUsed, errors.Errorf("unhandled selection expression type: %s", reflect.TypeOf(t))
	}
}

// planTypedMaybeNullProjectionOperators is used to plan projection operators, but is able to
// plan constNullOperators in the case that we know the "type" of the null. It is currently
// unsafe to plan a constNullOperator when we don't know the type of the null.
func planTypedMaybeNullProjectionOperators(
	ctx *tree.EvalContext,
	expr tree.TypedExpr,
	exprTyp *types.T,
	columnTypes []types.T,
	input Operator,
) (op Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	if expr == tree.DNull {
		resultIdx = len(columnTypes)
		op = NewConstNullOp(input, resultIdx, typeconv.FromColumnType(exprTyp))
		ct = append(columnTypes, *exprTyp)
		memUsed = op.(StaticMemoryOperator).EstimateStaticMemoryUsage()
		return op, resultIdx, ct, memUsed, nil
	}
	return planProjectionOperators(ctx, expr, columnTypes, input)
}

// planProjectionOperators plans a chain of operators to execute the provided
// expression. It returns the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planProjectionOperators(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []types.T, input Operator,
) (op Operator, resultIdx int, ct []types.T, memUsed int, err error) {
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
		// If the expression is NULL, we use planTypedMaybeNullProjectionOperators instead of planProjectionOperators
		// because we can say that the type of the NULL is the type that we are casting to, rather than unknown.
		// We can't use planProjectionOperators because it will reject planning a constNullOp without knowing
		// the post typechecking "type" of the NULL.
		if expr.ResolvedType() == types.Unknown {
			op, resultIdx, ct, memUsed, err = planTypedMaybeNullProjectionOperators(ctx, expr, t.Type, columnTypes, input)
		} else {
			op, resultIdx, ct, memUsed, err = planProjectionOperators(ctx, expr, columnTypes, input)
		}
		if err != nil {
			return nil, 0, nil, 0, err
		}
		outputIdx := len(ct)
		op, err = GetCastOperator(op, resultIdx, outputIdx, expr.ResolvedType(), t.Type)
		ct = append(ct, *t.Type)
		if sMem, ok := op.(StaticMemoryOperator); ok {
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
		op = NewBuiltinFunctionOperator(ctx, t, ct, inputCols, resultIdx, op)
		return op, resultIdx, ct, memUsed, nil
	case tree.Datum:
		datumType := t.ResolvedType()
		ct = columnTypes
		resultIdx = len(ct)
		ct = append(ct, *datumType)
		if datumType.Family() == types.UnknownFamily {
			return nil, resultIdx, ct, memUsed, errors.New("cannot plan null type unknown")
		}
		typ := typeconv.FromColumnType(datumType)
		constVal, err := typeconv.GetDatumToPhysicalFn(datumType)(t)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		op, err := NewConstOp(input, typ, constVal, resultIdx)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		return op, resultIdx, ct, memUsed, nil
	case *tree.CaseExpr:
		if t.Expr != nil {
			return nil, resultIdx, ct, 0, errors.New("CASE <expr> WHEN expressions unsupported")
		}

		buffer := NewBufferOp(input)
		caseOps := make([]Operator, len(t.Whens))
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
			caseOps[i], resultIdx, ct, whenMemUsed, err = planTypedMaybeNullProjectionOperators(
				ctx, when.Cond.(tree.TypedExpr), t.ResolvedType(), ct, buffer)
			if err != nil {
				return nil, resultIdx, ct, 0, err
			}
			// Transform the booleans to a selection vector.
			caseOps[i] = NewBoolVecToSelOp(caseOps[i], resultIdx)

			// Run the "then" clause on those tuples that were selected.
			caseOps[i], thenIdxs[i], ct, thenMemUsed, err = planTypedMaybeNullProjectionOperators(
				ctx, when.Val.(tree.TypedExpr), t.ResolvedType(), ct, caseOps[i])
			if err != nil {
				return nil, resultIdx, ct, 0, err
			}

			memUsed += whenMemUsed + thenMemUsed
		}
		var elseMem int
		var elseOp Operator
		elseExpr := t.Else
		if elseExpr == nil {
			// If there's no ELSE arm, we write NULLs.
			elseExpr = tree.DNull
		}
		elseOp, thenIdxs[len(t.Whens)], ct, elseMem, err = planTypedMaybeNullProjectionOperators(
			ctx, elseExpr.(tree.TypedExpr), t.ResolvedType(), ct, buffer)
		if err != nil {
			return nil, resultIdx, ct, 0, err
		}
		memUsed += elseMem

		op := NewCaseOp(buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)

		return op, caseOutputIdx, ct, memUsed, nil
	case *tree.AndExpr, *tree.OrExpr:
		return planLogicalProjectionOp(ctx, expr, columnTypes, input)
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
	input Operator,
) (op Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	resultIdx = -1
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	lConstArg, lConst := left.(tree.Datum)
	if lConst {
		// Case one: The left is constant.
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for non-commutative
		// operators such as - and /, though, so we still need this case.
		var rightOp Operator
		var rightIdx int
		rightOp, rightIdx, ct, memUsed, err = planProjectionOperators(ctx, right, columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, memUsed, err
		}
		resultIdx = len(ct)
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = GetProjectionLConstOperator(left.ResolvedType(), &ct[rightIdx], binOp, rightOp, rightIdx, lConstArg, resultIdx)
		ct = append(ct, *outputType)
		if sMem, ok := op.(StaticMemoryOperator); ok {
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
			op, err = GetLikeProjectionOperator(
				ctx, leftOp, leftIdx, resultIdx, string(tree.MustBeDString(rConstArg)), negate)
		} else if binOp == tree.In || binOp == tree.NotIn {
			negate := binOp == tree.NotIn
			datumTuple, ok := tree.AsDTuple(rConstArg)
			if !ok {
				err = errors.Errorf("IN operator supported only on constant expressions")
				return nil, resultIdx, ct, leftMem, err
			}
			op, err = GetInProjectionOperator(&ct[leftIdx], leftOp, leftIdx, resultIdx, datumTuple, negate)
		} else if binOp == tree.IsDistinctFrom || binOp == tree.IsNotDistinctFrom {
			if right != tree.DNull {
				err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
				return nil, resultIdx, ct, leftMem, err
			}
			// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we want to
			// negate when IS DISTINCT FROM is used.
			negate := binOp == tree.IsDistinctFrom
			op = newIsNullProjOp(leftOp, leftIdx, resultIdx, negate)
		} else {
			op, err = GetProjectionRConstOperator(&ct[leftIdx], right.ResolvedType(), binOp, leftOp, leftIdx, rConstArg, resultIdx)
		}
		ct = append(ct, *outputType)
		if sMem, ok := op.(StaticMemoryOperator); ok {
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
	op, err = GetProjectionOperator(&ct[leftIdx], &ct[rightIdx], binOp, rightOp, leftIdx, rightIdx, resultIdx)
	ct = append(ct, *outputType)
	if sMem, ok := op.(StaticMemoryOperator); ok {
		memUsed += sMem.EstimateStaticMemoryUsage()
	}
	return op, resultIdx, ct, leftMem + rightMem + memUsed, err
}

// planLogicalProjectionOp plans all the needed operators for a projection of
// a logical operation (either AND or OR).
func planLogicalProjectionOp(
	ctx *tree.EvalContext, expr tree.TypedExpr, columnTypes []types.T, input Operator,
) (op Operator, resultIdx int, ct []types.T, memUsed int, err error) {
	// Add a new boolean column that will store the result of the projection.
	resultIdx = len(columnTypes)
	ct = append(columnTypes, *types.Bool)
	var (
		typedLeft, typedRight                       tree.TypedExpr
		leftProjOpChain, rightProjOpChain, outputOp Operator
		leftIdx, rightIdx, lMemUsed, rMemUsed       int
		leftFeedOp, rightFeedOp                     feedOperator
	)
	switch t := expr.(type) {
	case *tree.AndExpr:
		typedLeft = t.TypedLeft()
		typedRight = t.TypedRight()
	case *tree.OrExpr:
		typedLeft = t.TypedLeft()
		typedRight = t.TypedRight()
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("unexpected logical expression type %s", t.String()))
	}
	leftProjOpChain, leftIdx, ct, lMemUsed, err = planTypedMaybeNullProjectionOperators(
		ctx, typedLeft, types.Bool, ct, &leftFeedOp,
	)
	if err != nil {
		return nil, resultIdx, ct, 0, err
	}
	rightProjOpChain, rightIdx, ct, rMemUsed, err = planTypedMaybeNullProjectionOperators(
		ctx, typedRight, types.Bool, ct, &rightFeedOp,
	)
	if err != nil {
		return nil, resultIdx, ct, 0, err
	}
	switch expr.(type) {
	case *tree.AndExpr:
		outputOp = NewAndProjOp(
			input, leftProjOpChain, rightProjOpChain,
			&leftFeedOp, &rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	case *tree.OrExpr:
		outputOp = NewOrProjOp(
			input, leftProjOpChain, rightProjOpChain,
			&leftFeedOp, &rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	}
	return outputOp, resultIdx, ct, lMemUsed + rMemUsed, nil
}
