// Copyright 2018 The Cockroach Authors.
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

package distsqlrun

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	inputTypes []semtypes.ColumnType,
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
	var columnTypes []semtypes.ColumnType

	switch {
	case core.Noop != nil:
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		op = exec.NewNoop(inputs[0])
	case core.TableReader != nil:
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		op, err = newColBatchScan(flowCtx, core.TableReader, post)
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
		for _, col := range aggSpec.GroupCols {
			if !orderedCols.Contains(int(col)) {
				return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
					"unsorted aggregation not supported")
			}
			groupCols.Add(int(col))
		}

		groupTyps := make([]types.T, len(spec.Input[0].ColumnTypes))
		for i := range spec.Input[0].ColumnTypes {
			groupTyps[i] = conv.FromColumnType(spec.Input[0].ColumnTypes[i])
		}
		if !orderedCols.SubsetOf(groupCols) {
			return nil, pgerror.NewAssertionErrorf("ordered cols must be a subset of grouping cols")
		}

		aggTyps := make([][]types.T, len(aggSpec.Aggregations))
		aggCols := make([][]uint32, len(aggSpec.Aggregations))
		aggFns := make([]distsqlpb.AggregatorSpec_Func, len(aggSpec.Aggregations))
		for i, agg := range aggSpec.Aggregations {
			if agg.Distinct {
				return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
					"distinct aggregation not supported")
			}
			if agg.FilterColIdx != nil {
				return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
					"filtering aggregation not supported")
			}
			if len(agg.Arguments) > 0 {
				return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
					"aggregates with arguments not supported")
			}
			aggTyps[i] = make([]types.T, len(agg.ColIdx))
			for j, colIdx := range agg.ColIdx {
				aggTyps[i][j] = conv.FromColumnType(spec.Input[0].ColumnTypes[colIdx])
			}
			aggCols[i] = agg.ColIdx
			aggFns[i] = agg.Func
			switch agg.Func {
			case distsqlpb.AggregatorSpec_SUM:
				switch aggTyps[i][0] {
				case types.Int8, types.Int16, types.Int32, types.Int64:
					// TODO(alfonso): plan ordinary SUM on integer types by casting to DECIMAL
					// at the end, mod issues with overflow. Perhaps to avoid the overflow
					// issues, at first, we could plan SUM for all types besides Int64.
					return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
						"sum on int cols not supported (use sum_int)")
				}
			}
		}
		op, err = exec.NewOrderedAggregator(
			inputs[0], aggSpec.GroupCols, groupTyps, aggFns, aggCols, aggTyps,
		)
		if err != nil {
			return nil, err
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
				return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
					"unsorted distinct not supported")
			}
			distinctCols.Add(int(col))
		}
		if !orderedCols.SubsetOf(distinctCols) {
			return nil, pgerror.NewAssertionErrorf("ordered cols must be a subset of distinct cols")
		}

		columnTypes = spec.Input[0].ColumnTypes
		typs := conv.FromColumnTypes(columnTypes)
		op, err = exec.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)

	case core.HashJoiner != nil:
		if err := checkNumIn(inputs, 2); err != nil {
			return nil, err
		}

		if !core.HashJoiner.OnExpr.Empty() {
			return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
				"can't plan hash join with on expressions")
		}

		leftTypes := conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		rightTypes := conv.FromColumnTypes(spec.Input[1].ColumnTypes)

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
			return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
				"can't plan merge join with on expressions")
		}
		if core.MergeJoiner.Type != sqlbase.InnerJoin {
			return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
				"can plan only inner merge join")
		}

		leftTypes := conv.FromColumnTypes(spec.Input[0].ColumnTypes)
		rightTypes := conv.FromColumnTypes(spec.Input[1].ColumnTypes)

		nLeftCols := uint32(len(leftTypes))
		nRightCols := uint32(len(rightTypes))

		leftEqCols := make([]uint32, 0, nLeftCols)
		rightEqCols := make([]uint32, 0, nRightCols)

		for _, oCol := range core.MergeJoiner.LeftOrdering.Columns {
			leftEqCols = append(leftEqCols, oCol.ColIdx)
		}

		for _, oCol := range core.MergeJoiner.RightOrdering.Columns {
			rightEqCols = append(rightEqCols, oCol.ColIdx)
		}

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
			leftEqCols,
			rightEqCols,
		)

		columnTypes = make([]semtypes.ColumnType, nLeftCols+nRightCols)
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
		if core.Sorter.OrderingMatchLen > 0 {
			op, err = exec.NewSortChunks(inputs[0],
				conv.FromColumnTypes(spec.Input[0].ColumnTypes),
				core.Sorter.OutputOrdering.Columns,
				int(core.Sorter.OrderingMatchLen))
		} else {
			op, err = exec.NewSorter(inputs[0],
				conv.FromColumnTypes(spec.Input[0].ColumnTypes),
				core.Sorter.OutputOrdering.Columns)
		}

	default:
		return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
			"unsupported processor core %s", core)
	}
	log.VEventf(ctx, 1, "Made op %T\n", op)

	if err != nil {
		return nil, err
	}

	if !post.Filter.Empty() {
		if columnTypes == nil {
			return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
				"unable to columnarize filter expression %q: columnTypes is unset", post.Filter.Expr)
		}
		var helper exprHelper
		err := helper.init(post.Filter, columnTypes, flowCtx.EvalCtx)
		if err != nil {
			return nil, err
		}
		var filterColumnTypes []semtypes.ColumnType
		op, _, filterColumnTypes, err = planExpressionOperators(helper.expr, columnTypes, op)
		if err != nil {
			return nil, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
				"unable to columnarize filter expression %q", post.Filter.Expr)
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
		if columnTypes == nil {
			return nil, pgerror.NewErrorf(pgerror.CodeDataExceptionError,
				"unable to columnarize projection. columnTypes is unset")
		}
		var renderedCols []uint32
		for _, expr := range post.RenderExprs {
			var helper exprHelper
			err := helper.init(expr, columnTypes, flowCtx.EvalCtx)
			if err != nil {
				return nil, err
			}
			var outputIdx int
			op, outputIdx, columnTypes, err = planExpressionOperators(helper.expr, columnTypes, op)
			if err != nil {
				return nil, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
					"unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return nil, pgerror.NewAssertionErrorf("missing outputIdx")
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

// planExpressionOperators plans a chain of operators to execute the provided
// expression. It returns the the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planExpressionOperators(
	expr tree.TypedExpr, columnTypes []semtypes.ColumnType, input exec.Operator,
) (op exec.Operator, resultIdx int, ct []semtypes.ColumnType, err error) {
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, nil
	case *tree.AndExpr:
		leftOp, _, ct, err := planExpressionOperators(t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		return planExpressionOperators(t.TypedRight(), ct, leftOp)
	case *tree.ComparisonExpr:
		// TODO(solon): Handle the case where a ComparisonExpr is a projection,
		// e.g. SELECT a > b FROM t. Currently we assume it is a selection.
		cmpOp := t.Operator
		leftOp, leftIdx, ct, err := planExpressionOperators(t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		typ := ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			op, err := exec.GetSelectionConstOperator(typ, cmpOp, leftOp, leftIdx, constArg)
			return op, resultIdx, ct, err
		}
		rightOp, rightIdx, ct, err := planExpressionOperators(t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		if !ct[leftIdx].Identical(&ct[rightIdx]) {
			err = errors.Errorf(
				"comparison between %s and %s is unhandled", ct[leftIdx].SemanticType,
				ct[rightIdx].SemanticType)
			return nil, resultIdx, ct, err
		}
		op, err := exec.GetSelectionOperator(typ, cmpOp, rightOp, leftIdx, rightIdx)
		return op, resultIdx, ct, err
	case *tree.BinaryExpr:
		binOp := t.Operator
		// There are 3 cases. Either the left is constant, the right is constant,
		// or neither are constant.
		lConstArg, lConst := t.Left.(tree.Datum)
		if lConst {
			// Case one: The left is constant.
			// Normally, the optimizer normalizes binary exprs so that the constant
			// argument is on the right side. This doesn't happen for non-commutative
			// operators such as - and /, though, so we still need this case.
			rightOp, rightIdx, ct, err := planExpressionOperators(t.TypedRight(), columnTypes, input)
			if err != nil {
				return nil, resultIdx, ct, err
			}
			resultIdx = len(ct)
			typ := ct[rightIdx]
			// The projection result will be outputted to a new column which is appended
			// to the input batch.
			op, err := exec.GetProjectionLConstOperator(typ, binOp, rightOp, rightIdx, lConstArg, resultIdx)
			ct = append(ct, typ)
			return op, resultIdx, ct, err
		}
		leftOp, leftIdx, ct, err := planExpressionOperators(t.TypedLeft(), columnTypes, input)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		typ := ct[leftIdx]
		if rConstArg, rConst := t.Right.(tree.Datum); rConst {
			// Case 2: The right is constant.
			// The projection result will be outputted to a new column which is appended
			// to the input batch.
			resultIdx = len(ct)
			op, err := exec.GetProjectionRConstOperator(typ, binOp, leftOp, leftIdx, rConstArg, resultIdx)
			ct = append(ct, typ)
			return op, resultIdx, ct, err
		}
		// Case 3: neither are constant.
		rightOp, rightIdx, ct, err := planExpressionOperators(t.TypedRight(), ct, leftOp)
		if err != nil {
			return nil, resultIdx, nil, err
		}
		if !ct[leftIdx].Identical(&ct[rightIdx]) {
			err = errors.Errorf(
				"projection on %s and %s is unhandled", ct[leftIdx].SemanticType,
				ct[rightIdx].SemanticType)
			return nil, resultIdx, ct, err
		}
		resultIdx = len(ct)
		op, err := exec.GetProjectionOperator(typ, binOp, rightOp, leftIdx, rightIdx, resultIdx)
		ct = append(ct, typ)
		return op, resultIdx, ct, err
	default:
		return nil, resultIdx, nil, errors.Errorf("unhandled expression type: %s", reflect.TypeOf(t))
	}
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
	metadataSourcesQueue := make([]MetadataSource, 0, 1)
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
		if metaSource, ok := op.(MetadataSource); ok {
			metadataSourcesQueue = append(metadataSourcesQueue, metaSource)
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
			proc, err := newMaterializer(
				&f.FlowCtx,
				pspec.ProcessorID,
				op,
				columnTypes,
				outputToInputColIdx,
				&distsqlpb.PostProcessSpec{},
				f.syncFlowConsumer,
				metadataSourcesQueue,
			)
			if err != nil {
				return err
			}
			metadataSourcesQueue = metadataSourcesQueue[:0]
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
