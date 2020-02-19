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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func checkNumIn(inputs []Operator, numIn int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// wrapRowSources, given input Operators, integrates toWrap into a columnar
// execution flow and returns toWrap's output as an Operator.
func wrapRowSources(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	inputs []Operator,
	inputTypes [][]types.T,
	acc *mon.BoundAccount,
	newToWrap func([]execinfra.RowSource) (execinfra.RowSource, error),
) (*Columnarizer, error) {
	var (
		toWrapInputs []execinfra.RowSource
		// TODO(asubiotto): Plumb proper processorIDs once we have stats.
		processorID int32
	)
	for i, input := range inputs {
		// Optimization: if the input is a Columnarizer, its input is necessarily a
		// execinfra.RowSource, so remove the unnecessary conversion.
		if c, ok := input.(*Columnarizer); ok {
			// TODO(asubiotto): We might need to do some extra work to remove references
			// to this operator (e.g. streamIDToOp).
			toWrapInputs = append(toWrapInputs, c.input)
		} else {
			toWrapInput, err := NewMaterializer(
				flowCtx,
				processorID,
				input,
				inputTypes[i],
				&execinfrapb.PostProcessSpec{},
				nil, /* output */
				nil, /* metadataSourcesQueue */
				nil, /* outputStatsToTrace */
				nil, /* cancelFlow */
			)
			if err != nil {
				return nil, err
			}
			toWrapInputs = append(toWrapInputs, toWrapInput)
		}
	}

	toWrap, err := newToWrap(toWrapInputs)
	if err != nil {
		return nil, err
	}

	return NewColumnarizer(ctx, NewAllocator(ctx, acc), flowCtx, processorID, toWrap)
}

// NewColOperatorArgs is a helper struct that encompasses all of the input
// arguments to NewColOperator call.
type NewColOperatorArgs struct {
	Spec                 *execinfrapb.ProcessorSpec
	Inputs               []Operator
	StreamingMemAccount  *mon.BoundAccount
	ProcessorConstructor execinfra.ProcessorConstructor
	TestingKnobs         struct {
		// UseStreamingMemAccountForBuffering specifies whether to use
		// StreamingMemAccount when creating buffering operators and should only be
		// set to 'true' in tests. The idea behind this flag is reducing the number
		// of memory accounts and monitors we need to close, so we plumbed it into
		// the planning code so that it doesn't create extra memory monitoring
		// infrastructure (and so that we could use testMemAccount defined in
		// main_test.go).
		UseStreamingMemAccountForBuffering bool
		// SpillingCallbackFn will be called when the spilling from an in-memory to
		// disk-backed operator occurs. It should only be set in tests.
		SpillingCallbackFn func()
		// DiskSpillingDisabled specifies whether only in-memory operators should
		// be created.
		DiskSpillingDisabled bool
	}
}

// NewColOperatorResult is a helper struct that encompasses all of the return
// values of NewColOperator call.
type NewColOperatorResult struct {
	Op                     Operator
	ColumnTypes            []types.T
	InternalMemUsage       int
	MetadataSources        []execinfrapb.MetadataSource
	IsStreaming            bool
	BufferingOpMemMonitors []*mon.BytesMonitor
	BufferingOpMemAccounts []*mon.BoundAccount
}

// isSupported checks whether we have a columnar operator equivalent to a
// processor described by spec. Note that it doesn't perform any other checks
// (like validity of the number of inputs).
func isSupported(spec *execinfrapb.ProcessorSpec) (bool, error) {
	core := spec.Core

	switch {
	case core.Noop != nil:
		return true, nil

	case core.TableReader != nil:
		if core.TableReader.IsCheck {
			return false, errors.Newf("scrub table reader is unsupported in vectorized")
		}
		return true, nil

	case core.Aggregator != nil:
		aggSpec := core.Aggregator
		for _, agg := range aggSpec.Aggregations {
			if agg.Distinct {
				return false, errors.Newf("distinct aggregation not supported")
			}
			if agg.FilterColIdx != nil {
				return false, errors.Newf("filtering aggregation not supported")
			}
			if len(agg.Arguments) > 0 {
				return false, errors.Newf("aggregates with arguments not supported")
			}
			var inputTypes []types.T
			for _, colIdx := range agg.ColIdx {
				inputTypes = append(inputTypes, spec.Input[0].ColumnTypes[colIdx])
			}
			if supported, err := isAggregateSupported(agg.Func, inputTypes); !supported {
				return false, err
			}
		}
		return true, nil

	case core.Distinct != nil:
		return true, nil

	case core.Ordinality != nil:
		return true, nil

	case core.HashJoiner != nil:
		if !core.HashJoiner.OnExpr.Empty() &&
			core.HashJoiner.Type != sqlbase.JoinType_INNER {
			return false, errors.Newf("can't plan non-inner hash join with on expressions")
		}
		return true, nil

	case core.MergeJoiner != nil:
		if !core.MergeJoiner.OnExpr.Empty() {
			switch core.MergeJoiner.Type {
			case sqlbase.JoinType_INNER, sqlbase.JoinType_LEFT_SEMI, sqlbase.JoinType_LEFT_ANTI:
			default:
				return false, errors.Errorf("can only plan INNER, LEFT SEMI, and LEFT ANTI merge joins with ON expressions")
			}
		}
		return true, nil

	case core.Sorter != nil:
		return true, nil

	case core.Windower != nil:
		if len(core.Windower.WindowFns) != 1 {
			return false, errors.Newf("only a single window function is currently supported")
		}
		wf := core.Windower.WindowFns[0]
		if wf.Frame != nil &&
			(wf.Frame.Mode != execinfrapb.WindowerSpec_Frame_RANGE ||
				wf.Frame.Bounds.Start.BoundType != execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING ||
				(wf.Frame.Bounds.End != nil && wf.Frame.Bounds.End.BoundType != execinfrapb.WindowerSpec_Frame_CURRENT_ROW)) {
			return false, errors.Newf("window functions with non-default window frames are not supported")
		}
		if wf.Func.AggregateFunc != nil {
			return false, errors.Newf("aggregate functions used as window functions are not supported")
		}

		switch *wf.Func.WindowFunc {
		case execinfrapb.WindowerSpec_ROW_NUMBER:
		case execinfrapb.WindowerSpec_RANK:
		case execinfrapb.WindowerSpec_DENSE_RANK:
		default:
			return false, errors.Newf("window function %s is not supported", wf.String())
		}
		return true, nil

	default:
		return false, errors.Newf("unsupported processor core %q", core)
	}
}

// NewColOperator creates a new columnar operator according to the given spec.
func NewColOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, args NewColOperatorArgs,
) (result NewColOperatorResult, err error) {
	// Make sure that we clean up memory monitoring infrastructure in case of an
	// error or a panic.
	defer func() {
		returnedErr := err
		panicErr := recover()
		if returnedErr != nil || panicErr != nil {
			for _, memAccount := range result.BufferingOpMemAccounts {
				memAccount.Close(ctx)
			}
			result.BufferingOpMemAccounts = result.BufferingOpMemAccounts[:0]
			for _, memMonitor := range result.BufferingOpMemMonitors {
				memMonitor.Stop(ctx)
			}
			result.BufferingOpMemMonitors = result.BufferingOpMemMonitors[:0]
		}
		if panicErr != nil {
			execerror.VectorizedInternalPanic(panicErr)
		}
	}()
	spec := args.Spec
	inputs := args.Inputs
	streamingMemAccount := args.StreamingMemAccount
	useStreamingMemAccountForBuffering := args.TestingKnobs.UseStreamingMemAccountForBuffering
	processorConstructor := args.ProcessorConstructor

	log.VEventf(ctx, 2, "planning col operator for spec %q", spec)

	core := &spec.Core
	post := &spec.Post

	// By default, we safely assume that an operator is not streaming. Note that
	// projections, renders, filters, limits, offsets as well as all internal
	// operators (like stats collectors and cancel checkers) are streaming, so in
	// order to determine whether the resulting chain of operators is streaming,
	// it is sufficient to look only at the "core" operator.
	result.IsStreaming = false

	supported, err := isSupported(spec)
	if !supported {
		// We refuse to wrap LocalPlanNode processor (which is a DistSQL wrapper
		// around a planNode) because it creates complications, and a flow with
		// such processor probably will not benefit from the vectorization.
		if core.LocalPlanNode != nil {
			return result, errors.Newf("core.LocalPlanNode is not supported")
		}
		// We also do not wrap MetadataTest{Sender,Receiver} because of the way
		// metadata is propagated through the vectorized flow - it is drained at
		// the flow shutdown unlike these test processors expect.
		if core.MetadataTestSender != nil {
			return result, errors.Newf("core.MetadataTestSender is not supported")
		}
		if core.MetadataTestReceiver != nil {
			return result, errors.Newf("core.MetadataTestReceiver is not supported")
		}

		log.VEventf(ctx, 1, "planning a wrapped processor because %s", err.Error())
		var (
			c          *Columnarizer
			inputTypes [][]types.T
		)

		for _, input := range spec.Input {
			inputTypes = append(inputTypes, input.ColumnTypes)
		}

		c, err = wrapRowSources(
			ctx,
			flowCtx,
			inputs,
			inputTypes,
			streamingMemAccount,
			func(inputs []execinfra.RowSource) (execinfra.RowSource, error) {
				// We provide a slice with a single nil as 'outputs' parameter because
				// all processors expect a single output. Passing nil is ok here
				// because when wrapping the processor, the materializer will be its
				// output, and it will be set up in wrapRowSources.
				proc, err := processorConstructor(
					ctx, flowCtx, spec.ProcessorID, core, post, inputs,
					[]execinfra.RowReceiver{nil}, /* outputs */
					nil,                          /* localProcessors */
				)
				if err != nil {
					return nil, err
				}
				var (
					rs execinfra.RowSource
					ok bool
				)
				if rs, ok = proc.(execinfra.RowSource); !ok {
					return nil, errors.Newf(
						"processor %s is not an execinfra.RowSource", core.String(),
					)
				}
				// The wrapped processors need to be passed the post-process specs,
				// since they inspect them to figure out information about needed
				// columns. This means that we'll let those processors do any renders
				// or filters, which isn't ideal. We could improve this.
				post = &execinfrapb.PostProcessSpec{}
				result.ColumnTypes = rs.OutputTypes()
				return rs, nil
			},
		)

		// We say that the wrapped processor is "streaming" because it is not a
		// buffering operator (even if it is a buffering processor). This is not a
		// problem for memory accounting because each processor does that on its
		// own, so the used memory will be accounted for.
		result.Op, result.IsStreaming = c, true
		result.MetadataSources = append(result.MetadataSources, c)
	} else {
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
			var scanOp *colBatchScan
			scanOp, err = newColBatchScan(NewAllocator(ctx, streamingMemAccount), flowCtx, core.TableReader, post)
			if err != nil {
				return result, err
			}
			result.Op, result.IsStreaming = scanOp, true
			result.MetadataSources = append(result.MetadataSources, scanOp)
			// colBatchScan is wrapped with a cancel checker below, so we need to
			// log its creation separately.
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
				result.Op, result.IsStreaming, err = NewSingleTupleNoInputOp(NewAllocator(ctx, streamingMemAccount)), true, nil
				// We make ColumnTypes non-nil so that sanity check doesn't panic.
				result.ColumnTypes = make([]types.T, 0)
				break
			}
			if len(aggSpec.GroupCols) == 0 &&
				len(aggSpec.Aggregations) == 1 &&
				aggSpec.Aggregations[0].FilterColIdx == nil &&
				aggSpec.Aggregations[0].Func == execinfrapb.AggregatorSpec_COUNT_ROWS &&
				!aggSpec.Aggregations[0].Distinct {
				result.Op, result.IsStreaming, err = NewCountOp(NewAllocator(ctx, streamingMemAccount), inputs[0]), true, nil
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
				aggTyps[i] = make([]types.T, len(agg.ColIdx))
				for j, colIdx := range agg.ColIdx {
					aggTyps[i][j] = spec.Input[0].ColumnTypes[colIdx]
				}
				aggCols[i] = agg.ColIdx
				aggFns[i] = agg.Func
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
				hashAggregatorMemAccount := streamingMemAccount
				if !useStreamingMemAccountForBuffering {
					hashAggregatorMemAccount = result.createBufferingMemAccount(ctx, flowCtx, "hash-aggregator")
				}
				result.Op, err = NewHashAggregator(
					NewAllocator(ctx, hashAggregatorMemAccount), inputs[0], typs, aggFns,
					aggSpec.GroupCols, aggCols, execinfrapb.IsScalarAggregate(aggSpec),
				)
			} else {
				result.Op, err = NewOrderedAggregator(
					NewAllocator(ctx, streamingMemAccount), inputs[0], typs, aggFns,
					aggSpec.GroupCols, aggCols, execinfrapb.IsScalarAggregate(aggSpec),
				)
				result.IsStreaming = true
			}

		case core.Distinct != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}

			var distinctCols, orderedCols util.FastIntSet
			allSorted := true

			for _, col := range core.Distinct.OrderedColumns {
				orderedCols.Add(int(col))
			}
			for _, col := range core.Distinct.DistinctColumns {
				distinctCols.Add(int(col))
				if !orderedCols.Contains(int(col)) {
					allSorted = false
				}
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
			// TODO(yuzefovich): implement the distinct on partially ordered columns.
			if allSorted {
				result.Op, err = NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)
				result.IsStreaming = true
			} else {
				result.Op = NewUnorderedDistinct(
					NewAllocator(ctx, streamingMemAccount), inputs[0],
					core.Distinct.DistinctColumns, typs,
				)
			}
		case core.Ordinality != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Op, result.IsStreaming = NewOrdinalityOp(NewAllocator(ctx, streamingMemAccount), inputs[0], outputIdx), true
			result.ColumnTypes = append(spec.Input[0].ColumnTypes, *types.Int)

		case core.HashJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return result, err
			}
			leftLogTypes := spec.Input[0].ColumnTypes
			leftPhysTypes, err := typeconv.FromColumnTypes(leftLogTypes)
			if err != nil {
				return result, err
			}
			rightLogTypes := spec.Input[1].ColumnTypes
			rightPhysTypes, err := typeconv.FromColumnTypes(rightLogTypes)
			if err != nil {
				return result, err
			}

			hashJoinerMemMonitorName := fmt.Sprintf("hash-joiner-%d", spec.ProcessorID)
			var hashJoinerMemAccount *mon.BoundAccount
			if useStreamingMemAccountForBuffering {
				hashJoinerMemAccount = streamingMemAccount
			} else {
				hashJoinerMemAccount = result.createBufferingMemAccount(
					ctx, flowCtx, hashJoinerMemMonitorName,
				)
			}
			// It is valid for empty set of equality columns to be considered as
			// "key" (for example, the input has at most 1 row). However, hash
			// joiner, in order to handle NULL values correctly, needs to think
			// that an empty set of equality columns doesn't form a key.
			rightEqColsAreKey := core.HashJoiner.RightEqColumnsAreKey && len(core.HashJoiner.RightEqColumns) > 0
			hjSpec, err := makeHashJoinerSpec(
				core.HashJoiner.Type,
				core.HashJoiner.LeftEqColumns,
				core.HashJoiner.RightEqColumns,
				leftPhysTypes,
				rightPhysTypes,
				rightEqColsAreKey,
			)
			if err != nil {
				return result, err
			}
			inMemoryHashJoiner := newHashJoiner(
				NewAllocator(ctx, hashJoinerMemAccount), hjSpec, inputs[0], inputs[1],
			)
			if args.TestingKnobs.DiskSpillingDisabled {
				// We will not be creating a disk-backed hash joiner because we're
				// running a test that explicitly asked for only in-memory hash
				// joiner.
				result.Op = inMemoryHashJoiner
			} else {
				result.Op = newTwoInputDiskSpiller(
					inputs[0], inputs[1], inMemoryHashJoiner.(bufferingInMemoryOperator),
					hashJoinerMemMonitorName,
					func(inputOne, inputTwo Operator) Operator {
						monitorNamePrefix := "external-hash-joiner-"
						allocator := NewAllocator(
							// Pass in the default limit explicitly since we don't want to
							// use the default memory limit of 1 if ForceDiskSpill is true, to
							// allow for the external hash join to have a normal amount of
							// memory (for initialization and internal joining).
							ctx, result.createBufferingMemAccountWithLimit(
								ctx, flowCtx, monitorNamePrefix, execinfra.GetWorkMemLimit(flowCtx.Cfg),
							))
						diskQueuesUnlimitedAllocator := NewAllocator(
							ctx, result.createBufferingUnlimitedMemAccount(
								ctx, flowCtx, monitorNamePrefix+"disk-queues",
							))
						return newExternalHashJoiner(
							allocator, hjSpec,
							inputOne, inputTwo,
							diskQueuesUnlimitedAllocator,
						)
					},
					args.TestingKnobs.SpillingCallbackFn,
				)
			}
			result.ColumnTypes = append(leftLogTypes, rightLogTypes...)

			if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type == sqlbase.JoinType_INNER {
				// We will plan other Operators on top of the hash joiner, so we need
				// to account for the internal memory explicitly.
				if internalMemOp, ok := result.Op.(InternalMemoryOperator); ok {
					result.InternalMemUsage += internalMemOp.InternalMemoryUsage()
				}
				if err = result.planFilterExpr(
					ctx, flowCtx.NewEvalCtx(), core.HashJoiner.OnExpr, streamingMemAccount,
				); err != nil {
					return result, err
				}
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

			leftLogTypes := spec.Input[0].ColumnTypes
			leftPhysTypes, err := typeconv.FromColumnTypes(leftLogTypes)
			if err != nil {
				return result, err
			}
			rightLogTypes := spec.Input[1].ColumnTypes
			rightPhysTypes, err := typeconv.FromColumnTypes(rightLogTypes)
			if err != nil {
				return result, err
			}

			var (
				onExpr            *execinfrapb.Expression
				filterOnlyOnLeft  bool
				filterConstructor func(Operator) (Operator, error)
			)
			joinType := core.MergeJoiner.Type
			if !core.MergeJoiner.OnExpr.Empty() {
				// At the moment, we want to be on the conservative side and not run
				// queries with ON expressions when vectorize=auto, so we say that the
				// merge join is not streaming which will reject running such a query
				// through vectorized engine with 'auto' setting.
				// TODO(yuzefovich): remove this when we're confident in ON expression
				// support.
				result.IsStreaming = false

				onExpr = &core.MergeJoiner.OnExpr
				switch joinType {
				case sqlbase.JoinType_LEFT_SEMI, sqlbase.JoinType_LEFT_ANTI:
					onExprPlanning := makeFilterPlanningState(len(leftPhysTypes), len(rightPhysTypes))
					filterOnlyOnLeft, err = onExprPlanning.isFilterOnlyOnLeft(*onExpr)
					filterConstructor = func(op Operator) (Operator, error) {
						r := NewColOperatorResult{
							Op:          op,
							ColumnTypes: append(leftLogTypes, rightLogTypes...),
						}
						err := r.planFilterExpr(ctx, flowCtx.NewEvalCtx(), *onExpr, streamingMemAccount)
						return r.Op, err
					}
				}
			}
			if err != nil {
				return result, err
			}

			mergeJoinerMemAccount := streamingMemAccount
			if !result.IsStreaming && !useStreamingMemAccountForBuffering {
				// Whether the merge joiner is streaming is already set above.
				mergeJoinerMemAccount = result.createBufferingMemAccount(ctx, flowCtx, "merge-joiner")
			}
			result.Op, err = NewMergeJoinOp(
				NewAllocator(ctx, mergeJoinerMemAccount),
				core.MergeJoiner.Type,
				inputs[0],
				inputs[1],
				leftPhysTypes,
				rightPhysTypes,
				core.MergeJoiner.LeftOrdering.Columns,
				core.MergeJoiner.RightOrdering.Columns,
				filterConstructor,
				filterOnlyOnLeft,
			)
			if err != nil {
				return result, err
			}

			result.ColumnTypes = append(leftLogTypes, rightLogTypes...)

			if onExpr != nil && joinType == sqlbase.JoinType_INNER {
				// We will plan other Operators on top of the merge joiner, so we need
				// to account for the internal memory explicitly.
				if internalMemOp, ok := result.Op.(InternalMemoryOperator); ok {
					result.InternalMemUsage += internalMemOp.InternalMemoryUsage()
				}
				if err = result.planFilterExpr(
					ctx, flowCtx.NewEvalCtx(), *onExpr, streamingMemAccount,
				); err != nil {
					return result, err
				}
			}

		case core.Sorter != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			input := inputs[0]
			var (
				inputTypes           []coltypes.T
				sorterMemMonitorName string
				inMemorySorter       Operator
			)
			inputTypes, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
			if err != nil {
				return result, err
			}
			orderingCols := core.Sorter.OutputOrdering.Columns
			matchLen := core.Sorter.OrderingMatchLen
			if len(orderingCols) == int(matchLen) {
				// The input is already fully ordered, so there is nothing to sort.
				result.Op = input
			} else if matchLen > 0 {
				// The input is already partially ordered. Use a chunks sorter to avoid
				// loading all the rows into memory.
				sorterMemMonitorName = fmt.Sprintf("sort-chunks-%d", spec.ProcessorID)
				var sortChunksMemAccount *mon.BoundAccount
				if useStreamingMemAccountForBuffering {
					sortChunksMemAccount = streamingMemAccount
				} else {
					sortChunksMemAccount = result.createBufferingMemAccount(
						ctx, flowCtx, sorterMemMonitorName,
					)
				}
				inMemorySorter, err = NewSortChunks(
					NewAllocator(ctx, sortChunksMemAccount), input, inputTypes,
					orderingCols, int(matchLen),
				)
			} else if post.Limit != 0 && post.Filter.Empty() && post.Limit+post.Offset < math.MaxUint16 {
				// There is a limit specified with no post-process filter, so we know
				// exactly how many rows the sorter should output. Choose a top K sorter,
				// which uses a heap to avoid storing more rows than necessary.
				sorterMemMonitorName = fmt.Sprintf("topk-sort-%d", spec.ProcessorID)
				var topKSorterMemAccount *mon.BoundAccount
				if useStreamingMemAccountForBuffering {
					topKSorterMemAccount = streamingMemAccount
				} else {
					topKSorterMemAccount = result.createBufferingMemAccount(
						ctx, flowCtx, sorterMemMonitorName,
					)
				}
				k := uint16(post.Limit + post.Offset)
				inMemorySorter = NewTopKSorter(
					NewAllocator(ctx, topKSorterMemAccount), input, inputTypes,
					orderingCols, k,
				)
			} else {
				// No optimizations possible. Default to the standard sort operator.
				sorterMemMonitorName = fmt.Sprintf("sort-all-%d", spec.ProcessorID)
				var sorterMemAccount *mon.BoundAccount
				if useStreamingMemAccountForBuffering {
					sorterMemAccount = streamingMemAccount
				} else {
					sorterMemAccount = result.createBufferingMemAccount(
						ctx, flowCtx, sorterMemMonitorName,
					)
				}
				inMemorySorter, err = NewSorter(
					NewAllocator(ctx, sorterMemAccount), input, inputTypes, orderingCols,
				)
			}
			if err != nil {
				return result, err
			}
			if inMemorySorter != nil {
				// NOTE: when spilling to disk, we're using the same general external
				// sorter regardless of which sorter variant we have instantiated (i.e.
				// we don't take advantage of the limits and of partial ordering). We
				// could improve this.
				result.Op = newOneInputDiskSpiller(
					input, inMemorySorter.(bufferingInMemoryOperator),
					sorterMemMonitorName,
					func(input Operator) Operator {
						monitorNamePrefix := "external-sorter"
						// We are using an unlimited memory monitor here because external
						// sort itself is responsible for making sure that we stay within
						// the memory limit.
						unlimitedAllocator := NewAllocator(
							ctx, result.createBufferingUnlimitedMemAccount(
								ctx, flowCtx, monitorNamePrefix,
							))
						diskQueuesUnlimitedAllocator := NewAllocator(
							ctx, result.createBufferingUnlimitedMemAccount(
								ctx, flowCtx, monitorNamePrefix+"-disk-queues",
							))
						return newExternalSorter(
							unlimitedAllocator,
							input, inputTypes, core.Sorter.OutputOrdering,
							execinfra.GetWorkMemLimit(flowCtx.Cfg),
							diskQueuesUnlimitedAllocator,
						)
					},
					args.TestingKnobs.SpillingCallbackFn,
				)
			}
			result.ColumnTypes = spec.Input[0].ColumnTypes

		case core.Windower != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			wf := core.Windower.WindowFns[0]
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
				windowSortingPartitionerMemAccount := streamingMemAccount
				if !useStreamingMemAccountForBuffering {
					windowSortingPartitionerMemAccount = result.createBufferingMemAccount(ctx, flowCtx, "window-sorting-partitioner")
				}
				input, err = NewWindowSortingPartitioner(
					NewAllocator(ctx, windowSortingPartitionerMemAccount), input, typs,
					core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx),
				)
				tempPartitionColOffset, partitionColIdx = 1, int(wf.OutputColIdx)
			} else {
				if len(wf.Ordering.Columns) > 0 {
					windowSorterMemAccount := streamingMemAccount
					if !useStreamingMemAccountForBuffering {
						windowSorterMemAccount = result.createBufferingMemAccount(ctx, flowCtx, "window-sorter")
					}
					input, err = NewSorter(
						NewAllocator(ctx, windowSorterMemAccount), input, typs,
						wf.Ordering.Columns,
					)
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
				result.Op = NewRowNumberOperator(NewAllocator(ctx, streamingMemAccount), input, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
			case execinfrapb.WindowerSpec_RANK:
				result.Op, err = NewRankOperator(NewAllocator(ctx, streamingMemAccount), input, typs, false /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
			case execinfrapb.WindowerSpec_DENSE_RANK:
				result.Op, err = NewRankOperator(NewAllocator(ctx, streamingMemAccount), input, typs, true /* dense */, orderingCols, int(wf.OutputColIdx)+tempPartitionColOffset, partitionColIdx)
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
	}

	if err != nil {
		return result, err
	}

	// After constructing the base operator, calculate its internal memory usage.
	if sMem, ok := result.Op.(InternalMemoryOperator); ok {
		result.InternalMemUsage += sMem.InternalMemoryUsage()
	}
	log.VEventf(ctx, 1, "made op %T\n", result.Op)

	// Note: at this point, it is legal for ColumnTypes to be empty (it is
	// legal for empty rows to be passed between processors).

	if !post.Filter.Empty() {
		if err = result.planFilterExpr(
			ctx, flowCtx.NewEvalCtx(), post.Filter, streamingMemAccount,
		); err != nil {
			return result, err
		}
	}
	if post.Projection {
		result.addProjection(post.OutputColumns)
	} else if post.RenderExprs != nil {
		log.VEventf(ctx, 2, "planning render expressions %+v", post.RenderExprs)
		var renderedCols []uint32
		for _, expr := range post.RenderExprs {
			var (
				helper            execinfra.ExprHelper
				renderInternalMem int
			)
			err := helper.Init(expr, result.ColumnTypes, flowCtx.EvalCtx)
			if err != nil {
				return result, err
			}
			var outputIdx int
			result.Op, outputIdx, result.ColumnTypes, renderInternalMem, err = planProjectionOperators(
				ctx, flowCtx.NewEvalCtx(), helper.Expr, result.ColumnTypes, result.Op, streamingMemAccount,
			)
			if err != nil {
				return result, errors.Wrapf(err, "unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return result, errors.AssertionFailedf("missing outputIdx")
			}
			result.InternalMemUsage += renderInternalMem
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
}

func makeFilterPlanningState(numLeftInputCols, numRightInputCols int) filterPlanningState {
	return filterPlanningState{
		numLeftInputCols:  numLeftInputCols,
		numRightInputCols: numRightInputCols,
	}
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

// createBufferingUnlimitedMemMonitor instantiates an unlimited memory monitor.
// These should only be used when spilling to disk and an operator is made aware
// of a memory usage limit separately.
// The receiver is updated to have a reference to the unlimited memory monitor.
func (r *NewColOperatorResult) createBufferingUnlimitedMemMonitor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BytesMonitor {
	bufferingOpUnlimitedMemMonitor := execinfra.NewMonitor(
		ctx, flowCtx.EvalCtx.Mon, name+"-unlimited",
	)
	r.BufferingOpMemMonitors = append(r.BufferingOpMemMonitors, bufferingOpUnlimitedMemMonitor)
	return bufferingOpUnlimitedMemMonitor
}

// createBufferingMemAccount instantiates a memory monitor and a memory account
// to be used with a buffering Operator with the default memory limit. The
// receiver is updated to have references to both objects.
func (r *NewColOperatorResult) createBufferingMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	bufferingOpMemMonitor := execinfra.NewLimitedMonitor(
		ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, name+"-limited",
	)
	r.BufferingOpMemMonitors = append(r.BufferingOpMemMonitors, bufferingOpMemMonitor)
	bufferingMemAccount := bufferingOpMemMonitor.MakeBoundAccount()
	r.BufferingOpMemAccounts = append(r.BufferingOpMemAccounts, &bufferingMemAccount)
	return &bufferingMemAccount
}

// createBufferingMemAccountWithLimit is identical to createBufferingMemAccount
// although it creates a monitor with the provided limit, rather than using the
// default limit. Only disk-aware operators should use this method, as it is
// useful to enforce different limits in testing scenarios.
func (r *NewColOperatorResult) createBufferingMemAccountWithLimit(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string, limit int64,
) *mon.BoundAccount {
	limitedMon := mon.MakeMonitorInheritWithLimit(name+"-limited", limit, flowCtx.EvalCtx.Mon)
	r.BufferingOpMemMonitors = append(r.BufferingOpMemMonitors, &limitedMon)
	limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
	bufferingMemAccount := limitedMon.MakeBoundAccount()
	r.BufferingOpMemAccounts = append(r.BufferingOpMemAccounts, &bufferingMemAccount)
	return &bufferingMemAccount
}

// createBufferingUnlimitedMemAccount instantiates an unlimited memory monitor
// and a memory account to be used with a buffering disk-backed Operator. The
// receiver is updated to have references to both objects.
func (r *NewColOperatorResult) createBufferingUnlimitedMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	bufferingOpUnlimitedMemMonitor := r.createBufferingUnlimitedMemMonitor(ctx, flowCtx, name)
	bufferingMemAccount := bufferingOpUnlimitedMemMonitor.MakeBoundAccount()
	r.BufferingOpMemAccounts = append(r.BufferingOpMemAccounts, &bufferingMemAccount)
	return &bufferingMemAccount
}

func (r *NewColOperatorResult) planFilterExpr(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	filter execinfrapb.Expression,
	acc *mon.BoundAccount,
) error {
	var (
		helper               execinfra.ExprHelper
		selectionInternalMem int
	)
	err := helper.Init(filter, r.ColumnTypes, evalCtx)
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
	r.Op, _, filterColumnTypes, selectionInternalMem, err = planSelectionOperators(
		ctx, evalCtx, helper.Expr, r.ColumnTypes, r.Op, acc,
	)
	if err != nil {
		return errors.Wrapf(err, "unable to columnarize filter expression %q", filter.Expr)
	}
	r.InternalMemUsage += selectionInternalMem
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
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []types.T,
	input Operator,
	acc *mon.BoundAccount,
) (op Operator, resultIdx int, ct []types.T, internalMemUsed int, err error) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return NewBoolVecToSelOp(input, t.Idx), -1, columnTypes, internalMemUsed, nil
	case *tree.AndExpr:
		// AND expressions are handled by an implicit AND'ing of selection vectors.
		// First we select out the tuples that are true on the left side, and then,
		// only among the matched tuples, we select out the tuples that are true on
		// the right side.
		var leftOp, rightOp Operator
		var internalMemUsedLeft, internalMemUsedRight int
		leftOp, _, ct, internalMemUsedLeft, err = planSelectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		rightOp, resultIdx, ct, internalMemUsedRight, err = planSelectionOperators(
			ctx, evalCtx, t.TypedRight(), ct, leftOp, acc,
		)
		return rightOp, resultIdx, ct, internalMemUsedLeft + internalMemUsedRight, err
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
			return nil, resultIdx, ct, internalMemUsed, err
		}
		op, resultIdx, ct, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, caseExpr, columnTypes, input, acc,
		)
		op = NewBoolVecToSelOp(op, resultIdx)
		return op, resultIdx, ct, internalMemUsed, err
	case *tree.CaseExpr:
		op, resultIdx, ct, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc,
		)
		op = NewBoolVecToSelOp(op, resultIdx)
		return op, resultIdx, ct, internalMemUsed, err
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, internalMemUsedLeft, err := planProjectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		lTyp := &ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			if t.Operator == tree.Like || t.Operator == tree.NotLike {
				negate := t.Operator == tree.NotLike
				op, err = GetLikeOperator(
					evalCtx, leftOp, leftIdx, string(tree.MustBeDString(constArg)), negate)
				return op, resultIdx, ct, internalMemUsedLeft, err
			}
			if t.Operator == tree.In || t.Operator == tree.NotIn {
				negate := t.Operator == tree.NotIn
				datumTuple, ok := tree.AsDTuple(constArg)
				if !ok {
					err = errors.Errorf("IN is only supported for constant expressions")
					return nil, resultIdx, ct, internalMemUsed, err
				}
				op, err = GetInOperator(lTyp, leftOp, leftIdx, datumTuple, negate)
				return op, resultIdx, ct, internalMemUsedLeft, err
			}
			if t.Operator == tree.IsDistinctFrom || t.Operator == tree.IsNotDistinctFrom {
				if t.Right != tree.DNull {
					err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
					return nil, resultIdx, ct, internalMemUsed, err
				}
				// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we want to
				// negate when IS DISTINCT FROM is used.
				negate := t.Operator == tree.IsDistinctFrom
				op = newIsNullSelOp(leftOp, leftIdx, negate)
				return op, resultIdx, ct, internalMemUsedLeft, err
			}
			op, err := GetSelectionConstOperator(lTyp, t.TypedRight().ResolvedType(), cmpOp, leftOp, leftIdx, constArg)
			return op, resultIdx, ct, internalMemUsedLeft, err
		}
		rightOp, rightIdx, ct, internalMemUsedRight, err := planProjectionOperators(
			ctx, evalCtx, t.TypedRight(), ct, leftOp, acc,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		op, err := GetSelectionOperator(lTyp, &ct[rightIdx], cmpOp, rightOp, leftIdx, rightIdx)
		return op, resultIdx, ct, internalMemUsedLeft + internalMemUsedRight, err
	default:
		return nil, resultIdx, nil, internalMemUsed, errors.Errorf("unhandled selection expression type: %s", reflect.TypeOf(t))
	}
}

// planTypedMaybeNullProjectionOperators is used to plan projection operators, but is able to
// plan constNullOperators in the case that we know the "type" of the null. It is currently
// unsafe to plan a constNullOperator when we don't know the type of the null.
func planTypedMaybeNullProjectionOperators(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	exprTyp *types.T,
	columnTypes []types.T,
	input Operator,
	acc *mon.BoundAccount,
) (op Operator, resultIdx int, ct []types.T, internalMemUsed int, err error) {
	if expr == tree.DNull {
		resultIdx = len(columnTypes)
		op = NewConstNullOp(NewAllocator(ctx, acc), input, resultIdx, typeconv.FromColumnType(exprTyp))
		ct = append(columnTypes, *exprTyp)
		return op, resultIdx, ct, internalMemUsed, nil
	}
	return planProjectionOperators(ctx, evalCtx, expr, columnTypes, input, acc)
}

// planCastOperator plans a CAST operator that casts the column at index
// 'inputIdx' coming from input of type 'fromType' into a column of type
// 'toType' that will be output at index 'resultIdx'.
func planCastOperator(
	ctx context.Context,
	acc *mon.BoundAccount,
	columnTypes []types.T,
	input Operator,
	inputIdx int,
	fromType *types.T,
	toType *types.T,
) (op Operator, resultIdx int, ct []types.T, err error) {
	outputIdx := len(columnTypes)
	op, err = GetCastOperator(NewAllocator(ctx, acc), input, inputIdx, outputIdx, fromType, toType)
	ct = append(columnTypes, *toType)
	return op, outputIdx, ct, err
}

// planProjectionOperators plans a chain of operators to execute the provided
// expression. It returns the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planProjectionOperators(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []types.T,
	input Operator,
	acc *mon.BoundAccount,
) (op Operator, resultIdx int, ct []types.T, internalMemUsed int, err error) {
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, internalMemUsed, nil
	case *tree.ComparisonExpr:
		return planProjectionExpr(ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(), columnTypes, input, acc)
	case *tree.BinaryExpr:
		return planProjectionExpr(ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(), columnTypes, input, acc)
	case *tree.CastExpr:
		expr := t.Expr.(tree.TypedExpr)
		// If the expression is NULL, we use planTypedMaybeNullProjectionOperators instead of planProjectionOperators
		// because we can say that the type of the NULL is the type that we are casting to, rather than unknown.
		// We can't use planProjectionOperators because it will reject planning a constNullOp without knowing
		// the post typechecking "type" of the NULL.
		if expr.ResolvedType() == types.Unknown {
			op, resultIdx, ct, internalMemUsed, err = planTypedMaybeNullProjectionOperators(ctx, evalCtx, expr, t.Type, columnTypes, input, acc)
		} else {
			op, resultIdx, ct, internalMemUsed, err = planProjectionOperators(ctx, evalCtx, expr, columnTypes, input, acc)
		}
		if err != nil {
			return nil, 0, nil, internalMemUsed, err
		}
		op, resultIdx, ct, err = planCastOperator(ctx, acc, ct, op, resultIdx, expr.ResolvedType(), t.Type)
		return op, resultIdx, ct, internalMemUsed, err
	case *tree.FuncExpr:
		var (
			inputCols             []int
			projectionInternalMem int
		)
		ct = columnTypes
		op = input
		for _, e := range t.Exprs {
			var err error
			// TODO(rohany): This could be done better, especially in the case of
			// constant arguments, because the vectorized engine right now
			// creates a new column full of the constant value.
			op, resultIdx, ct, projectionInternalMem, err = planProjectionOperators(
				ctx, evalCtx, e.(tree.TypedExpr), ct, op, acc,
			)
			if err != nil {
				return nil, resultIdx, nil, internalMemUsed, err
			}
			inputCols = append(inputCols, resultIdx)
			internalMemUsed += projectionInternalMem
		}
		funcOutputType := t.ResolvedType()
		resultIdx = len(ct)
		ct = append(ct, *funcOutputType)
		op, err = NewBuiltinFunctionOperator(NewAllocator(ctx, acc), evalCtx, t, ct, inputCols, resultIdx, op)
		return op, resultIdx, ct, internalMemUsed, err
	case tree.Datum:
		datumType := t.ResolvedType()
		ct = columnTypes
		resultIdx = len(ct)
		ct = append(ct, *datumType)
		if datumType.Family() == types.UnknownFamily {
			return nil, resultIdx, ct, internalMemUsed, errors.New("cannot plan null type unknown")
		}
		typ := typeconv.FromColumnType(datumType)
		constVal, err := typeconv.GetDatumToPhysicalFn(datumType)(t)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		op, err := NewConstOp(NewAllocator(ctx, acc), input, typ, constVal, resultIdx)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		return op, resultIdx, ct, internalMemUsed, nil
	case *tree.CaseExpr:
		if t.Expr != nil {
			return nil, resultIdx, ct, internalMemUsed, errors.New("CASE <expr> WHEN expressions unsupported")
		}

		buffer := NewBufferOp(input)
		caseOps := make([]Operator, len(t.Whens))
		caseOutputType := typeconv.FromColumnType(t.ResolvedType())
		switch caseOutputType {
		case coltypes.Bytes:
			// Currently, there is a contradiction between the way CASE operator
			// works (which populates its output in arbitrary order) and the flat
			// bytes implementation of Bytes type (which prohibits sets in arbitrary
			// order), so we reject such scenario to fall back to row-by-row engine.
			return nil, resultIdx, ct, internalMemUsed, errors.Newf(
				"unsupported type %s in CASE operator", t.ResolvedType().String())
		case coltypes.Unhandled:
			return nil, resultIdx, ct, internalMemUsed, errors.Newf(
				"unsupported type %s", t.ResolvedType().String())
		}
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
			whenTyped := when.Cond.(tree.TypedExpr)
			whenResolvedType := whenTyped.ResolvedType()
			whenColType := typeconv.FromColumnType(whenResolvedType)
			if whenColType == coltypes.Unhandled {
				return nil, resultIdx, ct, internalMemUsed, errors.Newf(
					"unsupported type %s in CASE WHEN expression", whenResolvedType.String())
			}
			var whenInternalMemUsed, thenInternalMemUsed int
			caseOps[i], resultIdx, ct, whenInternalMemUsed, err = planTypedMaybeNullProjectionOperators(
				ctx, evalCtx, whenTyped, whenResolvedType, ct, buffer, acc,
			)
			if err != nil {
				return nil, resultIdx, ct, internalMemUsed, err
			}
			// Transform the booleans to a selection vector.
			caseOps[i] = NewBoolVecToSelOp(caseOps[i], resultIdx)

			// Run the "then" clause on those tuples that were selected.
			caseOps[i], thenIdxs[i], ct, thenInternalMemUsed, err = planTypedMaybeNullProjectionOperators(
				ctx, evalCtx, when.Val.(tree.TypedExpr), t.ResolvedType(), ct, caseOps[i], acc,
			)
			if err != nil {
				return nil, resultIdx, ct, internalMemUsed, err
			}
			internalMemUsed += whenInternalMemUsed + thenInternalMemUsed
			if !ct[thenIdxs[i]].Equal(ct[caseOutputIdx]) {
				// It is possible that the projection of this THEN arm has different
				// column type (for example, we expect INT2, but INT8 is given). In
				// such case, we need to plan a cast.
				fromType, toType := &ct[thenIdxs[i]], &ct[caseOutputIdx]
				caseOps[i], thenIdxs[i], ct, err = planCastOperator(
					ctx, acc, ct, caseOps[i], thenIdxs[i], fromType, toType,
				)
				if err != nil {
					return nil, resultIdx, ct, internalMemUsed, err
				}
			}
		}
		var elseInternalMemUsed int
		var elseOp Operator
		elseExpr := t.Else
		if elseExpr == nil {
			// If there's no ELSE arm, we write NULLs.
			elseExpr = tree.DNull
		}
		elseOp, thenIdxs[len(t.Whens)], ct, elseInternalMemUsed, err = planTypedMaybeNullProjectionOperators(
			ctx, evalCtx, elseExpr.(tree.TypedExpr), t.ResolvedType(), ct, buffer, acc,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		internalMemUsed += elseInternalMemUsed
		if !ct[thenIdxs[len(t.Whens)]].Equal(ct[caseOutputIdx]) {
			// It is possible that the projection of the ELSE arm has different
			// column type (for example, we expect INT2, but INT8 is given). In
			// such case, we need to plan a cast.
			elseIdx := thenIdxs[len(t.Whens)]
			fromType, toType := &ct[elseIdx], &ct[caseOutputIdx]
			elseOp, thenIdxs[len(t.Whens)], ct, err = planCastOperator(
				ctx, acc, ct, elseOp, elseIdx, fromType, toType,
			)
			if err != nil {
				return nil, resultIdx, ct, internalMemUsed, err
			}
		}

		op := NewCaseOp(NewAllocator(ctx, acc), buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)
		internalMemUsed += op.(InternalMemoryOperator).InternalMemoryUsage()
		return op, caseOutputIdx, ct, internalMemUsed, err
	case *tree.AndExpr, *tree.OrExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, acc)
	default:
		return nil, resultIdx, nil, internalMemUsed, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func planProjectionExpr(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	binOp tree.Operator,
	outputType *types.T,
	left, right tree.TypedExpr,
	columnTypes []types.T,
	input Operator,
	acc *mon.BoundAccount,
) (op Operator, resultIdx int, ct []types.T, internalMemUsed int, err error) {
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
		rightOp, rightIdx, ct, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, right, columnTypes, input, acc,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		resultIdx = len(ct)
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = GetProjectionLConstOperator(
			NewAllocator(ctx, acc), left.ResolvedType(), &ct[rightIdx], binOp,
			rightOp, rightIdx, lConstArg, resultIdx,
		)
		ct = append(ct, *outputType)
		if sMem, ok := op.(InternalMemoryOperator); ok {
			internalMemUsed += sMem.InternalMemoryUsage()
		}
		return op, resultIdx, ct, internalMemUsed, err
	}
	leftOp, leftIdx, ct, internalMemUsedLeft, err := planProjectionOperators(
		ctx, evalCtx, left, columnTypes, input, acc,
	)
	if err != nil {
		return nil, resultIdx, ct, internalMemUsed, err
	}
	internalMemUsed += internalMemUsedLeft
	if rConstArg, rConst := right.(tree.Datum); rConst {
		// Case 2: The right is constant.
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		resultIdx = len(ct)
		if binOp == tree.Like || binOp == tree.NotLike {
			negate := binOp == tree.NotLike
			op, err = GetLikeProjectionOperator(
				NewAllocator(ctx, acc), evalCtx, leftOp, leftIdx, resultIdx,
				string(tree.MustBeDString(rConstArg)), negate,
			)
		} else if binOp == tree.In || binOp == tree.NotIn {
			negate := binOp == tree.NotIn
			datumTuple, ok := tree.AsDTuple(rConstArg)
			if !ok {
				err = errors.Errorf("IN operator supported only on constant expressions")
				return nil, resultIdx, ct, internalMemUsed, err
			}
			op, err = GetInProjectionOperator(
				NewAllocator(ctx, acc), &ct[leftIdx], leftOp, leftIdx,
				resultIdx, datumTuple, negate,
			)
		} else if binOp == tree.IsDistinctFrom || binOp == tree.IsNotDistinctFrom {
			if right != tree.DNull {
				err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
				return nil, resultIdx, ct, internalMemUsed, err
			}
			// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we want to
			// negate when IS DISTINCT FROM is used.
			negate := binOp == tree.IsDistinctFrom
			op = newIsNullProjOp(NewAllocator(ctx, acc), leftOp, leftIdx, resultIdx, negate)
		} else {
			op, err = GetProjectionRConstOperator(
				NewAllocator(ctx, acc), &ct[leftIdx], right.ResolvedType(), binOp,
				leftOp, leftIdx, rConstArg, resultIdx,
			)
		}
		ct = append(ct, *outputType)
		if sMem, ok := op.(InternalMemoryOperator); ok {
			internalMemUsed += sMem.InternalMemoryUsage()
		}
		return op, resultIdx, ct, internalMemUsed, err
	}
	// Case 3: neither are constant.
	rightOp, rightIdx, ct, internalMemUsedRight, err := planProjectionOperators(
		ctx, evalCtx, right, ct, leftOp, acc,
	)
	if err != nil {
		return nil, resultIdx, nil, internalMemUsed, err
	}
	internalMemUsed += internalMemUsedRight
	resultIdx = len(ct)
	op, err = GetProjectionOperator(
		NewAllocator(ctx, acc), &ct[leftIdx], &ct[rightIdx], binOp, rightOp,
		leftIdx, rightIdx, resultIdx,
	)
	ct = append(ct, *outputType)
	if sMem, ok := op.(InternalMemoryOperator); ok {
		internalMemUsed += sMem.InternalMemoryUsage()
	}
	return op, resultIdx, ct, internalMemUsed, err
}

// planLogicalProjectionOp plans all the needed operators for a projection of
// a logical operation (either AND or OR).
func planLogicalProjectionOp(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []types.T,
	input Operator,
	acc *mon.BoundAccount,
) (op Operator, resultIdx int, ct []types.T, internalMemUsed int, err error) {
	// Add a new boolean column that will store the result of the projection.
	resultIdx = len(columnTypes)
	ct = append(columnTypes, *types.Bool)
	var (
		typedLeft, typedRight                       tree.TypedExpr
		leftProjOpChain, rightProjOpChain, outputOp Operator
		leftIdx, rightIdx                           int
		internalMemUsedLeft, internalMemUsedRight   int
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
	leftProjOpChain, leftIdx, ct, internalMemUsedLeft, err = planTypedMaybeNullProjectionOperators(
		ctx, evalCtx, typedLeft, types.Bool, ct, &leftFeedOp, acc,
	)
	if err != nil {
		return nil, resultIdx, ct, internalMemUsed, err
	}
	rightProjOpChain, rightIdx, ct, internalMemUsedRight, err = planTypedMaybeNullProjectionOperators(
		ctx, evalCtx, typedRight, types.Bool, ct, &rightFeedOp, acc,
	)
	if err != nil {
		return nil, resultIdx, ct, internalMemUsed, err
	}
	switch expr.(type) {
	case *tree.AndExpr:
		outputOp = NewAndProjOp(
			NewAllocator(ctx, acc),
			input, leftProjOpChain, rightProjOpChain,
			&leftFeedOp, &rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	case *tree.OrExpr:
		outputOp = NewOrProjOp(
			NewAllocator(ctx, acc),
			input, leftProjOpChain, rightProjOpChain,
			&leftFeedOp, &rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	}
	return outputOp, resultIdx, ct, internalMemUsedLeft + internalMemUsedRight, nil
}
