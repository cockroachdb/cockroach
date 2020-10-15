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
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
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
				nil, /* toClose */
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
	DiskQueueCfg         colcontainer.DiskQueueCfg
	FDSemaphore          semaphore.Semaphore
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
		// NumForcedRepartitions specifies a number of "repartitions" that a
		// disk-backed operator should be forced to perform. "Repartition" can mean
		// different things depending on the operator (for example, for hash joiner
		// it is dividing original partition into multiple new partitions; for
		// sorter it is merging already created partitions into new one before
		// proceeding to the next partition from the input).
		NumForcedRepartitions int
		// DelegateFDAcquisitions should be observed by users of a
		// PartitionedDiskQueue. During normal operations, these should acquire the
		// maximum number of file descriptors they will use from FDSemaphore up
		// front. Setting this testing knob to true disables that behavior and
		// lets the PartitionedDiskQueue interact with the semaphore as partitions
		// are opened/closed, which ensures that the number of open files never
		// exceeds what is expected.
		DelegateFDAcquisitions bool
	}
}

// NewColOperatorResult is a helper struct that encompasses all of the return
// values of NewColOperator call.
type NewColOperatorResult struct {
	Op               Operator
	ColumnTypes      []types.T
	InternalMemUsage int
	MetadataSources  []execinfrapb.MetadataSource
	// ToClose is a slice of components that need to be Closed. Close should be
	// idempotent.
	ToClose     []IdempotentCloser
	IsStreaming bool
	OpMonitors  []*mon.BytesMonitor
	OpAccounts  []*mon.BoundAccount
}

// resetToState resets r to the state specified in arg. arg may be a shallow
// copy made at a given point in time.
func (r *NewColOperatorResult) resetToState(ctx context.Context, arg NewColOperatorResult) {
	// MetadataSources are left untouched since there is no need to do any
	// cleaning there.

	// Close BoundAccounts that are not present in arg.OpAccounts.
	accs := make(map[*mon.BoundAccount]struct{})
	for _, a := range arg.OpAccounts {
		accs[a] = struct{}{}
	}
	for _, a := range r.OpAccounts {
		if _, ok := accs[a]; !ok {
			a.Close(ctx)
		}
	}
	// Stop BytesMonitors that are not present in arg.OpMonitors.
	mons := make(map[*mon.BytesMonitor]struct{})
	for _, m := range arg.OpMonitors {
		mons[m] = struct{}{}
	}

	for _, m := range r.OpMonitors {
		if _, ok := mons[m]; !ok {
			m.Stop(ctx)
		}
	}

	// Shallow copy over the rest.
	*r = arg
}

const noFilterIdx = -1

// isSupported checks whether we have a columnar operator equivalent to a
// processor described by spec. Note that it doesn't perform any other checks
// (like validity of the number of inputs).
func isSupported(
	allocator *Allocator, mode sessiondata.VectorizeExecMode, spec *execinfrapb.ProcessorSpec,
) (bool, error) {
	core := spec.Core
	isFullVectorization := mode == sessiondata.VectorizeOn ||
		mode == sessiondata.VectorizeExperimentalAlways

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
			inputTypes := make([]types.T, len(agg.ColIdx))
			for pos, colIdx := range agg.ColIdx {
				inputTypes[pos] = spec.Input[0].ColumnTypes[colIdx]
			}
			if supported, err := isAggregateSupported(allocator, agg.Func, inputTypes); !supported {
				return false, err
			}
		}
		return true, nil

	case core.Distinct != nil:
		if core.Distinct.NullsAreDistinct {
			return false, errors.Newf("distinct with unique nulls not supported")
		}
		if core.Distinct.ErrorOnDup != "" {
			return false, errors.Newf("distinct with error on duplicates not supported")
		}
		if !isFullVectorization {
			if len(core.Distinct.OrderedColumns) < len(core.Distinct.DistinctColumns) {
				return false, errors.Newf("unordered distinct can only run in vectorize 'on' mode")
			}
		}
		return true, nil

	case core.Ordinality != nil:
		return true, nil

	case core.HashJoiner != nil:
		if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type != sqlbase.InnerJoin {
			return false, errors.Newf("can't plan vectorized non-inner hash joins with ON expressions")
		}
		if core.HashJoiner.Type.IsSetOpJoin() {
			return false, errors.Newf("vectorized hash join of type %s is not supported", core.HashJoiner.Type)
		}
		leftInput, rightInput := spec.Input[0], spec.Input[1]
		if len(leftInput.ColumnTypes) == 0 || len(rightInput.ColumnTypes) == 0 {
			// We have a cross join of two inputs, and at least one of them has
			// zero-length schema. However, the hash join operators (both
			// external and in-memory) have a built-in assumption of non-empty
			// inputs, so we will fallback to row execution in such cases.
			// TODO(yuzefovich): implement specialized cross join operator.
			return false, errors.Newf("can't plan vectorized hash joins with an empty input schema")
		}
		return true, nil

	case core.MergeJoiner != nil:
		if !core.MergeJoiner.OnExpr.Empty() &&
			core.MergeJoiner.Type != sqlbase.JoinType_INNER {
			return false, errors.Errorf("can't plan non-inner merge join with ON expressions")
		}
		if core.MergeJoiner.Type.IsSetOpJoin() {
			return false, errors.Newf("vectorized merge join of type %s is not supported", core.MergeJoiner.Type)
		}
		return true, nil

	case core.Sorter != nil:
		return true, nil

	case core.Windower != nil:
		for _, wf := range core.Windower.WindowFns {
			if wf.Frame != nil {
				frame, err := wf.Frame.ConvertToAST()
				if err != nil {
					return false, err
				}
				if !frame.IsDefaultFrame() {
					return false, errors.Newf("window functions with non-default window frames are not supported")
				}
			}
			if wf.FilterColIdx != noFilterIdx {
				return false, errors.Newf("window functions with FILTER clause are not supported")
			}
			if wf.Func.AggregateFunc != nil {
				return false, errors.Newf("aggregate functions used as window functions are not supported")
			}

			if _, supported := SupportedWindowFns[*wf.Func.WindowFunc]; !supported {
				return false, errors.Newf("window function %s is not supported", wf.String())
			}
			if !isFullVectorization {
				switch *wf.Func.WindowFunc {
				case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
					return false, errors.Newf("window function %s can only run in vectorize 'on' mode", wf.String())
				}
			}
		}
		return true, nil

	default:
		return false, errors.Newf("unsupported processor core %q", core)
	}
}

// createDiskBackedSort creates a new disk-backed operator that sorts the input
// according to ordering.
// - matchLen specifies the length of the prefix of ordering columns the input
// is already ordered on.
// - maxNumberPartitions (when non-zero) overrides the semi-dynamically
// computed maximum number of partitions that the external sorter will have
// at once.
// - processorID is the ProcessorID of the processor core that requested
// creation of this operator. It is used only to distinguish memory monitors.
// - post describes the post-processing spec of the processor. It will be used
// to determine whether top K sort can be planned. If you want the general sort
// operator, then pass in empty struct.
func (r *NewColOperatorResult) createDiskBackedSort(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args NewColOperatorArgs,
	input Operator,
	inputTypes []coltypes.T,
	ordering execinfrapb.Ordering,
	matchLen uint32,
	maxNumberPartitions int,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
	memMonitorNamePrefix string,
) (Operator, error) {
	streamingMemAccount := args.StreamingMemAccount
	useStreamingMemAccountForBuffering := args.TestingKnobs.UseStreamingMemAccountForBuffering
	var (
		sorterMemMonitorName string
		inMemorySorter       Operator
		err                  error
	)
	if len(ordering.Columns) == int(matchLen) {
		// The input is already fully ordered, so there is nothing to sort.
		return input, nil
	}
	if matchLen > 0 {
		// The input is already partially ordered. Use a chunks sorter to avoid
		// loading all the rows into memory.
		sorterMemMonitorName = fmt.Sprintf("%ssort-chunks-%d", memMonitorNamePrefix, processorID)
		var sortChunksMemAccount *mon.BoundAccount
		if useStreamingMemAccountForBuffering {
			sortChunksMemAccount = streamingMemAccount
		} else {
			sortChunksMemAccount = r.createMemAccountForSpillStrategy(
				ctx, flowCtx, sorterMemMonitorName,
			)
		}
		inMemorySorter, err = NewSortChunks(
			NewAllocator(ctx, sortChunksMemAccount), input, inputTypes,
			ordering.Columns, int(matchLen),
		)
	} else if post.Limit != 0 && post.Filter.Empty() && post.Limit+post.Offset < math.MaxUint16 {
		// There is a limit specified with no post-process filter, so we know
		// exactly how many rows the sorter should output. Choose a top K sorter,
		// which uses a heap to avoid storing more rows than necessary.
		sorterMemMonitorName = fmt.Sprintf("%stopk-sort-%d", memMonitorNamePrefix, processorID)
		var topKSorterMemAccount *mon.BoundAccount
		if useStreamingMemAccountForBuffering {
			topKSorterMemAccount = streamingMemAccount
		} else {
			topKSorterMemAccount = r.createMemAccountForSpillStrategy(
				ctx, flowCtx, sorterMemMonitorName,
			)
		}
		k := uint16(post.Limit + post.Offset)
		inMemorySorter = NewTopKSorter(
			NewAllocator(ctx, topKSorterMemAccount), input, inputTypes,
			ordering.Columns, k,
		)
	} else {
		// No optimizations possible. Default to the standard sort operator.
		sorterMemMonitorName = fmt.Sprintf("%ssort-all-%d", memMonitorNamePrefix, processorID)
		var sorterMemAccount *mon.BoundAccount
		if useStreamingMemAccountForBuffering {
			sorterMemAccount = streamingMemAccount
		} else {
			sorterMemAccount = r.createMemAccountForSpillStrategy(
				ctx, flowCtx, sorterMemMonitorName,
			)
		}
		inMemorySorter, err = NewSorter(
			NewAllocator(ctx, sorterMemAccount), input, inputTypes, ordering.Columns,
		)
	}
	if err != nil {
		return nil, err
	}
	if inMemorySorter == nil {
		return nil, errors.AssertionFailedf("unexpectedly inMemorySorter is nil")
	}
	// NOTE: when spilling to disk, we're using the same general external
	// sorter regardless of which sorter variant we have instantiated (i.e.
	// we don't take advantage of the limits and of partial ordering). We
	// could improve this.
	return newOneInputDiskSpiller(
		input, inMemorySorter.(bufferingInMemoryOperator),
		sorterMemMonitorName,
		func(input Operator) Operator {
			monitorNamePrefix := fmt.Sprintf("%sexternal-sorter", memMonitorNamePrefix)
			// We are using an unlimited memory monitor here because external
			// sort itself is responsible for making sure that we stay within
			// the memory limit.
			unlimitedAllocator := NewAllocator(
				ctx, r.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorNamePrefix,
				))
			standaloneMemAccount := r.createStandaloneMemAccount(
				ctx, flowCtx, monitorNamePrefix,
			)
			diskAccount := r.createDiskAccount(ctx, flowCtx, monitorNamePrefix)
			// Make a copy of the DiskQueueCfg and set defaults for the sorter.
			// The cache mode is chosen to reuse the cache to have a smaller
			// cache per partition without affecting performance.
			diskQueueCfg := args.DiskQueueCfg
			diskQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeReuseCache
			diskQueueCfg.SetDefaultBufferSizeBytesForCacheMode()
			if args.TestingKnobs.NumForcedRepartitions != 0 {
				maxNumberPartitions = args.TestingKnobs.NumForcedRepartitions
			}
			es := newExternalSorter(
				ctx,
				unlimitedAllocator,
				standaloneMemAccount,
				input, inputTypes, ordering,
				execinfra.GetWorkMemLimit(flowCtx.Cfg),
				maxNumberPartitions,
				args.TestingKnobs.DelegateFDAcquisitions,
				diskQueueCfg,
				args.FDSemaphore,
				diskAccount,
			)
			r.ToClose = append(r.ToClose, es.(IdempotentCloser))
			return es
		},
		args.TestingKnobs.SpillingCallbackFn,
	), nil
}

// createAndWrapRowSource takes a processor spec, creating the row source and
// wrapping it using wrapRowSources. Note that the post process spec is included
// in the processor creation, so make sure to clear it if it will be inspected
// again. NewColOperatorResult is updated with the new OutputTypes and the
// resulting Columnarizer if there is no error. The result is also annotated as
// streaming because the resulting operator is not a buffering operator (even if
// it is a buffering processor). This is not a problem for memory accounting
// because each processor does that on its own, so the used memory will be
// accounted for.
func (r *NewColOperatorResult) createAndWrapRowSource(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	inputs []Operator,
	inputTypes [][]types.T,
	streamingMemAccount *mon.BoundAccount,
	spec *execinfrapb.ProcessorSpec,
	processorConstructor execinfra.ProcessorConstructor,
) error {
	if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.VectorizeAuto &&
		spec.Core.JoinReader == nil {
		return errors.New("rowexec processor wrapping for non-JoinReader core unsupported in vectorize=auto mode")
	}
	c, err := wrapRowSources(
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
				ctx, flowCtx, spec.ProcessorID, &spec.Core, &spec.Post, inputs,
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
					"processor %s is not an execinfra.RowSource", spec.Core.String(),
				)
			}
			r.ColumnTypes = rs.OutputTypes()
			return rs, nil
		},
	)
	if err != nil {
		return err
	}
	// We say that the wrapped processor is "streaming" because it is not a
	// buffering operator (even if it is a buffering processor). This is not a
	// problem for memory accounting because each processor does that on its
	// own, so the used memory will be accounted for.
	r.Op, r.IsStreaming = c, true
	r.MetadataSources = append(r.MetadataSources, c)
	return nil
}

// NOTE: throughout this file we do not append an output type of a projecting
// operator to the passed-in type schema - we, instead, always allocate a new
// type slice and copy over the old schema and set the output column of a
// projecting operator in the next slot. We attempt to enforce this by a linter
// rule, and such behavior prevents the type schema corruption scenario as
// described below.
//
// Without explicit new allocations, it is possible that planSelectionOperators
// (and other planning functions) reuse the same array for filterColumnTypes as
// result.ColumnTypes is using because there was enough capacity to do so.
// As an example, consider the following scenario in the context of
// planFilterExpr method:
// 1. r.ColumnTypes={*types.Bool} with len=1 and cap=4
// 2. planSelectionOperators adds another types.Int column, so
//    filterColumnTypes={*types.Bool, *types.Int} with len=2 and cap=4
//    Crucially, it uses exact same underlying array as r.ColumnTypes
//    uses.
// 3. we project out second column, so r.ColumnTypes={*types.Bool}
// 4. later, we add another *types.Float column, so
//    r.ColumnTypes={*types.Bool, *types.Float}, but there is enough
//    capacity in the array, so we simply overwrite the second slot
//    with the new type which corrupts filterColumnTypes to become
//    {*types.Bool, *types.Float}, and we can get into a runtime type
//    mismatch situation.

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
			for _, acc := range result.OpAccounts {
				acc.Close(ctx)
			}
			result.OpAccounts = result.OpAccounts[:0]
			for _, mon := range result.OpMonitors {
				mon.Stop(ctx)
			}
			result.OpMonitors = result.OpMonitors[:0]
		}
		if panicErr != nil {
			execerror.VectorizedInternalPanic(panicErr)
		}
	}()
	spec := args.Spec
	inputs := args.Inputs
	streamingMemAccount := args.StreamingMemAccount
	streamingAllocator := NewAllocator(ctx, streamingMemAccount)
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

	// resultPreSpecPlanningStateShallowCopy is a shallow copy of the result
	// before any specs are planned. Used if there is a need to backtrack.
	resultPreSpecPlanningStateShallowCopy := result

	supported, err := isSupported(streamingAllocator, flowCtx.EvalCtx.SessionData.VectorizeMode, spec)
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
		// We do not wrap Change{Aggregator,Frontier} because these processors
		// are very row-oriented and the Columnarizer might block indefinitely
		// while buffering coldata.BatchSize() tuples to emit as a single
		// batch.
		if core.ChangeAggregator != nil {
			return result, errors.Newf("core.ChangeAggregator is not supported")
		}
		if core.ChangeFrontier != nil {
			return result, errors.Newf("core.ChangeFrontier is not supported")
		}
		log.VEventf(ctx, 1, "planning a wrapped processor because %s", err.Error())

		inputTypes := make([][]types.T, len(spec.Input))
		for inputIdx, input := range spec.Input {
			inputTypes[inputIdx] = make([]types.T, len(input.ColumnTypes))
			copy(inputTypes[inputIdx], input.ColumnTypes)
		}

		err = result.createAndWrapRowSource(ctx, flowCtx, inputs, inputTypes, streamingMemAccount, spec, processorConstructor)
		// The wrapped processors need to be passed the post-process specs,
		// since they inspect them to figure out information about needed
		// columns. This means that we'll let those processors do any renders
		// or filters, which isn't ideal. We could improve this.
		post = &execinfrapb.PostProcessSpec{}

	} else {
		switch {
		case core.Noop != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			result.Op, result.IsStreaming = NewNoop(inputs[0]), true
			result.ColumnTypes = make([]types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
		case core.TableReader != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return result, err
			}
			var scanOp *colBatchScan
			scanOp, err = newColBatchScan(streamingAllocator, flowCtx, core.TableReader, post)
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
				result.Op, result.IsStreaming, err = NewSingleTupleNoInputOp(streamingAllocator), true, nil
				// We make ColumnTypes non-nil so that sanity check doesn't panic.
				result.ColumnTypes = []types.T{}
				break
			}
			if aggSpec.IsRowCount() {
				result.Op, result.IsStreaming, err = NewCountOp(streamingAllocator, inputs[0]), true, nil
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
					// Create an unlimited mem account explicitly even though there is no
					// disk spilling because the memory usage of an aggregator is
					// proportional to the number of groups, not the number of inputs.
					// The row execution engine also gives an unlimited (that still
					// needs to be approved by the upstream monitor, so not really
					// "unlimited") amount of memory to the aggregator.
					hashAggregatorMemAccount = result.createBufferingUnlimitedMemAccount(ctx, flowCtx, "hash-aggregator")
				}
				result.Op, err = NewHashAggregator(
					NewAllocator(ctx, hashAggregatorMemAccount), inputs[0], typs, aggFns,
					aggSpec.GroupCols, aggCols,
				)
			} else {
				result.Op, err = NewOrderedAggregator(
					streamingAllocator, inputs[0], typs, aggFns,
					aggSpec.GroupCols, aggCols, aggSpec.IsScalar(),
				)
				result.IsStreaming = true
			}

		case core.Distinct != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			result.ColumnTypes = make([]types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			var typs []coltypes.T
			typs, err = typeconv.FromColumnTypes(result.ColumnTypes)
			if err != nil {
				return result, err
			}
			if len(core.Distinct.OrderedColumns) == len(core.Distinct.DistinctColumns) {
				result.Op, err = NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, typs)
				result.IsStreaming = true
			} else {
				distinctMemAccount := streamingMemAccount
				if !useStreamingMemAccountForBuffering {
					// Create an unlimited mem account explicitly even though there is no
					// disk spilling because the memory usage of an unordered distinct
					// operator is proportional to the number of distinct tuples, not the
					// number of input tuples.
					// The row execution engine also gives an unlimited amount (that still
					// needs to be approved by the upstream monitor, so not really
					// "unlimited") amount of memory to the unordered distinct operator.
					distinctMemAccount = result.createBufferingUnlimitedMemAccount(ctx, flowCtx, "distinct")
				}
				// TODO(yuzefovich): we have an implementation of partially ordered
				// distinct, and we should plan it when we have non-empty ordered
				// columns and we think that the probability of distinct tuples in the
				// input is about 0.01 or less.
				result.Op = NewUnorderedDistinct(
					NewAllocator(ctx, distinctMemAccount), inputs[0],
					core.Distinct.DistinctColumns, typs, hashTableNumBuckets,
				)
			}

		case core.Ordinality != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Op = NewOrdinalityOp(
				streamingAllocator, inputs[0], outputIdx,
			)
			result.IsStreaming = true
			result.ColumnTypes = make([]types.T, outputIdx+1)
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			result.ColumnTypes[outputIdx] = *types.Int

		case core.HashJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return result, err
			}
			leftLogTypes := make([]types.T, len(spec.Input[0].ColumnTypes))
			copy(leftLogTypes, spec.Input[0].ColumnTypes)
			leftPhysTypes, err := typeconv.FromColumnTypes(leftLogTypes)
			if err != nil {
				return result, err
			}
			rightLogTypes := make([]types.T, len(spec.Input[1].ColumnTypes))
			copy(rightLogTypes, spec.Input[1].ColumnTypes)
			rightPhysTypes, err := typeconv.FromColumnTypes(rightLogTypes)
			if err != nil {
				return result, err
			}

			hashJoinerMemMonitorName := fmt.Sprintf("hash-joiner-%d", spec.ProcessorID)
			var hashJoinerMemAccount *mon.BoundAccount
			if useStreamingMemAccountForBuffering {
				hashJoinerMemAccount = streamingMemAccount
			} else {
				hashJoinerMemAccount = result.createMemAccountForSpillStrategy(
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
				diskAccount := result.createDiskAccount(ctx, flowCtx, hashJoinerMemMonitorName)
				result.Op = newTwoInputDiskSpiller(
					inputs[0], inputs[1], inMemoryHashJoiner.(bufferingInMemoryOperator),
					hashJoinerMemMonitorName,
					func(inputOne, inputTwo Operator) Operator {
						monitorNamePrefix := "external-hash-joiner"
						unlimitedAllocator := NewAllocator(
							ctx, result.createBufferingUnlimitedMemAccount(
								ctx, flowCtx, monitorNamePrefix,
							))
						// Make a copy of the DiskQueueCfg and set defaults for the hash
						// joiner. The cache mode is chosen to automatically close the cache
						// belonging to partitions at a parent level when repartitioning.
						diskQueueCfg := args.DiskQueueCfg
						diskQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
						diskQueueCfg.SetDefaultBufferSizeBytesForCacheMode()
						ehj := newExternalHashJoiner(
							unlimitedAllocator, hjSpec,
							inputOne, inputTwo,
							execinfra.GetWorkMemLimit(flowCtx.Cfg),
							diskQueueCfg,
							args.FDSemaphore,
							func(input Operator, inputTypes []coltypes.T, orderingCols []execinfrapb.Ordering_Column, maxNumberPartitions int) (Operator, error) {
								sortArgs := args
								if !args.TestingKnobs.DelegateFDAcquisitions {
									// Set the FDSemaphore to nil. This indicates that no FDs
									// should be acquired. The external hash joiner will do this
									// up front.
									sortArgs.FDSemaphore = nil
								}
								return result.createDiskBackedSort(
									ctx, flowCtx, sortArgs, input, inputTypes,
									execinfrapb.Ordering{Columns: orderingCols},
									0 /* matchLen */, maxNumberPartitions, spec.ProcessorID,
									&execinfrapb.PostProcessSpec{}, monitorNamePrefix+"-")
							},
							args.TestingKnobs.NumForcedRepartitions,
							args.TestingKnobs.DelegateFDAcquisitions,
							diskAccount,
						)
						result.ToClose = append(result.ToClose, ehj.(IdempotentCloser))
						return ehj
					},
					args.TestingKnobs.SpillingCallbackFn,
				)
			}
			result.ColumnTypes = make([]types.T, len(leftLogTypes)+len(rightLogTypes))
			copy(result.ColumnTypes, leftLogTypes)
			if core.HashJoiner.Type == sqlbase.JoinType_LEFT_SEMI ||
				core.HashJoiner.Type == sqlbase.JoinType_LEFT_ANTI {
				result.ColumnTypes = result.ColumnTypes[:len(leftLogTypes):len(leftLogTypes)]
			} else {
				copy(result.ColumnTypes[len(leftLogTypes):], rightLogTypes)
			}

			if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type == sqlbase.JoinType_INNER {
				if err = result.planAndMaybeWrapOnExprAsFilter(ctx, flowCtx, core.HashJoiner.OnExpr, streamingMemAccount, processorConstructor); err != nil {
					return result, err
				}
			}

		case core.MergeJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return result, err
			}
			// Merge joiner is a streaming operator when equality columns form a key
			// for both of the inputs.
			result.IsStreaming = core.MergeJoiner.LeftEqColumnsAreKey && core.MergeJoiner.RightEqColumnsAreKey

			leftLogTypes := make([]types.T, len(spec.Input[0].ColumnTypes))
			copy(leftLogTypes, spec.Input[0].ColumnTypes)
			leftPhysTypes, err := typeconv.FromColumnTypes(leftLogTypes)
			if err != nil {
				return result, err
			}
			rightLogTypes := make([]types.T, len(spec.Input[1].ColumnTypes))
			copy(rightLogTypes, spec.Input[1].ColumnTypes)
			rightPhysTypes, err := typeconv.FromColumnTypes(rightLogTypes)
			if err != nil {
				return result, err
			}

			joinType := core.MergeJoiner.Type
			var onExpr *execinfrapb.Expression
			if !core.MergeJoiner.OnExpr.Empty() {
				if joinType != sqlbase.JoinType_INNER {
					return result, errors.AssertionFailedf(
						"ON expression (%s) was unexpectedly planned for merge joiner with join type %s",
						core.MergeJoiner.OnExpr.String(), core.MergeJoiner.Type.String(),
					)
				}
				onExpr = &core.MergeJoiner.OnExpr
			}

			monitorName := "merge-joiner"
			// We are using an unlimited memory monitor here because merge joiner
			// itself is responsible for making sure that we stay within the memory
			// limit, and it will fall back to disk if necessary.
			unlimitedAllocator := NewAllocator(
				ctx, result.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorName,
				))
			diskAccount := result.createDiskAccount(ctx, flowCtx, monitorName)
			mj, err := newMergeJoinOp(
				unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx.Cfg),
				args.DiskQueueCfg, args.FDSemaphore,
				joinType, inputs[0], inputs[1], leftPhysTypes, rightPhysTypes,
				core.MergeJoiner.LeftOrdering.Columns, core.MergeJoiner.RightOrdering.Columns,
				diskAccount,
			)
			if err != nil {
				return result, err
			}

			result.Op = mj
			result.ToClose = append(result.ToClose, mj.(IdempotentCloser))
			result.ColumnTypes = make([]types.T, len(leftLogTypes)+len(rightLogTypes))
			copy(result.ColumnTypes, leftLogTypes)
			if core.MergeJoiner.Type == sqlbase.JoinType_LEFT_SEMI ||
				core.MergeJoiner.Type == sqlbase.JoinType_LEFT_ANTI {
				result.ColumnTypes = result.ColumnTypes[:len(leftLogTypes):len(leftLogTypes)]
			} else {
				copy(result.ColumnTypes[len(leftLogTypes):], rightLogTypes)
			}
			copy(result.ColumnTypes[len(leftLogTypes):], rightLogTypes)

			if onExpr != nil {
				if err = result.planAndMaybeWrapOnExprAsFilter(ctx, flowCtx, *onExpr, streamingMemAccount, processorConstructor); err != nil {
					return result, err
				}
			}

		case core.Sorter != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			input := inputs[0]
			result.ColumnTypes = make([]types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			var inputTypes []coltypes.T
			inputTypes, err = typeconv.FromColumnTypes(spec.Input[0].ColumnTypes)
			if err != nil {
				return result, err
			}
			ordering := core.Sorter.OutputOrdering
			matchLen := core.Sorter.OrderingMatchLen
			result.Op, err = result.createDiskBackedSort(
				ctx, flowCtx, args, input, inputTypes, ordering, matchLen, 0, /* maxNumberPartitions */
				spec.ProcessorID, post, "", /* memMonitorNamePrefix */
			)

		case core.Windower != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return result, err
			}
			memMonitorsPrefix := "window-"
			input := inputs[0]
			result.ColumnTypes = make([]types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			for _, wf := range core.Windower.WindowFns {
				var typs []coltypes.T
				typs, err = typeconv.FromColumnTypes(result.ColumnTypes)
				if err != nil {
					return result, err
				}
				tempColOffset, partitionColIdx := uint32(0), columnOmitted
				peersColIdx := columnOmitted
				windowFn := *wf.Func.WindowFunc
				if len(core.Windower.PartitionBy) > 0 {
					// TODO(yuzefovich): add support for hashing partitioner (probably by
					// leveraging hash routers once we can distribute). The decision about
					// which kind of partitioner to use should come from the optimizer.
					partitionColIdx = int(wf.OutputColIdx)
					input, err = NewWindowSortingPartitioner(
						streamingAllocator, input, typs,
						core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx),
						func(input Operator, inputTypes []coltypes.T, orderingCols []execinfrapb.Ordering_Column) (Operator, error) {
							return result.createDiskBackedSort(
								ctx, flowCtx, args, input, inputTypes,
								execinfrapb.Ordering{Columns: orderingCols}, 0, /* matchLen */
								0 /* maxNumberPartitions */, spec.ProcessorID,
								&execinfrapb.PostProcessSpec{}, memMonitorsPrefix)
						},
					)
					// Window partitioner will append a boolean column.
					tempColOffset++
					oldTyps := typs
					typs = make([]coltypes.T, len(oldTyps)+1)
					copy(typs, oldTyps)
					typs[len(oldTyps)] = coltypes.Bool
				} else {
					if len(wf.Ordering.Columns) > 0 {
						input, err = result.createDiskBackedSort(
							ctx, flowCtx, args, input, typs,
							wf.Ordering, 0 /* matchLen */, 0, /* maxNumberPartitions */
							spec.ProcessorID, &execinfrapb.PostProcessSpec{}, memMonitorsPrefix,
						)
					}
				}
				if err != nil {
					return result, err
				}
				if windowFnNeedsPeersInfo(*wf.Func.WindowFunc) {
					peersColIdx = int(wf.OutputColIdx + tempColOffset)
					input, err = NewWindowPeerGrouper(
						streamingAllocator,
						input, typs, wf.Ordering.Columns,
						partitionColIdx, peersColIdx,
					)
					// Window peer grouper will append a boolean column.
					tempColOffset++
					oldTyps := typs
					typs = make([]coltypes.T, len(oldTyps)+1)
					copy(typs, oldTyps)
					typs[len(oldTyps)] = coltypes.Bool
				}

				outputIdx := int(wf.OutputColIdx + tempColOffset)
				switch windowFn {
				case execinfrapb.WindowerSpec_ROW_NUMBER:
					result.Op = NewRowNumberOperator(
						streamingAllocator, input, outputIdx, partitionColIdx,
					)
				case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
					result.Op, err = NewRankOperator(
						streamingAllocator, input, windowFn,
						wf.Ordering.Columns, outputIdx, partitionColIdx, peersColIdx,
					)
				case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
					// We are using an unlimited memory monitor here because
					// relative rank operators themselves are responsible for
					// making sure that we stay within the memory limit, and
					// they will fall back to disk if necessary.
					memAccName := memMonitorsPrefix + "relative-rank"
					unlimitedAllocator := NewAllocator(
						ctx, result.createBufferingUnlimitedMemAccount(ctx, flowCtx, memAccName),
					)
					diskAcc := result.createDiskAccount(ctx, flowCtx, memAccName)
					result.Op, err = NewRelativeRankOperator(
						unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx.Cfg), args.DiskQueueCfg,
						args.FDSemaphore, input, typs, windowFn, wf.Ordering.Columns,
						outputIdx, partitionColIdx, peersColIdx, diskAcc,
					)
					// NewRelativeRankOperator sometimes returns a constOp when there
					// are no ordering columns, so we check that the returned operator
					// is an IdempotentCloser.
					if c, ok := result.Op.(IdempotentCloser); ok {
						result.ToClose = append(result.ToClose, c)
					}
				default:
					return result, errors.AssertionFailedf("window function %s is not supported", wf.String())
				}

				if tempColOffset > 0 {
					// We want to project out temporary columns (which have indices in the
					// range [wf.OutputColIdx, wf.OutputColIdx+tempColOffset)).
					projection := make([]uint32, 0, wf.OutputColIdx+tempColOffset)
					for i := uint32(0); i < wf.OutputColIdx; i++ {
						projection = append(projection, i)
					}
					projection = append(projection, wf.OutputColIdx+tempColOffset)
					result.Op = NewSimpleProjectOp(result.Op, int(wf.OutputColIdx+tempColOffset), projection)
				}

				_, returnType, err := execinfrapb.GetWindowFunctionInfo(wf.Func, []types.T{}...)
				if err != nil {
					return result, err
				}
				oldColumnTypes := result.ColumnTypes
				result.ColumnTypes = make([]types.T, len(oldColumnTypes)+1)
				copy(result.ColumnTypes, oldColumnTypes)
				result.ColumnTypes[len(oldColumnTypes)] = *returnType
				input = result.Op
			}

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

	ppr := postProcessResult{
		Op:          result.Op,
		ColumnTypes: result.ColumnTypes,
	}
	err = ppr.planPostProcessSpec(ctx, flowCtx, post, streamingMemAccount)
	if err != nil {
		log.VEventf(
			ctx, 2,
			"vectorized post process planning failed with error %v post spec is %s, attempting to wrap as a row source",
			err, post,
		)
		if core.TableReader != nil {
			// We cannot naively wrap a TableReader's post-processing spec since it
			// might project out unneeded columns that are of unsupported types. These
			// columns are still returned, either as coltypes.Unhandled if the type is
			// unsupported, or as an empty column of a supported type. If we were to
			// wrap an unsupported post-processing spec, a Materializer would naively
			// decode these columns, which would return errors (e.g. UUIDs require 16
			// bytes, coltypes.Unhandled may not be decoded).
			inputTypes := make([][]types.T, len(spec.Input))
			for inputIdx, input := range spec.Input {
				inputTypes[inputIdx] = make([]types.T, len(input.ColumnTypes))
				copy(inputTypes[inputIdx], input.ColumnTypes)
			}
			result.resetToState(ctx, resultPreSpecPlanningStateShallowCopy)
			err = result.createAndWrapRowSource(
				ctx, flowCtx, inputs, inputTypes, streamingMemAccount, spec, processorConstructor,
			)
			if err != nil {
				// There was an error wrapping the TableReader.
				return result, err
			}
		} else {
			err = result.wrapPostProcessSpec(ctx, flowCtx, post, streamingMemAccount, processorConstructor)
		}
	} else {
		// The result can be updated with the post process result.
		result.updateWithPostProcessResult(ppr)
	}
	return result, err
}

// planAndMaybeWrapOnExprAsFilter plans a joiner ON expression as a filter. If
// the filter is unsupported, it is planned as a wrapped noop processor with
// the filter as a post-processing stage.
func (r *NewColOperatorResult) planAndMaybeWrapOnExprAsFilter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	onExpr execinfrapb.Expression,
	streamingMemAccount *mon.BoundAccount,
	processorConstructor execinfra.ProcessorConstructor,
) error {
	// We will plan other Operators on top of r.Op, so we need to account for the
	// internal memory explicitly.
	if internalMemOp, ok := r.Op.(InternalMemoryOperator); ok {
		r.InternalMemUsage += internalMemOp.InternalMemoryUsage()
	}
	ppr := postProcessResult{
		Op:          r.Op,
		ColumnTypes: r.ColumnTypes,
	}
	if err := ppr.planFilterExpr(
		ctx, flowCtx.NewEvalCtx(), onExpr, streamingMemAccount,
	); err != nil {
		// ON expression planning failed. Fall back to planning the filter
		// using row execution.
		log.VEventf(
			ctx, 2,
			"vectorized join ON expr planning failed with error %v ON expr is %s, attempting to wrap as a row source",
			err, onExpr.String(),
		)

		onExprAsFilter := &execinfrapb.PostProcessSpec{Filter: onExpr}
		return r.wrapPostProcessSpec(ctx, flowCtx, onExprAsFilter, streamingMemAccount, processorConstructor)
	}
	r.updateWithPostProcessResult(ppr)
	return nil
}

// wrapPostProcessSpec plans the given post process spec by wrapping a noop
// processor with that output spec. This is used to fall back to row execution
// when encountering unsupported post processing specs. An error is returned
// if the wrapping failed. A reason for this could be an unsupported type, in
// which case the row execution engine is used fully.
func (r *NewColOperatorResult) wrapPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	post *execinfrapb.PostProcessSpec,
	streamingMemAccount *mon.BoundAccount,
	processorConstructor execinfra.ProcessorConstructor,
) error {
	noopSpec := &execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post: *post,
	}
	return r.createAndWrapRowSource(
		ctx, flowCtx, []Operator{r.Op}, [][]types.T{r.ColumnTypes}, streamingMemAccount, noopSpec, processorConstructor,
	)
}

// planPostProcessSpec plans the post processing stage specified in post on top
// of r.Op.
func (r *postProcessResult) planPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	post *execinfrapb.PostProcessSpec,
	streamingMemAccount *mon.BoundAccount,
) error {
	if !post.Filter.Empty() {
		if err := r.planFilterExpr(
			ctx, flowCtx.NewEvalCtx(), post.Filter, streamingMemAccount,
		); err != nil {
			return err
		}
	}

	if post.Projection {
		r.addProjection(post.OutputColumns)
	} else if post.RenderExprs != nil {
		log.VEventf(ctx, 2, "planning render expressions %+v", post.RenderExprs)
		var renderedCols []uint32
		for _, expr := range post.RenderExprs {
			var (
				helper            execinfra.ExprHelper
				renderInternalMem int
			)
			err := helper.Init(expr, r.ColumnTypes, flowCtx.EvalCtx)
			if err != nil {
				return err
			}
			var outputIdx int
			r.Op, outputIdx, r.ColumnTypes, renderInternalMem, err = planProjectionOperators(
				ctx, flowCtx.NewEvalCtx(), helper.Expr, r.ColumnTypes, r.Op, streamingMemAccount,
			)
			if err != nil {
				return errors.Wrapf(err, "unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return errors.AssertionFailedf("missing outputIdx")
			}
			r.InternalMemUsage += renderInternalMem
			renderedCols = append(renderedCols, uint32(outputIdx))
		}
		r.Op = NewSimpleProjectOp(r.Op, len(r.ColumnTypes), renderedCols)
		newTypes := make([]types.T, len(renderedCols))
		for i, j := range renderedCols {
			newTypes[i] = r.ColumnTypes[j]
		}
		r.ColumnTypes = newTypes
	}
	if post.Offset != 0 {
		r.Op = NewOffsetOp(r.Op, int(post.Offset))
	}
	if post.Limit != 0 {
		r.Op = NewLimitOp(r.Op, int(post.Limit))
	}
	return nil
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
	r.OpMonitors = append(r.OpMonitors, bufferingOpUnlimitedMemMonitor)
	return bufferingOpUnlimitedMemMonitor
}

// createMemAccountForSpillStrategy instantiates a memory monitor and a memory
// account to be used with a buffering Operator that can fall back to disk.
// The default memory limit is used, if flowCtx.Cfg.ForceDiskSpill is used, this
// will be 1. The receiver is updated to have references to both objects.
func (r *NewColOperatorResult) createMemAccountForSpillStrategy(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	bufferingOpMemMonitor := execinfra.NewLimitedMonitor(
		ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, name+"-limited",
	)
	r.OpMonitors = append(r.OpMonitors, bufferingOpMemMonitor)
	bufferingMemAccount := bufferingOpMemMonitor.MakeBoundAccount()
	r.OpAccounts = append(r.OpAccounts, &bufferingMemAccount)
	return &bufferingMemAccount
}

// createBufferingUnlimitedMemAccount instantiates an unlimited memory monitor
// and a memory account to be used with a buffering disk-backed Operator. The
// receiver is updated to have references to both objects. Note that the
// returned account is only "unlimited" in that it does not have a hard limit
// that it enforces, but a limit might be enforced by a root monitor.
func (r *NewColOperatorResult) createBufferingUnlimitedMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	bufferingOpUnlimitedMemMonitor := r.createBufferingUnlimitedMemMonitor(ctx, flowCtx, name)
	bufferingMemAccount := bufferingOpUnlimitedMemMonitor.MakeBoundAccount()
	r.OpAccounts = append(r.OpAccounts, &bufferingMemAccount)
	return &bufferingMemAccount
}

// createStandaloneMemAccount instantiates an unlimited memory monitor and a
// memory account that have a standalone budget. This means that the memory
// registered with these objects is *not* reported to the root monitor (i.e.
// it will not count towards max-sql-memory). Use it only when the memory in
// use is accounted for with a different memory monitor. The receiver is
// updated to have references to both objects.
func (r *NewColOperatorResult) createStandaloneMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	standaloneMemMonitor := mon.MakeMonitor(
		name+"-standalone",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
		flowCtx.Cfg.Settings,
	)
	r.OpMonitors = append(r.OpMonitors, &standaloneMemMonitor)
	standaloneMemMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	standaloneMemAccount := standaloneMemMonitor.MakeBoundAccount()
	r.OpAccounts = append(r.OpAccounts, &standaloneMemAccount)
	return &standaloneMemAccount
}

// createDiskAccount instantiates an unlimited disk monitor and a disk account
// to be used for disk spilling infrastructure in vectorized engine.
// TODO(azhng): consolidates all allocation monitors/account manage into one
// place after branch cut for 20.1.
func (r *NewColOperatorResult) createDiskAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	opDiskMonitor := execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, name)
	r.OpMonitors = append(r.OpMonitors, opDiskMonitor)
	opDiskAccount := opDiskMonitor.MakeBoundAccount()
	r.OpAccounts = append(r.OpAccounts, &opDiskAccount)
	return &opDiskAccount
}

type postProcessResult struct {
	Op               Operator
	ColumnTypes      []types.T
	InternalMemUsage int
}

func (r *NewColOperatorResult) updateWithPostProcessResult(ppr postProcessResult) {
	r.Op = ppr.Op
	r.ColumnTypes = make([]types.T, len(ppr.ColumnTypes))
	copy(r.ColumnTypes, ppr.ColumnTypes)
	r.InternalMemUsage += ppr.InternalMemUsage
}

func (r *postProcessResult) planFilterExpr(
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
func (r *postProcessResult) addProjection(projection []uint32) {
	r.Op = NewSimpleProjectOp(r.Op, len(r.ColumnTypes), projection)
	// Update output ColumnTypes.
	newTypes := make([]types.T, len(projection))
	for i, j := range projection {
		newTypes[i] = r.ColumnTypes[j]
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
		ct = make([]types.T, len(columnTypes)+1)
		copy(ct, columnTypes)
		ct[len(columnTypes)] = *exprTyp
		return op, resultIdx, ct, internalMemUsed, nil
	}
	return planProjectionOperators(ctx, evalCtx, expr, columnTypes, input, acc)
}

func checkCastSupported(fromType, toType *types.T) error {
	switch toType.Family() {
	case types.DecimalFamily:
		// If we're casting to a decimal, we're only allowing casting from the
		// decimal of the same precision due to the fact that we're losing
		// precision information once we start operating on coltypes.T. For
		// such casts we will fallback to row-by-row engine.
		if !fromType.Equal(*toType) {
			return errors.New("decimal casts with rounding unsupported")
		}
	}
	return nil
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
	if err := checkCastSupported(fromType, toType); err != nil {
		return op, resultIdx, ct, err
	}
	outputIdx := len(columnTypes)
	op, err = GetCastOperator(NewAllocator(ctx, acc), input, inputIdx, outputIdx, fromType, toType)
	ct = make([]types.T, len(columnTypes)+1)
	copy(ct, columnTypes)
	ct[len(columnTypes)] = *toType
	return op, outputIdx, ct, err
}

// toPhysTypesMaybeUnhandled converts logical types to their physical
// equivalents. If a logical type is not supported by the vectorized engine, it
// is converted into coltypes.Unhandled and no error occurs.
//
// It is the same as typeconv.FromColumnTypes but without an error, and this
// method was created for the sole purpose of supporting render expressions
// with projection operators that are planned after colBatchScan which can
// output batches with coltypes.Unhandled when those columns are not needed.
// For example, if we have a table with type schema (a INT, j JSON), but the
// query uses only column 'a', then colBatchScan will output batch with schema
// (coltypes.Int64, coltypes.Unhandled), and we want to be ok with that when
// enforcing the prefix of type schema of the batch.
//
// WARNING: use this with caution - only when the caller of this method is
// *not* responsible for making sure that we support all types in ct, when it
// should have been checked before.
func toPhysTypesMaybeUnhandled(ct []types.T) []coltypes.T {
	physTypesMaybeUnhandled := make([]coltypes.T, len(ct))
	for i, logType := range ct {
		physTypesMaybeUnhandled[i] = typeconv.FromColumnType(&logType)
	}
	return physTypesMaybeUnhandled
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
		ct = make([]types.T, len(columnTypes))
		copy(ct, columnTypes)
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
		oldTyps := ct
		ct = make([]types.T, len(oldTyps)+1)
		copy(ct, oldTyps)
		ct[len(oldTyps)] = *funcOutputType
		op, err = NewBuiltinFunctionOperator(
			NewAllocator(ctx, acc), evalCtx, t, ct, inputCols, resultIdx, op,
		)
		return op, resultIdx, ct, internalMemUsed, err
	case tree.Datum:
		datumType := t.ResolvedType()
		ct = make([]types.T, len(columnTypes)+1)
		copy(ct, columnTypes)
		resultIdx = len(columnTypes)
		ct[resultIdx] = *datumType
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

		allocator := NewAllocator(ctx, acc)
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
		ct = make([]types.T, len(columnTypes)+1)
		copy(ct, columnTypes)
		ct[caseOutputIdx] = *t.ResolvedType()
		// We don't know the schema yet and will update it below, right before
		// instantiating caseOp. The same goes for subsetEndIdx.
		schemaEnforcer := newBatchSchemaSubsetEnforcer(
			allocator, input, nil /* typs */, caseOutputIdx, -1, /* subsetEndIdx */
		)
		buffer := NewBufferOp(schemaEnforcer)
		caseOps := make([]Operator, len(t.Whens))
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

		schemaEnforcer.typs = toPhysTypesMaybeUnhandled(ct)
		schemaEnforcer.subsetEndIdx = len(ct)
		op := NewCaseOp(allocator, buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)
		internalMemUsed += op.(InternalMemoryOperator).InternalMemoryUsage()
		return op, caseOutputIdx, ct, internalMemUsed, err
	case *tree.AndExpr, *tree.OrExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, acc)
	default:
		return nil, resultIdx, nil, internalMemUsed, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func checkSupportedProjectionExpr(binOp tree.Operator, left, right tree.TypedExpr) error {
	leftTyp := left.ResolvedType()
	rightTyp := right.ResolvedType()
	if leftTyp.Equivalent(rightTyp) {
		return nil
	}

	// The types are not equivalent. Check if either is a type we'd like to avoid.
	for _, t := range []*types.T{leftTyp, rightTyp} {
		switch t.Family() {
		case types.DateFamily, types.TimestampFamily, types.TimestampTZFamily, types.IntervalFamily:
			return errors.New("dates, timestamp(tz), and intervals not supported in mixed-type expressions in the vectorized engine")
		}
	}

	// Because we want to be conservative, we allow specialized mixed-type
	// operators with simple types and deny all else.
	switch binOp {
	case tree.Like, tree.NotLike:
	case tree.In, tree.NotIn:
	case tree.IsDistinctFrom, tree.IsNotDistinctFrom:
	default:
		return errors.New("binary operation not supported with mixed types")
	}
	return nil
}

func planProjectionExpr(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	projOp tree.Operator,
	outputType *types.T,
	left, right tree.TypedExpr,
	columnTypes []types.T,
	input Operator,
	acc *mon.BoundAccount,
) (op Operator, resultIdx int, ct []types.T, internalMemUsed int, err error) {
	if err := checkSupportedProjectionExpr(projOp, left, right); err != nil {
		return nil, resultIdx, ct, internalMemUsed, err
	}
	resultIdx = -1
	outputPhysType := typeconv.FromColumnType(outputType)
	// actualOutputType tracks the logical type of the output column of the
	// projection operator. See the comment below for more details.
	actualOutputType := outputType
	if outputType.Identical(types.Int) {
		// Currently, SQL type system does not respect the width of integers
		// when figuring out the type of the output of a projection expression
		// (for example, INT2 + INT2 will be typed as INT8); however,
		// vectorized operators do respect the width when both operands have
		// the same width. In order to go around this limitation, we explicitly
		// check whether output type is INT8, and if so, we override the output
		// physical types to be what the vectorized projection operators will
		// actually output.
		//
		// Note that in mixed-width scenarios (i.e. INT2 + INT4) the vectorized
		// engine will output INT8, so no overriding is needed.
		//
		// We do, however, need to plan a cast to the expected logical type and
		// we will do that below.
		leftPhysType := typeconv.FromColumnType(left.ResolvedType())
		rightPhysType := typeconv.FromColumnType(right.ResolvedType())
		if leftPhysType == coltypes.Int16 && rightPhysType == coltypes.Int16 {
			actualOutputType = types.Int2
			outputPhysType = coltypes.Int16
		} else if leftPhysType == coltypes.Int32 && rightPhysType == coltypes.Int32 {
			actualOutputType = types.Int4
			outputPhysType = coltypes.Int32
		}
	}
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	if lConstArg, lConst := left.(tree.Datum); lConst {
		// Case one: The left is constant.
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for non-commutative
		// operators such as - and /, though, so we still need this case.
		var rightIdx int
		input, rightIdx, ct, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, right, columnTypes, input, acc,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		resultIdx = len(ct)
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = GetProjectionLConstOperator(
			NewAllocator(ctx, acc), left.ResolvedType(), &ct[rightIdx], outputPhysType,
			projOp, input, rightIdx, lConstArg, resultIdx,
		)
	} else {
		var (
			leftIdx             int
			internalMemUsedLeft int
		)
		input, leftIdx, ct, internalMemUsedLeft, err = planProjectionOperators(
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
			if projOp == tree.Like || projOp == tree.NotLike {
				negate := projOp == tree.NotLike
				op, err = GetLikeProjectionOperator(
					NewAllocator(ctx, acc), evalCtx, input, leftIdx, resultIdx,
					string(tree.MustBeDString(rConstArg)), negate,
				)
			} else if projOp == tree.In || projOp == tree.NotIn {
				negate := projOp == tree.NotIn
				datumTuple, ok := tree.AsDTuple(rConstArg)
				if !ok {
					err = errors.Errorf("IN operator supported only on constant expressions")
					return nil, resultIdx, ct, internalMemUsed, err
				}
				op, err = GetInProjectionOperator(
					NewAllocator(ctx, acc), &ct[leftIdx], input, leftIdx,
					resultIdx, datumTuple, negate,
				)
			} else if projOp == tree.IsDistinctFrom || projOp == tree.IsNotDistinctFrom {
				if right != tree.DNull {
					err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
					return nil, resultIdx, ct, internalMemUsed, err
				}
				// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we want to
				// negate when IS DISTINCT FROM is used.
				negate := projOp == tree.IsDistinctFrom
				op = newIsNullProjOp(NewAllocator(ctx, acc), input, leftIdx, resultIdx, negate)
			} else {
				op, err = GetProjectionRConstOperator(
					NewAllocator(ctx, acc), &ct[leftIdx], right.ResolvedType(), outputPhysType,
					projOp, input, leftIdx, rConstArg, resultIdx,
				)
			}
		} else {
			// Case 3: neither are constant.
			var (
				rightIdx             int
				internalMemUsedRight int
			)
			input, rightIdx, ct, internalMemUsedRight, err = planProjectionOperators(
				ctx, evalCtx, right, ct, input, acc,
			)
			if err != nil {
				return nil, resultIdx, nil, internalMemUsed, err
			}
			internalMemUsed += internalMemUsedRight
			resultIdx = len(ct)
			op, err = GetProjectionOperator(
				NewAllocator(ctx, acc), &ct[leftIdx], &ct[rightIdx], outputPhysType,
				projOp, input, leftIdx, rightIdx, resultIdx,
			)
		}
	}
	if err != nil {
		return op, resultIdx, ct, internalMemUsed, err
	}
	if sMem, ok := op.(InternalMemoryOperator); ok {
		internalMemUsed += sMem.InternalMemoryUsage()
	}
	oldTyps := ct
	ct = make([]types.T, len(oldTyps)+1)
	copy(ct, oldTyps)
	ct[len(oldTyps)] = *actualOutputType
	if !outputType.Identical(actualOutputType) {
		// The projection operator outputs a column of a different type than
		// the expected logical type. In order to "synchronize" the reality and
		// the expectations, we plan a cast.
		//
		// For example, INT2 + INT2 will be typed as INT8 by the SQL type
		// system, but we will plan a projection operator that outputs
		// coltypes.Int16 = INT2, so in such scenario we will have
		//    actualOutputType = types.Int2
		//          outputType = types.Int8
		// and will plan the corresponding cast.
		//
		// NOTE: this is *only* needed for integer types and should be removed
		// once #46940 is resolved.
		op, resultIdx, ct, err = planCastOperator(ctx, acc, ct, op, resultIdx, actualOutputType, outputType)
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
	ct = make([]types.T, resultIdx+1)
	copy(ct, columnTypes)
	ct[resultIdx] = *types.Bool
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
	allocator := NewAllocator(ctx, acc)
	input = newBatchSchemaSubsetEnforcer(allocator, input, toPhysTypesMaybeUnhandled(ct), resultIdx, len(ct))
	switch expr.(type) {
	case *tree.AndExpr:
		outputOp = NewAndProjOp(
			allocator,
			input, leftProjOpChain, rightProjOpChain,
			&leftFeedOp, &rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	case *tree.OrExpr:
		outputOp = NewOrProjOp(
			allocator,
			input, leftProjOpChain, rightProjOpChain,
			&leftFeedOp, &rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	}
	return outputOp, resultIdx, ct, internalMemUsedLeft + internalMemUsedRight, nil
}
