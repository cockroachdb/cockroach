// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colbuilder

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colfetcher"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
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
)

func checkNumIn(inputs []colexecbase.Operator, numIn int) error {
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
	inputs []colexecbase.Operator,
	inputTypes [][]*types.T,
	acc *mon.BoundAccount,
	processorID int32,
	newToWrap func([]execinfra.RowSource) (execinfra.RowSource, error),
	factory coldata.ColumnFactory,
) (*colexec.Columnarizer, error) {
	var toWrapInputs []execinfra.RowSource
	for i, input := range inputs {
		// Optimization: if the input is a Columnarizer, its input is necessarily a
		// execinfra.RowSource, so remove the unnecessary conversion.
		if c, ok := input.(*colexec.Columnarizer); ok {
			// TODO(asubiotto): We might need to do some extra work to remove references
			// to this operator (e.g. streamIDToOp).
			toWrapInputs = append(toWrapInputs, c.Input())
		} else {
			toWrapInput, err := colexec.NewMaterializer(
				flowCtx,
				processorID,
				input,
				inputTypes[i],
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

	return colexec.NewColumnarizer(ctx, colmem.NewAllocator(ctx, acc, factory), flowCtx, processorID, toWrap)
}

type opResult struct {
	*colexec.NewColOperatorResult
}

// resetToState resets r to the state specified in arg. arg may be a shallow
// copy made at a given point in time.
func (r *opResult) resetToState(ctx context.Context, arg colexec.NewColOperatorResult) {
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
	*r.NewColOperatorResult = arg
}

// isSupported checks whether we have a columnar operator equivalent to a
// processor described by spec. Note that it doesn't perform any other checks
// (like validity of the number of inputs).
func isSupported(mode sessiondata.VectorizeExecMode, spec *execinfrapb.ProcessorSpec) error {
	core := spec.Core
	isFullVectorization := mode == sessiondata.VectorizeOn ||
		mode == sessiondata.VectorizeExperimentalAlways

	switch {
	case core.Noop != nil:
		return nil

	case core.Values != nil:
		if core.Values.NumRows != 0 {
			return errors.Newf("values core only with zero rows supported")
		}
		return nil

	case core.TableReader != nil:
		if core.TableReader.IsCheck {
			return errors.Newf("scrub table reader is unsupported in vectorized")
		}
		return nil

	case core.Aggregator != nil:
		aggSpec := core.Aggregator
		for _, agg := range aggSpec.Aggregations {
			if agg.Distinct {
				return errors.Newf("distinct aggregation not supported")
			}
			if agg.FilterColIdx != nil {
				return errors.Newf("filtering aggregation not supported")
			}
			if len(agg.Arguments) > 0 {
				return errors.Newf("aggregates with arguments not supported")
			}
			inputTypes := make([]*types.T, len(agg.ColIdx))
			for pos, colIdx := range agg.ColIdx {
				inputTypes[pos] = spec.Input[0].ColumnTypes[colIdx]
			}
			if err := isAggregateSupported(agg.Func, inputTypes); err != nil {
				return err
			}
		}
		return nil

	case core.Distinct != nil:
		if core.Distinct.NullsAreDistinct {
			return errors.Newf("distinct with unique nulls not supported")
		}
		if core.Distinct.ErrorOnDup != "" {
			return errors.Newf("distinct with error on duplicates not supported")
		}
		if !isFullVectorization {
			if len(core.Distinct.OrderedColumns) < len(core.Distinct.DistinctColumns) {
				return errors.Newf("unordered distinct can only run in vectorize 'on' mode")
			}
		}
		return nil

	case core.Ordinality != nil:
		return nil

	case core.HashJoiner != nil:
		if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type != sqlbase.InnerJoin {
			return errors.Newf("can't plan vectorized non-inner hash joins with ON expressions")
		}
		leftInput, rightInput := spec.Input[0], spec.Input[1]
		if len(leftInput.ColumnTypes) == 0 || len(rightInput.ColumnTypes) == 0 {
			// We have a cross join of two inputs, and at least one of them has
			// zero-length schema. However, the hash join operators (both
			// external and in-memory) have a built-in assumption of non-empty
			// inputs, so we will fallback to row execution in such cases.
			// TODO(yuzefovich): implement specialized cross join operator.
			return errors.Newf("can't plan vectorized hash joins with an empty input schema")
		}
		return nil

	case core.MergeJoiner != nil:
		if !core.MergeJoiner.OnExpr.Empty() &&
			core.MergeJoiner.Type != sqlbase.InnerJoin {
			return errors.Errorf("can't plan non-inner merge join with ON expressions")
		}
		return nil

	case core.Sorter != nil:
		return nil

	case core.Windower != nil:
		for _, wf := range core.Windower.WindowFns {
			if wf.Frame != nil {
				frame, err := wf.Frame.ConvertToAST()
				if err != nil {
					return err
				}
				if !frame.IsDefaultFrame() {
					return errors.Newf("window functions with non-default window frames are not supported")
				}
			}
			if wf.FilterColIdx != tree.NoColumnIdx {
				return errors.Newf("window functions with FILTER clause are not supported")
			}
			if wf.Func.AggregateFunc != nil {
				return errors.Newf("aggregate functions used as window functions are not supported")
			}

			if _, supported := SupportedWindowFns[*wf.Func.WindowFunc]; !supported {
				return errors.Newf("window function %s is not supported", wf.String())
			}
			if !isFullVectorization {
				switch *wf.Func.WindowFunc {
				case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
					return errors.Newf("window function %s can only run in vectorize 'on' mode", wf.String())
				}
			}
		}
		return nil

	default:
		return errors.Newf("unsupported processor core %q", core)
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
func (r opResult) createDiskBackedSort(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args colexec.NewColOperatorArgs,
	input colexecbase.Operator,
	inputTypes []*types.T,
	ordering execinfrapb.Ordering,
	matchLen uint32,
	maxNumberPartitions int,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
	memMonitorNamePrefix string,
	factory coldata.ColumnFactory,
) (colexecbase.Operator, error) {
	streamingMemAccount := args.StreamingMemAccount
	useStreamingMemAccountForBuffering := args.TestingKnobs.UseStreamingMemAccountForBuffering
	var (
		sorterMemMonitorName string
		inMemorySorter       colexecbase.Operator
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
		inMemorySorter, err = colexec.NewSortChunks(
			colmem.NewAllocator(ctx, sortChunksMemAccount, factory), input, inputTypes,
			ordering.Columns, int(matchLen),
		)
	} else if post.Limit != 0 && post.Filter.Empty() && int(post.Limit+post.Offset) > 0 {
		// There is a limit specified with no post-process filter, so we know
		// exactly how many rows the sorter should output. The last part of the
		// condition is making sure there is no overflow when converting from
		// the sum of two uint64s to int.
		//
		// Choose a top K sorter, which uses a heap to avoid storing more rows
		// than necessary.
		sorterMemMonitorName = fmt.Sprintf("%stopk-sort-%d", memMonitorNamePrefix, processorID)
		var topKSorterMemAccount *mon.BoundAccount
		if useStreamingMemAccountForBuffering {
			topKSorterMemAccount = streamingMemAccount
		} else {
			topKSorterMemAccount = r.createMemAccountForSpillStrategy(
				ctx, flowCtx, sorterMemMonitorName,
			)
		}
		k := int(post.Limit + post.Offset)
		inMemorySorter = colexec.NewTopKSorter(
			colmem.NewAllocator(ctx, topKSorterMemAccount, factory), input, inputTypes,
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
		inMemorySorter, err = colexec.NewSorter(
			colmem.NewAllocator(ctx, sorterMemAccount, factory), input, inputTypes, ordering.Columns,
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
	return colexec.NewOneInputDiskSpiller(
		input, inMemorySorter.(colexecbase.BufferingInMemoryOperator),
		sorterMemMonitorName,
		func(input colexecbase.Operator) colexecbase.Operator {
			monitorNamePrefix := fmt.Sprintf("%sexternal-sorter", memMonitorNamePrefix)
			// We are using an unlimited memory monitor here because external
			// sort itself is responsible for making sure that we stay within
			// the memory limit.
			unlimitedAllocator := colmem.NewAllocator(
				ctx, r.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorNamePrefix,
				), factory)
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
			es := colexec.NewExternalSorter(
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
			r.ToClose = append(r.ToClose, es.(colexec.IdempotentCloser))
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
func (r opResult) createAndWrapRowSource(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	inputs []colexecbase.Operator,
	inputTypes [][]*types.T,
	streamingMemAccount *mon.BoundAccount,
	spec *execinfrapb.ProcessorSpec,
	processorConstructor execinfra.ProcessorConstructor,
	factory coldata.ColumnFactory,
) error {
	if processorConstructor == nil {
		// TODO(yuzefovich): update unit tests to remove panic-catcher when
		// fallback to rowexec is not allowed.
		return errors.New("processorConstructor is nil")
	}
	if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.Vectorize201Auto &&
		spec.Core.JoinReader == nil {
		return errors.New("rowexec processor wrapping for non-JoinReader core unsupported in vectorize=201auto mode")
	}
	c, err := wrapRowSources(
		ctx,
		flowCtx,
		inputs,
		inputTypes,
		streamingMemAccount,
		spec.ProcessorID,
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
		factory,
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
// 1. r.ColumnTypes={types.Bool} with len=1 and cap=4
// 2. planSelectionOperators adds another types.Int column, so
//    filterColumnTypes={types.Bool, types.Int} with len=2 and cap=4
//    Crucially, it uses exact same underlying array as r.ColumnTypes
//    uses.
// 3. we project out second column, so r.ColumnTypes={types.Bool}
// 4. later, we add another types.Float column, so
//    r.ColumnTypes={types.Bool, types.Float}, but there is enough
//    capacity in the array, so we simply overwrite the second slot
//    with the new type which corrupts filterColumnTypes to become
//    {types.Bool, types.Float}, and we can get into a runtime type
//    mismatch situation.

// NewColOperator creates a new columnar operator according to the given spec.
func NewColOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, args colexec.NewColOperatorArgs,
) (r colexec.NewColOperatorResult, err error) {
	result := opResult{NewColOperatorResult: &r}
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
			colexecerror.InternalError(panicErr)
		}
	}()
	spec := args.Spec
	inputs := args.Inputs
	factory := coldataext.NewExtendedColumnFactory(flowCtx.NewEvalCtx())
	streamingMemAccount := args.StreamingMemAccount
	streamingAllocator := colmem.NewAllocator(ctx, streamingMemAccount, factory)
	useStreamingMemAccountForBuffering := args.TestingKnobs.UseStreamingMemAccountForBuffering
	processorConstructor := args.ProcessorConstructor
	if args.ExprHelper == nil {
		args.ExprHelper = colexec.NewDefaultExprHelper()
	}

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
	resultPreSpecPlanningStateShallowCopy := r

	if err = isSupported(flowCtx.EvalCtx.SessionData.VectorizeMode, spec); err != nil {
		// We refuse to wrap LocalPlanNode processor (which is a DistSQL wrapper
		// around a planNode) because it creates complications, and a flow with
		// such processor probably will not benefit from the vectorization.
		if core.LocalPlanNode != nil {
			return r, errors.Newf("core.LocalPlanNode is not supported")
		}
		// We also do not wrap MetadataTest{Sender,Receiver} because of the way
		// metadata is propagated through the vectorized flow - it is drained at
		// the flow shutdown unlike these test processors expect.
		if core.MetadataTestSender != nil {
			return r, errors.Newf("core.MetadataTestSender is not supported")
		}
		if core.MetadataTestReceiver != nil {
			return r, errors.Newf("core.MetadataTestReceiver is not supported")
		}
		if core.InvertedFilterer != nil {
			// colfetcher.cfetcher currently tries to decode the inverted
			// column needed for inverted filtering, but that inverted column is
			// not of the same type as the original column that was indexed (e.g.
			// for geometry, the inverted column contains an int, and for arrays
			// the inverted column contains the array element type).
			// For now, we do not vectorize flows with inverted filterers.
			return r, errors.Newf("core.InvertedFilterer is not supported")
		}
		log.VEventf(ctx, 1, "planning a wrapped processor because %s", err.Error())

		inputTypes := make([][]*types.T, len(spec.Input))
		for inputIdx, input := range spec.Input {
			inputTypes[inputIdx] = make([]*types.T, len(input.ColumnTypes))
			copy(inputTypes[inputIdx], input.ColumnTypes)
		}

		err = result.createAndWrapRowSource(ctx, flowCtx, inputs, inputTypes,
			streamingMemAccount, spec, processorConstructor, factory)
		// The wrapped processors need to be passed the post-process specs,
		// since they inspect them to figure out information about needed
		// columns. This means that we'll let those processors do any renders
		// or filters, which isn't ideal. We could improve this.
		post = &execinfrapb.PostProcessSpec{}
	} else {
		switch {
		case core.Noop != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			result.Op, result.IsStreaming = colexec.NewNoop(inputs[0]), true
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)

		case core.Values != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}
			if core.Values.NumRows != 0 {
				return r, errors.AssertionFailedf("values core only with zero rows supported, %d given", core.Values.NumRows)
			}
			result.Op, result.IsStreaming = colexec.NewZeroOpNoInput(), true
			result.ColumnTypes = make([]*types.T, len(core.Values.Columns))
			for i, col := range core.Values.Columns {
				result.ColumnTypes[i] = col.Type
			}

		case core.TableReader != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}
			scanOp, err := colfetcher.NewColBatchScan(streamingAllocator, flowCtx, core.TableReader, post)
			if err != nil {
				return r, err
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
			result.Op = colexec.NewCancelChecker(result.Op)
			returnMutations := core.TableReader.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
			result.ColumnTypes = core.TableReader.Table.ColumnTypesWithMutations(returnMutations)
		case core.Aggregator != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
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
				result.Op, result.IsStreaming, err = colexec.NewSingleTupleNoInputOp(streamingAllocator), true, nil
				// We make ColumnTypes non-nil so that sanity check doesn't panic.
				result.ColumnTypes = []*types.T{}
				break
			}
			if aggSpec.IsRowCount() {
				result.Op, result.IsStreaming, err = colexec.NewCountOp(streamingAllocator, inputs[0]), true, nil
				result.ColumnTypes = []*types.T{types.Int}
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
				return r, errors.AssertionFailedf("ordered cols must be a subset of grouping cols")
			}

			aggTyps := make([][]*types.T, len(aggSpec.Aggregations))
			aggCols := make([][]uint32, len(aggSpec.Aggregations))
			aggFns := make([]execinfrapb.AggregatorSpec_Func, len(aggSpec.Aggregations))
			for i, agg := range aggSpec.Aggregations {
				aggTyps[i] = make([]*types.T, len(agg.ColIdx))
				for j, colIdx := range agg.ColIdx {
					aggTyps[i][j] = spec.Input[0].ColumnTypes[colIdx]
				}
				aggCols[i] = agg.ColIdx
				aggFns[i] = agg.Func
			}
			result.ColumnTypes, err = colexec.MakeAggregateFuncsOutputTypes(aggTyps, aggFns)
			if err != nil {
				return r, err
			}
			typs := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(typs, spec.Input[0].ColumnTypes)
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
				result.Op, err = colexec.NewHashAggregator(
					colmem.NewAllocator(ctx, hashAggregatorMemAccount, factory), inputs[0], typs, aggFns,
					aggSpec.GroupCols, aggCols,
				)
			} else {
				result.Op, err = colexec.NewOrderedAggregator(
					streamingAllocator, inputs[0], typs, aggFns,
					aggSpec.GroupCols, aggCols, aggSpec.IsScalar(),
				)
				result.IsStreaming = true
			}

		case core.Distinct != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			if len(core.Distinct.OrderedColumns) == len(core.Distinct.DistinctColumns) {
				result.Op, err = colexec.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, result.ColumnTypes)
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
				result.Op = colexec.NewUnorderedDistinct(
					colmem.NewAllocator(ctx, distinctMemAccount, factory), inputs[0],
					core.Distinct.DistinctColumns, result.ColumnTypes, colexec.HashTableNumBuckets,
				)
			}

		case core.Ordinality != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Op = colexec.NewOrdinalityOp(streamingAllocator, inputs[0], outputIdx)
			result.IsStreaming = true
			result.ColumnTypes = appendOneType(spec.Input[0].ColumnTypes, types.Int)

		case core.HashJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}
			leftTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(leftTypes, spec.Input[0].ColumnTypes)
			rightTypes := make([]*types.T, len(spec.Input[1].ColumnTypes))
			copy(rightTypes, spec.Input[1].ColumnTypes)

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
			hjSpec, err := colexec.MakeHashJoinerSpec(
				core.HashJoiner.Type,
				core.HashJoiner.LeftEqColumns,
				core.HashJoiner.RightEqColumns,
				leftTypes,
				rightTypes,
				rightEqColsAreKey,
			)
			if err != nil {
				return r, err
			}
			inMemoryHashJoiner := colexec.NewHashJoiner(
				colmem.NewAllocator(ctx, hashJoinerMemAccount, factory), hjSpec, inputs[0], inputs[1],
			)
			if args.TestingKnobs.DiskSpillingDisabled {
				// We will not be creating a disk-backed hash joiner because we're
				// running a test that explicitly asked for only in-memory hash
				// joiner.
				result.Op = inMemoryHashJoiner
			} else {
				diskAccount := result.createDiskAccount(ctx, flowCtx, hashJoinerMemMonitorName)
				result.Op = colexec.NewTwoInputDiskSpiller(
					inputs[0], inputs[1], inMemoryHashJoiner.(colexecbase.BufferingInMemoryOperator),
					hashJoinerMemMonitorName,
					func(inputOne, inputTwo colexecbase.Operator) colexecbase.Operator {
						monitorNamePrefix := "external-hash-joiner"
						unlimitedAllocator := colmem.NewAllocator(
							ctx, result.createBufferingUnlimitedMemAccount(
								ctx, flowCtx, monitorNamePrefix,
							), factory)
						// Make a copy of the DiskQueueCfg and set defaults for the hash
						// joiner. The cache mode is chosen to automatically close the cache
						// belonging to partitions at a parent level when repartitioning.
						diskQueueCfg := args.DiskQueueCfg
						diskQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
						diskQueueCfg.SetDefaultBufferSizeBytesForCacheMode()
						ehj := colexec.NewExternalHashJoiner(
							unlimitedAllocator, hjSpec,
							inputOne, inputTwo,
							execinfra.GetWorkMemLimit(flowCtx.Cfg),
							diskQueueCfg,
							args.FDSemaphore,
							func(input colexecbase.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column, maxNumberPartitions int) (colexecbase.Operator, error) {
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
									&execinfrapb.PostProcessSpec{}, monitorNamePrefix+"-", factory)
							},
							args.TestingKnobs.NumForcedRepartitions,
							args.TestingKnobs.DelegateFDAcquisitions,
							diskAccount,
						)
						result.ToClose = append(result.ToClose, ehj.(colexec.IdempotentCloser))
						return ehj
					},
					args.TestingKnobs.SpillingCallbackFn,
				)
			}
			result.ColumnTypes = make([]*types.T, len(leftTypes)+len(rightTypes))
			copy(result.ColumnTypes, leftTypes)
			if !core.HashJoiner.Type.ShouldIncludeRightColsInOutput() {
				result.ColumnTypes = result.ColumnTypes[:len(leftTypes):len(leftTypes)]
			} else {
				copy(result.ColumnTypes[len(leftTypes):], rightTypes)
			}

			if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type == sqlbase.InnerJoin {
				if err =
					result.planAndMaybeWrapOnExprAsFilter(
						ctx, flowCtx, core.HashJoiner.OnExpr, streamingMemAccount, processorConstructor, factory, args.ExprHelper,
					); err != nil {
					return r, err
				}
			}

		case core.MergeJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}
			// Merge joiner is a streaming operator when equality columns form a key
			// for both of the inputs.
			result.IsStreaming = core.MergeJoiner.LeftEqColumnsAreKey && core.MergeJoiner.RightEqColumnsAreKey

			leftTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(leftTypes, spec.Input[0].ColumnTypes)
			rightTypes := make([]*types.T, len(spec.Input[1].ColumnTypes))
			copy(rightTypes, spec.Input[1].ColumnTypes)

			joinType := core.MergeJoiner.Type
			var onExpr *execinfrapb.Expression
			if !core.MergeJoiner.OnExpr.Empty() {
				if joinType != sqlbase.InnerJoin {
					return r, errors.AssertionFailedf(
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
			unlimitedAllocator := colmem.NewAllocator(
				ctx, result.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorName,
				), factory)
			diskAccount := result.createDiskAccount(ctx, flowCtx, monitorName)
			mj, err := colexec.NewMergeJoinOp(
				unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx.Cfg),
				args.DiskQueueCfg, args.FDSemaphore,
				joinType, inputs[0], inputs[1], leftTypes, rightTypes,
				core.MergeJoiner.LeftOrdering.Columns, core.MergeJoiner.RightOrdering.Columns,
				diskAccount,
			)
			if err != nil {
				return r, err
			}

			result.Op = mj
			result.ToClose = append(result.ToClose, mj.(colexec.IdempotentCloser))
			result.ColumnTypes = make([]*types.T, len(leftTypes)+len(rightTypes))
			copy(result.ColumnTypes, leftTypes)
			if !core.MergeJoiner.Type.ShouldIncludeRightColsInOutput() {
				result.ColumnTypes = result.ColumnTypes[:len(leftTypes):len(leftTypes)]
			} else {
				copy(result.ColumnTypes[len(leftTypes):], rightTypes)
			}

			if onExpr != nil {
				if err = result.planAndMaybeWrapOnExprAsFilter(
					ctx, flowCtx, *onExpr, streamingMemAccount, processorConstructor, factory, args.ExprHelper,
				); err != nil {
					return r, err
				}
			}

		case core.Sorter != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			input := inputs[0]
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			ordering := core.Sorter.OutputOrdering
			matchLen := core.Sorter.OrderingMatchLen
			result.Op, err = result.createDiskBackedSort(
				ctx, flowCtx, args, input, result.ColumnTypes, ordering, matchLen, 0, /* maxNumberPartitions */
				spec.ProcessorID, post, "" /* memMonitorNamePrefix */, factory,
			)

		case core.Windower != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			memMonitorsPrefix := "window-"
			input := inputs[0]
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			for _, wf := range core.Windower.WindowFns {
				// We allocate the capacity for two extra types because of the
				// temporary columns that can be appended below.
				typs := make([]*types.T, len(result.ColumnTypes), len(result.ColumnTypes)+2)
				copy(typs, result.ColumnTypes)
				tempColOffset, partitionColIdx := uint32(0), tree.NoColumnIdx
				peersColIdx := tree.NoColumnIdx
				windowFn := *wf.Func.WindowFunc
				if len(core.Windower.PartitionBy) > 0 {
					// TODO(yuzefovich): add support for hashing partitioner (probably by
					// leveraging hash routers once we can distribute). The decision about
					// which kind of partitioner to use should come from the optimizer.
					partitionColIdx = int(wf.OutputColIdx)
					input, err = colexec.NewWindowSortingPartitioner(
						streamingAllocator, input, typs,
						core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx),
						func(input colexecbase.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column) (colexecbase.Operator, error) {
							return result.createDiskBackedSort(
								ctx, flowCtx, args, input, inputTypes,
								execinfrapb.Ordering{Columns: orderingCols}, 0, /* matchLen */
								0 /* maxNumberPartitions */, spec.ProcessorID,
								&execinfrapb.PostProcessSpec{}, memMonitorsPrefix, factory)
						},
					)
					// Window partitioner will append a boolean column.
					tempColOffset++
					typs = typs[:len(typs)+1]
					typs[len(typs)-1] = types.Bool
				} else {
					if len(wf.Ordering.Columns) > 0 {
						input, err = result.createDiskBackedSort(
							ctx, flowCtx, args, input, typs,
							wf.Ordering, 0 /* matchLen */, 0, /* maxNumberPartitions */
							spec.ProcessorID, &execinfrapb.PostProcessSpec{}, memMonitorsPrefix, factory,
						)
					}
				}
				if err != nil {
					return r, err
				}
				if windowFnNeedsPeersInfo(*wf.Func.WindowFunc) {
					peersColIdx = int(wf.OutputColIdx + tempColOffset)
					input, err = colexec.NewWindowPeerGrouper(
						streamingAllocator, input, typs, wf.Ordering.Columns,
						partitionColIdx, peersColIdx,
					)
					// Window peer grouper will append a boolean column.
					tempColOffset++
					typs = typs[:len(typs)+1]
					typs[len(typs)-1] = types.Bool
				}

				outputIdx := int(wf.OutputColIdx + tempColOffset)
				switch windowFn {
				case execinfrapb.WindowerSpec_ROW_NUMBER:
					result.Op = colexec.NewRowNumberOperator(streamingAllocator, input, outputIdx, partitionColIdx)
				case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
					result.Op, err = colexec.NewRankOperator(
						streamingAllocator, input, windowFn, wf.Ordering.Columns,
						outputIdx, partitionColIdx, peersColIdx,
					)
				case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
					// We are using an unlimited memory monitor here because
					// relative rank operators themselves are responsible for
					// making sure that we stay within the memory limit, and
					// they will fall back to disk if necessary.
					memAccName := memMonitorsPrefix + "relative-rank"
					unlimitedAllocator := colmem.NewAllocator(
						ctx, result.createBufferingUnlimitedMemAccount(ctx, flowCtx, memAccName), factory,
					)
					diskAcc := result.createDiskAccount(ctx, flowCtx, memAccName)
					result.Op, err = colexec.NewRelativeRankOperator(
						unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx.Cfg), args.DiskQueueCfg,
						args.FDSemaphore, input, typs, windowFn, wf.Ordering.Columns,
						outputIdx, partitionColIdx, peersColIdx, diskAcc,
					)
					// NewRelativeRankOperator sometimes returns a constOp when there
					// are no ordering columns, so we check that the returned operator
					// is an IdempotentCloser.
					if c, ok := result.Op.(colexec.IdempotentCloser); ok {
						result.ToClose = append(result.ToClose, c)
					}
				default:
					return r, errors.AssertionFailedf("window function %s is not supported", wf.String())
				}

				if tempColOffset > 0 {
					// We want to project out temporary columns (which have indices in the
					// range [wf.OutputColIdx, wf.OutputColIdx+tempColOffset)).
					projection := make([]uint32, 0, wf.OutputColIdx+tempColOffset)
					for i := uint32(0); i < wf.OutputColIdx; i++ {
						projection = append(projection, i)
					}
					projection = append(projection, wf.OutputColIdx+tempColOffset)
					result.Op = colexec.NewSimpleProjectOp(result.Op, int(wf.OutputColIdx+tempColOffset), projection)
				}

				_, returnType, err := execinfrapb.GetWindowFunctionInfo(wf.Func, []*types.T{}...)
				if err != nil {
					return r, err
				}
				result.ColumnTypes = appendOneType(result.ColumnTypes, returnType)
				input = result.Op
			}

		default:
			return r, errors.Newf("unsupported processor core %q", core)
		}
	}

	if err != nil {
		return r, err
	}

	// After constructing the base operator, calculate its internal memory usage.
	if sMem, ok := result.Op.(colexec.InternalMemoryOperator); ok {
		result.InternalMemUsage += sMem.InternalMemoryUsage()
	}
	log.VEventf(ctx, 1, "made op %T\n", result.Op)

	// Note: at this point, it is legal for ColumnTypes to be empty (it is
	// legal for empty rows to be passed between processors).

	ppr := postProcessResult{
		Op:          result.Op,
		ColumnTypes: result.ColumnTypes,
	}
	err = ppr.planPostProcessSpec(ctx, flowCtx, post, streamingMemAccount, factory, args.ExprHelper)
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
			inputTypes := make([][]*types.T, len(spec.Input))
			for inputIdx, input := range spec.Input {
				inputTypes[inputIdx] = make([]*types.T, len(input.ColumnTypes))
				copy(inputTypes[inputIdx], input.ColumnTypes)
			}
			result.resetToState(ctx, resultPreSpecPlanningStateShallowCopy)
			err = result.createAndWrapRowSource(
				ctx, flowCtx, inputs, inputTypes, streamingMemAccount, spec, processorConstructor, factory,
			)
			if err != nil {
				// There was an error wrapping the TableReader.
				return r, err
			}
		} else {
			err = result.wrapPostProcessSpec(ctx, flowCtx, post, streamingMemAccount, processorConstructor, factory)
		}
	} else {
		// The result can be updated with the post process result.
		result.updateWithPostProcessResult(ppr)
	}
	return r, err
}

// planAndMaybeWrapOnExprAsFilter plans a joiner ON expression as a filter. If
// the filter is unsupported, it is planned as a wrapped noop processor with
// the filter as a post-processing stage.
func (r opResult) planAndMaybeWrapOnExprAsFilter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	onExpr execinfrapb.Expression,
	streamingMemAccount *mon.BoundAccount,
	processorConstructor execinfra.ProcessorConstructor,
	factory coldata.ColumnFactory,
	helper colexec.ExprHelper,
) error {
	// We will plan other Operators on top of r.Op, so we need to account for the
	// internal memory explicitly.
	if internalMemOp, ok := r.Op.(colexec.InternalMemoryOperator); ok {
		r.InternalMemUsage += internalMemOp.InternalMemoryUsage()
	}
	ppr := postProcessResult{
		Op:          r.Op,
		ColumnTypes: r.ColumnTypes,
	}
	if err := ppr.planFilterExpr(
		ctx, flowCtx.NewEvalCtx(), onExpr, streamingMemAccount, factory, helper,
	); err != nil {
		// ON expression planning failed. Fall back to planning the filter
		// using row execution.
		log.VEventf(
			ctx, 2,
			"vectorized join ON expr planning failed with error %v ON expr is %s, attempting to wrap as a row source",
			err, onExpr.String(),
		)

		onExprAsFilter := &execinfrapb.PostProcessSpec{Filter: onExpr}
		return r.wrapPostProcessSpec(ctx, flowCtx, onExprAsFilter, streamingMemAccount, processorConstructor, factory)
	}
	r.updateWithPostProcessResult(ppr)
	return nil
}

// wrapPostProcessSpec plans the given post process spec by wrapping a noop
// processor with that output spec. This is used to fall back to row execution
// when encountering unsupported post processing specs. An error is returned
// if the wrapping failed. A reason for this could be an unsupported type, in
// which case the row execution engine is used fully.
func (r opResult) wrapPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	post *execinfrapb.PostProcessSpec,
	streamingMemAccount *mon.BoundAccount,
	processorConstructor execinfra.ProcessorConstructor,
	factory coldata.ColumnFactory,
) error {
	noopSpec := &execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post: *post,
	}
	return r.createAndWrapRowSource(
		ctx, flowCtx, []colexecbase.Operator{r.Op}, [][]*types.T{r.ColumnTypes},
		streamingMemAccount, noopSpec, processorConstructor, factory,
	)
}

// planPostProcessSpec plans the post processing stage specified in post on top
// of r.Op.
func (r *postProcessResult) planPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	post *execinfrapb.PostProcessSpec,
	streamingMemAccount *mon.BoundAccount,
	factory coldata.ColumnFactory,
	helper colexec.ExprHelper,
) error {
	if !post.Filter.Empty() {
		if err := r.planFilterExpr(
			ctx, flowCtx.NewEvalCtx(), post.Filter, streamingMemAccount, factory, helper,
		); err != nil {
			return err
		}
	}

	if post.Projection {
		r.addProjection(post.OutputColumns)
	} else if post.RenderExprs != nil {
		log.VEventf(ctx, 2, "planning render expressions %+v", post.RenderExprs)
		var renderedCols []uint32
		for _, renderExpr := range post.RenderExprs {
			var renderInternalMem int
			expr, err := helper.ProcessExpr(renderExpr, flowCtx.EvalCtx, r.ColumnTypes)
			if err != nil {
				return err
			}
			var outputIdx int
			r.Op, outputIdx, r.ColumnTypes, renderInternalMem, err = planProjectionOperators(
				ctx, flowCtx.NewEvalCtx(), expr, r.ColumnTypes, r.Op, streamingMemAccount, factory,
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
		r.Op = colexec.NewSimpleProjectOp(r.Op, len(r.ColumnTypes), renderedCols)
		newTypes := make([]*types.T, len(renderedCols))
		for i, j := range renderedCols {
			newTypes[i] = r.ColumnTypes[j]
		}
		r.ColumnTypes = newTypes
	}
	if post.Offset != 0 {
		r.Op = colexec.NewOffsetOp(r.Op, int(post.Offset))
	}
	if post.Limit != 0 {
		r.Op = colexec.NewLimitOp(r.Op, int(post.Limit))
	}
	return nil
}

// createBufferingUnlimitedMemMonitor instantiates an unlimited memory monitor.
// These should only be used when spilling to disk and an operator is made aware
// of a memory usage limit separately.
// The receiver is updated to have a reference to the unlimited memory monitor.
func (r opResult) createBufferingUnlimitedMemMonitor(
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
func (r opResult) createMemAccountForSpillStrategy(
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
func (r opResult) createBufferingUnlimitedMemAccount(
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
func (r opResult) createStandaloneMemAccount(
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
func (r opResult) createDiskAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	opDiskMonitor := execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, name)
	r.OpMonitors = append(r.OpMonitors, opDiskMonitor)
	opDiskAccount := opDiskMonitor.MakeBoundAccount()
	r.OpAccounts = append(r.OpAccounts, &opDiskAccount)
	return &opDiskAccount
}

type postProcessResult struct {
	Op               colexecbase.Operator
	ColumnTypes      []*types.T
	InternalMemUsage int
}

func (r opResult) updateWithPostProcessResult(ppr postProcessResult) {
	r.Op = ppr.Op
	r.ColumnTypes = make([]*types.T, len(ppr.ColumnTypes))
	copy(r.ColumnTypes, ppr.ColumnTypes)
	r.InternalMemUsage += ppr.InternalMemUsage
}

func (r *postProcessResult) planFilterExpr(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	filter execinfrapb.Expression,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
	helper colexec.ExprHelper,
) error {
	var selectionInternalMem int
	expr, err := helper.ProcessExpr(filter, evalCtx, r.ColumnTypes)
	if err != nil {
		return err
	}
	if expr == tree.DNull {
		// The filter expression is tree.DNull meaning that it is always false, so
		// we put a zero operator.
		r.Op = colexec.NewZeroOp(r.Op)
		return nil
	}
	var filterColumnTypes []*types.T
	r.Op, _, filterColumnTypes, selectionInternalMem, err = planSelectionOperators(
		ctx, evalCtx, expr, r.ColumnTypes, r.Op, acc, factory,
	)
	if err != nil {
		return errors.Wrapf(err, "unable to columnarize filter expression %q", filter)
	}
	r.InternalMemUsage += selectionInternalMem
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

// addProjection adds a simple projection to r (Op and ColumnTypes are updated
// accordingly).
func (r *postProcessResult) addProjection(projection []uint32) {
	r.Op = colexec.NewSimpleProjectOp(r.Op, len(r.ColumnTypes), projection)
	// Update output ColumnTypes.
	newTypes := make([]*types.T, len(projection))
	for i, j := range projection {
		newTypes[i] = r.ColumnTypes[j]
	}
	r.ColumnTypes = newTypes
}

func planSelectionOperators(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecbase.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
) (op colexecbase.Operator, resultIdx int, typs []*types.T, internalMemUsed int, err error) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		op, err = colexec.BoolOrUnknownToSelOp(input, columnTypes, t.Idx)
		return op, -1, columnTypes, internalMemUsed, err
	case *tree.AndExpr:
		// AND expressions are handled by an implicit AND'ing of selection vectors.
		// First we select out the tuples that are true on the left side, and then,
		// only among the matched tuples, we select out the tuples that are true on
		// the right side.
		var leftOp, rightOp colexecbase.Operator
		var internalMemUsedLeft, internalMemUsedRight int
		leftOp, _, typs, internalMemUsedLeft, err = planSelectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		rightOp, resultIdx, typs, internalMemUsedRight, err = planSelectionOperators(
			ctx, evalCtx, t.TypedRight(), typs, leftOp, acc, factory,
		)
		return rightOp, resultIdx, typs, internalMemUsedLeft + internalMemUsedRight, err
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
			return nil, resultIdx, typs, internalMemUsed, err
		}
		op, resultIdx, typs, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, caseExpr, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		op, err = colexec.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, internalMemUsed, err
	case *tree.CaseExpr:
		op, resultIdx, typs, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc, factory,
		)
		if err != nil {
			return op, resultIdx, typs, internalMemUsed, err
		}
		op, err = colexec.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, internalMemUsed, err
	case *tree.IsNullExpr:
		op, resultIdx, typs, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory,
		)
		op = colexec.NewIsNullSelOp(op, resultIdx, false)
		return op, resultIdx, typs, internalMemUsed, err
	case *tree.IsNotNullExpr:
		op, resultIdx, typs, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory,
		)
		op = colexec.NewIsNullSelOp(op, resultIdx, true)
		return op, resultIdx, typs, internalMemUsed, err
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, internalMemUsedLeft, err := planProjectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		lTyp := ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			if t.Operator == tree.Like || t.Operator == tree.NotLike {
				negate := t.Operator == tree.NotLike
				op, err = colexec.GetLikeOperator(
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
				op, err = colexec.GetInOperator(lTyp, leftOp, leftIdx, datumTuple, negate)
				return op, resultIdx, ct, internalMemUsedLeft, err
			}
			if t.Operator == tree.IsDistinctFrom || t.Operator == tree.IsNotDistinctFrom {
				if t.Right != tree.DNull {
					err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
					return nil, resultIdx, ct, internalMemUsed, err
				}
				// IS NOT DISTINCT FROM NULL is synonymous with IS NULL and IS
				// DISTINCT FROM NULL is synonymous with IS NOT NULL (except for
				// tuples). Therefore, negate when the operator is IS DISTINCT
				// FROM NULL.
				negate := t.Operator == tree.IsDistinctFrom
				op = colexec.NewIsNullSelOp(leftOp, leftIdx, negate)
				return op, resultIdx, ct, internalMemUsedLeft, err
			}
			op, err := colexec.GetSelectionConstOperator(
				lTyp, t.TypedRight().ResolvedType(), cmpOp, leftOp, leftIdx, constArg, nil, /* binFn */
			)
			return op, resultIdx, ct, internalMemUsedLeft, err
		}
		rightOp, rightIdx, ct, internalMemUsedRight, err := planProjectionOperators(
			ctx, evalCtx, t.TypedRight(), ct, leftOp, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, ct, internalMemUsed, err
		}
		op, err := colexec.GetSelectionOperator(
			lTyp, ct[rightIdx], cmpOp, rightOp, leftIdx, rightIdx, nil, /* binFn */
		)
		return op, resultIdx, ct, internalMemUsedLeft + internalMemUsedRight, err
	default:
		return nil, resultIdx, nil, internalMemUsed, errors.Errorf("unhandled selection expression type: %s", reflect.TypeOf(t))
	}
}

func checkCastSupported(fromType, toType *types.T) error {
	switch toType.Family() {
	case types.DecimalFamily:
		// If we're casting to a decimal, we're only allowing casting from the
		// decimal of the same precision due to the fact that we're losing
		// precision information once we start operating on coltypes.T. For
		// such casts we will fallback to row-by-row engine.
		// TODO(yuzefovich): coltypes.T type system has been removed,
		// reevaluate the situation.
		if !fromType.Identical(toType) {
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
	columnTypes []*types.T,
	input colexecbase.Operator,
	inputIdx int,
	fromType *types.T,
	toType *types.T,
	factory coldata.ColumnFactory,
) (op colexecbase.Operator, resultIdx int, typs []*types.T, err error) {
	if err := checkCastSupported(fromType, toType); err != nil {
		return op, resultIdx, typs, err
	}
	outputIdx := len(columnTypes)
	op, err = colexec.GetCastOperator(colmem.NewAllocator(ctx, acc, factory), input, inputIdx, outputIdx, fromType, toType)
	typs = appendOneType(columnTypes, toType)
	return op, outputIdx, typs, err
}

// planProjectionOperators plans a chain of operators to execute the provided
// expression. It returns the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planProjectionOperators(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecbase.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
) (op colexecbase.Operator, resultIdx int, typs []*types.T, internalMemUsed int, err error) {
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, internalMemUsed, nil
	case *tree.ComparisonExpr:
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(),
			columnTypes, input, acc, factory, nil, /* binFn */
		)
	case *tree.BinaryExpr:
		if err = checkSupportedBinaryExpr(t.TypedLeft(), t.TypedRight(), t.ResolvedType()); err != nil {
			return op, resultIdx, typs, internalMemUsed, err
		}
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(),
			columnTypes, input, acc, factory, t.Fn,
		)
	case *tree.IsNullExpr:
		t.TypedInnerExpr()
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, acc, false /* negate */, factory)
	case *tree.IsNotNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, acc, true /* negate */, factory)
	case *tree.CastExpr:
		expr := t.Expr.(tree.TypedExpr)
		op, resultIdx, typs, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, 0, nil, internalMemUsed, err
		}
		op, resultIdx, typs, err = planCastOperator(ctx, acc, typs, op, resultIdx, expr.ResolvedType(), t.ResolvedType(), factory)
		return op, resultIdx, typs, internalMemUsed, err
	case *tree.FuncExpr:
		var (
			inputCols             []int
			projectionInternalMem int
		)
		typs = make([]*types.T, len(columnTypes))
		copy(typs, columnTypes)
		op = input
		for _, e := range t.Exprs {
			var err error
			// TODO(rohany): This could be done better, especially in the case of
			// constant arguments, because the vectorized engine right now
			// creates a new column full of the constant value.
			op, resultIdx, typs, projectionInternalMem, err = planProjectionOperators(
				ctx, evalCtx, e.(tree.TypedExpr), typs, op, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, nil, internalMemUsed, err
			}
			inputCols = append(inputCols, resultIdx)
			internalMemUsed += projectionInternalMem
		}
		resultIdx = len(typs)
		op, err = colexec.NewBuiltinFunctionOperator(
			colmem.NewAllocator(ctx, acc, factory), evalCtx, t, typs, inputCols, resultIdx, op,
		)
		typs = appendOneType(typs, t.ResolvedType())
		return op, resultIdx, typs, internalMemUsed, err
	case tree.Datum:
		datumType := t.ResolvedType()
		resultIdx = len(columnTypes)
		typs = appendOneType(columnTypes, datumType)
		if datumType.Family() == types.UnknownFamily {
			// We handle Unknown type by planning a special constant null
			// operator.
			op = colexec.NewConstNullOp(colmem.NewAllocator(ctx, acc, factory), input, resultIdx)
			return op, resultIdx, typs, internalMemUsed, nil
		}
		constVal, err := colexec.GetDatumToPhysicalFn(datumType)(t)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		op, err := colexec.NewConstOp(colmem.NewAllocator(ctx, acc, factory), input, datumType, constVal, resultIdx)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		return op, resultIdx, typs, internalMemUsed, nil
	case *tree.CaseExpr:
		if t.Expr != nil {
			return nil, resultIdx, typs, internalMemUsed, errors.New("CASE <expr> WHEN expressions unsupported")
		}

		allocator := colmem.NewAllocator(ctx, acc, factory)
		caseOutputType := t.ResolvedType()
		if typeconv.TypeFamilyToCanonicalTypeFamily(caseOutputType.Family()) == types.BytesFamily {
			// Currently, there is a contradiction between the way CASE operator
			// works (which populates its output in arbitrary order) and the flat
			// bytes implementation of Bytes type (which prohibits sets in arbitrary
			// order), so we reject such scenario to fall back to row-by-row engine.
			return nil, resultIdx, typs, internalMemUsed, errors.Newf(
				"unsupported type %s in CASE operator", caseOutputType)
		}
		caseOutputIdx := len(columnTypes)
		// We don't know the schema yet and will update it below, right before
		// instantiating caseOp. The same goes for subsetEndIdx.
		schemaEnforcer := colexec.NewBatchSchemaSubsetEnforcer(
			allocator, input, nil /* typs */, caseOutputIdx, -1, /* subsetEndIdx */
		)
		buffer := colexec.NewBufferOp(schemaEnforcer)
		caseOps := make([]colexecbase.Operator, len(t.Whens))
		typs = appendOneType(columnTypes, caseOutputType)
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
			var whenInternalMemUsed, thenInternalMemUsed int
			caseOps[i], resultIdx, typs, whenInternalMemUsed, err = planProjectionOperators(
				ctx, evalCtx, whenTyped, typs, buffer, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, typs, internalMemUsed, err
			}
			caseOps[i], err = colexec.BoolOrUnknownToSelOp(caseOps[i], typs, resultIdx)
			if err != nil {
				return nil, resultIdx, typs, internalMemUsed, err
			}

			// Run the "then" clause on those tuples that were selected.
			caseOps[i], thenIdxs[i], typs, thenInternalMemUsed, err = planProjectionOperators(
				ctx, evalCtx, when.Val.(tree.TypedExpr), typs, caseOps[i], acc, factory,
			)
			if err != nil {
				return nil, resultIdx, typs, internalMemUsed, err
			}
			internalMemUsed += whenInternalMemUsed + thenInternalMemUsed
			if !typs[thenIdxs[i]].Identical(typs[caseOutputIdx]) {
				// It is possible that the projection of this THEN arm has different
				// column type (for example, we expect INT2, but INT8 is given). In
				// such case, we need to plan a cast.
				fromType, toType := typs[thenIdxs[i]], typs[caseOutputIdx]
				caseOps[i], thenIdxs[i], typs, err = planCastOperator(
					ctx, acc, typs, caseOps[i], thenIdxs[i], fromType, toType, factory,
				)
				if err != nil {
					return nil, resultIdx, typs, internalMemUsed, err
				}
			}
		}
		var elseInternalMemUsed int
		var elseOp colexecbase.Operator
		elseExpr := t.Else
		if elseExpr == nil {
			// If there's no ELSE arm, we write NULLs.
			elseExpr = tree.DNull
		}
		elseOp, thenIdxs[len(t.Whens)], typs, elseInternalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, elseExpr.(tree.TypedExpr), typs, buffer, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		internalMemUsed += elseInternalMemUsed
		if !typs[thenIdxs[len(t.Whens)]].Identical(typs[caseOutputIdx]) {
			// It is possible that the projection of the ELSE arm has different
			// column type (for example, we expect INT2, but INT8 is given). In
			// such case, we need to plan a cast.
			elseIdx := thenIdxs[len(t.Whens)]
			fromType, toType := typs[elseIdx], typs[caseOutputIdx]
			elseOp, thenIdxs[len(t.Whens)], typs, err = planCastOperator(
				ctx, acc, typs, elseOp, elseIdx, fromType, toType, factory,
			)
			if err != nil {
				return nil, resultIdx, typs, internalMemUsed, err
			}
		}

		schemaEnforcer.SetTypes(typs)
		op := colexec.NewCaseOp(allocator, buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)
		internalMemUsed += op.(colexec.InternalMemoryOperator).InternalMemoryUsage()
		return op, caseOutputIdx, typs, internalMemUsed, err
	case *tree.AndExpr, *tree.OrExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, acc, factory)
	default:
		return nil, resultIdx, nil, internalMemUsed, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func checkSupportedProjectionExpr(left, right tree.TypedExpr) error {
	leftTyp := left.ResolvedType()
	rightTyp := right.ResolvedType()
	if leftTyp.Equivalent(rightTyp) {
		return nil
	}

	// The types are not equivalent. Check if either is a type we'd like to avoid.
	for _, t := range []*types.T{leftTyp, rightTyp} {
		switch t.Family() {
		case types.DateFamily, types.TimestampFamily, types.TimestampTZFamily:
			return errors.New("dates and timestamp(tz) not supported in mixed-type expressions in the vectorized engine")
		}
	}
	return nil
}

func checkSupportedBinaryExpr(left, right tree.TypedExpr, outputType *types.T) error {
	leftDatumBacked := typeconv.TypeFamilyToCanonicalTypeFamily(left.ResolvedType().Family()) == typeconv.DatumVecCanonicalTypeFamily
	rightDatumBacked := typeconv.TypeFamilyToCanonicalTypeFamily(right.ResolvedType().Family()) == typeconv.DatumVecCanonicalTypeFamily
	outputDatumBacked := typeconv.TypeFamilyToCanonicalTypeFamily(outputType.Family()) == typeconv.DatumVecCanonicalTypeFamily
	if (leftDatumBacked || rightDatumBacked) && !outputDatumBacked {
		return errors.New("datum-backed arguments and not datum-backed " +
			"output of a binary expression is currently not supported")
	}
	return nil
}

func planProjectionExpr(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	projOp tree.Operator,
	outputType *types.T,
	left, right tree.TypedExpr,
	columnTypes []*types.T,
	input colexecbase.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
	binFn *tree.BinOp,
) (op colexecbase.Operator, resultIdx int, typs []*types.T, internalMemUsed int, err error) {
	if err := checkSupportedProjectionExpr(left, right); err != nil {
		return nil, resultIdx, typs, internalMemUsed, err
	}
	resultIdx = -1
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	if lConstArg, lConst := left.(tree.Datum); lConst {
		// Case one: The left is constant.
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for non-commutative
		// operators such as - and /, though, so we still need this case.
		var rightIdx int
		input, rightIdx, typs, internalMemUsed, err = planProjectionOperators(
			ctx, evalCtx, right, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		resultIdx = len(typs)
		// The projection result will be outputted to a new column which is appended
		// to the input batch.
		op, err = colexec.GetProjectionLConstOperator(
			colmem.NewAllocator(ctx, acc, factory), left.ResolvedType(), typs[rightIdx], outputType,
			projOp, input, rightIdx, lConstArg, resultIdx, binFn, evalCtx,
		)
	} else {
		var (
			leftIdx             int
			internalMemUsedLeft int
		)
		input, leftIdx, typs, internalMemUsedLeft, err = planProjectionOperators(
			ctx, evalCtx, left, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, internalMemUsed, err
		}
		internalMemUsed += internalMemUsedLeft
		if rConstArg, rConst := right.(tree.Datum); rConst {
			// Case 2: The right is constant.
			// The projection result will be outputted to a new column which is appended
			// to the input batch.
			resultIdx = len(typs)
			if projOp == tree.Like || projOp == tree.NotLike {
				negate := projOp == tree.NotLike
				op, err = colexec.GetLikeProjectionOperator(
					colmem.NewAllocator(ctx, acc, factory), evalCtx, input, leftIdx, resultIdx,
					string(tree.MustBeDString(rConstArg)), negate,
				)
			} else if projOp == tree.In || projOp == tree.NotIn {
				negate := projOp == tree.NotIn
				datumTuple, ok := tree.AsDTuple(rConstArg)
				if !ok {
					err = errors.Errorf("IN operator supported only on constant expressions")
					return nil, resultIdx, typs, internalMemUsed, err
				}
				op, err = colexec.GetInProjectionOperator(
					colmem.NewAllocator(ctx, acc, factory), typs[leftIdx], input, leftIdx,
					resultIdx, datumTuple, negate,
				)
			} else if projOp == tree.IsDistinctFrom || projOp == tree.IsNotDistinctFrom {
				if right != tree.DNull {
					err = errors.Errorf("IS DISTINCT FROM and IS NOT DISTINCT FROM are supported only with NULL argument")
					return nil, resultIdx, typs, internalMemUsed, err
				}
				// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we want to
				// negate when IS DISTINCT FROM is used.
				negate := projOp == tree.IsDistinctFrom
				op = colexec.NewIsNullProjOp(colmem.NewAllocator(ctx, acc, factory), input, leftIdx, resultIdx, negate)
			} else {
				op, err = colexec.GetProjectionRConstOperator(
					colmem.NewAllocator(ctx, acc, factory), typs[leftIdx], right.ResolvedType(), outputType,
					projOp, input, leftIdx, rConstArg, resultIdx, binFn, evalCtx,
				)
			}
		} else {
			// Case 3: neither are constant.
			var (
				rightIdx             int
				internalMemUsedRight int
			)
			input, rightIdx, typs, internalMemUsedRight, err = planProjectionOperators(
				ctx, evalCtx, right, typs, input, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, nil, internalMemUsed, err
			}
			internalMemUsed += internalMemUsedRight
			resultIdx = len(typs)
			op, err = colexec.GetProjectionOperator(
				colmem.NewAllocator(ctx, acc, factory), typs[leftIdx], typs[rightIdx], outputType,
				projOp, input, leftIdx, rightIdx, resultIdx, binFn, evalCtx,
			)
		}
	}
	if err != nil {
		return op, resultIdx, typs, internalMemUsed, err
	}
	if sMem, ok := op.(colexec.InternalMemoryOperator); ok {
		internalMemUsed += sMem.InternalMemoryUsage()
	}
	typs = appendOneType(typs, outputType)
	return op, resultIdx, typs, internalMemUsed, err
}

// planLogicalProjectionOp plans all the needed operators for a projection of
// a logical operation (either AND or OR).
func planLogicalProjectionOp(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecbase.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
) (op colexecbase.Operator, resultIdx int, typs []*types.T, internalMemUsed int, err error) {
	// Add a new boolean column that will store the result of the projection.
	resultIdx = len(columnTypes)
	typs = appendOneType(columnTypes, types.Bool)
	var (
		typedLeft, typedRight                       tree.TypedExpr
		leftProjOpChain, rightProjOpChain, outputOp colexecbase.Operator
		leftIdx, rightIdx                           int
		internalMemUsedLeft, internalMemUsedRight   int
	)
	leftFeedOp := colexec.NewFeedOperator()
	rightFeedOp := colexec.NewFeedOperator()
	switch t := expr.(type) {
	case *tree.AndExpr:
		typedLeft = t.TypedLeft()
		typedRight = t.TypedRight()
	case *tree.OrExpr:
		typedLeft = t.TypedLeft()
		typedRight = t.TypedRight()
	default:
		colexecerror.InternalError(fmt.Sprintf("unexpected logical expression type %s", t.String()))
	}
	leftProjOpChain, leftIdx, typs, internalMemUsedLeft, err = planProjectionOperators(
		ctx, evalCtx, typedLeft, typs, leftFeedOp, acc, factory,
	)
	if err != nil {
		return nil, resultIdx, typs, internalMemUsed, err
	}
	rightProjOpChain, rightIdx, typs, internalMemUsedRight, err = planProjectionOperators(
		ctx, evalCtx, typedRight, typs, rightFeedOp, acc, factory,
	)
	if err != nil {
		return nil, resultIdx, typs, internalMemUsed, err
	}
	allocator := colmem.NewAllocator(ctx, acc, factory)
	input = colexec.NewBatchSchemaSubsetEnforcer(allocator, input, typs, resultIdx, len(typs))
	switch expr.(type) {
	case *tree.AndExpr:
		outputOp = colexec.NewAndProjOp(
			allocator,
			input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	case *tree.OrExpr:
		outputOp = colexec.NewOrProjOp(
			allocator,
			input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp,
			leftIdx, rightIdx, resultIdx,
		)
	}
	return outputOp, resultIdx, typs, internalMemUsedLeft + internalMemUsedRight, nil
}

// planIsNullProjectionOp plans the operator for IS NULL and IS NOT NULL
// expressions (tree.IsNullExpr and tree.IsNotNullExpr, respectively).
func planIsNullProjectionOp(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	outputType *types.T,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecbase.Operator,
	acc *mon.BoundAccount,
	negate bool,
	factory coldata.ColumnFactory,
) (op colexecbase.Operator, resultIdx int, typs []*types.T, internalMemUsed int, err error) {
	op, resultIdx, typs, internalMemUsed, err = planProjectionOperators(
		ctx, evalCtx, expr, columnTypes, input, acc, factory,
	)
	outputIdx := len(typs)
	op = colexec.NewIsNullProjOp(colmem.NewAllocator(ctx, acc, factory), op, resultIdx, outputIdx, negate)
	typs = appendOneType(typs, outputType)
	return op, outputIdx, typs, internalMemUsed, err
}

// appendOneType appends a *types.T to then end of a []*types.T. The size of the
// underlying array of the resulting slice is 1 greater than the input slice.
// This differs from the built-in append function, which can double the capacity
// of the slice if its length is less than 1024, or increase by 25% otherwise.
func appendOneType(typs []*types.T, t *types.T) []*types.T {
	newTyps := make([]*types.T, len(typs)+1)
	copy(newTyps, typs)
	newTyps[len(newTyps)-1] = t
	return newTyps
}
