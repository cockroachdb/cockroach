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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecproj"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecsel"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecwindow"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colfetcher"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func checkNumIn(inputs []colexecop.Operator, numIn int) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// wrapRowSources, given input Operators, integrates toWrap into a columnar
// execution flow and returns toWrap's output as an Operator.
// - materializerSafeToRelease indicates whether the materializers created in
// order to row-sourcify the inputs are safe to be released on the flow cleanup.
func wrapRowSources(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	inputs []colexecop.Operator,
	inputTypes [][]*types.T,
	args *colexecargs.NewColOperatorArgs,
	newToWrap func([]execinfra.RowSource) (execinfra.RowSource, error),
	materializerSafeToRelease bool,
	factory coldata.ColumnFactory,
) (*colexec.Columnarizer, []execinfra.Releasable, error) {
	var toWrapInputs []execinfra.RowSource
	var releasables []execinfra.Releasable
	for i, input := range inputs {
		// Optimization: if the input is a Columnarizer, its input is
		// necessarily a execinfra.RowSource, so remove the unnecessary
		// conversion.
		if c, ok := input.(*colexec.Columnarizer); ok {
			// Since this Columnarizer has been previously added to Closers and
			// MetadataSources, this call ensures that all future calls are noops.
			// Modifying the slices at this stage is difficult.
			c.MarkAsRemovedFromFlow()
			toWrapInputs = append(toWrapInputs, c.Input())
		} else {
			var metadataSources colexecop.MetadataSources
			if len(args.MetadataSources) > i {
				// In some testing paths, MetadataSources might be left unset,
				// so we check whether the slice has ith element. In the
				// production setting though the length of Inputs is always the
				// same as the length of MetadataSources.
				metadataSources = args.MetadataSources[i]
				// We pass on the responsibility of draining metadata sources to
				// the materializer.
				args.MetadataSources[i] = nil
			}
			toWrapInput, err := colexec.NewMaterializer(
				flowCtx,
				args.Spec.ProcessorID,
				input,
				inputTypes[i],
				nil, /* output */
				nil, /* getStats */
				metadataSources,
				nil, /* toClose */
				nil, /* cancelFlow */
			)
			if err != nil {
				return nil, releasables, err
			}
			toWrapInputs = append(toWrapInputs, toWrapInput)
			if materializerSafeToRelease {
				releasables = append(releasables, toWrapInput)
			}
		}
	}

	toWrap, err := newToWrap(toWrapInputs)
	if err != nil {
		return nil, releasables, err
	}

	var c *colexec.Columnarizer
	if _, mustBeStreaming := toWrap.(execinfra.StreamingProcessor); mustBeStreaming {
		c, err = colexec.NewStreamingColumnarizer(
			ctx, colmem.NewAllocator(ctx, args.StreamingMemAccount, factory), flowCtx, args.Spec.ProcessorID, toWrap,
		)
	} else {
		c, err = colexec.NewBufferingColumnarizer(
			ctx, colmem.NewAllocator(ctx, args.StreamingMemAccount, factory), flowCtx, args.Spec.ProcessorID, toWrap,
		)
	}
	return c, releasables, err
}

type opResult struct {
	*colexecargs.NewColOperatorResult
}

// resetToState resets r to the state specified in arg. arg may be a shallow
// copy made at a given point in time.
func (r *opResult) resetToState(ctx context.Context, arg colexecargs.NewColOperatorResult) {
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

func needHashAggregator(aggSpec *execinfrapb.AggregatorSpec) (bool, error) {
	var groupCols, orderedCols util.FastIntSet
	for _, col := range aggSpec.OrderedGroupCols {
		orderedCols.Add(int(col))
	}
	for _, col := range aggSpec.GroupCols {
		if !orderedCols.Contains(int(col)) {
			return true, nil
		}
		groupCols.Add(int(col))
	}
	if !orderedCols.SubsetOf(groupCols) {
		return false, errors.AssertionFailedf("ordered cols must be a subset of grouping cols")
	}
	return false, nil
}

// IsSupported returns an error if the given spec is not supported by the
// vectorized engine (neither natively nor by wrapping the corresponding row
// execution processor).
func IsSupported(mode sessiondatapb.VectorizeExecMode, spec *execinfrapb.ProcessorSpec) error {
	err := supportedNatively(spec)
	if err != nil {
		if wrapErr := canWrap(mode, spec); wrapErr == nil {
			// We don't support this spec natively, but we can wrap the row
			// execution processor.
			return nil
		}
	}
	return err
}

// supportedNatively checks whether we have a columnar operator equivalent to a
// processor described by spec. Note that it doesn't perform any other checks
// (like validity of the number of inputs).
func supportedNatively(spec *execinfrapb.ProcessorSpec) error {
	switch {
	case spec.Core.Noop != nil:
		return nil

	case spec.Core.Values != nil:
		if spec.Core.Values.NumRows != 0 && len(spec.Core.Values.Columns) != 0 {
			return errors.Newf("values core is supported only with zero rows or zero columns")
		}
		return nil

	case spec.Core.TableReader != nil:
		if spec.Core.TableReader.IsCheck {
			return errors.Newf("scrub table reader is unsupported in vectorized")
		}
		return nil

	case spec.Core.Filterer != nil:
		return nil

	case spec.Core.Aggregator != nil:
		for _, agg := range spec.Core.Aggregator.Aggregations {
			if agg.FilterColIdx != nil {
				return errors.Newf("filtering aggregation not supported")
			}
		}
		return nil

	case spec.Core.Distinct != nil:
		if spec.Core.Distinct.NullsAreDistinct {
			return errors.Newf("distinct with unique nulls not supported")
		}
		if spec.Core.Distinct.ErrorOnDup != "" {
			return errors.Newf("distinct with error on duplicates not supported")
		}
		return nil

	case spec.Core.Ordinality != nil:
		return nil

	case spec.Core.HashJoiner != nil:
		if !spec.Core.HashJoiner.OnExpr.Empty() && spec.Core.HashJoiner.Type != descpb.InnerJoin {
			return errors.Newf("can't plan vectorized non-inner hash joins with ON expressions")
		}
		return nil

	case spec.Core.MergeJoiner != nil:
		if !spec.Core.MergeJoiner.OnExpr.Empty() && spec.Core.MergeJoiner.Type != descpb.InnerJoin {
			return errors.Errorf("can't plan non-inner merge join with ON expressions")
		}
		return nil

	case spec.Core.Sorter != nil:
		return nil

	case spec.Core.Windower != nil:
		for _, wf := range spec.Core.Windower.WindowFns {
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

			if _, supported := colexecwindow.SupportedWindowFns[*wf.Func.WindowFunc]; !supported {
				return errors.Newf("window function %s is not supported", wf.String())
			}
		}
		return nil

	default:
		return errCoreUnsupportedNatively
	}
}

var (
	errCoreUnsupportedNatively        = errors.New("unsupported processor core")
	errMetadataTestSenderWrap         = errors.New("core.MetadataTestSender is not supported")
	errMetadataTestReceiverWrap       = errors.New("core.MetadataTestReceiver is not supported")
	errChangeAggregatorWrap           = errors.New("core.ChangeAggregator is not supported")
	errChangeFrontierWrap             = errors.New("core.ChangeFrontier is not supported")
	errBackfillerWrap                 = errors.New("core.Backfiller is not supported (not an execinfra.RowSource)")
	errCSVWriterWrap                  = errors.New("core.CSVWriter is not supported (not an execinfra.RowSource)")
	errSamplerWrap                    = errors.New("core.Sampler is not supported (not an execinfra.RowSource)")
	errSampleAggregatorWrap           = errors.New("core.SampleAggregator is not supported (not an execinfra.RowSource)")
	errExperimentalWrappingProhibited = errors.New("wrapping for non-JoinReader and non-LocalPlanNode cores is prohibited in vectorize=experimental_always")
)

func canWrap(mode sessiondatapb.VectorizeExecMode, spec *execinfrapb.ProcessorSpec) error {
	if mode == sessiondatapb.VectorizeExperimentalAlways && spec.Core.JoinReader == nil && spec.Core.LocalPlanNode == nil {
		return errExperimentalWrappingProhibited
	}
	switch {
	case spec.Core.Noop != nil:
	case spec.Core.TableReader != nil:
	case spec.Core.JoinReader != nil:
	case spec.Core.Sorter != nil:
	case spec.Core.Aggregator != nil:
	case spec.Core.Distinct != nil:
	case spec.Core.MergeJoiner != nil:
	case spec.Core.HashJoiner != nil:
	case spec.Core.Values != nil:
	case spec.Core.Backfiller != nil:
		return errBackfillerWrap
	case spec.Core.ReadImport != nil:
	case spec.Core.CSVWriter != nil:
		return errCSVWriterWrap
	case spec.Core.Sampler != nil:
		return errSamplerWrap
	case spec.Core.SampleAggregator != nil:
		return errSampleAggregatorWrap
	case spec.Core.MetadataTestSender != nil:
		// We do not wrap MetadataTestSender because of the way metadata is
		// propagated through the vectorized flow - it is drained at the flow
		// shutdown unlike these test processors expect.
		return errMetadataTestSenderWrap
	case spec.Core.MetadataTestReceiver != nil:
		// We do not wrap MetadataTestReceiver because of the way metadata is
		// propagated through the vectorized flow - it is drained at the flow
		// shutdown unlike these test processors expect.
		return errMetadataTestReceiverWrap
	case spec.Core.ZigzagJoiner != nil:
	case spec.Core.ProjectSet != nil:
	case spec.Core.Windower != nil:
	case spec.Core.LocalPlanNode != nil:
	case spec.Core.ChangeAggregator != nil:
		// Currently, there is an issue with cleaning up the changefeed flows
		// (#55408), so we fallback to the row-by-row engine.
		return errChangeAggregatorWrap
	case spec.Core.ChangeFrontier != nil:
		// Currently, there is an issue with cleaning up the changefeed flows
		// (#55408), so we fallback to the row-by-row engine.
		return errChangeFrontierWrap
	case spec.Core.Ordinality != nil:
	case spec.Core.BulkRowWriter != nil:
	case spec.Core.InvertedFilterer != nil:
	case spec.Core.InvertedJoiner != nil:
	case spec.Core.BackupData != nil:
	case spec.Core.SplitAndScatter != nil:
	case spec.Core.RestoreData != nil:
	case spec.Core.Filterer != nil:
	case spec.Core.StreamIngestionData != nil:
	case spec.Core.StreamIngestionFrontier != nil:
	default:
		return errors.AssertionFailedf("unexpected processor core %q", spec.Core)
	}
	return nil
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
	args *colexecargs.NewColOperatorArgs,
	input colexecop.Operator,
	inputTypes []*types.T,
	ordering execinfrapb.Ordering,
	matchLen uint32,
	maxNumberPartitions int,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
	memMonitorNamePrefix string,
	factory coldata.ColumnFactory,
) (colexecop.Operator, error) {
	streamingMemAccount := args.StreamingMemAccount
	useStreamingMemAccountForBuffering := args.TestingKnobs.UseStreamingMemAccountForBuffering
	var (
		sorterMemMonitorName string
		inMemorySorter       colexecop.Operator
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
	} else if post.Limit != 0 && post.Limit < math.MaxUint64-post.Offset {
		// There is a limit specified, so we know exactly how many rows the
		// sorter should output. The last part of the condition is making sure
		// there is no overflow.
		//
		// Choose a top K sorter, which uses a heap to avoid storing more rows
		// than necessary.
		//
		// TODO(radu): we should not choose this processor when K is very large
		// - it is slower unless we get significantly more rows than the limit.
		sorterMemMonitorName = fmt.Sprintf("%stopk-sort-%d", memMonitorNamePrefix, processorID)
		var topKSorterMemAccount *mon.BoundAccount
		if useStreamingMemAccountForBuffering {
			topKSorterMemAccount = streamingMemAccount
		} else {
			topKSorterMemAccount = r.createMemAccountForSpillStrategy(
				ctx, flowCtx, sorterMemMonitorName,
			)
		}
		k := post.Limit + post.Offset
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
		input, inMemorySorter.(colexecop.BufferingInMemoryOperator),
		sorterMemMonitorName,
		func(input colexecop.Operator) colexecop.Operator {
			monitorNamePrefix := fmt.Sprintf("%sexternal-sorter", memMonitorNamePrefix)
			// We are using unlimited memory monitors here because external
			// sort itself is responsible for making sure that we stay within
			// the memory limit.
			sortUnlimitedAllocator := colmem.NewAllocator(
				ctx, r.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorNamePrefix+"-sort",
				), factory)
			mergeUnlimitedAllocator := colmem.NewAllocator(
				ctx, r.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorNamePrefix+"-merge",
				), factory)
			outputUnlimitedAllocator := colmem.NewAllocator(
				ctx, r.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorNamePrefix+"-output",
				), factory)
			diskAccount := r.createDiskAccount(ctx, flowCtx, monitorNamePrefix)
			es := colexec.NewExternalSorter(
				sortUnlimitedAllocator,
				mergeUnlimitedAllocator,
				outputUnlimitedAllocator,
				input, inputTypes, ordering,
				execinfra.GetWorkMemLimit(flowCtx.Cfg),
				maxNumberPartitions,
				args.TestingKnobs.NumForcedRepartitions,
				args.TestingKnobs.DelegateFDAcquisitions,
				args.DiskQueueCfg,
				args.FDSemaphore,
				diskAccount,
			)
			r.ToClose = append(r.ToClose, es.(colexecop.Closer))
			return es
		},
		args.TestingKnobs.SpillingCallbackFn,
	), nil
}

// makeDistBackedSorterConstructors creates a DiskBackedSorterConstructor that
// can be used by the hash-based partitioner.
// NOTE: unless DelegateFDAcquisitions testing knob is set to true, it is up to
// the caller to acquire the necessary file descriptors up front.
func (r opResult) makeDiskBackedSorterConstructor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	monitorNamePrefix string,
	factory coldata.ColumnFactory,
) colexec.DiskBackedSorterConstructor {
	return func(input colexecop.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column, maxNumberPartitions int) colexecop.Operator {
		if maxNumberPartitions < colexecop.ExternalSorterMinPartitions {
			colexecerror.InternalError(errors.AssertionFailedf(
				"external sorter is attempted to be created with %d partitions, minimum %d required",
				maxNumberPartitions, colexecop.ExternalSorterMinPartitions,
			))
		}
		sortArgs := *args
		if !args.TestingKnobs.DelegateFDAcquisitions {
			// Set the FDSemaphore to nil. This indicates that no FDs should be
			// acquired. The hash-based partitioner will do this up front.
			sortArgs.FDSemaphore = nil
		}
		sorter, err := r.createDiskBackedSort(
			ctx, flowCtx, &sortArgs, input, inputTypes,
			execinfrapb.Ordering{Columns: orderingCols},
			0 /* matchLen */, maxNumberPartitions, args.Spec.ProcessorID,
			&execinfrapb.PostProcessSpec{}, monitorNamePrefix+"-", factory,
		)
		if err != nil {
			colexecerror.InternalError(err)
		}
		return sorter
	}
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
// - causeToWrap is an error that prompted us to wrap a processor core into the
// vectorized plan (for example, it could be an unsupported processor core, an
// unsupported function, etc).
func (r opResult) createAndWrapRowSource(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	inputs []colexecop.Operator,
	inputTypes [][]*types.T,
	spec *execinfrapb.ProcessorSpec,
	factory coldata.ColumnFactory,
	causeToWrap error,
) error {
	if args.ProcessorConstructor == nil {
		return errors.New("processorConstructor is nil")
	}
	if err := canWrap(flowCtx.EvalCtx.SessionData.VectorizeMode, spec); err != nil {
		return causeToWrap
	}
	// Note that the materializers aren't safe to release in all cases since in
	// some cases they could be released before being closed. Namely, this would
	// occur if we have a subquery with LocalPlanNode core and a materializer is
	// added in order to wrap that core - what will happen is that all
	// releasables are put back into their pools upon the subquery's flow
	// cleanup, yet the subquery planNode tree isn't closed yet since its
	// closure is done when the main planNode tree is being closed.
	materializerSafeToRelease := spec.Core.LocalPlanNode == nil
	c, releasables, err := wrapRowSources(
		ctx,
		flowCtx,
		inputs,
		inputTypes,
		args,
		func(inputs []execinfra.RowSource) (execinfra.RowSource, error) {
			// We provide a slice with a single nil as 'outputs' parameter
			// because all processors expect a single output. Passing nil is ok
			// here because when wrapping the processor, the materializer will
			// be its output, and it will be set up in wrapRowSources.
			proc, err := args.ProcessorConstructor(
				ctx, flowCtx, spec.ProcessorID, &spec.Core, &spec.Post, inputs,
				[]execinfra.RowReceiver{nil} /* outputs */, args.LocalProcessors,
			)
			if err != nil {
				return nil, err
			}
			var (
				rs execinfra.RowSource
				ok bool
			)
			if rs, ok = proc.(execinfra.RowSource); !ok {
				return nil, errors.AssertionFailedf(
					"processor %s is not an execinfra.RowSource", spec.Core.String(),
				)
			}
			r.ColumnTypes = rs.OutputTypes()
			return rs, nil
		},
		materializerSafeToRelease,
		factory,
	)
	if err != nil {
		return err
	}
	r.Op = c
	if args.TestingKnobs.PlanInvariantsCheckers {
		r.Op = colexec.NewInvariantsChecker(r.Op)
	}
	r.MetadataSources = append(r.MetadataSources, r.Op.(colexecop.MetadataSource))
	r.ToClose = append(r.ToClose, c)
	r.Releasables = append(r.Releasables, releasables...)
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
	ctx context.Context, flowCtx *execinfra.FlowCtx, args *colexecargs.NewColOperatorArgs,
) (_ *colexecargs.NewColOperatorResult, err error) {
	result := opResult{NewColOperatorResult: colexecargs.GetNewColOperatorResult()}
	r := result.NewColOperatorResult
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
			colexecerror.InternalError(logcrash.PanicAsError(0, panicErr))
		}
	}()
	spec := args.Spec
	inputs := args.Inputs
	evalCtx := flowCtx.NewEvalCtx()
	factory := args.Factory
	if factory == nil {
		factory = coldataext.NewExtendedColumnFactory(evalCtx)
	}
	streamingMemAccount := args.StreamingMemAccount
	streamingAllocator := colmem.NewAllocator(ctx, streamingMemAccount, factory)
	useStreamingMemAccountForBuffering := args.TestingKnobs.UseStreamingMemAccountForBuffering
	if args.ExprHelper == nil {
		args.ExprHelper = colexecargs.NewExprHelper()
	}

	if log.V(2) {
		log.Infof(ctx, "planning col operator for spec %q", spec)
	}

	core := &spec.Core
	post := &spec.Post

	// resultPreSpecPlanningStateShallowCopy is a shallow copy of the result
	// before any specs are planned. Used if there is a need to backtrack.
	resultPreSpecPlanningStateShallowCopy := *r

	if err = supportedNatively(spec); err != nil {
		if err := canWrap(flowCtx.EvalCtx.SessionData.VectorizeMode, spec); err != nil {
			return r, err
		}
		if log.V(1) {
			log.Infof(ctx, "planning a wrapped processor because %s", err.Error())
		}

		inputTypes := make([][]*types.T, len(spec.Input))
		for inputIdx, input := range spec.Input {
			inputTypes[inputIdx] = make([]*types.T, len(input.ColumnTypes))
			copy(inputTypes[inputIdx], input.ColumnTypes)
		}

		err = result.createAndWrapRowSource(ctx, flowCtx, args, inputs, inputTypes, spec, factory, err)
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
			result.Op = colexecop.NewNoop(inputs[0])
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)

		case core.Values != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}
			if core.Values.NumRows != 0 && len(core.Values.Columns) != 0 {
				return r, errors.AssertionFailedf(
					"values core is supported only with zero rows or zero columns, %d rows, %d columns given",
					core.Values.NumRows, len(core.Values.Columns),
				)
			}
			result.Op = colexecutils.NewFixedNumTuplesNoInputOp(streamingAllocator, int(core.Values.NumRows), nil /* opToInitialize */)
			result.ColumnTypes = make([]*types.T, len(core.Values.Columns))
			for i, col := range core.Values.Columns {
				result.ColumnTypes[i] = col.Type
			}

		case core.TableReader != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}

			estimatedRowCount := spec.EstimatedRowCount
			scanOp, err := colfetcher.NewColBatchScan(
				ctx, streamingAllocator, flowCtx, evalCtx, core.TableReader, post, estimatedRowCount,
			)
			if err != nil {
				return r, err
			}
			// colBatchScan is wrapped with a cancel checker below, so we need
			// to log its creation separately.
			if log.V(1) {
				log.Infof(ctx, "made op %T\n", scanOp)
			}
			result.Op = scanOp
			if args.TestingKnobs.PlanInvariantsCheckers {
				result.Op = colexec.NewInvariantsChecker(result.Op)
			}
			result.KVReader = scanOp
			result.MetadataSources = append(result.MetadataSources, result.Op.(colexecop.MetadataSource))
			result.Releasables = append(result.Releasables, scanOp)

			// We want to check for cancellation once per input batch, and
			// wrapping only colBatchScan with a CancelChecker allows us to do
			// just that. It's sufficient for most of the operators since they
			// are extremely fast. However, some of the long-running operators
			// (for example, sorter) are still responsible for doing the
			// cancellation check on their own while performing long operations.
			result.Op = colexecutils.NewCancelChecker(result.Op)
			result.ColumnTypes = scanOp.ResultTypes
			result.ToClose = append(result.ToClose, scanOp)

		case core.Filterer != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}

			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			result.Op = inputs[0]
			if err := result.planAndMaybeWrapFilter(
				ctx, flowCtx, evalCtx, args, spec.ProcessorID, core.Filterer.Filter, factory,
			); err != nil {
				return r, err
			}

		case core.Aggregator != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			aggSpec := core.Aggregator
			if len(aggSpec.Aggregations) == 0 {
				// We can get an aggregator when no aggregate functions are
				// present if HAVING clause is present, for example, with a
				// query as follows: SELECT 1 FROM t HAVING true. In this case,
				// we plan a special operator that outputs a batch of length 1
				// without actual columns once and then zero-length batches. The
				// actual "data" will be added by projections below.
				// TODO(solon): The distsql plan for this case includes a
				// TableReader, so we end up creating an orphaned colBatchScan.
				// We should avoid that. Ideally the optimizer would not plan a
				// scan in this unusual case.
				result.Op, err = colexecutils.NewFixedNumTuplesNoInputOp(streamingAllocator, 1 /* numTuples */, inputs[0]), nil
				// We make ColumnTypes non-nil so that sanity check doesn't
				// panic.
				result.ColumnTypes = []*types.T{}
				break
			}
			if aggSpec.IsRowCount() {
				result.Op, err = colexec.NewCountOp(streamingAllocator, inputs[0]), nil
				result.ColumnTypes = []*types.T{types.Int}
				break
			}

			var needHash bool
			needHash, err = needHashAggregator(aggSpec)
			if err != nil {
				return r, err
			}
			inputTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(inputTypes, spec.Input[0].ColumnTypes)
			newAggArgs := &colexecagg.NewAggregatorArgs{
				Input:      inputs[0],
				InputTypes: inputTypes,
				Spec:       aggSpec,
				EvalCtx:    evalCtx,
			}
			semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(evalCtx.Txn)
			newAggArgs.Constructors, newAggArgs.ConstArguments, newAggArgs.OutputTypes, err = colexecagg.ProcessAggregations(
				evalCtx, semaCtx, aggSpec.Aggregations, inputTypes,
			)
			if err != nil {
				return r, err
			}
			result.ColumnTypes = newAggArgs.OutputTypes

			if needHash {
				// We have separate unit tests that instantiate the in-memory
				// hash aggregators, so we don't need to look at
				// args.TestingKnobs.DiskSpillingDisabled and always instantiate
				// a disk-backed one here.
				hashAggregatorMemMonitorName := fmt.Sprintf("hash-aggregator-%d", spec.ProcessorID)
				diskSpillingDisabled := !colexec.HashAggregationDiskSpillingEnabled.Get(&flowCtx.Cfg.Settings.SV)
				if diskSpillingDisabled {
					// The disk spilling is disabled by the cluster setting, so
					// we give an unlimited memory account to the in-memory
					// hash aggregator and don't set up the disk spiller.
					hashAggregatorUnlimitedMemAccount := result.createBufferingUnlimitedMemAccount(
						ctx, flowCtx, hashAggregatorMemMonitorName,
					)
					newAggArgs.Allocator = colmem.NewAllocator(
						ctx, hashAggregatorUnlimitedMemAccount, factory,
					)
					newAggArgs.MemAccount = hashAggregatorUnlimitedMemAccount
					evalCtx.SingleDatumAggMemAccount = hashAggregatorUnlimitedMemAccount
					// The second argument is nil because we disable the
					// tracking of the input tuples.
					result.Op, err = colexec.NewHashAggregator(newAggArgs, nil /* newSpillingQueueArgs */)
				} else {
					// We will divide the available memory equally between the
					// two usages - the hash aggregation itself and the input
					// tuples tracking.
					totalMemLimit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
					hashAggregatorMemAccount := result.createMemAccountForSpillStrategyWithLimit(
						ctx, flowCtx, hashAggregatorMemMonitorName, totalMemLimit/2,
					)
					spillingQueueMemMonitorName := hashAggregatorMemMonitorName + "-spilling-queue"
					// We need to create a separate memory account for the
					// spilling queue because it looks at how much memory it has
					// already used in order to decide when to spill to disk.
					spillingQueueMemAccount := result.createBufferingUnlimitedMemAccount(ctx, flowCtx, spillingQueueMemMonitorName)
					spillingQueueCfg := args.DiskQueueCfg
					spillingQueueCfg.CacheMode = colcontainer.DiskQueueCacheModeReuseCache
					spillingQueueCfg.SetDefaultBufferSizeBytesForCacheMode()
					newAggArgs.Allocator = colmem.NewAllocator(ctx, hashAggregatorMemAccount, factory)
					newAggArgs.MemAccount = hashAggregatorMemAccount
					var inMemoryHashAggregator colexecop.Operator
					inMemoryHashAggregator, err = colexec.NewHashAggregator(
						newAggArgs,
						&colexecutils.NewSpillingQueueArgs{
							UnlimitedAllocator: colmem.NewAllocator(ctx, spillingQueueMemAccount, factory),
							Types:              inputTypes,
							MemoryLimit:        totalMemLimit / 2,
							DiskQueueCfg:       spillingQueueCfg,
							FDSemaphore:        args.FDSemaphore,
							DiskAcc:            result.createDiskAccount(ctx, flowCtx, spillingQueueMemMonitorName),
						},
					)
					if err != nil {
						return r, err
					}
					ehaMonitorNamePrefix := fmt.Sprintf("external-hash-aggregator-%d", spec.ProcessorID)
					ehaMemAccount := result.createBufferingUnlimitedMemAccount(ctx, flowCtx, ehaMonitorNamePrefix)
					// Note that we will use an unlimited memory account here
					// even for the in-memory hash aggregator since it is easier
					// to do so than to try to replace the memory account if the
					// spilling to disk occurs (if we don't replace it in such
					// case, the wrapped aggregate functions might hit a memory
					// error even when used by the external hash aggregator).
					evalCtx.SingleDatumAggMemAccount = ehaMemAccount
					result.Op = colexec.NewOneInputDiskSpiller(
						inputs[0], inMemoryHashAggregator.(colexecop.BufferingInMemoryOperator),
						hashAggregatorMemMonitorName,
						func(input colexecop.Operator) colexecop.Operator {
							newAggArgs := *newAggArgs
							// Note that the hash-based partitioner will make
							// sure that partitions to process using the
							// in-memory hash aggregator fit under the limit, so
							// we use an unlimited allocator.
							newAggArgs.Allocator = colmem.NewAllocator(ctx, ehaMemAccount, factory)
							newAggArgs.MemAccount = ehaMemAccount
							newAggArgs.Input = input
							return colexec.NewExternalHashAggregator(
								flowCtx,
								args,
								&newAggArgs,
								result.makeDiskBackedSorterConstructor(ctx, flowCtx, args, ehaMonitorNamePrefix, factory),
								result.createDiskAccount(ctx, flowCtx, ehaMonitorNamePrefix),
							)
						},
						args.TestingKnobs.SpillingCallbackFn,
					)
				}
			} else {
				evalCtx.SingleDatumAggMemAccount = streamingMemAccount
				newAggArgs.Allocator = streamingAllocator
				newAggArgs.MemAccount = streamingMemAccount
				result.Op, err = colexec.NewOrderedAggregator(newAggArgs)
			}
			result.ToClose = append(result.ToClose, result.Op.(colexecop.Closer))

		case core.Distinct != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			if len(core.Distinct.OrderedColumns) == len(core.Distinct.DistinctColumns) {
				result.Op, err = colexecbase.NewOrderedDistinct(inputs[0], core.Distinct.OrderedColumns, result.ColumnTypes)
			} else {
				// We have separate unit tests that instantiate in-memory
				// distinct operators, so we don't need to look at
				// args.TestingKnobs.DiskSpillingDisabled and always instantiate
				// a disk-backed one here.
				distinctMemMonitorName := fmt.Sprintf("distinct-%d", spec.ProcessorID)
				distinctMemAccount := result.createMemAccountForSpillStrategy(
					ctx, flowCtx, distinctMemMonitorName,
				)
				// TODO(yuzefovich): we have an implementation of partially
				// ordered distinct, and we should plan it when we have
				// non-empty ordered columns and we think that the probability
				// of distinct tuples in the input is about 0.01 or less.
				allocator := colmem.NewAllocator(ctx, distinctMemAccount, factory)
				inMemoryUnorderedDistinct := colexec.NewUnorderedDistinct(
					allocator, inputs[0], core.Distinct.DistinctColumns, result.ColumnTypes,
				)
				diskAccount := result.createDiskAccount(ctx, flowCtx, distinctMemMonitorName)
				result.Op = colexec.NewOneInputDiskSpiller(
					inputs[0], inMemoryUnorderedDistinct.(colexecop.BufferingInMemoryOperator),
					distinctMemMonitorName,
					func(input colexecop.Operator) colexecop.Operator {
						monitorNamePrefix := fmt.Sprintf("external-distinct-%d", spec.ProcessorID)
						unlimitedAllocator := colmem.NewAllocator(
							ctx, result.createBufferingUnlimitedMemAccount(ctx, flowCtx, monitorNamePrefix), factory,
						)
						return colexec.NewExternalDistinct(
							unlimitedAllocator,
							flowCtx,
							args,
							input,
							result.ColumnTypes,
							result.makeDiskBackedSorterConstructor(ctx, flowCtx, args, monitorNamePrefix, factory),
							inMemoryUnorderedDistinct,
							diskAccount,
						)
					},
					args.TestingKnobs.SpillingCallbackFn,
				)
				result.ToClose = append(result.ToClose, result.Op.(colexecop.Closer))
			}

		case core.Ordinality != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Op = colexecbase.NewOrdinalityOp(streamingAllocator, inputs[0], outputIdx)
			result.ColumnTypes = appendOneType(spec.Input[0].ColumnTypes, types.Int)

		case core.HashJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}
			leftTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(leftTypes, spec.Input[0].ColumnTypes)
			rightTypes := make([]*types.T, len(spec.Input[1].ColumnTypes))
			copy(rightTypes, spec.Input[1].ColumnTypes)

			memoryLimit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
			if len(core.HashJoiner.LeftEqColumns) == 0 {
				// We are performing a cross-join, so we need to plan a
				// specialized operator.
				crossJoinerMemMonitorName := fmt.Sprintf("cross-joiner-%d", spec.ProcessorID)
				crossJoinerMemAccount := result.createBufferingUnlimitedMemAccount(ctx, flowCtx, crossJoinerMemMonitorName)
				crossJoinerDiskAcc := result.createDiskAccount(ctx, flowCtx, crossJoinerMemMonitorName)
				unlimitedAllocator := colmem.NewAllocator(ctx, crossJoinerMemAccount, factory)
				result.Op = colexecjoin.NewCrossJoiner(
					unlimitedAllocator,
					memoryLimit,
					args.DiskQueueCfg,
					args.FDSemaphore,
					core.HashJoiner.Type,
					inputs[0], inputs[1],
					leftTypes, rightTypes,
					crossJoinerDiskAcc,
				)
				result.ToClose = append(result.ToClose, result.Op.(colexecop.Closer))
			} else {
				hashJoinerMemMonitorName := fmt.Sprintf("hash-joiner-%d", spec.ProcessorID)
				var hashJoinerMemAccount *mon.BoundAccount
				var hashJoinerUnlimitedAllocator *colmem.Allocator
				if useStreamingMemAccountForBuffering {
					hashJoinerMemAccount = streamingMemAccount
					hashJoinerUnlimitedAllocator = streamingAllocator
				} else {
					hashJoinerMemAccount = result.createMemAccountForSpillStrategy(
						ctx, flowCtx, hashJoinerMemMonitorName,
					)
					hashJoinerUnlimitedAllocator = colmem.NewAllocator(
						ctx, result.createBufferingUnlimitedMemAccount(ctx, flowCtx, hashJoinerMemMonitorName), factory,
					)
				}
				hjSpec := colexecjoin.MakeHashJoinerSpec(
					core.HashJoiner.Type,
					core.HashJoiner.LeftEqColumns,
					core.HashJoiner.RightEqColumns,
					leftTypes,
					rightTypes,
					core.HashJoiner.RightEqColumnsAreKey,
				)

				inMemoryHashJoiner := colexecjoin.NewHashJoiner(
					colmem.NewAllocator(ctx, hashJoinerMemAccount, factory),
					hashJoinerUnlimitedAllocator, hjSpec, inputs[0], inputs[1],
					colexecjoin.HashJoinerInitialNumBuckets, memoryLimit,
				)
				if args.TestingKnobs.DiskSpillingDisabled {
					// We will not be creating a disk-backed hash joiner because
					// we're running a test that explicitly asked for only
					// in-memory hash joiner.
					result.Op = inMemoryHashJoiner
				} else {
					diskAccount := result.createDiskAccount(ctx, flowCtx, hashJoinerMemMonitorName)
					result.Op = colexec.NewTwoInputDiskSpiller(
						inputs[0], inputs[1], inMemoryHashJoiner.(colexecop.BufferingInMemoryOperator),
						hashJoinerMemMonitorName,
						func(inputOne, inputTwo colexecop.Operator) colexecop.Operator {
							monitorNamePrefix := fmt.Sprintf("external-hash-joiner-%d", spec.ProcessorID)
							unlimitedAllocator := colmem.NewAllocator(
								ctx, result.createBufferingUnlimitedMemAccount(ctx, flowCtx, monitorNamePrefix), factory,
							)
							ehj := colexec.NewExternalHashJoiner(
								unlimitedAllocator,
								flowCtx,
								args,
								hjSpec,
								inputOne, inputTwo,
								result.makeDiskBackedSorterConstructor(ctx, flowCtx, args, monitorNamePrefix, factory),
								diskAccount,
							)
							result.ToClose = append(result.ToClose, ehj.(colexecop.Closer))
							return ehj
						},
						args.TestingKnobs.SpillingCallbackFn,
					)
				}
			}

			result.ColumnTypes = core.HashJoiner.Type.MakeOutputTypes(leftTypes, rightTypes)

			if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type == descpb.InnerJoin {
				if err = result.planAndMaybeWrapFilter(
					ctx, flowCtx, evalCtx, args, spec.ProcessorID, core.HashJoiner.OnExpr, factory,
				); err != nil {
					return r, err
				}
			}

		case core.MergeJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}

			leftTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(leftTypes, spec.Input[0].ColumnTypes)
			rightTypes := make([]*types.T, len(spec.Input[1].ColumnTypes))
			copy(rightTypes, spec.Input[1].ColumnTypes)

			joinType := core.MergeJoiner.Type
			var onExpr *execinfrapb.Expression
			if !core.MergeJoiner.OnExpr.Empty() {
				if joinType != descpb.InnerJoin {
					return r, errors.AssertionFailedf(
						"ON expression (%s) was unexpectedly planned for merge joiner with join type %s",
						core.MergeJoiner.OnExpr.String(), core.MergeJoiner.Type.String(),
					)
				}
				onExpr = &core.MergeJoiner.OnExpr
			}

			monitorName := "merge-joiner"
			// We are using an unlimited memory monitor here because merge
			// joiner itself is responsible for making sure that we stay within
			// the memory limit, and it will fall back to disk if necessary.
			unlimitedAllocator := colmem.NewAllocator(
				ctx, result.createBufferingUnlimitedMemAccount(
					ctx, flowCtx, monitorName,
				), factory)
			diskAccount := result.createDiskAccount(ctx, flowCtx, monitorName)
			mj, err := colexecjoin.NewMergeJoinOp(
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
			result.ToClose = append(result.ToClose, mj.(colexecop.Closer))
			result.ColumnTypes = core.MergeJoiner.Type.MakeOutputTypes(leftTypes, rightTypes)

			if onExpr != nil {
				if err = result.planAndMaybeWrapFilter(
					ctx, flowCtx, evalCtx, args, spec.ProcessorID, *onExpr, factory,
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
					// TODO(yuzefovich): add support for hashing partitioner
					// (probably by leveraging hash routers once we can
					// distribute). The decision about which kind of partitioner
					// to use should come from the optimizer.
					partitionColIdx = int(wf.OutputColIdx)
					input, err = colexecwindow.NewWindowSortingPartitioner(
						streamingAllocator, input, typs,
						core.Windower.PartitionBy, wf.Ordering.Columns, int(wf.OutputColIdx),
						func(input colexecop.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column) (colexecop.Operator, error) {
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
				if colexecwindow.WindowFnNeedsPeersInfo(*wf.Func.WindowFunc) {
					peersColIdx = int(wf.OutputColIdx + tempColOffset)
					input, err = colexecwindow.NewWindowPeerGrouper(
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
					result.Op = colexecwindow.NewRowNumberOperator(streamingAllocator, input, outputIdx, partitionColIdx)
				case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
					result.Op, err = colexecwindow.NewRankOperator(
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
					result.Op, err = colexecwindow.NewRelativeRankOperator(
						unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx.Cfg), args.DiskQueueCfg,
						args.FDSemaphore, input, typs, windowFn, wf.Ordering.Columns,
						outputIdx, partitionColIdx, peersColIdx, diskAcc,
					)
					// NewRelativeRankOperator sometimes returns a constOp when
					// there are no ordering columns, so we check that the
					// returned operator is a Closer.
					if c, ok := result.Op.(colexecop.Closer); ok {
						result.ToClose = append(result.ToClose, c)
					}
				default:
					return r, errors.AssertionFailedf("window function %s is not supported", wf.String())
				}

				if tempColOffset > 0 {
					// We want to project out temporary columns (which have
					// indices in the range [wf.OutputColIdx,
					// wf.OutputColIdx+tempColOffset)).
					projection := make([]uint32, 0, wf.OutputColIdx+tempColOffset)
					for i := uint32(0); i < wf.OutputColIdx; i++ {
						projection = append(projection, i)
					}
					projection = append(projection, wf.OutputColIdx+tempColOffset)
					result.Op = colexecbase.NewSimpleProjectOp(result.Op, int(wf.OutputColIdx+tempColOffset), projection)
				}

				_, returnType, err := execinfrapb.GetWindowFunctionInfo(wf.Func, []*types.T{}...)
				if err != nil {
					return r, err
				}
				result.ColumnTypes = appendOneType(result.ColumnTypes, returnType)
				input = result.Op
			}

		default:
			return r, errors.AssertionFailedf("unsupported processor core %q", core)
		}
	}

	if err != nil {
		return r, err
	}

	if log.V(1) {
		log.Infof(ctx, "made op %T\n", result.Op)
	}

	// Note: at this point, it is legal for ColumnTypes to be empty (it is
	// legal for empty rows to be passed between processors).

	ppr := postProcessResult{
		Op:          result.Op,
		ColumnTypes: result.ColumnTypes,
	}
	err = ppr.planPostProcessSpec(ctx, flowCtx, evalCtx, args, post, factory)
	if err != nil {
		if log.V(2) {
			log.Infof(
				ctx,
				"vectorized post process planning failed with error %v post spec is %s, attempting to wrap as a row source",
				err, post,
			)
		}
		if core.TableReader != nil {
			// We cannot naively wrap a TableReader's post-processing spec since
			// it might project out unneeded columns with unset values. These
			// columns are still returned, and if we were to wrap an unsupported
			// post-processing spec, a Materializer would naively decode these
			// columns, which would return errors (e.g. UUIDs require 16 bytes).
			inputTypes := make([][]*types.T, len(spec.Input))
			for inputIdx, input := range spec.Input {
				inputTypes[inputIdx] = make([]*types.T, len(input.ColumnTypes))
				copy(inputTypes[inputIdx], input.ColumnTypes)
			}
			result.resetToState(ctx, resultPreSpecPlanningStateShallowCopy)
			err = result.createAndWrapRowSource(ctx, flowCtx, args, inputs, inputTypes, spec, factory, err)
		} else {
			err = result.wrapPostProcessSpec(ctx, flowCtx, args, post, args.Spec.ResultTypes, factory, err)
		}
	} else {
		// The result can be updated with the post process result.
		result.updateWithPostProcessResult(ppr)
	}
	if err != nil {
		return r, err
	}

	// Check that the actual output types are equal to the expected ones and
	// plan casts if they are not.
	if len(args.Spec.ResultTypes) != len(r.ColumnTypes) {
		return r, errors.AssertionFailedf("unexpectedly different number of columns are output: expected %v, actual %v", args.Spec.ResultTypes, r.ColumnTypes)
	}
	// projection is lazily allocated when the first column that needs an
	// explicit cast is found. It'll remain nil if projection isn't necessary.
	var projection []uint32
	for i := range args.Spec.ResultTypes {
		expected, actual := args.Spec.ResultTypes[i], r.ColumnTypes[i]
		if !actual.Identical(expected) {
			input := r.Op
			castedIdx := len(r.ColumnTypes)
			resultTypes := appendOneType(r.ColumnTypes, expected)
			r.Op, err = colexecbase.GetCastOperator(
				streamingAllocator, input, i, castedIdx, actual, expected,
			)
			if err != nil {
				// We don't support a native vectorized cast between these
				// types, so we will plan a noop row-execution processor to
				// handle it with a post-processing spec that simply passes
				// through all of the columns from the input and appends the
				// result of the cast to the end of the schema.
				post := &execinfrapb.PostProcessSpec{}
				post.RenderExprs = make([]execinfrapb.Expression, castedIdx+1)
				for j := 0; j < castedIdx; j++ {
					post.RenderExprs[j].Expr = fmt.Sprintf("@%d", j+1)
				}
				post.RenderExprs[castedIdx].Expr = fmt.Sprintf("@%d::%s", i+1, expected.SQLStandardName())
				result.Op = input
				if err = result.wrapPostProcessSpec(ctx, flowCtx, args, post, resultTypes, factory, err); err != nil {
					return r, err
				}
			}
			r.ColumnTypes = resultTypes
			if projection == nil {
				// This is the first column that needs an explicit cast, so we
				// need to actually allocate the slice and set all previous
				// columns to be used as is.
				projection = make([]uint32, len(args.Spec.ResultTypes))
				for j := 0; j < i; j++ {
					projection[j] = uint32(j)
				}
			}
			projection[i] = uint32(castedIdx)
		} else if projection != nil {
			projection[i] = uint32(i)
		}
	}
	if projection != nil {
		r.Op, r.ColumnTypes = addProjection(r.Op, r.ColumnTypes, projection)
	}
	if args.TestingKnobs.PlanInvariantsCheckers {
		// Plan an invariants checker if it isn't already the root of the tree.
		if _, isInvariantsChecker := r.Op.(*colexec.InvariantsChecker); !isInvariantsChecker {
			r.Op = colexec.NewInvariantsChecker(r.Op)
		}
	}
	// Handle the metadata sources from the input trees. Note that it is
	// possible that we have created a materializer which took over draining
	// the sources given to us by the caller.
	for i := range args.MetadataSources {
		r.MetadataSources = append(r.MetadataSources, args.MetadataSources[i]...)
	}
	return r, err
}

// planAndMaybeWrapFilter plans a filter. If the filter is unsupported, it is
// planned as a wrapped filterer processor.
func (r opResult) planAndMaybeWrapFilter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	args *colexecargs.NewColOperatorArgs,
	processorID int32,
	filter execinfrapb.Expression,
	factory coldata.ColumnFactory,
) error {
	op, err := planFilterExpr(
		ctx, flowCtx, evalCtx, r.Op, r.ColumnTypes, filter, args.StreamingMemAccount, factory, args.ExprHelper,
	)
	if err != nil {
		// ON expression planning failed. Fall back to planning the filter
		// using row execution.
		if log.V(2) {
			log.Infof(
				ctx,
				"vectorized join ON expr planning failed with error %v ON expr is %s, attempting to wrap as a row source",
				err, filter.String(),
			)
		}

		filtererSpec := &execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{
				Filterer: &execinfrapb.FiltererSpec{
					Filter: filter,
				},
			},
			ProcessorID: processorID,
			ResultTypes: args.Spec.ResultTypes,
		}
		return r.createAndWrapRowSource(
			ctx, flowCtx, args, []colexecop.Operator{r.Op}, [][]*types.T{r.ColumnTypes},
			filtererSpec, factory, err,
		)
	}
	r.Op = op
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
	args *colexecargs.NewColOperatorArgs,
	post *execinfrapb.PostProcessSpec,
	resultTypes []*types.T,
	factory coldata.ColumnFactory,
	causeToWrap error,
) error {
	noopSpec := &execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post:        *post,
		ResultTypes: resultTypes,
	}
	return r.createAndWrapRowSource(
		ctx, flowCtx, args, []colexecop.Operator{r.Op}, [][]*types.T{r.ColumnTypes},
		noopSpec, factory, causeToWrap,
	)
}

// planPostProcessSpec plans the post processing stage specified in post on top
// of r.Op.
func (r *postProcessResult) planPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	args *colexecargs.NewColOperatorArgs,
	post *execinfrapb.PostProcessSpec,
	factory coldata.ColumnFactory,
) error {
	if post.Projection {
		r.Op, r.ColumnTypes = addProjection(r.Op, r.ColumnTypes, post.OutputColumns)
	} else if post.RenderExprs != nil {
		if log.V(2) {
			log.Infof(ctx, "planning render expressions %+v", post.RenderExprs)
		}
		semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(evalCtx.Txn)
		var renderedCols []uint32
		for _, renderExpr := range post.RenderExprs {
			expr, err := args.ExprHelper.ProcessExpr(renderExpr, semaCtx, evalCtx, r.ColumnTypes)
			if err != nil {
				return err
			}
			var outputIdx int
			r.Op, outputIdx, r.ColumnTypes, err = planProjectionOperators(
				ctx, evalCtx, expr, r.ColumnTypes, r.Op, args.StreamingMemAccount, factory,
			)
			if err != nil {
				return errors.Wrapf(err, "unable to columnarize render expression %q", expr)
			}
			if outputIdx < 0 {
				return errors.AssertionFailedf("missing outputIdx")
			}
			renderedCols = append(renderedCols, uint32(outputIdx))
		}
		r.Op = colexecbase.NewSimpleProjectOp(r.Op, len(r.ColumnTypes), renderedCols)
		newTypes := make([]*types.T, len(renderedCols))
		for i, j := range renderedCols {
			newTypes[i] = r.ColumnTypes[j]
		}
		r.ColumnTypes = newTypes
	}
	if post.Offset != 0 {
		r.Op = colexec.NewOffsetOp(r.Op, post.Offset)
	}
	if post.Limit != 0 {
		r.Op = colexec.NewLimitOp(r.Op, post.Limit)
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

// createMemAccountForSpillStrategyWithLimit is the same as
// createMemAccountForSpillStrategy except that it takes in a custom limit
// instead of using the number obtained via execinfra.GetWorkMemLimit.
func (r opResult) createMemAccountForSpillStrategyWithLimit(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string, limit int64,
) *mon.BoundAccount {
	if flowCtx.Cfg.TestingKnobs.ForceDiskSpill {
		limit = 1
	}
	bufferingOpMemMonitor := mon.NewMonitorInheritWithLimit(name+"-limited", limit, flowCtx.EvalCtx.Mon)
	bufferingOpMemMonitor.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
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

// createDiskAccount instantiates an unlimited disk monitor and a disk account
// to be used for disk spilling infrastructure in vectorized engine.
// TODO(azhng): consolidates all allocation monitors/account manage into one
// place after branch cut for 20.1.
func (r opResult) createDiskAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	opDiskMonitor := execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, name)
	r.OpMonitors = append(r.OpMonitors, opDiskMonitor)
	opDiskAccount := opDiskMonitor.MakeBoundAccount()
	r.OpAccounts = append(r.OpAccounts, &opDiskAccount)
	return &opDiskAccount
}

type postProcessResult struct {
	Op          colexecop.Operator
	ColumnTypes []*types.T
}

func (r opResult) updateWithPostProcessResult(ppr postProcessResult) {
	r.Op = ppr.Op
	r.ColumnTypes = make([]*types.T, len(ppr.ColumnTypes))
	copy(r.ColumnTypes, ppr.ColumnTypes)
}

// planFilterExpr creates all operators to implement filter expression.
func planFilterExpr(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	evalCtx *tree.EvalContext,
	input colexecop.Operator,
	columnTypes []*types.T,
	filter execinfrapb.Expression,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
	helper *colexecargs.ExprHelper,
) (colexecop.Operator, error) {
	semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(evalCtx.Txn)
	expr, err := helper.ProcessExpr(filter, semaCtx, evalCtx, columnTypes)
	if err != nil {
		return nil, err
	}
	if expr == tree.DNull {
		// The filter expression is tree.DNull meaning that it is always false, so
		// we put a zero operator.
		return colexecutils.NewZeroOp(input), nil
	}
	op, _, filterColumnTypes, err := planSelectionOperators(
		ctx, evalCtx, expr, columnTypes, input, acc, factory,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to columnarize filter expression %q", filter)
	}
	if len(filterColumnTypes) > len(columnTypes) {
		// Additional columns were appended to store projections while
		// evaluating the filter. Project them away.
		var outputColumns []uint32
		for i := range columnTypes {
			outputColumns = append(outputColumns, uint32(i))
		}
		op = colexecbase.NewSimpleProjectOp(op, len(filterColumnTypes), outputColumns)
	}
	return op, nil
}

// addProjection adds a simple projection on top of op according to projection
// and returns the updated operator and type schema.
func addProjection(
	op colexecop.Operator, typs []*types.T, projection []uint32,
) (colexecop.Operator, []*types.T) {
	newTypes := make([]*types.T, len(projection))
	for i, j := range projection {
		newTypes[i] = typs[j]
	}
	return colexecbase.NewSimpleProjectOp(op, len(typs), projection), newTypes
}

func planSelectionOperators(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		op, err = colexecutils.BoolOrUnknownToSelOp(input, columnTypes, t.Idx)
		return op, -1, columnTypes, err
	case *tree.AndExpr:
		// AND expressions are handled by an implicit AND'ing of selection
		// vectors. First we select out the tuples that are true on the left
		// side, and then, only among the matched tuples, we select out the
		// tuples that are true on the right side.
		var leftOp, rightOp colexecop.Operator
		leftOp, _, typs, err = planSelectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		rightOp, resultIdx, typs, err = planSelectionOperators(
			ctx, evalCtx, t.TypedRight(), typs, leftOp, acc, factory,
		)
		return rightOp, resultIdx, typs, err
	case *tree.OrExpr:
		// OR expressions are handled by converting them to an equivalent CASE
		// statement. Since CASE statements don't have a selection form, plan a
		// projection and then convert the resulting boolean to a selection
		// vector.
		//
		// Rewrite the OR expression as an equivalent CASE expression. "a OR b"
		// becomes "CASE WHEN a THEN true WHEN b THEN true ELSE false END". This
		// way we can take advantage of the short-circuiting logic built into
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
			return nil, resultIdx, typs, err
		}
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, caseExpr, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		op, err = colexecutils.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, err
	case *tree.CaseExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc, factory,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op, err = colexecutils.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, err
	case *tree.IsNullExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op = colexec.NewIsNullSelOp(
			op, resultIdx, false /* negate */, typs[resultIdx].Family() == types.TupleFamily,
		)
		return op, resultIdx, typs, err
	case *tree.IsNotNullExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op = colexec.NewIsNullSelOp(
			op, resultIdx, true /* negate */, typs[resultIdx].Family() == types.TupleFamily,
		)
		return op, resultIdx, typs, err
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, err := planProjectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		lTyp := ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			switch cmpOp {
			case tree.Like, tree.NotLike:
				negate := cmpOp == tree.NotLike
				op, err = colexecsel.GetLikeOperator(
					evalCtx, leftOp, leftIdx, string(tree.MustBeDString(constArg)), negate,
				)
			case tree.In, tree.NotIn:
				negate := cmpOp == tree.NotIn
				datumTuple, ok := tree.AsDTuple(constArg)
				if !ok || tupleContainsTuples(datumTuple) {
					// Optimized IN operator is supported only on constant
					// expressions that don't contain tuples (because tuples
					// require special null-handling logic), so we fallback to
					// the default comparison operator.
					break
				}
				op, err = colexec.GetInOperator(lTyp, leftOp, leftIdx, datumTuple, negate)
			case tree.IsDistinctFrom, tree.IsNotDistinctFrom:
				if constArg != tree.DNull {
					// Optimized IsDistinctFrom and IsNotDistinctFrom are
					// supported only with NULL argument, so we fallback to the
					// default comparison operator.
					break
				}
				// IS NOT DISTINCT FROM NULL is synonymous with IS NULL and IS
				// DISTINCT FROM NULL is synonymous with IS NOT NULL (except for
				// tuples). Therefore, negate when the operator is IS DISTINCT
				// FROM NULL.
				negate := cmpOp == tree.IsDistinctFrom
				op = colexec.NewIsNullSelOp(leftOp, leftIdx, negate, false /* isTupleNull */)
			}
			if op == nil || err != nil {
				// op hasn't been created yet, so let's try the constructor for
				// all other selection operators.
				op, err = colexecsel.GetSelectionConstOperator(
					cmpOp, leftOp, ct, leftIdx, constArg, evalCtx, t,
				)
			}
			return op, resultIdx, ct, err
		}
		rightOp, rightIdx, ct, err := planProjectionOperators(
			ctx, evalCtx, t.TypedRight(), ct, leftOp, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		op, err = colexecsel.GetSelectionOperator(
			cmpOp, rightOp, ct, leftIdx, rightIdx, evalCtx, t,
		)
		return op, resultIdx, ct, err
	default:
		return nil, resultIdx, nil, errors.Errorf("unhandled selection expression type: %s", reflect.TypeOf(t))
	}
}

// planCastOperator plans a CAST operator that casts the column at index
// 'inputIdx' coming from input of type 'fromType' into a column of type
// 'toType' that will be output at index 'resultIdx'.
func planCastOperator(
	ctx context.Context,
	acc *mon.BoundAccount,
	columnTypes []*types.T,
	input colexecop.Operator,
	inputIdx int,
	fromType *types.T,
	toType *types.T,
	factory coldata.ColumnFactory,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	outputIdx := len(columnTypes)
	op, err = colexecbase.GetCastOperator(colmem.NewAllocator(ctx, acc, factory), input, inputIdx, outputIdx, fromType, toType)
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
	input colexecop.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	// projectDatum is a helper function that adds a new constant projection
	// operator for the given datum. typs are updated accordingly.
	projectDatum := func(datum tree.Datum) (colexecop.Operator, error) {
		resultIdx = len(columnTypes)
		datumType := datum.ResolvedType()
		typs = appendOneType(columnTypes, datumType)
		if datumType.Family() == types.UnknownFamily {
			// We handle Unknown type by planning a special constant null
			// operator.
			return colexecbase.NewConstNullOp(colmem.NewAllocator(ctx, acc, factory), input, resultIdx), nil
		}
		constVal := colconv.GetDatumToPhysicalFn(datumType)(datum)
		return colexecbase.NewConstOp(colmem.NewAllocator(ctx, acc, factory), input, datumType, constVal, resultIdx)
	}
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, nil
	case *tree.ComparisonExpr:
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(),
			columnTypes, input, acc, factory, nil /* binFn */, t,
		)
	case *tree.BinaryExpr:
		if err = checkSupportedBinaryExpr(t.TypedLeft(), t.TypedRight(), t.ResolvedType()); err != nil {
			return op, resultIdx, typs, err
		}
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(),
			columnTypes, input, acc, factory, t.Fn.Fn, nil, /* cmpExpr */
		)
	case *tree.IsNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, acc, false /* negate */, factory)
	case *tree.IsNotNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, acc, true /* negate */, factory)
	case *tree.CastExpr:
		expr := t.Expr.(tree.TypedExpr)
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, 0, nil, err
		}
		op, resultIdx, typs, err = planCastOperator(ctx, acc, typs, op, resultIdx, expr.ResolvedType(), t.ResolvedType(), factory)
		return op, resultIdx, typs, err
	case *tree.FuncExpr:
		var inputCols []int
		typs = make([]*types.T, len(columnTypes))
		copy(typs, columnTypes)
		op = input
		for _, e := range t.Exprs {
			var err error
			// TODO(rohany): This could be done better, especially in the case
			// of constant arguments, because the vectorized engine right now
			// creates a new column full of the constant value.
			op, resultIdx, typs, err = planProjectionOperators(
				ctx, evalCtx, e.(tree.TypedExpr), typs, op, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, nil, err
			}
			inputCols = append(inputCols, resultIdx)
		}
		resultIdx = len(typs)
		op, err = colexec.NewBuiltinFunctionOperator(
			colmem.NewAllocator(ctx, acc, factory), evalCtx, t, typs, inputCols, resultIdx, op,
		)
		typs = appendOneType(typs, t.ResolvedType())
		return op, resultIdx, typs, err
	case tree.Datum:
		op, err = projectDatum(t)
		return op, resultIdx, typs, err
	case *tree.Tuple:
		isConstTuple := true
		for _, expr := range t.Exprs {
			if _, isDatum := expr.(tree.Datum); !isDatum {
				isConstTuple = false
				break
			}
		}
		if isConstTuple {
			// Tuple expression is a constant, so we can evaluate it and
			// project the resulting datum.
			tuple, err := t.Eval(evalCtx)
			if err != nil {
				return nil, resultIdx, typs, err
			}
			op, err = projectDatum(tuple)
			return op, resultIdx, typs, err
		}
		outputType := t.ResolvedType()
		typs = make([]*types.T, len(columnTypes))
		copy(typs, columnTypes)
		tupleContentsIdxs := make([]int, len(t.Exprs))
		for i, expr := range t.Exprs {
			input, tupleContentsIdxs[i], typs, err = planProjectionOperators(
				ctx, evalCtx, expr.(tree.TypedExpr), typs, input, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
		}
		resultIdx = len(typs)
		op = colexec.NewTupleProjOp(
			colmem.NewAllocator(ctx, acc, factory), typs, tupleContentsIdxs, outputType, input, resultIdx,
		)
		typs = appendOneType(typs, outputType)
		return op, resultIdx, typs, err
	case *tree.CaseExpr:
		if t.Expr != nil {
			return nil, resultIdx, typs, errors.New("CASE <expr> WHEN expressions unsupported")
		}

		allocator := colmem.NewAllocator(ctx, acc, factory)
		caseOutputType := t.ResolvedType()
		if typeconv.TypeFamilyToCanonicalTypeFamily(caseOutputType.Family()) == types.BytesFamily {
			// Currently, there is a contradiction between the way CASE operator
			// works (which populates its output in arbitrary order) and the
			// flat bytes implementation of Bytes type (which prohibits sets in
			// arbitrary order), so we reject such scenario to fall back to
			// row-by-row engine.
			return nil, resultIdx, typs, errors.Newf(
				"unsupported type %s in CASE operator", caseOutputType)
		}
		caseOutputIdx := len(columnTypes)
		// We don't know the schema yet and will update it below, right before
		// instantiating caseOp. The same goes for subsetEndIdx.
		schemaEnforcer := colexecutils.NewBatchSchemaSubsetEnforcer(
			allocator, input, nil /* typs */, caseOutputIdx, -1, /* subsetEndIdx */
		)
		buffer := colexec.NewBufferOp(schemaEnforcer)
		caseOps := make([]colexecop.Operator, len(t.Whens))
		typs = appendOneType(columnTypes, caseOutputType)
		thenIdxs := make([]int, len(t.Whens)+1)
		for i, when := range t.Whens {
			// The case operator is assembled from n WHEN arms, n THEN arms, and
			// an ELSE arm. Each WHEN arm is a boolean projection. Each THEN arm
			// (and the ELSE arm) is a projection of the type of the CASE
			// expression. We set up each WHEN arm to write its output to a
			// fresh column, and likewise for the THEN arms and the ELSE arm.
			// Each WHEN arm individually acts on the single input batch from
			// the CaseExpr's input and is then transformed into a selection
			// vector, after which the THEN arm runs to create the output just
			// for the tuples that matched the WHEN arm. Each subsequent WHEN
			// arm will use the inverse of the selection vector to avoid running
			// the WHEN projection on tuples that have already been matched by a
			// previous WHEN arm. Finally, after each WHEN arm runs, we copy the
			// results of the WHEN into a single output vector, assembling the
			// final result of the case projection.
			whenTyped := when.Cond.(tree.TypedExpr)
			caseOps[i], resultIdx, typs, err = planProjectionOperators(
				ctx, evalCtx, whenTyped, typs, buffer, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
			caseOps[i], err = colexecutils.BoolOrUnknownToSelOp(caseOps[i], typs, resultIdx)
			if err != nil {
				return nil, resultIdx, typs, err
			}

			// Run the "then" clause on those tuples that were selected.
			caseOps[i], thenIdxs[i], typs, err = planProjectionOperators(
				ctx, evalCtx, when.Val.(tree.TypedExpr), typs, caseOps[i], acc, factory,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
			if !typs[thenIdxs[i]].Identical(typs[caseOutputIdx]) {
				// It is possible that the projection of this THEN arm has
				// different column type (for example, we expect INT2, but INT8
				// is given). In such case, we need to plan a cast.
				fromType, toType := typs[thenIdxs[i]], typs[caseOutputIdx]
				caseOps[i], thenIdxs[i], typs, err = planCastOperator(
					ctx, acc, typs, caseOps[i], thenIdxs[i], fromType, toType, factory,
				)
				if err != nil {
					return nil, resultIdx, typs, err
				}
			}
		}
		var elseOp colexecop.Operator
		elseExpr := t.Else
		if elseExpr == nil {
			// If there's no ELSE arm, we write NULLs.
			elseExpr = tree.DNull
		}
		elseOp, thenIdxs[len(t.Whens)], typs, err = planProjectionOperators(
			ctx, evalCtx, elseExpr.(tree.TypedExpr), typs, buffer, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
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
				return nil, resultIdx, typs, err
			}
		}

		schemaEnforcer.SetTypes(typs)
		op := colexec.NewCaseOp(allocator, buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)
		return op, caseOutputIdx, typs, err
	case *tree.AndExpr, *tree.OrExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, acc, factory)
	default:
		return nil, resultIdx, nil, errors.Errorf("unhandled projection expression type: %s", reflect.TypeOf(t))
	}
}

func checkSupportedProjectionExpr(left, right tree.TypedExpr) error {
	leftTyp := left.ResolvedType()
	rightTyp := right.ResolvedType()
	if leftTyp.Equivalent(rightTyp) {
		return nil
	}

	// The types are not equivalent. Check if either is a type we'd like to
	// avoid.
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
	input colexecop.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
	binFn tree.TwoArgFn,
	cmpExpr *tree.ComparisonExpr,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	if err := checkSupportedProjectionExpr(left, right); err != nil {
		return nil, resultIdx, typs, err
	}
	allocator := colmem.NewAllocator(ctx, acc, factory)
	resultIdx = -1
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	if lConstArg, lConst := left.(tree.Datum); lConst {
		// Case one: The left is constant.
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for
		// non-commutative operators such as - and /, though, so we still need
		// this case.
		var rightIdx int
		input, rightIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, right, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		resultIdx = len(typs)
		// The projection result will be outputted to a new column which is
		// appended to the input batch.
		op, err = colexecproj.GetProjectionLConstOperator(
			allocator, typs, left.ResolvedType(), outputType, projOp, input,
			rightIdx, lConstArg, resultIdx, evalCtx, binFn, cmpExpr,
		)
	} else {
		var leftIdx int
		input, leftIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, left, columnTypes, input, acc, factory,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		// Note that this check exists only for testing purposes. On the
		// regular workloads, we expect that tree.Tuples have already been
		// pre-evaluated. We also don't need fully-fledged planning as we have
		// for tree.Tuple in planProjectionOperators because the tests only
		// use constant tuples.
		if tuple, ok := right.(*tree.Tuple); ok {
			tupleDatum, err := tuple.Eval(evalCtx)
			if err != nil {
				return nil, resultIdx, typs, err
			}
			right = tupleDatum
		}
		if rConstArg, rConst := right.(tree.Datum); rConst {
			// Case 2: The right is constant.
			// The projection result will be outputted to a new column which is
			// appended to the input batch.
			resultIdx = len(typs)
			switch projOp {
			case tree.Like, tree.NotLike:
				negate := projOp == tree.NotLike
				op, err = colexecproj.GetLikeProjectionOperator(
					allocator, evalCtx, input, leftIdx, resultIdx,
					string(tree.MustBeDString(rConstArg)), negate,
				)
			case tree.In, tree.NotIn:
				negate := projOp == tree.NotIn
				datumTuple, ok := tree.AsDTuple(rConstArg)
				if !ok || tupleContainsTuples(datumTuple) {
					// Optimized IN operator is supported only on constant
					// expressions that don't contain tuples (because tuples
					// require special null-handling logic), so we fallback to
					// the default comparison operator.
					break
				}
				op, err = colexec.GetInProjectionOperator(
					allocator, typs[leftIdx], input, leftIdx, resultIdx, datumTuple, negate,
				)
			case tree.IsDistinctFrom, tree.IsNotDistinctFrom:
				if right != tree.DNull {
					// Optimized IsDistinctFrom and IsNotDistinctFrom are
					// supported only with NULL argument, so we fallback to the
					// default comparison operator.
					break
				}
				// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we
				// want to negate when IS DISTINCT FROM is used.
				negate := projOp == tree.IsDistinctFrom
				op = colexec.NewIsNullProjOp(
					allocator, input, leftIdx, resultIdx, negate, false, /* isTupleNull */
				)
			}
			if op == nil || err != nil {
				// op hasn't been created yet, so let's try the constructor for
				// all other projection operators.
				op, err = colexecproj.GetProjectionRConstOperator(
					allocator, typs, right.ResolvedType(), outputType, projOp,
					input, leftIdx, rConstArg, resultIdx, evalCtx, binFn, cmpExpr,
				)
			}
		} else {
			// Case 3: neither are constant.
			var rightIdx int
			input, rightIdx, typs, err = planProjectionOperators(
				ctx, evalCtx, right, typs, input, acc, factory,
			)
			if err != nil {
				return nil, resultIdx, nil, err
			}
			resultIdx = len(typs)
			op, err = colexecproj.GetProjectionOperator(
				allocator, typs, outputType, projOp, input, leftIdx, rightIdx,
				resultIdx, evalCtx, binFn, cmpExpr,
			)
		}
	}
	if err != nil {
		return op, resultIdx, typs, err
	}
	typs = appendOneType(typs, outputType)
	return op, resultIdx, typs, err
}

// planLogicalProjectionOp plans all the needed operators for a projection of
// a logical operation (either AND or OR).
func planLogicalProjectionOp(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	// Add a new boolean column that will store the result of the projection.
	resultIdx = len(columnTypes)
	typs = appendOneType(columnTypes, types.Bool)
	var (
		typedLeft, typedRight             tree.TypedExpr
		leftProjOpChain, rightProjOpChain colexecop.Operator
		leftIdx, rightIdx                 int
	)
	leftFeedOp := colexecop.NewFeedOperator()
	rightFeedOp := colexecop.NewFeedOperator()
	switch t := expr.(type) {
	case *tree.AndExpr:
		typedLeft = t.TypedLeft()
		typedRight = t.TypedRight()
	case *tree.OrExpr:
		typedLeft = t.TypedLeft()
		typedRight = t.TypedRight()
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unexpected logical expression type %s", t.String()))
	}
	leftProjOpChain, leftIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, typedLeft, typs, leftFeedOp, acc, factory,
	)
	if err != nil {
		return nil, resultIdx, typs, err
	}
	rightProjOpChain, rightIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, typedRight, typs, rightFeedOp, acc, factory,
	)
	if err != nil {
		return nil, resultIdx, typs, err
	}
	allocator := colmem.NewAllocator(ctx, acc, factory)
	input = colexecutils.NewBatchSchemaSubsetEnforcer(allocator, input, typs, resultIdx, len(typs))
	switch expr.(type) {
	case *tree.AndExpr:
		op, err = colexec.NewAndProjOp(
			allocator,
			input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp,
			typs[leftIdx], typs[rightIdx],
			leftIdx, rightIdx, resultIdx,
		)
	case *tree.OrExpr:
		op, err = colexec.NewOrProjOp(
			allocator,
			input, leftProjOpChain, rightProjOpChain,
			leftFeedOp, rightFeedOp,
			typs[leftIdx], typs[rightIdx],
			leftIdx, rightIdx, resultIdx,
		)
	}
	return op, resultIdx, typs, err
}

// planIsNullProjectionOp plans the operator for IS NULL and IS NOT NULL
// expressions (tree.IsNullExpr and tree.IsNotNullExpr, respectively).
func planIsNullProjectionOp(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	outputType *types.T,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	acc *mon.BoundAccount,
	negate bool,
	factory coldata.ColumnFactory,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	op, resultIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, expr, columnTypes, input, acc, factory,
	)
	if err != nil {
		return op, resultIdx, typs, err
	}
	outputIdx := len(typs)
	isTupleNull := typs[resultIdx].Family() == types.TupleFamily
	op = colexec.NewIsNullProjOp(
		colmem.NewAllocator(ctx, acc, factory), op, resultIdx, outputIdx, negate, isTupleNull,
	)
	typs = appendOneType(typs, outputType)
	return op, outputIdx, typs, nil
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

func tupleContainsTuples(tuple *tree.DTuple) bool {
	for _, typ := range tuple.ResolvedType().TupleContents() {
		if typ.Family() == types.TupleFamily {
			return true
		}
	}
	return false
}
