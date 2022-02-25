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
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

func checkNumIn(inputs []colexecargs.OpWithMetaInfo, numIn int) error {
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
	inputs []colexecargs.OpWithMetaInfo,
	inputTypes [][]*types.T,
	streamingMemAccount *mon.BoundAccount,
	processorID int32,
	newToWrap func([]execinfra.RowSource) (execinfra.RowSource, error),
	materializerSafeToRelease bool,
	factory coldata.ColumnFactory,
) (*colexec.Columnarizer, []execinfra.Releasable, error) {
	var toWrapInputs []execinfra.RowSource
	var releasables []execinfra.Releasable
	for i := range inputs {
		// Optimization: if the input is a Columnarizer, its input is
		// necessarily a execinfra.RowSource, so remove the unnecessary
		// conversion.
		if c, ok := inputs[i].Root.(*colexec.Columnarizer); ok {
			// Since this Columnarizer has been previously added to Closers and
			// MetadataSources, this call ensures that all future calls are noops.
			// Modifying the slices at this stage is difficult.
			c.MarkAsRemovedFromFlow()
			toWrapInputs = append(toWrapInputs, c.Input())
		} else {
			toWrapInput := colexec.NewMaterializer(
				flowCtx,
				processorID,
				inputs[i],
				inputTypes[i],
			)
			// We passed the ownership over the meta components to the
			// materializer.
			// TODO(yuzefovich): possibly set the length to 0 in order to be
			// able to pool the underlying slices.
			inputs[i].StatsCollectors = nil
			inputs[i].MetadataSources = nil
			inputs[i].ToClose = nil
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

	proc, isProcessor := toWrap.(execinfra.Processor)
	if !isProcessor {
		return nil, nil, errors.AssertionFailedf("unexpectedly %T is not an execinfra.Processor", toWrap)
	}
	var c *colexec.Columnarizer
	if proc.MustBeStreaming() {
		c = colexec.NewStreamingColumnarizer(
			colmem.NewAllocator(ctx, streamingMemAccount, factory), flowCtx, processorID, toWrap,
		)
	} else {
		c = colexec.NewBufferingColumnarizer(
			colmem.NewAllocator(ctx, streamingMemAccount, factory), flowCtx, processorID, toWrap,
		)
	}
	return c, releasables, nil
}

type opResult struct {
	*colexecargs.NewColOperatorResult
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
		return nil

	case spec.Core.TableReader != nil:
		return nil

	case spec.Core.JoinReader != nil:
		if !spec.Core.JoinReader.IsIndexJoin() {
			return errLookupJoinUnsupported
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
			if wf.FilterColIdx != tree.NoColumnIdx {
				return errors.Newf("window functions with FILTER clause are not supported")
			}
			if wf.Func.AggregateFunc != nil {
				if !colexecagg.IsAggOptimized(*wf.Func.AggregateFunc) {
					return errors.Newf("default aggregate window functions not supported")
				}
			}
		}
		return nil

	case spec.Core.LocalPlanNode != nil:
		// LocalPlanNode core is special (we don't have any plans on vectorizing
		// it at the moment), so we want to return a custom error for it to
		// distinguish from other unsupported cores.
		return errLocalPlanNodeWrap

	default:
		return errCoreUnsupportedNatively
	}
}

var (
	errCoreUnsupportedNatively        = errors.New("unsupported processor core")
	errLocalPlanNodeWrap              = errors.New("LocalPlanNode core needs to be wrapped")
	errMetadataTestSenderWrap         = errors.New("core.MetadataTestSender is not supported")
	errMetadataTestReceiverWrap       = errors.New("core.MetadataTestReceiver is not supported")
	errChangeAggregatorWrap           = errors.New("core.ChangeAggregator is not supported")
	errChangeFrontierWrap             = errors.New("core.ChangeFrontier is not supported")
	errReadImportWrap                 = errors.New("core.ReadImport is not supported")
	errBackupDataWrap                 = errors.New("core.BackupData is not supported")
	errBackfillerWrap                 = errors.New("core.Backfiller is not supported (not an execinfra.RowSource)")
	errExporterWrap                   = errors.New("core.Exporter is not supported (not an execinfra.RowSource)")
	errSamplerWrap                    = errors.New("core.Sampler is not supported (not an execinfra.RowSource)")
	errSampleAggregatorWrap           = errors.New("core.SampleAggregator is not supported (not an execinfra.RowSource)")
	errExperimentalWrappingProhibited = errors.New("wrapping for non-JoinReader and non-LocalPlanNode cores is prohibited in vectorize=experimental_always")
	errWrappedCast                    = errors.New("mismatched types in NewColOperator and unsupported casts")
	errLookupJoinUnsupported          = errors.New("lookup join reader is unsupported in vectorized")
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
		return errReadImportWrap
	case spec.Core.Exporter != nil:
		return errExporterWrap
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
		return errBackupDataWrap
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
	limit int64,
	matchLen uint32,
	maxNumberPartitions int,
	processorID int32,
	opNamePrefix string,
	factory coldata.ColumnFactory,
) colexecop.Operator {
	var (
		sorterMemMonitorName string
		inMemorySorter       colexecop.Operator
	)
	if len(ordering.Columns) == int(matchLen) {
		// The input is already fully ordered, so there is nothing to sort.
		return input
	}
	totalMemLimit := execinfra.GetWorkMemLimit(flowCtx)
	spoolMemLimit := totalMemLimit * 4 / 5
	maxOutputBatchMemSize := totalMemLimit - spoolMemLimit
	if totalMemLimit == 1 {
		// If total memory limit is 1, we're likely in a "force disk spill"
		// scenario, so we'll set all internal limits to 1 too (if we don't,
		// they will end up as 0 which is treated as "no limit").
		spoolMemLimit = 1
		maxOutputBatchMemSize = 1
	}
	if limit != 0 {
		// There is a limit specified, so we know exactly how many rows the
		// sorter should output. Use a top K sorter, which uses a heap to avoid
		// storing more rows than necessary.
		var topKSorterMemAccount *mon.BoundAccount
		topKSorterMemAccount, sorterMemMonitorName = args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
			ctx, flowCtx, spoolMemLimit, opNamePrefix+"topk-sort", processorID,
		)
		inMemorySorter = colexec.NewTopKSorter(
			colmem.NewAllocator(ctx, topKSorterMemAccount, factory), input,
			inputTypes, ordering.Columns, int(matchLen), uint64(limit), maxOutputBatchMemSize,
		)
	} else if matchLen > 0 {
		// The input is already partially ordered. Use a chunks sorter to avoid
		// loading all the rows into memory.
		var sortChunksMemAccount *mon.BoundAccount
		sortChunksMemAccount, sorterMemMonitorName = args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
			ctx, flowCtx, spoolMemLimit, opNamePrefix+"sort-chunks", processorID,
		)
		inMemorySorter = colexec.NewSortChunks(
			colmem.NewAllocator(ctx, sortChunksMemAccount, factory), input, inputTypes,
			ordering.Columns, int(matchLen), maxOutputBatchMemSize,
		)
	} else {
		// No optimizations possible. Default to the standard sort operator.
		var sorterMemAccount *mon.BoundAccount
		sorterMemAccount, sorterMemMonitorName = args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
			ctx, flowCtx, spoolMemLimit, opNamePrefix+"sort-all", processorID,
		)
		inMemorySorter = colexec.NewSorter(
			colmem.NewAllocator(ctx, sorterMemAccount, factory), input,
			inputTypes, ordering.Columns, maxOutputBatchMemSize,
		)
	}
	if args.TestingKnobs.DiskSpillingDisabled {
		// In some testing scenarios we actually don't want to create a
		// disk-backed sort.
		return inMemorySorter
	}
	// NOTE: when spilling to disk, we're using the same general external
	// sorter regardless of which sorter variant we have instantiated (i.e.
	// we don't take advantage of the limits and of partial ordering). We
	// could improve this.
	return colexec.NewOneInputDiskSpiller(
		input, inMemorySorter.(colexecop.BufferingInMemoryOperator),
		sorterMemMonitorName,
		func(input colexecop.Operator) colexecop.Operator {
			opName := opNamePrefix + "external-sorter"
			// We are using unlimited memory monitors here because external
			// sort itself is responsible for making sure that we stay within
			// the memory limit.
			sortUnlimitedAllocator := colmem.NewAllocator(
				ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(
					ctx, flowCtx, opName+"-sort", processorID,
				), factory)
			mergeUnlimitedAllocator := colmem.NewAllocator(
				ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(
					ctx, flowCtx, opName+"-merge", processorID,
				), factory)
			outputUnlimitedAllocator := colmem.NewAllocator(
				ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(
					ctx, flowCtx, opName+"-output", processorID,
				), factory)
			diskAccount := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, processorID)
			es := colexec.NewExternalSorter(
				sortUnlimitedAllocator,
				mergeUnlimitedAllocator,
				outputUnlimitedAllocator,
				input, inputTypes, ordering, uint64(limit),
				int(matchLen),
				execinfra.GetWorkMemLimit(flowCtx),
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
	)
}

// makeDiskBackedSorterConstructor creates a colexec.DiskBackedSorterConstructor
// that can be used by the hash-based partitioner.
// NOTE: unless DelegateFDAcquisitions testing knob is set to true, it is up to
// the caller to acquire the necessary file descriptors up front.
func (r opResult) makeDiskBackedSorterConstructor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	opNamePrefix string,
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
		return r.createDiskBackedSort(
			ctx, flowCtx, &sortArgs, input, inputTypes,
			execinfrapb.Ordering{Columns: orderingCols}, 0, /* limit */
			0 /* matchLen */, maxNumberPartitions, args.Spec.ProcessorID,
			opNamePrefix+"-", factory,
		)
	}
}

// TODO(yuzefovich): introduce some way to unit test that the meta info tracking
// works correctly. See #64256 for more details.

// takeOverMetaInfo iterates over all meta components from the input trees and
// passes the responsibility of handling those to target. The input objects are
// updated in-place accordingly.
func takeOverMetaInfo(target *colexecargs.OpWithMetaInfo, inputs []colexecargs.OpWithMetaInfo) {
	for i := range inputs {
		target.StatsCollectors = append(target.StatsCollectors, inputs[i].StatsCollectors...)
		target.MetadataSources = append(target.MetadataSources, inputs[i].MetadataSources...)
		target.ToClose = append(target.ToClose, inputs[i].ToClose...)
		inputs[i].MetadataSources = nil
		inputs[i].StatsCollectors = nil
		inputs[i].ToClose = nil
	}
}

// createAndWrapRowSource takes a processor spec, creating the row source and
// wrapping it using wrapRowSources. Note that the post process spec is included
// in the processor creation, so make sure to clear it if it will be inspected
// again. opResult is updated with the new ColumnTypes and the resulting
// Columnarizer if there is no error.
// - causeToWrap is an error that prompted us to wrap a processor core into the
// vectorized plan (for example, it could be an unsupported processor core, an
// unsupported function, etc).
func (r opResult) createAndWrapRowSource(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	inputs []colexecargs.OpWithMetaInfo,
	inputTypes [][]*types.T,
	spec *execinfrapb.ProcessorSpec,
	factory coldata.ColumnFactory,
	causeToWrap error,
) error {
	if args.ProcessorConstructor == nil {
		return errors.New("processorConstructor is nil")
	}
	log.VEventf(ctx, 1, "planning a row-execution processor in the vectorized flow: %v", causeToWrap)
	if err := canWrap(flowCtx.EvalCtx.SessionData().VectorizeMode, spec); err != nil {
		log.VEventf(ctx, 1, "planning a wrapped processor failed: %v", err)
		// Return the original error for why we don't support this spec
		// natively since it is more interesting.
		return causeToWrap
	}
	// Note that the materializers aren't safe to release in all cases since in
	// some cases they could be released before being closed. Namely, this would
	// occur if we have a subquery with LocalPlanNode core and a materializer is
	// added in order to wrap that core - what will happen is that all
	// releasables are put back into their pools upon the subquery's flow
	// cleanup, yet the subquery planNode tree isn't closed yet since its
	// closure is done when the main planNode tree is being closed.
	// TODO(yuzefovich): currently there are some other cases as well, figure
	// those out. I believe all those cases can occur **only** if we have
	// LocalPlanNode cores which is the case when we have non-empty
	// LocalProcessors.
	materializerSafeToRelease := len(args.LocalProcessors) == 0
	c, releasables, err := wrapRowSources(
		ctx,
		flowCtx,
		inputs,
		inputTypes,
		args.StreamingMemAccount,
		spec.ProcessorID,
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
	r.Root = c
	r.Columnarizer = c
	if buildutil.CrdbTestBuild {
		r.Root = colexec.NewInvariantsChecker(r.Root)
	}
	takeOverMetaInfo(&r.OpWithMetaInfo, inputs)
	r.MetadataSources = append(r.MetadataSources, r.Root.(colexecop.MetadataSource))
	r.ToClose = append(r.ToClose, r.Root.(colexecop.Closer))
	r.Releasables = append(r.Releasables, releasables...)
	return nil
}

// MaybeRemoveRootColumnarizer examines whether r represents such a tree of
// operators that has a columnarizer as its root with no responsibility over
// other meta components. If that's the case, the input to the columnarizer is
// returned and the columnarizer is marked as removed from the flow; otherwise,
// nil is returned.
func MaybeRemoveRootColumnarizer(r colexecargs.OpWithMetaInfo) execinfra.RowSource {
	root := r.Root
	if buildutil.CrdbTestBuild {
		// We might have an invariants checker as the root right now, we gotta
		// peek inside of it if so.
		root = colexec.MaybeUnwrapInvariantsChecker(root)
	}
	c, isColumnarizer := root.(*colexec.Columnarizer)
	if !isColumnarizer {
		return nil
	}
	// We have the columnarizer as the root, and it must be included into the
	// MetadataSources and ToClose slices, so if we don't see any other objects,
	// then the responsibility over other meta components has been claimed by
	// the children of the columnarizer.
	if len(r.StatsCollectors) != 0 || len(r.MetadataSources) != 1 || len(r.ToClose) != 1 {
		return nil
	}
	c.MarkAsRemovedFromFlow()
	return c.Input()
}

func getStreamingAllocator(
	ctx context.Context, args *colexecargs.NewColOperatorArgs,
) *colmem.Allocator {
	return colmem.NewAllocator(ctx, args.StreamingMemAccount, args.Factory)
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
	spec := args.Spec
	inputs := args.Inputs
	if args.Factory == nil {
		// This code path is only used in tests.
		args.Factory = coldataext.NewExtendedColumnFactory(flowCtx.EvalCtx)
	}
	factory := args.Factory
	if args.ExprHelper == nil {
		args.ExprHelper = colexecargs.NewExprHelper()
		args.ExprHelper.SemaCtx = flowCtx.NewSemaContext(flowCtx.Txn)
	}
	if args.MonitorRegistry == nil {
		args.MonitorRegistry = &colexecargs.MonitorRegistry{}
	}

	core := &spec.Core
	post := &spec.Post

	if err = supportedNatively(spec); err != nil {
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
			result.Root = colexecop.NewNoop(inputs[0].Root)
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)

		case core.Values != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}
			if core.Values.NumRows == 0 || len(core.Values.Columns) == 0 {
				// To simplify valuesOp we handle some special cases with
				// fixedNumTuplesNoInputOp.
				result.Root = colexecutils.NewFixedNumTuplesNoInputOp(
					getStreamingAllocator(ctx, args), int(core.Values.NumRows), nil, /* opToInitialize */
				)
			} else {
				result.Root = colexec.NewValuesOp(getStreamingAllocator(ctx, args), core.Values)
			}
			result.ColumnTypes = make([]*types.T, len(core.Values.Columns))
			for i, col := range core.Values.Columns {
				result.ColumnTypes[i] = col.Type
			}

		case core.TableReader != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}
			// We have to create a separate account in order for the cFetcher to
			// be able to precisely track the size of its output batch. This
			// memory account is "streaming" in its nature, so we create an
			// unlimited one.
			cFetcherMemAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
				ctx, flowCtx, "cfetcher" /* opName */, spec.ProcessorID,
			)
			kvFetcherMemAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
				ctx, flowCtx, "kvfetcher" /* opName */, spec.ProcessorID,
			)
			estimatedRowCount := spec.EstimatedRowCount
			scanOp, err := colfetcher.NewColBatchScan(
				ctx, colmem.NewAllocator(ctx, cFetcherMemAcc, factory), kvFetcherMemAcc,
				flowCtx, core.TableReader, post, estimatedRowCount,
			)
			if err != nil {
				return r, err
			}
			result.finishScanPlanning(scanOp, scanOp.ResultTypes)

		case core.JoinReader != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			if !core.JoinReader.IsIndexJoin() {
				return r, errors.AssertionFailedf("lookup join reader is unsupported in vectorized")
			}
			// We have to create a separate account in order for the cFetcher to
			// be able to precisely track the size of its output batch. This
			// memory account is "streaming" in its nature, so we create an
			// unlimited one.
			cFetcherMemAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
				ctx, flowCtx, "cfetcher" /* opName */, spec.ProcessorID,
			)
			kvFetcherMemAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
				ctx, flowCtx, "kvfetcher" /* opName */, spec.ProcessorID,
			)
			var streamerBudgetAcc *mon.BoundAccount
			// We have an index join, and when the ordering doesn't have to be
			// maintained, we might use the Streamer API which requires a
			// separate memory account that is bound to an unlimited memory
			// monitor.
			if !core.JoinReader.MaintainOrdering {
				streamerBudgetAcc = args.MonitorRegistry.CreateUnlimitedMemAccount(
					ctx, flowCtx, "streamer" /* opName */, spec.ProcessorID,
				)
			}
			inputTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(inputTypes, spec.Input[0].ColumnTypes)
			indexJoinOp, err := colfetcher.NewColIndexJoin(
				ctx, getStreamingAllocator(ctx, args),
				colmem.NewAllocator(ctx, cFetcherMemAcc, factory),
				kvFetcherMemAcc, streamerBudgetAcc, flowCtx,
				inputs[0].Root, core.JoinReader, inputTypes,
			)
			if err != nil {
				return r, err
			}
			result.finishScanPlanning(indexJoinOp, indexJoinOp.ResultTypes)

		case core.Filterer != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}

			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			result.Root = inputs[0].Root
			if err := result.planAndMaybeWrapFilter(
				ctx, flowCtx, args, spec.ProcessorID, core.Filterer.Filter, factory,
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
				result.Root, err = colexecutils.NewFixedNumTuplesNoInputOp(
					getStreamingAllocator(ctx, args), 1 /* numTuples */, inputs[0].Root,
				), nil
				// We make ColumnTypes non-nil so that sanity check doesn't
				// panic.
				result.ColumnTypes = []*types.T{}
				break
			}
			if aggSpec.IsRowCount() {
				result.Root, err = colexec.NewCountOp(getStreamingAllocator(ctx, args), inputs[0].Root), nil
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
			// Make a copy of the evalCtx since we're modifying it below.
			evalCtx := flowCtx.NewEvalCtx()
			newAggArgs := &colexecagg.NewAggregatorArgs{
				Input:      inputs[0].Root,
				InputTypes: inputTypes,
				Spec:       aggSpec,
				EvalCtx:    evalCtx,
			}
			newAggArgs.Constructors, newAggArgs.ConstArguments, newAggArgs.OutputTypes, err = colexecagg.ProcessAggregations(
				evalCtx, args.ExprHelper.SemaCtx, aggSpec.Aggregations, inputTypes,
			)
			if err != nil {
				return r, err
			}
			result.ColumnTypes = newAggArgs.OutputTypes

			if needHash {
				opName := "hash-aggregator"
				outputUnlimitedAllocator := colmem.NewAllocator(
					ctx,
					args.MonitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, opName+"-output", spec.ProcessorID),
					factory,
				)
				// We have separate unit tests that instantiate the in-memory
				// hash aggregators, so we don't need to look at
				// args.TestingKnobs.DiskSpillingDisabled and always instantiate
				// a disk-backed one here.
				diskSpillingDisabled := !colexec.HashAggregationDiskSpillingEnabled.Get(&flowCtx.Cfg.Settings.SV)
				if diskSpillingDisabled {
					// The disk spilling is disabled by the cluster setting, so
					// we give an unlimited memory account to the in-memory
					// hash aggregator and don't set up the disk spiller.
					hashAggregatorUnlimitedMemAccount := args.MonitorRegistry.CreateUnlimitedMemAccount(
						ctx, flowCtx, opName, spec.ProcessorID,
					)
					newAggArgs.Allocator = colmem.NewAllocator(
						ctx, hashAggregatorUnlimitedMemAccount, factory,
					)
					newAggArgs.MemAccount = hashAggregatorUnlimitedMemAccount
					evalCtx.SingleDatumAggMemAccount = hashAggregatorUnlimitedMemAccount
					maxOutputBatchMemSize := execinfra.GetWorkMemLimit(flowCtx)
					// The second argument is nil because we disable the
					// tracking of the input tuples.
					result.Root = colexec.NewHashAggregator(
						newAggArgs, nil, /* newSpillingQueueArgs */
						outputUnlimitedAllocator, maxOutputBatchMemSize,
					)
				} else {
					// We will divide the available memory equally between the
					// two usages - the hash aggregation itself and the input
					// tuples tracking.
					totalMemLimit := execinfra.GetWorkMemLimit(flowCtx)
					// We will give 20% of the hash aggregation budget to the
					// output batch.
					maxOutputBatchMemSize := totalMemLimit / 10
					hashAggregationMemLimit := totalMemLimit/2 - maxOutputBatchMemSize
					inputTuplesTrackingMemLimit := totalMemLimit / 2
					if totalMemLimit == 1 {
						// If total memory limit is 1, we're likely in a "force
						// disk spill" scenario, so we'll set all internal
						// limits to 1 too (if we don't, they will end up as 0
						// which is treated as "no limit").
						maxOutputBatchMemSize = 1
						hashAggregationMemLimit = 1
						inputTuplesTrackingMemLimit = 1
					}
					hashAggregatorMemAccount, hashAggregatorMemMonitorName := args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
						ctx, flowCtx, hashAggregationMemLimit, opName, spec.ProcessorID,
					)
					spillingQueueMemMonitorName := hashAggregatorMemMonitorName + "-spilling-queue"
					// We need to create a separate memory account for the
					// spilling queue because it looks at how much memory it has
					// already used in order to decide when to spill to disk.
					spillingQueueMemAccount := args.MonitorRegistry.CreateUnlimitedMemAccount(
						ctx, flowCtx, spillingQueueMemMonitorName, spec.ProcessorID,
					)
					newAggArgs.Allocator = colmem.NewAllocator(ctx, hashAggregatorMemAccount, factory)
					newAggArgs.MemAccount = hashAggregatorMemAccount
					inMemoryHashAggregator := colexec.NewHashAggregator(
						newAggArgs,
						&colexecutils.NewSpillingQueueArgs{
							UnlimitedAllocator: colmem.NewAllocator(ctx, spillingQueueMemAccount, factory),
							Types:              inputTypes,
							MemoryLimit:        inputTuplesTrackingMemLimit,
							DiskQueueCfg:       args.DiskQueueCfg,
							FDSemaphore:        args.FDSemaphore,
							DiskAcc:            args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, spillingQueueMemMonitorName, spec.ProcessorID),
						},
						outputUnlimitedAllocator,
						maxOutputBatchMemSize,
					)
					ehaOpName := "external-hash-aggregator"
					ehaMemAccount := args.MonitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, ehaOpName, spec.ProcessorID)
					// Note that we will use an unlimited memory account here
					// even for the in-memory hash aggregator since it is easier
					// to do so than to try to replace the memory account if the
					// spilling to disk occurs (if we don't replace it in such
					// case, the wrapped aggregate functions might hit a memory
					// error even when used by the external hash aggregator).
					evalCtx.SingleDatumAggMemAccount = ehaMemAccount
					result.Root = colexec.NewOneInputDiskSpiller(
						inputs[0].Root, inMemoryHashAggregator.(colexecop.BufferingInMemoryOperator),
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
								result.makeDiskBackedSorterConstructor(ctx, flowCtx, args, ehaOpName, factory),
								args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, ehaOpName, spec.ProcessorID),
								// Note that here we can use the same allocator
								// object as we passed to the in-memory hash
								// aggregator because only one (either in-memory
								// or external) operator will reach the output
								// state.
								outputUnlimitedAllocator,
								maxOutputBatchMemSize,
							)
						},
						args.TestingKnobs.SpillingCallbackFn,
					)
				}
			} else {
				evalCtx.SingleDatumAggMemAccount = args.StreamingMemAccount
				newAggArgs.Allocator = getStreamingAllocator(ctx, args)
				newAggArgs.MemAccount = args.StreamingMemAccount
				result.Root = colexec.NewOrderedAggregator(newAggArgs)
			}
			result.ToClose = append(result.ToClose, result.Root.(colexecop.Closer))

		case core.Distinct != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			if len(core.Distinct.OrderedColumns) == len(core.Distinct.DistinctColumns) {
				result.Root = colexecbase.NewOrderedDistinct(
					inputs[0].Root, core.Distinct.OrderedColumns, result.ColumnTypes,
					core.Distinct.NullsAreDistinct, core.Distinct.ErrorOnDup,
				)
			} else {
				// We have separate unit tests that instantiate in-memory
				// distinct operators, so we don't need to look at
				// args.TestingKnobs.DiskSpillingDisabled and always instantiate
				// a disk-backed one here.
				distinctMemAccount, distinctMemMonitorName := args.MonitorRegistry.CreateMemAccountForSpillStrategy(
					ctx, flowCtx, "distinct" /* opName */, spec.ProcessorID,
				)
				// TODO(yuzefovich): we have an implementation of partially
				// ordered distinct, and we should plan it when we have
				// non-empty ordered columns and we think that the probability
				// of distinct tuples in the input is about 0.01 or less.
				allocator := colmem.NewAllocator(ctx, distinctMemAccount, factory)
				inMemoryUnorderedDistinct := colexec.NewUnorderedDistinct(
					allocator, inputs[0].Root, core.Distinct.DistinctColumns, result.ColumnTypes,
					core.Distinct.NullsAreDistinct, core.Distinct.ErrorOnDup,
				)
				edOpName := "external-distinct"
				diskAccount := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, edOpName, spec.ProcessorID)
				result.Root = colexec.NewOneInputDiskSpiller(
					inputs[0].Root, inMemoryUnorderedDistinct.(colexecop.BufferingInMemoryOperator),
					distinctMemMonitorName,
					func(input colexecop.Operator) colexecop.Operator {
						unlimitedAllocator := colmem.NewAllocator(
							ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, edOpName, spec.ProcessorID), factory,
						)
						return colexec.NewExternalDistinct(
							unlimitedAllocator,
							flowCtx,
							args,
							input,
							result.ColumnTypes,
							result.makeDiskBackedSorterConstructor(ctx, flowCtx, args, edOpName, factory),
							inMemoryUnorderedDistinct,
							diskAccount,
						)
					},
					args.TestingKnobs.SpillingCallbackFn,
				)
				result.ToClose = append(result.ToClose, result.Root.(colexecop.Closer))
			}

		case core.Ordinality != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Root = colexecbase.NewOrdinalityOp(
				getStreamingAllocator(ctx, args), inputs[0].Root, outputIdx,
			)
			result.ColumnTypes = appendOneType(spec.Input[0].ColumnTypes, types.Int)

		case core.HashJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}
			leftTypes := make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(leftTypes, spec.Input[0].ColumnTypes)
			rightTypes := make([]*types.T, len(spec.Input[1].ColumnTypes))
			copy(rightTypes, spec.Input[1].ColumnTypes)

			memoryLimit := execinfra.GetWorkMemLimit(flowCtx)
			if len(core.HashJoiner.LeftEqColumns) == 0 {
				// We are performing a cross-join, so we need to plan a
				// specialized operator.
				opName := "cross-joiner"
				crossJoinerMemAccount := args.MonitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, opName, spec.ProcessorID)
				crossJoinerDiskAcc := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, spec.ProcessorID)
				unlimitedAllocator := colmem.NewAllocator(ctx, crossJoinerMemAccount, factory)
				result.Root = colexecjoin.NewCrossJoiner(
					unlimitedAllocator,
					memoryLimit,
					args.DiskQueueCfg,
					args.FDSemaphore,
					core.HashJoiner.Type,
					inputs[0].Root, inputs[1].Root,
					leftTypes, rightTypes,
					crossJoinerDiskAcc,
				)
				result.ToClose = append(result.ToClose, result.Root.(colexecop.Closer))
			} else {
				opName := "hash-joiner"
				hashJoinerMemAccount, hashJoinerMemMonitorName := args.MonitorRegistry.CreateMemAccountForSpillStrategy(
					ctx, flowCtx, opName, spec.ProcessorID,
				)
				hashJoinerUnlimitedAllocator := colmem.NewAllocator(
					ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, opName, spec.ProcessorID), factory,
				)
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
					hashJoinerUnlimitedAllocator, hjSpec, inputs[0].Root, inputs[1].Root,
					colexecjoin.HashJoinerInitialNumBuckets,
				)
				if args.TestingKnobs.DiskSpillingDisabled {
					// We will not be creating a disk-backed hash joiner because
					// we're running a test that explicitly asked for only
					// in-memory hash joiner.
					result.Root = inMemoryHashJoiner
				} else {
					opName := "external-hash-joiner"
					diskAccount := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, spec.ProcessorID)
					result.Root = colexec.NewTwoInputDiskSpiller(
						inputs[0].Root, inputs[1].Root, inMemoryHashJoiner.(colexecop.BufferingInMemoryOperator),
						hashJoinerMemMonitorName,
						func(inputOne, inputTwo colexecop.Operator) colexecop.Operator {
							unlimitedAllocator := colmem.NewAllocator(
								ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, opName, spec.ProcessorID), factory,
							)
							ehj := colexec.NewExternalHashJoiner(
								unlimitedAllocator,
								flowCtx,
								args,
								hjSpec,
								inputOne, inputTwo,
								result.makeDiskBackedSorterConstructor(ctx, flowCtx, args, opName, factory),
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
					ctx, flowCtx, args, spec.ProcessorID, core.HashJoiner.OnExpr, factory,
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

			opName := "merge-joiner"
			// We are using an unlimited memory monitor here because merge
			// joiner itself is responsible for making sure that we stay within
			// the memory limit, and it will fall back to disk if necessary.
			unlimitedAllocator := colmem.NewAllocator(
				ctx, args.MonitorRegistry.CreateUnlimitedMemAccount(
					ctx, flowCtx, opName, spec.ProcessorID,
				), factory)
			diskAccount := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, spec.ProcessorID)
			mj := colexecjoin.NewMergeJoinOp(
				unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx),
				args.DiskQueueCfg, args.FDSemaphore,
				joinType, inputs[0].Root, inputs[1].Root, leftTypes, rightTypes,
				core.MergeJoiner.LeftOrdering.Columns, core.MergeJoiner.RightOrdering.Columns,
				diskAccount, flowCtx.EvalCtx,
			)

			result.Root = mj
			result.ToClose = append(result.ToClose, mj.(colexecop.Closer))
			result.ColumnTypes = core.MergeJoiner.Type.MakeOutputTypes(leftTypes, rightTypes)

			if onExpr != nil {
				if err = result.planAndMaybeWrapFilter(
					ctx, flowCtx, args, spec.ProcessorID, *onExpr, factory,
				); err != nil {
					return r, err
				}
			}

		case core.Sorter != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			input := inputs[0].Root
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			ordering := core.Sorter.OutputOrdering
			matchLen := core.Sorter.OrderingMatchLen
			limit := core.Sorter.Limit
			result.Root = result.createDiskBackedSort(
				ctx, flowCtx, args, input, result.ColumnTypes, ordering, limit, matchLen, 0, /* maxNumberPartitions */
				spec.ProcessorID, "" /* opNamePrefix */, factory,
			)

		case core.Windower != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			opNamePrefix := "window-"
			input := inputs[0].Root
			result.ColumnTypes = make([]*types.T, len(spec.Input[0].ColumnTypes))
			copy(result.ColumnTypes, spec.Input[0].ColumnTypes)
			for _, wf := range core.Windower.WindowFns {
				// We allocate the capacity for two extra types because of the temporary
				// columns that can be appended below. Capacity is also allocated for
				// each of the argument types in case casting is necessary.
				typs := make([]*types.T, len(result.ColumnTypes), len(result.ColumnTypes)+len(wf.ArgsIdxs)+2)
				copy(typs, result.ColumnTypes)

				// Set any nil values in the window frame to their default values.
				wf.Frame = colexecwindow.NormalizeWindowFrame(wf.Frame)

				tempColOffset := uint32(0)
				argTypes := make([]*types.T, len(wf.ArgsIdxs))
				argIdxs := make([]int, len(wf.ArgsIdxs))
				for i, idx := range wf.ArgsIdxs {
					// Retrieve the type of each argument and perform any necessary casting.
					needsCast, expectedType := colexecwindow.WindowFnArgNeedsCast(wf.Func, typs[idx], i)
					if needsCast {
						// We must cast to the expected argument type.
						castIdx := len(typs)
						input, err = colexecbase.GetCastOperator(
							getStreamingAllocator(ctx, args), input, int(idx),
							castIdx, typs[idx], expectedType, flowCtx.EvalCtx,
						)
						if err != nil {
							colexecerror.InternalError(errors.AssertionFailedf(
								"failed to cast window function argument to type %v", expectedType))
						}
						typs = append(typs, expectedType)
						idx = uint32(castIdx)
						tempColOffset++
					}
					argTypes[i] = expectedType
					argIdxs[i] = int(idx)
				}
				partitionColIdx := tree.NoColumnIdx
				peersColIdx := tree.NoColumnIdx

				if len(core.Windower.PartitionBy) > 0 {
					// TODO(yuzefovich): add support for hashing partitioner
					// (probably by leveraging hash routers once we can
					// distribute). The decision about which kind of partitioner
					// to use should come from the optimizer.
					partitionColIdx = int(wf.OutputColIdx + tempColOffset)
					input = colexecwindow.NewWindowSortingPartitioner(
						getStreamingAllocator(ctx, args), input, typs,
						core.Windower.PartitionBy, wf.Ordering.Columns, partitionColIdx,
						func(input colexecop.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column) colexecop.Operator {
							return result.createDiskBackedSort(
								ctx, flowCtx, args, input, inputTypes,
								execinfrapb.Ordering{Columns: orderingCols}, 0 /*limit */, 0, /* matchLen */
								0 /* maxNumberPartitions */, spec.ProcessorID,
								opNamePrefix, factory)
						},
					)
					// Window partitioner will append a boolean column.
					tempColOffset++
					typs = typs[:len(typs)+1]
					typs[len(typs)-1] = types.Bool
				} else {
					if len(wf.Ordering.Columns) > 0 {
						input = result.createDiskBackedSort(
							ctx, flowCtx, args, input, typs,
							wf.Ordering, 0 /* limit */, 0 /* matchLen */, 0, /* maxNumberPartitions */
							spec.ProcessorID, opNamePrefix, factory,
						)
					}
				}
				if colexecwindow.WindowFnNeedsPeersInfo(&wf) {
					peersColIdx = int(wf.OutputColIdx + tempColOffset)
					input = colexecwindow.NewWindowPeerGrouper(
						getStreamingAllocator(ctx, args), input, typs,
						wf.Ordering.Columns, partitionColIdx, peersColIdx,
					)
					// Window peer grouper will append a boolean column.
					tempColOffset++
					typs = typs[:len(typs)+1]
					typs[len(typs)-1] = types.Bool
				}
				outputIdx := int(wf.OutputColIdx + tempColOffset)

				windowArgs := &colexecwindow.WindowArgs{
					EvalCtx:         flowCtx.EvalCtx,
					MemoryLimit:     execinfra.GetWorkMemLimit(flowCtx),
					QueueCfg:        args.DiskQueueCfg,
					FdSemaphore:     args.FDSemaphore,
					Input:           input,
					InputTypes:      typs,
					OutputColIdx:    outputIdx,
					PartitionColIdx: partitionColIdx,
					PeersColIdx:     peersColIdx,
				}

				// Some window functions always return an INT result, so we use
				// it as default and will update whenever necessary. We cannot
				// use execinfrapb.GetWindowFunctionInfo because that function
				// doesn't distinguish integers with different widths.
				returnType := types.Int
				if wf.Func.WindowFunc != nil {
					// This is a 'pure' window function (e.g. not an aggregation).
					windowFn := *wf.Func.WindowFunc
					// Note that in some cases, window functions will use an unlimited
					// memory monitor. In these cases, the window function itself is
					// responsible for making sure it stays within the memory limit, and
					// will fall back to disk if necessary.
					switch windowFn {
					case execinfrapb.WindowerSpec_ROW_NUMBER:
						windowArgs.MainAllocator = getStreamingAllocator(ctx, args)
						result.Root = colexecwindow.NewRowNumberOperator(windowArgs)
					case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
						windowArgs.MainAllocator = getStreamingAllocator(ctx, args)
						result.Root, err = colexecwindow.NewRankOperator(windowArgs, windowFn, wf.Ordering.Columns)
					case execinfrapb.WindowerSpec_PERCENT_RANK, execinfrapb.WindowerSpec_CUME_DIST:
						opName := opNamePrefix + "relative-rank"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, false, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewRelativeRankOperator(
							windowArgs, windowFn, wf.Ordering.Columns)
						returnType = types.Float
					case execinfrapb.WindowerSpec_NTILE:
						opName := opNamePrefix + "ntile"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, false, /* needsBuffer */
						)
						result.Root = colexecwindow.NewNTileOperator(windowArgs, argIdxs[0])
					case execinfrapb.WindowerSpec_LAG:
						opName := opNamePrefix + "lag"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewLagOperator(
							windowArgs, argIdxs[0], argIdxs[1], argIdxs[2])
						returnType = typs[argIdxs[0]]
					case execinfrapb.WindowerSpec_LEAD:
						opName := opNamePrefix + "lead"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewLeadOperator(
							windowArgs, argIdxs[0], argIdxs[1], argIdxs[2])
						returnType = typs[argIdxs[0]]
					case execinfrapb.WindowerSpec_FIRST_VALUE:
						opName := opNamePrefix + "first_value"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewFirstValueOperator(
							windowArgs, wf.Frame, &wf.Ordering, argIdxs)
						returnType = typs[argIdxs[0]]
					case execinfrapb.WindowerSpec_LAST_VALUE:
						opName := opNamePrefix + "last_value"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewLastValueOperator(
							windowArgs, wf.Frame, &wf.Ordering, argIdxs)
						returnType = typs[argIdxs[0]]
					case execinfrapb.WindowerSpec_NTH_VALUE:
						opName := opNamePrefix + "nth_value"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewNthValueOperator(
							windowArgs, wf.Frame, &wf.Ordering, argIdxs)
						returnType = typs[argIdxs[0]]
					default:
						return r, errors.AssertionFailedf("window function %s is not supported", wf.String())
					}
				} else if wf.Func.AggregateFunc != nil {
					// This is an aggregate window function.
					opName := opNamePrefix + strings.ToLower(wf.Func.AggregateFunc.String())
					result.finishBufferedWindowerArgs(
						ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
						spec.ProcessorID, factory, true, /* needsBuffer */
					)
					aggType := *wf.Func.AggregateFunc
					switch *wf.Func.AggregateFunc {
					case execinfrapb.CountRows:
						// count_rows has a specialized implementation.
						result.Root = colexecwindow.NewCountRowsOperator(windowArgs, wf.Frame, &wf.Ordering)
					default:
						aggArgs := colexecagg.NewAggregatorArgs{
							Allocator:  windowArgs.MainAllocator,
							InputTypes: argTypes,
							EvalCtx:    flowCtx.EvalCtx,
						}
						// The aggregate function will be presented with a ColVec slice
						// containing only the argument columns.
						colIdx := make([]uint32, len(argTypes))
						for i := range argIdxs {
							colIdx[i] = uint32(i)
						}
						aggregations := []execinfrapb.AggregatorSpec_Aggregation{{
							Func:   aggType,
							ColIdx: colIdx,
						}}
						aggArgs.Constructors, aggArgs.ConstArguments, aggArgs.OutputTypes, err =
							colexecagg.ProcessAggregations(flowCtx.EvalCtx, args.ExprHelper.SemaCtx, aggregations, argTypes)
						var toClose colexecop.Closers
						var aggFnsAlloc *colexecagg.AggregateFuncsAlloc
						if (aggType != execinfrapb.Min && aggType != execinfrapb.Max) ||
							wf.Frame.Exclusion != execinfrapb.WindowerSpec_Frame_NO_EXCLUSION ||
							!colexecwindow.WindowFrameCanShrink(wf.Frame, &wf.Ordering) {
							// Min and max window functions have specialized implementations
							// when the frame can shrink and has a default exclusion clause.
							aggFnsAlloc, _, toClose, err = colexecagg.NewAggregateFuncsAlloc(
								&aggArgs, aggregations, 1 /* allocSize */, colexecagg.WindowAggKind,
							)
							if err != nil {
								colexecerror.InternalError(err)
							}
						}
						result.Root = colexecwindow.NewWindowAggregatorOperator(
							windowArgs, aggType, wf.Frame, &wf.Ordering, argIdxs,
							aggArgs.OutputTypes[0], aggFnsAlloc, toClose)
						returnType = aggArgs.OutputTypes[0]
					}
				} else {
					colexecerror.InternalError(errors.AssertionFailedf("window function spec is nil"))
				}
				if c, ok := result.Root.(colexecop.Closer); ok {
					result.ToClose = append(result.ToClose, c)
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
					result.Root = colexecbase.NewSimpleProjectOp(result.Root, int(wf.OutputColIdx+tempColOffset), projection)
				}

				result.ColumnTypes = appendOneType(result.ColumnTypes, returnType)
				input = result.Root
			}

		default:
			return r, errors.AssertionFailedf("unsupported processor core %q", core)
		}
	}

	if err != nil {
		return r, err
	}

	// Note: at this point, it is legal for ColumnTypes to be empty (it is
	// legal for empty rows to be passed between processors).

	ppr := postProcessResult{
		Op:          result.Root,
		ColumnTypes: result.ColumnTypes,
	}
	err = ppr.planPostProcessSpec(ctx, flowCtx, args, post, factory, &r.Releasables)
	if err != nil {
		err = result.wrapPostProcessSpec(ctx, flowCtx, args, post, args.Spec.ResultTypes, factory, err)
	} else {
		// The result can be updated with the post process result.
		result.updateWithPostProcessResult(ppr)
	}
	if err != nil {
		return r, err
	}

	// Check that the actual output types are equal to the expected ones and
	// plan casts if they are not.
	//
	// For each output column we check whether the actual and expected types are
	// identical, and if not, whether we support a native vectorized cast
	// between them. If for at least one column the native cast is not
	// supported, we will plan a wrapped row-execution noop processor that will
	// be responsible for casting all mismatched columns (and for performing the
	// projection of the original no longer needed types).
	// TODO(yuzefovich): consider whether planning some vectorized casts is
	// worth it even if we need to plan a wrapped processor for some other
	// columns.
	if len(args.Spec.ResultTypes) != len(r.ColumnTypes) {
		return r, errors.AssertionFailedf("unexpectedly different number of columns are output: expected %v, actual %v", args.Spec.ResultTypes, r.ColumnTypes)
	}
	numMismatchedTypes, needWrappedCast := 0, false
	for i := range args.Spec.ResultTypes {
		expected, actual := args.Spec.ResultTypes[i], r.ColumnTypes[i]
		if !actual.Identical(expected) {
			numMismatchedTypes++
			if !colexecbase.IsCastSupported(actual, expected) {
				needWrappedCast = true
			}
		}
	}

	if needWrappedCast {
		post := &execinfrapb.PostProcessSpec{
			RenderExprs: make([]execinfrapb.Expression, len(args.Spec.ResultTypes)),
		}
		for i := range args.Spec.ResultTypes {
			expected, actual := args.Spec.ResultTypes[i], r.ColumnTypes[i]
			if !actual.Identical(expected) {
				post.RenderExprs[i].LocalExpr = tree.NewTypedCastExpr(tree.NewTypedOrdinalReference(i, actual), expected)
			} else {
				post.RenderExprs[i].LocalExpr = tree.NewTypedOrdinalReference(i, args.Spec.ResultTypes[i])
			}
		}
		if err = result.wrapPostProcessSpec(ctx, flowCtx, args, post, args.Spec.ResultTypes, factory, errWrappedCast); err != nil {
			return r, err
		}
	} else if numMismatchedTypes > 0 {
		// We will need to project out the original mismatched columns, so
		// we're keeping track of the required projection.
		projection := make([]uint32, len(args.Spec.ResultTypes))
		typesWithCasts := make([]*types.T, len(args.Spec.ResultTypes), len(args.Spec.ResultTypes)+numMismatchedTypes)
		// All original mismatched columns will be passed through by all of the
		// vectorized cast operators.
		copy(typesWithCasts, r.ColumnTypes)
		for i := range args.Spec.ResultTypes {
			expected, actual := args.Spec.ResultTypes[i], r.ColumnTypes[i]
			if !actual.Identical(expected) {
				castedIdx := len(typesWithCasts)
				r.Root, err = colexecbase.GetCastOperator(
					getStreamingAllocator(ctx, args), r.Root, i, castedIdx,
					actual, expected, flowCtx.EvalCtx,
				)
				if err != nil {
					return r, errors.NewAssertionErrorWithWrappedErrf(err, "unexpectedly couldn't plan a cast although IsCastSupported returned true")
				}
				projection[i] = uint32(castedIdx)
				typesWithCasts = append(typesWithCasts, expected)
			} else {
				projection[i] = uint32(i)
			}
		}
		r.Root, r.ColumnTypes = addProjection(r.Root, typesWithCasts, projection)
	}

	takeOverMetaInfo(&result.OpWithMetaInfo, inputs)
	if buildutil.CrdbTestBuild {
		// Plan an invariants checker if it isn't already the root of the
		// tree.
		if i := colexec.MaybeUnwrapInvariantsChecker(r.Root); i == r.Root {
			r.Root = colexec.NewInvariantsChecker(r.Root)
		}
		// Also verify planning assumptions.
		args.MonitorRegistry.AssertInvariants()
	}
	return r, err
}

// planAndMaybeWrapFilter plans a filter. If the filter is unsupported, it is
// planned as a wrapped filterer processor.
func (r opResult) planAndMaybeWrapFilter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	processorID int32,
	filter execinfrapb.Expression,
	factory coldata.ColumnFactory,
) error {
	op, err := planFilterExpr(
		ctx, flowCtx, r.Root, r.ColumnTypes, filter, args.StreamingMemAccount, factory, args.ExprHelper, &r.Releasables,
	)
	if err != nil {
		// Filter expression planning failed. Fall back to planning the filter
		// using row execution.
		filtererSpec := &execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{
				Filterer: &execinfrapb.FiltererSpec{
					Filter: filter,
				},
			},
			ProcessorID: processorID,
			ResultTypes: args.Spec.ResultTypes,
		}
		inputToMaterializer := colexecargs.OpWithMetaInfo{Root: r.Root}
		takeOverMetaInfo(&inputToMaterializer, args.Inputs)
		return r.createAndWrapRowSource(
			ctx, flowCtx, args, []colexecargs.OpWithMetaInfo{inputToMaterializer},
			[][]*types.T{r.ColumnTypes}, filtererSpec, factory, err,
		)
	}
	r.Root = op
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
	inputToMaterializer := colexecargs.OpWithMetaInfo{Root: r.Root}
	takeOverMetaInfo(&inputToMaterializer, args.Inputs)
	// createAndWrapRowSource updates r.ColumnTypes accordingly.
	return r.createAndWrapRowSource(
		ctx, flowCtx, args, []colexecargs.OpWithMetaInfo{inputToMaterializer},
		[][]*types.T{r.ColumnTypes}, noopSpec, factory, causeToWrap,
	)
}

// planPostProcessSpec plans the post processing stage specified in post on top
// of r.Op.
func (r *postProcessResult) planPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	post *execinfrapb.PostProcessSpec,
	factory coldata.ColumnFactory,
	releasables *[]execinfra.Releasable,
) error {
	if post.Projection {
		r.Op, r.ColumnTypes = addProjection(r.Op, r.ColumnTypes, post.OutputColumns)
	} else if post.RenderExprs != nil {
		var renderedCols []uint32
		for _, renderExpr := range post.RenderExprs {
			expr, err := args.ExprHelper.ProcessExpr(renderExpr, flowCtx.EvalCtx, r.ColumnTypes)
			if err != nil {
				return err
			}
			var outputIdx int
			r.Op, outputIdx, r.ColumnTypes, err = planProjectionOperators(
				ctx, flowCtx.EvalCtx, expr, r.ColumnTypes, r.Op, args.StreamingMemAccount, factory, releasables,
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

type postProcessResult struct {
	Op          colexecop.Operator
	ColumnTypes []*types.T
}

func (r opResult) updateWithPostProcessResult(ppr postProcessResult) {
	r.Root = ppr.Op
	r.ColumnTypes = make([]*types.T, len(ppr.ColumnTypes))
	copy(r.ColumnTypes, ppr.ColumnTypes)
}

func (r opResult) finishBufferedWindowerArgs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	monitorRegistry *colexecargs.MonitorRegistry,
	args *colexecwindow.WindowArgs,
	opName string,
	processorID int32,
	factory coldata.ColumnFactory,
	needsBuffer bool,
) {
	args.DiskAcc = monitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, processorID)
	mainAcc := monitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, opName, processorID)
	args.MainAllocator = colmem.NewAllocator(ctx, mainAcc, factory)
	if needsBuffer {
		bufferAcc := monitorRegistry.CreateUnlimitedMemAccount(ctx, flowCtx, opName, processorID)
		args.BufferAllocator = colmem.NewAllocator(ctx, bufferAcc, factory)
	}
}

func (r opResult) finishScanPlanning(op colfetcher.ScanOperator, resultTypes []*types.T) {
	r.Root = op
	if buildutil.CrdbTestBuild {
		r.Root = colexec.NewInvariantsChecker(r.Root)
	}
	r.KVReader = op
	r.MetadataSources = append(r.MetadataSources, r.Root.(colexecop.MetadataSource))
	r.Releasables = append(r.Releasables, op)

	// We want to check for cancellation once per input batch, and
	// wrapping only colBatchScan with a CancelChecker allows us to do
	// just that. It's sufficient for most of the operators since they
	// are extremely fast. However, some of the long-running operators
	// (for example, sorter) are still responsible for doing the
	// cancellation check on their own while performing long operations.
	r.Root = colexecutils.NewCancelChecker(r.Root)
	r.ColumnTypes = resultTypes
	r.ToClose = append(r.ToClose, op)
}

// planFilterExpr creates all operators to implement filter expression.
func planFilterExpr(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecop.Operator,
	columnTypes []*types.T,
	filter execinfrapb.Expression,
	acc *mon.BoundAccount,
	factory coldata.ColumnFactory,
	helper *colexecargs.ExprHelper,
	releasables *[]execinfra.Releasable,
) (colexecop.Operator, error) {
	expr, err := helper.ProcessExpr(filter, flowCtx.EvalCtx, columnTypes)
	if err != nil {
		return nil, err
	}
	if expr == tree.DNull {
		// The filter expression is tree.DNull meaning that it is always false, so
		// we put a zero operator.
		return colexecutils.NewZeroOp(input), nil
	}
	op, _, filterColumnTypes, err := planSelectionOperators(
		ctx, flowCtx.EvalCtx, expr, columnTypes, input, acc, factory, releasables,
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
	releasables *[]execinfra.Releasable,
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
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		rightOp, resultIdx, typs, err = planSelectionOperators(
			ctx, evalCtx, t.TypedRight(), typs, leftOp, acc, factory, releasables,
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
			ctx, evalCtx, caseExpr, columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		op, err = colexecutils.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, err
	case *tree.CaseExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op, err = colexecutils.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, err
	case *tree.IsNullExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory, releasables,
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
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op = colexec.NewIsNullSelOp(
			op, resultIdx, true /* negate */, typs[resultIdx].Family() == types.TupleFamily,
		)
		return op, resultIdx, typs, err
	case *tree.NotExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op, err = colexec.NewNotExprSelOp(t.TypedInnerExpr().ResolvedType().Family(), op, resultIdx)
		return op, resultIdx, typs, err
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, err := planProjectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		lTyp := ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			switch cmpOp.Symbol {
			case treecmp.Like, treecmp.NotLike:
				negate := cmpOp.Symbol == treecmp.NotLike
				op, err = colexecsel.GetLikeOperator(
					evalCtx, leftOp, leftIdx, string(tree.MustBeDString(constArg)), negate,
				)
			case treecmp.In, treecmp.NotIn:
				negate := cmpOp.Symbol == treecmp.NotIn
				datumTuple, ok := tree.AsDTuple(constArg)
				if !ok || useDefaultCmpOpForIn(datumTuple) {
					break
				}
				op, err = colexec.GetInOperator(evalCtx, lTyp, leftOp, leftIdx, datumTuple, negate)
			case treecmp.IsDistinctFrom, treecmp.IsNotDistinctFrom:
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
				negate := cmpOp.Symbol == treecmp.IsDistinctFrom
				op = colexec.NewIsNullSelOp(leftOp, leftIdx, negate, false /* isTupleNull */)
			}
			if op == nil || err != nil {
				// op hasn't been created yet, so let's try the constructor for
				// all other selection operators.
				op, err = colexecsel.GetSelectionConstOperator(
					cmpOp, leftOp, ct, leftIdx, constArg, evalCtx, t,
				)
				if r, ok := op.(execinfra.Releasable); ok {
					*releasables = append(*releasables, r)
				}
			}
			return op, resultIdx, ct, err
		}
		rightOp, rightIdx, ct, err := planProjectionOperators(
			ctx, evalCtx, t.TypedRight(), ct, leftOp, acc, factory, releasables,
		)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		op, err = colexecsel.GetSelectionOperator(
			cmpOp, rightOp, ct, leftIdx, rightIdx, evalCtx, t,
		)
		if r, ok := op.(execinfra.Releasable); ok {
			*releasables = append(*releasables, r)
		}
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
	evalCtx *tree.EvalContext,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	outputIdx := len(columnTypes)
	op, err = colexecbase.GetCastOperator(colmem.NewAllocator(ctx, acc, factory), input, inputIdx, outputIdx, fromType, toType, evalCtx)
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
	releasables *[]execinfra.Releasable,
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
			columnTypes, input, acc, factory, nil /* binFn */, t, releasables,
		)
	case *tree.BinaryExpr:
		if err = checkSupportedBinaryExpr(t.TypedLeft(), t.TypedRight(), t.ResolvedType()); err != nil {
			return op, resultIdx, typs, err
		}
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(),
			columnTypes, input, acc, factory, t.Fn.Fn, nil /* cmpExpr */, releasables,
		)
	case *tree.IsNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, acc, false /* negate */, factory, releasables)
	case *tree.IsNotNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, acc, true /* negate */, factory, releasables)
	case *tree.NotExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		outputIdx, allocator := len(typs), colmem.NewAllocator(ctx, acc, factory)
		op, err = colexec.NewNotExprProjOp(
			t.TypedInnerExpr().ResolvedType().Family(), allocator, op, resultIdx, outputIdx,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		typs = appendOneType(typs, t.ResolvedType())
		return op, outputIdx, typs, nil
	case *tree.CastExpr:
		expr := t.Expr.(tree.TypedExpr)
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return nil, 0, nil, err
		}
		op, resultIdx, typs, err = planCastOperator(ctx, acc, typs, op, resultIdx, expr.ResolvedType(), t.ResolvedType(), factory, evalCtx)
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
				ctx, evalCtx, e.(tree.TypedExpr), typs, op, acc, factory, releasables,
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
		if r, ok := op.(execinfra.Releasable); ok {
			*releasables = append(*releasables, r)
		}
		typs = appendOneType(typs, t.ResolvedType())
		return op, resultIdx, typs, err
	case tree.Datum:
		op, err = projectDatum(t)
		return op, resultIdx, typs, err
	case *tree.Tuple:
		tuple, isConstTuple := evalTupleIfConst(evalCtx, t)
		if isConstTuple {
			// Tuple expression is a constant, so we can just project the
			// resulting datum.
			op, err = projectDatum(tuple)
			return op, resultIdx, typs, err
		}
		outputType := t.ResolvedType()
		typs = make([]*types.T, len(columnTypes))
		copy(typs, columnTypes)
		tupleContentsIdxs := make([]int, len(t.Exprs))
		for i, expr := range t.Exprs {
			input, tupleContentsIdxs[i], typs, err = planProjectionOperators(
				ctx, evalCtx, expr.(tree.TypedExpr), typs, input, acc, factory, releasables,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
		}
		resultIdx = len(typs)
		op = colexec.NewTupleProjOp(
			colmem.NewAllocator(ctx, acc, factory), typs, tupleContentsIdxs, outputType, input, resultIdx,
		)
		*releasables = append(*releasables, op.(execinfra.Releasable))
		typs = appendOneType(typs, outputType)
		return op, resultIdx, typs, err
	case *tree.CaseExpr:
		allocator := colmem.NewAllocator(ctx, acc, factory)
		caseOutputType := t.ResolvedType()
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
				ctx, evalCtx, whenTyped, typs, buffer, acc, factory, releasables,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
			if t.Expr != nil {
				// If we have 'CASE <expr> WHEN ...' form, then we need to
				// evaluate the equality between '<expr>' and whatever has been
				// projected by this WHEN arm. We do so by constructing the
				// corresponding comparison expr and letting the planning do its
				// job.
				left := t.Expr.(tree.TypedExpr)
				// The result of evaluation of this WHEN arm is put at position
				// resultIdx, so we simply create an ordinal referencing that
				// column.
				right := tree.NewTypedOrdinalReference(resultIdx, whenTyped.ResolvedType())
				cmpExpr := tree.NewTypedComparisonExpr(treecmp.MakeComparisonOperator(treecmp.EQ), left, right)
				caseOps[i], resultIdx, typs, err = planProjectionOperators(
					ctx, evalCtx, cmpExpr, typs, caseOps[i], acc, factory, releasables,
				)
				if err != nil {
					return nil, resultIdx, typs, err
				}
			}
			caseOps[i], err = colexecutils.BoolOrUnknownToSelOp(caseOps[i], typs, resultIdx)
			if err != nil {
				return nil, resultIdx, typs, err
			}

			// Run the "then" clause on those tuples that were selected.
			caseOps[i], thenIdxs[i], typs, err = planProjectionOperators(
				ctx, evalCtx, when.Val.(tree.TypedExpr), typs, caseOps[i], acc, factory, releasables,
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
					ctx, acc, typs, caseOps[i], thenIdxs[i], fromType, toType, factory, evalCtx,
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
			ctx, evalCtx, elseExpr.(tree.TypedExpr), typs, buffer, acc, factory, releasables,
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
				ctx, acc, typs, elseOp, elseIdx, fromType, toType, factory, evalCtx,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
		}

		schemaEnforcer.SetTypes(typs)
		op := colexec.NewCaseOp(allocator, buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)
		return op, caseOutputIdx, typs, err
	case *tree.AndExpr, *tree.OrExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, acc, factory, releasables)
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
	if (leftDatumBacked && rightDatumBacked) && !outputDatumBacked {
		return errors.New("datum-backed arguments on both sides and not datum-backed " +
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
	releasables *[]execinfra.Releasable,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	if err := checkSupportedProjectionExpr(left, right); err != nil {
		return nil, resultIdx, typs, err
	}
	allocator := colmem.NewAllocator(ctx, acc, factory)
	resultIdx = -1

	cmpProjOp, isCmpProjOp := projOp.(treecmp.ComparisonOperator)
	var hasOptimizedOp bool
	if isCmpProjOp {
		switch cmpProjOp.Symbol {
		case treecmp.Like, treecmp.NotLike, treecmp.In, treecmp.NotIn, treecmp.IsDistinctFrom, treecmp.IsNotDistinctFrom:
			hasOptimizedOp = true
		}
	}
	// There are 3 cases. Either the left is constant, the right is constant,
	// or neither are constant.
	if lConstArg, lConst := left.(tree.Datum); lConst && !hasOptimizedOp {
		// Case one: The left is constant (and we don't have an optimized
		// operator for this expression).
		// Normally, the optimizer normalizes binary exprs so that the constant
		// argument is on the right side. This doesn't happen for
		// non-commutative operators such as - and /, though, so we still need
		// this case.
		var rightIdx int
		input, rightIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, right, columnTypes, input, acc, factory, releasables,
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
			ctx, evalCtx, left, columnTypes, input, acc, factory, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}

		if tuple, ok := right.(*tree.Tuple); ok {
			// If we have a tuple on the right, try to pre-evaluate it in case
			// it consists only of constant expressions.
			tupleDatum, ok := evalTupleIfConst(evalCtx, tuple)
			if ok {
				right = tupleDatum
			}
		}

		// We have a special case behavior for Is{Not}DistinctFrom before
		// checking whether the right expression is constant below in order to
		// extract NULL from the cast expression.
		//
		// Normally, the optimizer folds all constants; however, for nulls it
		// creates a cast expression from tree.DNull to the desired type in
		// order to propagate the type of the null. We need to extract the
		// constant NULL so that the optimized operator was planned below.
		if isCmpProjOp && (cmpProjOp.Symbol == treecmp.IsDistinctFrom || cmpProjOp.Symbol == treecmp.IsNotDistinctFrom) {
			if cast, ok := right.(*tree.CastExpr); ok {
				if cast.Expr == tree.DNull {
					right = tree.DNull
				}
			}
		}
		if rConstArg, rConst := right.(tree.Datum); rConst {
			// Case 2: The right is constant.
			// The projection result will be outputted to a new column which is
			// appended to the input batch.
			resultIdx = len(typs)
			if isCmpProjOp {
				switch cmpProjOp.Symbol {
				case treecmp.Like, treecmp.NotLike:
					negate := cmpProjOp.Symbol == treecmp.NotLike
					op, err = colexecproj.GetLikeProjectionOperator(
						allocator, evalCtx, input, leftIdx, resultIdx,
						string(tree.MustBeDString(rConstArg)), negate,
					)
				case treecmp.In, treecmp.NotIn:
					negate := cmpProjOp.Symbol == treecmp.NotIn
					datumTuple, ok := tree.AsDTuple(rConstArg)
					if !ok || useDefaultCmpOpForIn(datumTuple) {
						break
					}
					op, err = colexec.GetInProjectionOperator(
						evalCtx, allocator, typs[leftIdx], input, leftIdx, resultIdx, datumTuple, negate,
					)
				case treecmp.IsDistinctFrom, treecmp.IsNotDistinctFrom:
					if right != tree.DNull {
						// Optimized IsDistinctFrom and IsNotDistinctFrom are
						// supported only with NULL argument, so we fallback to the
						// default comparison operator.
						break
					}
					// IS NULL is replaced with IS NOT DISTINCT FROM NULL, so we
					// want to negate when IS DISTINCT FROM is used.
					negate := cmpProjOp.Symbol == treecmp.IsDistinctFrom
					op = colexec.NewIsNullProjOp(
						allocator, input, leftIdx, resultIdx, negate, false, /* isTupleNull */
					)
				}
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
				ctx, evalCtx, right, typs, input, acc, factory, releasables,
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
	if r, ok := op.(execinfra.Releasable); ok {
		*releasables = append(*releasables, r)
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
	releasables *[]execinfra.Releasable,
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
		ctx, evalCtx, typedLeft, typs, leftFeedOp, acc, factory, releasables,
	)
	if err != nil {
		return nil, resultIdx, typs, err
	}
	rightProjOpChain, rightIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, typedRight, typs, rightFeedOp, acc, factory, releasables,
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
	releasables *[]execinfra.Releasable,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	op, resultIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, expr, columnTypes, input, acc, factory, releasables,
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

// useDefaultCmpOpForIn returns whether IN and NOT IN projection/selection
// operators should be handled via the default operators. This is the case when
// we have an empty tuple or the tuple contains other tuples (these cases
// require special null-handling logic).
func useDefaultCmpOpForIn(tuple *tree.DTuple) bool {
	tupleContents := tuple.ResolvedType().TupleContents()
	if len(tupleContents) == 0 {
		return true
	}
	for _, typ := range tupleContents {
		if typ.Family() == types.TupleFamily {
			return true
		}
	}
	return false
}

// evalTupleIfConst checks whether t contains only constant (i.e. tree.Datum)
// expressions and evaluates the tuple if so.
func evalTupleIfConst(evalCtx *tree.EvalContext, t *tree.Tuple) (_ tree.Datum, ok bool) {
	for _, expr := range t.Exprs {
		if _, isDatum := expr.(tree.Datum); !isDatum {
			// Not a constant expression.
			return nil, false
		}
	}
	tuple, err := t.Eval(evalCtx)
	if err != nil {
		return nil, false
	}
	return tuple, true
}
