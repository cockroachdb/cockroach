// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colbuilder

import (
	"context"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecdisk"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecproj"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecprojconst"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecsel"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecwindow"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colfetcher"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execreleasable"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func checkNumIn(inputs []colexecargs.OpWithMetaInfo, numIn int) error {
	if len(inputs) != numIn {
		return errors.AssertionFailedf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// wrapRowSources, given input Operators, integrates toWrap into a columnar
// execution flow and returns toWrap's output as an Operator.
func wrapRowSources(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	inputs []colexecargs.OpWithMetaInfo,
	inputTypes [][]*types.T,
	processorID int32,
	newToWrap func([]execinfra.RowSource) (execinfra.RowSource, error),
	factory coldata.ColumnFactory,
	releasables *[]execreleasable.Releasable,
) (*colexec.Columnarizer, error) {
	var toWrapInputs []execinfra.RowSource
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
				getStreamingMemAccount(args, flowCtx),
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
			toWrapInputs = append(toWrapInputs, toWrapInput)
			*releasables = append(*releasables, toWrapInput)
		}
	}

	toWrap, err := newToWrap(toWrapInputs)
	if err != nil {
		return nil, err
	}

	proc, isProcessor := toWrap.(execinfra.Processor)
	if !isProcessor {
		return nil, errors.AssertionFailedf("unexpectedly %T is not an execinfra.Processor", toWrap)
	}
	batchAllocator := colmem.NewAllocator(ctx, args.MonitorRegistry.NewStreamingMemAccount(flowCtx), factory)
	var c *colexec.Columnarizer
	if proc.MustBeStreaming() {
		c = colexec.NewStreamingColumnarizer(batchAllocator, getStreamingMemAccount(args, flowCtx), flowCtx, processorID, toWrap)
	} else {
		c = colexec.NewBufferingColumnarizer(batchAllocator, getStreamingMemAccount(args, flowCtx), flowCtx, processorID, toWrap)
	}
	return c, nil
}

type opResult struct {
	*colexecargs.NewColOperatorResult
}

// IsSupported returns an error if the given spec is not supported by the
// vectorized engine (neither natively nor by wrapping the corresponding row
// execution processor).
func IsSupported(mode sessiondatapb.VectorizeExecMode, spec *execinfrapb.ProcessorSpec) error {
	err := supportedNatively(&spec.Core)
	if err != nil {
		if wrapErr := canWrap(mode, &spec.Core); wrapErr == nil {
			// We don't support this spec natively, but we can wrap the row
			// execution processor.
			return nil
		}
	}
	return err
}

// supportedNatively checks whether we have a columnar operator equivalent to a
// processor described by core. Note that it doesn't perform any other checks
// (like validity of the number of inputs).
func supportedNatively(core *execinfrapb.ProcessorCoreUnion) error {
	switch {
	case core.Noop != nil:
		return nil

	case core.Values != nil:
		return nil

	case core.TableReader != nil:
		return nil

	case core.JoinReader != nil:
		if !core.JoinReader.IsIndexJoin() {
			return errLookupJoinUnsupported
		}
		return nil

	case core.Filterer != nil:
		return nil

	case core.Aggregator != nil:
		for _, agg := range core.Aggregator.Aggregations {
			if agg.FilterColIdx != nil {
				return errFilteringAggregation
			}
		}
		return nil

	case core.Distinct != nil:
		return nil

	case core.Ordinality != nil:
		return nil

	case core.HashJoiner != nil:
		if !core.HashJoiner.OnExpr.Empty() && core.HashJoiner.Type != descpb.InnerJoin {
			return errNonInnerHashJoinWithOnExpr
		}
		return nil

	case core.MergeJoiner != nil:
		if !core.MergeJoiner.OnExpr.Empty() && core.MergeJoiner.Type != descpb.InnerJoin {
			return errNonInnerMergeJoinWithOnExpr
		}
		return nil

	case core.HashGroupJoiner != nil:
		return nil

	case core.Sorter != nil:
		return nil

	case core.Windower != nil:
		for _, wf := range core.Windower.WindowFns {
			if wf.FilterColIdx != tree.NoColumnIdx {
				return errWindowFunctionFilterClause
			}
			if wf.Func.AggregateFunc != nil {
				if !colexecagg.IsAggOptimized(*wf.Func.AggregateFunc) {
					return errDefaultAggregateWindowFunction
				}
			}
		}
		return nil

	case core.LocalPlanNode != nil:
		// LocalPlanNode core is special (we don't have any plans on vectorizing
		// it at the moment), so we want to return a custom error for it to
		// distinguish from other unsupported cores.
		return errLocalPlanNodeWrap

	case core.Insert != nil:
		return nil

	default:
		return errCoreUnsupportedNatively
	}
}

var (
	errCoreUnsupportedNatively        = errors.New("unsupported processor core")
	errLocalPlanNodeWrap              = errors.New("LocalPlanNode core needs to be wrapped")
	errChangeAggregatorWrap           = errors.New("core.ChangeAggregator is not supported")
	errChangeFrontierWrap             = errors.New("core.ChangeFrontier is not supported")
	errReadImportWrap                 = errors.New("core.ReadImport is not supported")
	errBackupDataWrap                 = errors.New("core.BackupData is not supported")
	errBackfillerWrap                 = errors.New("core.Backfiller is not supported (not an execinfra.RowSource)")
	errExporterWrap                   = errors.New("core.Exporter is not supported (not an execinfra.RowSource)")
	errSamplerWrap                    = errors.New("core.Sampler is not supported (not an execinfra.RowSource)")
	errSampleAggregatorWrap           = errors.New("core.SampleAggregator is not supported (not an execinfra.RowSource)")
	errExperimentalWrappingProhibited = errors.Newf("wrapping for non-JoinReader and non-LocalPlanNode cores is prohibited in vectorize=%s", sessiondatapb.VectorizeExperimentalAlways)
	errWrappedCast                    = errors.New("mismatched types in NewColOperator and unsupported casts")
	errLookupJoinUnsupported          = errors.New("lookup join reader is unsupported in vectorized")
	errFilteringAggregation           = errors.New("filtering aggregation not supported")
	errNonInnerHashJoinWithOnExpr     = errors.New("can't plan vectorized non-inner hash joins with ON expressions")
	errNonInnerMergeJoinWithOnExpr    = errors.New("can't plan vectorized non-inner merge joins with ON expressions")
	errWindowFunctionFilterClause     = errors.New("window functions with FILTER clause are not supported")
	errDefaultAggregateWindowFunction = errors.New("default aggregate window functions not supported")
	errStreamIngestionWrap            = errors.New("core.StreamIngestion{Data,Frontier} is not supported because of #55758")
	errFallbackToRenderWrapping       = errors.New("falling back to wrapping a row-by-row processor due to many renders and low estimated row count")
	errUnhandledSelectionExpression   = errors.New("unhandled selection expression")
	errUnhandledProjectionExpression  = errors.New("unhandled projection expression")

	errBinaryExprWithDatums = unimplemented.NewWithIssue(
		49780, "datum-backed arguments on both sides and not datum-backed "+
			"output of a binary expression is currently not supported",
	)
	errMixedTypeBinaryUnsupported = unimplemented.NewWithIssue(
		46198, "dates and timestamptz not supported in mixed-type binary "+
			"expressions in the vectorized engine",
	)
	errMixedTypeComparisonUnsupported = unimplemented.NewWithIssue(
		44770, "dates and timestamp(tz) not supported in mixed-type "+
			"comparison expressions in the vectorized engine",
	)
)

func canWrap(mode sessiondatapb.VectorizeExecMode, core *execinfrapb.ProcessorCoreUnion) error {
	if mode == sessiondatapb.VectorizeExperimentalAlways && core.JoinReader == nil && core.LocalPlanNode == nil {
		return errExperimentalWrappingProhibited
	}
	switch {
	case core.Noop != nil:
	case core.TableReader != nil:
	case core.JoinReader != nil:
	case core.Sorter != nil:
	case core.Aggregator != nil:
	case core.Distinct != nil:
	case core.MergeJoiner != nil:
	case core.HashJoiner != nil:
	case core.Values != nil:
	case core.Backfiller != nil:
		return errBackfillerWrap
	case core.ReadImport != nil:
		return errReadImportWrap
	case core.Exporter != nil:
		return errExporterWrap
	case core.Sampler != nil:
		return errSamplerWrap
	case core.SampleAggregator != nil:
		return errSampleAggregatorWrap
	case core.ZigzagJoiner != nil:
	case core.ProjectSet != nil:
	case core.Windower != nil:
	case core.LocalPlanNode != nil:
	case core.ChangeAggregator != nil:
		// Currently, there is an issue with cleaning up the changefeed flows
		// (#55408), so we fallback to the row-by-row engine.
		return errChangeAggregatorWrap
	case core.ChangeFrontier != nil:
		// Currently, there is an issue with cleaning up the changefeed flows
		// (#55408), so we fallback to the row-by-row engine.
		return errChangeFrontierWrap
	case core.Ordinality != nil:
	case core.BulkRowWriter != nil:
	case core.InvertedFilterer != nil:
	case core.InvertedJoiner != nil:
	case core.BackupData != nil:
		return errBackupDataWrap
	case core.RestoreData != nil:
	case core.Filterer != nil:
	case core.StreamIngestionData != nil:
		return errStreamIngestionWrap
	case core.StreamIngestionFrontier != nil:
		return errStreamIngestionWrap
	case core.HashGroupJoiner != nil:
	default:
		return errors.AssertionFailedf("unexpected processor core %q", core)
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
// - diskBackedReuseMode indicates whether this disk-backed sort can be used
// multiple times.
func createDiskBackedSort(
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
	opNamePrefix redact.SafeString,
	factory coldata.ColumnFactory,
	diskBackedReuseMode colexecop.BufferingOpReuseMode,
) colexecop.Operator {
	var (
		sorterMemMonitorName redact.SafeString
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
		opName := opNamePrefix + "topk-sort"
		var topKSorterMemAccount *mon.BoundAccount
		topKSorterMemAccount, sorterMemMonitorName = args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
			ctx, flowCtx, spoolMemLimit, opName, processorID,
		)
		unlimitedMemAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
			ctx, flowCtx, opName, processorID,
		)
		inMemorySorter = colexec.NewTopKSorter(
			colmem.NewLimitedAllocator(ctx, topKSorterMemAccount, unlimitedMemAcc, factory), input,
			inputTypes, ordering.Columns, int(matchLen), uint64(limit), maxOutputBatchMemSize,
		)
	} else if matchLen > 0 {
		// The input is already partially ordered. Use a chunks sorter to avoid
		// loading all the rows into memory.
		opName := opNamePrefix + "sort-chunks"
		accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
			ctx, flowCtx, opName, processorID, 2, /* numAccounts */
		)
		deselectorUnlimitedAllocator := colmem.NewAllocator(ctx, accounts[0], factory)
		var sortChunksMemAccount *mon.BoundAccount
		sortChunksMemAccount, sorterMemMonitorName = args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
			ctx, flowCtx, spoolMemLimit, opName, processorID,
		)
		inMemorySorter = colexec.NewSortChunks(
			deselectorUnlimitedAllocator,
			colmem.NewLimitedAllocator(ctx, sortChunksMemAccount, accounts[1], factory),
			input, inputTypes, ordering.Columns, int(matchLen), maxOutputBatchMemSize,
		)
	} else {
		// No optimizations possible. Default to the standard sort operator.
		var sorterMemAccount *mon.BoundAccount
		opName := opNamePrefix + "sort-all"
		sorterMemAccount, sorterMemMonitorName = args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
			ctx, flowCtx, spoolMemLimit, opName, processorID,
		)
		unlimitedMemAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
			ctx, flowCtx, opName, processorID,
		)
		inMemorySorter = colexec.NewSorter(
			colmem.NewLimitedAllocator(ctx, sorterMemAccount, unlimitedMemAcc, factory),
			input, inputTypes, ordering.Columns, maxOutputBatchMemSize,
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
	diskSpiller := colexecdisk.NewOneInputDiskSpiller(
		input, inMemorySorter.(colexecop.BufferingInMemoryOperator),
		sorterMemMonitorName,
		func(input colexecop.Operator) colexecop.Operator {
			opName := opNamePrefix + "external-sorter"
			// We are using unlimited memory accounts here because external
			// sort itself is responsible for making sure that we stay within
			// the memory limit.
			accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
				ctx, flowCtx, opName, processorID, 4, /* numAccounts */
			)
			sortUnlimitedAllocator := colmem.NewAllocator(ctx, accounts[0], factory)
			mergeUnlimitedAllocator := colmem.NewAllocator(ctx, accounts[1], factory)
			outputUnlimitedAllocator := colmem.NewAllocator(ctx, accounts[2], factory)
			es := colexecdisk.NewExternalSorter(
				flowCtx,
				processorID,
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
				args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, processorID),
				accounts[3],
				flowCtx.TestingKnobs().VecFDsToAcquire,
			)
			args.CloserRegistry.AddCloser(es.(colexecop.Closer))
			return es
		},
		diskBackedReuseMode,
		args.TestingKnobs.SpillingCallbackFn,
	)
	args.CloserRegistry.AddCloser(diskSpiller)
	return diskSpiller
}

// makeDiskBackedSorterConstructor creates a colexec.DiskBackedSorterConstructor
// that can be used by the hash-based partitioner.
// NOTE: unless DelegateFDAcquisitions testing knob is set to true, it is up to
// the caller to acquire the necessary file descriptors up front.
func makeDiskBackedSorterConstructor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	opNamePrefix redact.SafeString,
	factory coldata.ColumnFactory,
) colexecdisk.DiskBackedSorterConstructor {
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
		// Given that this disk-backed sort is created for the fallback strategy
		// of the hash-based partitioner, it can be reused multiple times (in
		// the worst case, for each partition).
		const reuseMode = colexecop.BufferingOpCanReuse
		return createDiskBackedSort(
			ctx, flowCtx, &sortArgs, input, inputTypes,
			execinfrapb.Ordering{Columns: orderingCols}, 0, /* limit */
			0 /* matchLen */, maxNumberPartitions, args.Spec.ProcessorID,
			opNamePrefix+"-", factory, reuseMode,
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
		inputs[i].MetadataSources = nil
		inputs[i].StatsCollectors = nil
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
	core *execinfrapb.ProcessorCoreUnion,
	post *execinfrapb.PostProcessSpec,
	processorID int32,
	factory coldata.ColumnFactory,
	causeToWrap error,
) error {
	if args.ProcessorConstructor == nil {
		return errors.Wrap(causeToWrap, "processorConstructor is nil")
	}
	log.VEventf(ctx, 1, "planning a row-execution processor in the vectorized flow: %v", causeToWrap)
	if err := canWrap(flowCtx.EvalCtx.SessionData().VectorizeMode, core); err != nil {
		log.VEventf(ctx, 1, "planning a wrapped processor failed: %v", err)
		// Return the original error for why we don't support this spec
		// natively since it is more interesting.
		return causeToWrap
	}
	c, err := wrapRowSources(
		ctx,
		flowCtx,
		args,
		inputs,
		inputTypes,
		processorID,
		func(inputs []execinfra.RowSource) (execinfra.RowSource, error) {
			// We provide a slice with a single nil as 'outputs' parameter
			// because all processors expect a single output. Passing nil is ok
			// here because when wrapping the processor, the materializer will
			// be its output, and it will be set up in wrapRowSources.
			proc, err := args.ProcessorConstructor(
				ctx, flowCtx, processorID, core, post, inputs, args.LocalProcessors,
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
					"processor %s is not an execinfra.RowSource", core.String(),
				)
			}
			r.ColumnTypes = rs.OutputTypes()
			if releasable, ok := rs.(execreleasable.Releasable); ok {
				r.Releasables = append(r.Releasables, releasable)
			}
			return rs, nil
		},
		factory,
		&r.Releasables,
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
	args.CloserRegistry.AddCloser(r.Root.(colexecop.Closer))
	r.Releasables = append(r.Releasables, c)
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
	// MetadataSources slice, so if we don't see any other objects, then the
	// responsibility over other meta components has been claimed by the
	// children of the columnarizer.
	if len(r.StatsCollectors) != 0 || len(r.MetadataSources) != 1 {
		return nil
	}
	c.MarkAsRemovedFromFlow()
	return c.Input()
}

func getStreamingMemAccount(
	args *colexecargs.NewColOperatorArgs, flowCtx *execinfra.FlowCtx,
) *mon.BoundAccount {
	if args.StreamingMemAccount == nil {
		args.StreamingMemAccount = args.MonitorRegistry.NewStreamingMemAccount(flowCtx)
	}
	return args.StreamingMemAccount
}

func getStreamingAllocator(
	ctx context.Context, args *colexecargs.NewColOperatorArgs, flowCtx *execinfra.FlowCtx,
) *colmem.Allocator {
	if args.StreamingAllocator == nil {
		args.StreamingAllocator = colmem.NewAllocator(ctx, getStreamingMemAccount(args, flowCtx), args.Factory)
	}
	return args.StreamingAllocator
}

func makeNewHashJoinerArgs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	opName redact.SafeString,
	core *execinfrapb.HashJoinerSpec,
	factory coldata.ColumnFactory,
) (colexecjoin.NewHashJoinerArgs, redact.SafeString) {
	hashJoinerMemAccount, hashJoinerMemMonitorName := args.MonitorRegistry.CreateMemAccountForSpillStrategy(
		ctx, flowCtx, opName, args.Spec.ProcessorID,
	)
	// Create two unlimited memory accounts (one for the output batch and
	// another for the "overdraft" accounting when spilling to disk occurs).
	accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
		ctx, flowCtx, opName, args.Spec.ProcessorID, 2, /* numAccounts */
	)
	spec := colexecjoin.MakeHashJoinerSpec(
		core.Type,
		core.LeftEqColumns,
		core.RightEqColumns,
		args.Spec.Input[0].ColumnTypes,
		args.Spec.Input[1].ColumnTypes,
		core.RightEqColumnsAreKey,
	)
	return colexecjoin.NewHashJoinerArgs{
		BuildSideAllocator:       colmem.NewLimitedAllocator(ctx, hashJoinerMemAccount, accounts[0], factory),
		OutputUnlimitedAllocator: colmem.NewAllocator(ctx, accounts[1], factory),
		Spec:                     spec,
		LeftSource:               args.Inputs[0].Root,
		RightSource:              args.Inputs[1].Root,
		InitialNumBuckets:        colexecjoin.HashJoinerInitialNumBuckets,
	}, hashJoinerMemMonitorName
}

func makeNewHashAggregatorArgs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	opName redact.SafeString,
	newAggArgs *colexecagg.NewAggregatorArgs,
	factory coldata.ColumnFactory,
) (*colexecagg.NewHashAggregatorArgs, *colexecutils.NewSpillingQueueArgs, redact.SafeString) {
	// We will divide the available memory equally between the two usages - the
	// hash aggregation itself and the input tuples tracking.
	totalMemLimit := execinfra.GetWorkMemLimit(flowCtx)
	// We will give 20% of the hash aggregation budget to the output batch.
	maxOutputBatchMemSize := totalMemLimit / 10
	hashAggregationMemLimit := totalMemLimit/2 - maxOutputBatchMemSize
	inputTuplesTrackingMemLimit := totalMemLimit / 2
	if totalMemLimit == 1 {
		// If total memory limit is 1, we're likely in a "force disk spill"
		// scenario, so we'll set all internal limits to 1 too (if we don't,
		// they will end up as 0 which is treated as "no limit").
		maxOutputBatchMemSize = 1
		hashAggregationMemLimit = 1
		inputTuplesTrackingMemLimit = 1
	}
	hashAggregatorMemAccount, hashAggregatorMemMonitorName := args.MonitorRegistry.CreateMemAccountForSpillStrategyWithLimit(
		ctx, flowCtx, hashAggregationMemLimit, opName, args.Spec.ProcessorID,
	)
	hashTableMemAccount := args.MonitorRegistry.CreateExtraMemAccountForSpillStrategy(
		hashAggregatorMemMonitorName,
	)
	// We need to create five unlimited memory accounts so that each component
	// could track precisely its own usage. The components are
	// - the hash aggregator
	// - the hash table
	// - output batch of the hash aggregator
	// - the spilling queue for the input tuples tracking (which requires two
	//   accounts).
	accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
		ctx, flowCtx, opName, args.Spec.ProcessorID, 5, /* numAccounts */
	)
	newAggArgs.Allocator = colmem.NewLimitedAllocator(ctx, hashAggregatorMemAccount, accounts[0], factory)
	return &colexecagg.NewHashAggregatorArgs{
			NewAggregatorArgs:        newAggArgs,
			HashTableAllocator:       colmem.NewLimitedAllocator(ctx, hashTableMemAccount, accounts[1], factory),
			OutputUnlimitedAllocator: colmem.NewAllocator(ctx, accounts[2], factory),
			MaxOutputBatchMemSize:    maxOutputBatchMemSize,
		},
		&colexecutils.NewSpillingQueueArgs{
			UnlimitedAllocator: colmem.NewAllocator(ctx, accounts[3], factory),
			MemoryLimit:        inputTuplesTrackingMemLimit,
			DiskQueueCfg:       args.DiskQueueCfg,
			FDSemaphore:        args.FDSemaphore,
			DiskAcc: args.MonitorRegistry.CreateDiskAccount(
				ctx, flowCtx, hashAggregatorMemMonitorName+"-spilling-queue", args.Spec.ProcessorID,
			),
			DiskQueueMemAcc: accounts[4],
		},
		hashAggregatorMemMonitorName
}

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
	if args.SemaCtx == nil {
		args.SemaCtx = flowCtx.NewSemaContext(flowCtx.Txn)
	}
	if args.MonitorRegistry == nil {
		args.MonitorRegistry = &colexecargs.MonitorRegistry{}
	}
	if args.CloserRegistry == nil {
		args.CloserRegistry = &colexecargs.CloserRegistry{}
	}

	core := &spec.Core
	post := &spec.Post

	if err = supportedNatively(core); err != nil {
		inputTypes := make([][]*types.T, len(spec.Input))
		for inputIdx, input := range spec.Input {
			inputTypes[inputIdx] = input.ColumnTypes
		}

		// We will pass all of the PostProcessSpec to the wrapped processor so
		// that it would handle projections as well as limits and offsets
		// itself.
		//
		// However, we'll keep the render exprs for the vectorized planning done
		// in planPostProcessSpec() below. In such a setup, the wrapped
		// processor will simply output all of its internal columns, so we don't
		// need to do anything special to remap the render exprs since they
		// still refer to columns using the correct ordinals.
		// Allocate both postprocesspecs at once.
		newPosts := [2]execinfrapb.PostProcessSpec{}
		wrappingPost := &newPosts[0]
		*wrappingPost = *post
		wrappingPost.RenderExprs = nil
		newPosts[1] = execinfrapb.PostProcessSpec{RenderExprs: post.RenderExprs}
		post = &newPosts[1]
		err = result.createAndWrapRowSource(
			ctx, flowCtx, args, inputs, inputTypes, core,
			wrappingPost, spec.ProcessorID, factory, err,
		)
	} else {
		switch {
		case core.Noop != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			result.Root = colexecop.NewNoop(inputs[0].Root)
			result.ColumnTypes = spec.Input[0].ColumnTypes

		case core.Values != nil:
			if err := checkNumIn(inputs, 0); err != nil {
				return r, err
			}
			if err := execinfra.HydrateTypesInDatumInfo(ctx, args.TypeResolver, core.Values.Columns); err != nil {
				return r, err
			}
			if core.Values.NumRows == 0 || len(core.Values.Columns) == 0 {
				// Handle coldata.Batch vector source.
				if b, ok := args.LocalVectorSources[args.Spec.ProcessorID]; ok {
					batch, ok := b.(coldata.Batch)
					if !ok {
						colexecerror.InternalError(errors.AssertionFailedf("LocalVectorSource wasn't a coldata.Batch"))
					}
					result.Root = colexecutils.NewRawColDataBatchOp(batch)
				} else {
					// To simplify valuesOp we handle some special cases with
					// fixedNumTuplesNoInputOp.
					result.Root = colexecutils.NewFixedNumTuplesNoInputOp(
						getStreamingAllocator(ctx, args, flowCtx), int(core.Values.NumRows), nil, /* opToInitialize */
					)
				}
			} else {
				result.Root = colexec.NewValuesOp(
					getStreamingAllocator(ctx, args, flowCtx), core.Values, execinfra.GetWorkMemLimit(flowCtx),
				)
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
			// unlimited one. We also need another unlimited account for the
			// KV fetcher.
			accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
				ctx, flowCtx, "cfetcher" /* opName */, spec.ProcessorID, 2, /* numAccounts */
			)
			estimatedRowCount := spec.EstimatedRowCount
			var scanOp colfetcher.ScanOperator
			var resultTypes []*types.T
			if flowCtx.EvalCtx.SessionData().DirectColumnarScansEnabled {
				canUseDirectScan := func() bool {
					// We currently don't use the direct scans if TraceKV is
					// enabled (due to not being able to tell the KV server
					// about it). One idea would be to include this boolean into
					// the fetchpb.IndexFetchSpec.
					// TODO(yuzefovich, 23.1): support TraceKV option.
					if flowCtx.TraceKV {
						return false
					}
					// The current implementation of non-default locking
					// strength as well as of SKIP LOCKED wait policy require
					// being able to access to the full keys after the
					// corresponding request is evaluated. This is not possible,
					// in general case, when using the direct scans since only
					// needed columns are included into the response.
					// TODO(yuzefovich): support non-default locking strength
					// and SKIP LOCKED wait policy somehow (#92950). One idea
					// would be to simply include all key columns into the set
					// of needed for the fetch and to project them away in the
					// ColBatchDirectScan.
					if row.GetKeyLockingStrength(core.TableReader.LockingStrength) != lock.None ||
						core.TableReader.LockingWaitPolicy == descpb.ScanLockingWaitPolicy_SKIP_LOCKED {
						return false
					}
					// At the moment, the ColBatchDirectScan cannot handle Gets
					// (it's not clear whether it is worth to handle them via
					// the same path as for Scans and ReverseScans (which could
					// have too large of an overhead) or by teaching the
					// operator to also decode a single KV (similar to what
					// regular ColBatchScan does)).
					// TODO(yuzefovich, 23.1): explore supporting Gets somehow.
					for i := range core.TableReader.Spans {
						if len(core.TableReader.Spans[i].EndKey) == 0 {
							return false
						}
					}
					fetchSpec := core.TableReader.FetchSpec
					// Handling user-defined types requires type hydration which
					// we cannot easily do on the KV server side, so for the
					// time being we disable the direct scans with such types.
					// However, we allow for enums to be processed by treating
					// them as bytes values.
					// TODO(yuzefovich): consider supporting non-enum UDTs
					// (#92954).
					for _, c := range fetchSpec.KeyAndSuffixColumns {
						if c.Type.UserDefined() && c.Type.Family() != types.EnumFamily {
							return false
						}
					}
					for _, c := range fetchSpec.FetchedColumns {
						if c.Type.UserDefined() && c.Type.Family() != types.EnumFamily {
							return false
						}
					}
					return true
				}
				if canUseDirectScan() {
					scanOp, resultTypes, err = colfetcher.NewColBatchDirectScan(
						ctx, colmem.NewAllocator(ctx, accounts[0], factory), accounts[1],
						flowCtx, spec.ProcessorID, core.TableReader, post, args.TypeResolver,
					)
					if err != nil {
						return r, err
					}
				}
			}
			if scanOp == nil {
				scanOp, resultTypes, err = colfetcher.NewColBatchScan(
					ctx, colmem.NewAllocator(ctx, accounts[0], factory), accounts[1],
					flowCtx, spec.ProcessorID, core.TableReader, post, estimatedRowCount, args.TypeResolver,
				)
				if err != nil {
					return r, err
				}
			}
			result.finishScanPlanning(scanOp, resultTypes, args.CloserRegistry)

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
			// unlimited one. We also need another unlimited account for the
			// KV fetcher. Additionally, we might use the Streamer API which
			// requires yet another separate memory account that is bound to an
			// unlimited memory monitor.
			accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
				ctx, flowCtx, "index-join" /* opName */, spec.ProcessorID, 3, /* numAccounts */
			)
			streamerDiskMonitor := args.MonitorRegistry.CreateDiskMonitor(
				ctx, flowCtx, "streamer" /* opName */, spec.ProcessorID,
			)
			indexJoinOp, err := colfetcher.NewColIndexJoin(
				ctx, getStreamingAllocator(ctx, args, flowCtx),
				colmem.NewAllocator(ctx, accounts[0], factory),
				accounts[1], accounts[2], flowCtx, spec.ProcessorID,
				inputs[0].Root, core.JoinReader, post, spec.Input[0].ColumnTypes,
				streamerDiskMonitor, args.TypeResolver,
			)
			if err != nil {
				return r, err
			}
			result.finishScanPlanning(indexJoinOp, indexJoinOp.ResultTypes, args.CloserRegistry)

		case core.Filterer != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}

			result.ColumnTypes = spec.Input[0].ColumnTypes
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
			if aggSpec.IsRowCount() {
				result.Root, err = colexec.NewCountOp(getStreamingAllocator(ctx, args, flowCtx), inputs[0].Root), nil
				result.ColumnTypes = []*types.T{types.Int}
				break
			}

			var needHash bool
			needHash, err = execagg.NeedHashAggregator(aggSpec)
			if err != nil {
				return r, err
			}
			// Make a copy of the evalCtx since we're modifying it below.
			evalCtx := flowCtx.NewEvalCtx()
			newAggArgs := &colexecagg.NewAggregatorArgs{
				Input:             inputs[0].Root,
				InputTypes:        spec.Input[0].ColumnTypes,
				Spec:              aggSpec,
				EvalCtx:           evalCtx,
				EstimatedRowCount: args.Spec.EstimatedRowCount,
			}
			newAggArgs.Constructors, newAggArgs.ConstArguments, newAggArgs.OutputTypes, err = colexecagg.ProcessAggregations(
				ctx, evalCtx, args.SemaCtx, aggSpec.Aggregations, spec.Input[0].ColumnTypes,
			)
			if err != nil {
				return r, err
			}
			result.ColumnTypes = newAggArgs.OutputTypes

			if needHash {
				opName := redact.SafeString("hash-aggregator")
				// We have separate unit tests that instantiate the in-memory
				// hash aggregators, so we don't need to look at
				// args.TestingKnobs.DiskSpillingDisabled and always instantiate
				// a disk-backed one here.
				diskSpillingDisabled := !colexec.HashAggregationDiskSpillingEnabled.Get(&flowCtx.Cfg.Settings.SV)
				if diskSpillingDisabled {
					// The disk spilling is disabled by the cluster setting, so
					// we give unlimited memory accounts to the in-memory hash
					// aggregator and all of its components and don't set up the
					// disk spiller.
					accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
						ctx, flowCtx, opName, spec.ProcessorID, 3, /* numAccounts */
					)
					hashAggregatorUnlimitedMemAccount := accounts[0]
					newAggArgs.Allocator = colmem.NewAllocator(
						ctx, hashAggregatorUnlimitedMemAccount, factory,
					)
					evalCtx.SingleDatumAggMemAccount = hashAggregatorUnlimitedMemAccount
					newHashAggArgs := &colexecagg.NewHashAggregatorArgs{
						NewAggregatorArgs:        newAggArgs,
						HashTableAllocator:       colmem.NewAllocator(ctx, accounts[1], factory),
						OutputUnlimitedAllocator: colmem.NewAllocator(ctx, accounts[2], factory),
						MaxOutputBatchMemSize:    execinfra.GetWorkMemLimit(flowCtx),
					}
					// The third argument is nil because we disable the tracking
					// of the input tuples.
					result.Root = colexec.NewHashAggregator(
						ctx, newHashAggArgs, nil, /* newSpillingQueueArgs */
					)
					args.CloserRegistry.AddCloser(result.Root.(colexecop.Closer))
				} else {
					newHashAggArgs, sqArgs, hashAggregatorMemMonitorName := makeNewHashAggregatorArgs(
						ctx, flowCtx, args, opName, newAggArgs, factory,
					)
					sqArgs.Types = spec.Input[0].ColumnTypes
					inMemoryHashAggregator := colexec.NewHashAggregator(
						ctx, newHashAggArgs, sqArgs,
					)
					ehaOpName := redact.SafeString("external-hash-aggregator")
					ehaAccounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
						ctx, flowCtx, ehaOpName, spec.ProcessorID, 4, /* numAccounts */
					)
					ehaMemAccount := ehaAccounts[0]
					// Note that we will use an unlimited memory account here
					// even for the in-memory hash aggregator since it is easier
					// to do so than to try to replace the memory account if the
					// spilling to disk occurs (if we don't replace it in such
					// case, the wrapped aggregate functions might hit a memory
					// error even when used by the external hash aggregator).
					evalCtx.SingleDatumAggMemAccount = ehaMemAccount
					diskSpiller := colexecdisk.NewOneInputDiskSpiller(
						inputs[0].Root, inMemoryHashAggregator.(colexecop.BufferingInMemoryOperator),
						hashAggregatorMemMonitorName,
						func(input colexecop.Operator) colexecop.Operator {
							newAggArgs := *newAggArgs
							// Note that the hash-based partitioner will make
							// sure that partitions to process using the
							// in-memory hash aggregator fit under the limit, so
							// we use an unlimited allocator.
							newAggArgs.Allocator = colmem.NewAllocator(ctx, ehaMemAccount, factory)
							newAggArgs.Input = input
							eha, toClose := colexecdisk.NewExternalHashAggregator(
								ctx,
								flowCtx,
								args,
								&colexecagg.NewHashAggregatorArgs{
									NewAggregatorArgs:        &newAggArgs,
									HashTableAllocator:       colmem.NewAllocator(ctx, ehaAccounts[1], factory),
									OutputUnlimitedAllocator: colmem.NewAllocator(ctx, ehaAccounts[2], factory),
									MaxOutputBatchMemSize:    newHashAggArgs.MaxOutputBatchMemSize,
								},
								makeDiskBackedSorterConstructor(ctx, flowCtx, args, ehaOpName, factory),
								args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, ehaOpName, spec.ProcessorID),
								ehaAccounts[3],
								spec.Core.Aggregator.OutputOrdering,
							)
							args.CloserRegistry.AddCloser(toClose)
							return eha
						},
						colexecop.BufferingOpNoReuse,
						args.TestingKnobs.SpillingCallbackFn,
					)
					result.Root = diskSpiller
					args.CloserRegistry.AddCloser(diskSpiller)
				}
			} else {
				evalCtx.SingleDatumAggMemAccount = getStreamingMemAccount(args, flowCtx)
				newAggArgs.Allocator = getStreamingAllocator(ctx, args, flowCtx)
				result.Root = colexec.NewOrderedAggregator(ctx, newAggArgs)
				args.CloserRegistry.AddCloser(result.Root.(colexecop.Closer))
			}

		case core.Distinct != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			result.ColumnTypes = spec.Input[0].ColumnTypes
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
				unlimitedAcc := args.MonitorRegistry.CreateUnlimitedMemAccount(
					ctx, flowCtx, "distinct" /* opName */, spec.ProcessorID,
				)
				allocator := colmem.NewLimitedAllocator(ctx, distinctMemAccount, unlimitedAcc, factory)
				inMemoryUnorderedDistinct := colexec.NewUnorderedDistinct(
					allocator, inputs[0].Root, core.Distinct.DistinctColumns, result.ColumnTypes,
					core.Distinct.NullsAreDistinct, core.Distinct.ErrorOnDup,
				)
				// Capture the current input type schema since the spilling to
				// disk might occur during the execution time, at which point
				// result.ColumnTypes might point to something different.
				inputTypes := result.ColumnTypes
				diskSpiller := colexecdisk.NewOneInputDiskSpiller(
					inputs[0].Root, inMemoryUnorderedDistinct.(colexecop.BufferingInMemoryOperator),
					distinctMemMonitorName,
					func(input colexecop.Operator) colexecop.Operator {
						edOpName := redact.SafeString("external-distinct")
						accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
							ctx, flowCtx, edOpName, spec.ProcessorID, 2, /* numAccounts */
						)
						unlimitedAllocator := colmem.NewAllocator(ctx, accounts[0], factory)
						ed, toClose := colexecdisk.NewExternalDistinct(
							unlimitedAllocator,
							flowCtx,
							args,
							input,
							inputTypes,
							makeDiskBackedSorterConstructor(ctx, flowCtx, args, edOpName, factory),
							inMemoryUnorderedDistinct,
							args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, edOpName, spec.ProcessorID),
							accounts[1],
						)
						args.CloserRegistry.AddCloser(toClose)
						return ed
					},
					colexecop.BufferingOpNoReuse,
					args.TestingKnobs.SpillingCallbackFn,
				)
				result.Root = diskSpiller
				args.CloserRegistry.AddCloser(diskSpiller)
			}

		case core.Ordinality != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Root = colexecbase.NewOrdinalityOp(
				getStreamingAllocator(ctx, args, flowCtx), inputs[0].Root, outputIdx,
			)
			result.ColumnTypes = spec.Input[0].ColumnTypes
			result.ColumnTypes = append(result.ColumnTypes, types.Int)

		case core.HashJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}

			if len(core.HashJoiner.LeftEqColumns) == 0 {
				// We are performing a cross-join, so we need to plan a
				// specialized operator.
				opName := redact.SafeString("cross-joiner")
				accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(ctx, flowCtx, opName, spec.ProcessorID, 2 /* numAccounts */)
				crossJoinerDiskAcc := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, spec.ProcessorID)
				unlimitedAllocator := colmem.NewAllocator(ctx, accounts[0], factory)
				result.Root = colexecjoin.NewCrossJoiner(
					unlimitedAllocator,
					execinfra.GetWorkMemLimit(flowCtx),
					args.DiskQueueCfg,
					args.FDSemaphore,
					core.HashJoiner.Type,
					inputs[0].Root, inputs[1].Root,
					spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes,
					crossJoinerDiskAcc,
					accounts[1],
				)
				args.CloserRegistry.AddCloser(result.Root.(colexecop.Closer))
			} else {
				opName := redact.SafeString("hash-joiner")
				hjArgs, hashJoinerMemMonitorName := makeNewHashJoinerArgs(
					ctx,
					flowCtx,
					args,
					opName,
					core.HashJoiner,
					factory,
				)
				inMemoryHashJoiner := colexecjoin.NewHashJoiner(hjArgs)
				if args.TestingKnobs.DiskSpillingDisabled {
					// We will not be creating a disk-backed hash joiner because
					// we're running a test that explicitly asked for only
					// in-memory hash joiner.
					result.Root = inMemoryHashJoiner
				} else {
					diskSpiller := colexecdisk.NewTwoInputDiskSpiller(
						inputs[0].Root, inputs[1].Root, inMemoryHashJoiner.(colexecop.BufferingInMemoryOperator),
						[]redact.SafeString{hashJoinerMemMonitorName},
						func(inputOne, inputTwo colexecop.Operator) colexecop.Operator {
							opName := redact.SafeString("external-hash-joiner")
							accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
								ctx, flowCtx, opName, spec.ProcessorID, 2, /* numAccounts */
							)
							unlimitedAllocator := colmem.NewAllocator(ctx, accounts[0], factory)
							ehj := colexecdisk.NewExternalHashJoiner(
								ctx,
								unlimitedAllocator,
								flowCtx,
								args,
								hjArgs.Spec,
								inputOne, inputTwo,
								makeDiskBackedSorterConstructor(ctx, flowCtx, args, opName, factory),
								args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, spec.ProcessorID),
								accounts[1],
							)
							args.CloserRegistry.AddCloser(ehj)
							return ehj
						},
						args.TestingKnobs.SpillingCallbackFn,
					)
					result.Root = diskSpiller
					args.CloserRegistry.AddCloser(diskSpiller)
				}
			}

			result.ColumnTypes = core.HashJoiner.Type.MakeOutputTypes(spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes)

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

			opName := redact.SafeString("merge-joiner")
			// We are using an unlimited memory monitor here because merge
			// joiner itself is responsible for making sure that we stay within
			// the memory limit, and it will fall back to disk if necessary.
			accounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(ctx, flowCtx, opName, spec.ProcessorID, 2 /* numAccounts */)
			unlimitedAllocator := colmem.NewAllocator(ctx, accounts[0], factory)
			diskAccount := args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, spec.ProcessorID)
			mj := colexecjoin.NewMergeJoinOp(
				ctx, unlimitedAllocator, execinfra.GetWorkMemLimit(flowCtx),
				args.DiskQueueCfg, args.FDSemaphore,
				joinType, inputs[0].Root, inputs[1].Root,
				spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes,
				core.MergeJoiner.LeftOrdering.Columns, core.MergeJoiner.RightOrdering.Columns,
				diskAccount, accounts[1], flowCtx.EvalCtx,
			)

			result.Root = mj
			args.CloserRegistry.AddCloser(mj.(colexecop.Closer))
			result.ColumnTypes = core.MergeJoiner.Type.MakeOutputTypes(spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes)

			if onExpr != nil {
				if err = result.planAndMaybeWrapFilter(
					ctx, flowCtx, args, spec.ProcessorID, *onExpr, factory,
				); err != nil {
					return r, err
				}
			}

		case core.HashGroupJoiner != nil:
			if err := checkNumIn(inputs, 2); err != nil {
				return r, err
			}

			hgjSpec := core.HashGroupJoiner
			hjSpec, aggSpec := &hgjSpec.HashJoinerSpec, &hgjSpec.AggregatorSpec
			opName := redact.SafeString("hash-group-joiner")
			hjArgs, hashJoinerMemMonitorName := makeNewHashJoinerArgs(
				ctx, flowCtx, args, opName, hjSpec, factory,
			)
			hjOutputTypes := hjSpec.Type.MakeOutputTypes(spec.Input[0].ColumnTypes, spec.Input[1].ColumnTypes)
			joinOutputTypes := hjOutputTypes
			if len(hgjSpec.JoinOutputColumns) > 0 {
				joinOutputTypes = make([]*types.T, len(hgjSpec.JoinOutputColumns))
				for i, hjIdx := range hgjSpec.JoinOutputColumns {
					joinOutputTypes[i] = hjOutputTypes[hjIdx]
				}
			}

			// TODO(yuzefovich): implement the optimized version if the
			// aggregation is a simple row count.

			// Make a copy of the evalCtx since we're modifying it below.
			evalCtx := flowCtx.NewEvalCtx()
			newAggArgs := &colexecagg.NewAggregatorArgs{
				InputTypes:        joinOutputTypes,
				Spec:              aggSpec,
				EvalCtx:           evalCtx,
				EstimatedRowCount: args.Spec.EstimatedRowCount,
			}
			newAggArgs.Constructors, newAggArgs.ConstArguments, newAggArgs.OutputTypes, err = colexecagg.ProcessAggregations(
				ctx, evalCtx, args.SemaCtx, aggSpec.Aggregations, joinOutputTypes,
			)
			if err != nil {
				return r, err
			}
			result.ColumnTypes = newAggArgs.OutputTypes

			// Note that we already gave full workmem limit to the hash joiner
			// above and now are using another workmem limit here. This seems
			// reasonable given that the hash group-join operation is a
			// "composite" one. The second workmem limit usage here will be
			// split for two use cases: one half will go to the hash aggregator,
			// and another half will be used for tracking the input tuples from
			// the left input (which is needed to spill to disk when the memory
			// limit is reached during the aggregation).
			newHashAggArgs, sqArgs, hashAggregatorMemMonitorName := makeNewHashAggregatorArgs(
				ctx, flowCtx, args, opName, newAggArgs, factory,
			)
			// Spilling queue is needed for the left input to the hash
			// group-join.
			sqArgs.Types = args.Spec.Input[0].ColumnTypes

			hgj := colexec.NewHashGroupJoiner(
				ctx,
				args.Inputs[0].Root,
				args.Inputs[1].Root,
				func(leftSource colexecop.Operator) colexecop.BufferingInMemoryOperator {
					hjArgs.LeftSource = leftSource
					return colexecjoin.NewHashJoiner(hjArgs).(colexecop.BufferingInMemoryOperator)
				},
				len(hjOutputTypes),
				hgjSpec.JoinOutputColumns,
				newHashAggArgs,
				sqArgs,
			)
			args.CloserRegistry.AddCloser(hgj.(colexecop.Closer))

			ehgjOpName := redact.SafeString("external-hash-group-joiner")
			ehgjAccounts := args.MonitorRegistry.CreateUnlimitedMemAccounts(
				ctx, flowCtx, ehgjOpName, spec.ProcessorID, 6, /* numAccounts */
			)
			ehjMemAccount := ehgjAccounts[0]
			ehaMemAccount := ehgjAccounts[1]
			// Note that we will use an unlimited memory account here even for
			// the in-memory hash aggregator since it is easier to do so than to
			// try to replace the memory account if the spilling to disk occurs
			// (if we don't replace it in such case, the wrapped aggregate
			// functions might hit a memory error even when used by the external
			// hash aggregator).
			evalCtx.SingleDatumAggMemAccount = ehaMemAccount
			diskSpiller := colexecdisk.NewTwoInputDiskSpiller(
				inputs[0].Root, inputs[1].Root, hgj,
				[]redact.SafeString{hashJoinerMemMonitorName, hashAggregatorMemMonitorName},
				func(inputOne, inputTwo colexecop.Operator) colexecop.Operator {
					// When we spill to disk, we just use a combo of an external
					// hash join followed by an external hash aggregation.
					ehj := colexecdisk.NewExternalHashJoiner(
						ctx,
						colmem.NewAllocator(ctx, ehjMemAccount, factory),
						flowCtx,
						args,
						hjArgs.Spec,
						inputOne, inputTwo,
						makeDiskBackedSorterConstructor(ctx, flowCtx, args, ehgjOpName+"-join", factory),
						args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, ehgjOpName+"-join", spec.ProcessorID),
						ehgjAccounts[2],
					)
					args.CloserRegistry.AddCloser(ehj)

					aggInput := ehj.(colexecop.Operator)
					if len(hgjSpec.JoinOutputColumns) > 0 {
						aggInput, _ = addProjection(aggInput, hjOutputTypes, hgjSpec.JoinOutputColumns)
					}

					newAggArgs := *newAggArgs
					// Note that the hash-based partitioner will make sure that
					// partitions to process using the in-memory hash aggregator
					// fit under the limit, so we use an unlimited allocator.
					newAggArgs.Allocator = colmem.NewAllocator(ctx, ehaMemAccount, factory)
					newAggArgs.Input = aggInput
					eha, toClose := colexecdisk.NewExternalHashAggregator(
						ctx,
						flowCtx,
						args,
						&colexecagg.NewHashAggregatorArgs{
							NewAggregatorArgs:        &newAggArgs,
							HashTableAllocator:       colmem.NewAllocator(ctx, ehgjAccounts[3], factory),
							OutputUnlimitedAllocator: colmem.NewAllocator(ctx, ehgjAccounts[4], factory),
							MaxOutputBatchMemSize:    newHashAggArgs.MaxOutputBatchMemSize,
						},
						makeDiskBackedSorterConstructor(ctx, flowCtx, args, ehgjOpName+"-agg", factory),
						args.MonitorRegistry.CreateDiskAccount(ctx, flowCtx, ehgjOpName+"-agg", spec.ProcessorID),
						ehgjAccounts[5],
						// TODO(yuzefovich): think through whether the hash
						// group-join needs to maintain the ordering.
						execinfrapb.Ordering{}, /* outputOrdering */
					)
					args.CloserRegistry.AddCloser(toClose)
					return eha
				},
				args.TestingKnobs.SpillingCallbackFn,
			)
			result.Root = diskSpiller
			args.CloserRegistry.AddCloser(diskSpiller)

		case core.Sorter != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			input := inputs[0].Root
			result.ColumnTypes = spec.Input[0].ColumnTypes
			ordering := core.Sorter.OutputOrdering
			matchLen := core.Sorter.OrderingMatchLen
			limit := core.Sorter.Limit
			result.Root = createDiskBackedSort(
				ctx, flowCtx, args, input, result.ColumnTypes, ordering, limit, matchLen, 0, /* maxNumberPartitions */
				spec.ProcessorID, "" /* opNamePrefix */, factory, colexecop.BufferingOpNoReuse,
			)

		case core.Windower != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			opNamePrefix := redact.SafeString("window-")
			input := inputs[0].Root
			result.ColumnTypes = spec.Input[0].ColumnTypes
			for _, wf := range core.Windower.WindowFns {
				numInputCols := len(result.ColumnTypes)

				// Set any nil values in the window frame to their default values.
				wf.Frame = colexecwindow.NormalizeWindowFrame(wf.Frame)

				// Copy the argument indices into a mutable slice.
				argIdxs := make([]int, len(wf.ArgsIdxs))
				argTypes := make([]*types.T, len(wf.ArgsIdxs))
				for i, idx := range wf.ArgsIdxs {
					argIdxs[i] = int(idx)
					argTypes[i] = result.ColumnTypes[idx]
				}

				// Perform any necessary casts for the argument columns.
				castTo := colexecwindow.WindowFnArgCasts(wf.Func, argTypes)
				for i, typ := range castTo {
					if typ == nil {
						continue
					}
					castIdx := len(result.ColumnTypes)
					input, err = colexecbase.GetCastOperator(
						ctx, getStreamingAllocator(ctx, args, flowCtx), input, argIdxs[i],
						castIdx, argTypes[i], typ, flowCtx.EvalCtx,
					)
					if err != nil {
						colexecerror.InternalError(err)
					}
					result.ColumnTypes = append(result.ColumnTypes, typ)
					argIdxs[i] = castIdx
					argTypes[i] = typ
				}

				partitionColIdx := tree.NoColumnIdx
				peersColIdx := tree.NoColumnIdx

				if len(core.Windower.PartitionBy) > 0 {
					// TODO(yuzefovich): add support for hashing partitioner
					// (probably by leveraging hash routers once we can
					// distribute). The decision about which kind of partitioner
					// to use should come from the optimizer.
					partitionColIdx = len(result.ColumnTypes)
					input = colexecwindow.NewWindowSortingPartitioner(
						getStreamingAllocator(ctx, args, flowCtx), input, result.ColumnTypes,
						core.Windower.PartitionBy, wf.Ordering.Columns, partitionColIdx,
						func(input colexecop.Operator, inputTypes []*types.T, orderingCols []execinfrapb.Ordering_Column) colexecop.Operator {
							return createDiskBackedSort(
								ctx, flowCtx, args, input, inputTypes,
								execinfrapb.Ordering{Columns: orderingCols}, 0 /*limit */, 0, /* matchLen */
								0 /* maxNumberPartitions */, spec.ProcessorID,
								opNamePrefix, factory, colexecop.BufferingOpNoReuse,
							)
						},
					)
					// Window partitioner will append a boolean column.
					result.ColumnTypes = append(result.ColumnTypes, types.Bool)
				} else {
					if len(wf.Ordering.Columns) > 0 {
						input = createDiskBackedSort(
							ctx, flowCtx, args, input, result.ColumnTypes,
							wf.Ordering, 0 /* limit */, 0 /* matchLen */, 0, /* maxNumberPartitions */
							spec.ProcessorID, opNamePrefix, factory, colexecop.BufferingOpNoReuse,
						)
					}
				}
				if colexecwindow.WindowFnNeedsPeersInfo(&wf) {
					peersColIdx = len(result.ColumnTypes)
					input = colexecwindow.NewWindowPeerGrouper(
						getStreamingAllocator(ctx, args, flowCtx), input, result.ColumnTypes,
						wf.Ordering.Columns, partitionColIdx, peersColIdx,
					)
					// Window peer grouper will append a boolean column.
					result.ColumnTypes = append(result.ColumnTypes, types.Bool)
				}

				// The output column is appended after any temporary columns.
				outputColIdx := len(result.ColumnTypes)

				windowArgs := &colexecwindow.WindowArgs{
					EvalCtx:         flowCtx.EvalCtx,
					MemoryLimit:     execinfra.GetWorkMemLimit(flowCtx),
					QueueCfg:        args.DiskQueueCfg,
					FdSemaphore:     args.FDSemaphore,
					Input:           input,
					InputTypes:      result.ColumnTypes,
					OutputColIdx:    outputColIdx,
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
						windowArgs.MainAllocator = getStreamingAllocator(ctx, args, flowCtx)
						result.Root = colexecwindow.NewRowNumberOperator(windowArgs)
					case execinfrapb.WindowerSpec_RANK, execinfrapb.WindowerSpec_DENSE_RANK:
						windowArgs.MainAllocator = getStreamingAllocator(ctx, args, flowCtx)
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
						returnType = result.ColumnTypes[argIdxs[0]]
					case execinfrapb.WindowerSpec_LEAD:
						opName := opNamePrefix + "lead"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewLeadOperator(
							windowArgs, argIdxs[0], argIdxs[1], argIdxs[2])
						returnType = result.ColumnTypes[argIdxs[0]]
					case execinfrapb.WindowerSpec_FIRST_VALUE:
						opName := opNamePrefix + "first_value"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewFirstValueOperator(
							windowArgs, wf.Frame, &wf.Ordering, argIdxs)
						returnType = result.ColumnTypes[argIdxs[0]]
					case execinfrapb.WindowerSpec_LAST_VALUE:
						opName := opNamePrefix + "last_value"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewLastValueOperator(
							windowArgs, wf.Frame, &wf.Ordering, argIdxs)
						returnType = result.ColumnTypes[argIdxs[0]]
					case execinfrapb.WindowerSpec_NTH_VALUE:
						opName := opNamePrefix + "nth_value"
						result.finishBufferedWindowerArgs(
							ctx, flowCtx, args.MonitorRegistry, windowArgs, opName,
							spec.ProcessorID, factory, true, /* needsBuffer */
						)
						result.Root, err = colexecwindow.NewNthValueOperator(
							windowArgs, wf.Frame, &wf.Ordering, argIdxs)
						returnType = result.ColumnTypes[argIdxs[0]]
					default:
						return r, errors.AssertionFailedf("window function %s is not supported", wf.String())
					}
				} else if wf.Func.AggregateFunc != nil {
					// This is an aggregate window function.
					opName := opNamePrefix + redact.SafeString(strings.ToLower(wf.Func.AggregateFunc.String()))
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
							colexecagg.ProcessAggregations(ctx, flowCtx.EvalCtx, args.SemaCtx, aggregations, argTypes)
						var toClose colexecop.Closers
						var aggFnsAlloc *colexecagg.AggregateFuncsAlloc
						if (aggType != execinfrapb.Min && aggType != execinfrapb.Max) ||
							wf.Frame.Exclusion != execinfrapb.WindowerSpec_Frame_NO_EXCLUSION ||
							!colexecwindow.WindowFrameCanShrink(wf.Frame, &wf.Ordering) {
							// Min and max window functions have specialized implementations
							// when the frame can shrink and has a default exclusion clause.
							aggFnsAlloc, _, toClose, err = colexecagg.NewAggregateFuncsAlloc(
								ctx, &aggArgs, aggregations, 1, /* initialAllocSize */
								1 /* maxAllocSize */, colexecagg.WindowAggKind,
							)
							if err != nil {
								colexecerror.InternalError(err)
							}
						}
						result.Root = colexecwindow.NewWindowAggregatorOperator(
							windowArgs, aggType, wf.Frame, &wf.Ordering, argIdxs,
							aggArgs.OutputTypes[0], aggFnsAlloc,
						)
						args.CloserRegistry.AddClosers(toClose)
						returnType = aggArgs.OutputTypes[0]
					}
				} else {
					colexecerror.InternalError(errors.AssertionFailedf("window function spec is nil"))
				}
				if c, ok := result.Root.(colexecop.Closer); ok {
					args.CloserRegistry.AddCloser(c)
				}

				result.ColumnTypes = append(result.ColumnTypes, returnType)
				if outputColIdx > numInputCols {
					// We want to project out temporary columns (which have been
					// added in between the input columns and output column) as
					// well as include the new output column (which is located
					// after any temporary columns).
					numOutputCols := numInputCols + 1
					projection := make([]uint32, numOutputCols)
					for i := 0; i < numInputCols; i++ {
						projection[i] = uint32(i)
					}
					projection[numInputCols] = uint32(outputColIdx)
					result.Root, result.ColumnTypes = addProjection(result.Root, result.ColumnTypes, projection)
				}

				input = result.Root
			}

		case core.Insert != nil:
			if err := checkNumIn(inputs, 1); err != nil {
				return r, err
			}
			outputIdx := len(spec.Input[0].ColumnTypes)
			result.Root = colexec.NewInsertOp(
				ctx, flowCtx, core.Insert, inputs[0].Root, spec.ResultTypes,
				outputIdx, getStreamingAllocator(ctx, args, flowCtx), args.SemaCtx,
			)
			result.ColumnTypes = spec.ResultTypes

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
	err = ppr.planPostProcessSpec(ctx, flowCtx, args, post, factory, &r.Releasables, args.Spec.EstimatedRowCount)
	if err != nil {
		err = result.wrapPostProcessSpec(ctx, flowCtx, args, post, spec.ProcessorID, factory, err)
	} else {
		// The result can be updated with the post process result.
		r.Root = ppr.Op
		r.ColumnTypes = ppr.ColumnTypes
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
		if err = result.wrapPostProcessSpec(ctx, flowCtx, args, post, spec.ProcessorID, factory, errWrappedCast); err != nil {
			return r, err
		}
	} else if numMismatchedTypes > 0 {
		// We will need to project out the original mismatched columns, so
		// we're keeping track of the required projection.
		projection := make([]uint32, len(args.Spec.ResultTypes))
		for i := range args.Spec.ResultTypes {
			expected, actual := args.Spec.ResultTypes[i], r.ColumnTypes[i]
			if !actual.Identical(expected) {
				castedIdx := len(r.ColumnTypes)
				r.Root, err = colexecbase.GetCastOperator(
					ctx, getStreamingAllocator(ctx, args, flowCtx), r.Root, i, castedIdx,
					actual, expected, flowCtx.EvalCtx,
				)
				if err != nil {
					return r, errors.NewAssertionErrorWithWrappedErrf(err, "unexpectedly couldn't plan a cast although IsCastSupported returned true")
				}
				projection[i] = uint32(castedIdx)
				r.ColumnTypes = append(r.ColumnTypes, expected)
			} else {
				projection[i] = uint32(i)
			}
		}
		r.Root, r.ColumnTypes = addProjection(r.Root, r.ColumnTypes, projection)
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

// processExpr processes the given expression and returns a well-typed
// expression.
//
// evalCtx will not be mutated.
func processExpr(
	ctx context.Context,
	expr execinfrapb.Expression,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	typs []*types.T,
) (tree.TypedExpr, error) {
	if expr.LocalExpr != nil {
		return expr.LocalExpr, nil
	}
	return execinfrapb.DeserializeExpr(ctx, expr, typs, semaCtx, evalCtx)
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
	err := r.planFilterExpr(
		ctx, flowCtx, args.SemaCtx, filter, getStreamingAllocator(ctx, args, flowCtx),
	)
	if err != nil {
		// Filter expression planning failed. Fall back to planning the filter
		// using row execution.
		filtererCore := &execinfrapb.ProcessorCoreUnion{
			Filterer: &execinfrapb.FiltererSpec{
				Filter: filter,
			},
		}
		inputToMaterializer := colexecargs.OpWithMetaInfo{Root: r.Root}
		takeOverMetaInfo(&inputToMaterializer, args.Inputs)
		return r.createAndWrapRowSource(
			ctx, flowCtx, args, []colexecargs.OpWithMetaInfo{inputToMaterializer},
			[][]*types.T{r.ColumnTypes}, filtererCore, &execinfrapb.PostProcessSpec{},
			processorID, factory, err,
		)
	}
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
	processorID int32,
	factory coldata.ColumnFactory,
	causeToWrap error,
) error {
	noopCore := &execinfrapb.ProcessorCoreUnion{
		Noop: &execinfrapb.NoopCoreSpec{},
	}
	inputToMaterializer := colexecargs.OpWithMetaInfo{Root: r.Root}
	takeOverMetaInfo(&inputToMaterializer, args.Inputs)
	// createAndWrapRowSource updates r.ColumnTypes accordingly.
	return r.createAndWrapRowSource(
		ctx, flowCtx, args, []colexecargs.OpWithMetaInfo{inputToMaterializer},
		[][]*types.T{r.ColumnTypes}, noopCore, post, processorID, factory, causeToWrap,
	)
}

// renderExprCountVisitor counts how many projection operators need to be
// planned across render expressions.
type renderExprCountVisitor struct {
	renderCount int64
}

var _ tree.Visitor = &renderExprCountVisitor{}

func (r *renderExprCountVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if _, ok := expr.(*tree.IndexedVar); ok {
		// IndexedVars don't get a projection operator (we just refer to the
		// vector by index), so they don't contribute to the render count.
		return false, expr
	}
	r.renderCount++
	return true, expr
}

func (r *renderExprCountVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

var renderWrappingRowCountThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.distsql.vectorize_render_wrapping.max_row_count",
	"determines the maximum number of estimated rows that flow through the render "+
		"expressions up to which we handle those renders by wrapping a row-by-row processor",
	128,
	settings.NonNegativeInt,
)

var renderWrappingRenderCountThreshold = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.distsql.vectorize_render_wrapping.min_render_count",
	"determines the minimum number of render expressions for which we fall "+
		"back to handling renders by wrapping a row-by-row processor",
	16,
	settings.NonNegativeInt,
)

// planPostProcessSpec plans the post processing stage specified in post on top
// of r.Op.
func (r *postProcessResult) planPostProcessSpec(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	args *colexecargs.NewColOperatorArgs,
	post *execinfrapb.PostProcessSpec,
	factory coldata.ColumnFactory,
	releasables *[]execreleasable.Releasable,
	estimatedRowCount uint64,
) error {
	if post.Projection {
		r.Op, r.ColumnTypes = addProjection(r.Op, r.ColumnTypes, post.OutputColumns)
	} else if post.RenderExprs != nil {
		// Deserialize expressions upfront.
		exprs := make([]tree.TypedExpr, len(post.RenderExprs))
		var err error
		for i := range exprs {
			exprs[i], err = processExpr(ctx, post.RenderExprs[i], flowCtx.EvalCtx, args.SemaCtx, r.ColumnTypes)
			if err != nil {
				return err
			}
		}
		// If we have an estimated row count and it doesn't exceed the wrapping
		// row count threshold, we might need to fall back to wrapping a
		// row-by-row processor to handle the render expressions (for better
		// performance).
		if estimatedRowCount != 0 &&
			estimatedRowCount <= uint64(renderWrappingRowCountThreshold.Get(&flowCtx.Cfg.Settings.SV)) {
			renderCountThreshold := renderWrappingRenderCountThreshold.Get(&flowCtx.Cfg.Settings.SV)
			// Walk over all expressions and estimate how many projection
			// operators will need to be created.
			var v renderExprCountVisitor
			for _, expr := range exprs {
				tree.WalkExpr(&v, expr)
				if v.renderCount >= renderCountThreshold {
					return errFallbackToRenderWrapping
				}
			}
		}
		renderedCols := make([]uint32, 0, len(exprs))
		for _, expr := range exprs {
			var outputIdx int
			r.Op, outputIdx, r.ColumnTypes, err = planProjectionOperators(
				ctx, flowCtx.EvalCtx, expr, r.ColumnTypes, r.Op, getStreamingAllocator(ctx, args, flowCtx), releasables,
			)
			if err != nil {
				if log.ExpensiveLogEnabled(ctx, 1) {
					err = errors.Wrapf(err, "unable to columnarize render expression %q", expr)
				}
				return err
			}
			if outputIdx < 0 {
				return errors.AssertionFailedf("missing outputIdx")
			}
			renderedCols = append(renderedCols, uint32(outputIdx))
		}
		r.Op, r.ColumnTypes = addProjection(r.Op, r.ColumnTypes, renderedCols)
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

func (r opResult) finishBufferedWindowerArgs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	monitorRegistry *colexecargs.MonitorRegistry,
	args *colexecwindow.WindowArgs,
	opName redact.SafeString,
	processorID int32,
	factory coldata.ColumnFactory,
	needsBuffer bool,
) {
	args.DiskAcc = monitorRegistry.CreateDiskAccount(ctx, flowCtx, opName, processorID)
	var mainAcc *mon.BoundAccount
	if needsBuffer {
		accounts := monitorRegistry.CreateUnlimitedMemAccounts(ctx, flowCtx, opName, processorID, 3 /* numAccounts */)
		mainAcc = accounts[0]
		args.BufferAllocator = colmem.NewAllocator(ctx, accounts[1], factory)
		args.DiskQueueMemAcc = accounts[2]
	} else {
		accounts := monitorRegistry.CreateUnlimitedMemAccounts(ctx, flowCtx, opName, processorID, 2 /* numAccounts */)
		mainAcc = accounts[0]
		args.DiskQueueMemAcc = accounts[1]
	}
	args.MainAllocator = colmem.NewAllocator(ctx, mainAcc, factory)
}

func (r opResult) finishScanPlanning(
	op colfetcher.ScanOperator, resultTypes []*types.T, closerRegistry *colexecargs.CloserRegistry,
) {
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
	closerRegistry.AddCloser(op)
}

// planFilterExpr creates all operators to implement filter expression.
func (r opResult) planFilterExpr(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	semaCtx *tree.SemaContext,
	filter execinfrapb.Expression,
	allocator *colmem.Allocator,
) error {
	expr, err := processExpr(ctx, filter, flowCtx.EvalCtx, semaCtx, r.ColumnTypes)
	if err != nil {
		return err
	}
	if expr == tree.DNull {
		// The filter expression is tree.DNull meaning that it is always false, so
		// we put a zero operator.
		r.Root = colexecutils.NewZeroOp(r.Root)
		return nil
	}
	op, _, filterColumnTypes, err := planSelectionOperators(
		ctx, flowCtx.EvalCtx, expr, r.ColumnTypes, r.Root, allocator, &r.Releasables,
	)
	if err != nil {
		if log.ExpensiveLogEnabled(ctx, 1) {
			err = errors.Wrapf(err, "unable to columnarize filter expression %q", filter)
		}
		return err
	}
	r.Root = op
	if len(filterColumnTypes) > len(r.ColumnTypes) {
		// Additional columns were appended to store projections while
		// evaluating the filter. Project them away.
		var outputColumns []uint32
		for i := range r.ColumnTypes {
			outputColumns = append(outputColumns, uint32(i))
		}
		r.Root, r.ColumnTypes = addProjection(r.Root, filterColumnTypes, outputColumns)
	}
	return nil
}

func examineLikeOp(op treecmp.ComparisonOperator) (negate bool, caseInsensitive bool) {
	negate = op.Symbol == treecmp.NotLike || op.Symbol == treecmp.NotILike
	caseInsensitive = op.Symbol == treecmp.ILike || op.Symbol == treecmp.NotILike
	return negate, caseInsensitive
}

func planSelectionOperators(
	ctx context.Context,
	evalCtx *eval.Context,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	allocator *colmem.Allocator,
	releasables *[]execreleasable.Releasable,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	switch t := expr.(type) {
	case *tree.AndExpr:
		// AND expressions are handled by an implicit AND'ing of selection
		// vectors. First we select out the tuples that are true on the left
		// side, and then, only among the matched tuples, we select out the
		// tuples that are true on the right side.
		var leftOp, rightOp colexecop.Operator
		leftOp, _, typs, err = planSelectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		rightOp, resultIdx, typs, err = planSelectionOperators(
			ctx, evalCtx, t.TypedRight(), typs, leftOp, allocator, releasables,
		)
		return rightOp, resultIdx, typs, err
	case *tree.CaseExpr, *tree.CastExpr, *tree.CoalesceExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op, err = colexecutils.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, err
	case *tree.ComparisonExpr:
		cmpOp := t.Operator
		leftOp, leftIdx, ct, err := planProjectionOperators(
			ctx, evalCtx, t.TypedLeft(), columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		lTyp := ct[leftIdx]
		if constArg, ok := t.Right.(tree.Datum); ok {
			if !t.Op.CalledOnNullInput && constArg == tree.DNull {
				// If the RHS is NULL and the operator is not called on NULL,
				// the result will be NULL and the selection vector will always
				// be empty. So we can simply plan a zero operator.
				op = colexecutils.NewZeroOp(input)
				return op, resultIdx, ct, err
			}
			switch cmpOp.Symbol {
			case treecmp.Like, treecmp.NotLike, treecmp.ILike, treecmp.NotILike:
				negate, caseInsensitive := examineLikeOp(cmpOp)
				op, err = colexecsel.GetLikeOperator(
					evalCtx, leftOp, leftIdx, string(tree.MustBeDString(constArg)),
					negate, caseInsensitive,
				)
			case treecmp.In, treecmp.NotIn:
				negate := cmpOp.Symbol == treecmp.NotIn
				datumTuple, ok := tree.AsDTuple(constArg)
				if !ok || useDefaultCmpOpForIn(datumTuple) {
					break
				}
				op, err = colexec.GetInOperator(ctx, evalCtx, lTyp, leftOp, leftIdx, datumTuple, negate)
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
				if r, ok := op.(execreleasable.Releasable); ok {
					*releasables = append(*releasables, r)
				}
			}
			return op, resultIdx, ct, err
		}
		rightOp, rightIdx, ct, err := planProjectionOperators(
			ctx, evalCtx, t.TypedRight(), ct, leftOp, allocator, releasables,
		)
		if err != nil {
			return nil, resultIdx, ct, err
		}
		op, err = colexecsel.GetSelectionOperator(
			cmpOp, rightOp, ct, leftIdx, rightIdx, evalCtx, t,
		)
		if r, ok := op.(execreleasable.Releasable); ok {
			*releasables = append(*releasables, r)
		}
		return op, resultIdx, ct, err
	case *tree.IndexedVar:
		op, err = colexecutils.BoolOrUnknownToSelOp(input, columnTypes, t.Idx)
		return op, -1, columnTypes, err
	case *tree.IsNotNullExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op = colexec.NewIsNullSelOp(
			op, resultIdx, true /* negate */, typs[resultIdx].Family() == types.TupleFamily,
		)
		return op, resultIdx, typs, err
	case *tree.IsNullExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op = colexec.NewIsNullSelOp(
			op, resultIdx, false /* negate */, typs[resultIdx].Family() == types.TupleFamily,
		)
		return op, resultIdx, typs, err
	case *tree.NotExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		op, err = colexec.NewNotExprSelOp(t.TypedInnerExpr().ResolvedType().Family(), op, resultIdx)
		return op, resultIdx, typs, err
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
			ctx, evalCtx, caseExpr, columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		op, err = colexecutils.BoolOrUnknownToSelOp(op, typs, resultIdx)
		return op, resultIdx, typs, err
	default:
		err = errUnhandledSelectionExpression
		if log.ExpensiveLogEnabled(ctx, 1) {
			err = errors.Newf("unhandled selection expression type: %s", reflect.TypeOf(t))
		}
		return nil, resultIdx, nil, err
	}
}

// planCastOperator plans a CAST operator that casts the column at index
// 'inputIdx' coming from input of type 'fromType' into a column of type
// 'toType' that will be output at index 'resultIdx'.
func planCastOperator(
	ctx context.Context,
	columnTypes []*types.T,
	input colexecop.Operator,
	inputIdx int,
	fromType *types.T,
	toType *types.T,
	allocator *colmem.Allocator,
	evalCtx *eval.Context,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	outputIdx := len(columnTypes)
	op, err = colexecbase.GetCastOperator(ctx, allocator, input, inputIdx, outputIdx, fromType, toType, evalCtx)
	typs = append(columnTypes, toType)
	return op, outputIdx, typs, err
}

// planProjectionOperators plans a chain of operators to execute the provided
// expression. It returns the tail of the chain, as well as the column index
// of the expression's result (if any, otherwise -1) and the column types of the
// resulting batches.
func planProjectionOperators(
	ctx context.Context,
	evalCtx *eval.Context,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	allocator *colmem.Allocator,
	releasables *[]execreleasable.Releasable,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	// projectDatum is a helper function that adds a new constant projection
	// operator for the given datum. typs are updated accordingly.
	projectDatum := func(datum tree.Datum) (colexecop.Operator, error) {
		resultIdx = len(columnTypes)
		datumType := datum.ResolvedType()
		typs = append(columnTypes, datumType)
		if datumType.Family() == types.UnknownFamily {
			// We handle Unknown type by planning a special constant null
			// operator.
			return colexecbase.NewConstNullOp(allocator, input, resultIdx), nil
		}
		constVal := colconv.GetDatumToPhysicalFn(datumType)(datum)
		return colexecbase.NewConstOp(allocator, input, datumType, constVal, resultIdx)
	}
	resultIdx = -1
	switch t := expr.(type) {
	case *tree.AndExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, allocator, releasables)
	case *tree.BinaryExpr:
		if err = checkSupportedBinaryExpr(t.TypedLeft(), t.TypedRight(), t.ResolvedType()); err != nil {
			return op, resultIdx, typs, err
		}
		leftExpr, rightExpr := t.TypedLeft(), t.TypedRight()
		if t.Operator.Symbol == treebin.Concat {
			// Concat requires special handling since it has special rules when
			// one of the arguments is an array or a string. We don't have
			// native vectorized support for arrays yet, so we don't have to do
			// anything extra for them, but we do need to handle the string
			// case.
			leftType, rightType := leftExpr.ResolvedType(), rightExpr.ResolvedType()
			if t.Op.ReturnType.Identical(types.String) && leftType.Family() != rightType.Family() {
				// This is a special case of the STRING concatenation - we have
				// to plan a cast of the non-string type to a STRING.
				if leftType.Family() == types.StringFamily {
					rightExpr = tree.NewTypedCastExpr(rightExpr, types.String)
				} else if rightType.Family() == types.StringFamily {
					leftExpr = tree.NewTypedCastExpr(leftExpr, types.String)
				} else {
					// This is unexpected.
					return op, resultIdx, typs, errors.AssertionFailedf("neither LHS or RHS of Concat operation is a STRING")
				}
			}
		}
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), leftExpr, rightExpr,
			columnTypes, input, allocator, t.Op.EvalOp, nil /* cmpExpr */, releasables, t.Op.CalledOnNullInput,
		)
	case *tree.CaseExpr:
		caseOutputType := t.ResolvedType()
		caseOutputIdx := len(columnTypes)
		// We don't know the schema yet and will update it below, right before
		// instantiating caseOp. The same goes for subsetEndIdx.
		schemaEnforcer := colexecutils.NewBatchSchemaSubsetEnforcer(
			allocator, input, nil /* typs */, caseOutputIdx, -1, /* subsetEndIdx */
		)
		buffer := colexec.NewBufferOp(schemaEnforcer)
		caseOps := make([]colexecop.Operator, len(t.Whens))
		typs = append(columnTypes, caseOutputType)
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
				ctx, evalCtx, whenTyped, typs, buffer, allocator, releasables,
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
					ctx, evalCtx, cmpExpr, typs, caseOps[i], allocator, releasables,
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
				ctx, evalCtx, when.Val.(tree.TypedExpr), typs, caseOps[i], allocator, releasables,
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
					ctx, typs, caseOps[i], thenIdxs[i], fromType, toType, allocator, evalCtx,
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
			ctx, evalCtx, elseExpr.(tree.TypedExpr), typs, buffer, allocator, releasables,
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
				ctx, typs, elseOp, elseIdx, fromType, toType, allocator, evalCtx,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
		}

		schemaEnforcer.SetTypes(typs)
		op = colexec.NewCaseOp(allocator, buffer, caseOps, elseOp, thenIdxs, caseOutputIdx, caseOutputType)
		return op, caseOutputIdx, typs, err
	case *tree.CastExpr:
		expr := t.Expr.(tree.TypedExpr)
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, expr, columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return nil, 0, nil, err
		}
		op, resultIdx, typs, err = planCastOperator(ctx, typs, op, resultIdx, expr.ResolvedType(), t.ResolvedType(), allocator, evalCtx)
		return op, resultIdx, typs, err
	case *tree.CoalesceExpr:
		// We handle CoalesceExpr by planning the equivalent CASE expression.
		// Each WHEN condition is `IS DISTINCT FROM NULL` if the expression in the
		// Coalesce allows that operation, otherwise, IS NOT NULL is used.
		// For example,
		//
		//   CASE
		//     WHEN CoalesceExpr.Exprs[0] IS DISTINCT FROM NULL THEN CoalesceExpr.Exprs[0]
		//     WHEN CoalesceExpr.Exprs[1] IS NOT NULL THEN CoalesceExpr.Exprs[1]
		//     ...
		//   END
		whenVals := make([]tree.When, len(t.Exprs))
		whens := make([]*tree.When, len(t.Exprs))
		for i := range whens {
			whenVals[i] = tree.When{
				Cond: t.GetWhenCondition(i),
				Val:  t.Exprs[i],
			}
			whens[i] = &whenVals[i]
		}
		caseExpr, err := tree.NewTypedCaseExpr(
			nil, /* expr */
			whens,
			nil, /* elseStmt */
			t.ResolvedType(),
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		return planProjectionOperators(ctx, evalCtx, caseExpr, columnTypes, input, allocator, releasables)
	case *tree.ComparisonExpr:
		if err = checkSupportedComparisonExpr(t.TypedLeft(), t.TypedRight()); err != nil {
			return op, resultIdx, typs, err
		}
		return planProjectionExpr(
			ctx, evalCtx, t.Operator, t.ResolvedType(), t.TypedLeft(), t.TypedRight(),
			columnTypes, input, allocator, nil /* binFn */, t, releasables, t.Op.CalledOnNullInput,
		)
	case tree.Datum:
		op, err = projectDatum(t)
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
				ctx, evalCtx, e.(tree.TypedExpr), typs, op, allocator, releasables,
			)
			if err != nil {
				return nil, resultIdx, nil, err
			}
			inputCols = append(inputCols, resultIdx)
		}
		resultIdx = len(typs)
		op, err = colexec.NewBuiltinFunctionOperator(
			allocator, evalCtx, t, typs, inputCols, resultIdx, op,
		)
		if r, ok := op.(execreleasable.Releasable); ok {
			*releasables = append(*releasables, r)
		}
		typs = append(typs, t.ResolvedType())
		return op, resultIdx, typs, err
	case *tree.IfExpr:
		// We handle IfExpr by planning the equivalent CASE expression, namely
		//   CASE WHEN IfExpr.Cond THEN IfExpr.True ELSE IfExpr.Else END.
		caseExpr, err := tree.NewTypedCaseExpr(
			nil, /* expr */
			[]*tree.When{{Cond: t.Cond, Val: t.True}},
			t.TypedElseExpr(),
			t.ResolvedType(),
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		return planProjectionOperators(ctx, evalCtx, caseExpr, columnTypes, input, allocator, releasables)
	case *tree.IndexedVar:
		return input, t.Idx, columnTypes, nil
	case *tree.IsNotNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, allocator, true /* negate */, releasables)
	case *tree.IsNullExpr:
		return planIsNullProjectionOp(ctx, evalCtx, t.ResolvedType(), t.TypedInnerExpr(), columnTypes, input, allocator, false /* negate */, releasables)
	case *tree.NotExpr:
		op, resultIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, t.TypedInnerExpr(), columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		outputIdx := len(typs)
		op, err = colexec.NewNotExprProjOp(
			t.TypedInnerExpr().ResolvedType().Family(), allocator, op, resultIdx, outputIdx,
		)
		if err != nil {
			return op, resultIdx, typs, err
		}
		typs = append(typs, t.ResolvedType())
		return op, outputIdx, typs, nil
	case *tree.NullIfExpr:
		// We handle NullIfExpr by planning the equivalent CASE expression,
		// namely
		//   CASE WHEN Expr1 == Expr2 THEN NULL ELSE Expr1 END.
		caseExpr, err := tree.NewTypedCaseExpr(
			nil, /* expr */
			[]*tree.When{{
				Cond: tree.NewTypedComparisonExpr(
					treecmp.MakeComparisonOperator(treecmp.EQ),
					t.Expr1.(tree.TypedExpr),
					t.Expr2.(tree.TypedExpr),
				),
				Val: tree.DNull,
			}},
			t.Expr1.(tree.TypedExpr),
			t.ResolvedType(),
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		return planProjectionOperators(ctx, evalCtx, caseExpr, columnTypes, input, allocator, releasables)
	case *tree.OrExpr:
		return planLogicalProjectionOp(ctx, evalCtx, expr, columnTypes, input, allocator, releasables)
	case *tree.Tuple:
		tuple, isConstTuple := evalTupleIfConst(ctx, evalCtx, t)
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
				ctx, evalCtx, expr.(tree.TypedExpr), typs, input, allocator, releasables,
			)
			if err != nil {
				return nil, resultIdx, typs, err
			}
		}
		resultIdx = len(typs)
		op = colexec.NewTupleProjOp(allocator, typs, tupleContentsIdxs, outputType, input, resultIdx)
		*releasables = append(*releasables, op.(execreleasable.Releasable))
		typs = append(typs, outputType)
		return op, resultIdx, typs, err
	default:
		err = errUnhandledProjectionExpression
		if log.ExpensiveLogEnabled(ctx, 1) {
			err = errors.Newf("unhandled projection expression type: %s", reflect.TypeOf(t))
		}
		return nil, resultIdx, nil, err
	}
}

// safeTypesForBinOrCmpExpr returns true if the given type pair is definitely
// safe for Binary or Comparison expressions (in other words, this type pair
// will definitely not result in problematic mixed-type expressions that we want
// to avoid (see a few errors below for examples)).
func safeTypesForBinOrCmpExpr(leftTyp, rightTyp *types.T) bool {
	if leftTyp.Equivalent(rightTyp) {
		// All same type expressions are safe.
		return true
	}
	if leftTyp.Family() == types.UnknownFamily || rightTyp.Family() == types.UnknownFamily {
		// If either type is of an Unknown family, then the corresponding vector
		// will only contain NULL values, so we won't run into the mixed-type
		// issues.
		//
		// Note that in the general case the optimizer should constant-fold such
		// expressions, but we are constructing some expressions manually during
		// the vectorized planning (e.g. handling of COALESCE expression
		// involves the creation of IS DISTINCT FROM comparison expression)
		// which don't go through the optimizer, so we have this check.
		return true
	}
	return false
}

func checkSupportedBinaryExpr(left, right tree.TypedExpr, outputType *types.T) error {
	leftTyp := left.ResolvedType()
	rightTyp := right.ResolvedType()

	leftDatumBacked := typeconv.TypeFamilyToCanonicalTypeFamily(leftTyp.Family()) == typeconv.DatumVecCanonicalTypeFamily
	rightDatumBacked := typeconv.TypeFamilyToCanonicalTypeFamily(rightTyp.Family()) == typeconv.DatumVecCanonicalTypeFamily
	outputDatumBacked := typeconv.TypeFamilyToCanonicalTypeFamily(outputType.Family()) == typeconv.DatumVecCanonicalTypeFamily
	if (leftDatumBacked && rightDatumBacked) && !outputDatumBacked {
		return errBinaryExprWithDatums
	}

	if safeTypesForBinOrCmpExpr(leftTyp, rightTyp) {
		return nil
	}

	// Check whether we have a mixed type expression with one of the types we
	// want to avoid.
	for _, t := range []*types.T{leftTyp, rightTyp} {
		switch t.Family() {
		case types.DateFamily, types.TimestampTZFamily:
			// Note that the Timestamp type is ok because the current
			// implementation of binary expressions that use TimestampTZ
			// canonical type family on one side hard-codes the choice of
			// Timestamp (as opposed to TimestampTZ).
			return errMixedTypeBinaryUnsupported
		}
	}

	return nil
}

func checkSupportedComparisonExpr(left, right tree.TypedExpr) error {
	leftTyp := left.ResolvedType()
	rightTyp := right.ResolvedType()

	if safeTypesForBinOrCmpExpr(leftTyp, rightTyp) {
		return nil
	}

	// Check whether we have a mixed type expression with one of the types we
	// want to avoid.
	for _, t := range []*types.T{leftTyp, rightTyp} {
		switch t.Family() {
		case types.DateFamily, types.TimestampFamily, types.TimestampTZFamily:
			return errMixedTypeComparisonUnsupported
		}
	}

	return nil
}

func planProjectionExpr(
	ctx context.Context,
	evalCtx *eval.Context,
	projOp tree.Operator,
	outputType *types.T,
	left, right tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	allocator *colmem.Allocator,
	binOp tree.BinaryEvalOp,
	cmpExpr *tree.ComparisonExpr,
	releasables *[]execreleasable.Releasable,
	calledOnNullInput bool,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	resultIdx = -1

	cmpProjOp, isCmpProjOp := projOp.(treecmp.ComparisonOperator)
	var hasOptimizedOp bool
	if isCmpProjOp {
		switch cmpProjOp.Symbol {
		case treecmp.Like, treecmp.NotLike, treecmp.ILike, treecmp.NotILike,
			treecmp.In, treecmp.NotIn, treecmp.IsDistinctFrom, treecmp.IsNotDistinctFrom:
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
			ctx, evalCtx, right, columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}
		resultIdx = len(typs)
		// The projection result will be outputted to a new column which is
		// appended to the input batch.
		// TODO(#127814): We may need to handle the case when the left is DNull.
		op, err = colexecprojconst.GetProjectionLConstOperator(
			allocator, typs, left.ResolvedType(), outputType, projOp, input,
			rightIdx, lConstArg, resultIdx, evalCtx, binOp, cmpExpr, calledOnNullInput,
		)
	} else {
		var leftIdx int
		input, leftIdx, typs, err = planProjectionOperators(
			ctx, evalCtx, left, columnTypes, input, allocator, releasables,
		)
		if err != nil {
			return nil, resultIdx, typs, err
		}

		if tuple, ok := right.(*tree.Tuple); ok {
			// If we have a tuple on the right, try to pre-evaluate it in case
			// it consists only of constant expressions.
			tupleDatum, ok := evalTupleIfConst(ctx, evalCtx, tuple)
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
			if !calledOnNullInput && right == tree.DNull {
				// If the right is NULL and the operator is not called on NULL,
				// simply project NULL.
				op = colexecbase.NewConstNullOp(allocator, input, resultIdx)
			} else if isCmpProjOp {
				// Use optimized operators for special cases.
				switch cmpProjOp.Symbol {
				case treecmp.Like, treecmp.NotLike, treecmp.ILike, treecmp.NotILike:
					negate, caseInsensitive := examineLikeOp(cmpProjOp)
					op, err = colexecprojconst.GetLikeProjectionOperator(
						allocator, evalCtx, input, leftIdx, resultIdx,
						string(tree.MustBeDString(rConstArg)), negate, caseInsensitive,
					)
				case treecmp.In, treecmp.NotIn:
					negate := cmpProjOp.Symbol == treecmp.NotIn
					datumTuple, ok := tree.AsDTuple(rConstArg)
					if !ok || useDefaultCmpOpForIn(datumTuple) {
						break
					}
					op, err = colexec.GetInProjectionOperator(
						ctx, evalCtx, allocator, typs[leftIdx], input, leftIdx, resultIdx, datumTuple, negate,
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
				op, err = colexecprojconst.GetProjectionRConstOperator(
					allocator, typs, right.ResolvedType(), outputType, projOp,
					input, leftIdx, rConstArg, resultIdx, evalCtx, binOp, cmpExpr, calledOnNullInput,
				)
			}
		} else {
			// Case 3: neither are constant.
			var rightIdx int
			input, rightIdx, typs, err = planProjectionOperators(
				ctx, evalCtx, right, typs, input, allocator, releasables,
			)
			if err != nil {
				return nil, resultIdx, nil, err
			}
			resultIdx = len(typs)
			op, err = colexecproj.GetProjectionOperator(
				allocator, typs, outputType, projOp, input, leftIdx, rightIdx,
				resultIdx, evalCtx, binOp, cmpExpr, calledOnNullInput,
			)
		}
	}
	if err != nil {
		return op, resultIdx, typs, err
	}
	if r, ok := op.(execreleasable.Releasable); ok {
		*releasables = append(*releasables, r)
	}
	typs = append(typs, outputType)
	return op, resultIdx, typs, err
}

// planLogicalProjectionOp plans all the needed operators for a projection of
// a logical operation (either AND or OR).
func planLogicalProjectionOp(
	ctx context.Context,
	evalCtx *eval.Context,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	allocator *colmem.Allocator,
	releasables *[]execreleasable.Releasable,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	// Add a new boolean column that will store the result of the projection.
	resultIdx = len(columnTypes)
	typs = append(columnTypes, types.Bool)
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
		ctx, evalCtx, typedLeft, typs, leftFeedOp, allocator, releasables,
	)
	if err != nil {
		return nil, resultIdx, typs, err
	}
	rightProjOpChain, rightIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, typedRight, typs, rightFeedOp, allocator, releasables,
	)
	if err != nil {
		return nil, resultIdx, typs, err
	}
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
	evalCtx *eval.Context,
	outputType *types.T,
	expr tree.TypedExpr,
	columnTypes []*types.T,
	input colexecop.Operator,
	allocator *colmem.Allocator,
	negate bool,
	releasables *[]execreleasable.Releasable,
) (op colexecop.Operator, resultIdx int, typs []*types.T, err error) {
	op, resultIdx, typs, err = planProjectionOperators(
		ctx, evalCtx, expr, columnTypes, input, allocator, releasables,
	)
	if err != nil {
		return op, resultIdx, typs, err
	}
	outputIdx := len(typs)
	isTupleNull := typs[resultIdx].Family() == types.TupleFamily
	op = colexec.NewIsNullProjOp(allocator, op, resultIdx, outputIdx, negate, isTupleNull)
	typs = append(typs, outputType)
	return op, outputIdx, typs, nil
}

// useDefaultCmpOpForIn returns whether IN and NOT IN projection/selection
// operators should be handled via the default operators. This is the case when
// we have an empty tuple or the tuple contains other tuples (these cases
// require special null-handling logic).
func useDefaultCmpOpForIn(tuple *tree.DTuple) bool {
	tupleContents := tuple.ResolvedType().TupleContents()
	if len(tupleContents) == 0 || len(tuple.D) == 0 {
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
func evalTupleIfConst(
	ctx context.Context, evalCtx *eval.Context, t *tree.Tuple,
) (_ tree.Datum, ok bool) {
	for _, expr := range t.Exprs {
		if _, isDatum := expr.(tree.Datum); !isDatum {
			// Not a constant expression.
			return nil, false
		}
	}
	tuple, err := eval.Expr(ctx, evalCtx, t)
	if err != nil {
		return nil, false
	}
	return tuple, true
}
