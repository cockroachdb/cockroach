// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"math"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// joinReaderState represents the state of the processor.
type joinReaderState int

const (
	jrStateUnknown joinReaderState = iota
	// jrReadingInput means that a batch of rows is being read from the input and
	// matching KVs from the lookup index are being scanned.
	jrReadingInput
	// jrFetchingLookupRows means we are fetching an index lookup row from the
	// buffered batch of lookup rows that were previously read from the KV layer.
	jrFetchingLookupRows
	// jrEmittingRows means we are emitting the results of the index lookup.
	jrEmittingRows
	// jrReadyToDrain means we are done but have not yet started draining.
	jrReadyToDrain
)

// joinReaderType represents the type of join being used.
type joinReaderType int

const (
	// lookupJoinReaderType means we are performing a lookup join.
	lookupJoinReaderType joinReaderType = iota
	// indexJoinReaderType means we are performing an index join.
	indexJoinReaderType
)

// joinReader performs a lookup join between `input` and the specified `index`.
// `lookupCols` specifies the input columns which will be used for the index
// lookup.
type joinReader struct {
	joinerBase
	strategy joinReaderStrategy

	// runningState represents the state of the joinReader. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState joinReaderState

	// memAcc is used to account for the memory used by the in-memory data
	// structures used directly by the joinReader. Note that the joinReader
	// strategies and span generators have separate accounts.
	memAcc mon.BoundAccount

	// accountedFor tracks the memory usage of scratchInputRows and
	// groupingState that is currently registered with memAcc.
	accountedFor struct {
		// scratchInputRows accounts only for the slice of scratchInputRows, not
		// the actual rows.
		scratchInputRows int64
		groupingState    int64
	}

	// limitedMemMonitor is a limited memory monitor to account for the memory
	// used by buffered rows in joinReaderOrderingStrategy. If the memory limit is
	// exceeded, the joinReader will spill to disk. diskMonitor is used to monitor
	// the disk utilization in this case.
	limitedMemMonitor   *mon.BytesMonitor
	unlimitedMemMonitor *mon.BytesMonitor
	diskMonitor         *mon.BytesMonitor

	fetchSpec      fetchpb.IndexFetchSpec
	splitFamilyIDs []descpb.FamilyID

	// Indicates that the join reader should maintain the ordering of the input
	// stream. This is applicable to both lookup joins and index joins. For lookup
	// joins, maintaining order is expensive because it requires buffering. For
	// index joins buffering is not required, but still, if ordering is not
	// required, we'll change the output order to allow for some Pebble
	// optimizations.
	maintainOrdering bool

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// joinReader to wrap the fetcher with a stat collector when necessary.
	fetcher            rowFetcher
	alloc              tree.DatumAlloc
	rowAlloc           rowenc.EncDatumRowAlloc
	shouldLimitBatches bool
	readerType         joinReaderType

	// txn is the transaction used by the join reader.
	txn *kv.Txn

	// usesStreamer indicates whether the joinReader performs the lookups using
	// the kvstreamer.Streamer API.
	usesStreamer bool
	streamerInfo struct {
		unlimitedMemMonitor *mon.BytesMonitor
		budgetAcc           mon.BoundAccount
		// maintainOrdering indicates whether the ordering of the input stream
		// needs to be maintained AND that we rely on the streamer for that. We
		// currently rely on the streamer in the following cases:
		//   1. When spec.SplitFamilyIDs has more than one family, for both
		//      index and lookup joins (this is needed to ensure that all KVs
		//      for a single row are returned contiguously).
		//   2. We are performing an index join and spec.MaintainOrdering is
		//      true.
		//   3. We are performing a lookup join and spec.MaintainLookupOrdering
		//      is true.
		// Note that in case (3), we don't rely on the streamer for maintaining
		// the ordering for lookup joins when spec.MaintainOrdering is true due
		// to implementation details (since we still buffer all looked up rows
		// and restore the ordering explicitly via the
		// joinReaderOrderingStrategy).
		maintainOrdering    bool
		diskMonitor         *mon.BytesMonitor
		txnKVStreamerMemAcc mon.BoundAccount
	}

	input execinfra.RowSource

	// lookupCols and lookupExpr (and optionally remoteLookupExpr) represent the
	// part of the join condition used to perform the lookup into the index.
	// Exactly one of lookupCols or lookupExpr must be non-empty.
	//
	// lookupCols is used when the lookup condition is just a simple equality
	// between input columns and index columns. In this case, lookupCols contains
	// the column indexes in the input stream specifying the columns which match
	// with the index columns. These are the equality columns of the join.
	//
	// lookupExpr is used when the lookup condition is more complicated than a
	// simple equality between input columns and index columns. In this case,
	// lookupExpr specifies the expression that will be used to construct the
	// spans for each lookup. See comments in the spec for details about the
	// supported expressions.
	//
	// If remoteLookupExpr is set, this is a locality optimized lookup join. In
	// this case, lookupExpr contains the lookup join conditions targeting ranges
	// located on local nodes (relative to the gateway region), and
	// remoteLookupExpr contains the lookup join conditions targeting remote
	// nodes. See comments in the spec for more details.
	lookupCols       []uint32
	lookupExpr       execinfrapb.ExprHelper
	remoteLookupExpr execinfrapb.ExprHelper

	// spansCanOverlap indicates whether the spans generated for a given input
	// batch can overlap. It is used in the fetcher when deciding whether a newly
	// read kv corresponds to a new row.
	spansCanOverlap bool

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSizeBytes    int64
	curBatchSizeBytes int64

	// pendingRow tracks the row that has already been read from the input but
	// was not included into the lookup batch because it would make the batch
	// exceed batchSizeBytes.
	pendingRow rowenc.EncDatumRow

	// rowsRead is the total number of rows that this fetcher read from
	// disk.
	rowsRead int64

	contentionEventsListener  execstats.ContentionEventsListener
	scanStatsListener         execstats.ScanStatsListener
	tenantConsumptionListener execstats.TenantConsumptionListener

	// curBatchRowsRead is the number of rows that this fetcher read from disk for
	// the current batch.
	curBatchRowsRead int64

	// curBatchInputRowCount is the number of input rows in the current batch.
	curBatchInputRowCount int64

	// State variables for each batch of input rows.
	scratchInputRows rowenc.EncDatumRows
	// resetScratchWhenReadingInput tracks whether scratchInputRows needs to be
	// reset the next time the joinReader is in the jrReadingInput state.
	resetScratchWhenReadingInput bool

	// Fields used when this is the second join in a pair of joins that are
	// together implementing left {outer,semi,anti} joins where the first join
	// produces false positives because it cannot evaluate the whole expression
	// (or evaluate it accurately, as is sometimes the case with inverted
	// indexes). The first join is running a left outer or inner join, and each
	// group of rows seen by the second join correspond to one left row.

	// The input rows in the current batch belong to groups which are tracked in
	// groupingState. The last row from the last batch is in
	// lastInputRowFromLastBatch -- it is tracked because we don't know if it
	// was the last row in a group until we get to the next batch. NB:
	// groupingState is used even when there is no grouping -- we simply have
	// groups of one. The no grouping cases include the case of this join being
	// the first join in the paired joins.
	groupingState *inputBatchGroupingState

	lastBatchState struct {
		lastInputRow       rowenc.EncDatumRow
		lastGroupMatched   bool
		lastGroupContinued bool
	}

	// Set to true when this is the first join in the paired-joins (see the
	// detailed comment in the spec). This can never be true for index joins,
	// and requires that the spec has MaintainOrdering set to true.
	outputGroupContinuationForLeftRow bool

	// lookupBatchBytesLimit controls the TargetBytes of lookup requests. If 0, a
	// default will be used. Regardless of this value, bytes limits aren't always
	// used.
	lookupBatchBytesLimit rowinfra.BytesLimit

	// limitHintHelper is used in limiting batches of input rows in the presence
	// of hard and soft limits.
	limitHintHelper execinfra.LimitHintHelper

	// Set errorOnLookup to true to cause the join to error out just prior to
	// performing a lookup. This is currently only set when the join contains
	// only lookups to rows in remote regions and remote accesses are set to
	// error out via a session setting.
	errorOnLookup bool

	// allowEnforceHomeRegionFollowerReads, if true, causes errors produced by the
	// above `errorOnLookup` flag to be retryable, and use follower reads to find
	// the query's home region during the retries.
	allowEnforceHomeRegionFollowerReads bool
}

var _ execinfra.Processor = &joinReader{}
var _ execinfra.RowSource = &joinReader{}
var _ execopnode.OpNode = &joinReader{}

const joinReaderProcName = "join reader"

// ParallelizeMultiKeyLookupJoinsEnabled determines whether the joinReader
// parallelizes KV batches in all cases.
var ParallelizeMultiKeyLookupJoinsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.distsql.parallelize_multi_key_lookup_joins.enabled",
	"determines whether KV batches are executed in parallel for lookup joins in all cases. "+
		"Enabling this will increase the speed of lookup joins when each input row might get "+
		"multiple looked up rows at the cost of increased memory usage.",
	false,
)

// newJoinReader returns a new joinReader.
func newJoinReader(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.JoinReaderSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	readerType joinReaderType,
) (execinfra.RowSourcedProcessor, error) {
	if spec.OutputGroupContinuationForLeftRow && !spec.MaintainOrdering {
		return nil, errors.AssertionFailedf(
			"lookup join must maintain ordering since it is first join in paired-joins")
	}
	switch readerType {
	case lookupJoinReaderType:
		switch spec.Type {
		case descpb.InnerJoin, descpb.LeftOuterJoin, descpb.LeftSemiJoin, descpb.LeftAntiJoin:
		default:
			return nil, errors.AssertionFailedf("only inner and left {outer, semi, anti} lookup joins are supported, %s requested", spec.Type)
		}
	case indexJoinReaderType:
		if spec.Type != descpb.InnerJoin {
			return nil, errors.AssertionFailedf("only inner index joins are supported, %s requested", spec.Type)
		}
		if !spec.LookupExpr.Empty() {
			return nil, errors.AssertionFailedf("non-empty lookup expressions are not supported for index joins")
		}
		if !spec.RemoteLookupExpr.Empty() {
			return nil, errors.AssertionFailedf("non-empty remote lookup expressions are not supported for index joins")
		}
		if !spec.OnExpr.Empty() {
			return nil, errors.AssertionFailedf("non-empty ON expressions are not supported for index joins")
		}
	}

	var lookupCols []uint32
	switch readerType {
	case indexJoinReaderType:
		lookupCols = make([]uint32, len(spec.FetchSpec.KeyColumns()))
		for i := range lookupCols {
			lookupCols[i] = uint32(i)
		}
	case lookupJoinReaderType:
		lookupCols = spec.LookupColumns
	default:
		return nil, errors.AssertionFailedf("unsupported joinReaderType")
	}
	// The joiner has a choice to make between getting DistSender-level
	// parallelism for its lookup batches and setting row and memory limits (due
	// to implementation limitations, you can't have both at the same time). We
	// choose parallelism when we know that each lookup returns at most one row:
	// in case of indexJoinReaderType, we know that there's exactly one lookup
	// row for each input row. Similarly, in case of spec.LookupColumnsAreKey,
	// we know that there's at most one lookup row per input row. In other
	// cases, we use limits.
	shouldLimitBatches := !spec.LookupColumnsAreKey && readerType == lookupJoinReaderType
	if flowCtx.EvalCtx.SessionData().ParallelizeMultiKeyLookupJoinsEnabled {
		shouldLimitBatches = false
	}
	if spec.MaintainLookupOrdering {
		// MaintainLookupOrdering indicates the output of the lookup joiner should
		// be sorted by <inputCols>, <lookupCols>. It doesn't make sense for
		// MaintainLookupOrdering to be true when MaintainOrdering is not.
		// Additionally, we need to disable parallelism for the traditional fetcher
		// in order to ensure the lookups are ordered, so set shouldLimitBatches.
		spec.MaintainOrdering, shouldLimitBatches = true, true
	}
	useStreamer, txn, err := flowCtx.UseStreamer(ctx)
	if err != nil {
		return nil, err
	}

	errorOnLookup := spec.RemoteOnlyLookups &&
		flowCtx.EvalCtx.Planner != nil && flowCtx.EvalCtx.Planner.EnforceHomeRegion()

	jr := &joinReader{
		fetchSpec:                           spec.FetchSpec,
		splitFamilyIDs:                      spec.SplitFamilyIDs,
		maintainOrdering:                    spec.MaintainOrdering,
		input:                               input,
		lookupCols:                          lookupCols,
		outputGroupContinuationForLeftRow:   spec.OutputGroupContinuationForLeftRow,
		shouldLimitBatches:                  shouldLimitBatches,
		readerType:                          readerType,
		txn:                                 txn,
		usesStreamer:                        useStreamer,
		lookupBatchBytesLimit:               rowinfra.BytesLimit(spec.LookupBatchBytesLimit),
		limitHintHelper:                     execinfra.MakeLimitHintHelper(spec.LimitHint, post),
		errorOnLookup:                       errorOnLookup,
		allowEnforceHomeRegionFollowerReads: flowCtx.EvalCtx.SessionData().EnforceHomeRegionFollowerReadsEnabled,
	}
	if readerType != indexJoinReaderType {
		jr.groupingState = &inputBatchGroupingState{doGrouping: spec.LeftJoinWithPairedJoiner}
	}

	// Make sure the key column types are hydrated. The fetched column types will
	// be hydrated in ProcessorBase.Init (via joinerBase.init).
	resolver := flowCtx.NewTypeResolver(jr.txn)
	for i := range spec.FetchSpec.KeyAndSuffixColumns {
		if err := typedesc.EnsureTypeIsHydrated(
			ctx, spec.FetchSpec.KeyAndSuffixColumns[i].Type, &resolver,
		); err != nil {
			return nil, err
		}
	}

	var leftTypes []*types.T
	switch readerType {
	case indexJoinReaderType:
		// Index join performs a join between a secondary index, the `input`,
		// and the primary index of the same table, `desc`, to retrieve columns
		// which are not stored in the secondary index. It outputs the looked
		// up rows as is (meaning that the output rows before post-processing
		// will contain all columns from the table) whereas the columns that
		// came from the secondary index (input rows) are ignored. As a result,
		// we leave leftTypes as empty.
	case lookupJoinReaderType:
		leftTypes = input.OutputTypes()
	default:
		return nil, errors.AssertionFailedf("unsupported joinReaderType")
	}
	rightTypes := spec.FetchSpec.FetchedColumnTypes()

	evalCtx, err := jr.joinerBase.init(
		ctx,
		jr,
		flowCtx,
		processorID,
		leftTypes,
		rightTypes,
		spec.Type,
		spec.OnExpr,
		spec.OutputGroupContinuationForLeftRow,
		post,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{jr.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				// We need to generate metadata before closing the processor
				// because InternalClose() updates jr.Ctx to the "original"
				// context.
				trailingMeta := jr.generateMeta()
				jr.close()
				return trailingMeta
			},
		},
	)
	if err != nil {
		return nil, err
	}

	if !spec.LookupExpr.Empty() {
		if evalCtx == flowCtx.EvalCtx {
			// We haven't created a copy of the eval context yet (because it is
			// only done in init if we have a non-empty ON expression), but we
			// actually need a copy.
			evalCtx = flowCtx.NewEvalCtx()
		}
		lookupExprTypes := make([]*types.T, 0, len(leftTypes)+len(rightTypes))
		lookupExprTypes = append(lookupExprTypes, leftTypes...)
		lookupExprTypes = append(lookupExprTypes, rightTypes...)

		semaCtx := flowCtx.NewSemaContext(jr.txn)
		if err := jr.lookupExpr.Init(ctx, spec.LookupExpr, lookupExprTypes, semaCtx, evalCtx); err != nil {
			return nil, err
		}
		if !spec.RemoteLookupExpr.Empty() {
			if err := jr.remoteLookupExpr.Init(
				ctx, spec.RemoteLookupExpr, lookupExprTypes, semaCtx, evalCtx,
			); err != nil {
				return nil, err
			}
		}
	}

	// We will create a memory monitor with a hard memory limit since the join
	// reader doesn't know how to spill its in-memory state to disk (separate
	// from the buffered rows). It is most likely that if the target limit is
	// really low then we're in a test scenario and we don't want to error out.
	minMemoryLimit := int64(8 << 20)
	// Streamer can handle lower memory limit and doing so makes testing at
	// the limits more efficient.
	if jr.usesStreamer {
		minMemoryLimit = 100 << 10
	}

	// Initialize memory monitors and bound account for data structures in the joinReader.
	jr.MemMonitor = execinfra.NewLimitedMonitorWithLowerBound(
		ctx, flowCtx, "joinreader-mem" /* name */, minMemoryLimit,
	)
	jr.memAcc = jr.MemMonitor.MakeBoundAccount()

	if err := jr.initJoinReaderStrategy(ctx, flowCtx, rightTypes, readerType); err != nil {
		return nil, err
	}
	jr.batchSizeBytes = jr.strategy.getLookupRowsBatchSizeHint(flowCtx.EvalCtx.SessionData())

	memoryLimit := execinfra.GetWorkMemLimit(flowCtx)
	if memoryLimit < minMemoryLimit {
		memoryLimit = minMemoryLimit
	}
	var streamingKVFetcher *row.KVFetcher
	if jr.usesStreamer {
		// NOTE: this comment should only be considered in a case of low workmem
		// limit (which is a testing scenario).
		//
		// When using the Streamer API, we want to limit the memory usage of the
		// join reader itself to be at most a quarter of the workmem limit. That
		// memory usage is comprised of several parts:
		// - the input batch (i.e. buffered input rows) which is limited by the
		//   batch size hint;
		// - some in-memory state of the join reader strategy;
		// - some in-memory state of the span generator.
		// We don't have any way of limiting the last two parts, so we apply a
		// simple heuristic that each of those parts takes up on the order of
		// the batch size hint.
		//
		// Thus, we arrive at the following setup:
		// - make the batch size hint to be at most 1/12 of workmem
		// - then reserve 3/12 of workmem for the memory usage of those three
		// parts.
		//
		// Note that it is ok if the batch size hint is set to zero since the
		// joinReader will always include at least one row into the lookup
		// batch.
		if jr.batchSizeBytes > memoryLimit/12 {
			jr.batchSizeBytes = memoryLimit / 12
		}
		// See the comment above for how we arrived at this calculation.
		//
		// That comment is made in the context of low workmem limit. However, in
		// production we expect workmem limit to be on the order of 64MiB
		// whereas the batch size hint is at most 4MiB, so the streamer will get
		// at least (depending on the hint) on the order of 52MiB which is
		// plenty enough.
		streamerBudgetLimit := memoryLimit - 3*jr.batchSizeBytes
		// We need to use an unlimited monitor for the streamer's budget since
		// the streamer itself is responsible for staying under the limit.
		jr.streamerInfo.unlimitedMemMonitor = mon.NewMonitorInheritWithLimit(
			"joinreader-streamer-unlimited" /* name */, math.MaxInt64, flowCtx.Mon, false, /* longLiving */
		)
		jr.streamerInfo.unlimitedMemMonitor.StartNoReserved(ctx, flowCtx.Mon)
		jr.streamerInfo.budgetAcc = jr.streamerInfo.unlimitedMemMonitor.MakeBoundAccount()
		jr.streamerInfo.txnKVStreamerMemAcc = jr.streamerInfo.unlimitedMemMonitor.MakeBoundAccount()
		// When we have SplitFamilyIDs with more than one family ID, then it's
		// possible for a single lookup span to be split into multiple "family"
		// spans, and in order to preserve the invariant that all KVs for a
		// single SQL row are contiguous we must ask the streamer to preserve
		// the ordering. See #113013 for an example.
		jr.streamerInfo.maintainOrdering = len(spec.SplitFamilyIDs) > 1
		if readerType == indexJoinReaderType {
			if spec.MaintainOrdering {
				// The index join can rely on the streamer to maintain the input
				// ordering.
				jr.streamerInfo.maintainOrdering = true
			}
		} else {
			// Due to implementation details (the join reader strategy restores
			// the desired order when spec.MaintainOrdering is set) we only need
			// to ask the streamer to maintain ordering if the results of each
			// lookup need to be returned in index order.
			if spec.MaintainLookupOrdering {
				jr.streamerInfo.maintainOrdering = true
			}
		}
		if jr.FlowCtx.EvalCtx.SessionData().StreamerAlwaysMaintainOrdering {
			jr.streamerInfo.maintainOrdering = true
		}

		var diskBuffer kvstreamer.ResultDiskBuffer
		if jr.streamerInfo.maintainOrdering {
			diskBufferMemAcc := jr.streamerInfo.unlimitedMemMonitor.MakeBoundAccount()
			jr.streamerInfo.diskMonitor = execinfra.NewMonitor(
				ctx, jr.FlowCtx.DiskMonitor, "streamer-disk", /* name */
			)
			diskBuffer = rowcontainer.NewKVStreamerResultDiskBuffer(
				jr.FlowCtx.Cfg.TempStorage, diskBufferMemAcc, jr.streamerInfo.diskMonitor,
			)
		}
		singleRowLookup := readerType == indexJoinReaderType || spec.LookupColumnsAreKey
		streamingKVFetcher = row.NewStreamingKVFetcher(
			flowCtx.Cfg.DistSender,
			flowCtx.Cfg.KVStreamerMetrics,
			flowCtx.Stopper(),
			jr.txn,
			flowCtx.Cfg.Settings,
			flowCtx.EvalCtx.SessionData(),
			spec.LockingWaitPolicy,
			spec.LockingStrength,
			spec.LockingDurability,
			streamerBudgetLimit,
			&jr.streamerInfo.budgetAcc,
			jr.streamerInfo.maintainOrdering,
			singleRowLookup,
			int(spec.FetchSpec.MaxKeysPerRow),
			diskBuffer,
			&jr.streamerInfo.txnKVStreamerMemAcc,
			spec.FetchSpec.External,
			row.FetchSpecRequiresRawMVCCValues(spec.FetchSpec),
		)
	} else {
		// When not using the Streamer API, we want to limit the batch size hint
		// to at most half of the workmem limit. Note that it is ok if it is set
		// to zero since the joinReader will always include at least one row
		// into the lookup batch.
		if jr.batchSizeBytes > memoryLimit/2 {
			jr.batchSizeBytes = memoryLimit / 2
		}
	}

	var fetcher row.Fetcher
	if err := fetcher.Init(
		ctx,
		row.FetcherInitArgs{
			StreamingKVFetcher:         streamingKVFetcher,
			Txn:                        jr.txn,
			LockStrength:               spec.LockingStrength,
			LockWaitPolicy:             spec.LockingWaitPolicy,
			LockDurability:             spec.LockingDurability,
			LockTimeout:                flowCtx.EvalCtx.SessionData().LockTimeout,
			DeadlockTimeout:            flowCtx.EvalCtx.SessionData().DeadlockTimeout,
			Alloc:                      &jr.alloc,
			MemMonitor:                 flowCtx.Mon,
			Spec:                       &spec.FetchSpec,
			TraceKV:                    flowCtx.TraceKV,
			ForceProductionKVBatchSize: flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
			SpansCanOverlap:            jr.spansCanOverlap,
		},
	); err != nil {
		return nil, err
	}

	if execstats.ShouldCollectStats(ctx, flowCtx.CollectStats) {
		jr.input = newInputStatCollector(jr.input)
		jr.fetcher = newRowFetcherStatCollector(&fetcher)
		jr.ExecStatsForTrace = jr.execStatsForTrace
	} else {
		jr.fetcher = &fetcher
	}

	// TODO(radu): verify the input types match the index key types
	return jr, nil
}

func (jr *joinReader) initJoinReaderStrategy(
	ctx context.Context, flowCtx *execinfra.FlowCtx, typs []*types.T, readerType joinReaderType,
) error {
	strategyMemAcc := jr.MemMonitor.MakeBoundAccount()
	spanGeneratorMemAcc := jr.MemMonitor.MakeBoundAccount()
	var generator joinReaderSpanGenerator
	if jr.lookupExpr.Expr() == nil {
		defGen := &defaultSpanGenerator{}
		if err := defGen.init(
			flowCtx.EvalCtx,
			flowCtx.Codec(),
			&jr.fetchSpec,
			jr.splitFamilyIDs,
			readerType == indexJoinReaderType, /* uniqueRows */
			jr.lookupCols,
			&spanGeneratorMemAcc,
		); err != nil {
			return err
		}
		generator = defGen
	} else {
		// Since jr.lookupExpr is set, we need to use either multiSpanGenerator or
		// localityOptimizedSpanGenerator, which support looking up multiple spans
		// per input row.

		// Map fetched columns to index key columns.
		var fetchedOrdToIndexKeyOrd util.FastIntMap
		fullColumns := jr.fetchSpec.KeyFullColumns()
		for keyOrdinal := range fullColumns {
			keyColID := fullColumns[keyOrdinal].ColumnID
			for fetchedOrdinal := range jr.fetchSpec.FetchedColumns {
				if jr.fetchSpec.FetchedColumns[fetchedOrdinal].ColumnID == keyColID {
					fetchedOrdToIndexKeyOrd.Set(fetchedOrdinal, keyOrdinal)
					break
				}
			}
		}

		// If jr.remoteLookupExpr is set, this is a locality optimized lookup join
		// and we need to use localityOptimizedSpanGenerator.
		var err error
		if jr.remoteLookupExpr.Expr() == nil {
			multiSpanGen := &multiSpanGenerator{}
			if jr.spansCanOverlap, err = multiSpanGen.init(
				flowCtx.EvalCtx,
				flowCtx.Codec(),
				&jr.fetchSpec,
				jr.splitFamilyIDs,
				len(jr.input.OutputTypes()),
				jr.lookupExpr.Expr(),
				fetchedOrdToIndexKeyOrd,
				&spanGeneratorMemAcc,
			); err != nil {
				return err
			}
			generator = multiSpanGen
		} else {
			localityOptSpanGen := &localityOptimizedSpanGenerator{}
			remoteSpanGenMemAcc := jr.MemMonitor.MakeBoundAccount()
			if jr.spansCanOverlap, err = localityOptSpanGen.init(
				flowCtx.EvalCtx,
				flowCtx.Codec(),
				&jr.fetchSpec,
				jr.splitFamilyIDs,
				len(jr.input.OutputTypes()),
				jr.lookupExpr.Expr(),
				jr.remoteLookupExpr.Expr(),
				fetchedOrdToIndexKeyOrd,
				&spanGeneratorMemAcc,
				&remoteSpanGenMemAcc,
			); err != nil {
				return err
			}
			generator = localityOptSpanGen
		}
	}

	defer func() {
		generator.setResizeMemoryAccountFunc(jr.strategy.resizeMemoryAccount)
	}()

	if readerType == indexJoinReaderType {
		jr.strategy = &joinReaderIndexJoinStrategy{
			joinerBase:              &jr.joinerBase,
			joinReaderSpanGenerator: generator,
			strategyMemAcc:          &strategyMemAcc,
		}
		return nil
	}

	if !jr.maintainOrdering {
		jr.strategy = &joinReaderNoOrderingStrategy{
			joinerBase:              &jr.joinerBase,
			joinReaderSpanGenerator: generator,
			isPartialJoin:           jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
			groupingState:           jr.groupingState,
			strategyMemAcc:          &strategyMemAcc,
		}
		return nil
	}

	// Limit the memory use by creating a child monitor with a hard limit.
	// joinReader will overflow to disk if this limit is not enough.
	limit := execinfra.GetWorkMemLimit(flowCtx)
	// Initialize memory monitors and row container for looked up rows.
	jr.limitedMemMonitor = execinfra.NewLimitedMonitor(ctx, jr.MemMonitor, flowCtx, "joinreader-limited")
	// We want to make sure that if the disk-backed container is spilled to
	// disk, it releases all of the memory reservations, so we make the
	// corresponding memory monitor not hold on to any bytes.
	jr.limitedMemMonitor.RelinquishAllOnReleaseBytes()
	jr.unlimitedMemMonitor = execinfra.NewMonitor(ctx, flowCtx.Mon, "joinreader-unlimited")
	jr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "joinreader-disk")
	drc := rowcontainer.NewDiskBackedNumberedRowContainer(
		false, /* deDup */
		typs,
		jr.FlowCtx.EvalCtx,
		jr.FlowCtx.Cfg.TempStorage,
		jr.limitedMemMonitor,
		jr.unlimitedMemMonitor,
		jr.diskMonitor,
	)
	if limit < mon.DefaultPoolAllocationSize {
		// The memory limit is too low for caching, most likely to force disk
		// spilling for testing.
		drc.DisableCache = true
	}
	jr.strategy = &joinReaderOrderingStrategy{
		joinerBase:                        &jr.joinerBase,
		joinReaderSpanGenerator:           generator,
		isPartialJoin:                     jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
		lookedUpRows:                      drc,
		groupingState:                     jr.groupingState,
		outputGroupContinuationForLeftRow: jr.outputGroupContinuationForLeftRow,
		strategyMemAcc:                    &strategyMemAcc,
	}
	return nil
}

// SetBatchSizeBytes sets the desired batch size. It should only be used in tests.
func (jr *joinReader) SetBatchSizeBytes(batchSize int64) {
	jr.batchSizeBytes = batchSize
}

// Spilled returns whether the joinReader spilled to disk.
func (jr *joinReader) Spilled() bool {
	return jr.strategy.spilled()
}

// Next is part of the RowSource interface.
func (jr *joinReader) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for jr.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch jr.runningState {
		case jrReadingInput:
			jr.runningState, row, meta = jr.readInput()
		case jrFetchingLookupRows:
			jr.runningState, meta = jr.fetchLookupRow()
		case jrEmittingRows:
			jr.runningState, row, meta = jr.emitRow()
		case jrReadyToDrain:
			jr.MoveToDraining(nil)
			meta = jr.DrainHelper()
			jr.runningState = jrStateUnknown
		default:
			log.Fatalf(jr.Ctx(), "unsupported state: %d", jr.runningState)
		}
		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := jr.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, jr.DrainHelper()
}

// addWorkmemHint checks whether err is non-nil, and if so, wraps it with a hint
// to increase workmem limit. It is expected that err was returned by the memory
// accounting system.
func addWorkmemHint(err error) error {
	if err == nil {
		return nil
	}
	return errors.WithHint(
		err, "consider increasing sql.distsql.temp_storage.workmem cluster"+
			" setting or distsql_workmem session variable",
	)
}

type spansWithSpanIDs struct {
	spans   roachpb.Spans
	spanIDs []int
}

var _ sort.Interface = &spansWithSpanIDs{}

func (s spansWithSpanIDs) Len() int {
	return len(s.spans)
}

func (s spansWithSpanIDs) Less(i, j int) bool {
	return s.spans[i].Key.Compare(s.spans[j].Key) < 0
}

func (s spansWithSpanIDs) Swap(i, j int) {
	s.spans[i], s.spans[j] = s.spans[j], s.spans[i]
	s.spanIDs[i], s.spanIDs[j] = s.spanIDs[j], s.spanIDs[i]
}

// sortSpans sorts the given spans while maintaining the spanIDs mapping (if it
// is non-nil).
func sortSpans(spans roachpb.Spans, spanIDs []int) {
	if spanIDs != nil {
		s := spansWithSpanIDs{
			spans:   spans,
			spanIDs: spanIDs,
		}
		sort.Sort(&s)
	} else {
		sort.Sort(spans)
	}
}

func (jr *joinReader) getBatchBytesLimit() rowinfra.BytesLimit {
	if jr.usesStreamer {
		// The streamer itself sets the correct TargetBytes parameter on the
		// BatchRequests.
		return rowinfra.NoBytesLimit
	}
	if !jr.shouldLimitBatches {
		// We deem it safe to not limit the batches in order to get the
		// DistSender-level parallelism.
		return rowinfra.NoBytesLimit
	}
	bytesLimit := jr.lookupBatchBytesLimit
	if bytesLimit == 0 {
		bytesLimit = rowinfra.GetDefaultBatchBytesLimit(jr.FlowCtx.EvalCtx.TestingKnobs.ForceProductionValues)
	}
	return bytesLimit
}

// readInput reads the next batch of input rows and starts an index scan, which
// for lookup join is the lookup of matching KVs for a batch of input rows.
// It can sometimes emit a single row on behalf of the previous batch.
func (jr *joinReader) readInput() (
	joinReaderState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if jr.groupingState != nil {
		// Lookup join.
		if jr.groupingState.initialized {
			// State is from last batch.
			jr.lastBatchState.lastGroupMatched = jr.groupingState.lastGroupMatched()
			jr.groupingState.reset()
			jr.lastBatchState.lastGroupContinued = false
		}
		// Else, returning meta interrupted reading the input batch, so we already
		// did the reset for this batch.
	}

	if jr.resetScratchWhenReadingInput {
		// Deeply reset the rows from the previous input batch.
		for i := range jr.scratchInputRows {
			jr.scratchInputRows[i] = nil
		}
		// We've just discarded the old rows, so we have to update the memory
		// accounting accordingly.
		newSz := jr.accountedFor.scratchInputRows + jr.accountedFor.groupingState
		if err := jr.strategy.resizeMemoryAccount(&jr.memAcc, jr.memAcc.Used(), newSz); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, nil, jr.DrainHelper()
		}
		jr.scratchInputRows = jr.scratchInputRows[:0]
		jr.resetScratchWhenReadingInput = false
	}

	// Read the next batch of input rows.
	for {
		var encDatumRow rowenc.EncDatumRow
		var rowSize int64
		if jr.pendingRow == nil {
			// There is no pending row, so we have to get the next one from the
			// input.
			var meta *execinfrapb.ProducerMetadata
			encDatumRow, meta = jr.input.Next()
			if meta != nil {
				if meta.Err != nil {
					jr.MoveToDraining(nil /* err */)
					return jrStateUnknown, nil, meta
				}

				if err := jr.performMemoryAccounting(); err != nil {
					jr.MoveToDraining(err)
					return jrStateUnknown, nil, meta
				}

				return jrReadingInput, nil, meta
			}
			if encDatumRow == nil {
				break
			}
			rowSize = int64(encDatumRow.Size())
			if jr.curBatchSizeBytes > 0 && jr.curBatchSizeBytes+rowSize > jr.batchSizeBytes {
				// Adding this row to the current batch will make the batch
				// exceed jr.batchSizeBytes. Additionally, the batch is not
				// empty, so we'll store this row as "pending" and will include
				// it into the next batch.
				//
				// The batch being non-empty is important because in case it was
				// empty and we decided to not include this (first) row into it,
				// then we'd be stalled - we'd generate no spans, so we'd not
				// perform the lookup of anything.
				jr.pendingRow = encDatumRow
				break
			}
		} else {
			encDatumRow = jr.pendingRow
			jr.pendingRow = nil
			rowSize = int64(encDatumRow.Size())
		}
		jr.curBatchSizeBytes += rowSize
		if jr.groupingState != nil {
			// Lookup Join.
			if err := jr.processContinuationValForRow(encDatumRow); err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, nil, jr.DrainHelper()
			}
		}
		// Keep the copy of the row after accounting for its memory usage.
		//
		// We need to subtract the EncDatumRowOverhead because that is already
		// tracked in jr.accountedFor.scratchInputRows.
		if err := jr.strategy.growMemoryAccount(&jr.memAcc, rowSize-int64(rowenc.EncDatumRowOverhead)); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, nil, jr.DrainHelper()
		}
		jr.scratchInputRows = append(jr.scratchInputRows, jr.rowAlloc.CopyRow(encDatumRow))

		if l := jr.limitHintHelper.LimitHint(); l != 0 && l == int64(len(jr.scratchInputRows)) {
			break
		}
	}

	if err := jr.performMemoryAccounting(); err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}

	var outRow rowenc.EncDatumRow
	// Finished reading the input batch.
	if jr.groupingState != nil {
		// Lookup join.
		outRow = jr.allContinuationValsProcessed()
	}

	if len(jr.scratchInputRows) == 0 {
		log.VEventf(jr.Ctx(), 1, "no more input rows")
		if outRow != nil {
			return jrReadyToDrain, outRow, nil
		}
		// We're done.
		jr.MoveToDraining(nil)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	log.VEventf(jr.Ctx(), 1, "read %d input rows", len(jr.scratchInputRows))

	if jr.groupingState != nil && len(jr.scratchInputRows) > 0 {
		jr.updateGroupingStateForNonEmptyBatch()
	}

	if err := jr.limitHintHelper.ReadSomeRows(int64(len(jr.scratchInputRows))); err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}

	// Figure out what key spans we need to lookup.
	spans, spanIDs, err := jr.strategy.processLookupRows(jr.scratchInputRows)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	jr.curBatchInputRowCount = int64(len(jr.scratchInputRows))
	jr.resetScratchWhenReadingInput = true
	jr.curBatchSizeBytes = 0
	jr.curBatchRowsRead = 0
	if len(spans) == 0 {
		// All of the input rows were filtered out. Skip the index lookup.
		return jrEmittingRows, outRow, nil
	}
	if jr.errorOnLookup {
		// If spans has a non-zero length, the call to StartScan below will
		// perform a batch lookup of kvs, so error out before that happens if
		// we were instructed to do so via the errorOnLookup flag.
		err = noHomeRegionError
		if jr.allowEnforceHomeRegionFollowerReads {
			err = execinfra.NewDynamicQueryHasNoHomeRegionError(err)
		}
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}

	// Sort the spans by key order, except for a special case: an index-join with
	// maintainOrdering. That case can be executed efficiently if we don't sort:
	// we know that, for an index-join, each input row corresponds to exactly one
	// lookup row, and vice-versa. So, `spans` has one span per input/lookup-row,
	// in the right order. joinReaderIndexJoinStrategy.processLookedUpRow()
	// immediately emits each looked up row (it never buffers or reorders rows)
	// so, if ordering matters, we cannot sort the spans here.
	//
	// In every other case than the one discussed above, we sort the spans because
	// a) if we sort, we can then configure the fetcher below with a limit (the
	//    fetcher only accepts a limit if the spans are sorted), and
	// b) Pebble has various optimizations for Seeks in sorted order.
	if jr.readerType == indexJoinReaderType && jr.maintainOrdering {
		// Assert that the index join doesn't have shouldLimitBatches set. Since we
		// didn't sort above, the fetcher doesn't support a limit.
		if jr.shouldLimitBatches {
			err := errors.AssertionFailedf("index join configured with both maintainOrdering and " +
				"shouldLimitBatched; this shouldn't have happened as the implementation doesn't support it")
			jr.MoveToDraining(err)
			return jrStateUnknown, nil, jr.DrainHelper()
		}
	} else {
		if !jr.usesStreamer || jr.streamerInfo.maintainOrdering {
			// We don't want to sort the spans here if we're using the Streamer,
			// and it will perform the sort on its own - currently, this is the
			// case with OutOfOrder mode.
			sortSpans(spans, spanIDs)
		}
	}

	log.VEventf(jr.Ctx(), 1, "scanning %d spans", len(spans))
	// Note that the fetcher takes ownership of the spans slice - it will modify
	// it and perform the memory accounting. We don't care about the
	// modification here, but we want to be conscious about the memory
	// accounting - we don't double count for any memory of spans because the
	// joinReaderStrategy doesn't account for any memory used by the spans.
	if err = jr.fetcher.StartScan(
		jr.Ctx(), spans, spanIDs, jr.getBatchBytesLimit(), rowinfra.NoRowLimit,
	); err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}

	return jrFetchingLookupRows, outRow, nil
}

var noHomeRegionError = pgerror.Newf(pgcode.QueryHasNoHomeRegion,
	"Query has no home region. Try using a lower LIMIT value or running the query from a different region. %s",
	sqlerrors.EnforceHomeRegionFurtherInfo)

// fetchLookupRow fetches the next lookup row from the fetcher's batchResponse
// buffer, which was filled via a call to StartScan in joinReader.readInput.
// It also starts the KV lookups for the remote branch of locality-optimized
// join, if those have not yet started.
func (jr *joinReader) fetchLookupRow() (joinReaderState, *execinfrapb.ProducerMetadata) {
	for {
		// Fetch the next row and tell the strategy to process it.
		lookedUpRow, spanID, err := jr.fetcher.NextRow(jr.Ctx())
		if err != nil {
			jr.MoveToDraining(scrub.UnwrapScrubError(err))
			return jrStateUnknown, jr.DrainHelper()
		}
		if lookedUpRow == nil {
			// Done with this input batch.
			break
		}
		jr.rowsRead++
		jr.curBatchRowsRead++

		if nextState, err := jr.strategy.processLookedUpRow(jr.Ctx(), lookedUpRow, spanID); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		} else if nextState != jrFetchingLookupRows {
			return nextState, nil
		}
	}

	// If this is a locality optimized lookup join and we haven't yet generated
	// remote spans, check whether all input rows in the batch had local matches.
	// If not all rows matched, generate remote spans and start a scan to search
	// the remote nodes for the current batch.
	if jr.remoteLookupExpr.Expr() != nil && !jr.strategy.generatedRemoteSpans() &&
		jr.curBatchRowsRead != jr.curBatchInputRowCount {
		spans, spanIDs, err := jr.strategy.generateRemoteSpans()
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}

		if len(spans) != 0 {
			if !jr.usesStreamer || jr.streamerInfo.maintainOrdering {
				// Sort the spans so that we can rely upon the fetcher to limit
				// the number of results per batch. It's safe to reorder the
				// spans here because we already restore the original order of
				// the output during the output collection phase.
				//
				// We don't want to sort the spans here if we're using the
				// Streamer, and it will perform the sort on its own -
				// currently, this is the case with OutOfOrder mode.
				sortSpans(spans, spanIDs)
			}

			log.VEventf(jr.Ctx(), 1, "scanning %d remote spans", len(spans))
			if err := jr.fetcher.StartScan(
				jr.Ctx(), spans, spanIDs, jr.getBatchBytesLimit(), rowinfra.NoRowLimit,
			); err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
			return jrFetchingLookupRows, nil
		}
	}

	log.VEvent(jr.Ctx(), 1, "done joining rows")
	jr.strategy.prepareToEmit(jr.Ctx())

	// Check if the strategy spilled to disk and reduce the batch size if it
	// did.
	// TODO(yuzefovich): we should probably also grow the batch size bytes limit
	// dynamically if we haven't spilled and are not close to spilling (say not
	// exceeding half of the memory limit of the disk-backed container), up to
	// some limit. (This would only apply to the joinReaderOrderingStrategy
	// since other strategies cannot spill in the first place.) Probably it'd be
	// good to look at not just the current batch of input rows, but to keep
	// some statistics over the last several batches to make a more informed
	// decision.
	if jr.strategy.spilled() && jr.batchSizeBytes > joinReaderMinBatchSize {
		jr.batchSizeBytes = jr.batchSizeBytes / 2
		if jr.batchSizeBytes < joinReaderMinBatchSize {
			jr.batchSizeBytes = joinReaderMinBatchSize
		}
	}

	return jrEmittingRows, nil
}

const joinReaderMinBatchSize = 10 << 10 /* 10 KiB */

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (
	joinReaderState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	rowToEmit, nextState, err := jr.strategy.nextRowToEmit(jr.Ctx())
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	return nextState, rowToEmit, nil
}

func (jr *joinReader) performMemoryAccounting() error {
	oldSz := jr.accountedFor.scratchInputRows + jr.accountedFor.groupingState
	jr.accountedFor.scratchInputRows = int64(cap(jr.scratchInputRows)) * int64(rowenc.EncDatumRowOverhead)
	jr.accountedFor.groupingState = jr.groupingState.memUsage()
	newSz := jr.accountedFor.scratchInputRows + jr.accountedFor.groupingState
	return jr.strategy.resizeMemoryAccount(&jr.memAcc, oldSz, newSz)
}

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) {
	ctx = jr.StartInternal(
		ctx, joinReaderProcName, &jr.contentionEventsListener,
		&jr.scanStatsListener, &jr.tenantConsumptionListener,
	)
	jr.input.Start(ctx)
	jr.runningState = jrReadingInput
}

// ConsumerClosed is part of the RowSource interface.
func (jr *joinReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	jr.close()
}

func (jr *joinReader) close() {
	if jr.InternalClose() {
		if jr.fetcher != nil {
			jr.fetcher.Close(jr.Ctx())
		}
		if jr.usesStreamer {
			jr.streamerInfo.budgetAcc.Close(jr.Ctx())
			jr.streamerInfo.txnKVStreamerMemAcc.Close(jr.Ctx())
			jr.streamerInfo.unlimitedMemMonitor.Stop(jr.Ctx())
			if jr.streamerInfo.diskMonitor != nil {
				jr.streamerInfo.diskMonitor.Stop(jr.Ctx())
			}
		}
		jr.strategy.close(jr.Ctx())
		jr.memAcc.Close(jr.Ctx())
		if jr.limitedMemMonitor != nil {
			jr.limitedMemMonitor.Stop(jr.Ctx())
		}
		if jr.MemMonitor != nil {
			jr.MemMonitor.Stop(jr.Ctx())
		}
		if jr.unlimitedMemMonitor != nil {
			jr.unlimitedMemMonitor.Stop(jr.Ctx())
		}
		if jr.diskMonitor != nil {
			jr.diskMonitor.Stop(jr.Ctx())
		}
	}
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (jr *joinReader) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(jr.input)
	if !ok {
		return nil
	}
	fis, ok := getFetcherInputStats(jr.fetcher)
	if !ok {
		return nil
	}

	ret := &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		KV: execinfrapb.KVStats{
			BytesRead:           optional.MakeUint(uint64(jr.fetcher.GetBytesRead())),
			KVPairsRead:         optional.MakeUint(uint64(jr.fetcher.GetKVPairsRead())),
			TuplesRead:          fis.NumTuples,
			KVTime:              fis.WaitTime,
			ContentionTime:      optional.MakeTimeValue(jr.contentionEventsListener.GetContentionTime()),
			BatchRequestsIssued: optional.MakeUint(uint64(jr.fetcher.GetBatchRequestsIssued())),
			KVCPUTime:           optional.MakeTimeValue(fis.kvCPUTime),
			UsedStreamer:        jr.usesStreamer,
		},
		Output: jr.OutputHelper.Stats(),
	}
	// Note that there is no need to include the maximum bytes of
	// jr.limitedMemMonitor because it is a child of jr.MemMonitor.
	ret.Exec.MaxAllocatedMem.Add(jr.MemMonitor.MaximumBytes())
	if jr.unlimitedMemMonitor != nil {
		ret.Exec.MaxAllocatedMem.Add(jr.unlimitedMemMonitor.MaximumBytes())
	}
	if jr.diskMonitor != nil {
		ret.Exec.MaxAllocatedDisk.Add(jr.diskMonitor.MaximumBytes())
	}
	if jr.usesStreamer {
		ret.Exec.MaxAllocatedMem.Add(jr.streamerInfo.unlimitedMemMonitor.MaximumBytes())
		if jr.streamerInfo.diskMonitor != nil {
			ret.Exec.MaxAllocatedDisk.Add(jr.streamerInfo.diskMonitor.MaximumBytes())
		}
	}
	ret.Exec.ConsumedRU = optional.MakeUint(jr.tenantConsumptionListener.GetConsumedRU())
	scanStats := jr.scanStatsListener.GetScanStats()
	execstats.PopulateKVMVCCStats(&ret.KV, &scanStats)
	return ret
}

func (jr *joinReader) generateMeta() []execinfrapb.ProducerMetadata {
	trailingMeta := make([]execinfrapb.ProducerMetadata, 1, 2)
	meta := &trailingMeta[0]
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.RowsRead = jr.rowsRead
	meta.Metrics.BytesRead = jr.fetcher.GetBytesRead()
	if tfs := execinfra.GetLeafTxnFinalState(jr.Ctx(), jr.txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// ChildCount is part of the execopnode.OpNode interface.
func (jr *joinReader) ChildCount(verbose bool) int {
	if _, ok := jr.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execopnode.OpNode interface.
func (jr *joinReader) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := jr.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to joinReader is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// processContinuationValForRow is called for each row in a batch which has a
// continuation column.
func (jr *joinReader) processContinuationValForRow(row rowenc.EncDatumRow) error {
	if !jr.groupingState.doGrouping {
		// Lookup join with no continuation column.
		jr.groupingState.addContinuationValForRow(false)
	} else {
		continuationEncDatum := row[len(row)-1]
		if err := continuationEncDatum.EnsureDecoded(types.Bool, &jr.alloc); err != nil {
			return err
		}
		continuationVal := bool(*continuationEncDatum.Datum.(*tree.DBool))
		jr.groupingState.addContinuationValForRow(continuationVal)
		if len(jr.scratchInputRows) == 0 && continuationVal {
			// First row in batch is a continuation of last group.
			jr.lastBatchState.lastGroupContinued = true
		}
	}
	return nil
}

// allContinuationValsProcessed is called after all the rows in the batch have
// been read, or the batch is empty, and processContinuationValForRow has been
// called. It returns a non-nil row if one needs to output a row from the
// batch previous to the current batch.
func (jr *joinReader) allContinuationValsProcessed() rowenc.EncDatumRow {
	var outRow rowenc.EncDatumRow
	jr.groupingState.initialized = true
	if jr.lastBatchState.lastInputRow != nil && !jr.lastBatchState.lastGroupContinued {
		// Group ended in previous batch and this is a lookup join with a
		// continuation column.
		if !jr.lastBatchState.lastGroupMatched {
			// Handle the cases where we need to emit the left row when there is no
			// match.
			switch jr.joinType {
			case descpb.LeftOuterJoin:
				outRow = jr.renderUnmatchedRow(jr.lastBatchState.lastInputRow, leftSide)
			case descpb.LeftAntiJoin:
				outRow = jr.lastBatchState.lastInputRow
			}
		}
		// Else the last group matched, so already emitted 1+ row for left outer
		// join, 1 row for semi join, and no need to emit for anti join.
	}
	// Else, last group continued, or this is the first ever batch, or all
	// groups are of length 1. Either way, we don't need to do anything
	// special for the last group from the last batch.

	jr.lastBatchState.lastInputRow = nil
	return outRow
}

// updateGroupingStateForNonEmptyBatch is called once the batch has been read
// and found to be non-empty.
func (jr *joinReader) updateGroupingStateForNonEmptyBatch() {
	if jr.groupingState.doGrouping {
		// Groups can continue from one batch to another.

		// Remember state from the last group in this batch.
		jr.lastBatchState.lastInputRow = jr.scratchInputRows[len(jr.scratchInputRows)-1]
		// Initialize matching state for the first group in this batch.
		if jr.lastBatchState.lastGroupMatched && jr.lastBatchState.lastGroupContinued {
			jr.groupingState.setFirstGroupMatched()
		}
	}
}

// inputBatchGroupingState encapsulates the state needed for all the
// groups in an input batch, for lookup joins (not used for index
// joins).
// It functions in one of two modes:
//   - doGrouping is false: It is expected that for each input row in
//     a batch, addContinuationValForRow(false) will be called.
//   - doGrouping is true: The join is functioning in a manner where
//     the continuation column in the input indicates the parameter
//     value of addContinuationValForRow calls.
//
// The initialization and resetting of state for a batch is
// handled by joinReader. Updates to this state based on row
// matching is done by the appropriate joinReaderStrategy
// implementation. The joinReaderStrategy implementations
// also lookup the state to decide when to output.
type inputBatchGroupingState struct {
	doGrouping bool

	initialized bool
	// Row index in batch to the group index. Only used when doGrouping = true.
	batchRowToGroupIndex []int
	// State per group.
	groupState []groupState
}

type groupState struct {
	// Whether the group matched.
	matched bool
	// The last row index in the group. Only valid when doGrouping = true.
	lastRow int
}

func (ib *inputBatchGroupingState) reset() {
	ib.batchRowToGroupIndex = ib.batchRowToGroupIndex[:0]
	ib.groupState = ib.groupState[:0]
	ib.initialized = false
}

// addContinuationValForRow is called with each row in an input batch, with
// the cont parameter indicating whether or not it is a continuation of the
// group from the previous row.
func (ib *inputBatchGroupingState) addContinuationValForRow(cont bool) {
	if len(ib.groupState) == 0 || !cont {
		// First row in input batch or the start of a new group. We need to
		// add entries in the group indexed slices.
		ib.groupState = append(ib.groupState,
			groupState{matched: false, lastRow: len(ib.batchRowToGroupIndex)})
	}
	if ib.doGrouping {
		groupIndex := len(ib.groupState) - 1
		ib.groupState[groupIndex].lastRow = len(ib.batchRowToGroupIndex)
		ib.batchRowToGroupIndex = append(ib.batchRowToGroupIndex, groupIndex)
	}
}

func (ib *inputBatchGroupingState) setFirstGroupMatched() {
	ib.groupState[0].matched = true
}

// setMatched records that the given rowIndex has matched. It returns the
// previous value of the matched field.
func (ib *inputBatchGroupingState) setMatched(rowIndex int) bool {
	groupIndex := rowIndex
	if ib.doGrouping {
		groupIndex = ib.batchRowToGroupIndex[rowIndex]
	}
	rv := ib.groupState[groupIndex].matched
	ib.groupState[groupIndex].matched = true
	return rv
}

func (ib *inputBatchGroupingState) getMatched(rowIndex int) bool {
	groupIndex := rowIndex
	if ib.doGrouping {
		groupIndex = ib.batchRowToGroupIndex[rowIndex]
	}
	return ib.groupState[groupIndex].matched
}

func (ib *inputBatchGroupingState) lastGroupMatched() bool {
	if !ib.doGrouping || len(ib.groupState) == 0 {
		return false
	}
	return ib.groupState[len(ib.groupState)-1].matched
}

func (ib *inputBatchGroupingState) isUnmatched(rowIndex int) bool {
	if !ib.doGrouping {
		// The rowIndex is also the groupIndex.
		return !ib.groupState[rowIndex].matched
	}
	groupIndex := ib.batchRowToGroupIndex[rowIndex]
	if groupIndex == len(ib.groupState)-1 {
		// Return false for last group since it is not necessarily complete yet --
		// the next batch may continue the group.
		return false
	}
	// Group is complete -- return true for the last row index in a group that
	// is unmatched. Note that there are join reader strategies that on a
	// row-by-row basis (a) evaluate the match condition, (b) decide whether to
	// output (including when there is no match). It is necessary to delay
	// saying that there is no match for the group until the last row in the
	// group since for earlier rows, when at step (b), one does not know the
	// match state of later rows in the group.
	return !ib.groupState[groupIndex].matched && ib.groupState[groupIndex].lastRow == rowIndex
}

func (ib *inputBatchGroupingState) memUsage() int64 {
	if ib == nil {
		return 0
	}
	return int64(cap(ib.groupState))*int64(unsafe.Sizeof(groupState{})) +
		int64(cap(ib.batchRowToGroupIndex))*memsize.Int
}
