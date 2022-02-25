// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"math"
	"sort"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	// jrReadingInput means that a batch of rows is being read from the input.
	jrReadingInput
	// jrPerformingLookup means we are performing an index lookup for the current
	// input row batch.
	jrPerformingLookup
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
	limitedMemMonitor *mon.BytesMonitor
	diskMonitor       *mon.BytesMonitor

	fetchSpec      descpb.IndexFetchSpec
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

	keyLocking     descpb.ScanLockingStrength
	lockWaitPolicy lock.WaitPolicy

	// usesStreamer indicates whether the joinReader performs the lookups using
	// the kvcoord.Streamer API.
	usesStreamer bool
	streamerInfo struct {
		*kvstreamer.Streamer
		unlimitedMemMonitor *mon.BytesMonitor
		budgetAcc           mon.BoundAccount
		budgetLimit         int64
		maxKeysPerRow       int
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

	// scanStats is collected from the trace after we finish doing work for this
	// join.
	scanStats execinfra.ScanStats
}

var _ execinfra.Processor = &joinReader{}
var _ execinfra.RowSource = &joinReader{}
var _ execinfra.OpNode = &joinReader{}

const joinReaderProcName = "join reader"

// ParallelizeMultiKeyLookupJoinsEnabled determines whether the joinReader
// parallelizes KV batches in all cases.
var ParallelizeMultiKeyLookupJoinsEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.distsql.parallelize_multi_key_lookup_joins.enabled",
	"determines whether KV batches are executed in parallel for lookup joins in all cases. "+
		"Enabling this will increase the speed of lookup joins when each input row might get "+
		"multiple looked up rows at the cost of increased memory usage.",
	false,
)

// newJoinReader returns a new joinReader.
func newJoinReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.JoinReaderSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
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
		return nil, errors.Errorf("unsupported joinReaderType")
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
	tryStreamer := flowCtx.Txn != nil && flowCtx.Txn.Type() == kv.LeafTxn &&
		row.CanUseStreamer(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Settings) &&
		!spec.MaintainOrdering

	jr := &joinReader{
		fetchSpec:                         spec.FetchSpec,
		splitFamilyIDs:                    spec.SplitFamilyIDs,
		maintainOrdering:                  spec.MaintainOrdering,
		input:                             input,
		lookupCols:                        lookupCols,
		outputGroupContinuationForLeftRow: spec.OutputGroupContinuationForLeftRow,
		shouldLimitBatches:                shouldLimitBatches,
		readerType:                        readerType,
		keyLocking:                        spec.LockingStrength,
		lockWaitPolicy:                    row.GetWaitPolicy(spec.LockingWaitPolicy),
		usesStreamer:                      (readerType == indexJoinReaderType) && tryStreamer,
		lookupBatchBytesLimit:             rowinfra.BytesLimit(spec.LookupBatchBytesLimit),
	}
	if readerType != indexJoinReaderType {
		jr.groupingState = &inputBatchGroupingState{doGrouping: spec.LeftJoinWithPairedJoiner}
	}

	// Make sure the key column types are hydrated. The fetched column types will
	// be hydrated in ProcessorBase.Init (via joinerBase.init).
	resolver := flowCtx.NewTypeResolver(flowCtx.Txn)
	for i := range spec.FetchSpec.KeyAndSuffixColumns {
		if err := typedesc.EnsureTypeIsHydrated(
			flowCtx.EvalCtx.Ctx(), spec.FetchSpec.KeyAndSuffixColumns[i].Type, &resolver,
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
		return nil, errors.Errorf("unsupported joinReaderType")
	}
	rightTypes := make([]*types.T, len(spec.FetchSpec.FetchedColumns))
	for i := range rightTypes {
		rightTypes[i] = spec.FetchSpec.FetchedColumns[i].Type
	}

	if err := jr.joinerBase.init(
		jr,
		flowCtx,
		processorID,
		leftTypes,
		rightTypes,
		spec.Type,
		spec.OnExpr,
		spec.OutputGroupContinuationForLeftRow,
		post,
		output,
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
	); err != nil {
		return nil, err
	}

	var fetcher row.Fetcher
	if err := fetcher.Init(
		flowCtx.EvalCtx.Context,
		false, /* reverse */
		spec.LockingStrength,
		spec.LockingWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		&jr.alloc,
		flowCtx.EvalCtx.Mon,
		&spec.FetchSpec,
	); err != nil {
		return nil, err
	}

	if execinfra.ShouldCollectStats(flowCtx.EvalCtx.Ctx(), flowCtx) {
		jr.input = newInputStatCollector(jr.input)
		jr.fetcher = newRowFetcherStatCollector(&fetcher)
		jr.ExecStatsForTrace = jr.execStatsForTrace
	} else {
		jr.fetcher = &fetcher
	}

	if !spec.LookupExpr.Empty() {
		lookupExprTypes := make([]*types.T, 0, len(leftTypes)+len(rightTypes))
		lookupExprTypes = append(lookupExprTypes, leftTypes...)
		lookupExprTypes = append(lookupExprTypes, rightTypes...)

		semaCtx := flowCtx.NewSemaContext(flowCtx.EvalCtx.Txn)
		if err := jr.lookupExpr.Init(spec.LookupExpr, lookupExprTypes, semaCtx, jr.EvalCtx); err != nil {
			return nil, err
		}
		if !spec.RemoteLookupExpr.Empty() {
			if err := jr.remoteLookupExpr.Init(
				spec.RemoteLookupExpr, lookupExprTypes, semaCtx, jr.EvalCtx,
			); err != nil {
				return nil, err
			}
		}
	}

	// We will create a memory monitor with at least 100KiB of memory limit
	// since the join reader doesn't know how to spill its in-memory state to
	// disk (separate from the buffered rows). It is most likely that if the
	// target limit is below 100KiB, then we're in a test scenario and we don't
	// want to error out.
	const minMemoryLimit = 100 << 10
	memoryLimit := execinfra.GetWorkMemLimit(flowCtx)
	if memoryLimit < minMemoryLimit {
		memoryLimit = minMemoryLimit
	}

	// Initialize memory monitors and bound account for data structures in the joinReader.
	jr.MemMonitor = mon.NewMonitorInheritWithLimit(
		"joinreader-mem" /* name */, memoryLimit, flowCtx.EvalCtx.Mon,
	)
	jr.MemMonitor.Start(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Mon, mon.BoundAccount{})
	jr.memAcc = jr.MemMonitor.MakeBoundAccount()

	if err := jr.initJoinReaderStrategy(flowCtx, rightTypes, readerType); err != nil {
		return nil, err
	}
	jr.batchSizeBytes = jr.strategy.getLookupRowsBatchSizeHint(flowCtx.EvalCtx.SessionData())

	if jr.usesStreamer {
		// When using the Streamer API, we want to limit the batch size hint to
		// at most a quarter of the workmem limit. Note that it is ok if it is
		// set to zero since the joinReader will always include at least one row
		// into the lookup batch.
		if jr.batchSizeBytes > memoryLimit/4 {
			jr.batchSizeBytes = memoryLimit / 4
		}
		// jr.batchSizeBytes will be used up by the input batch, and we'll give
		// everything else to the streamer budget.
		jr.streamerInfo.budgetLimit = memoryLimit - jr.batchSizeBytes
		// We need to use an unlimited monitor for the streamer's budget since
		// the streamer itself is responsible for staying under the limit.
		jr.streamerInfo.unlimitedMemMonitor = mon.NewMonitorInheritWithLimit(
			"joinreader-streamer-unlimited" /* name */, math.MaxInt64, flowCtx.EvalCtx.Mon,
		)
		jr.streamerInfo.unlimitedMemMonitor.Start(flowCtx.EvalCtx.Ctx(), flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		jr.streamerInfo.budgetAcc = jr.streamerInfo.unlimitedMemMonitor.MakeBoundAccount()
		jr.streamerInfo.maxKeysPerRow = int(jr.fetchSpec.MaxKeysPerRow)
	} else {
		// When not using the Streamer API, we want to limit the batch size hint
		// to at most half of the workmem limit. Note that it is ok if it is set
		// to zero since the joinReader will always include at least one row
		// into the lookup batch.
		if jr.batchSizeBytes > memoryLimit/2 {
			jr.batchSizeBytes = memoryLimit / 2
		}
	}

	// TODO(radu): verify the input types match the index key types
	return jr, nil
}

func (jr *joinReader) initJoinReaderStrategy(
	flowCtx *execinfra.FlowCtx, typs []*types.T, readerType joinReaderType,
) error {
	strategyMemAcc := jr.MemMonitor.MakeBoundAccount()
	spanGeneratorMemAcc := jr.MemMonitor.MakeBoundAccount()
	var generator joinReaderSpanGenerator
	if jr.lookupExpr.Expr == nil {
		var keyToInputRowIndices map[string][]int
		// See the comment in defaultSpanGenerator on why we don't need
		// this map for index joins.
		if readerType != indexJoinReaderType {
			keyToInputRowIndices = make(map[string][]int)
		}
		defGen := &defaultSpanGenerator{}
		if err := defGen.init(
			flowCtx.EvalCtx,
			flowCtx.Codec(),
			&jr.fetchSpec,
			jr.splitFamilyIDs,
			keyToInputRowIndices,
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
		if jr.remoteLookupExpr.Expr == nil {
			multiSpanGen := &multiSpanGenerator{}
			if err := multiSpanGen.init(
				flowCtx.EvalCtx,
				flowCtx.Codec(),
				&jr.fetchSpec,
				jr.splitFamilyIDs,
				len(jr.input.OutputTypes()),
				&jr.lookupExpr,
				fetchedOrdToIndexKeyOrd,
				&spanGeneratorMemAcc,
			); err != nil {
				return err
			}
			generator = multiSpanGen
		} else {
			localityOptSpanGen := &localityOptimizedSpanGenerator{}
			remoteSpanGenMemAcc := jr.MemMonitor.MakeBoundAccount()
			if err := localityOptSpanGen.init(
				flowCtx.EvalCtx,
				flowCtx.Codec(),
				&jr.fetchSpec,
				jr.splitFamilyIDs,
				len(jr.input.OutputTypes()),
				&jr.lookupExpr,
				&jr.remoteLookupExpr,
				fetchedOrdToIndexKeyOrd,
				&spanGeneratorMemAcc,
				&remoteSpanGenMemAcc,
			); err != nil {
				return err
			}
			generator = localityOptSpanGen
		}
	}

	if readerType == indexJoinReaderType {
		jr.strategy = &joinReaderIndexJoinStrategy{
			joinerBase:              &jr.joinerBase,
			joinReaderSpanGenerator: generator,
			memAcc:                  &strategyMemAcc,
		}
		return nil
	}

	if !jr.maintainOrdering {
		jr.strategy = &joinReaderNoOrderingStrategy{
			joinerBase:              &jr.joinerBase,
			joinReaderSpanGenerator: generator,
			isPartialJoin:           jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
			groupingState:           jr.groupingState,
			memAcc:                  &strategyMemAcc,
		}
		return nil
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// joinReader will overflow to disk if this limit is not enough.
	limit := execinfra.GetWorkMemLimit(flowCtx)
	// Initialize memory monitors and row container for looked up rows.
	jr.limitedMemMonitor = execinfra.NewLimitedMonitor(ctx, jr.MemMonitor, flowCtx, "joinreader-limited")
	jr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "joinreader-disk")
	drc := rowcontainer.NewDiskBackedNumberedRowContainer(
		false, /* deDup */
		typs,
		jr.EvalCtx,
		jr.FlowCtx.Cfg.TempStorage,
		jr.limitedMemMonitor,
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
		memAcc:                            &strategyMemAcc,
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
		case jrPerformingLookup:
			jr.runningState, meta = jr.performLookup()
		case jrEmittingRows:
			jr.runningState, row, meta = jr.emitRow()
		case jrReadyToDrain:
			jr.MoveToDraining(nil)
			meta = jr.DrainHelper()
			jr.runningState = jrStateUnknown
		default:
			log.Fatalf(jr.Ctx, "unsupported state: %d", jr.runningState)
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

// readInput reads the next batch of input rows and starts an index scan.
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
		if err := jr.memAcc.ResizeTo(jr.Ctx, newSz); err != nil {
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
		if err := jr.memAcc.Grow(jr.Ctx, rowSize-int64(rowenc.EncDatumRowOverhead)); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, nil, jr.DrainHelper()
		}
		jr.scratchInputRows = append(jr.scratchInputRows, jr.rowAlloc.CopyRow(encDatumRow))
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
		log.VEventf(jr.Ctx, 1, "no more input rows")
		if outRow != nil {
			return jrReadyToDrain, outRow, nil
		}
		// We're done.
		jr.MoveToDraining(nil)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	log.VEventf(jr.Ctx, 1, "read %d input rows", len(jr.scratchInputRows))

	if jr.groupingState != nil && len(jr.scratchInputRows) > 0 {
		jr.updateGroupingStateForNonEmptyBatch()
	}

	// Figure out what key spans we need to lookup.
	spans, err := jr.strategy.processLookupRows(jr.scratchInputRows)
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
		sort.Sort(spans)
	}

	log.VEventf(jr.Ctx, 1, "scanning %d spans", len(spans))
	// Note that the fetcher takes ownership of the spans slice - it will modify
	// it and perform the memory accounting. We don't care about the
	// modification here, but we want to be conscious about the memory
	// accounting - we don't double count for any memory of spans because the
	// joinReaderStrategy doesn't account for any memory used by the spans.
	if jr.usesStreamer {
		var kvBatchFetcher *row.TxnKVStreamer
		kvBatchFetcher, err = row.NewTxnKVStreamer(jr.Ctx, jr.streamerInfo.Streamer, spans, jr.keyLocking)
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, nil, jr.DrainHelper()
		}
		err = jr.fetcher.StartScanFrom(jr.Ctx, kvBatchFetcher, jr.FlowCtx.TraceKV)
	} else {
		var bytesLimit rowinfra.BytesLimit
		if !jr.shouldLimitBatches {
			bytesLimit = rowinfra.NoBytesLimit
		} else {
			bytesLimit = jr.lookupBatchBytesLimit
			if jr.lookupBatchBytesLimit == 0 {
				bytesLimit = rowinfra.DefaultBatchBytesLimit
			}
		}
		err = jr.fetcher.StartScan(
			jr.Ctx, jr.FlowCtx.Txn, spans, bytesLimit, rowinfra.NoRowLimit,
			jr.FlowCtx.TraceKV, jr.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
		)
	}
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}

	return jrPerformingLookup, outRow, nil
}

// performLookup reads the next batch of index rows.
func (jr *joinReader) performLookup() (joinReaderState, *execinfrapb.ProducerMetadata) {
	for {
		// Construct a "partial key" of nCols, so we can match the key format that
		// was stored in our keyToInputRowIndices map. This matches the format that
		// is output in jr.generateSpan.
		var key roachpb.Key
		// Index joins do not look at this key parameter so don't bother populating
		// it, since it is not cheap for long keys.
		if jr.readerType != indexJoinReaderType {
			var err error
			key, err = jr.fetcher.PartialKey(jr.strategy.getMaxLookupKeyCols())
			if err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
		}

		// Fetch the next row and tell the strategy to process it.
		lookedUpRow, err := jr.fetcher.NextRow(jr.Ctx)
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

		if nextState, err := jr.strategy.processLookedUpRow(jr.Ctx, lookedUpRow, key); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		} else if nextState != jrPerformingLookup {
			return nextState, nil
		}
	}

	// If this is a locality optimized lookup join and we haven't yet generated
	// remote spans, check whether all input rows in the batch had local matches.
	// If not all rows matched, generate remote spans and start a scan to search
	// the remote nodes for the current batch.
	if jr.remoteLookupExpr.Expr != nil && !jr.strategy.generatedRemoteSpans() &&
		jr.curBatchRowsRead != jr.curBatchInputRowCount {
		spans, err := jr.strategy.generateRemoteSpans()
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}

		if len(spans) != 0 {
			// Sort the spans so that we can rely upon the fetcher to limit the number
			// of results per batch. It's safe to reorder the spans here because we
			// already restore the original order of the output during the output
			// collection phase.
			sort.Sort(spans)

			log.VEventf(jr.Ctx, 1, "scanning %d remote spans", len(spans))
			bytesLimit := rowinfra.DefaultBatchBytesLimit
			if !jr.shouldLimitBatches {
				bytesLimit = rowinfra.NoBytesLimit
			}
			if err := jr.fetcher.StartScan(
				jr.Ctx, jr.FlowCtx.Txn, spans, bytesLimit, rowinfra.NoRowLimit,
				jr.FlowCtx.TraceKV, jr.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
			); err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
			return jrPerformingLookup, nil
		}
	}

	log.VEvent(jr.Ctx, 1, "done joining rows")
	jr.strategy.prepareToEmit(jr.Ctx)

	return jrEmittingRows, nil
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (
	joinReaderState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	rowToEmit, nextState, err := jr.strategy.nextRowToEmit(jr.Ctx)
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
	return jr.memAcc.Resize(jr.Ctx, oldSz, newSz)
}

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) {
	ctx = jr.StartInternal(ctx, joinReaderProcName)
	jr.input.Start(ctx)
	if jr.usesStreamer {
		jr.streamerInfo.Streamer = kvstreamer.NewStreamer(
			jr.FlowCtx.Cfg.DistSender,
			jr.FlowCtx.Stopper(),
			jr.FlowCtx.Txn,
			jr.FlowCtx.EvalCtx.Settings,
			jr.lockWaitPolicy,
			jr.streamerInfo.budgetLimit,
			&jr.streamerInfo.budgetAcc,
		)
		jr.streamerInfo.Streamer.Init(
			kvstreamer.OutOfOrder,
			kvstreamer.Hints{UniqueRequests: true},
			jr.streamerInfo.maxKeysPerRow,
		)
	}
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
			jr.fetcher.Close(jr.Ctx)
		}
		if jr.usesStreamer {
			// We have to cleanup the streamer after closing the fetcher because
			// the latter might release some memory tracked by the budget of the
			// streamer.
			if jr.streamerInfo.Streamer != nil {
				jr.streamerInfo.Streamer.Close()
			}
			jr.streamerInfo.budgetAcc.Close(jr.Ctx)
			jr.streamerInfo.unlimitedMemMonitor.Stop(jr.Ctx)
		}
		jr.strategy.close(jr.Ctx)
		jr.memAcc.Close(jr.Ctx)
		if jr.limitedMemMonitor != nil {
			jr.limitedMemMonitor.Stop(jr.Ctx)
		}
		if jr.MemMonitor != nil {
			jr.MemMonitor.Stop(jr.Ctx)
		}
		if jr.diskMonitor != nil {
			jr.diskMonitor.Stop(jr.Ctx)
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

	// TODO(asubiotto): Add memory and disk usage to EXPLAIN ANALYZE.
	jr.scanStats = execinfra.GetScanStats(jr.Ctx)
	ret := &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		KV: execinfrapb.KVStats{
			BytesRead:      optional.MakeUint(uint64(jr.fetcher.GetBytesRead())),
			TuplesRead:     fis.NumTuples,
			KVTime:         fis.WaitTime,
			ContentionTime: optional.MakeTimeValue(execinfra.GetCumulativeContentionTime(jr.Ctx)),
		},
		Output: jr.OutputHelper.Stats(),
	}
	execinfra.PopulateKVMVCCStats(&ret.KV, &jr.scanStats)
	return ret
}

func (jr *joinReader) generateMeta() []execinfrapb.ProducerMetadata {
	trailingMeta := make([]execinfrapb.ProducerMetadata, 1, 2)
	meta := &trailingMeta[0]
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.RowsRead = jr.rowsRead
	meta.Metrics.BytesRead = jr.fetcher.GetBytesRead()
	if tfs := execinfra.GetLeafTxnFinalState(jr.Ctx, jr.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// ChildCount is part of the execinfra.OpNode interface.
func (jr *joinReader) ChildCount(verbose bool) int {
	if _, ok := jr.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (jr *joinReader) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := jr.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to joinReader is not an execinfra.OpNode")
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
// - doGrouping is false: It is expected that for each input row in
//   a batch, addContinuationValForRow(false) will be called.
// - doGrouping is true: The join is functioning in a manner where
//   the continuation column in the input indicates the parameter
//   value of addContinuationValForRow calls.
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
