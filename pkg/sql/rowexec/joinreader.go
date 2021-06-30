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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
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

	diskMonitor *mon.BytesMonitor

	desc      catalog.TableDescriptor
	index     catalog.Index
	colIdxMap catalog.TableColMap
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
	alloc              rowenc.DatumAlloc
	rowAlloc           rowenc.EncDatumRowAlloc
	shouldLimitBatches bool
	readerType         joinReaderType

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
}

var _ execinfra.Processor = &joinReader{}
var _ execinfra.RowSource = &joinReader{}
var _ execinfra.OpNode = &joinReader{}

const joinReaderProcName = "join reader"

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
	if spec.IndexIdx != 0 && readerType == indexJoinReaderType {
		return nil, errors.AssertionFailedf("index join must be against primary index")
	}
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
	tableDesc := spec.BuildTableDescriptor()
	switch readerType {
	case indexJoinReaderType:
		lookupCols = make([]uint32, tableDesc.GetPrimaryIndex().NumKeyColumns())
		for i := range lookupCols {
			lookupCols[i] = uint32(i)
		}
	case lookupJoinReaderType:
		lookupCols = spec.LookupColumns
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}
	jr := &joinReader{
		desc:                              tableDesc,
		maintainOrdering:                  spec.MaintainOrdering,
		input:                             input,
		lookupCols:                        lookupCols,
		outputGroupContinuationForLeftRow: spec.OutputGroupContinuationForLeftRow,
		// The joiner has a choice to make between getting DistSender-level
		// parallelism for its lookup batches and setting row and memory limits (due
		// to implementation limitations, you can't have both at the same time). We
		// choose parallelism when we know that each lookup returns at most one row:
		// in case of indexJoinReaderType, we know that there's exactly one lookup
		// row for each input row. Similarly, in case of spec.LookupColumnsAreKey,
		// we know that there's at most one lookup row per input row. In other
		// cases, we use limits.
		shouldLimitBatches: !spec.LookupColumnsAreKey && readerType == lookupJoinReaderType,
		readerType:         readerType,
	}
	if readerType != indexJoinReaderType {
		jr.groupingState = &inputBatchGroupingState{doGrouping: spec.LeftJoinWithPairedJoiner}
	}
	var err error
	var isSecondary bool
	indexIdx := int(spec.IndexIdx)
	if indexIdx >= len(jr.desc.ActiveIndexes()) {
		return nil, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	jr.index = jr.desc.ActiveIndexes()[indexIdx]
	isSecondary = !jr.index.Primary()
	cols := jr.desc.PublicColumns()
	if spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic {
		cols = jr.desc.DeletableColumns()
	}
	jr.colIdxMap = catalog.ColumnIDToOrdinalMap(cols)
	columnTypes := catalog.ColumnTypes(cols)

	columnIDs, _ := catalog.FullIndexColumnIDs(jr.index)
	indexCols := make([]uint32, len(columnIDs))
	for i, columnID := range columnIDs {
		indexCols[i] = uint32(columnID)
	}

	// Add all requested system columns to the output.
	if spec.HasSystemColumns {
		for _, sysCol := range jr.desc.SystemColumns() {
			columnTypes = append(columnTypes, sysCol.GetType())
			jr.colIdxMap.Set(sysCol.GetID(), jr.colIdxMap.Len())
		}
	}

	var leftTypes []*types.T
	var leftEqCols []uint32
	switch readerType {
	case indexJoinReaderType:
		// Index join performs a join between a secondary index, the `input`,
		// and the primary index of the same table, `desc`, to retrieve columns
		// which are not stored in the secondary index. It outputs the looked
		// up rows as is (meaning that the output rows before post-processing
		// will contain all columns from the table) whereas the columns that
		// came from the secondary index (input rows) are ignored. As a result,
		// we leave leftTypes as empty.
		leftEqCols = indexCols
	case lookupJoinReaderType:
		leftTypes = input.OutputTypes()
		leftEqCols = jr.lookupCols
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}

	if err := jr.joinerBase.init(
		jr,
		flowCtx,
		processorID,
		leftTypes,
		columnTypes,
		spec.Type,
		spec.OnExpr,
		leftEqCols,
		indexCols,
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

	rightCols := jr.neededRightCols()
	if isSecondary {
		set := getIndexColSet(jr.index, jr.colIdxMap)
		if !rightCols.SubsetOf(set) {
			return nil, errors.Errorf("joinreader index does not cover all columns")
		}
	}

	var fetcher row.Fetcher
	_, _, err = initRowFetcher(
		flowCtx, &fetcher, jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		rightCols, false /* isCheck */, jr.EvalCtx.Mon, &jr.alloc, spec.Visibility, spec.LockingStrength,
		spec.LockingWaitPolicy, spec.HasSystemColumns, nil, /* virtualColumn */
	)

	if err != nil {
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
		lookupExprTypes := make([]*types.T, 0, len(leftTypes)+len(columnTypes))
		lookupExprTypes = append(lookupExprTypes, leftTypes...)
		lookupExprTypes = append(lookupExprTypes, columnTypes...)

		semaCtx := flowCtx.TypeResolverFactory.NewSemaContext(flowCtx.EvalCtx.Txn)
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

	if err := jr.initJoinReaderStrategy(flowCtx, columnTypes, len(columnIDs), rightCols, readerType); err != nil {
		return nil, err
	}
	jr.batchSizeBytes = jr.strategy.getLookupRowsBatchSizeHint()

	// TODO(radu): verify the input types match the index key types
	return jr, nil
}

func (jr *joinReader) initJoinReaderStrategy(
	flowCtx *execinfra.FlowCtx,
	typs []*types.T,
	numKeyCols int,
	neededRightCols util.FastIntSet,
	readerType joinReaderType,
) error {
	spanBuilder := span.MakeBuilder(flowCtx.EvalCtx, flowCtx.Codec(), jr.desc, jr.index)
	spanBuilder.SetNeededColumns(neededRightCols)

	var generator joinReaderSpanGenerator
	var keyToInputRowIndices map[string][]int
	if readerType != indexJoinReaderType {
		keyToInputRowIndices = make(map[string][]int)
	}
	// Else: see the comment in defaultSpanGenerator on why we don't need
	// this map for index joins.

	if jr.lookupExpr.Expr == nil {
		generator = &defaultSpanGenerator{
			spanBuilder:          spanBuilder,
			keyToInputRowIndices: keyToInputRowIndices,
			numKeyCols:           numKeyCols,
			lookupCols:           jr.lookupCols,
		}
	} else {
		// Since jr.lookupExpr is set, we need to use either multiSpanGenerator or
		// localityOptimizedSpanGenerator, which support looking up multiple spans
		// per input row.
		tableOrdToIndexOrd := util.FastIntMap{}
		columnIDs, _ := catalog.FullIndexColumnIDs(jr.index)
		for i, colID := range columnIDs {
			tabOrd := jr.colIdxMap.GetDefault(colID)
			tableOrdToIndexOrd.Set(tabOrd, i)
		}

		// If jr.remoteLookupExpr is set, this is a locality optimized lookup join
		// and we need to use localityOptimizedSpanGenerator.
		if jr.remoteLookupExpr.Expr == nil {
			multiSpanGen := &multiSpanGenerator{}
			if err := multiSpanGen.init(
				spanBuilder,
				numKeyCols,
				len(jr.input.OutputTypes()),
				keyToInputRowIndices,
				&jr.lookupExpr,
				tableOrdToIndexOrd,
			); err != nil {
				return err
			}
			generator = multiSpanGen
		} else {
			localityOptSpanGen := &localityOptimizedSpanGenerator{}
			if err := localityOptSpanGen.init(
				spanBuilder,
				numKeyCols,
				len(jr.input.OutputTypes()),
				keyToInputRowIndices,
				&jr.lookupExpr,
				&jr.remoteLookupExpr,
				tableOrdToIndexOrd,
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
		}
		return nil
	}

	if !jr.maintainOrdering {
		jr.strategy = &joinReaderNoOrderingStrategy{
			joinerBase:              &jr.joinerBase,
			joinReaderSpanGenerator: generator,
			isPartialJoin:           jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
			groupingState:           jr.groupingState,
		}
		return nil
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// joinReader will overflow to disk if this limit is not enough.
	limit := execinfra.GetWorkMemLimit(flowCtx)
	// Initialize memory monitors and row container for looked up rows.
	jr.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx, "joinreader-limited")
	jr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, "joinreader-disk")
	drc := rowcontainer.NewDiskBackedNumberedRowContainer(
		false, /* deDup */
		typs,
		jr.EvalCtx,
		jr.FlowCtx.Cfg.TempStorage,
		jr.MemMonitor,
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
	}
	return nil
}

// getIndexColSet returns a set of all column indices for the given index.
func getIndexColSet(index catalog.Index, colIdxMap catalog.TableColMap) util.FastIntSet {
	cols := util.MakeFastIntSet()
	{
		colIDs := index.CollectKeyColumnIDs()
		colIDs.UnionWith(index.CollectSecondaryStoredColumnIDs())
		colIDs.UnionWith(index.CollectKeySuffixColumnIDs())
		colIDs.ForEach(func(colID descpb.ColumnID) {
			cols.Add(colIdxMap.GetDefault(colID))
		})
	}
	return cols
}

// SetBatchSizeBytes sets the desired batch size. It should only be used in tests.
func (jr *joinReader) SetBatchSizeBytes(batchSize int64) {
	jr.batchSizeBytes = batchSize
}

// Spilled returns whether the joinReader spilled to disk.
func (jr *joinReader) Spilled() bool {
	return jr.strategy.spilled()
}

// neededRightCols returns the set of column indices which need to be fetched
// from the right side of the join (jr.desc).
func (jr *joinReader) neededRightCols() util.FastIntSet {
	neededCols := jr.OutputHelper.NeededColumns()

	if jr.readerType == indexJoinReaderType {
		// For index joins, all columns from the left side are not output, so no
		// shift is needed. Also, onCond is always empty for index joins, so
		// there is no need to iterate over it either.
		return neededCols
	}

	// Get the columns from the right side of the join and shift them over by
	// the size of the left side so the right side starts at 0.
	numInputTypes := len(jr.input.OutputTypes())
	neededRightCols := util.MakeFastIntSet()
	var lastCol int
	for i, ok := neededCols.Next(numInputTypes); ok; i, ok = neededCols.Next(i + 1) {
		lastCol = i - numInputTypes
		neededRightCols.Add(lastCol)
	}
	if jr.outputGroupContinuationForLeftRow {
		// The lastCol is the bool continuation column and not a right
		// column.
		neededRightCols.Remove(lastCol)
	}

	// Add columns needed by OnExpr.
	for _, v := range jr.onCond.Vars.GetIndexedVars() {
		rightIdx := v.Idx - numInputTypes
		if rightIdx >= 0 {
			neededRightCols.Add(rightIdx)
		}
	}

	return neededRightCols
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
	// Read the next batch of input rows.
	for jr.curBatchSizeBytes < jr.batchSizeBytes {
		row, meta := jr.input.Next()
		if meta != nil {
			if meta.Err != nil {
				jr.MoveToDraining(nil /* err */)
				return jrStateUnknown, nil, meta
			}
			return jrReadingInput, nil, meta
		}
		if row == nil {
			break
		}
		jr.curBatchSizeBytes += int64(row.Size())
		if jr.groupingState != nil {
			// Lookup Join.
			if err := jr.processContinuationValForRow(row); err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, nil, jr.DrainHelper()
			}
		}
		jr.scratchInputRows = append(jr.scratchInputRows, jr.rowAlloc.CopyRow(row))
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
	jr.scratchInputRows = jr.scratchInputRows[:0]
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
	if err := jr.fetcher.StartScan(
		jr.Ctx, jr.FlowCtx.Txn, spans, jr.shouldLimitBatches, 0, /* limitHint */
		jr.FlowCtx.TraceKV, jr.EvalCtx.TestingKnobs.ForceProductionBatchSizes,
	); err != nil {
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
		lookedUpRow, _, _, err := jr.fetcher.NextRow(jr.Ctx)
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
			if err := jr.fetcher.StartScan(
				jr.Ctx, jr.FlowCtx.Txn, spans, jr.shouldLimitBatches, 0, /* limitHint */
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

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) {
	ctx = jr.StartInternal(ctx, joinReaderProcName)
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
			jr.fetcher.Close(jr.Ctx)
		}
		jr.strategy.close(jr.Ctx)
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
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		KV: execinfrapb.KVStats{
			BytesRead:      optional.MakeUint(uint64(jr.fetcher.GetBytesRead())),
			TuplesRead:     fis.NumTuples,
			KVTime:         fis.WaitTime,
			ContentionTime: optional.MakeTimeValue(execinfra.GetCumulativeContentionTime(jr.Ctx)),
		},
		Output: jr.OutputHelper.Stats(),
	}
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
