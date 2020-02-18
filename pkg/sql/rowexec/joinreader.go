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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// TODO(radu): we currently create one batch at a time and run the KV operations
// on this node. In the future we may want to build separate batches for the
// nodes that "own" the respective ranges, and send out flows on those nodes.
const joinReaderBatchSize = 100

// partialJoinSentinel is used as the inputRowIdxToLookedUpRowIdx value for
// semi- and anti-joins, where we only need to know about the existence of a
// match.
var partialJoinSentinel = []int{-1}

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
)

// joinReader performs a lookup join between `input` and the specified `index`.
// `lookupCols` specifies the input columns which will be used for the index
// lookup.
type joinReader struct {
	joinerBase

	// runningState represents the state of the joinReader. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState joinReaderState

	diskMonitor *mon.BytesMonitor

	desc      sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	colIdxMap map[sqlbase.ColumnID]int

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// joinReader to wrap the fetcher with a stat collector when necessary.
	fetcher            rowFetcher
	alloc              sqlbase.DatumAlloc
	rowAlloc           sqlbase.EncDatumRowAlloc
	shouldLimitBatches bool

	input      execinfra.RowSource
	inputTypes []types.T
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols []uint32

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// State variables for each batch of input rows.
	inputRows            sqlbase.EncDatumRows
	lookedUpRows         rowcontainer.IndexedRowContainer
	keyToInputRowIndices map[string][]int
	// inputRowIdxToLookedUpRowIdx is a multimap from input row indices to
	// corresponding looked up row indices. It's populated in the
	// jrPerformingLookup state. For non partial joins (everything but semi/anti
	// join), the looked up rows are the rows that came back from the lookup
	// span for each input row, without checking for matches with respect to the
	// on-condition. For semi/anti join, we store at most one sentinel value,
	// indicating a matching lookup if it's present, since the right side of a
	// semi/anti join is not used.
	inputRowIdxToLookedUpRowIdx [][]int
	// emitCursor contains information about where the next row to emit is within
	// inputRowIdxToLookedUpRowIdx.
	emitCursor struct {
		// inputRowIdx contains the index into inputRowIdxToLookedUpRowIdx that
		// we're about to emit.
		inputRowIdx int
		// outputRowIdx contains the index into the inputRowIdx'th row of
		// inputRowIdxToLookedUpRowIdx that we're about to emit.
		outputRowIdx int
		// seenMatch is true if there was a match at the current inputRowIdx. A
		// match means that there's no need to output an outer or anti join row.
		seenMatch bool
	}

	spanBuilder *span.Builder
	numKeyCols  int

	indexKeyRow sqlbase.EncDatumRow
}

var _ execinfra.Processor = &joinReader{}
var _ execinfra.RowSource = &joinReader{}
var _ execinfrapb.MetadataSource = &joinReader{}
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
) (execinfra.RowSourcedProcessor, error) {
	jr := &joinReader{
		desc:                 spec.Table,
		input:                input,
		inputTypes:           input.OutputTypes(),
		lookupCols:           spec.LookupColumns,
		batchSize:            joinReaderBatchSize,
		keyToInputRowIndices: make(map[string][]int),
	}

	var err error
	var isSecondary bool
	jr.index, isSecondary, err = jr.desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	returnMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	jr.colIdxMap = jr.desc.ColumnIdxMapWithMutations(returnMutations)

	columnIDs, _ := jr.index.FullColumnIDs()
	indexCols := make([]uint32, len(columnIDs))
	jr.numKeyCols = len(columnIDs)
	columnTypes := jr.desc.ColumnTypesWithMutations(returnMutations)
	for i, columnID := range columnIDs {
		indexCols[i] = uint32(columnID)
	}

	// If the lookup columns form a key, there is only one result per lookup, so the fetcher
	// should parallelize the key lookups it performs.
	jr.shouldLimitBatches = !spec.LookupColumnsAreKey

	if err := jr.joinerBase.init(
		jr,
		flowCtx,
		processorID,
		input.OutputTypes(),
		columnTypes,
		spec.Type,
		spec.OnExpr,
		jr.lookupCols,
		indexCols,
		0, /* numMergedColumns */
		post,
		output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{jr.input},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				jr.close()
				return jr.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}

	collectingStats := false
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		collectingStats = true
	}

	neededRightCols := jr.neededRightCols()
	if isSecondary && !neededRightCols.SubsetOf(getIndexColSet(jr.index, jr.colIdxMap)) {
		return nil, errors.Errorf("joinreader index does not cover all columns")
	}

	var fetcher row.Fetcher
	_, _, err = initRowFetcher(
		&fetcher, &jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		neededRightCols, false /* isCheck */, &jr.alloc, spec.Visibility, spec.LockingStrength,
	)
	if err != nil {
		return nil, err
	}
	if collectingStats {
		jr.input = newInputStatCollector(jr.input)
		jr.fetcher = newRowFetcherStatCollector(&fetcher)
		jr.FinishTrace = jr.outputStatsToTrace
	} else {
		jr.fetcher = &fetcher
	}

	jr.spanBuilder = span.MakeBuilder(&jr.desc, jr.index)
	jr.spanBuilder.SetNeededColumns(jr.neededRightCols())

	// Initialize memory monitors and row container for looked up rows.
	st := flowCtx.Cfg.Settings
	ctx := flowCtx.EvalCtx.Ctx()
	if execinfra.SettingUseTempStorageJoins.Get(&st.SV) {
		// Limit the memory use by creating a child monitor with a hard limit.
		// joinReader will overflow to disk if this limit is not enough.
		limit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
		if flowCtx.Cfg.TestingKnobs.ForceDiskSpill {
			limit = 1
		}
		jr.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "joiner-limited")
		jr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "joinreader-disk")
		drc := rowcontainer.NewDiskBackedIndexedRowContainer(
			nil, /* ordering */
			jr.desc.ColumnTypesWithMutations(returnMutations),
			jr.EvalCtx,
			jr.FlowCtx.Cfg.TempStorage,
			jr.MemMonitor,
			jr.diskMonitor,
			0, /* rowCapacity */
		)
		if limit < mon.DefaultPoolAllocationSize {
			// The memory limit is too low for caching, most likely to force disk
			// spilling for testing.
			drc.DisableCache = true
		}
		jr.lookedUpRows = drc
	} else {
		jr.MemMonitor = execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "joinreader-mem")
		rc := rowcontainer.MemRowContainer{}
		rc.InitWithMon(
			nil, /* ordering */
			jr.desc.ColumnTypesWithMutations(returnMutations),
			jr.EvalCtx,
			jr.MemMonitor,
			0, /* rowCapacity */
		)
		jr.lookedUpRows = &rc
	}

	// TODO(radu): verify the input types match the index key types
	return jr, nil
}

// getIndexColSet returns a set of all column indices for the given index.
func getIndexColSet(
	index *sqlbase.IndexDescriptor, colIdxMap map[sqlbase.ColumnID]int,
) util.FastIntSet {
	cols := util.MakeFastIntSet()
	err := index.RunOverAllColumns(func(id sqlbase.ColumnID) error {
		cols.Add(colIdxMap[id])
		return nil
	})
	if err != nil {
		// This path should never be hit since the column function never returns an
		// error.
		panic(err)
	}
	return cols
}

// SetBatchSize sets the desired batch size. It should only be used in tests.
func (jr *joinReader) SetBatchSize(batchSize int) {
	jr.batchSize = batchSize
}

// Spilled returns whether the joinReader spilled to disk.
func (jr *joinReader) Spilled() bool {
	return jr.lookedUpRows.(*rowcontainer.DiskBackedIndexedRowContainer).Spilled()
}

// neededRightCols returns the set of column indices which need to be fetched
// from the right side of the join (jr.desc).
func (jr *joinReader) neededRightCols() util.FastIntSet {
	neededCols := jr.Out.NeededColumns()

	// Get the columns from the right side of the join and shift them over by
	// the size of the left side so the right side starts at 0.
	neededRightCols := util.MakeFastIntSet()
	for i, ok := neededCols.Next(len(jr.inputTypes)); ok; i, ok = neededCols.Next(i + 1) {
		neededRightCols.Add(i - len(jr.inputTypes))
	}

	// Add columns needed by OnExpr.
	for _, v := range jr.onCond.Vars.GetIndexedVars() {
		rightIdx := v.Idx - len(jr.inputTypes)
		if rightIdx >= 0 {
			neededRightCols.Add(rightIdx)
		}
	}

	return neededRightCols
}

// Generate spans for a given row.
// If lookup columns are specified will use those to collect the relevant
// columns. Otherwise the first rows are assumed to correspond with the index.
// It additionally returns whether the row contains null, which is needed to
// decide whether or not to split the generated span into separate family
// specific spans.
func (jr *joinReader) generateSpan(
	row sqlbase.EncDatumRow,
) (_ roachpb.Span, containsNull bool, _ error) {
	numLookupCols := len(jr.lookupCols)
	if numLookupCols > jr.numKeyCols {
		return roachpb.Span{}, false, errors.Errorf(
			"%d lookup columns specified, expecting at most %d", numLookupCols, jr.numKeyCols)
	}

	jr.indexKeyRow = jr.indexKeyRow[:0]
	for _, id := range jr.lookupCols {
		jr.indexKeyRow = append(jr.indexKeyRow, row[id])
	}
	return jr.spanBuilder.SpanFromEncDatums(jr.indexKeyRow, numLookupCols)
}

// Next is part of the RowSource interface.
func (jr *joinReader) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for jr.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch jr.runningState {
		case jrReadingInput:
			jr.runningState, meta = jr.readInput()
		case jrPerformingLookup:
			jr.runningState, meta = jr.performLookup()
		case jrEmittingRows:
			jr.runningState, row, meta = jr.emitRow()
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
func (jr *joinReader) readInput() (joinReaderState, *execinfrapb.ProducerMetadata) {
	// Read the next batch of input rows.
	for len(jr.inputRows) < jr.batchSize {
		row, meta := jr.input.Next()
		if meta != nil {
			if meta.Err != nil {
				jr.MoveToDraining(nil /* err */)
				return jrStateUnknown, meta
			}
			return jrReadingInput, meta
		}
		if row == nil {
			break
		}
		jr.inputRows = append(jr.inputRows, jr.rowAlloc.CopyRow(row))
	}

	if len(jr.inputRows) == 0 {
		log.VEventf(jr.Ctx, 1, "no more input rows")
		// We're done.
		jr.MoveToDraining(nil)
		return jrStateUnknown, jr.DrainHelper()
	}
	log.VEventf(jr.Ctx, 1, "read %d input rows", len(jr.inputRows))

	// Maintain a map from input row index to the corresponding output rows. This
	// will allow us to preserve the order of the input in the face of multiple
	// input rows having the same lookup keyspan, or if we're doing an outer join
	// and we need to emit unmatched rows.
	if cap(jr.inputRowIdxToLookedUpRowIdx) >= len(jr.inputRows) {
		jr.inputRowIdxToLookedUpRowIdx = jr.inputRowIdxToLookedUpRowIdx[:len(jr.inputRows)]
		for i := range jr.inputRowIdxToLookedUpRowIdx {
			jr.inputRowIdxToLookedUpRowIdx[i] = jr.inputRowIdxToLookedUpRowIdx[i][:0]
		}
	} else {
		jr.inputRowIdxToLookedUpRowIdx = make([][]int, len(jr.inputRows))
	}

	// Start the index lookup. We maintain a map from index key to the
	// corresponding input rows so we can join the index results to the
	// inputs.
	var spans roachpb.Spans
	for i, inputRow := range jr.inputRows {
		if jr.hasNullLookupColumn(inputRow) {
			continue
		}
		span, containsNull, err := jr.generateSpan(inputRow)
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}
		inputRowIndices := jr.keyToInputRowIndices[string(span.Key)]
		if inputRowIndices == nil {
			spans = jr.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(
				spans, span, len(jr.lookupCols), containsNull)
		}
		jr.keyToInputRowIndices[string(span.Key)] = append(inputRowIndices, i)
	}
	if len(spans) == 0 {
		// All of the input rows were filtered out. Skip the index lookup.
		return jrEmittingRows, nil
	}
	// Sort the spans so that we can rely upon the fetcher to limit the number of
	// results per batch. It's safe to reorder the spans here because we already
	// restore the original order of the output during the output collection
	// phase.
	sort.Sort(spans)
	log.VEventf(jr.Ctx, 1, "scanning %d spans", len(spans))
	err := jr.fetcher.StartScan(
		jr.Ctx, jr.FlowCtx.Txn, spans, jr.shouldLimitBatches, 0, /* limitHint */
		jr.FlowCtx.TraceKV)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, jr.DrainHelper()
	}

	return jrPerformingLookup, nil
}

// performLookup reads the next batch of index rows, joins them to the
// corresponding input rows, and adds the results to
// jr.inputRowIdxToLookedUpRowIdx.
func (jr *joinReader) performLookup() (joinReaderState, *execinfrapb.ProducerMetadata) {
	nCols := len(jr.lookupCols)

	isJoinTypePartialJoin := jr.joinType == sqlbase.LeftSemiJoin || jr.joinType == sqlbase.LeftAntiJoin
	log.VEventf(jr.Ctx, 1, "joining rows")
	// Read the entire set of rows looked up for the last input batch.
	lookedUpRowIdx := 0
	for ; ; lookedUpRowIdx++ {
		// Construct a "partial key" of nCols, so we can match the key format that
		// was stored in our keyToInputRowIndices map. This matches the format that
		// is output in jr.generateSpan.
		key, err := jr.fetcher.PartialKey(nCols)
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}

		// Fetch the next row and copy it into the row container.
		lookedUpRow, _, _, err := jr.fetcher.NextRow(jr.Ctx)
		if err != nil {
			jr.MoveToDraining(scrub.UnwrapScrubError(err))
			return jrStateUnknown, jr.DrainHelper()
		}
		if lookedUpRow == nil {
			// Done with this input batch.
			break
		}
		if !isJoinTypePartialJoin {
			// Replace missing values with nulls to appease the row container.
			for i := range lookedUpRow {
				if lookedUpRow[i].IsUnset() {
					lookedUpRow[i].Datum = tree.DNull
				}
			}
			if err := jr.lookedUpRows.AddRow(jr.Ctx, lookedUpRow); err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
		}

		// Update our map from input rows to looked up rows.
		for _, inputRowIdx := range jr.keyToInputRowIndices[string(key)] {
			if isJoinTypePartialJoin {
				// During a SemiJoin or AntiJoin, we only output if we've seen no match
				// for this input row yet. Additionally, since we don't have to render
				// anything to output a Semi or Anti join match, we can evaluate our
				// on condition now and only buffer if we pass it.
				if len(jr.inputRowIdxToLookedUpRowIdx[inputRowIdx]) == 0 {
					renderedRow, err := jr.render(jr.inputRows[inputRowIdx], lookedUpRow)
					if err != nil {
						jr.MoveToDraining(err)
						return jrStateUnknown, jr.DrainHelper()
					}
					if renderedRow == nil {
						// We failed our on-condition - don't buffer anything.
						continue
					}
					jr.inputRowIdxToLookedUpRowIdx[inputRowIdx] = partialJoinSentinel
				}
			} else {
				jr.inputRowIdxToLookedUpRowIdx[inputRowIdx] = append(
					jr.inputRowIdxToLookedUpRowIdx[inputRowIdx], lookedUpRowIdx)
			}
		}
	}
	log.VEventf(jr.Ctx, 1, "done joining rows (%d loop iterations)", lookedUpRowIdx)

	return jrEmittingRows, nil
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (
	joinReaderState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	// Loop until we find a valid row to emit, or the cursor runs off the end.
	if jr.emitCursor.inputRowIdx >= len(jr.inputRowIdxToLookedUpRowIdx) {
		log.VEventf(jr.Ctx, 1, "done emitting rows")
		// Ready for another input batch. Reset state.
		jr.inputRows = jr.inputRows[:0]
		jr.keyToInputRowIndices = make(map[string][]int)
		jr.emitCursor.outputRowIdx = 0
		jr.emitCursor.inputRowIdx = 0
		jr.emitCursor.seenMatch = false
		if err := jr.lookedUpRows.UnsafeReset(jr.Ctx); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, nil, jr.DrainHelper()
		}
		return jrReadingInput, nil, nil
	}
	inputRow := jr.inputRows[jr.emitCursor.inputRowIdx]
	lookedUpRows := jr.inputRowIdxToLookedUpRowIdx[jr.emitCursor.inputRowIdx]
	if jr.emitCursor.outputRowIdx >= len(lookedUpRows) {
		// We have no more rows for the current input row. Emit an outer or anti
		// row if we didn't see a match, and bump to the next input row.
		jr.emitCursor.inputRowIdx++
		jr.emitCursor.outputRowIdx = 0
		seenMatch := jr.emitCursor.seenMatch
		jr.emitCursor.seenMatch = false
		if !seenMatch {
			switch jr.joinType {
			case sqlbase.LeftOuterJoin:
				// An outer-join non-match means we emit the input row with NULLs for
				// the right side (if it passes the ON-condition).
				if renderedRow := jr.renderUnmatchedRow(inputRow, leftSide); renderedRow != nil {
					return jrEmittingRows, renderedRow, nil
				}
			case sqlbase.LeftAntiJoin:
				// An anti-join non-match means we emit the input row.
				return jrEmittingRows, inputRow, nil
			}
		}
		return jrEmittingRows, nil, nil
	}

	lookedUpRowIdx := lookedUpRows[jr.emitCursor.outputRowIdx]
	jr.emitCursor.outputRowIdx++
	switch jr.joinType {
	case sqlbase.LeftSemiJoin:
		// A semi-join match means we emit our input row.
		jr.emitCursor.seenMatch = true
		return jrEmittingRows, inputRow, nil
	case sqlbase.LeftAntiJoin:
		// An anti-join match means we emit nothing.
		jr.emitCursor.seenMatch = true
		return jrEmittingRows, nil, nil
	}

	lookedUpRow, err := jr.lookedUpRows.GetRow(jr.Ctx, lookedUpRowIdx)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	outputRow, err := jr.render(inputRow, lookedUpRow.(rowcontainer.IndexedRow).Row)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	if outputRow != nil {
		jr.emitCursor.seenMatch = true
	}
	return jrEmittingRows, outputRow, nil
}

func (jr *joinReader) hasNullLookupColumn(row sqlbase.EncDatumRow) bool {
	for _, colIdx := range jr.lookupCols {
		if row[colIdx].IsNull() {
			return true
		}
	}
	return false
}

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) context.Context {
	jr.input.Start(ctx)
	ctx = jr.StartInternal(ctx, joinReaderProcName)
	jr.runningState = jrReadingInput
	return ctx
}

// ConsumerClosed is part of the RowSource interface.
func (jr *joinReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	jr.close()
}

func (jr *joinReader) close() {
	if jr.InternalClose() {
		if jr.lookedUpRows != nil {
			jr.lookedUpRows.Close(jr.Ctx)
		}
		jr.MemMonitor.Stop(jr.Ctx)
		if jr.diskMonitor != nil {
			jr.diskMonitor.Stop(jr.Ctx)
		}
	}
}

var _ execinfrapb.DistSQLSpanStats = &JoinReaderStats{}

const joinReaderTagPrefix = "joinreader."

// Stats implements the SpanStats interface.
func (jrs *JoinReaderStats) Stats() map[string]string {
	statsMap := jrs.InputStats.Stats(joinReaderTagPrefix)
	toMerge := jrs.IndexLookupStats.Stats(joinReaderTagPrefix + "index.")
	for k, v := range toMerge {
		statsMap[k] = v
	}
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (jrs *JoinReaderStats) StatsForQueryPlan() []string {
	is := append(
		jrs.InputStats.StatsForQueryPlan(""),
		jrs.IndexLookupStats.StatsForQueryPlan("index ")...,
	)
	return is
}

// outputStatsToTrace outputs the collected joinReader stats to the trace. Will
// fail silently if the joinReader is not collecting stats.
func (jr *joinReader) outputStatsToTrace() {
	is, ok := getInputStats(jr.FlowCtx, jr.input)
	if !ok {
		return
	}
	ils, ok := getFetcherInputStats(jr.FlowCtx, jr.fetcher)
	if !ok {
		return
	}

	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if sp := opentracing.SpanFromContext(jr.Ctx); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}

func (jr *joinReader) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if tfs := execinfra.GetLeafTxnFinalState(ctx, jr.FlowCtx.Txn); tfs != nil {
		return []execinfrapb.ProducerMetadata{{LeafTxnFinalState: tfs}}
	}
	return nil
}

// DrainMeta is part of the MetadataSource interface.
func (jr *joinReader) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return jr.generateMeta(ctx)
}

// ChildCount is part of the execinfra.OpNode interface.
func (jr *joinReader) ChildCount(verbose bool) int {
	return 1
}

// Child is part of the execinfra.OpNode interface.
func (jr *joinReader) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := jr.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to joinReader is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
