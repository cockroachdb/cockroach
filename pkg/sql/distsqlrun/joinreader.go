// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// TODO(radu): we currently create one batch at a time and run the KV operations
// on this node. In the future we may want to build separate batches for the
// nodes that "own" the respective ranges, and send out flows on those nodes.
const joinReaderBatchSize = 100

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

	desc      sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	colIdxMap map[sqlbase.ColumnID]int

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// joinReader to wrap the fetcher with a stat collector when necessary.
	fetcher        rowFetcher
	indexKeyPrefix []byte
	alloc          sqlbase.DatumAlloc
	rowAlloc       sqlbase.EncDatumRowAlloc

	input      RowSource
	inputTypes []types.T
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols columns
	// indexTypes is an array of the types of the index we're looking up into,
	// in the order of the columns in that index.
	indexTypes []types.T
	// indexDirs is an array of the directions for the index's key columns.
	indexDirs []sqlbase.IndexDescriptor_Direction

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// State variables for each batch of input rows.
	inputRows            sqlbase.EncDatumRows
	keyToInputRowIndices map[string][]int
	// inputRowIdxToLookedUpRows is a slice of looked up rows, one per row in
	// inputRows. It's populated in the jrPerformingLookup state. For non partial
	// joins (everything but semi/anti join), the looked up rows are the rows that
	// came back from the lookup span for each input row, without checking for
	// matches with respect to the on-condition. For semi/anti join, we store at
	// most one nil, indicating a matching lookup if it's present, since the right
	// side of a semi/anti join is not used.
	inputRowIdxToLookedUpRows []sqlbase.EncDatumRows
	// emitCursor contains information about where the next row to emit is within
	// inputRowIdxToOutputRows.
	emitCursor struct {
		// inputRowIdx contains the index into inputRowIdxToOutputRows that we're
		// about to emit.
		inputRowIdx int
		// outputRowIdx contains the index into the inputRowIdx'th row of
		// inputRowIdxToOutputRows that we're about to emit.
		outputRowIdx int
		// seenMatch is true if there was a match at the current inputRowIdx. A
		// match means that there's no need to output an outer or anti join row.
		seenMatch bool
	}

	// neededFamilies maintains what families we need to query from if our
	// needed columns span multiple queries
	neededFamilies []sqlbase.FamilyID

	indexKeyRow sqlbase.EncDatumRow
}

var _ Processor = &joinReader{}
var _ RowSource = &joinReader{}
var _ distsqlpb.MetadataSource = &joinReader{}

const joinReaderProcName = "join reader"

func newJoinReader(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.JoinReaderSpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*joinReader, error) {
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
	returnMutations := spec.Visibility == distsqlpb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	jr.colIdxMap = jr.desc.ColumnIdxMapWithMutations(returnMutations)

	var columnIDs []sqlbase.ColumnID
	columnIDs, jr.indexDirs = jr.index.FullColumnIDs()
	indexCols := make([]uint32, len(columnIDs))
	jr.indexTypes = make([]types.T, len(columnIDs))
	columnTypes := jr.desc.ColumnTypesWithMutations(returnMutations)
	for i, columnID := range columnIDs {
		indexCols[i] = uint32(columnID)
		jr.indexTypes[i] = columnTypes[jr.colIdxMap[columnID]]
	}

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
		ProcStateOpts{
			InputsToDrain: []RowSource{jr.input},
			TrailingMetaCallback: func(ctx context.Context) []distsqlpb.ProducerMetadata {
				jr.InternalClose()
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
		neededRightCols, false /* isCheck */, &jr.alloc, spec.Visibility,
	)
	if err != nil {
		return nil, err
	}
	if collectingStats {
		jr.input = NewInputStatCollector(jr.input)
		jr.fetcher = newRowFetcherStatCollector(&fetcher)
		jr.finishTrace = jr.outputStatsToTrace
	} else {
		jr.fetcher = &rowFetcherWrapper{Fetcher: &fetcher}
	}

	jr.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(&jr.desc, jr.index.ID)

	jr.neededFamilies = sqlbase.NeededColumnFamilyIDs(
		spec.Table.ColumnIdxMap(),
		spec.Table.Families,
		jr.neededRightCols(),
	)

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

// neededRightCols returns the set of column indices which need to be fetched
// from the right side of the join (jr.desc).
func (jr *joinReader) neededRightCols() util.FastIntSet {
	neededCols := jr.out.neededColumns()

	// Get the columns from the right side of the join and shift them over by
	// the size of the left side so the right side starts at 0.
	neededRightCols := util.MakeFastIntSet()
	for i, ok := neededCols.Next(len(jr.inputTypes)); ok; i, ok = neededCols.Next(i + 1) {
		neededRightCols.Add(i - len(jr.inputTypes))
	}

	// Add columns needed by OnExpr.
	for _, v := range jr.onCond.vars.GetIndexedVars() {
		rightIdx := v.Idx - len(jr.inputTypes)
		if rightIdx >= 0 {
			neededRightCols.Add(rightIdx)
		}
	}

	return neededRightCols
}

// Generate a span for a given row.
// If lookup columns are specified will use those to collect the relevant
// columns. Otherwise the first rows are assumed to correspond with the index.
func (jr *joinReader) generateSpan(row sqlbase.EncDatumRow) (roachpb.Span, error) {
	numKeyCols := len(jr.indexTypes)
	numLookupCols := len(jr.lookupCols)

	if numLookupCols > numKeyCols {
		return roachpb.Span{}, errors.Errorf(
			"%d lookup columns specified, expecting at most %d", numLookupCols, numKeyCols)
	}

	jr.indexKeyRow = jr.indexKeyRow[:0]
	for _, id := range jr.lookupCols {
		jr.indexKeyRow = append(jr.indexKeyRow, row[id])
	}
	return sqlbase.MakeSpanFromEncDatums(
		jr.indexKeyPrefix, jr.indexKeyRow, jr.indexTypes[:numLookupCols], jr.indexDirs, &jr.desc,
		jr.index, &jr.alloc)
}

func (jr *joinReader) maybeSplitSpanIntoSeparateFamilies(span roachpb.Span) roachpb.Spans {
	// check the following:
	// - we have more than one needed family
	// - we are looking at the primary key
	// - our table has more than the default family
	// - we have all the columns of the index
	if len(jr.neededFamilies) > 0 &&
		jr.index.ID == jr.desc.PrimaryIndex.ID &&
		len(jr.lookupCols) == len(jr.index.ColumnIDs) &&
		len(jr.neededFamilies) < len(jr.desc.Families) {
		return sqlbase.SplitSpanIntoSeparateFamilies(span, jr.neededFamilies)
	}
	return roachpb.Spans{span}
}

// Next is part of the RowSource interface.
func (jr *joinReader) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for jr.State == StateRunning {
		var row sqlbase.EncDatumRow
		var meta *distsqlpb.ProducerMetadata
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
func (jr *joinReader) readInput() (joinReaderState, *distsqlpb.ProducerMetadata) {
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
		// We're done.
		jr.MoveToDraining(nil)
		return jrStateUnknown, jr.DrainHelper()
	}

	// Maintain a map from input row index to the corresponding output rows. This
	// will allow us to preserve the order of the input in the face of multiple
	// input rows having the same lookup keyspan, or if we're doing an outer join
	// and we need to emit unmatched rows.
	if cap(jr.inputRowIdxToLookedUpRows) >= len(jr.inputRows) {
		jr.inputRowIdxToLookedUpRows = jr.inputRowIdxToLookedUpRows[:len(jr.inputRows)]
		for i := range jr.inputRowIdxToLookedUpRows {
			jr.inputRowIdxToLookedUpRows[i] = jr.inputRowIdxToLookedUpRows[i][:0]
		}
	} else {
		jr.inputRowIdxToLookedUpRows = make([]sqlbase.EncDatumRows, len(jr.inputRows))
	}

	// Start the index lookup. We maintain a map from index key to the
	// corresponding input rows so we can join the index results to the
	// inputs.
	var spans roachpb.Spans
	for i, inputRow := range jr.inputRows {
		if jr.hasNullLookupColumn(inputRow) {
			continue
		}
		span, err := jr.generateSpan(inputRow)
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}
		inputRowIndices := jr.keyToInputRowIndices[string(span.Key)]
		if inputRowIndices == nil {
			spans = append(spans, jr.maybeSplitSpanIntoSeparateFamilies(span)...)
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
	err := jr.fetcher.StartScan(
		jr.Ctx, jr.flowCtx.txn, spans, true /* limitBatches */, 0, /* limitHint */
		jr.flowCtx.traceKV)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, jr.DrainHelper()
	}

	return jrPerformingLookup, nil
}

// performLookup reads the next batch of index rows, joins them to the
// corresponding input rows, and adds the results to jr.inputRowIdxToOutputRows.
func (jr *joinReader) performLookup() (joinReaderState, *distsqlpb.ProducerMetadata) {
	nCols := len(jr.lookupCols)

	isJoinTypePartialJoin := jr.joinType == sqlbase.LeftSemiJoin || jr.joinType == sqlbase.LeftAntiJoin
	// Read the entire set of rows looked up for the last input batch.
	for {
		// Construct a "partial key" of nCols, so we can match the key format that
		// was stored in our keyToInputRowIndices map. This matches the format that
		// is output in jr.generateSpan.
		key, err := jr.fetcher.PartialKey(nCols)
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}

		lookedUpRow, meta := jr.fetcher.Next()
		if meta != nil {
			jr.MoveToDraining(scrub.UnwrapScrubError(meta.Err))
			return jrStateUnknown, jr.DrainHelper()
		}
		if lookedUpRow == nil {
			// Done with this input batch.
			break
		}

		for _, inputRowIdx := range jr.keyToInputRowIndices[string(key)] {
			if isJoinTypePartialJoin {
				// During a SemiJoin or AntiJoin, we only output if we've seen no match
				// for this input row yet. Additionally, since we don't have to render
				// anything to output a Semi or Anti join match, we can evaluate our
				// on condition now and only buffer if we pass it.
				if len(jr.inputRowIdxToLookedUpRows[inputRowIdx]) == 0 {
					renderedRow, err := jr.render(jr.inputRows[inputRowIdx], lookedUpRow)
					if err != nil {
						jr.MoveToDraining(err)
						return jrStateUnknown, jr.DrainHelper()
					}
					if renderedRow == nil {
						// We failed our on-condition - don't buffer anything.
						continue
					}
					jr.inputRowIdxToLookedUpRows[inputRowIdx] = append(
						jr.inputRowIdxToLookedUpRows[inputRowIdx], nil)
				}
			} else {
				jr.inputRowIdxToLookedUpRows[inputRowIdx] = append(
					jr.inputRowIdxToLookedUpRows[inputRowIdx],
					jr.out.rowAlloc.CopyRow(lookedUpRow))
			}
		}
	}

	return jrEmittingRows, nil
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (
	joinReaderState,
	sqlbase.EncDatumRow,
	*distsqlpb.ProducerMetadata,
) {
	// Loop until we find a valid row to emit, or the cursor runs off the end.
	if jr.emitCursor.inputRowIdx >= len(jr.inputRowIdxToLookedUpRows) {
		// Ready for another input batch. Reset state.
		jr.inputRows = jr.inputRows[:0]
		jr.keyToInputRowIndices = make(map[string][]int)
		jr.emitCursor.outputRowIdx = 0
		jr.emitCursor.inputRowIdx = 0
		jr.emitCursor.seenMatch = false
		return jrReadingInput, nil, nil
	}
	inputRow := jr.inputRows[jr.emitCursor.inputRowIdx]
	lookedUpRows := jr.inputRowIdxToLookedUpRows[jr.emitCursor.inputRowIdx]
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

	lookedUpRow := lookedUpRows[jr.emitCursor.outputRowIdx]
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

	outputRow, err := jr.render(inputRow, lookedUpRow)
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
	jr.fetcher.Start(ctx)
	jr.runningState = jrReadingInput
	return ctx
}

// ConsumerClosed is part of the RowSource interface.
func (jr *joinReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	jr.InternalClose()
}

var _ distsqlpb.DistSQLSpanStats = &JoinReaderStats{}

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
	is, ok := getInputStats(jr.flowCtx, jr.input)
	if !ok {
		return
	}
	ils, ok := getFetcherInputStats(jr.flowCtx, jr.fetcher)
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

func (jr *joinReader) generateMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	if meta := getTxnCoordMeta(ctx, jr.flowCtx.txn); meta != nil {
		return []distsqlpb.ProducerMetadata{{TxnCoordMeta: meta}}
	}
	return nil
}

// DrainMeta is part of the MetadataSource interface.
func (jr *joinReader) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	return jr.generateMeta(ctx)
}
