// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	// processorBase.state - the runningState is only relevant when
	// processorBase.state == stateRunning.
	runningState joinReaderState

	desc      sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	colIdxMap map[sqlbase.ColumnID]int

	// fetcherInput wraps fetcher in a RowSource implementation and should be used
	// to get rows from the fetcher. This enables the joinReader to wrap the
	// fetcherInput with a stat collector when necessary.
	fetcherInput   RowSource
	fetcher        sqlbase.RowFetcher
	indexKeyPrefix []byte
	alloc          sqlbase.DatumAlloc
	rowAlloc       sqlbase.EncDatumRowAlloc

	input      RowSource
	inputTypes []sqlbase.ColumnType
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols columns
	// indexFilter is the filter expression from the index's scanNode in a
	// lookup join. It is applied to index rows before they are joined to the
	// input rows.
	indexFilter exprHelper

	// These fields are set only for lookup joins on secondary indexes which
	// require an additional primary index lookup.
	// primaryFetcherInput wraps primaryFetcher in a RowSource implementation for
	// the same reason that fetcher is wrapped.
	primaryFetcherInput RowSource
	primaryFetcher      *sqlbase.RowFetcher
	primaryColumnTypes  []sqlbase.ColumnType
	primaryKeyPrefix    []byte

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// State variables for each batch of input rows.
	inputRows            sqlbase.EncDatumRows
	keyToInputRowIndices map[string][]int
	emitted              []bool
	finalLookupBatch     bool
	toEmit               sqlbase.EncDatumRows
}

var _ Processor = &joinReader{}
var _ RowSource = &joinReader{}

const joinReaderProcName = "join reader"

func newJoinReader(
	flowCtx *FlowCtx,
	processorID int32,
	spec *JoinReaderSpec,
	input RowSource,
	post *PostProcessSpec,
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
	jr.colIdxMap = jr.desc.ColumnIdxMap()

	indexCols := make([]uint32, len(jr.index.ColumnIDs))
	for i, columnID := range jr.index.ColumnIDs {
		indexCols[i] = uint32(columnID)
	}
	if err := jr.joinerBase.init(
		jr,
		flowCtx,
		processorID,
		input.OutputTypes(),
		jr.desc.ColumnTypes(),
		spec.Type,
		spec.OnExpr,
		jr.lookupCols,
		indexCols,
		0, /* numMergedColumns */
		post,
		output,
		procStateOpts{
			inputsToDrain: []RowSource{jr.input},
			trailingMetaCallback: func() []ProducerMetadata {
				jr.internalClose()
				if meta := getTxnCoordMeta(jr.flowCtx.txn); meta != nil {
					return []ProducerMetadata{{TxnCoordMeta: meta}}
				}
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	if spec.IndexFilterExpr.Expr != "" {
		err := jr.indexFilter.init(spec.IndexFilterExpr, jr.desc.ColumnTypes(), jr.evalCtx)
		if err != nil {
			return nil, err
		}
	}

	// neededIndexColumns is the set of columns we need to fetch from jr.index.
	var neededIndexColumns util.FastIntSet

	collectingStats := false
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		collectingStats = true
	}

	if !isSecondary || jr.neededRightCols().SubsetOf(getIndexColSet(jr.index, jr.colIdxMap)) {
		// jr.index includes all the needed output columns, so only need one lookup.
		neededIndexColumns = jr.neededRightCols()
	} else {
		// jr.index is a secondary index which does not contain all the needed
		// output columns. First we'll retrieve the primary index columns from the
		// secondary index, then do a second lookup on the primary index to get the
		// needed output columns.
		neededIndexColumns = getIndexColSet(&jr.desc.PrimaryIndex, jr.colIdxMap)
		jr.primaryFetcher = &sqlbase.RowFetcher{}
		_, _, err = initRowFetcher(
			jr.primaryFetcher, &jr.desc, 0 /* indexIdx */, jr.colIdxMap, false, /* reverse */
			jr.neededRightCols(), false /* isCheck */, &jr.alloc,
		)
		if err != nil {
			return nil, err
		}
		jr.primaryColumnTypes, err = getPrimaryColumnTypes(&jr.desc)
		if err != nil {
			return nil, err
		}
		jr.primaryKeyPrefix = sqlbase.MakeIndexKeyPrefix(&jr.desc, jr.desc.PrimaryIndex.ID)

		jr.primaryFetcherInput = &rowFetcherWrapper{RowFetcher: jr.primaryFetcher}
		if collectingStats {
			jr.primaryFetcherInput = NewInputStatCollector(jr.primaryFetcherInput)
		}
	}
	_, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		neededIndexColumns, false /* isCheck */, &jr.alloc,
	)
	if err != nil {
		return nil, err
	}
	jr.fetcherInput = &rowFetcherWrapper{RowFetcher: &jr.fetcher}
	if collectingStats {
		jr.input = NewInputStatCollector(jr.input)
		jr.fetcherInput = NewInputStatCollector(jr.fetcherInput)
		jr.finishTrace = jr.outputStatsToTrace
	}

	jr.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(&jr.desc, jr.index.ID)

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

func getPrimaryColumnTypes(table *sqlbase.TableDescriptor) ([]sqlbase.ColumnType, error) {
	columnTypes := make([]sqlbase.ColumnType, len(table.PrimaryIndex.ColumnIDs))
	for i, columnID := range table.PrimaryIndex.ColumnIDs {
		column, err := table.FindColumnByID(columnID)
		if err != nil {
			return nil, err
		}
		columnTypes[i] = column.Type
	}
	return columnTypes, nil
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

	// Add columns needed by the index filter.
	for _, v := range jr.indexFilter.vars.GetIndexedVars() {
		neededRightCols.Add(v.Idx)
	}

	return neededRightCols
}

// Generate a key to create a span for a given row.
// If lookup columns are specified will use those to collect the relevant
// columns. Otherwise the first rows are assumed to correspond with the index.
func (jr *joinReader) generateKey(row sqlbase.EncDatumRow) (roachpb.Key, error) {
	numKeyCols := len(jr.index.ColumnIDs)

	// There may be extra values on the row, e.g. to allow an ordered synchronizer
	// to interleave multiple input streams.
	// Will need at most numKeyCols.
	keyRow := make(sqlbase.EncDatumRow, 0, numKeyCols)
	types := make([]sqlbase.ColumnType, 0, numKeyCols)

	if len(jr.lookupCols) > numKeyCols {
		return nil, errors.Errorf(
			"%d lookup columns specified, expecting at most %d", len(jr.lookupCols), numKeyCols)
	}
	for _, id := range jr.lookupCols {
		keyRow = append(keyRow, row[id])
		types = append(types, jr.inputTypes[id])
	}

	return sqlbase.MakeKeyFromEncDatums(
		types, keyRow, &jr.desc, jr.index, jr.indexKeyPrefix, &jr.alloc)
}

// Next is part of the RowSource interface.
func (jr *joinReader) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - If the index is a secondary index which does not contain all the needed
	//   output columns, perform a second lookup on the primary index.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for jr.state == stateRunning {
		var row sqlbase.EncDatumRow
		var meta *ProducerMetadata
		switch jr.runningState {
		case jrReadingInput:
			jr.runningState, meta = jr.readInput()
		case jrPerformingLookup:
			jr.runningState, meta = jr.performLookup()
		case jrEmittingRows:
			jr.runningState, row, meta = jr.emitRow()
		default:
			log.Fatalf(jr.ctx, "unsupported state: %d", jr.runningState)
		}
		if row != nil || meta != nil {
			return row, meta
		}
	}
	return nil, jr.drainHelper()
}

// readInput reads the next batch of input rows and starts an index scan.
func (jr *joinReader) readInput() (joinReaderState, *ProducerMetadata) {
	// Read the next batch of input rows.
	for len(jr.inputRows) < jr.batchSize {
		row, meta := jr.input.Next()
		if meta != nil {
			if meta.Err != nil {
				jr.moveToDraining(nil /* err */)
				return jrStateUnknown, meta
			}
			return jrReadingInput, meta
		}
		if row == nil {
			break
		}
		if !jr.hasNullLookupColumn(row) {
			jr.inputRows = append(jr.inputRows, jr.rowAlloc.CopyRow(row))
		}
	}

	if len(jr.inputRows) == 0 {
		// We're done.
		jr.moveToDraining(nil)
		return jrStateUnknown, jr.drainHelper()
	}

	// Start the index lookup. We maintain a map from index key to the
	// corresponding input rows so we can join the index results to the
	// inputs.
	var spans roachpb.Spans
	for i, inputRow := range jr.inputRows {
		key, err := jr.generateKey(inputRow)
		if err != nil {
			jr.moveToDraining(err)
			return jrStateUnknown, jr.drainHelper()
		}
		if jr.keyToInputRowIndices[key.String()] == nil {
			spans = append(spans, roachpb.Span{Key: key, EndKey: key.PrefixEnd()})
		}
		jr.keyToInputRowIndices[key.String()] = append(jr.keyToInputRowIndices[key.String()], i)
	}
	err := jr.fetcher.StartScan(
		jr.ctx, jr.flowCtx.txn, spans, false /* limitBatches */, 0, /* limitHint */
		jr.flowCtx.traceKV)
	if err != nil {
		jr.moveToDraining(err)
		return jrStateUnknown, jr.drainHelper()
	}

	// If this is an outer join, track emitted rows so we can emit unmatched rows
	// at the end. (Note: we track emitted rows rather than non-emitted since
	// there are multiple reasons a row might not be emitted: the index lookup
	// might return no corresponding rows, or all the rows it returns might fail
	// the ON condition, which is applied in the render step.)
	if jr.joinType == sqlbase.LeftOuterJoin {
		jr.emitted = make([]bool, len(jr.inputRows))
	}

	return jrPerformingLookup, nil
}

// performLookup reads the next batch of index rows (performing a second lookup
// against the primary index if necessary), joins them to the corresponding
// input rows, and adds the results to jr.toEmit.
func (jr *joinReader) performLookup() (joinReaderState, *ProducerMetadata) {
	// lookupRow represents an index key and the corresponding index row.
	type lookupRow struct {
		key string
		row sqlbase.EncDatumRow
	}
	lookupRows := make([]lookupRow, 0, jr.batchSize)

	// Read the next batch of index rows.
	for len(lookupRows) < jr.batchSize {
		key := jr.fetcher.IndexKeyString(len(jr.lookupCols))
		indexRow, meta := jr.fetcherInput.Next()
		if meta != nil {
			jr.moveToDraining(scrub.UnwrapScrubError(meta.Err))
			return jrStateUnknown, jr.drainHelper()
		}
		if indexRow == nil {
			// Done with this input batch.
			jr.finalLookupBatch = true
			break
		}
		lookupRows = append(lookupRows, lookupRow{key: key, row: jr.rowAlloc.CopyRow(indexRow)})
	}

	if jr.primaryFetcher != nil {
		// The lookup was on a non-covering secondary index, so we need to do a
		// second lookup against the primary index and replace our previous
		// results with the primary rows.
		// TODO(solon): Allocate this up front rather than once per batch.
		secondaryIndexRows := make([]sqlbase.EncDatumRow, len(lookupRows))
		for i := range lookupRows {
			secondaryIndexRows[i] = lookupRows[i].row
		}
		primaryRows, err := jr.primaryLookup(jr.ctx, jr.flowCtx.txn, secondaryIndexRows)
		if err != nil {
			jr.moveToDraining(err)
			return jrStateUnknown, jr.drainHelper()
		}
		for i := range primaryRows {
			lookupRows[i].row = primaryRows[i]
		}
	}

	// Iterate over the lookup results, map them to the input rows, and emit the
	// rendered rows.
	for _, lookupRow := range lookupRows {
		if jr.indexFilter.expr != nil {
			// Apply index filter.
			res, err := jr.indexFilter.evalFilter(lookupRow.row)
			if err != nil {
				jr.moveToDraining(err)
				return jrStateUnknown, jr.drainHelper()
			}
			if !res {
				continue
			}
		}
		for _, inputRowIdx := range jr.keyToInputRowIndices[lookupRow.key] {
			renderedRow, err := jr.render(jr.inputRows[inputRowIdx], lookupRow.row)
			if err != nil {
				jr.moveToDraining(err)
				return jrStateUnknown, jr.drainHelper()
			}
			if renderedRow != nil {
				if row := jr.processRowHelper(renderedRow); row != nil {
					jr.toEmit = append(jr.toEmit, jr.out.rowAlloc.CopyRow(row))
					if jr.emitted != nil {
						jr.emitted[inputRowIdx] = true
					}
				}
			}
		}
	}

	if jr.finalLookupBatch && jr.emitted != nil {
		// Emit unmatched rows.
		for i := 0; i < len(jr.inputRows); i++ {
			if !jr.emitted[i] {
				if renderedRow := jr.renderUnmatchedRow(jr.inputRows[i], leftSide); renderedRow != nil {
					if row := jr.processRowHelper(renderedRow); row != nil {
						jr.toEmit = append(jr.toEmit, jr.out.rowAlloc.CopyRow(row))
					}
				}
			}
		}
	}

	return jrEmittingRows, nil
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (joinReaderState, sqlbase.EncDatumRow, *ProducerMetadata) {
	if len(jr.toEmit) == 0 {
		if jr.finalLookupBatch {
			// Ready for another input batch. Reset state.
			jr.inputRows = jr.inputRows[:0]
			jr.keyToInputRowIndices = make(map[string][]int)
			jr.finalLookupBatch = false
			return jrReadingInput, nil, nil
		}
		// Process the next index lookup batch.
		return jrPerformingLookup, nil, nil
	}
	row := jr.toEmit[0]
	jr.toEmit = jr.toEmit[1:]
	return jrEmittingRows, row, nil
}

func (jr *joinReader) hasNullLookupColumn(row sqlbase.EncDatumRow) bool {
	for _, colIdx := range jr.lookupCols {
		if row[colIdx].IsNull() {
			return true
		}
	}
	return false
}

// primaryLookup looks up the corresponding primary index rows, given a batch of
// secondary index rows. Since we expect a 1-1 correspondence between the input
// and output, it returns a slice of rows where each row corresponds to the
// input with the same slice index.
func (jr *joinReader) primaryLookup(
	ctx context.Context, txn *client.Txn, secondaryIndexRows []sqlbase.EncDatumRow,
) ([]sqlbase.EncDatumRow, error) {
	batchSize := len(secondaryIndexRows)
	if batchSize == 0 {
		return nil, nil
	}
	numKeyCols := len(jr.desc.PrimaryIndex.ColumnIDs)
	// keyToInputRowIdx maps primary index keys to the input rows.
	keyToInputRowIdx := make(map[string]int, batchSize)
	outRows := make([]sqlbase.EncDatumRow, batchSize)

	// Build spans for the primary index lookup.
	spans := make([]roachpb.Span, batchSize)
	for rowIdx, row := range secondaryIndexRows {
		values := make(sqlbase.EncDatumRow, numKeyCols)
		for i, columnID := range jr.desc.PrimaryIndex.ColumnIDs {
			values[i] = row[jr.colIdxMap[columnID]]
		}
		key, err := sqlbase.MakeKeyFromEncDatums(
			jr.primaryColumnTypes, values, &jr.desc, &jr.desc.PrimaryIndex, jr.primaryKeyPrefix,
			&jr.alloc)
		if err != nil {
			return nil, err
		}
		keyToInputRowIdx[key.String()] = rowIdx
		spans[rowIdx] = roachpb.Span{Key: key, EndKey: key.PrefixEnd()}
	}

	// Perform the primary index scan.
	err := jr.primaryFetcher.StartScan(
		ctx, txn, spans, false /* limitBatches */, 0 /* limitHint */, jr.flowCtx.traceKV)
	if err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return nil, err
	}

	// Iterate over the fetched rows and map them onto the input rows so we can
	// return them in the same order.
	for i := 0; i < batchSize; i++ {
		key := jr.primaryFetcher.IndexKeyString(numKeyCols)
		rowIdx, ok := keyToInputRowIdx[key]
		if !ok {
			return nil, errors.Errorf("failed to find key %v in keyToInputRowIdx %v", key, keyToInputRowIdx)
		}
		row, meta := jr.primaryFetcherInput.Next()
		if meta != nil {
			return nil, meta.Err
		}
		if row == nil {
			return nil, errors.Errorf("expected %d rows but found %d", batchSize, i)
		}
		outRows[rowIdx] = jr.rowAlloc.CopyRow(row)
	}

	// Verify that we consumed all the fetched rows.
	nextRow, meta := jr.primaryFetcherInput.Next()
	if meta != nil {
		return nil, meta.Err
	}
	if nextRow != nil {
		return nil, errors.Errorf("expected %d rows but found more", batchSize)
	}

	return outRows, nil
}

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) context.Context {
	jr.input.Start(ctx)
	jr.fetcherInput.Start(ctx)
	if jr.primaryFetcherInput != nil {
		jr.primaryFetcherInput.Start(ctx)
	}
	jr.runningState = jrReadingInput
	return jr.startInternal(ctx, joinReaderProcName)
}

// ConsumerDone is part of the RowSource interface.
func (jr *joinReader) ConsumerDone() {
	jr.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (jr *joinReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	jr.internalClose()
}

var _ DistSQLSpanStats = &JoinReaderStats{}

const joinReaderTagPrefix = "joinreader."

// Stats implements the SpanStats interface.
func (jrs *JoinReaderStats) Stats() map[string]string {
	statsMap := jrs.InputStats.Stats(joinReaderTagPrefix)
	toMerge := jrs.IndexLookupStats.Stats(joinReaderTagPrefix + "index.")
	for k, v := range toMerge {
		statsMap[k] = v
	}
	if jrs.PrimaryIndexLookupStats != nil {
		toMerge = jrs.PrimaryIndexLookupStats.Stats(joinReaderTagPrefix + "primary.index.")
		for k, v := range toMerge {
			statsMap[k] = v
		}
	}
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (jrs *JoinReaderStats) StatsForQueryPlan() []string {
	is := append(
		jrs.InputStats.StatsForQueryPlan(""),
		jrs.IndexLookupStats.StatsForQueryPlan("index ")...,
	)
	if jrs.PrimaryIndexLookupStats != nil {
		is = append(is, jrs.PrimaryIndexLookupStats.StatsForQueryPlan("primary index ")...)
	}
	return is
}

// outputStatsToTrace outputs the collected joinReader stats to the trace. Will
// fail silently if the joinReader is not collecting stats.
func (jr *joinReader) outputStatsToTrace() {
	is, ok := getInputStats(jr.flowCtx, jr.input)
	if !ok {
		return
	}
	ils, ok := getInputStats(jr.flowCtx, jr.fetcherInput)
	if !ok {
		return
	}

	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if jr.primaryFetcher != nil {
		eils, ok := getInputStats(jr.flowCtx, jr.primaryFetcherInput)
		if !ok {
			return
		}
		jrs.PrimaryIndexLookupStats = &eils
	}
	if sp := opentracing.SpanFromContext(jr.ctx); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}
