// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package distsqlrun

import (
	"context"

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
	opentracing "github.com/opentracing/opentracing-go"
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
	// jrCollectingOutputRows means we are collecting the result of the index
	// lookup to be emitted, while preserving the order of the input, and
	// optionally rendering rows for unmatched inputs for left outer joins.
	jrCollectingOutputRows
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

	// fetcherInput wraps fetcher in a RowSource implementation and should be used
	// to get rows from the fetcher. This enables the joinReader to wrap the
	// fetcherInput with a stat collector when necessary.
	fetcherInput   RowSource
	fetcher        row.Fetcher
	indexKeyPrefix []byte
	alloc          sqlbase.DatumAlloc
	rowAlloc       sqlbase.EncDatumRowAlloc

	input      RowSource
	inputTypes []types.T
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols columns
	// indexFilter is the filter expression from the index's scanNode in a
	// lookup join. It is applied to index rows before they are joined to the
	// input rows.
	indexFilter exprHelper
	// indexTypes is an array of the types of the index we're looking up into,
	// in the order of the columns in that index.
	indexTypes []types.T
	// indexDirs is an array of the directions for the index's key columns.
	indexDirs []sqlbase.IndexDescriptor_Direction

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int

	// State variables for each batch of input rows.
	inputRows               sqlbase.EncDatumRows
	keyToInputRowIndices    map[string][]int
	inputRowIdxToOutputRows []sqlbase.EncDatumRows
	finalLookupBatch        bool
	toEmit                  sqlbase.EncDatumRows

	// A few scratch buffers, to avoid re-allocating.
	lookupRows  []lookupRow
	indexKeyRow sqlbase.EncDatumRow
}

// lookupRow represents an index key and the corresponding index row.
type lookupRow struct {
	key string
	row sqlbase.EncDatumRow
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
	if spec.Visibility != distsqlpb.ScanVisibility_PUBLIC {
		return nil, errors.AssertionFailedf("joinReader specified with visibility %+v", spec.Visibility)
	}

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

	var columnIDs []sqlbase.ColumnID
	columnIDs, jr.indexDirs = jr.index.FullColumnIDs()
	indexCols := make([]uint32, len(columnIDs))
	jr.indexTypes = make([]types.T, len(columnIDs))
	columnTypes := jr.desc.ColumnTypesWithMutations(true)
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
	if err := jr.indexFilter.init(spec.IndexFilterExpr, columnTypes, jr.evalCtx); err != nil {
		return nil, err
	}

	collectingStats := false
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		collectingStats = true
	}

	if isSecondary && !jr.neededRightCols().SubsetOf(getIndexColSet(jr.index, jr.colIdxMap)) {
		return nil, errors.Errorf("joinreader index does not cover all columns")
	}

	_, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		jr.neededRightCols(), false /* isCheck */, &jr.alloc,
		distsqlpb.ScanVisibility_PUBLIC,
	)
	if err != nil {
		return nil, err
	}
	jr.fetcherInput = &rowFetcherWrapper{Fetcher: &jr.fetcher}
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
		case jrCollectingOutputRows:
			jr.runningState = jr.collectOutputRows()
		case jrEmittingRows:
			jr.runningState, row = jr.emitRow()
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
	if len(jr.inputRowIdxToOutputRows) >= len(jr.inputRows) {
		jr.inputRowIdxToOutputRows = jr.inputRowIdxToOutputRows[:len(jr.inputRows)]
		for i := range jr.inputRowIdxToOutputRows {
			jr.inputRowIdxToOutputRows[i] = nil
		}
	} else {
		jr.inputRowIdxToOutputRows = make([]sqlbase.EncDatumRows, len(jr.inputRows))
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
			spans = append(spans, span)
		}
		jr.keyToInputRowIndices[string(span.Key)] = append(inputRowIndices, i)
	}
	if len(spans) == 0 {
		// All of the input rows were filtered out. Skip the index lookup.
		jr.finalLookupBatch = true
		return jrCollectingOutputRows, nil
	}
	err := jr.fetcher.StartScan(
		jr.Ctx, jr.flowCtx.txn, spans, false /* limitBatches */, 0, /* limitHint */
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
	jr.lookupRows = jr.lookupRows[:0]
	nCols := len(jr.lookupCols)

	// Read the next batch of index rows.
	for len(jr.lookupRows) < jr.batchSize {
		// Construct a "partial key" of nCols, so we can match the key format that
		// was stored in our keyToInputRowIndices map. This matches the format that
		// is output in jr.generateSpan.
		key, err := jr.fetcher.PartialKey(nCols)
		if err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		}

		indexRow, meta := jr.fetcherInput.Next()
		if meta != nil {
			jr.MoveToDraining(scrub.UnwrapScrubError(meta.Err))
			return jrStateUnknown, jr.DrainHelper()
		}
		if indexRow == nil {
			// Done with this input batch.
			jr.finalLookupBatch = true
			break
		}

		jr.lookupRows = append(jr.lookupRows, lookupRow{key: string(key), row: jr.rowAlloc.CopyRow(indexRow)})
	}

	// Iterate over the lookup results, map them to the input rows, and emit the
	// rendered rows.
	isJoinTypeLeftSemiJoin := jr.joinType == sqlbase.LeftSemiJoin
	for _, lookupRow := range jr.lookupRows {
		if jr.indexFilter.expr != nil {
			// Apply index filter.
			res, err := jr.indexFilter.evalFilter(lookupRow.row)
			if err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
			if !res {
				continue
			}
		}
		for _, inputRowIdx := range jr.keyToInputRowIndices[lookupRow.key] {
			// Only add to output if joinType is not LeftSemiJoin or if we have not gotten the match
			if !isJoinTypeLeftSemiJoin || len(jr.inputRowIdxToOutputRows[inputRowIdx]) == 0 {
				renderedRow, err := jr.render(jr.inputRows[inputRowIdx], lookupRow.row)
				if err != nil {
					jr.MoveToDraining(err)
					return jrStateUnknown, jr.DrainHelper()
				}
				if renderedRow != nil {
					rowCopy := jr.out.rowAlloc.CopyRow(renderedRow)
					jr.inputRowIdxToOutputRows[inputRowIdx] = append(
						jr.inputRowIdxToOutputRows[inputRowIdx], rowCopy)
				}
			}
		}
	}

	if jr.finalLookupBatch {
		return jrCollectingOutputRows, nil
	}

	return jrEmittingRows, nil
}

// collectOutputRows iterates over jr.inputRowIdxToOutputRows and adds output
// rows to jr.Emit, rendering rows for unmatched inputs if the join is a left
// outer join or left anti join, while preserving the input order.
func (jr *joinReader) collectOutputRows() joinReaderState {
	for i, outputRows := range jr.inputRowIdxToOutputRows {
		if len(outputRows) == 0 {
			if jr.joinType == sqlbase.LeftOuterJoin || jr.joinType == sqlbase.LeftAntiJoin {
				if row := jr.renderUnmatchedRow(jr.inputRows[i], leftSide); row != nil {
					jr.toEmit = append(jr.toEmit, jr.out.rowAlloc.CopyRow(row))
				}
			}
		} else {
			if jr.joinType != sqlbase.LeftAntiJoin {
				jr.toEmit = append(jr.toEmit, outputRows...)
			}
		}
	}
	return jrEmittingRows
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (joinReaderState, sqlbase.EncDatumRow) {
	if len(jr.toEmit) == 0 {
		if jr.finalLookupBatch {
			// Ready for another input batch. Reset state.
			jr.inputRows = jr.inputRows[:0]
			jr.keyToInputRowIndices = make(map[string][]int)
			jr.finalLookupBatch = false
			return jrReadingInput, nil
		}
		// Process the next index lookup batch.
		return jrPerformingLookup, nil
	}
	row := jr.toEmit[0]
	jr.toEmit = jr.toEmit[1:]
	return jrEmittingRows, row
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
	jr.fetcherInput.Start(ctx)
	jr.runningState = jrReadingInput
	return jr.StartInternal(ctx, joinReaderProcName)
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
	ils, ok := getInputStats(jr.flowCtx, jr.fetcherInput)
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
