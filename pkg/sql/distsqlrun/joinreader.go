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
	"sync"

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

// A joinReader can perform an index join or a lookup join. Specifying a
// non-empty value for lookupCols indicates it is a lookup join.
//
// For an index join, the `input`, considered to be the left side, is subset of
// rows of the `desc` table where the first n columns correspond to the n
// columns of the index. These come from a secondary index. The right side comes
// from a primary index.
//
// For a lookup join, the input is another table and the columns which match the
// index are specified in the `lookupCols` field. If the index is a secondary
// index which does not cover the needed output columns, a second lookup will be
// performed to retrieve those columns from the primary index.
type joinReader struct {
	joinerBase

	desc      sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	colIdxMap map[sqlbase.ColumnID]int

	fetcher  sqlbase.RowFetcher
	alloc    sqlbase.DatumAlloc
	rowAlloc sqlbase.EncDatumRowAlloc

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
	primaryFetcher     *sqlbase.RowFetcher
	primaryColumnTypes []sqlbase.ColumnType
	primaryKeyPrefix   []byte

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSize int
}

var _ Processor = &joinReader{}

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
		desc:       spec.Table,
		input:      input,
		inputTypes: input.OutputTypes(),
		lookupCols: spec.LookupColumns,
		batchSize:  joinReaderBatchSize,
	}

	var err error
	jr.index, _, err = jr.desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	jr.colIdxMap = jr.desc.ColumnIdxMap()
	if jr.isLookupJoin() {
		indexCols := make([]uint32, len(jr.index.ColumnIDs))
		for i, columnID := range jr.index.ColumnIDs {
			indexCols[i] = uint32(columnID)
		}
		if err := jr.joinerBase.init(
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
			procStateOpts{}, // joinReader doesn't implement RowSource and so doesn't use it.
		); err != nil {
			return nil, err
		}
		if spec.IndexFilterExpr.Expr != "" {
			err := jr.indexFilter.init(spec.IndexFilterExpr, jr.desc.ColumnTypes(), jr.evalCtx)
			if err != nil {
				return nil, err
			}
		}
	} else {
		if err := jr.processorBase.init(
			post, jr.desc.ColumnTypes(), flowCtx, processorID, output,
			procStateOpts{}, // joinReader doesn't implement RowSource and so doesn't use it.
		); err != nil {
			return nil, err
		}
	}

	// neededIndexColumns is the set of columns we need to fetch from jr.index.
	var neededIndexColumns util.FastIntSet

	if jr.index == &jr.desc.PrimaryIndex ||
		jr.neededRightCols().SubsetOf(getIndexColSet(jr.index, jr.colIdxMap)) {
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
	}
	_, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		neededIndexColumns, false /* isCheck */, &jr.alloc,
	)
	if err != nil {
		return nil, err
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
	if !jr.isLookupJoin() {
		return neededCols
	}

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
func (jr *joinReader) generateKey(
	row sqlbase.EncDatumRow, alloc *sqlbase.DatumAlloc, keyPrefix []byte, lookupCols columns,
) (roachpb.Key, error) {
	index := jr.index
	numKeyCols := len(index.ColumnIDs)
	if !jr.isLookupJoin() && len(row) < numKeyCols {
		return nil, errors.Errorf("joinReader input has %d columns, expected at least %d",
			len(row), numKeyCols)
	}
	// There may be extra values on the row, e.g. to allow an ordered synchronizer
	// to interleave multiple input streams.
	// Will need at most numKeyCols.
	keyRow := make(sqlbase.EncDatumRow, 0, numKeyCols)
	types := make([]sqlbase.ColumnType, 0, numKeyCols)
	if jr.isLookupJoin() {
		if len(lookupCols) > numKeyCols {
			return nil, errors.Errorf("%d equality columns specified, expecting at most %d",
				len(jr.lookupCols), numKeyCols)
		}
		for _, id := range lookupCols {
			keyRow = append(keyRow, row[id])
			types = append(types, jr.inputTypes[id])
		}
	} else {
		keyRow = row[:numKeyCols]
		types = jr.inputTypes[:numKeyCols]
	}

	return sqlbase.MakeKeyFromEncDatums(types, keyRow, &jr.desc, index, keyPrefix, alloc)
}

func (jr *joinReader) pushTrailingMeta(ctx context.Context) {
	sendTraceData(ctx, jr.out.output)
	sendTxnCoordMetaMaybe(jr.flowCtx.txn, jr.out.output)
}

// mainLoop runs the mainLoop and returns any error.
//
// If no error is returned, the input has been drained and the output has been
// closed. If an error is returned, the input hasn't been drained; the caller
// should drain and close the output. The caller should also pass the returned
// error to the consumer.
func (jr *joinReader) mainLoop(ctx context.Context) error {
	primaryKeyPrefix := sqlbase.MakeIndexKeyPrefix(&jr.desc, jr.index.ID)

	var alloc sqlbase.DatumAlloc
	var rows []sqlbase.EncDatumRow
	var spans roachpb.Spans

	txn := jr.flowCtx.txn
	if txn == nil {
		log.Fatalf(ctx, "joinReader outside of txn")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	for {
		// TODO(radu): figure out how to send smaller batches if the source has
		// a soft limit (perhaps send the batch out if we don't get a result
		// within a certain amount of time).
		rowIdx := 0
		rows = rows[:0]
		spans = spans[:0]
		spanToRowIndices := make(map[string][]int, joinReaderBatchSize)
		for len(spans) < jr.batchSize {
			row, meta := jr.input.Next()
			if meta != nil {
				if meta.Err != nil {
					return meta.Err
				}
				if !emitHelper(ctx, &jr.out, nil /* row */, meta, jr.pushTrailingMeta, jr.input) {
					return nil
				}
				continue
			}
			if row == nil {
				if len(spans) == 0 {
					// No fetching needed since we have collected no spans and
					// the input has signaled that no more records are coming.
					jr.out.Close()
					return nil
				}
				break
			}

			key, err := jr.generateKey(row, &alloc, primaryKeyPrefix, jr.lookupCols)
			if err != nil {
				return err
			}

			span := roachpb.Span{
				Key:    key,
				EndKey: key.PrefixEnd(),
			}
			if jr.isLookupJoin() {
				rows = append(rows, row)
				if spanToRowIndices[key.String()] == nil {
					spans = append(spans, span)
				}
				spanToRowIndices[key.String()] = append(spanToRowIndices[key.String()], rowIdx)
				rowIdx++
			} else {
				spans = append(spans, span)
			}
		}

		// TODO(radu): we are consuming all results from a fetch before starting
		// the next batch. We could start the next batch early while we are
		// outputting rows.
		var earlyExit bool
		var err error
		if jr.isLookupJoin() {
			earlyExit, err = jr.lookupJoinLookup(ctx, txn, spans, rows, spanToRowIndices)
		} else {
			earlyExit, err = jr.indexJoinLookup(ctx, txn, spans)
		}
		if err != nil {
			return err
		} else if earlyExit {
			return nil
		}

		if len(spans) != jr.batchSize {
			// This was the last batch.
			jr.pushTrailingMeta(ctx)
			jr.out.Close()
			return nil
		}
	}
}

// A joinReader performs a lookup join is lookup columns were specified,
// otherwise it performs an index join.
func (jr *joinReader) isLookupJoin() bool {
	return len(jr.lookupCols) > 0
}

// indexJoinLookup performs an index lookup for the purposes of an index join.
// It fetches the specified spans from the primary index and emits the results.
//
// Returns false if more rows need to be produced, true otherwise. If true is
// returned, both the inputs and the output have been drained and closed, except
// if an error is returned.
func (jr *joinReader) indexJoinLookup(
	ctx context.Context, txn *client.Txn, spans roachpb.Spans,
) (bool, error) {
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	err := jr.fetcher.StartScan(
		ctx, txn, spans, false /* limitBatches */, 0 /* limitHint */, false /* traceKV */)
	if err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return true, err
	}
	for {
		row, _, _, err := jr.fetcher.NextRow(ctx)
		if err != nil {
			err = scrub.UnwrapScrubError(err)
			return true, err
		}
		if row == nil {
			// Done with this batch.
			break
		}
		// Emit the row; stop if no more rows are needed.
		if !emitHelper(ctx, &jr.out, row, nil /* meta */, jr.pushTrailingMeta, jr.input) {
			return true, nil
		}
	}
	return false, nil
}

// lookupJoinLookup performs an index lookup for the purposes of a lookup join.
// `spans` is the set of spans which should be fetched from the index. `rows` is
// the corresponding input rows. `spanToRowIndices` maps span keys onto the
// corresponding `rows` indices.
//
// Note that if jr.primaryFetcher is non-nil, this function will perform two
// lookups: one to fetch rows from a secondary index, and a second to fetch the
// corresponding primary rows. This is for lookup joins on a secondary index
// which does not cover all the needed output columns. (Due to batching it may
// actually perform multiple primary lookups.)
//
// Returns false if more rows need to be produced, true otherwise. If true is
// returned, both the inputs and the output have been drained and closed, except
// if an error is returned.
func (jr *joinReader) lookupJoinLookup(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	rows []sqlbase.EncDatumRow,
	spanToRowIndices map[string][]int,
) (bool, error) {
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	err := jr.fetcher.StartScan(
		ctx, txn, spans, false /* limitBatches */, 0 /* limitHint */, false /* traceKV */)
	if err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return true, err
	}

	// If this is an outer join, track emitted rows so we can emit unmatched rows
	// at the end. (Note: we track emitted rows rather than non-emitted since
	// there are multiple reasons a row might not be emitted: the index lookup
	// might return no corresponding rows, or all the rows it returns might fail
	// the ON condition, which is applied in the render step.)
	var emitted []bool
	if jr.joinType == sqlbase.LeftOuterJoin {
		emitted = make([]bool, len(rows))
	}

	// lookupRow represents an index key and the corresponding row.
	type lookupRow struct {
		key string
		row sqlbase.EncDatumRow
	}
	lookupRows := make([]lookupRow, 0, joinReaderBatchSize)

	// The index scan may have returned more rows than len(spans), so process the
	// results in batches.
	for {
		// Get the next batch of lookup results.
		lookupRows = lookupRows[:0]
		for len(lookupRows) < joinReaderBatchSize {
			key := jr.fetcher.IndexKeyString(len(jr.lookupCols))
			row, _, _, err := jr.fetcher.NextRow(ctx)
			if err != nil {
				err = scrub.UnwrapScrubError(err)
				return true, err
			}
			if row == nil {
				// Done with this batch.
				break
			}
			lookupRows = append(lookupRows, lookupRow{key, jr.rowAlloc.CopyRow(row)})
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
			primaryRows, err := jr.primaryLookup(ctx, txn, secondaryIndexRows)
			if err != nil {
				return false, err
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
					return false, err
				}
				if !res {
					continue
				}
			}
			for _, rowIdx := range spanToRowIndices[lookupRow.key] {
				renderedRow, err := jr.render(rows[rowIdx], lookupRow.row)
				if err != nil {
					return false, err
				}
				if renderedRow == nil {
					continue
				}

				// Emit the row; stop if no more rows are needed.
				if !emitHelper(
					ctx, &jr.out, renderedRow, nil /* meta */, jr.pushTrailingMeta, jr.input,
				) {
					return true, nil
				}
				if emitted != nil {
					emitted[rowIdx] = true
				}
			}
		}

		if len(lookupRows) < joinReaderBatchSize {
			// This was the last batch.
			break
		}
	}

	if emitted != nil {
		// Emit unmatched rows.
		for i := 0; i < len(rows); i++ {
			if !emitted[i] {
				renderedRow := jr.renderUnmatchedRow(rows[i], leftSide)
				if !emitHelper(
					ctx, &jr.out, renderedRow, nil /* meta */, jr.pushTrailingMeta, jr.input,
				) {
					return true, nil
				}
			}
		}
	}
	return false, nil
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
		ctx, txn, spans, false /* limitBatches */, 0 /* limitHint */, false /* traceKV */)
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
		row, _, _, err := jr.primaryFetcher.NextRow(ctx)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, errors.Errorf("expected %d rows but found %d", batchSize, i)
		}
		outRows[rowIdx] = jr.rowAlloc.CopyRow(row)
	}

	// Verify that we consumed all the fetched rows.
	nextRow, _, _, err := jr.primaryFetcher.NextRow(ctx)
	if err != nil {
		return nil, err
	}
	if nextRow != nil {
		return nil, errors.Errorf("expected %d rows but found more", batchSize)
	}

	return outRows, nil
}

// Run is part of the processor interface.
func (jr *joinReader) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	jr.input.Start(ctx)
	ctx = jr.startInternal(ctx, joinReaderProcName)
	defer tracing.FinishSpan(jr.span)

	err := jr.mainLoop(ctx)
	if err != nil {
		DrainAndClose(ctx, jr.out.output, err /* cause */, jr.pushTrailingMeta, jr.input)
	}
}
