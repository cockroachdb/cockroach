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
// index are specified in the `lookupCols` field.
type joinReader struct {
	joinerBase

	desc  sqlbase.TableDescriptor
	index *sqlbase.IndexDescriptor

	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc

	input      RowSource
	inputTypes []sqlbase.ColumnType
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols columns

	renderedRow sqlbase.EncDatumRow
}

var _ Processor = &joinReader{}

const joinReaderProcName = "join reader"

func newJoinReader(
	flowCtx *FlowCtx,
	spec *JoinReaderSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*joinReader, error) {
	if spec.IndexIdx != 0 {
		// TODO(radu): for now we only support joining with the primary index
		return nil, errors.Errorf("join with index not implemented")
	}

	jr := &joinReader{
		desc:       spec.Table,
		input:      input,
		inputTypes: input.OutputTypes(),
		lookupCols: spec.LookupColumns,
	}

	var err error
	jr.index, _, err = jr.desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	indexCols := make([]uint32, len(jr.index.ColumnIDs))
	for i := 0; i < len(jr.index.ColumnIDs); i++ {
		indexCols[i] = uint32(jr.index.ColumnIDs[i])
	}
	if jr.isLookupJoin() {
		if err := jr.joinerBase.init(
			flowCtx,
			input.OutputTypes(),
			jr.desc.ColumnTypes(),
			sqlbase.InnerJoin,
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
	} else {
		if err := jr.processorBase.init(
			post, jr.desc.ColumnTypes(), flowCtx, output,
			procStateOpts{}, // joinReader doesn't implement RowSource and so doesn't use it.
		); err != nil {
			return nil, err
		}
	}

	_, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), false, /* reverse */
		jr.rowFetcherColumns(), false /* isCheck */, &jr.alloc,
	)
	if err != nil {
		return nil, err
	}

	jr.renderedRow = make(sqlbase.EncDatumRow, 0, len(jr.out.outputCols))

	// TODO(radu): verify the input types match the index key types

	return jr, nil
}

// The rows that will be non-empty when fetched from the row fetcher. Used so
// extra information doesn't need to be sent over the wire.
func (jr *joinReader) rowFetcherColumns() util.FastIntSet {
	neededCols := jr.out.neededColumns()
	if !jr.isLookupJoin() {
		return neededCols
	}

	// Get the columns from the right side of the join and shift them over by
	// the size of the left side so the right side starts at 0.
	rowFetcherColumns := util.MakeFastIntSet()
	for i, ok := neededCols.Next(len(jr.inputTypes)); ok; i, ok = neededCols.Next(i + 1) {
		rowFetcherColumns.Add(i - len(jr.inputTypes))
	}

	// Add columns needed by OnExpr.
	for _, v := range jr.onCond.vars.GetIndexedVars() {
		rightIdx := v.Idx - len(jr.inputTypes)
		if rightIdx >= 0 {
			rowFetcherColumns.Add(rightIdx)
		}
	}

	return rowFetcherColumns
}

// Generate a key to create a span for a given row.
// If lookup columns are specified will use those to collect the relevant
// columns. Otherwise the first rows are assumed to correspond with the index.
func (jr *joinReader) generateKey(
	row sqlbase.EncDatumRow, alloc *sqlbase.DatumAlloc, primaryKeyPrefix []byte, lookupCols columns,
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

	return sqlbase.MakeKeyFromEncDatums(types, keyRow, &jr.desc, index, primaryKeyPrefix, alloc)
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
	spans := make(roachpb.Spans, 0, joinReaderBatchSize)

	txn := jr.flowCtx.txn
	if txn == nil {
		log.Fatalf(ctx, "joinReader outside of txn")
	}

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	spanToRows := make(map[string][]sqlbase.EncDatumRow)
	for {
		// TODO(radu): figure out how to send smaller batches if the source has
		// a soft limit (perhaps send the batch out if we don't get a result
		// within a certain amount of time).
		for spans = spans[:0]; len(spans) < joinReaderBatchSize; {
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
				if spanToRows[key.String()] == nil {
					spans = append(spans, span)
				}
				spanToRows[key.String()] = append(spanToRows[key.String()], row)
			} else {
				spans = append(spans, span)
			}
		}

		// TODO(radu): we are consuming all results from a fetch before starting
		// the next batch. We could start the next batch early while we are
		// outputting rows.
		if earlyExit, err := jr.indexLookup(ctx, txn, spans, spanToRows); err != nil {
			return err
		} else if earlyExit {
			return nil
		}

		if len(spans) != joinReaderBatchSize {
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

// Index lookup iterates through all matches of the given spans and emits the
// corresponding row.
//
// Returns false if more rows need to be produced, true otherwise. If true is
// returned, both the inputs and the output have been drained and closed, except
// if an error is returned.
func (jr *joinReader) indexLookup(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	spanToRows map[string][]sqlbase.EncDatumRow,
) (bool, error) {
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	err := jr.fetcher.StartScan(
		ctx,
		txn,
		spans,
		false, /* no batch limits */
		0,
		false, /* traceKV */
	)
	if err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return true, err
	}
	for {
		indexKey := jr.fetcher.IndexKeyString(len(jr.lookupCols))
		indexRow, _, _, err := jr.fetcher.NextRow(ctx)
		if err != nil {
			err = scrub.UnwrapScrubError(err)
			return true, err
		}
		if indexRow == nil {
			// Done with this batch.
			break
		}

		rows := spanToRows[indexKey]

		if !jr.isLookupJoin() {
			// Emit the row; stop if no more rows are needed.
			if !emitHelper(ctx, &jr.out, indexRow, nil /* meta */, jr.pushTrailingMeta, jr.input) {
				return true, nil
			}
		} else {
			for _, row := range rows {
				jr.renderedRow = jr.renderedRow[:0]
				if jr.renderedRow, err = jr.render(row, indexRow); err != nil {
					return false, err
				}
				if jr.renderedRow == nil {
					continue
				}

				// Emit the row; stop if no more rows are needed.
				if !emitHelper(
					ctx, &jr.out, jr.renderedRow, nil /* meta */, jr.pushTrailingMeta, jr.input,
				) {
					return true, nil
				}
			}
		}

	}

	return false, nil
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
