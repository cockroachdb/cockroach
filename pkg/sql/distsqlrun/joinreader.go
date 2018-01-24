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

// A joinReader can preform an index join or a lookup join. Specifying a
// non-empty value for eqCols indicates it is a lookup join.
//
// For an index join, the input is subset of rows of the `desc` table where the
// first n columns correspond to the n columns of the index. The join reader
// finds the full row based on the primary index.
//
// For a lookup join, the input is another table and the columns which match the
// index are specified in the `eqCols` field.
type joinReader struct {
	processorBase

	desc  sqlbase.TableDescriptor
	index *sqlbase.IndexDescriptor

	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc

	input      RowSource
	inputTypes []sqlbase.ColumnType
	eqCols     []sqlbase.ColumnID
}

var _ Processor = &joinReader{}

func newJoinReader(
	flowCtx *FlowCtx,
	spec *JoinReaderSpec,
	input RowSource,
	eqCols []sqlbase.ColumnID,
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
		eqCols:     eqCols,
	}

	size := len(spec.Table.Columns) + len(input.OutputTypes())
	types := make([]sqlbase.ColumnType, size)
	offset := len(spec.Table.Columns)
	for i, col := range spec.Table.Columns {
		types[i] = col.Type
	}
	for i, t := range input.OutputTypes() {
		types[offset+i] = t
	}

	if err := jr.init(post, types, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}

	var err error
	fetchedColumns := jr.leftCols()
	jr.index, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), false, /* reverse */
		fetchedColumns, false /* isCheck */, &jr.alloc,
	)
	if err != nil {
		return nil, err
	}

	// TODO(radu): verify the input types match the index key types

	return jr, nil
}

func (jr *joinReader) leftCols() util.FastIntSet {
	fetchedColumns := jr.out.neededColumns().Copy()
	outputCols := util.MakeFastIntSet()
	outputCols.AddRange(0, len(jr.desc.Columns)-1)
	fetchedColumns.IntersectionWith(outputCols)
	return fetchedColumns
}

func (jr *joinReader) generateKey(
	row sqlbase.EncDatumRow, alloc *sqlbase.DatumAlloc, primaryKeyPrefix []byte,
) (roachpb.Key, error) {
	index := jr.index
	numKeyCols := len(index.ColumnIDs)
	if len(row) < numKeyCols {
		return nil, errors.Errorf("joinReader input has %d columns, expected at least %d",
			len(row))
	}
	// There may be extra values on the row, e.g. to allow an ordered synchronizer
	// to interleave multiple input streams.
	keyRow := make(sqlbase.EncDatumRow, numKeyCols)
	types := make([]sqlbase.ColumnType, numKeyCols)
	if len(jr.eqCols) == 0 {
		keyRow = row[:numKeyCols]
		types = jr.inputTypes[:numKeyCols]
	} else {
		if len(jr.eqCols) != numKeyCols {
			return nil, errors.Errorf("%d equality columns specified, expecting exactly %d",
				len(jr.eqCols), numKeyCols)
		}
		for i, id := range jr.eqCols {
			keyRow[i] = row[id]
			types[i] = jr.inputTypes[id]
		}
	}

	return sqlbase.MakeKeyFromEncDatums(types, keyRow, &jr.desc, index, primaryKeyPrefix, alloc)
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

	for {
		// TODO(radu): figure out how to send smaller batches if the source has
		// a soft limit (perhaps send the batch out if we don't get a result
		// within a certain amount of time).
		spanCount := 0
		for spans = spans[:0]; len(spans) < joinReaderBatchSize; spanCount++ {
			row, meta := jr.input.Next()
			if meta != nil {
				if meta.Err != nil {
					return meta.Err
				}
				if !emitHelper(ctx, &jr.out, nil /* row */, meta, jr.input) {
					return nil
				}
				continue
			}
			if row == nil {
				if spanCount == 0 {
					// No fetching needed since we have collected no spans and
					// the input has signaled that no more records are coming.
					jr.out.Close()
					return nil
				}
				break
			}

			key, err := jr.generateKey(row, &alloc, primaryKeyPrefix)
			if err != nil {
				return err
			}

			span := roachpb.Span{
				Key:    key,
				EndKey: key.PrefixEnd(),
			}
			// TODO: Should only need to do this in the else case of the
			// condition below, but the final condition needs to be fixed.

			if jr.isIndexJoin() {
				spans = append(spans, span)
			} else {
				// Lookup join: lookup every row if information from the input
				// cols needed.
				// TODO(pbardea): Prefetching optimization below will not work
				// with this implementation of lookupJoins.
				if err := jr.indexLookup(ctx, txn, roachpb.Spans{span}, &row); err != nil {
					return err
				}
			}

		}

		// If input columns are not needed, batch input rows as spans.
		if jr.isIndexJoin() {
			// TODO(radu): we are consuming all results from a fetch before starting
			// the next batch. We could start the next batch early while we are
			// outputting rows.
			if err := jr.indexLookup(ctx, txn, spans, nil /* row */); err != nil {
				return err
			}
		}

		if spanCount != joinReaderBatchSize {
			// This was the last batch.
			sendTraceData(ctx, jr.out.output)
			jr.out.Close()
			return nil
		}
	}
}

func (jr *joinReader) isIndexJoin() bool {
	return len(jr.eqCols) == 0
}

func (jr *joinReader) indexLookup(ctx context.Context, txn *client.Txn, spans roachpb.Spans, row *sqlbase.EncDatumRow) error {
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	err := jr.fetcher.StartScan(ctx, txn, spans, false /* no batch limits */, 0, false /* traceKV */)
	if err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return err
	}

	for {
		indexRow, _, _, err := jr.fetcher.NextRow(ctx)
		if err != nil {
			err = scrub.UnwrapScrubError(err)
			return err
		}
		if indexRow == nil {
			// Done with this batch.
			break
		}

		rendered := indexRow
		if row != nil {
			rendered = append(indexRow, *row...)
			if err != nil {
				return err
			}
		}

		// Emit the row; stop if no more rows are needed.
		if !emitHelper(ctx, &jr.out, rendered, nil /* meta */, jr.input) {
			return nil
		}
	}
	return nil
}

// Run is part of the processor interface.
func (jr *joinReader) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx := log.WithLogTagInt(jr.flowCtx.Ctx, "JoinReader", int(jr.desc.ID))
	ctx, span := processorSpan(ctx, "join reader")
	defer tracing.FinishSpan(span)

	err := jr.mainLoop(ctx)
	if err != nil {
		DrainAndClose(ctx, jr.out.output, err /* cause */, jr.input)
	}
}
