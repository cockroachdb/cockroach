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
	"fmt"
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
	processorBase

	desc  sqlbase.TableDescriptor
	index *sqlbase.IndexDescriptor

	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc

	input      RowSource
	inputTypes []sqlbase.ColumnType
	lookupCols columns
}

var _ Processor = &joinReader{}

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

	types := jr.getOutputTypes()

	if err := jr.init(post, types, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}

	var err error
	fmt.Println(jr.rightOutputCols())
	jr.index, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), false, /* reverse */
		jr.rightOutputCols(), false /* isCheck */, &jr.alloc,
	)
	if err != nil {
		return nil, err
	}

	// TODO(radu): verify the input types match the index key types

	return jr, nil
}

// For an index join only the right side (primary index side) is emitted.
// For a lookup join, both sides are included in the output.
func (jr *joinReader) getOutputTypes() []sqlbase.ColumnType {
	numTypes := len(jr.desc.Columns)
	if jr.isLookupJoin() {
		numTypes += len(jr.input.OutputTypes())
	}

	types := make([]sqlbase.ColumnType, numTypes)
	i := 0
	if jr.isLookupJoin() {
		for _, t := range jr.input.OutputTypes() {
			types[i] = t
			i++
		}
	}
	for _, col := range jr.desc.Columns {
		types[i] = col.Type
		i++
	}
	return types
}

// Returns the columns from the right relation that will be outputted.
func (jr *joinReader) rightOutputCols() util.FastIntSet {
	offset := 0
	if jr.isLookupJoin() {
		offset = len(jr.input.OutputTypes())
	}
	neededCols := jr.out.neededColumns()
	neededCols = neededCols.Shift(-1 * offset)
	outputCols := util.MakeFastIntSet()
	outputCols.AddRange(0, len(jr.desc.Columns)-1)
	return neededCols.Intersection(outputCols)
}

func (jr *joinReader) generateKey(
	row sqlbase.EncDatumRow, alloc *sqlbase.DatumAlloc, primaryKeyPrefix []byte,
) (roachpb.Key, error) {
	index := jr.index
	numKeyCols := len(index.ColumnIDs)
	if len(row) < numKeyCols {
		return nil, errors.Errorf("joinReader input has %d columns, expected at least %d",
			len(row), numKeyCols)
	}
	// There may be extra values on the row, e.g. to allow an ordered synchronizer
	// to interleave multiple input streams.
	keyRow := make(sqlbase.EncDatumRow, numKeyCols)
	types := make([]sqlbase.ColumnType, numKeyCols)
	if len(jr.lookupCols) == 0 {
		keyRow = row[:numKeyCols]
		types = jr.inputTypes[:numKeyCols]
	} else {
		if len(jr.lookupCols) != numKeyCols {
			return nil, errors.Errorf("%d equality columns specified, expecting exactly %d",
				len(jr.lookupCols), numKeyCols)
		}
		for i, id := range jr.lookupCols {
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
		rowCount := 0
		for spans = spans[:0]; len(spans) < joinReaderBatchSize; {
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
				if rowCount == 0 {
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

			if jr.isLookupJoin() {
				// TODO(pbardea): Consider possibly buffering rows in a map
				// from string(key) to rows and can lookup all the spans at
				// once.
				earlyExit, err := jr.indexLookup(ctx, txn, roachpb.Spans{span}, row)
				if err != nil {
					return err
				} else if earlyExit {
					return nil
				}
			} else {
				spans = append(spans, span)
			}
			rowCount++
		}

		if !jr.isLookupJoin() {
			// TODO(radu): we are consuming all results from a fetch before starting
			// the next batch. We could start the next batch early while we are
			// outputting rows.
			if earlyExit, err := jr.indexLookup(ctx, txn, spans, nil /* row */); err != nil {
				return err
			} else if earlyExit {
				return nil
			}
		}

		if rowCount != joinReaderBatchSize {
			// This was the last batch.
			sendTraceData(ctx, jr.out.output)
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

// Index lookup iterates through all matches of the given `spans`. A `row`
// which corrresponds to the given span (of size 1) can be provided and
// then the lookup will emit the concatenation of the rows from both tables.
func (jr *joinReader) indexLookup(
	ctx context.Context, txn *client.Txn, spans roachpb.Spans, row sqlbase.EncDatumRow,
) (bool, error) {
	// TODO(radu,andrei,knz): set the traceKV flag when requested by the session.
	err := jr.fetcher.StartScan(ctx, txn, spans, false /* no batch limits */, 0, false /* traceKV */)
	if err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return true, err
	}

	for {
		indexRow, _, _, err := jr.fetcher.NextRow(ctx)
		if err != nil {
			err = scrub.UnwrapScrubError(err)
			return true, err
		}
		if indexRow == nil {
			// Done with this batch.
			break
		}
		var renderedRow sqlbase.EncDatumRow
		if row != nil {
			renderedRow = append(renderedRow, row...)
		}
		renderedRow = append(renderedRow, indexRow...)

		// Emit the row; stop if no more rows are needed.
		if !emitHelper(ctx, &jr.out, renderedRow, nil /* meta */, jr.input) {
			return true, nil
		}
	}

	return false, nil
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
