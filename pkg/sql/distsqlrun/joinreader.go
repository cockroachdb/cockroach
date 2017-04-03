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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// TODO(radu): we currently create one batch at a time and run the KV operations
// on this node. In the future we may want to build separate batches for the
// nodes that "own" the respective ranges, and send out flows on those nodes.
const joinReaderBatchSize = 100

type joinReader struct {
	flowCtx *FlowCtx

	desc  sqlbase.TableDescriptor
	index *sqlbase.IndexDescriptor

	fetcher sqlbase.RowFetcher

	input RowSource
	out   procOutputHelper
}

var _ processor = &joinReader{}

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
		flowCtx: flowCtx,
		desc:    spec.Table,
		input:   input,
	}

	types := make([]sqlbase.ColumnType, len(spec.Table.Columns))
	for i := range types {
		types[i] = spec.Table.Columns[i].Type
	}

	if err := jr.out.init(post, types, &flowCtx.evalCtx, output); err != nil {
		return nil, err
	}

	var err error
	jr.index, _, err = initRowFetcher(
		&jr.fetcher, &jr.desc, int(spec.IndexIdx), false /* reverse */, jr.out.neededColumns(),
	)
	if err != nil {
		return nil, err
	}

	return jr, nil
}

func (jr *joinReader) generateKey(
	row sqlbase.EncDatumRow, alloc *sqlbase.DatumAlloc, primaryKeyPrefix []byte,
) (roachpb.Key, error) {
	index := jr.index
	if len(row) < len(index.ColumnIDs) {
		return nil, errors.Errorf("joinReader input has %d columns, expected at least %d",
			len(row), len(jr.desc.PrimaryIndex.ColumnIDs))
	}
	row = row[:len(index.ColumnIDs)]

	return sqlbase.MakeKeyFromEncDatums(row, &jr.desc, index, primaryKeyPrefix, alloc)
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

	txn := jr.flowCtx.setupTxn()

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	for {
		// TODO(radu): figure out how to send smaller batches if the source has
		// a soft limit (perhaps send the batch out if we don't get a result
		// within a certain amount of time).
		for spans = spans[:0]; len(spans) < joinReaderBatchSize; {
			row, meta := jr.input.Next()
			if !meta.Empty() {
				if meta.Err != nil {
					return meta.Err
				}
				if !emitHelper(ctx, &jr.out, nil /* row */, meta, jr.input) {
					return nil
				}
				continue
			}
			if row == nil {
				if len(spans) == 0 {
					// No fetching needed since we have collected no spans and
					// the input has signalled that no more records are coming.
					jr.out.close()
					return nil
				}
				break
			}

			key, err := jr.generateKey(row, &alloc, primaryKeyPrefix)
			if err != nil {
				return err
			}

			spans = append(spans, roachpb.Span{
				Key:    key,
				EndKey: key.PrefixEnd(),
			})
		}

		err := jr.fetcher.StartScan(ctx, txn, spans, false /* no batch limits */, 0)
		if err != nil {
			log.Errorf(ctx, "scan error: %s", err)
			return err
		}

		// TODO(radu): we are consuming all results from a fetch before starting
		// the next batch. We could start the next batch early while we are
		// outputting rows.
		for {
			fetcherRow, err := jr.fetcher.NextRow(ctx)
			if err != nil {
				return err
			}
			if fetcherRow == nil {
				// Done with this batch.
				break
			}

			// Emit the row; stop if no more rows are needed.
			if !emitHelper(ctx, &jr.out, fetcherRow, ProducerMetadata{}, jr.input) {
				return nil
			}
		}

		if len(spans) != joinReaderBatchSize {
			// This was the last batch.
			jr.out.close()
			return nil
		}
	}
}

// Run is part of the processor interface.
func (jr *joinReader) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTagInt(ctx, "JoinReader", int(jr.desc.ID))
	ctx, span := tracing.ChildSpan(ctx, "join reader")
	defer tracing.FinishSpan(span)

	err := jr.mainLoop(ctx)
	if err != nil {
		DrainAndClose(ctx, jr.out.output, err /* cause */, jr.input)
	}
}
