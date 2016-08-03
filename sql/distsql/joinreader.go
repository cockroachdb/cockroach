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

package distsql

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

// TODO(radu): we currently create one batch at a time and run the KV operations
// on this node. In the future we may want to build separate batches for the
// nodes that "own" the respective ranges, and send out flows on those nodes.
const joinReaderBatchSize = 100

type joinReader struct {
	readerBase

	ctx context.Context

	input  RowSource
	output RowReceiver
}

var _ processor = &joinReader{}

func newJoinReader(
	flowCtx *FlowCtx, spec *JoinReaderSpec, input RowSource, output RowReceiver,
) (*joinReader, error) {
	jr := &joinReader{
		input:  input,
		output: output,
	}

	if spec.IndexIdx != 0 {
		// TODO(radu): for now we only support joining with the primary index
		return nil, errors.Errorf("join with index not implemented")
	}

	err := jr.readerBase.init(flowCtx, &spec.Table, int(spec.IndexIdx), spec.Filter,
		spec.OutputColumns, false)
	if err != nil {
		return nil, err
	}

	jr.ctx = log.WithLogTagInt(jr.flowCtx.Context, "JoinReader", int(jr.desc.ID))
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

	// Verify the types.
	// TODO(radu): not strictly needed, perhaps enable only for tests.
	for i, cid := range index.ColumnIDs {
		colType := jr.desc.Columns[jr.colIdxMap[cid]].Type.Kind
		if row[i].Type != colType {
			return nil, errors.Errorf("joinReader input column %d has invalid type %s, expected %s",
				i, row[i].Type, colType)
		}
	}

	return sqlbase.MakeKeyFromEncDatums(row, index.ColumnDirections, primaryKeyPrefix, alloc)
}

// mainLoop runs the mainLoop and returns any error.
// It does not close the output.
func (jr *joinReader) mainLoop() error {
	primaryKeyPrefix := sqlbase.MakeIndexKeyPrefix(&jr.desc, jr.index.ID)

	var alloc sqlbase.DatumAlloc
	spans := make(sqlbase.Spans, 0, joinReaderBatchSize)

	if log.V(2) {
		log.Infof(jr.ctx, "starting (filter: %s)", jr.filter)
		defer log.Infof(jr.ctx, "exiting")
	}

	for {
		// TODO(radu): figure out how to send smaller batches if the source has
		// a soft limit (perhaps send the batch out if we don't get a result
		// within a certain amount of time).
		for spans = spans[:0]; len(spans) < joinReaderBatchSize; {
			row, err := jr.input.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				if len(spans) == 0 {
					return nil
				}
				break
			}
			key, err := jr.generateKey(row, &alloc, primaryKeyPrefix)
			if err != nil {
				return err
			}

			spans = append(spans, sqlbase.Span{
				Start: key,
				End:   key.PrefixEnd(),
			})
		}

		err := jr.fetcher.StartScan(jr.flowCtx.txn, spans, 0)
		if err != nil {
			log.Errorf(jr.ctx, "scan error: %s", err)
			return err
		}

		// TODO(radu): we are consuming all results from a fetch before starting
		// the next batch. We could start the next batch early while we are
		// outputting rows.
		for {
			outRow, err := jr.nextRow()
			if err != nil {
				return err
			}
			if outRow == nil {
				// Done.
				break
			}
			if log.V(3) {
				log.Infof(jr.ctx, "pushing row %s\n", outRow)
			}
			// Push the row to the output RowReceiver; stop if they don't need more
			// rows.
			if !jr.output.PushRow(outRow) {
				if log.V(2) {
					log.Infof(jr.ctx, "no more rows required")
				}
				return nil
			}
		}

		if len(spans) != joinReaderBatchSize {
			// This was the last batch.
			return nil
		}
	}
}

// Run is part of the processor interface.
func (jr *joinReader) Run(wg *sync.WaitGroup) {
	err := jr.mainLoop()
	jr.output.Close(err)
	if wg != nil {
		wg.Done()
	}
}
