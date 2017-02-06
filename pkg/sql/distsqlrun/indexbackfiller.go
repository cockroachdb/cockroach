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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	// indexBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill. The index backfill involves a table
	// scan, and a number of individual ops presented in a batch. This value
	// is smaller than ColumnTruncateAndBackfillChunkSize, because it involves
	// a number of individual index row updates that can be scattered over
	// many ranges.
	indexBackfillChunkSize = 100
)

// indexBackfiller is a processor for backfilling indexes.
type indexBackfiller struct {
	ctx     context.Context
	spec    BackfillerSpec
	output  RowReceiver
	flowCtx *FlowCtx
	fetcher sqlbase.RowFetcher

	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap  map[sqlbase.ColumnID]int
	outputCols []int

	// Last row returned by the rowFetcher; it has one entry per table column.
	rowAlloc sqlbase.EncDatumRowAlloc
}

var _ processor = &indexBackfiller{}

func newIndexBackfiller(
	flowCtx *FlowCtx, spec *BackfillerSpec, post *PostProcessSpec, output RowReceiver,
) (*indexBackfiller, error) {
	ib := &indexBackfiller{
		flowCtx: flowCtx,
		output:  output,
		spec:    *spec,
	}

	cols := make([]int, 0, len(spec.Table.Columns)+len(spec.Table.Mutations))
	for i := range spec.Table.Columns {
		cols = append(cols, i)
	}
	// Also add new columns in mutations that can contribute to an index.
	if len(spec.Table.Mutations) > 0 {
		mutationID := spec.Table.Mutations[0].MutationID
		for i, mutation := range spec.Table.Mutations {
			if mutation.MutationID != mutationID {
				break
			}
			if c := mutation.GetColumn(); c != nil &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD {
				cols = append(cols, len(spec.Table.Columns)+i)
			}
		}
	}

	if err := ib.init(cols); err != nil {
		return nil, err
	}

	ib.ctx = log.WithLogTagInt(ib.flowCtx.Context, "IndexBackfiller", int(spec.Table.ID))
	return ib, nil
}

func (ib *indexBackfiller) init(outputCols []int) error {
	ib.outputCols = outputCols
	desc := ib.spec.Table
	numCols := len(desc.Columns)

	cols := desc.Columns
	if len(desc.Mutations) > 0 {
		cols = make([]sqlbase.ColumnDescriptor, 0, numCols+len(desc.Mutations))
		cols = append(cols, desc.Columns...)
		for _, c := range ib.outputCols {
			if c >= numCols+len(desc.Mutations) {
				return errors.Errorf("invalid column index %d", c)
			}
			if c >= numCols {
				if column := desc.Mutations[c-numCols].GetColumn(); column != nil {
					// Even if the column is non-nullable it can be null in the
					// middle of a schema change.
					col := *column
					col.Nullable = true
					cols = append(cols, col)
				} else {
					return errors.Errorf("invalid column index %d", c)
				}
			}
		}
	}

	// Figure out which columns we need: the output columns plus any other
	// columns used by the filter expression.
	valNeededForCol := make([]bool, len(cols))
	idx := 0
	for _, c := range ib.outputCols {
		if c >= numCols+len(desc.Mutations) {
			return errors.Errorf("invalid column index %d", c)
		}
		if c < numCols {
			valNeededForCol[c] = true
		} else {
			valNeededForCol[numCols+idx] = true
			idx++
		}
	}

	ib.colIdxMap = make(map[sqlbase.ColumnID]int, len(cols))
	for i, c := range cols {
		ib.colIdxMap[c.ID] = i
	}
	return ib.fetcher.Init(
		&desc, ib.colIdxMap, &desc.PrimaryIndex, false, false, cols, valNeededForCol, false,
	)
}

// nextRow processes table rows.
func (ib *indexBackfiller) nextRow() (sqlbase.EncDatumRow, error) {
	fetcherRow, err := ib.fetcher.NextRow()
	if err != nil || fetcherRow == nil {
		return nil, err
	}

	outRow := ib.rowAlloc.AllocRow(len(ib.outputCols))
	for i := range ib.outputCols {
		outRow[i] = fetcherRow[i]
	}
	return outRow, nil
}

func convertBackfillError(tableDesc *sqlbase.TableDescriptor, b *client.Batch) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// convertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc := protoutil.Clone(tableDesc).(*sqlbase.TableDescriptor)
	mutationID := desc.Mutations[0].MutationID
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		desc.MakeMutationComplete(mutation)
	}
	return sqlbase.ConvertBatchError(desc, b)
}

// runChunk returns the next-key, done and an error. next-key and
// done are invalid if error != nil. next-key is invalid if done is true.
func (ib *indexBackfiller) runChunk(
	table sqlbase.TableDescriptor, added []sqlbase.IndexDescriptor, sp roachpb.Span, chunkSize int64,
) (roachpb.Key, bool, error) {
	var nextKey roachpb.Key
	done := false
	secondaryIndexEntries := make([]sqlbase.IndexEntry, len(added))
	err := ib.flowCtx.clientDB.Txn(ib.flowCtx.Context, func(txn *client.Txn) error {
		if ib.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := ib.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if ib.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
			defer ib.flowCtx.testingKnobs.RunAfterBackfillChunk()
		}

		// Get the next set of rows.
		//
		// Running the scan and applying the changes in many transactions
		// is fine because the schema change is in the correct state to
		// handle intermediate OLTP commands which delete and add values
		// during the scan.
		if err := ib.fetcher.StartScan(
			txn, []roachpb.Span{sp}, true /* limit batches */, chunkSize,
		); err != nil {
			log.Errorf(txn.Context, "scan error: %s", err)
			return err
		}

		b := &client.Batch{}
		numRows := int64(0)
		var rowVals parser.Datums
		for ; numRows < chunkSize; numRows++ {
			encRow, err := ib.nextRow()
			if err != nil {
				return err
			}
			if encRow == nil {
				break
			}
			if len(rowVals) == 0 {
				rowVals = make(parser.Datums, len(encRow))
			}
			var da sqlbase.DatumAlloc
			if err := sqlbase.EncDatumRowToDatums(rowVals, encRow, &da); err != nil {
				return err
			}

			if err := sqlbase.EncodeSecondaryIndexes(
				&ib.spec.Table, added, ib.colIdxMap,
				rowVals, secondaryIndexEntries); err != nil {
				return err
			}
			for _, secondaryIndexEntry := range secondaryIndexEntries {
				if log.V(2) {
					log.Infof(txn.Context, "InitPut %s -> %v", secondaryIndexEntry.Key,
						secondaryIndexEntry.Value)
				}
				b.InitPut(secondaryIndexEntry.Key, &secondaryIndexEntry.Value)
			}
		}
		// Write the new index values.
		if err := txn.Run(b); err != nil {
			return convertBackfillError(&ib.spec.Table, b)
		}
		// Have we processed all the rows?
		if done = numRows < chunkSize; done {
			return nil
		}
		// Keep track of the next key.
		nextKey = ib.fetcher.Key()
		return nil
	})
	return nextKey, done, err
}

// mainLoop runs the mainLoop and returns any error.
// It does not close the output.
func (ib *indexBackfiller) mainLoop() error {
	ctx, span := tracing.ChildSpan(ib.ctx, "index backfiller")
	defer tracing.FinishSpan(span)

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	var addedIndexDescs []sqlbase.IndexDescriptor
	const mutationSentinel = -1
	addedIndexMutationIdx := mutationSentinel
	desc := ib.spec.Table
	if len(desc.Mutations) == 0 {
		return nil
	}
	mutationID := desc.Mutations[0].MutationID
	for i, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if index := m.GetIndex(); index != nil && m.Direction == sqlbase.DescriptorMutation_ADD {
			addedIndexDescs = append(addedIndexDescs, *index)
			if addedIndexMutationIdx == mutationSentinel {
				addedIndexMutationIdx = i
			}
		}
	}

	if addedIndexMutationIdx == mutationSentinel ||
		len(ib.spec.Spans) == 0 {
		log.Infof(ctx, "completed processing all spans for index add (%d, %d)", desc.ID, mutationID)
		return nil
	}
	work := ib.spec.Spans[0].Span

	// Backfill the index entries for all the rows.
	chunkSize := ib.getChunkSize(indexBackfillChunkSize)
	start := timeutil.Now()
	var resume roachpb.Span
	sp := work
	for row, done := int64(0), false; !done; row += chunkSize {
		if log.V(2) {
			log.Infof(ctx, "index add (%d, %d) at row: %d, span: %s",
				desc.ID, mutationID, row, sp)
		}
		var err error
		sp.Key, done, err = ib.runChunk(desc, addedIndexDescs, sp, chunkSize)
		if err != nil {
			return err
		}
		if timeutil.Since(start) > ib.spec.Duration && !done {
			resume = sp
			break
		}
	}
	return ib.writeResumeSpan(work, resume, addedIndexMutationIdx)
}

// Run is part of the processor interface.
func (ib *indexBackfiller) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	err := ib.mainLoop()
	ib.output.Close(err)
}

// writeResumeSpan writes a checkpoint for the work done so far.
func (ib *indexBackfiller) writeResumeSpan(
	origSpan roachpb.Span, resume roachpb.Span, mutationIdx int,
) error {
	return ib.flowCtx.clientDB.Txn(ib.flowCtx.Context, func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, ib.spec.Table.ID)
		if err != nil {
			return err
		}

		for i, sp := range tableDesc.Mutations[mutationIdx].ResumeSpan {
			if sp.Key.Compare(origSpan.Key) <= 0 &&
				sp.EndKey.Compare(origSpan.EndKey) >= 0 {
				// origSpan is in sp; split sp if needed.

				// maybe delete checkpoint if processing is complete.
				maybeRemoveCheckpoint := func(idx int) {
					if resume.Key == nil {
						tableDesc.Mutations[mutationIdx].ResumeSpan =
							append(tableDesc.Mutations[mutationIdx].ResumeSpan[:idx],
								tableDesc.Mutations[mutationIdx].ResumeSpan[idx+1:]...)
					}
				}
				addCheckpoint := func(spans []roachpb.Span) {
					tableDesc.Mutations[mutationIdx].ResumeSpan =
						append(tableDesc.Mutations[mutationIdx].ResumeSpan[:i],
							append(spans, tableDesc.Mutations[mutationIdx].ResumeSpan[i+1:]...)...)
				}
				if sp.Key.Equal(origSpan.Key) {
					if sp.EndKey.Equal(origSpan.EndKey) {
						// Update the checkpoint; common case.
						tableDesc.Mutations[mutationIdx].ResumeSpan[i] = resume
					} else {
						right := sp
						right.Key = origSpan.EndKey
						addCheckpoint([]roachpb.Span{resume, right})
					}
					maybeRemoveCheckpoint(i)
				} else {
					left := sp
					left.EndKey = origSpan.Key
					if sp.EndKey.Equal(origSpan.EndKey) {
						addCheckpoint([]roachpb.Span{left, resume})
					} else {
						right := sp
						right.Key = origSpan.EndKey
						addCheckpoint([]roachpb.Span{left, resume, right})
					}
					maybeRemoveCheckpoint(i + 1)
				}

				txn.SetSystemConfigTrigger()
				return txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.GetID()), sqlbase.WrapDescriptor(tableDesc))
			}
		}
		// Unable to find a span containing origSpan?
		return errors.Errorf(
			"span %+v not found among %+v", origSpan, tableDesc.Mutations[mutationIdx].ResumeSpan,
		)
	})
}

func (ib *indexBackfiller) getChunkSize(chunkSize int64) int64 {
	if ib.flowCtx.testingKnobs.BackfillChunkSize > 0 {
		return ib.flowCtx.testingKnobs.BackfillChunkSize
	}
	return chunkSize
}
