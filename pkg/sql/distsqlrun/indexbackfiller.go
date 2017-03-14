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

// indexBackfiller is a processor for backfilling indexes.
type indexBackfiller struct {
	spec    BackfillerSpec
	output  RowReceiver
	flowCtx *FlowCtx
	fetcher sqlbase.RowFetcher

	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap map[sqlbase.ColumnID]int
	numCols   int

	rowVals parser.Datums
	da      sqlbase.DatumAlloc
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

	if err := ib.init(); err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *indexBackfiller) init() error {
	desc := ib.spec.Table
	numCols := len(desc.Columns)

	cols := desc.Columns
	if len(desc.Mutations) > 0 {
		cols = make([]sqlbase.ColumnDescriptor, 0, numCols+len(desc.Mutations))
		cols = append(cols, desc.Columns...)
		for _, m := range desc.Mutations {
			if column := m.GetColumn(); column != nil {
				cols = append(cols, *column)
			}
		}
	}
	ib.numCols = len(cols)
	// We need all the columns.
	valNeededForCol := make([]bool, len(cols))
	for i := range valNeededForCol {
		valNeededForCol[i] = true
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
	return fetcherRow, nil
}

// ConvertBackfillError returns a cleaner SQL error for a failed Batch.
func ConvertBackfillError(tableDesc *sqlbase.TableDescriptor, b *client.Batch) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
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

// runChunk returns the next-key and an error. next-key is nil
// once the backfill is complete.
func (ib *indexBackfiller) runChunk(
	ctx context.Context,
	table sqlbase.TableDescriptor,
	added []sqlbase.IndexDescriptor,
	sp roachpb.Span,
	chunkSize int64,
) (roachpb.Key, error) {
	secondaryIndexEntries := make([]sqlbase.IndexEntry, len(added))
	err := ib.flowCtx.clientDB.Txn(ctx, func(txn *client.Txn) error {
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
		// during the scan. Index entries in the new index are being
		// populated and deleted by the OLTP commands but not otherwise
		// read or used
		if err := ib.fetcher.StartScan(
			txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize,
		); err != nil {
			log.Errorf(txn.Context, "scan error: %s", err)
			return err
		}

		b := &client.Batch{}
		for i := int64(0); i < chunkSize; i++ {
			encRow, err := ib.nextRow()
			if err != nil {
				return err
			}
			if encRow == nil {
				break
			}
			if len(ib.rowVals) == 0 {
				ib.rowVals = make(parser.Datums, len(encRow))
			}
			if err := sqlbase.EncDatumRowToDatums(ib.rowVals, encRow, &ib.da); err != nil {
				return err
			}
			// TODO(vivek): Ideally we should make a version of this that takes EncDatums.
			// It would not only avoid allocations, but also decode-encode cycles in some
			// cases (e.g. anything that is part of the PK). Similar to how the joinreader
			// uses MakeKeyFromEncDatums.
			if err := sqlbase.EncodeSecondaryIndexes(
				&ib.spec.Table, added, ib.colIdxMap,
				ib.rowVals, secondaryIndexEntries); err != nil {
				return err
			}
			for _, secondaryIndexEntry := range secondaryIndexEntries {
				log.VEventf(txn.Context, 3, "InitPut %s -> %v", secondaryIndexEntry.Key,
					secondaryIndexEntry.Value)
				b.InitPut(secondaryIndexEntry.Key, &secondaryIndexEntry.Value)
			}
		}
		// Write the new index values.
		if err := txn.Run(b); err != nil {
			return ConvertBackfillError(&ib.spec.Table, b)
		}
		return nil
	})
	return ib.fetcher.Key(), err
}

// mainLoop scans chunks of rows and constructs indexes.
// It does not close the output.
func (ib *indexBackfiller) mainLoop(ctx context.Context) error {
	var addedIndexDescs []sqlbase.IndexDescriptor
	const noNewIndex = -1
	addedIndexMutationIdx := noNewIndex
	desc := ib.spec.Table
	if len(desc.Mutations) == 0 {
		return errors.Errorf("no schema changes for table ID=%d", desc.ID)
	}
	mutationID := desc.Mutations[0].MutationID
	for i, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if index := m.GetIndex(); index != nil && m.Direction == sqlbase.DescriptorMutation_ADD {
			addedIndexDescs = append(addedIndexDescs, *index)
			if addedIndexMutationIdx == noNewIndex {
				addedIndexMutationIdx = i
			}
		}
	}

	if addedIndexMutationIdx == noNewIndex ||
		len(ib.spec.Spans) == 0 {
		return errors.Errorf("completed processing all spans for index add (%d, %d)", desc.ID, mutationID)
	}
	work := ib.spec.Spans[0].Span

	// Backfill the index entries for all the rows.
	chunkSize := ib.spec.ChunkSize
	start := timeutil.Now()
	var resume roachpb.Span
	sp := work
	for row := int64(0); sp.Key != nil; row += chunkSize {
		if log.V(2) {
			log.Infof(ctx, "index add (%d, %d) at row: %d, span: %s",
				desc.ID, mutationID, row, sp)
		}
		var err error
		sp.Key, err = ib.runChunk(ctx, desc, addedIndexDescs, sp, chunkSize)
		if err != nil {
			return err
		}
		if timeutil.Since(start) > ib.spec.Duration && sp.Key != nil {
			resume = sp
			break
		}
	}
	return WriteResumeSpan(ctx,
		ib.flowCtx.clientDB,
		ib.spec.Table.ID,
		work,
		resume,
		addedIndexMutationIdx)
}

// Run is part of the processor interface.
func (ib *indexBackfiller) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTagInt(ctx, "IndexBackfiller", int(ib.spec.Table.ID))
	ctx, span := tracing.ChildSpan(ctx, "index backfiller")
	defer tracing.FinishSpan(span)

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
	}

	err := ib.mainLoop(ctx)
	ib.output.ProducerDone(err)
}

// WriteResumeSpan writes a checkpoint for the backfill work on origSpan.
// origSpan is the span of keys that were assigned to be backfilled,
// resume is the left over work from origSpan.
func WriteResumeSpan(
	ctx context.Context,
	db *client.DB,
	id sqlbase.ID,
	origSpan roachpb.Span,
	resume roachpb.Span,
	mutationIdx int,
) error {
	ctx, traceSpan := tracing.ChildSpan(ctx, "checkpoint")
	defer tracing.FinishSpan(traceSpan)
	if resume.Key != nil && !resume.EndKey.Equal(origSpan.EndKey) {
		panic("resume must end on the same key as origSpan")
	}
	cnt := 0
	return db.Txn(ctx, func(txn *client.Txn) error {
		cnt++
		tableDesc, err := sqlbase.GetTableDescFromID(txn, id)
		if err != nil {
			return err
		}
		if cnt > 1 {
			log.Infof(ctx, "retrying adding checkpoint %s to table %s", resume, tableDesc.Name)
		}

		// This loop is finding a span in the checkpoint that fits
		// origSpan. It then carves a spot for origSpan in the
		// checkpoint, and replaces origSpan in the checkpoint with
		// resume.
		mutation := &tableDesc.Mutations[mutationIdx]
		for i, sp := range mutation.ResumeSpans {
			if sp.Key.Compare(origSpan.Key) <= 0 &&
				sp.EndKey.Compare(origSpan.EndKey) >= 0 {
				// origSpan is in sp; split sp if needed to accommodate
				// origSpan and replace origSpan with resume.
				before := mutation.ResumeSpans[:i]
				after := append([]roachpb.Span{}, mutation.ResumeSpans[i+1:]...)

				// add span to before, but merge it with the last span
				// if possible.
				addSpan := func(begin, end roachpb.Key) {
					if begin.Equal(end) {
						return
					}
					if len(before) > 0 && before[len(before)-1].EndKey.Equal(begin) {
						before[len(before)-1].EndKey = end
					} else {
						before = append(before, roachpb.Span{Key: begin, EndKey: end})
					}
				}

				// The work done = [origSpan.Key...resume.Key]
				addSpan(sp.Key, origSpan.Key)
				if resume.Key != nil {
					addSpan(resume.Key, resume.EndKey)
				} else {
					log.VEventf(txn.Context, 2, "completed processing of span: %+v", origSpan)
				}
				addSpan(origSpan.EndKey, sp.EndKey)
				mutation.ResumeSpans = append(before, after...)

				log.VEventf(txn.Context, 2, "ckpt %+v", mutation.ResumeSpans)
				txn.SetSystemConfigTrigger()
				return txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.GetID()), sqlbase.WrapDescriptor(tableDesc))
			}
		}
		// Unable to find a span containing origSpan?
		return errors.Errorf(
			"span %+v not found among %+v", origSpan, mutation.ResumeSpans,
		)
	})
}
