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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfiller

	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap map[sqlbase.ColumnID]int

	types   []sqlbase.ColumnType
	rowVals tree.Datums
	da      sqlbase.DatumAlloc
}

var _ Processor = &indexBackfiller{}
var _ chunkBackfiller = &indexBackfiller{}

// IndexMutationFilter is a filter that allows mutations that add indexes.
func IndexMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetIndex() != nil && m.Direction == sqlbase.DescriptorMutation_ADD
}

func newIndexBackfiller(
	flowCtx *FlowCtx, spec BackfillerSpec, post *PostProcessSpec, output RowReceiver,
) (*indexBackfiller, error) {
	ib := &indexBackfiller{
		backfiller: backfiller{
			name:    "Index",
			filter:  IndexMutationFilter,
			flowCtx: flowCtx,
			output:  output,
			spec:    spec,
		},
	}
	ib.backfiller.chunkBackfiller = ib

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
	ib.types = make([]sqlbase.ColumnType, len(cols))
	for i := range cols {
		ib.types[i] = cols[i].Type
	}

	ib.colIdxMap = make(map[sqlbase.ColumnID]int, len(cols))
	for i, c := range cols {
		ib.colIdxMap[c.ID] = i
	}

	var valNeededForCol util.FastIntSet
	mutationID := desc.Mutations[0].MutationID
	for _, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if IndexMutationFilter(m) {
			idx := m.GetIndex()
			for i, col := range cols {
				if idx.ContainsColumnID(col.ID) {
					valNeededForCol.Add(i)
				}
			}
		}
	}

	tableArgs := sqlbase.RowFetcherTableArgs{
		Desc:            &desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       ib.colIdxMap,
		Cols:            cols,
		ValNeededForCol: valNeededForCol,
	}
	return ib.fetcher.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &ib.alloc, tableArgs,
	)
}

func (ib *indexBackfiller) runChunk(
	tctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	if ib.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
		if err := ib.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, err
		}
	}
	if ib.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
		defer ib.flowCtx.testingKnobs.RunAfterBackfillChunk()
	}

	ctx, traceSpan := tracing.ChildSpan(tctx, "chunk")
	defer tracing.FinishSpan(traceSpan)

	added := make([]sqlbase.IndexDescriptor, len(mutations))
	for i, m := range mutations {
		added[i] = *m.GetIndex()
	}
	secondaryIndexEntries := make([]sqlbase.IndexEntry, len(mutations))

	buildIndexEntries := func(ctx context.Context, txn *client.Txn) ([]sqlbase.IndexEntry, error) {
		entries := make([]sqlbase.IndexEntry, 0, chunkSize*int64(len(added)))

		// Get the next set of rows.
		//
		// Running the scan and applying the changes in many transactions
		// is fine because the schema change is in the correct state to
		// handle intermediate OLTP commands which delete and add values
		// during the scan. Index entries in the new index are being
		// populated and deleted by the OLTP commands but not otherwise
		// read or used
		if err := ib.fetcher.StartScan(
			ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize, false, /* traceKV */
		); err != nil {
			log.Errorf(ctx, "scan error: %s", err)
			return nil, err
		}

		for i := int64(0); i < chunkSize; i++ {
			encRow, _, _, err := ib.fetcher.NextRow(ctx)
			if err != nil {
				err = scrub.UnwrapScrubError(err)
				return nil, err
			}
			if encRow == nil {
				break
			}
			if len(ib.rowVals) == 0 {
				ib.rowVals = make(tree.Datums, len(encRow))
			}
			if err := sqlbase.EncDatumRowToDatums(ib.types, ib.rowVals, encRow, &ib.da); err != nil {
				return nil, err
			}

			// We're resetting the length of this slice for variable length indexes such as inverted
			// indexes which can append entries to the end of the slice. If we don't do this, then everything
			// EncodeSecondaryIndexes appends to secondaryIndexEntries for a row, would stay in the slice for
			// subsequent rows and we would then have duplicates in entries on output.
			secondaryIndexEntries = secondaryIndexEntries[:len(mutations)]
			if secondaryIndexEntries, err = sqlbase.EncodeSecondaryIndexes(
				&ib.spec.Table, added, ib.colIdxMap,
				ib.rowVals, secondaryIndexEntries); err != nil {
				return nil, err
			}
			entries = append(entries, secondaryIndexEntries...)
		}
		return entries, nil
	}

	transactionalChunk := func(ctx context.Context) error {
		return ib.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			entries, err := buildIndexEntries(ctx, txn)
			if err != nil {
				return err
			}
			batch := txn.NewBatch()

			for _, entry := range entries {
				batch.InitPut(entry.Key, &entry.Value, false /* failOnTombstones */)
			}
			if err := txn.CommitInBatch(ctx, batch); err != nil {
				return ConvertBackfillError(ctx, &ib.spec.Table, batch)
			}
			return nil
		})
	}

	// TODO(jordan): enable this once IsMigrated is a real implementation.
	/*
		if !util.IsMigrated() {
			// If we're running a mixed cluster, some of the nodes will have an old
			// implementation of InitPut that doesn't take into account the expected
			// timetsamp. In that case, we have to run our chunk transactionally at the
			// current time.
			err := transactionalChunk(ctx)
			return ib.fetcher.Key(), err
		}
	*/

	var entries []sqlbase.IndexEntry
	if err := ib.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)

		var err error
		entries, err = buildIndexEntries(ctx, txn)
		return err
	}); err != nil {
		return nil, err
	}

	retried := false
	// Write the new index values.
	if err := ib.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		batch := txn.NewBatch()

		for _, entry := range entries {
			// Since we're not regenerating the index entries here, if the
			// transaction restarts the values might already have their checksums
			// set which is invalid - clear them.
			if retried {
				// Reset the value slice. This is necessary because gRPC may still be
				// holding onto the underlying slice here. See #17348 for more details.
				// We only need to reset RawBytes because neither entry nor entry.Value
				// are pointer types.
				rawBytes := entry.Value.RawBytes
				entry.Value.RawBytes = make([]byte, len(rawBytes))
				copy(entry.Value.RawBytes, rawBytes)
				entry.Value.ClearChecksum()
			}
			batch.InitPut(entry.Key, &entry.Value, true /* failOnTombstones */)
		}
		retried = true
		if err := txn.CommitInBatch(ctx, batch); err != nil {
			if _, ok := batch.MustPErr().GetDetail().(*roachpb.ConditionFailedError); ok {
				return pgerror.NewError(pgerror.CodeUniqueViolationError, "")
			}
			return err
		}
		return nil
	}); err != nil {
		if sqlbase.IsUniquenessConstraintViolationError(err) {
			log.VEventf(ctx, 2, "failed write. retrying transactionally: %v", err)
			// Someone wrote a value above one of our new index entries. Since we did
			// a historical read, we didn't have the most up-to-date value for the
			// row we were backfilling so we can't just blindly write it to the
			// index. Instead, we retry the transaction at the present timestamp.
			if err := transactionalChunk(ctx); err != nil {
				log.VEventf(ctx, 2, "failed transactional write: %v", err)
				return nil, err
			}
		} else {
			log.VEventf(ctx, 2, "failed write due to other error, not retrying: %v", err)
			return nil, err
		}
	}

	return ib.fetcher.Key(), nil
}
