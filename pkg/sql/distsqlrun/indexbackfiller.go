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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfiller

	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap map[sqlbase.ColumnID]int

	rowVals parser.Datums
	da      sqlbase.DatumAlloc
}

var _ processor = &indexBackfiller{}
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

	ib.colIdxMap = make(map[sqlbase.ColumnID]int, len(cols))
	for i, c := range cols {
		ib.colIdxMap[c.ID] = i
	}

	valNeededForCol := make([]bool, len(cols))
	mutationID := desc.Mutations[0].MutationID
	for _, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if IndexMutationFilter(m) {
			idx := m.GetIndex()
			for i, col := range cols {
				valNeededForCol[i] = valNeededForCol[i] || idx.ContainsColumnID(col.ID)
			}
		}
	}

	return ib.fetcher.Init(
		&desc, ib.colIdxMap, &desc.PrimaryIndex, false, false, cols, valNeededForCol, false,
	)
}

func (ib *indexBackfiller) runChunk(
	ctx context.Context, mutations []sqlbase.DescriptorMutation, sp roachpb.Span, chunkSize int64,
) (roachpb.Key, error) {
	added := make([]sqlbase.IndexDescriptor, len(mutations))
	for i, m := range mutations {
		added[i] = *m.GetIndex()
	}
	secondaryIndexEntries := make([]sqlbase.IndexEntry, len(mutations))
	err := ib.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
			ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize,
		); err != nil {
			log.Errorf(ctx, "scan error: %s", err)
			return err
		}

		b := txn.NewBatch()
		for i := int64(0); i < chunkSize; i++ {
			encRow, err := ib.fetcher.NextRow(ctx)
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
			if err := sqlbase.EncodeSecondaryIndexes(
				&ib.spec.Table, added, ib.colIdxMap,
				ib.rowVals, secondaryIndexEntries); err != nil {
				return err
			}
			for _, secondaryIndexEntry := range secondaryIndexEntries {
				log.VEventf(ctx, 3, "InitPut %s -> %v", secondaryIndexEntry.Key,
					secondaryIndexEntry.Value)
				b.InitPut(secondaryIndexEntry.Key, &secondaryIndexEntry.Value)
			}
		}
		// Write the new index values.
		if err := txn.CommitInBatch(ctx, b); err != nil {
			return ConvertBackfillError(&ib.spec.Table, b)
		}
		return nil
	})
	return ib.fetcher.Key(), err
}
