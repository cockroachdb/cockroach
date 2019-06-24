// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// strictTableUpserter implements the conflict-intolerant path for an upsert. See
// tableUpserter for the general case.
//
// strictTableUpserter is used for insert statements with the clause ON CONFLICT DO NOTHING
// without any specified columns. In this case, whenever a conflict is detected
// for an insert row on any of the indexes with unique key constraints, the insert is not done.
type strictTableUpserter struct {
	tableUpserterBase

	// internal state
	conflictIndexes []sqlbase.IndexDescriptor
}

// desc is part of the tableWriter interface.
func (*strictTableUpserter) desc() string { return "strict upserter" }

// init is part of the tableWriter interface.
func (tu *strictTableUpserter) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)

	err := tu.tableUpserterBase.init(txn, evalCtx)
	if err != nil {
		return err
	}

	return tu.getUniqueIndexes()
}

// atBatchEnd is part of the extendedTableWriter interface.
func (tu *strictTableUpserter) atBatchEnd(ctx context.Context, traceKV bool) error {
	conflictingRows, err := tu.getConflictingRows(ctx, traceKV)
	if err != nil {
		return err
	}
	for i := 0; i < tu.insertRows.Len(); i++ {
		insertRow := tu.insertRows.At(i)

		// Has this insert row been marked as conflicting? If so, skip.
		if _, ok := conflictingRows[i]; ok {
			continue
		}

		if err := tu.ri.InsertRow(ctx, tu.b, insertRow, true, row.CheckFKs, traceKV); err != nil {
			return err
		}

		tu.resultCount++

		// for ... RETURNING clause
		if tu.collectRows {
			var resultRow tree.Datums
			if tu.insertReorderingRequired {
				resultRow = tu.makeResultFromRow(insertRow, tu.ri.InsertColIDtoRowIndex)
			} else {
				resultRow = insertRow
			}
			_, err = tu.rowsUpserted.AddRow(ctx, resultRow)
			if err != nil {
				return err
			}
		}
	}

	if tu.collectRows {
		// If we have populate rowsUpserted, the consumer
		// will want to know exactly how many rows there in there.
		// Use that as final resultCount. This overrides any
		// other value computed in the main loop above.
		tu.resultCount = tu.rowsUpserted.Len()
	}

	return nil
}

// Get all unique indexes and store them in tu.ConflictIndexes.
func (tu *strictTableUpserter) getUniqueIndexes() (err error) {
	tableDesc := tu.tableDesc()
	indexes := tableDesc.Indexes
	for _, index := range indexes {
		if index.Unique {
			tu.conflictIndexes = append(tu.conflictIndexes, index)
		}
	}
	return nil
}

// getConflictingRows returns all of the the rows that are in conflict.
//
// The return value is a set of all the rows (by their place in tu.InsertRows)
// that are in conflict with either an existing row in the table or another insert row.
func (tu *strictTableUpserter) getConflictingRows(
	ctx context.Context, traceKV bool,
) (conflictingRows map[int]struct{}, err error) {
	tableDesc := tu.tableDesc()

	// We will operate in two phases: collect the existence booleans
	// in storage, and then in a second phase compute the
	// marker for the caller to indicate whether a row should be inserted or not.

	// The first phase will issue KV requests.
	// For every row there will be 1 + len(tu.conflictIndexes) requests/responses.
	b := tu.txn.NewBatch()

	for i := 0; i < tu.insertRows.Len(); i++ {
		row := tu.insertRows.At(i)

		// Get the primary key of the insert row.
		upsertRowPKBytes, _, err := sqlbase.EncodeIndexKey(
			tableDesc.TableDesc(), &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}

		upsertRowPK := roachpb.Key(keys.MakeFamilyKey(upsertRowPKBytes, 0))
		if traceKV {
			log.VEventf(ctx, 2, "Get %s", upsertRowPK)
		}
		b.Get(upsertRowPK)

		// Ditto for secondary indexes.

		for _, idx := range tu.conflictIndexes {
			entries, err := sqlbase.EncodeSecondaryIndex(
				tableDesc.TableDesc(), &idx, tu.ri.InsertColIDtoRowIndex, row)
			if err != nil {
				return nil, err
			}

			// Conflict indexes are unique and thus cannot be inverted indexes.
			if len(entries) != 1 {
				return nil, errors.AssertionFailedf(
					"conflict index for INSERT ON CONFLICT DO NOTHING does not have a single encoding")
			}
			entry := entries[0]

			if traceKV {
				log.VEventf(ctx, 2, "Get %s", entry.Key)
			}
			b.Get(entry.Key)
		}
	}

	// Now run the batch to collect the existence booleans.
	if err := tu.txn.Run(ctx, b); err != nil {
		return nil, err
	}

	// In the second phase, we compute the markers for the caller.

	// conflictingRows will indicate to the caller whether the row needs
	// to be inserted (conflictingRows = false) or omitted
	// (conflictingRows = true).
	conflictingRows = make(map[int]struct{})

	// seenKeys below handles the case where the row did not exist yet
	// in the table but is mentioned two times in the data source.  In
	// that case, we must ensure that the first occurrence gets
	// conflictingRows = false, but the second occurrence gets
	// conflictingRows = true.
	seenKeys := make(map[string]struct{})

	reqsPerRow := 1 + len(tu.conflictIndexes)
	for insertRowIdx := 0; insertRowIdx < tu.insertRows.Len(); insertRowIdx++ {
		// We will want to operate in two phases: process the existence
		// results from storage, and only then populate seenKeys.

		// Process the results of the existence checks.
		// We iterate on the subset of b.Results that correspond to the
		// current insert row.
		startRequestIdx := reqsPerRow * insertRowIdx
		endRequestIdx := startRequestIdx + reqsPerRow
		for requestIdx := startRequestIdx; requestIdx < endRequestIdx; requestIdx++ {
			row := b.Results[requestIdx].Rows[0]
			// If any of the result values are not nil, the row exists in storage.
			// Mark it as "conflicting" so that the caller knows to not insert it.
			if row.Value != nil {
				conflictingRows[insertRowIdx] = struct{}{}
				if traceKV {
					log.VEventf(ctx, 2, "Get %s -> row exists", row.Key)
				}
			} else {
				// The row does not exist in storage. Has it been seen earlier
				// in the data source?
				if _, ok := seenKeys[string(row.Key)]; ok {
					// Yes, also mark as conflicting to omit the insert.
					conflictingRows[insertRowIdx] = struct{}{}
				}
			}
		}

		// Now we must handle the case where a row was not found in
		// storage, but may be found in a later iteration in the data
		// source: in that case we must also prevent the duplicate
		// but there will be no row in storage. The loop above
		// uses seenKeys for this purpose, which we must populate here.
		//
		// Note: we cannot populate seenKeys "as we go" any earlier than this point:
		// - we cannot do it as the key gets encoded next to the b.Get() calls
		//   (this was bug #33313 - see bug description + regression test for case)
		// - we cannot do it in the first loop over b.Results above,
		//   because it's possible the conflict is only detected on a secondary index.
		if _, ok := conflictingRows[insertRowIdx]; !ok {
			startRequestIdx := reqsPerRow * insertRowIdx
			endRequestIdx := startRequestIdx + reqsPerRow
			for requestIdx := startRequestIdx; requestIdx < endRequestIdx; requestIdx++ {
				seenKeys[string(b.Results[requestIdx].Rows[0].Key)] = struct{}{}
			}
		}
	}

	return conflictingRows, nil
}

// walkExprs is part of the tableWriter interface.
func (tu *strictTableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {}

var _ batchedTableWriter = (*strictTableUpserter)(nil)
var _ tableWriter = (*strictTableUpserter)(nil)
