// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	for i := 0; i < tu.insertRows.Len(); i++ {
		row := tu.insertRows.At(i)
		b := tu.txn.NewBatch()
		requestIdx := 0

		// Get the primary key of the insert row.
		upsertRowPK, _, err := sqlbase.EncodeIndexKey(
			tableDesc.TableDesc(), &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}

		upsertRowPK = keys.MakeFamilyKey(upsertRowPK, 0)

		// If the primary key of the insert row has already been seen
		// among the input (data source) to be inserted, it will not exist
		// in storage but we must not insert it twice either.
		// In that case we use seenKeys (prior insertion seen in data source)
		// to determine what to do and skip the KV Get.
		if _, ok := seenKeys[string(upsertRowPK)]; ok {
			// Here we have seen the same values earlier in the data source, and we
			// discovered the tuple did not exist in the table (ie it must be inserted).
			// This entails two things:
			// - a 2nd Get will serve no purpose, because if it failed the first time it won't succeed any better the 2nd time.
			// - we must forbid marking the new tuple for insertion (otherwise it would be a duplicate).

			// So we mark it as "conflicting" (insert ignored in caller) and skip the KV Get.
			conflictingRows[i] = struct{}{}
			if traceKV {
				log.VEventfDepth(ctx, 1, 2, "Get seen PK row, omitting request")
			}
		} else {
			// Here we have not seen the same values earlier in the data source.
			// We need to check if they exist in storage.
			key := roachpb.Key(upsertRowPK)
			if traceKV {
				log.VEventf(ctx, 2, "Get %s", key)
			}
			b.Get(key)

			requestIdx++
		}

		// For each secondary index that has a unique constraint, do something similar:
		// check if the secondary index key has already been seen among the insert rows,
		// and if not, mark the key to be checked against the table.

		for _, idx := range tu.conflictIndexes {
			entries, err := sqlbase.EncodeSecondaryIndex(
				tableDesc.TableDesc(), &idx, tu.ri.InsertColIDtoRowIndex, row)
			if err != nil {
				return nil, err
			}

			// Conflict indexes are unique and thus cannot be inverted indexes.
			if len(entries) != 1 {
				return nil, pgerror.NewAssertionErrorf(
					"conflict index for INSERT ON CONFLICT DO NOTHING does not have a single encoding")
			}

			entry := entries[0]
			if _, ok := seenKeys[string(entry.Key)]; ok {
				// Entry seen earlier in data source; mark as conflicting
				// (prevent 2nd insert) and omit KV request.
				conflictingRows[i] = struct{}{}
				if traceKV {
					log.VEventfDepth(ctx, 1, 2, "Get seen index row, omitting request")
				}
			} else {
				// Entry not seen earlier in data source; need to check in storage.
				if traceKV {
					log.VEventf(ctx, 2, "Get %s", entry.Key)
				}
				b.Get(entry.Key)

				requestIdx++
			}
		}

		if requestIdx == 0 {
			// We have skipped the row entirely. No batch to run.
			continue
		}

		// Now run the batch to verify the row.
		if err := tu.txn.Run(ctx, b); err != nil {
			return nil, err
		}

		for _, result := range b.Results {
			row := result.Rows[0]
			// If any of the result values are not nil, the row exists in storage.
			// Mark it as "conflicting" so that the caller knows to not insert it.
			if row.Value != nil {
				conflictingRows[i] = struct{}{}
				if traceKV {
					log.VEventf(ctx, 2, "Get %s -> row exists", row.Key)
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
		if _, ok := conflictingRows[i]; !ok {
			for _, result := range b.Results {
				// seenKeys is needed in case we are inserting the row two times.
				// The 2nd time must be handled like a conflict.
				seenKeys[string(result.Rows[0].Key)] = struct{}{}
			}
		}
	}

	return conflictingRows, nil
}

// walkExprs is part of the tableWriter interface.
func (tu *strictTableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {}

var _ batchedTableWriter = (*strictTableUpserter)(nil)
var _ tableWriter = (*strictTableUpserter)(nil)
