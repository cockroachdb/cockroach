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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	conflictingRows, err := tu.getConflictingRows(ctx, false)
	if err != nil {
		return err
	}
	for i := 0; i < tu.insertRows.Len(); i++ {
		insertRow := tu.insertRows.At(i)

		// Has this insert row been marked as conflicting? If so, skip.
		if _, ok := conflictingRows[i]; ok {
			continue
		}

		if err := tu.ri.InsertRow(ctx, tu.b, insertRow, true, sqlbase.CheckFKs, traceKV); err != nil {
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
	ctx context.Context, _ bool,
) (conflictingRows map[int]struct{}, err error) {
	tableDesc := tu.tableDesc()

	conflictingRows = make(map[int]struct{})

	seenKeys := make(map[string]struct{})

	b := tu.txn.NewBatch()

	for i := 0; i < tu.insertRows.Len(); i++ {
		row := tu.insertRows.At(i)

		// Get the primary key of the insert row.
		upsertRowPK, _, err := sqlbase.EncodeIndexKey(
			tableDesc, &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}

		upsertRowPK = keys.MakeFamilyKey(upsertRowPK, 0)

		// If the primary key of the insert row has already been seen among the insert rows,
		// mark this row as conflicting.
		if _, ok := seenKeys[string(upsertRowPK)]; ok {
			conflictingRows[i] = struct{}{}
		}

		b.Get(roachpb.Key(upsertRowPK))
		seenKeys[string(upsertRowPK)] = struct{}{}

		// Otherwise, check the primary key against the table.

		// For each secondary index that has a unique constraint, do something similar:
		// check if the secondary index key has already been seen among the insert rows,
		// and if not, mark the key to be checked against the table.
		for _, idx := range tu.conflictIndexes {
			entries, err := sqlbase.EncodeSecondaryIndex(
				tableDesc, &idx, tu.ri.InsertColIDtoRowIndex, row)
			if err != nil {
				return nil, err
			}

			for _, entry := range entries {
				if _, ok := seenKeys[string(entry.Key)]; ok {
					conflictingRows[i] = struct{}{}
				}
				b.Get(entry.Key)
				seenKeys[string(entry.Key)] = struct{}{}
			}
		}
	}

	// Run a transaction to see if any of the rows conflict with any existing rows in the table or with other insert rows.
	if err := tu.txn.Run(ctx, b); err != nil {
		return nil, err
	}

	numIndexes := 1 + len(tu.conflictIndexes)
	for i, result := range b.Results {
		// There are (1 + len(tu.conflictIndexes)) * len(tu.InsertRows) results.
		// All the results that correspond to a particular insertRow are next to each other.
		// By this logic, the division computes the insertRow that is being queried for.
		insertRowIndex := i / numIndexes

		for _, row := range result.Rows {
			// If any of the result values are not nil, then that means that the insert row is in conflict and should be marked as such.
			if row.Value != nil {
				conflictingRows[insertRowIndex] = struct{}{}
			}
		}
	}

	return conflictingRows, nil
}

// walkExprs is part of the tableWriter interface.
func (tu *strictTableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {}

var _ batchedTableWriter = (*strictTableUpserter)(nil)
var _ tableWriter = (*strictTableUpserter)(nil)
