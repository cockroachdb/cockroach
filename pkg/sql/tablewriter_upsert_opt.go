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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// optTableUpserter implements the upsert operation when it is planned by the
// cost-based optimizer (CBO). The CBO can use a much simpler upserter because
// it incorporates conflict detection, update and computed column evaluation,
// and other upsert operations into the input query, rather than requiring the
// upserter to do it. For example:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   INSERT INTO abc VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b=10
//
// The CBO will generate an input expression similar to this:
//
//   SELECT ins_a, ins_b, ins_c, fetch_a, fetch_b, fetch_c, 10 AS upd_b
//   FROM (VALUES (1, 2, NULL)) AS ins(ins_a, ins_b, ins_c)
//   LEFT OUTER JOIN abc AS fetch(fetch_a, fetch_b, fetch_c)
//   ON ins_a = fetch_a
//
// The other non-CBO upserters perform custom left lookup joins. However, that
// doesn't allow sharing of optimization rules and doesn't work with correlated
// SET expressions.
//
// For more details on how the CBO compiles UPSERT statements, see the block
// comment on Builder.buildInsert in opt/optbuilder/insert.go.
type optTableUpserter struct {
	tableWriterBase

	ri row.Inserter

	// Should we collect the rows for a RETURNING clause?
	rowsNeeded bool

	// A mapping of column IDs to the return index used to shape the resulting
	// rows to those required by the returning clause. Only required if
	// rowsNeeded is true.
	colIDToReturnIndex catalog.TableColMap

	// Do the result rows have a different order than insert rows. Only set if
	// rowsNeeded is true.
	insertReorderingRequired bool

	// fetchCols indicate which columns need to be fetched from the target table,
	// in order to detect whether a conflict has occurred, as well as to provide
	// existing values for updates.
	fetchCols []catalog.Column

	// updateCols indicate which columns need an update during a conflict.
	updateCols []catalog.Column

	// returnCols indicate which columns need to be returned by the Upsert.
	returnCols []catalog.Column

	// canaryOrdinal is the ordinal position of the column within the input row
	// that is used to decide whether to execute an insert or update operation.
	// If the canary column is null, then an insert will be performed; otherwise,
	// an update is performed. This column will always be one of the fetchCols.
	canaryOrdinal int

	// resultRow is a reusable slice of Datums used to store result rows.
	resultRow tree.Datums

	// ru is used when updating rows.
	ru row.Updater

	// tabColIdxToRetIdx is the mapping from the columns in the table to the
	// columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the table column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column of the table is
	// to be returned.
	tabColIdxToRetIdx []int
}

var _ tableWriter = &optTableUpserter{}

// init is part of the tableWriter interface.
func (tu *optTableUpserter) init(
	ctx context.Context, txn *kv.Txn, evalCtx *tree.EvalContext, sv *settings.Values,
) error {
	tu.tableWriterBase.init(txn, tu.ri.Helper.TableDesc, evalCtx, sv)

	// rowsNeeded, set upon initialization, indicates pkg/sql/backfill.gowhether or not we want
	// rows returned from the operation.
	if tu.rowsNeeded {
		tu.resultRow = make(tree.Datums, len(tu.returnCols))
		tu.rows = rowcontainer.NewRowContainer(
			evalCtx.Mon.MakeBoundAccount(),
			colinfo.ColTypeInfoFromColumns(tu.returnCols),
		)

		// Create the map from colIds to the expected columns.
		// Note that this map will *not* contain any mutation columns - that's
		// because even though we might insert values into mutation columns, we
		// never return them back to the user.
		tu.colIDToReturnIndex = catalog.ColumnIDToOrdinalMap(tu.tableDesc().PublicColumns())
		if tu.ri.InsertColIDtoRowIndex.Len() == tu.colIDToReturnIndex.Len() {
			for i := range tu.ri.InsertCols {
				colID := tu.ri.InsertCols[i].GetID()
				resultIndex, ok := tu.colIDToReturnIndex.Get(colID)
				if !ok || resultIndex != tu.ri.InsertColIDtoRowIndex.GetDefault(colID) {
					tu.insertReorderingRequired = true
					break
				}
			}
		} else {
			tu.insertReorderingRequired = true
		}
	}

	return nil
}

// makeResultFromRow reshapes a row that was inserted or updated to a row
// suitable for storing for a RETURNING clause, shaped by the target table's
// descriptor.
// There are two main examples of this reshaping:
// 1) A row may not contain values for nullable columns, so insert those NULLs.
// 2) Don't return values we wrote into non-public mutation columns.
func (tu *optTableUpserter) makeResultFromRow(
	row tree.Datums, colIDToRowIndex catalog.TableColMap,
) tree.Datums {
	resultRow := make(tree.Datums, tu.colIDToReturnIndex.Len())
	tu.colIDToReturnIndex.ForEach(func(colID descpb.ColumnID, returnIndex int) {
		rowIndex, ok := colIDToRowIndex.Get(colID)
		if ok {
			resultRow[returnIndex] = row[rowIndex]
		} else {
			// If the row doesn't have all columns filled out. Fill the columns that
			// weren't included with NULLs. This will only be true for nullable
			// columns.
			resultRow[returnIndex] = tree.DNull
		}
	})
	return resultRow
}

// desc is part of the tableWriter interface.
func (*optTableUpserter) desc() string { return "opt upserter" }

// row is part of the tableWriter interface.
func (tu *optTableUpserter) row(
	ctx context.Context, row tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	tu.currentBatchSize++

	// Consult the canary column to determine whether to insert or update. For
	// more details on how canary columns work, see the block comment on
	// Builder.buildInsert in opt/optbuilder/insert.go.
	insertEnd := len(tu.ri.InsertCols)
	if tu.canaryOrdinal == -1 {
		// No canary column means that existing row should be overwritten (i.e.
		// the insert and update columns are the same, so no need to choose).
		return tu.insertNonConflictingRow(ctx, tu.b, row[:insertEnd], pm, true /* overwrite */, traceKV)
	}
	if row[tu.canaryOrdinal] == tree.DNull {
		// No conflict, so insert a new row.
		return tu.insertNonConflictingRow(ctx, tu.b, row[:insertEnd], pm, false /* overwrite */, traceKV)
	}

	// If no columns need to be updated, then possibly collect the unchanged row.
	fetchEnd := insertEnd + len(tu.fetchCols)
	if len(tu.updateCols) == 0 {
		if !tu.rowsNeeded {
			return nil
		}
		_, err := tu.rows.AddRow(ctx, row[insertEnd:fetchEnd])
		return err
	}

	// Update the row.
	updateEnd := fetchEnd + len(tu.updateCols)
	return tu.updateConflictingRow(
		ctx,
		tu.b,
		row[insertEnd:fetchEnd],
		row[fetchEnd:updateEnd],
		pm,
		traceKV,
	)
}

// insertNonConflictingRow inserts the given source row into the table when
// there was no conflict. If the RETURNING clause was specified, then the
// inserted row is stored in the rowsUpserted collection.
func (tu *optTableUpserter) insertNonConflictingRow(
	ctx context.Context,
	b *kv.Batch,
	insertRow tree.Datums,
	pm row.PartialIndexUpdateHelper,
	overwrite, traceKV bool,
) error {
	// Perform the insert proper.
	if err := tu.ri.InsertRow(ctx, b, insertRow, pm, overwrite, traceKV); err != nil {
		return err
	}

	if !tu.rowsNeeded {
		return nil
	}

	// Reshape the row if needed.
	if tu.insertReorderingRequired {
		tableRow := tu.makeResultFromRow(insertRow, tu.ri.InsertColIDtoRowIndex)

		// TODO(ridwanmsharif): Why didn't they update the value of tu.resultRow
		//  before? Is it safe to be doing it now?
		// Map the upserted columns into the result row before adding it.
		for tabIdx := range tableRow {
			if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
				tu.resultRow[retIdx] = tableRow[tabIdx]
			}
		}
		_, err := tu.rows.AddRow(ctx, tu.resultRow)
		return err
	}

	// Map the upserted columns into the result row before adding it.
	for tabIdx := range insertRow {
		if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
			tu.resultRow[retIdx] = insertRow[tabIdx]
		}
	}
	_, err := tu.rows.AddRow(ctx, tu.resultRow)
	return err
}

// updateConflictingRow updates an existing row in the table when there was a
// conflict. The existing values from the row are provided in fetchRow, and the
// updated values are provided in updateValues. The updater is assumed to
// already be initialized with the descriptors for the fetch and update values.
// If the RETURNING clause was specified, then the updated row is stored in the
// rowsUpserted collection.
func (tu *optTableUpserter) updateConflictingRow(
	ctx context.Context,
	b *kv.Batch,
	fetchRow tree.Datums,
	updateValues tree.Datums,
	pm row.PartialIndexUpdateHelper,
	traceKV bool,
) error {
	// Enforce the column constraints.
	// Note: the column constraints are already enforced for fetchRow,
	// because:
	// - for the insert part, they were checked upstream in upsertNode
	//   via GenerateInsertRow().
	// - for the fetched part, we assume that the data in the table is
	//   correct already.
	if err := enforceLocalColumnConstraints(updateValues, tu.updateCols); err != nil {
		return err
	}

	// Queue the update in KV. This also returns an "update row"
	// containing the updated values for every column in the
	// table. This is useful for RETURNING, which we collect below.
	_, err := tu.ru.UpdateRow(ctx, b, fetchRow, updateValues, pm, traceKV)
	if err != nil {
		return err
	}

	// We only need a result row if we're collecting rows.
	if !tu.rowsNeeded {
		return nil
	}

	// We now need a row that has the shape of the result row with
	// the appropriate return columns. Make sure all the fetch columns
	// are present.
	tableRow := tu.makeResultFromRow(fetchRow, tu.ru.FetchColIDtoRowIndex)

	// Make sure all the updated columns are present.
	tu.colIDToReturnIndex.ForEach(func(colID descpb.ColumnID, returnIndex int) {
		// If an update value for a given column exists, use that; else use the
		// existing value of that column if it has been fetched.
		rowIndex, ok := tu.ru.UpdateColIDtoRowIndex.Get(colID)
		if ok {
			tableRow[returnIndex] = updateValues[rowIndex]
		}
	})

	// Map the upserted columns into the result row before adding it.
	for tabIdx := range tableRow {
		if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
			tu.resultRow[retIdx] = tableRow[tabIdx]
		}
	}

	// The resulting row may have nil values for columns that aren't
	// being upserted, updated or fetched.
	_, err = tu.rows.AddRow(ctx, tu.resultRow)
	return err
}

// tableDesc is part of the tableWriter interface.
func (tu *optTableUpserter) tableDesc() catalog.TableDescriptor {
	return tu.ri.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (tu *optTableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
}
