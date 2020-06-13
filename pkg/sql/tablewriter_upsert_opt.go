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
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
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

	ri    row.Inserter
	alloc *sqlbase.DatumAlloc

	// Should we collect the rows for a RETURNING clause?
	collectRows bool

	// Rows returned if collectRows is true.
	rowsUpserted *rowcontainer.RowContainer

	// A mapping of column IDs to the return index used to shape the resulting
	// rows to those required by the returning clause. Only required if
	// collectRows is true.
	colIDToReturnIndex map[sqlbase.ColumnID]int

	// Do the result rows have a different order than insert rows. Only set if
	// collectRows is true.
	insertReorderingRequired bool

	// resultCount is the number of upserts. Mirrors rowsUpserted.Len() if
	// collectRows is set, counted separately otherwise.
	resultCount int

	// Contains all the rows to be inserted.
	insertRows rowcontainer.RowContainer

	// existingRows is used to store rows in a batch when checking for conflicts
	// with rows earlier in the batch. Is is reused per batch.
	existingRows *rowcontainer.RowContainer

	// For allocation avoidance.
	indexKeyPrefix []byte

	// fetchCols indicate which columns need to be fetched from the target table,
	// in order to detect whether a conflict has occurred, as well as to provide
	// existing values for updates.
	fetchCols []sqlbase.ColumnDescriptor

	// updateCols indicate which columns need an update during a conflict.
	updateCols []sqlbase.ColumnDescriptor

	// returnCols indicate which columns need to be returned by the Upsert.
	returnCols []sqlbase.ColumnDescriptor

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
	ctx context.Context, txn *kv.Txn, evalCtx *tree.EvalContext,
) error {
	tu.tableWriterBase.init(txn)
	tableDesc := tu.tableDesc()

	tu.insertRows.Init(
		evalCtx.Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColDescs(tu.ri.InsertCols), 0,
	)

	// collectRows, set upon initialization, indicates whether or not we want rows returned from the operation.
	if tu.collectRows {
		tu.rowsUpserted = rowcontainer.NewRowContainer(
			evalCtx.Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromColDescs(tableDesc.Columns),
			tu.insertRows.Len(),
		)

		// Create the map from colIds to the expected columns.
		// Note that this map will *not* contain any mutation columns - that's
		// because even though we might insert values into mutation columns, we
		// never return them back to the user.
		tu.colIDToReturnIndex = map[sqlbase.ColumnID]int{}
		for i := range tableDesc.Columns {
			id := tableDesc.Columns[i].ID
			tu.colIDToReturnIndex[id] = i
		}

		if len(tu.ri.InsertColIDtoRowIndex) == len(tu.colIDToReturnIndex) {
			for colID, insertIndex := range tu.ri.InsertColIDtoRowIndex {
				resultIndex, ok := tu.colIDToReturnIndex[colID]
				if !ok || resultIndex != insertIndex {
					tu.insertReorderingRequired = true
					break
				}
			}
		} else {
			tu.insertReorderingRequired = true
		}
	}

	tu.insertRows.Init(
		evalCtx.Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColDescs(tu.ri.InsertCols), 0,
	)

	tu.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(
		evalCtx.Codec, tableDesc.TableDesc(), tableDesc.PrimaryIndex.ID,
	)

	if tu.collectRows {
		tu.resultRow = make(tree.Datums, len(tu.returnCols))
		tu.rowsUpserted = rowcontainer.NewRowContainer(
			evalCtx.Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromColDescs(tu.returnCols),
			tu.insertRows.Len(),
		)
	}

	return nil
}

// flushAndStartNewBatch is part of the tableWriter interface.
func (tu *optTableUpserter) flushAndStartNewBatch(ctx context.Context) error {
	tu.insertRows.Clear(ctx)
	if tu.collectRows {
		tu.rowsUpserted.Clear(ctx)
	}
	if tu.existingRows != nil {
		tu.existingRows.Clear(ctx)
	}
	return tu.tableWriterBase.flushAndStartNewBatch(ctx, tu.tableDesc())
}

// batchedCount is part of the batchedTableWriter interface.
func (tu *optTableUpserter) batchedCount() int { return tu.resultCount }

// batchedValues is part of the batchedTableWriter interface.
func (tu *optTableUpserter) batchedValues(rowIdx int) tree.Datums {
	if !tu.collectRows {
		panic("return row requested but collect rows was not set")
	}
	return tu.rowsUpserted.At(rowIdx)
}

func (tu *optTableUpserter) curBatchSize() int { return tu.insertRows.Len() }

// close is part of the tableWriter interface.
func (tu *optTableUpserter) close(ctx context.Context) {
	tu.insertRows.Close(ctx)
	if tu.existingRows != nil {
		tu.existingRows.Close(ctx)
	}
	if tu.rowsUpserted != nil {
		tu.rowsUpserted.Close(ctx)
	}
}

// finalize is part of the tableWriter interface.
func (tu *optTableUpserter) finalize(
	ctx context.Context, traceKV bool,
) (*rowcontainer.RowContainer, error) {
	return nil, tu.tableWriterBase.finalize(ctx, tu.tableDesc())
}

// makeResultFromRow reshapes a row that was inserted or updated to a row
// suitable for storing for a RETURNING clause, shaped by the target table's
// descriptor.
// There are two main examples of this reshaping:
// 1) A row may not contain values for nullable columns, so insert those NULLs.
// 2) Don't return values we wrote into non-public mutation columns.
func (tu *optTableUpserter) makeResultFromRow(
	row tree.Datums, colIDToRowIndex map[sqlbase.ColumnID]int,
) tree.Datums {
	resultRow := make(tree.Datums, len(tu.colIDToReturnIndex))
	for colID, returnIndex := range tu.colIDToReturnIndex {
		rowIndex, ok := colIDToRowIndex[colID]
		if ok {
			resultRow[returnIndex] = row[rowIndex]
		} else {
			// If the row doesn't have all columns filled out. Fill the columns that
			// weren't included with NULLs. This will only be true for nullable
			// columns.
			resultRow[returnIndex] = tree.DNull
		}
	}
	return resultRow
}

// desc is part of the tableWriter interface.
func (*optTableUpserter) desc() string { return "opt upserter" }

// row is part of the tableWriter interface.
// TODO(mgartner): Use ignoreIndexes to avoid writing to partial indexes when
// the row does not match the partial index predicate.
func (tu *optTableUpserter) row(
	ctx context.Context, row tree.Datums, ignoreIndexes util.FastIntSet, traceKV bool,
) error {
	tu.batchSize++
	tu.resultCount++

	// Consult the canary column to determine whether to insert or update. For
	// more details on how canary columns work, see the block comment on
	// Builder.buildInsert in opt/optbuilder/insert.go.
	insertEnd := len(tu.ri.InsertCols)
	if tu.canaryOrdinal == -1 {
		// No canary column means that existing row should be overwritten (i.e.
		// the insert and update columns are the same, so no need to choose).
		return tu.insertNonConflictingRow(ctx, tu.b, row[:insertEnd], true /* overwrite */, traceKV)
	}
	if row[tu.canaryOrdinal] == tree.DNull {
		// No conflict, so insert a new row.
		return tu.insertNonConflictingRow(ctx, tu.b, row[:insertEnd], false /* overwrite */, traceKV)
	}

	// If no columns need to be updated, then possibly collect the unchanged row.
	fetchEnd := insertEnd + len(tu.fetchCols)
	if len(tu.updateCols) == 0 {
		if !tu.collectRows {
			return nil
		}
		_, err := tu.rowsUpserted.AddRow(ctx, row[insertEnd:fetchEnd])
		return err
	}

	// Update the row.
	updateEnd := fetchEnd + len(tu.updateCols)
	return tu.updateConflictingRow(
		ctx,
		tu.b,
		row[insertEnd:fetchEnd],
		row[fetchEnd:updateEnd],
		tu.tableDesc(),
		traceKV,
	)
}

// atBatchEnd is part of the tableWriter interface.
func (tu *optTableUpserter) atBatchEnd(ctx context.Context, traceKV bool) error {
	// Nothing to do, because the row method does everything.
	return nil
}

// insertNonConflictingRow inserts the given source row into the table when
// there was no conflict. If the RETURNING clause was specified, then the
// inserted row is stored in the rowsUpserted collection.
func (tu *optTableUpserter) insertNonConflictingRow(
	ctx context.Context, b *kv.Batch, insertRow tree.Datums, overwrite, traceKV bool,
) error {
	// Perform the insert proper.
	// TODO(mgartner): Pass ignoreIndexes to InsertRow and do not write index
	// entries for indexes in the set.
	var ignoreIndexes util.FastIntSet
	if err := tu.ri.InsertRow(ctx, b, insertRow, ignoreIndexes, overwrite, traceKV); err != nil {
		return err
	}

	if !tu.collectRows {
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
		_, err := tu.rowsUpserted.AddRow(ctx, tu.resultRow)
		return err
	}

	// Map the upserted columns into the result row before adding it.
	for tabIdx := range insertRow {
		if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
			tu.resultRow[retIdx] = insertRow[tabIdx]
		}
	}
	_, err := tu.rowsUpserted.AddRow(ctx, tu.resultRow)
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
	tableDesc *sqlbase.ImmutableTableDescriptor,
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
	_, err := tu.ru.UpdateRow(ctx, b, fetchRow, updateValues, traceKV)
	if err != nil {
		return err
	}

	// We only need a result row if we're collecting rows.
	if !tu.collectRows {
		return nil
	}

	// We now need a row that has the shape of the result row with
	// the appropriate return columns. Make sure all the fetch columns
	// are present.
	tableRow := tu.makeResultFromRow(fetchRow, tu.ru.FetchColIDtoRowIndex)

	// Make sure all the updated columns are present.
	for colID, returnIndex := range tu.colIDToReturnIndex {
		// If an update value for a given column exists, use that; else use the
		// existing value of that column if it has been fetched.
		rowIndex, ok := tu.ru.UpdateColIDtoRowIndex[colID]
		if ok {
			tableRow[returnIndex] = updateValues[rowIndex]
		}
	}

	// Map the upserted columns into the result row before adding it.
	for tabIdx := range tableRow {
		if retIdx := tu.tabColIdxToRetIdx[tabIdx]; retIdx >= 0 {
			tu.resultRow[retIdx] = tableRow[tabIdx]
		}
	}

	// The resulting row may have nil values for columns that aren't
	// being upserted, updated or fetched.
	_, err = tu.rowsUpserted.AddRow(ctx, tu.resultRow)
	return err
}

// tableDesc is part of the tableWriter interface.
func (tu *optTableUpserter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ri.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (tu *optTableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
}
