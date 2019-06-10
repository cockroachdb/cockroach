// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
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
	tableUpserterBase

	// fetchCols indicate which columns need to be fetched from the target table,
	// in order to detect whether a conflict has occurred, as well as to provide
	// existing values for updates.
	fetchCols []sqlbase.ColumnDescriptor

	// updateCols indicate which columns need an update during a conflict.
	updateCols []sqlbase.ColumnDescriptor

	// canaryOrdinal is the ordinal position of the column within the input row
	// that is used to decide whether to execute an insert or update operation.
	// If the canary column is null, then an insert will be performed; otherwise,
	// an update is performed. This column will always be one of the fetchCols.
	canaryOrdinal int

	// resultRow is a reusable slice of Datums used to store result rows.
	resultRow tree.Datums

	// fkTables is used for foreign key checks in the update case.
	fkTables row.FkTableMetadata

	// ru is used when updating rows.
	ru row.Updater
}

// init is part of the tableWriter interface.
func (tu *optTableUpserter) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	err := tu.tableUpserterBase.init(txn, evalCtx)
	if err != nil {
		return err
	}

	if tu.collectRows {
		tu.resultRow = make(tree.Datums, len(tu.colIDToReturnIndex))
	}

	tu.ru, err = row.MakeUpdater(
		txn,
		tu.tableDesc(),
		tu.fkTables,
		tu.updateCols,
		tu.fetchCols,
		row.UpdaterDefault,
		evalCtx,
		tu.alloc,
	)
	return err
}

// desc is part of the tableWriter interface.
func (*optTableUpserter) desc() string { return "opt upserter" }

// row is part of the tableWriter interface.
func (tu *optTableUpserter) row(ctx context.Context, row tree.Datums, traceKV bool) error {
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

// atBatchEnd is part of the extendedTableWriter interface.
func (tu *optTableUpserter) atBatchEnd(ctx context.Context, traceKV bool) error {
	// Nothing to do, because the row method does everything.
	return nil
}

// insertNonConflictingRow inserts the given source row into the table when
// there was no conflict. If the RETURNING clause was specified, then the
// inserted row is stored in the rowsUpserted collection.
func (tu *optTableUpserter) insertNonConflictingRow(
	ctx context.Context, b *client.Batch, insertRow tree.Datums, overwrite, traceKV bool,
) error {
	// Perform the insert proper.
	if err := tu.ri.InsertRow(
		ctx, b, insertRow, overwrite, row.CheckFKs, traceKV); err != nil {
		return err
	}

	if !tu.collectRows {
		return nil
	}

	// Reshape the row if needed.
	if tu.insertReorderingRequired {
		resultRow := tu.makeResultFromRow(insertRow, tu.ri.InsertColIDtoRowIndex)
		_, err := tu.rowsUpserted.AddRow(ctx, resultRow)
		return err
	}

	_, err := tu.rowsUpserted.AddRow(ctx, insertRow)
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
	b *client.Batch,
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
	_, err := tu.ru.UpdateRow(ctx, b, fetchRow, updateValues, row.CheckFKs, traceKV)
	if err != nil {
		return err
	}

	// We only need a result row if we're collecting rows.
	if !tu.collectRows {
		return nil
	}

	// We now need a row that has the shape of the result row.
	for colID, returnIndex := range tu.colIDToReturnIndex {
		// If an update value for a given column exists, use that; else use the
		// existing value of that column.
		rowIndex, ok := tu.ru.UpdateColIDtoRowIndex[colID]
		if ok {
			tu.resultRow[returnIndex] = updateValues[rowIndex]
		} else {
			rowIndex, ok = tu.ru.FetchColIDtoRowIndex[colID]
			if !ok {
				return errors.AssertionFailedf("no existing value is available for column")
			}
			tu.resultRow[returnIndex] = fetchRow[rowIndex]
		}
	}

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
