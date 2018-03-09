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

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// expressionCarrier handles visiting sub-expressions.
type expressionCarrier interface {
	// walkExprs explores all sub-expressions held by this object, if
	// any.
	walkExprs(func(desc string, index int, expr tree.TypedExpr))
}

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//   err := tw.init(txn, evalCtx)
//   // Handle err.
//   for {
//      values := ...
//      row, err := tw.row(values)
//      // Handle err.
//   }
//   err := tw.finalize()
//   // Handle err.
type tableWriter interface {
	expressionCarrier

	// init provides the tableWriter with a Txn and optional monitor to write to
	// and returns an error if it was misconfigured.
	init(*client.Txn, *tree.EvalContext) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
	// The passed Datums is not used after `row` returns. The returned Datums is
	// suitable for use with returningHelper.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. We use a separate argument here instead
	// of a Value field on the context because Value access in context.Context
	// is rather expensive and the tableWriter interface is used on the
	// inner loop of table accesses.
	row(context.Context, tree.Datums, bool /* traceKV */) (tree.Datums, error)

	// finalize flushes out any remaining writes. It is called after all calls to
	// row.  It returns a slice of all Datums not yet returned by calls to `row`.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. See the comment above for why
	// this a separate parameter as opposed to a Value field on the context.
	//
	// autoCommit specifies whether the tableWriter is free to commit the txn in
	// which it was operating once all writes are performed.
	finalize(
		ctx context.Context, autoCommit autoCommitOpt, traceKV bool,
	) (*sqlbase.RowContainer, error)

	// tableDesc returns the TableDescriptor for the table that the tableWriter
	// will modify.
	tableDesc() *sqlbase.TableDescriptor

	// fkSpanCollector returns the FkSpanCollector for the tableWriter.
	fkSpanCollector() sqlbase.FkSpanCollector

	// close frees all resources held by the tableWriter.
	close(context.Context)
}

type autoCommitOpt int

const (
	noAutoCommit autoCommitOpt = iota
	autoCommitEnabled
)

var _ tableWriter = (*tableInserter)(nil)
var _ tableWriter = (*tableUpdater)(nil)
var _ tableWriter = (*tableUpserter)(nil)
var _ tableWriter = (*tableDeleter)(nil)

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	ri sqlbase.RowInserter

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}

func (ti *tableInserter) init(txn *client.Txn, _ *tree.EvalContext) error {
	ti.txn = txn
	ti.b = txn.NewBatch()
	return nil
}

func (ti *tableInserter) row(
	ctx context.Context, values tree.Datums, traceKV bool,
) (tree.Datums, error) {
	return nil, ti.ri.InsertRow(ctx, ti.b, values, false, sqlbase.CheckFKs, traceKV)
}

func (ti *tableInserter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, _ bool,
) (*sqlbase.RowContainer, error) {
	var err error
	if autoCommit == autoCommitEnabled {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = ti.txn.CommitInBatch(ctx, ti.b)
	} else {
		err = ti.txn.Run(ctx, ti.b)
	}

	if err != nil {
		return nil, sqlbase.ConvertBatchError(ctx, ti.ri.Helper.TableDesc, ti.b)
	}
	return nil, nil
}

func (ti *tableInserter) tableDesc() *sqlbase.TableDescriptor {
	return ti.ri.Helper.TableDesc
}

func (ti *tableInserter) fkSpanCollector() sqlbase.FkSpanCollector {
	return ti.ri.Fks
}

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	ru sqlbase.RowUpdater

	// Set by init.
	txn     *client.Txn
	evalCtx *tree.EvalContext
	b       *client.Batch
}

func (ti *tableInserter) close(_ context.Context) {}

func (tu *tableUpdater) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}

func (tu *tableUpdater) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	tu.txn = txn
	tu.evalCtx = evalCtx
	tu.b = txn.NewBatch()
	return nil
}

func (tu *tableUpdater) row(
	ctx context.Context, values tree.Datums, traceKV bool,
) (tree.Datums, error) {
	oldValues := values[:len(tu.ru.FetchCols)]
	updateValues := values[len(tu.ru.FetchCols):]
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues, sqlbase.CheckFKs, traceKV)
}

func (tu *tableUpdater) finalize(
	ctx context.Context, autoCommit autoCommitOpt, _ bool,
) (*sqlbase.RowContainer, error) {
	var err error
	if autoCommit == autoCommitEnabled {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tu.txn.CommitInBatch(ctx, tu.b)
	} else {
		err = tu.txn.Run(ctx, tu.b)
	}

	if err != nil {
		return nil, sqlbase.ConvertBatchError(ctx, tu.ru.Helper.TableDesc, tu.b)
	}
	return nil, nil
}

func (tu *tableUpdater) tableDesc() *sqlbase.TableDescriptor {
	return tu.ru.Helper.TableDesc
}

func (tu *tableUpdater) fkSpanCollector() sqlbase.FkSpanCollector {
	return tu.ru.Fks
}

func (tu *tableUpdater) close(_ context.Context) {}

type tableUpsertEvaler interface {
	expressionCarrier

	// TODO(dan): The tableUpsertEvaler interface separation was an attempt to
	// keep sql logic out of the mapping between table rows and kv operations.
	// Unfortunately, it was a misguided effort. tableUpserter's responsibilities
	// should really be defined as those needed in distributed sql leaf nodes,
	// which will necessarily include expr evaluation.

	// eval populates into resultRow the values for the update case of
	// an upsert, given the row that would have been inserted and the
	// existing (conflicting) values.
	eval(insertRow, existingRow, resultRow tree.Datums) (tree.Datums, error)

	// evalComputedCols evaluates the computed columns for this upsert using the
	// values in updatedRow and appends the results, in order, to appendTo,
	// returning the result.
	evalComputedCols(updatedRow tree.Datums, appendTo tree.Datums) (tree.Datums, error)

	// shouldUpdate returns the result of evaluating the WHERE clause of the
	// ON CONFLICT ... DO UPDATE clause.
	shouldUpdate(insertRow tree.Datums, existingRow tree.Datums) (bool, error)
}

// tableUpserter handles writing kvs and forming table rows for upserts.
//
// There are two distinct "modes" that tableUpserter can use, one of
// which is selected by newUpsertNode(). In the general mode, rows to
// be inserted are batched up from calls to `row` and then the upsert
// is processed using finalize(). The other mode is the fast
// path. See fastTableUpserter below.
//
// In the general mode, the insert rows are accumulated in insertRows.
// Then during finalize(), the final upsert processing occurs. This
// uses 1 or 2 `client.Batch`s from the init'd txn to fetch the
// existing (conflicting) values, followed by one more `client.Batch`
// with the appropriate inserts and updates. In this case, all
// necessary `client.Batch`s are created and run within the lifetime
// of `finalize`.
//
type tableUpserter struct {
	ri sqlbase.RowInserter

	// insertRows are the rows produced by the insertion data source.
	// These are accumulated while iterating on the insertion data source.
	insertRows sqlbase.RowContainer

	// updateCols indicates which columns need an update during a
	// conflict.  There is one entry per column descriptors in the
	// table. However only the entries identified in the conflict clause
	// of the original statement will be populated, to disambiguate
	// columns that need an update from those that don't.
	updateCols []sqlbase.ColumnDescriptor

	conflictIndex sqlbase.IndexDescriptor
	alloc         *sqlbase.DatumAlloc
	collectRows   bool
	anyComputed   bool

	// These are set for ON CONFLICT DO UPDATE, but not for DO NOTHING
	evaler *upsertHelper

	// updateValues is the temporary buffer for rows computed by
	// evaler.eval(). It is reused from one row to the next to limit
	// allocations.
	updateValues tree.Datums

	// Set by init.
	txn                   *client.Txn
	evalCtx               *tree.EvalContext
	fkTables              sqlbase.TableLookupsByID // for fk checks in update case
	ru                    sqlbase.RowUpdater
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	fetchCols             []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	fetcher               sqlbase.RowFetcher

	// Rows returned if collectRows is true.
	rowsUpserted *sqlbase.RowContainer
	// rowTemplate is used to prepare rows to add to rowsUpserted.
	rowTemplate tree.Datums
	// rowIdxToRetIdx maps the indices in the inserted rows
	// back to indices in rowTemplate.
	rowIdxToRetIdx []int

	// For allocation avoidance.
	indexKeyPrefix []byte
}

func (tu *tableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
	if tu.evaler != nil {
		tu.evaler.walkExprs(walk)
	}
}

func (tu *tableUpserter) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	tableDesc := tu.tableDesc()

	tu.txn = txn
	tu.evalCtx = evalCtx
	tu.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)

	tu.insertRows.Init(
		tu.evalCtx.Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColDescs(tu.ri.InsertCols), 0,
	)

	if tu.collectRows {
		tu.rowsUpserted = sqlbase.NewRowContainer(
			tu.evalCtx.Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromColDescs(tableDesc.Columns),
			tu.insertRows.Len(),
		)

		// In some cases (e.g. `INSERT INTO t (a) ...`) rowVals does not contain
		// all the table columns. We need to pass values for all table columns
		// to rh, in the correct order; we will use rowTemplate for this. We
		// also need a table that maps row indices to rowTemplate indices to
		// fill in the row values; any absent values will be NULLs.
		tu.rowTemplate = make(tree.Datums, len(tableDesc.Columns))
	}

	colIDToRetIndex := map[sqlbase.ColumnID]int{}
	for i, col := range tableDesc.Columns {
		colIDToRetIndex[col.ID] = i
	}

	tu.rowIdxToRetIdx = make([]int, len(tu.ri.InsertCols))
	for i, col := range tu.ri.InsertCols {
		tu.rowIdxToRetIdx[i] = colIDToRetIndex[col.ID]
	}

	// TODO(dan): This could be made tighter, just the rows needed for the ON
	// CONFLICT and RETURNING exprs.
	requestedCols := tableDesc.Columns

	if len(tu.updateCols) == 0 {
		tu.fetchCols = requestedCols
		tu.fetchColIDtoRowIndex = sqlbase.ColIDtoRowIndexFromCols(requestedCols)
	} else {
		var err error
		tu.ru, err = sqlbase.MakeRowUpdater(
			txn,
			tableDesc,
			tu.fkTables,
			tu.updateCols,
			requestedCols,
			sqlbase.RowUpdaterDefault,
			tu.evalCtx,
			tu.alloc,
		)
		if err != nil {
			return err
		}
		// t.ru.fetchCols can also contain columns undergoing mutation.
		tu.fetchCols = tu.ru.FetchCols
		tu.fetchColIDtoRowIndex = tu.ru.FetchColIDtoRowIndex

		tu.updateColIDtoRowIndex = make(map[sqlbase.ColumnID]int)
		for i, updateCol := range tu.ru.UpdateCols {
			tu.updateColIDtoRowIndex[updateCol.ID] = i
		}
	}

	tu.insertRows.Init(
		tu.evalCtx.Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColDescs(tu.ri.InsertCols), 0,
	)

	var valNeededForCol util.FastIntSet
	for i, col := range tu.fetchCols {
		if _, ok := tu.fetchColIDtoRowIndex[col.ID]; ok {
			valNeededForCol.Add(i)
		}
	}

	tableArgs := sqlbase.RowFetcherTableArgs{
		Desc:            tableDesc,
		Index:           &tableDesc.PrimaryIndex,
		ColIdxMap:       tu.fetchColIDtoRowIndex,
		Cols:            tu.fetchCols,
		ValNeededForCol: valNeededForCol,
	}

	return tu.fetcher.Init(
		false /* reverse */, false /*returnRangeInfo*/, false /* isCheck */, tu.alloc, tableArgs,
	)
}

func (tu *tableUpserter) row(
	ctx context.Context, row tree.Datums, traceKV bool,
) (tree.Datums, error) {
	// TODO(dan): If len(tu.insertRows) > some threshold, call flush().
	return tu.insertRows.AddRow(ctx, row)
}

// finalize commits to tu.txn any rows batched up in tu.insertRows.
func (tu *tableUpserter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, traceKV bool,
) (*sqlbase.RowContainer, error) {
	// Fetch the information about which rows in tu.insertRows currently
	// conflict with rows in-db.
	existingRows, pkToRowIdx, conflictingPKs, err := tu.fetchExisting(ctx, traceKV)
	if err != nil {
		return nil, err
	}

	// At this point existingRows contains data for the conflicting
	// rows, and pkToRowIdx maps PKs in tu.insertRows that are known to
	// have a conflict to an entry in existingRows.

	// During the upsert processing below, existingRows will contain
	// initially the data from KV, but will be extended with each new
	// inserted row that didn't exist in KV. Then each update will
	// modify existingRows in-place, so that subsequent updates can
	// discover the modified values.

	tableDesc := tu.tableDesc()
	b := tu.txn.NewBatch()

	for i := 0; i < tu.insertRows.Len(); i++ {
		insertRow := tu.insertRows.At(i)

		// conflictingRowPK will be the key of the conflicting row. This may
		// be different from insertRow's PK if the conflicting index is a
		// secondary index.
		conflictingRowPK, err := tu.getConflictingRowPK(insertRow, i, conflictingPKs, tableDesc)
		if err != nil {
			return nil, err
		}

		// At this point, conflictingRowPK is either:
		// - nil if a secondary index was used and it was determined there
		//   is no conflict already;
		// - non-nil if a conflict may be present. In that case
		//   we must consult pkToRowIdx to determine whether we already
		//   have data (i.e. a conflict) in existingRows.

		// conflictingRowIdx will be set to a valid value if a conflict is
		// detected.
		conflictingRowIdx := -1
		if conflictingRowPK != nil {
			if rowIdx, ok := pkToRowIdx[string(conflictingRowPK)]; ok {
				// There was a conflict after all.
				conflictingRowIdx = rowIdx
			}
		}

		// We'll use resultRow to produce a RETURNING row below, if one is needed.
		var resultRow tree.Datums

		// Do we have a conflict?
		if conflictingRowIdx == -1 {
			// We don't have a conflict. This is a new row in KV. Create it.
			resultRow, existingRows, err = tu.insertNonConflictingRow(
				ctx, b, insertRow, conflictingRowPK, existingRows, pkToRowIdx, tableDesc, traceKV)
			if err != nil {
				return nil, err
			}
		} else {
			// There was a row already. Do we need to update it?

			if len(tu.updateCols) == 0 {
				// If len(tu.updateCols) == 0, then we're in the DO NOTHING
				// case. There is no update to be done, also no result row to be collected.
				// See the pg docs, e.g.: https://www.postgresql.org/docs/10/static/sql-insert.html
				//
				//     The optional RETURNING clause causes INSERT to compute and
				//     return value(s) based on each row actually inserted (or
				//     updated, if an ON CONFLICT DO UPDATE clause was used). This
				//     is primarily useful for obtaining values that were supplied
				//     by defaults, such as a serial sequence number. However, any
				//     expression using the table's columns is allowed. The syntax
				//     of the RETURNING list is identical to that of the output list
				//     of SELECT. Only rows that were successfully inserted or
				//     updated will be returned.
				//
				continue
			}

			// This is the ON CONFLICT DO UPDATE ... clause.
			//
			// However we still don't know yet whether to do an update;
			// we'll need to ask the WHERE clause first, if any.

			// existingRow carries the values previously seen in
			// KV or newly inserted earlier in this batch.
			existingRow := existingRows[conflictingRowIdx]

			// Check the ON CONFLICT DO UPDATE WHERE ... clause.
			conflictingRowValues := existingRow[:len(tu.ru.FetchCols)]
			shouldUpdate, err := tu.evaler.shouldUpdate(insertRow, conflictingRowValues)
			if err != nil {
				return nil, err
			}
			if !shouldUpdate {
				// WHERE tells us there is nothing to do. Stop here.
				// There is also no RETURNING result.
				// See https://www.postgresql.org/docs/10/static/sql-insert.html and the
				// quoted excerpt above.
				continue
			}

			// We know there was a row already, and we know we need to update it. Do it.
			resultRow, existingRows, err = tu.updateConflictingRow(
				ctx, b, insertRow,
				conflictingRowPK, conflictingRowIdx, conflictingRowValues,
				existingRows, pkToRowIdx,
				tableDesc, traceKV)
			if err != nil {
				return nil, err
			}
		}

		// Do we need to remember a result for RETURNING?
		if tu.collectRows {
			// Yes, collect it.
			_, err = tu.rowsUpserted.AddRow(ctx, resultRow)
			if err != nil {
				return nil, err
			}
		}
	}

	// The upsert resolution is finished.
	// This has prepared a KV batch in b.
	// Now run/commit the KV batch.

	if autoCommit == autoCommitEnabled {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tu.txn.CommitInBatch(ctx, b)
	} else {
		err = tu.txn.Run(ctx, b)
	}
	if err != nil {
		return nil, sqlbase.ConvertBatchError(ctx, tableDesc, b)
	}
	return tu.rowsUpserted, nil
}

// updateConflictingRow updates the existing row
// in the table, when there was a conflict.
// Inputs:
// - b is the KV batch to use for the insert.
// - insertRow is the new row to upsert, containing the "excluded" values.
// - conflictingRowPK is the PK of the previously seen conflicting row.
// - conflictingRowIdx is the index of the values of the previously seen conflicting row in existingRows.
// - conflictingRowValues is the prefetched existingRows[conflictingRowIdx].
// Outputs:
// - resultRow is the row that was updated, shaped in the order
//   of the table descriptor. This may be different than the
//   shape of insertRow if there are nullable columns.
// Input/Outputs:
// - existingRows contains the previously seen rows, and is modified
//   or extended depending on how the PK columns are updated by the SET
//   clauses.
// - pkToRowIdx is extended with the index of the new entry in existingRows.
func (tu *tableUpserter) updateConflictingRow(
	ctx context.Context,
	b *client.Batch,
	insertRow tree.Datums,
	conflictingRowPK roachpb.Key,
	conflictingRowIdx int,
	conflictingRowValues tree.Datums,
	existingRows []tree.Datums,
	pkToRowIdx map[string]int,
	tableDesc *sqlbase.TableDescriptor,
	traceKV bool,
) (resultRow tree.Datums, newExistingRows []tree.Datums, err error) {
	// First compute all the updates via SET (or the pseudo-SET generated
	// for UPSERT statements).
	updateValues, err := tu.evaler.eval(insertRow, conflictingRowValues, tu.updateValues)
	if err != nil {
		return nil, nil, err
	}

	// Do we need to (re-)compute computed columns?
	if tu.anyComputed {
		// Yes, do it. This appends the
		// computed columns at the end of updateValues.
		//
		// TODO(justin): We're currently wasteful here: we construct the
		// result row *twice* because we need it once to evaluate any computed
		// columns and again to actually perform the update. we need to find a
		// way to reuse it. I'm not sure right now how best to factor this -
		// suggestions welcome.
		// TODO(nathan/knz): Reuse a row buffer here.
		newValues := make([]tree.Datum, len(conflictingRowValues))
		copy(newValues, conflictingRowValues)
		for i, updateValue := range updateValues {
			newValues[tu.ru.FetchColIDtoRowIndex[tu.ru.UpdateCols[i].ID]] = updateValue
		}

		// Now that we have a complete row except for its computed columns,
		// since the computed columns are at the end of the update row, we
		// must evaluate the computed columns and add the results to the end
		// of updateValues.
		updateValues, err = tu.evaler.evalComputedCols(newValues, updateValues)
		if err != nil {
			return nil, nil, err
		}
	}

	// Queue the update in KV. This also returns an "update row"
	// containing the updated values for every column in the
	// table. This is useful for RETURNING, which we collect below.
	updatedRow, err := tu.ru.UpdateRow(
		ctx, b, conflictingRowValues, updateValues, sqlbase.CheckFKs, traceKV,
	)
	if err != nil {
		return nil, nil, err
	}

	// Keep the slice for reuse.
	tu.updateValues = updateValues[:0]

	// Maybe the PK was updated by SET. We need to recompute a fresh PK
	// for the current row from updatedRow. We use
	// tu.evaler.ccIvarContainer.mapping which contains the suitable
	// mapping for the table columns already.
	updatedConflictingRowPK, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, tu.evaler.ccIvarContainer.mapping, updatedRow, tu.indexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}

	// It's possible that the PK for the updated values is different
	// from the PK for the original conflicting row, if the SET has
	// updated some of the PK columns. We need to detect that in
	// pkChanged.
	var pkChanged bool

	// Now update the known data in existingRows.
	// Perhaps we just also inserted a new row.
	if updatedConflictingRowIdx, ok := pkToRowIdx[string(updatedConflictingRowPK)]; ok {
		// This case indicates that the (possibly new) PK of the updated
		// row was already seen in a previous iteration (by a previous
		// upsert resolution).
		//
		// We need to update that known copy, so that subsequent
		// iterations can find it.
		copy(existingRows[updatedConflictingRowIdx], updatedRow)

		// The following line is meant to read:
		//
		//    pkChanged = !bytes.Equal(updatedConflictingRowPK, conflictingRowPK)
		//
		// However we already know that the row indices in existingRows are different
		// if the PKs are different, so we can compare the row indices instead
		// for efficiency.
		pkChanged = updatedConflictingRowIdx != conflictingRowIdx
	} else {
		// We're sure to have a new PK.
		pkChanged = true

		// Now add the new one.
		existingRows = appendKnownConflictingRow(updatedRow, updatedConflictingRowPK, existingRows, pkToRowIdx)
	}

	if pkChanged {
		// The previous PK is guaranteed to not exist any more. Remove it.
		delete(pkToRowIdx, string(conflictingRowPK))
	}

	// We're done!
	return updatedRow, existingRows, nil
}

// insertNonConflictingRow inserts the source row insertRow
// into the table, when there was no conflict.
// Inputs:
// - b is the KV batch to use for the insert.
// - insertRow is the new row to insert.
// - conflictingRowPK is the PK of that new row, if it is known already
//   (e.g. by getConflictingRowPK from the primary index).
// Outputs:
// - resultRow is the row that was inserted, shaped in the order
//   of the table descriptor. This may be different than the
//   shape of insertRow if there are nullablec olumns.
// Input/Outputs:
// - existingRows is extended with resultRow to produce newExistingRows.
// - pkToRowIdx is extended with the index of the new entry in existingRows.
func (tu *tableUpserter) insertNonConflictingRow(
	ctx context.Context,
	b *client.Batch,
	insertRow tree.Datums,
	conflictingRowPK roachpb.Key,
	existingRows []tree.Datums,
	pkToRowIdx map[string]int,
	tableDesc *sqlbase.TableDescriptor,
	traceKV bool,
) (resultRow tree.Datums, newExistingRows []tree.Datums, err error) {
	// Perform the insert proper.
	if err := tu.ri.InsertRow(
		ctx, b, insertRow, false /* ignoreConflicts */, sqlbase.CheckFKs, traceKV); err != nil {
		return nil, nil, err
	}

	// We may not know the conflictingRowPK yet for the new row, for
	// example when the conflicting index was a secondary index.
	// In that case, compute it now.
	if conflictingRowPK == nil {
		conflictingRowPK, _, err = sqlbase.EncodeIndexKey(
			tableDesc, &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
		if err != nil {
			return nil, nil, err
		}
	}

	// We now need a row that has the shape of the result row.
	resultRow = tu.makeResultFromInsertRow(insertRow, tableDesc.Columns)
	// Then remember it for further upserts.
	existingRows = appendKnownConflictingRow(resultRow, conflictingRowPK, existingRows, pkToRowIdx)

	return resultRow, existingRows, nil
}

// appendKnownConflictingRow adds a new row to existingRows and
// remembers its position in pkToRowIdx.
func appendKnownConflictingRow(
	newRow tree.Datums, newRowPK roachpb.Key, existingRows []tree.Datums, pkToRowIdx map[string]int,
) (newExistingRows []tree.Datums) {
	pkToRowIdx[string(newRowPK)] = len(existingRows)
	return append(existingRows, newRow)
}

// getConflictingRowPK returns the primary key of the row that may
// conflict with insertRow, if any. It returns a nil PK if there was
// definitely no conflict.
//
// rowIdx is the index of insertRow in tu.insertRows, and can be used
// to index conflictingPKs.
//
// conflictingPKs is an array of pre-computed PKs for each row in
// tu.insertRows, in the case the conflict was resolved using a
// secondary index.
func (tu *tableUpserter) getConflictingRowPK(
	insertRow tree.Datums,
	rowIdx int,
	conflictingPKs map[int]roachpb.Key,
	tableDesc *sqlbase.TableDescriptor,
) (conflictingRowPK roachpb.Key, err error) {
	if conflictingPKs != nil {
		// If a secondary index helped us find the conflicting PK for this
		// row, use that information. A nil here indicates the row
		// definitely did not exist (because it was not present in the
		// secondary index).
		return conflictingPKs[rowIdx], nil
	}

	// Otherwise, encode the values to determine the primary key.
	insertRowPK, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
	return insertRowPK, err
}

// makeResultFromInsertRow reshapes a row that was inserted by the
// data source (in tu.insertRow) to a row suitable for storing for a
// later RETURNING clause, shaped by the target table's descriptor.
// For example, the inserted row may not contain values for nullable
// columns.
func (tu *tableUpserter) makeResultFromInsertRow(
	insertRow tree.Datums, cols []sqlbase.ColumnDescriptor,
) tree.Datums {
	resultRow := insertRow
	if len(resultRow) < len(cols) {
		resultRow = make(tree.Datums, len(cols))
		// Pre-fill with NULLs.
		for i := range resultRow {
			resultRow[i] = tree.DNull
		}
		// Fill the other values from insertRow.
		for i, val := range insertRow {
			resultRow[tu.rowIdxToRetIdx[i]] = val
		}
	}
	return resultRow
}

// upsertRowPKs returns the primary key of every row in tu.insertRows
// with potential upsert conflicts.
//
// - if the conflicting index is the PK, the primary key for every
//   row in tu.insertRow is computed (with no KV access) and returned.
//   fetchExisting() will later determine whether the
//   row is already present in KV or not with a lookup.
//
// - if the conflicting index is secondary, that index is used to look
//   up the primary key. If the row is absent, no key is generated.
//
// The keys returned are guaranteed to be unique.
//
// The second return value is returned non-nil when the conflicting
// index is a secondary index. It maps each row in insertRow to a PK with which
// it conflicts. Note that this may not be the PK of the row in insertRow
// itself -- merely that of _some_ row that's in KV already with the same PK.
func (tu *tableUpserter) upsertRowPKs(
	ctx context.Context, traceKV bool,
) ([]roachpb.Key, map[int]roachpb.Key, error) {
	upsertRowPKs := make([]roachpb.Key, 0, tu.insertRows.Len())
	uniquePKs := make(map[string]struct{})

	tableDesc := tu.tableDesc()
	if tu.conflictIndex.ID == tableDesc.PrimaryIndex.ID {
		// If the conflict index is the primary index, we can compute them directly.
		// In this case, the slice will be filled, but not all rows will have
		// conflicts.
		for i := 0; i < tu.insertRows.Len(); i++ {
			insertRow := tu.insertRows.At(i)

			// Compute the PK for the current row.
			upsertRowPK, _, err := sqlbase.EncodeIndexKey(
				tableDesc, &tu.conflictIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
			if err != nil {
				return nil, nil, err
			}

			// If the row has been seen already, we already know there's a
			// conflict. There's nothing to do in that case.  Otherwise, we
			// need to remember there's a conflict by storing that row in
			// `upsertRowPKs`.
			if _, ok := uniquePKs[string(upsertRowPK)]; !ok {
				// Conflict was not previously known. Remember it.
				upsertRowPKs = append(upsertRowPKs, upsertRowPK)
				uniquePKs[string(upsertRowPK)] = struct{}{}
			}
		}
		return upsertRowPKs, nil, nil
	}

	// Otherwise, compute the keys for the conflict index and look them up. The
	// primary key can be constructed from the entries that come back. In this
	// case, some spots in the slice will be nil (indicating no conflict) and the
	// others will be conflicting rows.
	b := tu.txn.NewBatch()
	for i := 0; i < tu.insertRows.Len(); i++ {
		insertRow := tu.insertRows.At(i)
		entries, err := sqlbase.EncodeSecondaryIndex(
			tableDesc, &tu.conflictIndex, tu.ri.InsertColIDtoRowIndex, insertRow)
		if err != nil {
			return nil, nil, err
		}

		for _, entry := range entries {
			if traceKV {
				log.VEventf(ctx, 2, "Get %s", entry.Key)
			}
			b.Get(entry.Key)
		}
	}

	if err := tu.txn.Run(ctx, b); err != nil {
		return nil, nil, err
	}
	conflictingPKs := make(map[int]roachpb.Key)
	for i, result := range b.Results {
		if len(result.Rows) == 1 {
			if result.Rows[0].Value != nil {
				upsertRowPK, err := sqlbase.ExtractIndexKey(tu.alloc, tableDesc, result.Rows[0])
				if err != nil {
					return nil, nil, err
				}
				conflictingPKs[i] = upsertRowPK
				if _, ok := uniquePKs[string(upsertRowPK)]; !ok {
					upsertRowPKs = append(upsertRowPKs, upsertRowPK)
					uniquePKs[string(upsertRowPK)] = struct{}{}
				}
			}
		} else if len(result.Rows) > 1 {
			panic(fmt.Errorf(
				"Expected <= 1 but got %d conflicts for row %s", len(result.Rows), tu.insertRows.At(i)))
		}
	}

	return upsertRowPKs, conflictingPKs, nil
}

// fetchExisting returns any existing rows in the table that conflict with the
// ones in tu.insertRows.
// Outputs:
// - existingRows contains data for conflicting rows.
// - pkToRowIdx relates the primary key values in the
//   data source to which entry in the returned slice contain data
//   for that primary key.
// - conflictingPKs contains the PK for each row in tu.insertRow that
//   has a known conflict. This is populated only if there were some
//   conflicts found and the conflict index was a secondary index.
func (tu *tableUpserter) fetchExisting(
	ctx context.Context, traceKV bool,
) (
	existingRows []tree.Datums,
	pkToRowIdx map[string]int,
	conflictingPKs map[int]roachpb.Key,
	err error,
) {
	tableDesc := tu.tableDesc()

	// primaryKeys contains the PK values to check for conflicts.
	primaryKeys, conflictingPKs, err := tu.upsertRowPKs(ctx, traceKV)
	if err != nil {
		return nil, nil, nil, err
	}

	// pkToRowIdx maps the PK values to positions in existingRows.
	pkToRowIdx = make(map[string]int)

	if len(primaryKeys) == 0 {
		// We know already there is no conflicting row, so there's nothing to fetch.
		return existingRows, pkToRowIdx, conflictingPKs, nil
	}

	// pkSpans will contain the spans for every entry in primaryKeys.
	pkSpans := make(roachpb.Spans, 0, len(primaryKeys))
	for _, primaryKey := range primaryKeys {
		pkSpans = append(pkSpans, roachpb.Span{Key: primaryKey, EndKey: primaryKey.PrefixEnd()})
	}

	// Start retrieving the PKs.
	// We don't limit batches here because the spans are unordered.
	if err := tu.fetcher.StartScan(ctx, tu.txn, pkSpans, false /* no batch limits */, 0, traceKV); err != nil {
		return nil, nil, nil, err
	}

	// Populate existingRows and pkToRowIdx.
	for {
		row, _, _, err := tu.fetcher.NextRowDecoded(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if row == nil {
			break // Done
		}

		rowPrimaryKey, _, err := sqlbase.EncodeIndexKey(
			tableDesc, &tableDesc.PrimaryIndex, tu.fetchColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, nil, nil, err
		}
		// The rows returned by rowFetcher are invalidated after the call to
		// NextRow, so we have to copy them to save them.
		// TODO(knz/nathan): try to reuse a large slice instead
		// of making many small slices.
		rowCopy := make(tree.Datums, len(row))
		copy(rowCopy, row)

		pkToRowIdx[string(rowPrimaryKey)] = len(existingRows)
		existingRows = append(existingRows, rowCopy)
	}

	return existingRows, pkToRowIdx, conflictingPKs, nil
}

func (tu *tableUpserter) tableDesc() *sqlbase.TableDescriptor {
	return tu.ri.Helper.TableDesc
}

func (tu *tableUpserter) fkSpanCollector() sqlbase.FkSpanCollector {
	return tu.ri.Fks
}

func (tu *tableUpserter) close(ctx context.Context) {
	tu.insertRows.Close(ctx)
	if tu.rowsUpserted != nil {
		tu.rowsUpserted.Close(ctx)
	}
}

// fastTableUpserter implements the fast path for an upsert. See
// tableUpserter above for the general case.
//
// If certain conditions are met (no secondary indexes, all table
// values being inserted, update expressions of the form `SET a =
// excluded.a`) then the upsert can be done in one `client.Batch` and
// using only `Put`s. In this case, the single batch is created during
// `init`, operated on during `row`, and run during `finalize`. This
// is the same model as the other `tableFoo`s, which are more simple
// than upsert.
type fastTableUpserter struct {
	ri sqlbase.RowInserter

	// Set by init.
	txn     *client.Txn
	evalCtx *tree.EvalContext

	// Used for the fast path.
	fastPathBatch *client.Batch
}

func (tu *fastTableUpserter) walkExprs(_ func(_ string, _ int, _ tree.TypedExpr)) {}

// init is part of the tableWriter interface.
func (tu *fastTableUpserter) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	tu.txn = txn
	tu.evalCtx = evalCtx

	tu.fastPathBatch = tu.txn.NewBatch()
	return nil
}

// row is part of the tableWriter interface.
func (tu *fastTableUpserter) row(
	ctx context.Context, row tree.Datums, traceKV bool,
) (tree.Datums, error) {
	// Use the fast path, ignore conflicts.
	err := tu.ri.InsertRow(
		ctx, tu.fastPathBatch, row, true /* ignoreConflicts */, sqlbase.CheckFKs, traceKV)
	return nil, err
}

// finalize is part of the tableWriter interface.
func (tu *fastTableUpserter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, traceKV bool,
) (*sqlbase.RowContainer, error) {
	if autoCommit == autoCommitEnabled {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		if err := tu.txn.CommitInBatch(ctx, tu.fastPathBatch); err != nil {
			return nil, err
		}
	} else {
		if err := tu.txn.Run(ctx, tu.fastPathBatch); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (tu *fastTableUpserter) fkSpanCollector() sqlbase.FkSpanCollector {
	return tu.ri.Fks
}

func (tu *fastTableUpserter) tableDesc() *sqlbase.TableDescriptor {
	return tu.ri.Helper.TableDesc
}

func (tu *fastTableUpserter) close(ctx context.Context) {}

// tableDeleter handles writing kvs and forming table rows for deletes.
type tableDeleter struct {
	rd        sqlbase.RowDeleter
	alloc     *sqlbase.DatumAlloc
	batchSize int

	// Set by init.
	txn     *client.Txn
	evalCtx *tree.EvalContext
	b       *client.Batch
}

func (td *tableDeleter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}

func (td *tableDeleter) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	td.txn = txn
	td.evalCtx = evalCtx
	td.b = txn.NewBatch()
	return nil
}

func (td *tableDeleter) row(
	ctx context.Context, values tree.Datums, traceKV bool,
) (tree.Datums, error) {
	// Rudimentarily chunk the deletions to avoid memory blowup in queries such
	// as `DELETE FROM mytable`.
	const maxBatchSize = 10000
	if td.batchSize >= maxBatchSize {
		if err := td.txn.Run(ctx, td.b); err != nil {
			return nil, err
		}
		td.b = td.txn.NewBatch()
		td.batchSize = 0
	}
	td.batchSize++

	return nil, td.rd.DeleteRow(ctx, td.b, values, sqlbase.CheckFKs, traceKV)
}

// finalize is part of the tableWriter interface.
func (td *tableDeleter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, _ bool,
) (*sqlbase.RowContainer, error) {
	if autoCommit == autoCommitEnabled {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		return nil, td.txn.CommitInBatch(ctx, td.b)
	}
	return nil, td.txn.Run(ctx, td.b)
}

// fastPathAvailable returns true if the fastDelete optimization can be used.
func (td *tableDeleter) fastPathAvailable(ctx context.Context) bool {
	if len(td.rd.Helper.Indexes) != 0 {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: values required to update %d secondary indexes", len(td.rd.Helper.Indexes))
		}
		return false
	}
	if td.rd.Helper.TableDesc.IsInterleaved() {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return false
	}
	if len(td.rd.Helper.TableDesc.PrimaryIndex.ReferencedBy) > 0 {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is referenced by foreign keys")
		}
		return false
	}
	return true
}

// fastDelete adds to the batch the kv operations necessary to delete sql rows
// without knowing the values that are currently present. fastDelete calls
// finalize, so it should not be called after.
func (td *tableDeleter) fastDelete(
	ctx context.Context, scan *scanNode, autoCommit autoCommitOpt, traceKV bool,
) (rowCount int, err error) {

	for _, span := range scan.spans {
		log.VEvent(ctx, 2, "fast delete: skipping scan")
		if traceKV {
			log.VEventf(ctx, 2, "DelRange %s - %s", span.Key, span.EndKey)
		}
		td.b.DelRange(span.Key, span.EndKey, true /* returnKeys */)
	}

	_, err = td.finalize(ctx, autoCommit, traceKV)
	if err != nil {
		return 0, err
	}

	for _, r := range td.b.Results {
		var prev []byte
		for _, i := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(i, prev) {
				continue
			}

			after, ok, err := scan.run.fetcher.ReadIndexKey(i)
			if err != nil {
				return 0, err
			}
			if !ok {
				return 0, errors.Errorf("key did not match descriptor")
			}
			k := i[:len(i)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				rowCount++
			}
		}
	}

	td.b = nil
	return rowCount, nil
}

// deleteAllRows runs the kv operations necessary to delete all sql rows in the
// table passed at construction. This may require a scan.
//
// resume is the resume-span which should be used for the table deletion when
// the table deletion is chunked. The first call to this method should use a
// zero resume-span. After a chunk is deleted a new resume-span is returned.
//
// limit is a limit on either the number of keys or table-rows (for
// interleaved tables) deleted in the operation.
func (td *tableDeleter) deleteAllRows(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
	if td.rd.Helper.TableDesc.IsInterleaved() {
		log.VEvent(ctx, 2, "delete forced to scan: table is interleaved")
		return td.deleteAllRowsScan(ctx, resume, limit, autoCommit, traceKV)
	}
	return td.deleteAllRowsFast(ctx, resume, limit, autoCommit, traceKV)
}

// deleteAllRowsFast employs a ClearRange KV API call to delete the
// underlying data quickly. Unlike DeleteRange, ClearRange doesn't
// leave tombstone data on individual keys, instead using a more
// efficient ranged tombstone, preventing unnecessary write
// amplification.
func (td *tableDeleter) deleteAllRowsFast(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		tablePrefix := roachpb.Key(
			encoding.EncodeUvarintAscending(nil, uint64(td.rd.Helper.TableDesc.ID)),
		)
		// Delete rows and indexes starting with the table's prefix.
		resume = roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		}
	}
	// If DropTime isn't set, assume this drop request is from a version
	// 1.1 server and invoke legacy code that uses DeleteRange and range GC.
	if td.tableDesc().DropTime == 0 {
		return td.legacyDeleteAllRowsFast(ctx, resume, limit, autoCommit, traceKV)
	}

	log.VEventf(ctx, 2, "ClearRange %s - %s", resume.Key, resume.EndKey)
	// ClearRange cannot be run in a transaction, so create a
	// non-transactional batch to send the request.
	b := &client.Batch{}
	// TODO(tschottdorf): this might need a cluster migration.
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		Span: roachpb.Span{
			Key:    resume.Key,
			EndKey: resume.EndKey,
		},
	})
	if err := td.txn.DB().Run(ctx, b); err != nil {
		return resume, err
	}
	if _, err := td.finalize(ctx, autoCommit, traceKV); err != nil {
		return resume, err
	}
	return roachpb.Span{}, nil
}

// legacyDeleteAllRowsFast handles cases where no GC deadline is set
// and so deletion must fall back to relying on DeleteRange and the
// eventual range GC cycle.
func (td *tableDeleter) legacyDeleteAllRowsFast(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
	log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if _, err := td.finalize(ctx, autoCommit, traceKV); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned", l))
	}
	return td.b.Results[0].ResumeSpan, nil
}

func (td *tableDeleter) deleteAllRowsScan(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.PrimaryIndexSpan()
	}

	var valNeededForCol util.FastIntSet
	for _, idx := range td.rd.FetchColIDtoRowIndex {
		valNeededForCol.Add(idx)
	}

	var rf sqlbase.RowFetcher
	tableArgs := sqlbase.RowFetcherTableArgs{
		Desc:            td.rd.Helper.TableDesc,
		Index:           &td.rd.Helper.TableDesc.PrimaryIndex,
		ColIdxMap:       td.rd.FetchColIDtoRowIndex,
		Cols:            td.rd.FetchCols,
		ValNeededForCol: valNeededForCol,
	}
	if err := rf.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, td.alloc, tableArgs,
	); err != nil {
		return resume, err
	}
	if err := rf.StartScan(ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0, traceKV); err != nil {
		return resume, err
	}

	for i := int64(0); i < limit; i++ {
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return resume, err
		}
		if datums == nil {
			// Done deleting all rows.
			resume = roachpb.Span{}
			break
		}
		_, err = td.row(ctx, datums, traceKV)
		if err != nil {
			return resume, err
		}
	}
	if resume.Key != nil {
		// Update the resume start key for the next iteration.
		resume.Key = rf.Key()
	}
	_, err := td.finalize(ctx, autoCommit, traceKV)
	return resume, err
}

// deleteIndex runs the kv operations necessary to delete all kv entries in the
// given index. This may require a scan.
//
// resume is the resume-span which should be used for the index deletion
// when the index deletion is chunked. The first call to this method should
// use a zero resume-span. After a chunk of the index is deleted a new resume-
// span is returned.
//
// limit is a limit on the number of index entries deleted in the operation.
func (td *tableDeleter) deleteIndex(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	resume roachpb.Span,
	limit int64,
	autoCommit autoCommitOpt,
	traceKV bool,
) (roachpb.Span, error) {
	if len(idx.Interleave.Ancestors) > 0 || len(idx.InterleavedBy) > 0 {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return td.deleteIndexScan(ctx, idx, resume, limit, autoCommit, traceKV)
	}
	return td.deleteIndexFast(ctx, idx, resume, limit, autoCommit, traceKV)
}

func (td *tableDeleter) deleteIndexFast(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	resume roachpb.Span,
	limit int64,
	autoCommit autoCommitOpt,
	traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.IndexSpan(idx.ID)
	}

	if traceKV {
		log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	// TODO(vivekmenezes): adapt index deletion to use the same GC
	// deadline / ClearRange fast path that table deletion uses.
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if _, err := td.finalize(ctx, autoCommit, traceKV); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned, expected 1", l))
	}
	return td.b.Results[0].ResumeSpan, nil
}

func (td *tableDeleter) deleteIndexScan(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	resume roachpb.Span,
	limit int64,
	autoCommit autoCommitOpt,
	traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.PrimaryIndexSpan()
	}

	var valNeededForCol util.FastIntSet
	for _, idx := range td.rd.FetchColIDtoRowIndex {
		valNeededForCol.Add(idx)
	}

	var rf sqlbase.RowFetcher
	tableArgs := sqlbase.RowFetcherTableArgs{
		Desc:            td.rd.Helper.TableDesc,
		Index:           &td.rd.Helper.TableDesc.PrimaryIndex,
		ColIdxMap:       td.rd.FetchColIDtoRowIndex,
		Cols:            td.rd.FetchCols,
		ValNeededForCol: valNeededForCol,
	}
	if err := rf.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, td.alloc, tableArgs,
	); err != nil {
		return resume, err
	}
	if err := rf.StartScan(ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0, traceKV); err != nil {
		return resume, err
	}

	for i := int64(0); i < limit; i++ {
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return resume, err
		}
		if datums == nil {
			// Done deleting all rows.
			resume = roachpb.Span{}
			break
		}
		if err := td.rd.DeleteIndexRow(ctx, td.b, idx, datums, traceKV); err != nil {
			return resume, err
		}
	}
	if resume.Key != nil {
		// Update the resume start key for the next iteration.
		resume.Key = rf.Key()
	}
	_, err := td.finalize(ctx, autoCommit, traceKV)
	return resume, err
}

func (td *tableDeleter) tableDesc() *sqlbase.TableDescriptor {
	return td.rd.Helper.TableDesc
}

func (td *tableDeleter) fkSpanCollector() sqlbase.FkSpanCollector {
	return td.rd.Fks
}

func (td *tableDeleter) close(_ context.Context) {}
