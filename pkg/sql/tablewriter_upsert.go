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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// tableUpserterBase contains common functionality between different upserter implementations.
type tableUpserterBase struct {
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
}

func (tu *tableUpserterBase) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
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

	tu.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(tableDesc.TableDesc(), tableDesc.PrimaryIndex.ID)

	return nil
}

func (tu *tableUpserterBase) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ri.Helper.TableDesc
}

// row is part of the tableWriter interface.
func (tu *tableUpserterBase) row(ctx context.Context, row tree.Datums, traceKV bool) error {
	tu.batchSize++
	_, err := tu.insertRows.AddRow(ctx, row)
	return err
}

// flushAndStartNewBatch is part of the extendedTableWriter interface.
func (tu *tableUpserterBase) flushAndStartNewBatch(ctx context.Context) error {
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
func (tu *tableUpserterBase) batchedCount() int { return tu.resultCount }

// batchedValues is part of the batchedTableWriter interface.
func (tu *tableUpserterBase) batchedValues(rowIdx int) tree.Datums {
	if !tu.collectRows {
		panic("return row requested but collect rows was not set")
	}
	return tu.rowsUpserted.At(rowIdx)
}

func (tu *tableUpserterBase) curBatchSize() int { return tu.insertRows.Len() }

// close is part of the tableWriter interface.
func (tu *tableUpserterBase) close(ctx context.Context) {
	tu.insertRows.Close(ctx)
	if tu.existingRows != nil {
		tu.existingRows.Close(ctx)
	}
	if tu.rowsUpserted != nil {
		tu.rowsUpserted.Close(ctx)
	}
}

// finalize is part of the tableWriter interface.
func (tu *tableUpserterBase) finalize(
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
func (tu *tableUpserterBase) makeResultFromRow(
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
	tableUpserterBase

	// updateCols indicates which columns need an update during a
	// conflict.  There is one entry per column descriptors in the
	// table. However only the entries identified in the conflict clause
	// of the original statement will be populated, to disambiguate
	// columns that need an update from those that don't.
	updateCols []sqlbase.ColumnDescriptor

	conflictIndex sqlbase.IndexDescriptor
	anyComputed   bool

	evalCtx *tree.EvalContext

	// These are set for ON CONFLICT DO UPDATE, but not for DO NOTHING
	evaler *upsertHelper

	// updateValues is the temporary buffer for rows computed by
	// evaler.eval(). It is reused from one row to the next to limit
	// allocations.
	updateValues tree.Datums

	// cleanedRow is a temporary buffer reused from one row to the next in
	// appendKnownConflictingRow to limit allocations.
	cleanedRow tree.Datums

	// Set by init.
	fkTables              row.FkTableMetadata // for fk checks in update case
	ru                    row.Updater
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	fetchCols             []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	fetcher               row.Fetcher
}

// desc is part of the tableWriter interface.
func (*tableUpserter) desc() string { return "upserter" }

// init is part of the tableWriter interface.
func (tu *tableUpserter) init(txn *client.Txn, evalCtx *tree.EvalContext) error {
	tu.tableWriterBase.init(txn)

	tu.evalCtx = evalCtx

	err := tu.tableUpserterBase.init(txn, evalCtx)
	if err != nil {
		return err
	}

	tableDesc := tu.tableDesc()

	requestedCols := tableDesc.Columns

	if len(tu.updateCols) == 0 {
		tu.fetchCols = requestedCols
		tu.fetchColIDtoRowIndex = row.ColIDtoRowIndexFromCols(requestedCols)
	} else {
		tu.ru, err = row.MakeUpdater(
			txn,
			tableDesc,
			tu.fkTables,
			tu.updateCols,
			requestedCols,
			row.UpdaterDefault,
			row.CheckFKs,
			evalCtx,
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

	var valNeededForCol util.FastIntSet
	for i := range tu.fetchCols {
		id := tu.fetchCols[i].ID
		if _, ok := tu.fetchColIDtoRowIndex[id]; ok {
			valNeededForCol.Add(i)
		}
	}

	tableArgs := row.FetcherTableArgs{
		Desc:            tableDesc,
		Index:           &tableDesc.PrimaryIndex,
		ColIdxMap:       tu.fetchColIDtoRowIndex,
		Cols:            tu.fetchCols,
		ValNeededForCol: valNeededForCol,
	}

	if err := tu.fetcher.Init(
		false /* reverse */, false /*returnRangeInfo*/, false /* isCheck */, tu.alloc, tableArgs,
	); err != nil {
		return err
	}

	tu.cleanedRow = make(tree.Datums, len(tu.fetchColIDtoRowIndex))
	pkColTypeInfo, err := sqlbase.MakeColTypeInfo(tu.tableDesc(), tu.fetchColIDtoRowIndex)
	if err != nil {
		return err
	}
	tu.existingRows = rowcontainer.NewRowContainer(
		tu.evalCtx.Mon.MakeBoundAccount(), pkColTypeInfo, tu.insertRows.Len(),
	)
	return nil
}

// atBatchEnd is part of the extendedTableWriter interface.
func (tu *tableUpserter) atBatchEnd(ctx context.Context, traceKV bool) error {
	// Fetch the information about which rows in tu.insertRows currently
	// conflict with rows in-db.
	pkToRowIdx, conflictingPKs, err := tu.fetchExisting(ctx, traceKV)
	if err != nil {
		return err
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

	for i := 0; i < tu.insertRows.Len(); i++ {
		insertRow := tu.insertRows.At(i)

		// conflictingRowPK will be the key of the conflicting row. This may
		// be different from insertRow's PK if the conflicting index is a
		// secondary index.
		conflictingRowPK, err := tu.getConflictingRowPK(insertRow, i, conflictingPKs, tableDesc)
		if err != nil {
			return err
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
			resultRow, err = tu.insertNonConflictingRow(
				ctx, tu.b, insertRow, conflictingRowPK, pkToRowIdx, tableDesc, traceKV,
			)
			if err != nil {
				return err
			}

			// We have processed a row, remember this for the rows affected
			// count in case we're not populating rowsUpserted below.
			tu.resultCount++
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
			existingRow := tu.existingRows.At(conflictingRowIdx)

			// Check the ON CONFLICT DO UPDATE WHERE ... clause.
			shouldUpdate, err := tu.evaler.shouldUpdate(insertRow, existingRow)
			if err != nil {
				return err
			}
			if !shouldUpdate {
				// WHERE tells us there is nothing to do. Stop here.
				// There is also no RETURNING result.
				// See https://www.postgresql.org/docs/10/static/sql-insert.html and the
				// quoted excerpt above.
				continue
			}

			// We know there was a row already, and we know we need to update it. Do it.
			resultRow, err = tu.updateConflictingRow(
				ctx, tu.b, insertRow,
				conflictingRowPK, conflictingRowIdx, existingRow,
				pkToRowIdx, tableDesc, traceKV,
			)
			if err != nil {
				return err
			}

			// We have processed a row, remember this for the rows affected
			// count in case we're not populating rowsUpserted below.
			tu.resultCount++
		}

		// Do we need to remember a result for RETURNING?
		if tu.collectRows {
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

// updateConflictingRow updates the existing row in the table, when there was a
// conflict. existingRows contains the previously seen rows, and is modified
// or extended depending on how the PK columns are updated by the SET clauses.
// Inputs:
// - b is the KV batch to use for the insert.
// - insertRow is the new row to upsert, containing the "excluded" values.
// - conflictingRowPK is the PK of the previously seen conflicting row.
// - conflictingRowIdx is the index of the values of the previously seen
//   conflicting row in existingRows.
// - conflictingRowValues is the prefetched existingRows[conflictingRowIdx].
// Outputs:
// - resultRow is the row that was updated, shaped in the order of the table
//   descriptor. This may be different than the shape of insertRow if there are
//   nullable columns. This is only returned if collectRows is true.
// Input/Outputs:
// - pkToRowIdx is extended with the index of the new entry in existingRows.
func (tu *tableUpserter) updateConflictingRow(
	ctx context.Context,
	b *client.Batch,
	insertRow tree.Datums,
	conflictingRowPK roachpb.Key,
	conflictingRowIdx int,
	conflictingRowValues tree.Datums,
	pkToRowIdx map[string]int,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) (resultRow tree.Datums, err error) {
	// First compute all the updates via SET (or the pseudo-SET generated
	// for UPSERT statements).

	updateValues, err := tu.evaler.eval(insertRow, conflictingRowValues, tu.updateValues)
	if err != nil {
		return nil, err
	}

	checkHelper := tu.fkTables[tableDesc.ID].CheckHelper

	// Do we need to (re-)compute computed columns or CHECK expressions?
	if tu.anyComputed || checkHelper != nil {
		// For computed columns, the goal for the following code appends
		// the computed columns at the end of updateValues.
		// For CHECK constraints, the goal of the following code
		// is to evaluate the constraints and verify they hold.
		//
		// However both computation require a fully formed row as input,
		// that is, with the original values, then the UPDATE SET values merged in.
		// The CHECK expressions want that, and also the computed values
		// appended (because they may be used as input too).
		//
		// TODO(justin): We're currently wasteful here: this construction
		// of the result row is done again in the row updater. We need to
		// find a way to reuse it. I'm not sure right now how best to
		// factor this - suggestions welcome.
		// TODO(nathan/knz): Reuse a row buffer here.

		// Build a row buffer input with the original data and UPDATE SET
		// assignments merged in.
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
			return nil, err
		}

		// Ensure that all the values produced by SET comply with the schema constraints.
		// We can assume that values *prior* to SET are OK because:
		// - data source values have been validated by GenerateInsertRow() already.
		// - values coming from the table are valid by construction.
		// However the SET expression can be arbitrary and can introduce errors
		// downstream, so we need to (re)validate here.
		if err := enforceLocalColumnConstraints(updateValues, tu.ru.UpdateCols); err != nil {
			return nil, err
		}

		if checkHelper != nil {
			// If there are CHECK expressions, we must add the computed
			// columns to the input row.

			// Merge the computed values into the newValues slice so that the checkHelper can see them.
			for i, updateCol := range tu.ru.UpdateCols {
				newValues[tu.ru.FetchColIDtoRowIndex[updateCol.ID]] = updateValues[i]
			}

			// Check CHECK constraints.
			if err := checkHelper.LoadEvalRow(tu.ru.FetchColIDtoRowIndex, newValues, false); err != nil {
				return nil, err
			}
			if err := checkHelper.CheckEval(tu.evalCtx); err != nil {
				return nil, err
			}
		}
	}

	// Queue the update in KV. This also returns an "update row"
	// containing the updated values for every column in the
	// table. This is useful for RETURNING, which we collect below.
	updatedRow, err := tu.ru.UpdateRow(
		ctx, b, conflictingRowValues, updateValues, row.CheckFKs, traceKV,
	)
	if err != nil {
		return nil, err
	}

	// Keep the slice for reuse.
	tu.updateValues = updateValues[:0]

	// Maybe the PK was updated by SET. We need to recompute a fresh PK
	// for the current row from updatedRow. We use
	// tu.evaler.ccIvarContainer.Mapping which contains the suitable
	// mapping for the table columns already.
	updatedConflictingRowPK, _, err := sqlbase.EncodeIndexKey(
		tableDesc.TableDesc(), &tableDesc.PrimaryIndex, tu.evaler.ccIvarContainer.Mapping, updatedRow, tu.indexKeyPrefix)
	if err != nil {
		return nil, err
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
		copy(tu.existingRows.At(updatedConflictingRowIdx), updatedRow)

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
		if err := tu.appendKnownConflictingRow(
			ctx, updatedRow, updatedConflictingRowPK, pkToRowIdx,
		); err != nil {
			return nil, err
		}
	}

	if pkChanged {
		// The previous PK is guaranteed to not exist any more. Remove it.
		delete(pkToRowIdx, string(conflictingRowPK))
	}

	// We only need a result row if we're collecting rows.
	if !tu.collectRows {
		return nil, nil
	}

	// We now need a row that has the shape of the result row.
	return tu.makeResultFromRow(updatedRow, tu.evaler.ccIvarContainer.Mapping), nil
}

// insertNonConflictingRow inserts the source row insertRow
// into the table, when there was no conflict.
// Inputs:
// - b is the KV batch to use for the insert.
// - insertRow is the new row to insert.
// - conflictingRowPK is the PK of that new row, if it is known already
//   (e.g. by getConflictingRowPK from the primary index).
// Outputs:
// - resultRow is the row that was inserted, shaped in the order of the table
//   descriptor. This may be different than the shape of insertRow if there are
//   nullable columns. This is only returned if collectRows is true.
// Input/Outputs:
// - pkToRowIdx is extended with the index of the new entry in existingRows.
func (tu *tableUpserter) insertNonConflictingRow(
	ctx context.Context,
	b *client.Batch,
	insertRow tree.Datums,
	conflictingRowPK roachpb.Key,
	pkToRowIdx map[string]int,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) (resultRow tree.Datums, err error) {
	// Perform the insert proper.
	if err := tu.ri.InsertRow(
		ctx, b, insertRow, false /* ignoreConflicts */, row.CheckFKs, traceKV); err != nil {
		return nil, err
	}

	// We may not know the conflictingRowPK yet for the new row, for
	// example when the conflicting index was a secondary index.
	// In that case, compute it now.
	if conflictingRowPK == nil {
		conflictingRowPK, _, err = sqlbase.EncodeIndexKey(
			tableDesc.TableDesc(), &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}
	}

	// Then remember it for further upserts.
	if err := tu.appendKnownConflictingRow(ctx, insertRow, conflictingRowPK, pkToRowIdx); err != nil {
		return nil, err
	}

	if !tu.collectRows {
		return nil, nil
	}

	// Reshape the row if needed.
	if tu.insertReorderingRequired {
		return tu.makeResultFromRow(insertRow, tu.ri.InsertColIDtoRowIndex), nil
	}
	return insertRow, nil
}

// appendKnownConflictingRow adds a new row to existingRows and remembers its
// position in pkToRowIdx.
func (tu *tableUpserter) appendKnownConflictingRow(
	ctx context.Context, newRow tree.Datums, newRowPK roachpb.Key, pkToRowIdx map[string]int,
) error {
	pkToRowIdx[string(newRowPK)] = tu.existingRows.Len()
	// We need to convert the new row to match the fetch columns required for
	// checking if there is a conflict.
	for fetchColID, fetchRowIndex := range tu.fetchColIDtoRowIndex {
		insertRowIndex, ok := tu.ri.InsertColIDtoRowIndex[fetchColID]
		if ok {
			tu.cleanedRow[fetchRowIndex] = newRow[insertRowIndex]
		} else {
			tu.cleanedRow[fetchRowIndex] = tree.DNull
		}
	}
	_, err := tu.existingRows.AddRow(ctx, tu.cleanedRow)
	return err
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
	tableDesc *sqlbase.ImmutableTableDescriptor,
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
		tableDesc.TableDesc(), &tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
	return insertRowPK, err
}

// upsertRowPKSpans returns key spans containing every row in tu.upsertRows
// that has potential upsert conflicts.
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
func (tu *tableUpserter) upsertRowPKSpans(
	ctx context.Context, traceKV bool,
) ([]roachpb.Span, map[int]roachpb.Key, error) {
	upsertRowPKSpans := make([]roachpb.Span, 0, tu.insertRows.Len())
	uniquePKs := make(map[string]struct{})

	tableDesc := tu.tableDesc()
	if tu.conflictIndex.ID == tableDesc.PrimaryIndex.ID {
		// If the conflict index is the primary index, we can compute them directly.
		// In this case, the slice will be filled, but not all rows will have
		// conflicts.
		for i := 0; i < tu.insertRows.Len(); i++ {
			insertRow := tu.insertRows.At(i)

			// Compute the PK span for the current row.
			upsertRowPKSpan, _, err := sqlbase.EncodeIndexSpan(
				tableDesc.TableDesc(), &tu.conflictIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
			if err != nil {
				return nil, nil, err
			}

			// If the row has been seen already, we already know there's a
			// conflict. There's nothing to do in that case.  Otherwise, we
			// need to remember there's a conflict by storing that row in
			// `upsertRowPKSpans`.
			if _, ok := uniquePKs[string(upsertRowPKSpan.Key)]; !ok {
				// Conflict was not previously known. Remember it.
				upsertRowPKSpans = append(upsertRowPKSpans, upsertRowPKSpan)
				uniquePKs[string(upsertRowPKSpan.Key)] = struct{}{}
			}
		}
		return upsertRowPKSpans, nil, nil
	}

	// Otherwise, compute the keys for the conflict index and look them up. The
	// primary key can be constructed from the entries that come back. In this
	// case, some spots in the slice will be nil (indicating no conflict) and the
	// others will be conflicting rows.
	b := tu.txn.NewBatch()
	for i := 0; i < tu.insertRows.Len(); i++ {
		insertRow := tu.insertRows.At(i)
		entries, err := sqlbase.EncodeSecondaryIndex(
			tableDesc.TableDesc(), &tu.conflictIndex, tu.ri.InsertColIDtoRowIndex, insertRow)
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
				upsertRowPK, err := sqlbase.ExtractIndexKey(tu.alloc, tableDesc.TableDesc(), result.Rows[0])
				if err != nil {
					return nil, nil, err
				}
				conflictingPKs[i] = upsertRowPK
				if _, ok := uniquePKs[string(upsertRowPK)]; !ok {
					span := roachpb.Span{
						Key:    upsertRowPK,
						EndKey: encoding.EncodeInterleavedSentinel(upsertRowPK),
					}
					upsertRowPKSpans = append(upsertRowPKSpans, span)
					uniquePKs[string(upsertRowPK)] = struct{}{}
				}
			}
		} else if len(result.Rows) > 1 {
			panic(fmt.Errorf(
				"expected <= 1 but got %d conflicts for row %s", len(result.Rows), tu.insertRows.At(i)))
		}
	}

	return upsertRowPKSpans, conflictingPKs, nil
}

// fetchExisting returns any existing rows in the table that conflict with the
// ones in tu.insertRows.
// Outputs:
// - pkToRowIdx relates the primary key values in the
//   data source to which entry in the returned slice contain data
//   for that primary key.
// - conflictingPKs contains the PK for each row in tu.insertRow that
//   has a known conflict. This is populated only if there were some
//   conflicts found and the conflict index was a secondary index.
func (tu *tableUpserter) fetchExisting(
	ctx context.Context, traceKV bool,
) (pkToRowIdx map[string]int, conflictingPKs map[int]roachpb.Key, err error) {
	tableDesc := tu.tableDesc()

	// primaryKeySpans contains the PK values to check for conflicts.
	primaryKeySpans, conflictingPKs, err := tu.upsertRowPKSpans(ctx, traceKV)
	if err != nil {
		return nil, nil, err
	}

	// pkToRowIdx maps the PK values to positions in existingRows.
	pkToRowIdx = make(map[string]int)

	if len(primaryKeySpans) == 0 {
		// We know already there is no conflicting row, so there's nothing to fetch.
		return pkToRowIdx, conflictingPKs, nil
	}

	// pkSpans will contain the spans for every entry in primaryKeySpans.
	pkSpans := make(roachpb.Spans, 0, len(primaryKeySpans))
	for _, primaryKeySpan := range primaryKeySpans {
		pkSpans = append(pkSpans, primaryKeySpan)
	}

	// Start retrieving the PKs.
	// We don't limit batches here because the spans are unordered.
	if err := tu.fetcher.StartScan(ctx, tu.txn, pkSpans, false /* no batch limits */, 0, traceKV); err != nil {
		return nil, nil, err
	}

	// Populate existingRows and pkToRowIdx.
	for {
		row, _, _, err := tu.fetcher.NextRowDecoded(ctx)
		if err != nil {
			return nil, nil, err
		}
		if row == nil {
			break // Done
		}

		rowPrimaryKey, _, err := sqlbase.EncodeIndexKey(
			tableDesc.TableDesc(), &tableDesc.PrimaryIndex, tu.fetchColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, nil, err
		}

		pkToRowIdx[string(rowPrimaryKey)] = tu.existingRows.Len()
		if _, err := tu.existingRows.AddRow(ctx, row); err != nil {
			return nil, nil, err
		}
	}

	return pkToRowIdx, conflictingPKs, nil
}

// tableDesc is part of the tableWriter interface.
func (tu *tableUpserter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return tu.ri.Helper.TableDesc
}

// walkExprs is part of the tableWriter interface.
func (tu *tableUpserter) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
	if tu.evaler != nil {
		tu.evaler.walkExprs(walk)
	}
}
