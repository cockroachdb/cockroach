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

// The Column and Index backfill primitives.

package backfill

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// MutationFilter is the type of a simple predicate on a mutation.
type MutationFilter func(sqlbase.DescriptorMutation) bool

// CheckMutationFilter is a filter that allows mutations that add
// check constraints.
func CheckMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetCheck() != nil &&
		m.Direction == sqlbase.DescriptorMutation_ADD &&
		m.GetCheck().Validity == sqlbase.ConstraintValidity_Validating
}

// ColumnMutationFilter is a filter that allows mutations that add or drop
// columns.
func ColumnMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetColumn() != nil &&
		(m.Direction == sqlbase.DescriptorMutation_ADD || m.Direction == sqlbase.DescriptorMutation_DROP)
}

// IndexMutationFilter is a filter that allows mutations that add indexes.
func IndexMutationFilter(m sqlbase.DescriptorMutation) bool {
	return m.GetIndex() != nil && m.Direction == sqlbase.DescriptorMutation_ADD
}

// backfiller is common to a ColumnBackfiller or an IndexBackfiller.
type backfiller struct {
	fetcher row.Fetcher
	alloc   sqlbase.DatumAlloc
}

// CheckBackfiller is capable of backfilling all the added checks.
type CheckBackfiller struct {
	backfiller

	addedExprs []tree.TypedExpr
	added      []sqlbase.TableDescriptor_CheckConstraint
	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap map[sqlbase.ColumnID]int
	cols      []sqlbase.ColumnDescriptor

	evalCtx *tree.EvalContext
}

// Init initializes a CheckBackfiller.
func (cb *CheckBackfiller) Init(
	evalCtx *tree.EvalContext, desc *sqlbase.ImmutableTableDescriptor,
) error {
	cb.evalCtx = evalCtx
	numCols := len(desc.Columns)
	cb.cols = desc.Columns
	if len(desc.Mutations) > 0 {
		cb.cols = make([]sqlbase.ColumnDescriptor, 0, numCols+len(desc.Mutations))
		cb.cols = append(cb.cols, desc.Columns...)
		for _, m := range desc.Mutations {
			if column := m.GetColumn(); column != nil &&
				m.Direction == sqlbase.DescriptorMutation_ADD &&
				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				cb.cols = append(cb.cols, *column)
			}
		}
	}

	mutationID := desc.Mutations[0].MutationID
	for _, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if CheckMutationFilter(m) {
			ck := m.GetCheck()
			cb.added = append(cb.added, *ck)
		}
	}

	var txCtx transform.ExprTransformContext
	var err error
	cb.addedExprs, err = sqlbase.MakeCheckExprs(
		cb.added,
		tree.NewUnqualifiedTableName(tree.Name(desc.Name)),
		cb.cols,
		&txCtx,
		cb.evalCtx,
	)
	if err != nil {
		return err
	}

	cb.colIdxMap = make(map[sqlbase.ColumnID]int, len(cb.cols))
	for i, c := range cb.cols {
		cb.colIdxMap[c.ID] = i
	}

	// Get all values for displaying the whole row that fails any check.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(cb.cols)-1)

	tableArgs := row.FetcherTableArgs{
		Desc:            desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       cb.colIdxMap,
		Cols:            cb.cols,
		ValNeededForCol: valNeededForCol,
	}
	return cb.fetcher.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &cb.alloc, tableArgs,
	)
}

// RunCheckBackfillChunk runs an check validation over a chunk of the table
// by traversing the span sp provided. The validation is run for the added
// checks.
func (cb *CheckBackfiller) RunCheckBackfillChunk(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	traceKV bool,
) (roachpb.Key, error) {
	// Get the next set of rows.
	//
	// Running the scan and applying the changes in many transactions
	// is fine because the schema change is in the correct state to
	// handle intermediate OLTP commands which delete and add values
	// during the scan. Index entries in the new index are being
	// populated and deleted by the OLTP commands but not otherwise
	// read or used
	if err := cb.fetcher.StartScan(
		ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize, traceKV,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return roachpb.Key{}, err
	}

	iv := &sqlbase.RowIndexedVarContainer{
		Cols:    cb.cols,
		Mapping: cb.colIdxMap,
	}
	cb.evalCtx.IVarContainer = iv
	for i := int64(0); i < chunkSize; i++ {
		datums, _, _, err := cb.fetcher.NextRowDecoded(ctx)
		if err != nil {
			return roachpb.Key{}, err
		}
		if datums == nil {
			break
		}
		iv.CurSourceRow = datums

		for j, e := range cb.addedExprs {
			val, err := e.Eval(cb.evalCtx)
			if err != nil {
				return roachpb.Key{}, sqlbase.NewInvalidSchemaDefinitionError(err)
			}
			// Expression already type checked.
			if val == tree.DBoolFalse {
				return roachpb.Key{}, errors.Errorf("validation of CHECK %q failed on row: %s",
					cb.added[j].Expr, sqlbase.LabeledRowValues(cb.cols, datums))
			}
		}
	}
	return cb.fetcher.Key(), nil
}

// ColumnBackfiller is capable of running a column backfill for all
// updateCols.
type ColumnBackfiller struct {
	backfiller

	added []sqlbase.ColumnDescriptor
	// updateCols is a slice of all column descriptors that are being modified.
	updateCols  []sqlbase.ColumnDescriptor
	updateExprs []tree.TypedExpr
	evalCtx     *tree.EvalContext
}

// Init initializes a column backfiller.
func (cb *ColumnBackfiller) Init(
	evalCtx *tree.EvalContext, desc *sqlbase.ImmutableTableDescriptor,
) error {
	cb.evalCtx = evalCtx
	var dropped []sqlbase.ColumnDescriptor
	if len(desc.Mutations) > 0 {
		for _, m := range desc.Mutations {
			if ColumnMutationFilter(m) {
				desc := *m.GetColumn()
				switch m.Direction {
				case sqlbase.DescriptorMutation_ADD:
					cb.added = append(cb.added, desc)
				case sqlbase.DescriptorMutation_DROP:
					dropped = append(dropped, desc)
				}
			}
		}
	}
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		cb.added, &transform.ExprTransformContext{}, cb.evalCtx,
	)
	if err != nil {
		return err
	}
	var txCtx transform.ExprTransformContext
	computedExprs, err := sqlbase.MakeComputedExprs(cb.added, desc,
		tree.NewUnqualifiedTableName(tree.Name(desc.Name)), &txCtx, cb.evalCtx, true /* addingCols */)
	if err != nil {
		return err
	}

	cb.updateCols = append(cb.added, dropped...)
	// Populate default or computed values.
	cb.updateExprs = make([]tree.TypedExpr, len(cb.updateCols))
	for j, col := range cb.added {
		if col.IsComputed() {
			cb.updateExprs[j] = computedExprs[j]
		} else if defaultExprs == nil || defaultExprs[j] == nil {
			cb.updateExprs[j] = tree.DNull
		} else {
			cb.updateExprs[j] = defaultExprs[j]
		}
	}
	for j := range dropped {
		cb.updateExprs[j+len(cb.added)] = tree.DNull
	}

	// We need all the columns.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(desc.Columns)-1)

	tableArgs := row.FetcherTableArgs{
		Desc:            desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       desc.ColumnIdxMap(),
		Cols:            desc.Columns,
		ValNeededForCol: valNeededForCol,
	}
	return cb.fetcher.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &cb.alloc, tableArgs,
	)
}

// RunColumnBackfillChunk runs column backfill over a chunk of the table using
// the span sp provided, for all updateCols.
func (cb *ColumnBackfiller) RunColumnBackfillChunk(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	otherTables []*sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	fkTables, _ := row.TablesNeededForFKs(
		ctx,
		tableDesc,
		row.CheckUpdates,
		row.NoLookup,
		row.NoCheckPrivilege,
		nil, /* AnalyzeExprFunction */
	)
	for i, fkTableDesc := range otherTables {
		found, ok := fkTables[fkTableDesc.ID]
		if !ok {
			// We got passed an extra table for some reason - just ignore it.
			continue
		}

		found.Table = otherTables[i]
		fkTables[fkTableDesc.ID] = found
	}
	for id, table := range fkTables {
		if table.Table == nil {
			// We weren't passed all of the tables that we need by the coordinator.
			return roachpb.Key{}, errors.Errorf("table %v not sent by coordinator", id)
		}
	}
	// TODO(dan): Tighten up the bound on the requestedCols parameter to
	// makeRowUpdater.
	requestedCols := make([]sqlbase.ColumnDescriptor, 0, len(tableDesc.Columns)+len(cb.added))
	requestedCols = append(requestedCols, tableDesc.Columns...)
	requestedCols = append(requestedCols, cb.added...)
	ru, err := row.MakeUpdater(
		txn,
		tableDesc,
		fkTables,
		cb.updateCols,
		requestedCols,
		row.UpdaterOnlyColumns,
		cb.evalCtx,
		&cb.alloc,
	)
	if err != nil {
		return roachpb.Key{}, err
	}

	// TODO(dan): This check is an unfortunate bleeding of the internals of
	// rowUpdater. Extract the sql row to k/v mapping logic out into something
	// usable here.
	if !ru.IsColumnOnlyUpdate() {
		panic("only column data should be modified, but the rowUpdater is configured otherwise")
	}

	// Get the next set of rows.
	//
	// Running the scan and applying the changes in many transactions
	// is fine because the schema change is in the correct state to
	// handle intermediate OLTP commands which delete and add values
	// during the scan. Index entries in the new index are being
	// populated and deleted by the OLTP commands but not otherwise
	// read or used
	if err := cb.fetcher.StartScan(
		ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize, traceKV,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return roachpb.Key{}, err
	}

	oldValues := make(tree.Datums, len(ru.FetchCols))
	updateValues := make(tree.Datums, len(cb.updateExprs))
	b := txn.NewBatch()
	rowLength := 0
	iv := &sqlbase.RowIndexedVarContainer{
		Cols:    append(tableDesc.Columns, cb.added...),
		Mapping: ru.FetchColIDtoRowIndex,
	}
	cb.evalCtx.IVarContainer = iv
	for i := int64(0); i < chunkSize; i++ {
		datums, _, _, err := cb.fetcher.NextRowDecoded(ctx)
		if err != nil {
			return roachpb.Key{}, err
		}
		if datums == nil {
			break
		}
		iv.CurSourceRow = datums

		// Evaluate the new values. This must be done separately for
		// each row so as to handle impure functions correctly.
		for j, e := range cb.updateExprs {
			val, err := e.Eval(cb.evalCtx)
			if err != nil {
				return roachpb.Key{}, sqlbase.NewInvalidSchemaDefinitionError(err)
			}
			if j < len(cb.added) && !cb.added[j].Nullable && val == tree.DNull {
				return roachpb.Key{}, sqlbase.NewNonNullViolationError(cb.added[j].Name)
			}

			// Added computed column values should be usable for the next
			// added columns being backfilled. They have already been type
			// checked.
			if j < len(cb.added) {
				iv.CurSourceRow = append(iv.CurSourceRow, val)
			}
			updateValues[j] = val
		}
		copy(oldValues, datums)
		// Update oldValues with NULL values where values weren't found;
		// only update when necessary.
		if rowLength != len(datums) {
			rowLength = len(datums)
			for j := rowLength; j < len(oldValues); j++ {
				oldValues[j] = tree.DNull
			}
		}
		if _, err := ru.UpdateRow(
			ctx, b, oldValues, updateValues, row.CheckFKs, traceKV,
		); err != nil {
			return roachpb.Key{}, err
		}
	}
	// Write the new row values.
	writeBatch := txn.Run
	if alsoCommit {
		writeBatch = txn.CommitInBatch
	}
	if err := writeBatch(ctx, b); err != nil {
		return roachpb.Key{}, ConvertBackfillError(ctx, tableDesc, b)
	}
	return cb.fetcher.Key(), nil
}

// ConvertBackfillError returns a cleaner SQL error for a failed Batch.
func ConvertBackfillError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, b *client.Batch,
) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc := sqlbase.NewMutableExistingTableDescriptor(*protoutil.Clone(tableDesc.TableDesc()).(*sqlbase.TableDescriptor))
	mutationID := desc.Mutations[0].MutationID
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		if err := desc.MakeMutationComplete(mutation); err != nil {
			return errors.Wrap(err, "backfill error")
		}
	}
	return row.ConvertBatchError(ctx, sqlbase.NewImmutableTableDescriptor(*desc.TableDesc()), b)
}

// IndexBackfiller is capable of backfilling all the added index.
type IndexBackfiller struct {
	backfiller

	added []sqlbase.IndexDescriptor
	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap map[sqlbase.ColumnID]int

	types   []sqlbase.ColumnType
	rowVals tree.Datums
}

// Init initializes an IndexBackfiller.
func (ib *IndexBackfiller) Init(desc *sqlbase.ImmutableTableDescriptor) error {
	numCols := len(desc.Columns)
	cols := desc.Columns
	if len(desc.Mutations) > 0 {
		cols = make([]sqlbase.ColumnDescriptor, 0, numCols+len(desc.Mutations))
		cols = append(cols, desc.Columns...)
		for _, m := range desc.Mutations {
			if column := m.GetColumn(); column != nil &&
				m.Direction == sqlbase.DescriptorMutation_ADD &&
				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				cols = append(cols, *column)
			}
		}
	}

	var valNeededForCol util.FastIntSet
	mutationID := desc.Mutations[0].MutationID
	for _, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if IndexMutationFilter(m) {
			idx := m.GetIndex()
			ib.added = append(ib.added, *idx)
			for i, col := range cols {
				if idx.ContainsColumnID(col.ID) {
					valNeededForCol.Add(i)
				}
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

	tableArgs := row.FetcherTableArgs{
		Desc:            desc,
		Index:           &desc.PrimaryIndex,
		ColIdxMap:       ib.colIdxMap,
		Cols:            cols,
		ValNeededForCol: valNeededForCol,
	}
	return ib.fetcher.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &ib.alloc, tableArgs,
	)
}

// BuildIndexEntriesChunk reads a chunk of rows from a table using the span sp
// provided, and builds all the added indexes.
func (ib *IndexBackfiller) BuildIndexEntriesChunk(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	traceKV bool,
) ([]sqlbase.IndexEntry, roachpb.Key, error) {
	entries := make([]sqlbase.IndexEntry, 0, chunkSize*int64(len(ib.added)))

	// Get the next set of rows.
	//
	// Running the scan and applying the changes in many transactions
	// is fine because the schema change is in the correct state to
	// handle intermediate OLTP commands which delete and add values
	// during the scan. Index entries in the new index are being
	// populated and deleted by the OLTP commands but not otherwise
	// read or used
	if err := ib.fetcher.StartScan(
		ctx, txn, []roachpb.Span{sp}, true /* limitBatches */, chunkSize, traceKV,
	); err != nil {
		log.Errorf(ctx, "scan error: %s", err)
		return nil, nil, err
	}

	buffer := make([]sqlbase.IndexEntry, len(ib.added))
	for i := int64(0); i < chunkSize; i++ {
		encRow, _, _, err := ib.fetcher.NextRow(ctx)
		if err != nil {
			return nil, nil, err
		}
		if encRow == nil {
			break
		}
		if len(ib.rowVals) == 0 {
			ib.rowVals = make(tree.Datums, len(encRow))
		}
		if err := sqlbase.EncDatumRowToDatums(ib.types, ib.rowVals, encRow, &ib.alloc); err != nil {
			return nil, nil, err
		}

		// We're resetting the length of this slice for variable length indexes such as inverted
		// indexes which can append entries to the end of the slice. If we don't do this, then everything
		// EncodeSecondaryIndexes appends to secondaryIndexEntries for a row, would stay in the slice for
		// subsequent rows and we would then have duplicates in entries on output.
		buffer = buffer[:len(ib.added)]
		if buffer, err = sqlbase.EncodeSecondaryIndexes(
			tableDesc.TableDesc(), ib.added, ib.colIdxMap,
			ib.rowVals, buffer); err != nil {
			return nil, nil, err
		}
		entries = append(entries, buffer...)
	}
	return entries, ib.fetcher.Key(), nil
}

// RunIndexBackfillChunk runs an index backfill over a chunk of the table
// by traversing the span sp provided. The backfill is run for the added
// indexes.
func (ib *IndexBackfiller) RunIndexBackfillChunk(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	alsoCommit bool,
	traceKV bool,
) (roachpb.Key, error) {
	entries, key, err := ib.BuildIndexEntriesChunk(ctx, txn, tableDesc, sp, chunkSize, traceKV)
	if err != nil {
		return nil, err
	}
	batch := txn.NewBatch()

	for _, entry := range entries {
		if traceKV {
			log.VEventf(ctx, 2, "InitPut %s -> %s", entry.Key, entry.Value.PrettyPrint())
		}
		batch.InitPut(entry.Key, &entry.Value, false /* failOnTombstones */)
	}
	writeBatch := txn.Run
	if alsoCommit {
		writeBatch = txn.CommitInBatch
	}
	if err := writeBatch(ctx, batch); err != nil {
		return nil, ConvertBackfillError(ctx, tableDesc, batch)
	}
	return key, nil
}
