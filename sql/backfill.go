// Copyright 2015 The Cockroach Authors.
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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/pkg/errors"
)

func makeColIDtoRowIndex(row planNode, desc *sqlbase.TableDescriptor) (
	map[sqlbase.ColumnID]int, error,
) {
	columns := row.Columns()
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(columns))
	for i, column := range columns {
		s, idx, err := desc.FindColumnByName(column.Name)
		if err != nil {
			return nil, err
		}
		switch s {
		case sqlbase.DescriptorActive:
			colIDtoRowIndex[desc.Columns[idx].ID] = i
		case sqlbase.DescriptorIncomplete:
			colIDtoRowIndex[desc.Mutations[idx].GetColumn().ID] = i
		default:
			panic("unreachable")
		}
	}
	return colIDtoRowIndex, nil
}

var _ sort.Interface = columnsByID{}
var _ sort.Interface = indexesByID{}

type columnsByID []sqlbase.ColumnDescriptor

func (cds columnsByID) Len() int {
	return len(cds)
}
func (cds columnsByID) Less(i, j int) bool {
	return cds[i].ID < cds[j].ID
}
func (cds columnsByID) Swap(i, j int) {
	cds[i], cds[j] = cds[j], cds[i]
}

type indexesByID []sqlbase.IndexDescriptor

func (ids indexesByID) Len() int {
	return len(ids)
}
func (ids indexesByID) Less(i, j int) bool {
	return ids[i].ID < ids[j].ID
}
func (ids indexesByID) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}

func convertBackfillError(tableDesc *sqlbase.TableDescriptor, b *client.Batch) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// convertBatchError() will only work correctly if the schema elements are
	// "live" in the tableDesc. Apply the mutations belonging to the same
	// mutationID to make all the mutations live in tableDesc. Note: this
	// tableDesc is not written to the k:v store.
	mutationID := tableDesc.Mutations[0].MutationID
	for _, mutation := range tableDesc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		tableDesc.MakeMutationComplete(mutation)
	}
	return convertBatchError(tableDesc, b)
}

// runBackfill runs the backfill for the schema changer.
func (sc *SchemaChanger) runBackfill(lease *sqlbase.TableDescriptor_SchemaChangeLease) error {
	l, err := sc.ExtendLease(*lease)
	if err != nil {
		return err
	}
	*lease = l

	// Mutations are applied in a FIFO order. Only apply the first set of
	// mutations. Collect the elements that are part of the mutation.
	var droppedColumnDescs []sqlbase.ColumnDescriptor
	var droppedIndexDescs []sqlbase.IndexDescriptor
	var addedColumnDescs []sqlbase.ColumnDescriptor
	var addedIndexDescs []sqlbase.IndexDescriptor
	if err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}

		for _, m := range tableDesc.Mutations {
			if m.MutationID != sc.mutationID {
				break
			}
			switch m.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch t := m.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					addedColumnDescs = append(addedColumnDescs, *t.Column)
				case *sqlbase.DescriptorMutation_Index:
					addedIndexDescs = append(addedIndexDescs, *t.Index)
				default:
					return errors.Errorf("unsupported mutation: %+v", m)
				}

			case sqlbase.DescriptorMutation_DROP:
				switch t := m.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					droppedColumnDescs = append(droppedColumnDescs, *t.Column)
				case *sqlbase.DescriptorMutation_Index:
					droppedIndexDescs = append(droppedIndexDescs, *t.Index)
				default:
					return errors.Errorf("unsupported mutation: %+v", m)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// First drop indexes, then add/drop columns, and only then add indexes.

	// Drop indexes.
	if err := sc.truncateIndexes(lease, droppedIndexDescs); err != nil {
		return err
	}

	// Add and drop columns.
	if err := sc.truncateAndBackfillColumns(
		lease, addedColumnDescs, droppedColumnDescs,
	); err != nil {
		return err
	}

	// Add new indexes.
	if err := sc.backfillIndexes(lease, addedIndexDescs); err != nil {
		return err
	}

	return nil
}

// getTableSpan returns a span containing the start and end key for a table.
func (sc *SchemaChanger) getTableSpan() (sqlbase.Span, error) {
	var tableDesc *sqlbase.TableDescriptor
	if err := sc.db.Txn(func(txn *client.Txn) error {
		var err error
		tableDesc, err = sqlbase.GetTableDescFromID(txn, sc.tableID)
		return err
	}); err != nil {
		return sqlbase.Span{}, err
	}
	prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID))
	return sqlbase.Span{
		Start: prefix,
		End:   prefix.PrefixEnd(),
	}, nil
}

// ColumnTruncateAndBackfillChunkSize is the maximum number of rows of keys
// processed per chunk during the column truncate or backfill.
//
// TODO(vivek): Run some experiments to set this value to something sensible
// or adjust it dynamically. Also add in a sleep after every chunk is
// processed to slow down the backfill and reduce its CPU usage.
const ColumnTruncateAndBackfillChunkSize = 600

func (sc *SchemaChanger) truncateAndBackfillColumns(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	added []sqlbase.ColumnDescriptor,
	dropped []sqlbase.ColumnDescriptor,
) error {
	// Set the eval context timestamps.
	pTime := timeutil.Now()
	sc.evalCtx = parser.EvalContext{}
	sc.evalCtx.SetTxnTimestamp(pTime)
	sc.evalCtx.SetStmtTimestamp(pTime)
	defaultExprs, err := makeDefaultExprs(added, &parser.Parser{}, &sc.evalCtx)
	if err != nil {
		return err
	}

	// Note if there is a new non nullable column with no default value.
	addingNonNullableColumn := false
	for _, columnDesc := range added {
		if columnDesc.DefaultExpr == nil && !columnDesc.Nullable {
			addingNonNullableColumn = true
			break
		}
	}

	// Add or Drop a column.
	if len(dropped) > 0 || addingNonNullableColumn || len(defaultExprs) > 0 {
		// Initialize start and end to represent a span of keys.
		sp, err := sc.getTableSpan()
		if err != nil {
			return err
		}

		// Run through the entire table key space adding and deleting columns.
		for done := false; !done; {
			// First extend the schema change lease.
			l, err := sc.ExtendLease(*lease)
			if err != nil {
				return err
			}
			*lease = l

			// Add and delete columns for a chunk of the key space.
			sp.Start, done, err = sc.truncateAndBackfillColumnsChunk(
				added, dropped, defaultExprs, &sc.evalCtx, sp,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sc *SchemaChanger) truncateAndBackfillColumnsChunk(
	added []sqlbase.ColumnDescriptor,
	dropped []sqlbase.ColumnDescriptor,
	defaultExprs []parser.TypedExpr,
	evalCtx *parser.EvalContext,
	sp sqlbase.Span,
) (roachpb.Key, bool, error) {
	var curIndexKey roachpb.Key
	done := false
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		// Short circuit the backfill if the table has been deleted.
		if tableDesc.Deleted() {
			done = true
			return nil
		}

		updateCols := append(added, dropped...)
		fkTables := TablesNeededForFKs(*tableDesc, CheckUpdates)
		for k := range fkTables {
			if fkTables[k], err = sqlbase.GetTableDescFromID(txn, k); err != nil {
				return err
			}
		}
		// TODO(dan): Tighten up the bound on the requestedCols parameter to
		// makeRowUpdater.
		requestedCols := make([]sqlbase.ColumnDescriptor, 0, len(tableDesc.Columns)+len(added))
		requestedCols = append(requestedCols, tableDesc.Columns...)
		requestedCols = append(requestedCols, added...)
		ru, err := makeRowUpdater(
			txn, tableDesc, fkTables, updateCols, requestedCols, rowUpdaterOnlyColumns,
		)
		if err != nil {
			return err
		}

		// TODO(dan): This check is an unfortunate bleeding of the internals of
		// rowUpdater. Extract the sql row to k/v mapping logic out into something
		// usable here.
		if !ru.isColumnOnlyUpdate() {
			panic("only column data should be modified, but the rowUpdater is configured otherwise")
		}

		// Run a scan across the table using the primary key. Running
		// the scan and applying the changes in many transactions is
		// fine because the schema change is in the correct state to
		// handle intermediate OLTP commands which delete and add
		// values during the scan.
		var rf sqlbase.RowFetcher
		colIDtoRowIndex := colIDtoRowIndexFromCols(tableDesc.Columns)
		valNeededForCol := make([]bool, len(tableDesc.Columns))
		for i := range valNeededForCol {
			_, valNeededForCol[i] = ru.fetchColIDtoRowIndex[tableDesc.Columns[i].ID]
		}
		err = rf.Init(tableDesc, colIDtoRowIndex, &tableDesc.PrimaryIndex, false, false,
			tableDesc.Columns, valNeededForCol)
		if err != nil {
			return err
		}
		// StartScan uses 0 as a sentinal for the default limit of entries scanned.
		if err := rf.StartScan(txn, sqlbase.Spans{sp}, 0); err != nil {
			return err
		}

		indexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
		oldValues := make(parser.DTuple, len(ru.fetchCols))
		updateValues := make(parser.DTuple, len(updateCols))

		writeBatch := &client.Batch{}
		var i int
		for ; i < ColumnTruncateAndBackfillChunkSize; i++ {
			row, err := rf.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				break // Done
			}

			curIndexKey, _, err = sqlbase.EncodeIndexKey(
				tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, row, indexKeyPrefix)

			for j, col := range added {
				if defaultExprs == nil || defaultExprs[j] == nil {
					updateValues[j] = parser.DNull
				} else {
					updateValues[j], err = defaultExprs[j].Eval(evalCtx)
					if err != nil {
						return err
					}
				}
				if !col.Nullable && updateValues[j].Compare(parser.DNull) == 0 {
					return sqlbase.NewNonNullViolationError(col.Name)
				}
			}
			for j := range dropped {
				updateValues[j+len(added)] = parser.DNull
			}

			copy(oldValues, row)
			for j := len(row); j < len(oldValues); j++ {
				oldValues[j] = parser.DNull
			}
			if _, err := ru.updateRow(context.TODO(), writeBatch, oldValues, updateValues); err != nil {
				return err
			}
		}
		if i < ColumnTruncateAndBackfillChunkSize {
			done = true
		}

		if err := txn.Run(writeBatch); err != nil {
			return convertBackfillError(tableDesc, writeBatch)
		}
		return nil
	})
	return curIndexKey.PrefixEnd(), done, err
}

func (sc *SchemaChanger) truncateIndexes(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	dropped []sqlbase.IndexDescriptor,
) error {
	for _, desc := range dropped {
		// First extend the schema change lease.
		l, err := sc.ExtendLease(*lease)
		if err != nil {
			return err
		}
		*lease = l
		if err := sc.db.Txn(func(txn *client.Txn) error {
			tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
			if err != nil {
				return err
			}
			// Short circuit the truncation if the table has been deleted.
			if tableDesc.Deleted() {
				return nil
			}

			rd, err := makeRowDeleter(txn, tableDesc, nil, nil, false)
			if err != nil {
				return err
			}
			td := tableDeleter{rd: rd}
			if err := td.init(txn); err != nil {
				return err
			}
			return td.deleteIndex(context.TODO(), &desc)
		}); err != nil {
			return err
		}
	}
	return nil
}

// IndexBackfillChunkSize is the maximum number of rows processed per chunk
// during the index backfill.
//
// TODO(vivek) Run some experiments to set this value to something sensible or
// adjust it dynamically. Also add in a sleep after every chunk is processed,
// to slow down the backfill and not have it interfere with OLTP commands.
const IndexBackfillChunkSize = 100

func (sc *SchemaChanger) backfillIndexes(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	added []sqlbase.IndexDescriptor,
) error {
	if len(added) == 0 {
		return nil
	}

	// Initialize start and end to represent a span of keys.
	sp, err := sc.getTableSpan()
	if err != nil {
		return err
	}

	// Backfill the index entries for all the rows.
	for done := false; !done; {
		// First extend the schema change lease.
		l, err := sc.ExtendLease(*lease)
		if err != nil {
			return err
		}
		*lease = l

		sp.Start, done, err = sc.backfillIndexesChunk(added, sp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *SchemaChanger) backfillIndexesChunk(
	added []sqlbase.IndexDescriptor,
	sp sqlbase.Span,
) (roachpb.Key, bool, error) {
	var nextKey roachpb.Key
	done := false
	err := sc.db.Txn(func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		// Short circuit the backfill if the table has been deleted.
		if tableDesc.Deleted() {
			done = true
			return nil
		}

		// Get the next set of rows.
		// TODO(tamird): Support partial indexes?
		//
		// Use a scanNode with SELECT to pass in a sqlbase.TableDescriptor to the
		// SELECT without needing to use a parser.QualifiedName, because we
		// want to run schema changes from a gossip feed of table IDs. Running
		// the scan and applying the changes in many transactions is fine
		// because the schema change is in the correct state to handle
		// intermediate OLTP commands which delete and add values during the
		// scan.
		planner := makePlanner()
		planner.setTxn(txn)
		scan := planner.Scan()
		scan.desc = *tableDesc
		scan.spans = []sqlbase.Span{sp}
		scan.initDescDefaults(publicAndNonPublicColumns)
		rows, err := selectIndex(scan, nil, false)
		if err != nil {
			return err
		}

		if err := rows.Start(); err != nil {
			return err
		}

		// Construct a map from column ID to the index the value appears at
		// within a row.
		colIDtoRowIndex, err := makeColIDtoRowIndex(rows, tableDesc)
		if err != nil {
			return err
		}
		b := &client.Batch{}
		numRows := 0
		for ; numRows < IndexBackfillChunkSize; numRows++ {
			if next, err := rows.Next(); !next {
				if err != nil {
					return err
				}
				break
			}
			rowVals := rows.Values()

			for _, desc := range added {
				secondaryIndexEntries := make([]sqlbase.IndexEntry, 1)
				err := sqlbase.EncodeSecondaryIndexes(
					tableDesc, []sqlbase.IndexDescriptor{desc}, colIDtoRowIndex,
					rowVals, secondaryIndexEntries)
				if err != nil {
					return err
				}
				for _, secondaryIndexEntry := range secondaryIndexEntries {
					if log.V(2) {
						log.Infof(context.TODO(), "InitPut %s -> %v", secondaryIndexEntry.Key,
							secondaryIndexEntry.Value)
					}
					b.InitPut(secondaryIndexEntry.Key, &secondaryIndexEntry.Value)
				}
			}
		}
		// Write the new index values.
		if err := txn.Run(b); err != nil {
			return convertBackfillError(tableDesc, b)
		}
		// Have we processed all the table rows?
		if numRows < IndexBackfillChunkSize {
			done = true
			return nil
		}
		// Keep track of the next key.
		nextKey = scan.fetcher.Key()
		return nil
	})
	return nextKey, done, err
}
