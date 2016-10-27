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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// TODO(vivek): Replace these constants with a runtime budget for the
	// operation chunk involved.

	// ColumnTruncateAndBackfillChunkSize is the maximum number of columns
	// processed per chunk during column truncate or backfill.
	ColumnTruncateAndBackfillChunkSize = 200

	// IndexTruncateChunkSize is the maximum number of index entries truncated
	// per chunk during an index truncation. This value is larger than the
	// other chunk constants because the operation involves only running a
	// DeleteRange().
	IndexTruncateChunkSize = 600

	// IndexBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill. The index backfill involves a table
	// scan, and a number of individual ops presented in a batch. This value
	// is smaller than ColumnTruncateAndBackfillChunkSize, because it involves
	// a number of individual index row updates that can be scattered over
	// many ranges.
	IndexBackfillChunkSize = 100

	// CheckpointInterval is the interval after which a checkpoint of the
	// schema change is posted.
	CheckpointInterval = 10 * time.Second
)

func makeColIDtoRowIndex(
	row planNode, desc *sqlbase.TableDescriptor,
) (map[sqlbase.ColumnID]int, error) {
	columns := row.Columns()
	colIDtoRowIndex := make(map[sqlbase.ColumnID]int, len(columns))
	for i, column := range columns {
		s, idx, err := desc.FindColumnByNormalizedName(parser.ReNormalizeName(column.Name))
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
	// Indexes within the Mutations slice for checkpointing.
	mutationSentinel := -1
	columnMutationIdx, addedIndexMutationIdx, droppedIndexMutationIdx :=
		mutationSentinel, mutationSentinel, mutationSentinel

	if err := sc.db.Txn(context.TODO(), func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}

		for i, m := range tableDesc.Mutations {
			if m.MutationID != sc.mutationID {
				break
			}
			switch m.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch t := m.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					addedColumnDescs = append(addedColumnDescs, *t.Column)
					if columnMutationIdx == mutationSentinel {
						columnMutationIdx = i
					}
				case *sqlbase.DescriptorMutation_Index:
					addedIndexDescs = append(addedIndexDescs, *t.Index)
					if addedIndexMutationIdx == mutationSentinel {
						addedIndexMutationIdx = i
					}
				default:
					return errors.Errorf("unsupported mutation: %+v", m)
				}

			case sqlbase.DescriptorMutation_DROP:
				switch t := m.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					droppedColumnDescs = append(droppedColumnDescs, *t.Column)
					if columnMutationIdx == mutationSentinel {
						columnMutationIdx = i
					}
				case *sqlbase.DescriptorMutation_Index:
					droppedIndexDescs = append(droppedIndexDescs, *t.Index)
					if droppedIndexMutationIdx == mutationSentinel {
						droppedIndexMutationIdx = i
					}
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
	if err := sc.truncateIndexes(lease, droppedIndexDescs, droppedIndexMutationIdx); err != nil {
		return err
	}

	// Add and drop columns.
	if err := sc.truncateAndBackfillColumns(
		lease, addedColumnDescs, droppedColumnDescs, columnMutationIdx,
	); err != nil {
		return err
	}

	// Add new indexes.
	if err := sc.backfillIndexes(lease, addedIndexDescs, addedIndexMutationIdx); err != nil {
		return err
	}

	return nil
}

// getTableSpan returns a span stored at a checkpoint idx, or in the absence
// of a checkpoint, the span over all keys within a table.
func (sc *SchemaChanger) getTableSpan(mutationIdx int) (roachpb.Span, error) {
	var tableDesc *sqlbase.TableDescriptor
	if err := sc.db.Txn(context.TODO(), func(txn *client.Txn) error {
		var err error
		tableDesc, err = sqlbase.GetTableDescFromID(txn, sc.tableID)
		return err
	}); err != nil {
		return roachpb.Span{}, err
	}
	if len(tableDesc.Mutations) < mutationIdx {
		return roachpb.Span{},
			errors.Errorf("cannot find idx %d among %d mutations", mutationIdx, len(tableDesc.Mutations))
	}
	if mutationID := tableDesc.Mutations[mutationIdx].MutationID; mutationID != sc.mutationID {
		return roachpb.Span{},
			errors.Errorf("mutation index pointing to the wrong schema change, %d vs expected %d", mutationID, sc.mutationID)
	}
	resumeSpan := tableDesc.Mutations[mutationIdx].ResumeSpan
	if resumeSpan.Key != nil {
		return resumeSpan, nil
	}
	prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID))
	return roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}, nil
}

func (sc *SchemaChanger) maybeWriteResumeSpan(
	txn *client.Txn,
	tableDesc *sqlbase.TableDescriptor,
	resume roachpb.Span,
	mutationIdx int,
	lastCheckpoint *time.Time,
) error {
	checkpointInterval := CheckpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		checkpointInterval = sc.testingKnobs.WriteCheckpointInterval
	}
	if timeutil.Since(*lastCheckpoint) < checkpointInterval {
		return nil
	}
	tableDesc.Mutations[mutationIdx].ResumeSpan = resume
	txn.SetSystemConfigTrigger()
	if err := txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.GetID()),
		sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return err
	}
	*lastCheckpoint = timeutil.Now()
	return nil
}

func (sc *SchemaChanger) truncateAndBackfillColumns(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	added []sqlbase.ColumnDescriptor,
	dropped []sqlbase.ColumnDescriptor,
	mutationIdx int,
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
		// Initialize a span of keys.
		sp, err := sc.getTableSpan(mutationIdx)
		if err != nil {
			return err
		}

		// Run through the entire table key space adding and deleting columns.
		const chunkSize = ColumnTruncateAndBackfillChunkSize
		// Evaluate default values.
		updateCols := append(added, dropped...)
		updateValues := make(parser.DTuple, len(updateCols))
		var nonNullViolationColumnName string
		for j, col := range added {
			if defaultExprs == nil || defaultExprs[j] == nil {
				updateValues[j] = parser.DNull
			} else {
				updateValues[j], err = defaultExprs[j].Eval(&sc.evalCtx)
				if err != nil {
					return err
				}
			}
			if !col.Nullable && updateValues[j].Compare(parser.DNull) == 0 {
				nonNullViolationColumnName = col.Name
			}
		}
		for j := range dropped {
			updateValues[j+len(added)] = parser.DNull
		}
		lastCheckpoint := timeutil.Now()
		for row, done := int64(0), false; !done; row += chunkSize {
			// First extend the schema change lease.
			l, err := sc.ExtendLease(*lease)
			if err != nil {
				return err
			}
			*lease = l
			if log.V(2) {
				log.Infof(context.TODO(), "column schema change (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, row, sp)
			}

			// Add and delete columns for a chunk of the key space.
			sp.Key, done, err = sc.truncateAndBackfillColumnsChunk(
				added, dropped, defaultExprs, sp,
				updateValues, nonNullViolationColumnName, chunkSize, mutationIdx, &lastCheckpoint)
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
	sp roachpb.Span,
	updateValues parser.DTuple,
	nonNullViolationColumnName string,
	chunkSize int64,
	mutationIdx int,
	lastCheckpoint *time.Time,
) (roachpb.Key, bool, error) {
	done := false
	var nextKey roachpb.Key
	err := sc.db.Txn(context.TODO(), func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		// Short circuit the backfill if the table has been deleted.
		if tableDesc.Deleted() {
			done = true
			return nil
		}

		if sc.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := sc.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}

		updateCols := append(added, dropped...)
		fkTables := tablesNeededForFKs(*tableDesc, CheckUpdates)
		for k := range fkTables {
			table, err := sqlbase.GetTableDescFromID(txn, k)
			if err != nil {
				return err
			}
			fkTables[k] = tableLookup{table: table}
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
		if err := rf.Init(
			tableDesc, colIDtoRowIndex, &tableDesc.PrimaryIndex, false, false,
			tableDesc.Columns, valNeededForCol,
		); err != nil {
			return err
		}
		if err := rf.StartScan(
			txn, roachpb.Spans{sp}, true /* limit batches */, chunkSize,
		); err != nil {
			return err
		}

		oldValues := make(parser.DTuple, len(ru.fetchCols))
		writeBatch := txn.NewBatch()
		rowLength := 0
		var lastRowSeen parser.DTuple
		i := int64(0)
		for ; i < chunkSize; i++ {
			row, err := rf.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
			lastRowSeen = row
			if nonNullViolationColumnName != "" {
				return sqlbase.NewNonNullViolationError(nonNullViolationColumnName)
			}

			copy(oldValues, row)
			// Update oldValues with NULL values where values weren't found;
			// only update when necessary.
			if rowLength != len(row) {
				rowLength = len(row)
				for j := rowLength; j < len(oldValues); j++ {
					oldValues[j] = parser.DNull
				}
			}
			if _, err := ru.updateRow(txn.Context, writeBatch, oldValues, updateValues); err != nil {
				return err
			}
		}
		if err := txn.Run(writeBatch); err != nil {
			return convertBackfillError(tableDesc, writeBatch)
		}
		if i < chunkSize {
			done = true
			return nil
		}
		curIndexKey, _, err := sqlbase.EncodeIndexKey(
			tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, lastRowSeen,
			sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID))
		if err != nil {
			return err
		}
		resume := roachpb.Span{Key: roachpb.Key(curIndexKey).PrefixEnd(), EndKey: sp.EndKey}
		if err := sc.maybeWriteResumeSpan(txn, tableDesc, resume, mutationIdx, lastCheckpoint); err != nil {
			return err
		}
		nextKey = resume.Key
		return nil
	})
	return nextKey, done, err
}

func (sc *SchemaChanger) truncateIndexes(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	dropped []sqlbase.IndexDescriptor,
	mutationIdx int,
) error {
	const chunkSize = IndexTruncateChunkSize
	for _, desc := range dropped {
		var resume roachpb.Span
		lastCheckpoint := timeutil.Now()
		for row, done := int64(0), false; !done; row += chunkSize {
			// First extend the schema change lease.
			l, err := sc.ExtendLease(*lease)
			if err != nil {
				return err
			}
			*lease = l

			resumeAt := resume
			if log.V(2) {
				log.Infof(context.TODO(), "drop index (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, row, resume)
			}
			if err := sc.db.Txn(context.TODO(), func(txn *client.Txn) error {
				tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
				if err != nil {
					return err
				}
				// Short circuit the truncation if the table has been deleted.
				if tableDesc.Deleted() {
					done = true
					return nil
				}

				if sc.testingKnobs.RunBeforeBackfillChunk != nil {
					if err := sc.testingKnobs.RunBeforeBackfillChunk(resume); err != nil {
						return err
					}
				}

				rd, err := makeRowDeleter(txn, tableDesc, nil, nil, false)
				if err != nil {
					return err
				}
				td := tableDeleter{rd: rd}
				if err := td.init(txn); err != nil {
					return err
				}
				resume, err = td.deleteIndex(
					txn.Context, &desc, resumeAt, chunkSize,
				)
				if err != nil {
					return err
				}
				if err := sc.maybeWriteResumeSpan(txn, tableDesc, resume, mutationIdx, &lastCheckpoint); err != nil {
					return err
				}
				done = resume.Key == nil
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sc *SchemaChanger) backfillIndexes(
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	added []sqlbase.IndexDescriptor,
	mutationIdx int,
) error {
	if len(added) == 0 {
		return nil
	}

	// Initialize a span of keys.
	sp, err := sc.getTableSpan(mutationIdx)
	if err != nil {
		return err
	}

	// Backfill the index entries for all the rows.
	const chunkSize = IndexBackfillChunkSize
	lastCheckpoint := timeutil.Now()
	for row, done := int64(0), false; !done; row += chunkSize {
		// First extend the schema change lease.
		l, err := sc.ExtendLease(*lease)
		if err != nil {
			return err
		}
		*lease = l
		if log.V(2) {
			log.Infof(context.TODO(), "index add (%d, %d) at row: %d, span: %s",
				sc.tableID, sc.mutationID, row, sp)
		}
		sp.Key, done, err = sc.backfillIndexesChunk(added, sp, chunkSize, mutationIdx, &lastCheckpoint)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *SchemaChanger) backfillIndexesChunk(
	added []sqlbase.IndexDescriptor,
	sp roachpb.Span,
	chunkSize int64,
	mutationIdx int,
	lastCheckpoint *time.Time,
) (roachpb.Key, bool, error) {
	var nextKey roachpb.Key
	done := false
	secondaryIndexEntries := make([]sqlbase.IndexEntry, len(added))
	err := sc.db.Txn(context.TODO(), func(txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		// Short circuit the backfill if the table has been deleted.
		if tableDesc.Deleted() {
			done = true
			return nil
		}

		if sc.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := sc.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}

		// Get the next set of rows.
		// TODO(tamird): Support partial indexes?
		//
		// Use a scanNode with SELECT to pass in a sqlbase.TableDescriptor
		// to the SELECT without needing to go through table name
		// resolution, because we want to run schema changes from a gossip
		// feed of table IDs. Running the scan and applying the changes in
		// many transactions is fine because the schema change is in the
		// correct state to handle intermediate OLTP commands which delete
		// and add values during the scan.
		planner := makePlanner("backfill")
		planner.setTxn(txn)
		scan := planner.Scan()
		scan.desc = *tableDesc
		scan.spans = []roachpb.Span{sp}
		scan.SetLimitHint(chunkSize, false)
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
		numRows := int64(0)
		for ; numRows < chunkSize; numRows++ {
			if next, err := rows.Next(); !next {
				if err != nil {
					return err
				}
				break
			}
			rowVals := rows.Values()

			err := sqlbase.EncodeSecondaryIndexes(
				tableDesc, added, colIDtoRowIndex,
				rowVals, secondaryIndexEntries)
			if err != nil {
				return err
			}
			for _, secondaryIndexEntry := range secondaryIndexEntries {
				if log.V(2) {
					log.Infof(txn.Context, "InitPut %s -> %v", secondaryIndexEntry.Key,
						secondaryIndexEntry.Value)
				}
				b.InitPut(secondaryIndexEntry.Key, &secondaryIndexEntry.Value)
			}
		}
		// Write the new index values.
		if err := txn.Run(b); err != nil {
			return convertBackfillError(tableDesc, b)
		}
		// Have we processed all the table rows?
		if numRows < chunkSize {
			done = true
			return nil
		}
		// Keep track of the next key.
		resume := roachpb.Span{Key: scan.fetcher.Key(), EndKey: sp.EndKey}
		if err := sc.maybeWriteResumeSpan(txn, tableDesc, resume, mutationIdx, lastCheckpoint); err != nil {
			return err
		}
		nextKey = resume.Key
		return nil
	})
	return nextKey, done, err
}
