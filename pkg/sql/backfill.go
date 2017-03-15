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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// TODO(vivek): Replace these constants with a runtime budget for the
	// operation chunk involved.

	// columnTruncateAndBackfillChunkSize is the maximum number of columns
	// processed per chunk during column truncate or backfill.
	columnTruncateAndBackfillChunkSize = 200

	// indexTruncateChunkSize is the maximum number of index entries truncated
	// per chunk during an index truncation. This value is larger than the
	// other chunk constants because the operation involves only running a
	// DeleteRange().
	indexTruncateChunkSize = 600

	// indexBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill. The index backfill involves a table
	// scan, and a number of individual ops presented in a batch. This value
	// is smaller than ColumnTruncateAndBackfillChunkSize, because it involves
	// a number of individual index row updates that can be scattered over
	// many ranges.
	indexBackfillChunkSize = 100

	// checkpointInterval is the interval after which a checkpoint of the
	// schema change is posted.
	checkpointInterval = 10 * time.Second
)

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

func (sc *SchemaChanger) getChunkSize(chunkSize int64) int64 {
	if sc.testingKnobs.BackfillChunkSize > 0 {
		return sc.testingKnobs.BackfillChunkSize
	}
	return chunkSize
}

// runBackfill runs the backfill for the schema changer.
func (sc *SchemaChanger) runBackfill(
	ctx context.Context, lease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	if sc.testingKnobs.RunBeforeBackfill != nil {
		if err := sc.testingKnobs.RunBeforeBackfill(); err != nil {
			return err
		}
	}
	if err := sc.ExtendLease(lease); err != nil {
		return err
	}

	// Mutations are applied in a FIFO order. Only apply the first set of
	// mutations. Collect the elements that are part of the mutation.
	var droppedColumnDescs []sqlbase.ColumnDescriptor
	var droppedIndexDescs []sqlbase.IndexDescriptor
	var addedColumnDescs []sqlbase.ColumnDescriptor
	var addedIndexDescs []sqlbase.IndexDescriptor
	// Indexes within the Mutations slice for checkpointing.
	mutationSentinel := -1
	var columnMutationIdx, addedIndexMutationIdx, droppedIndexMutationIdx int

	var tableDesc *sqlbase.TableDescriptor
	if err := sc.db.Txn(ctx, func(txn *client.Txn) error {
		var err error
		tableDesc, err = sqlbase.GetTableDescFromID(txn, sc.tableID)
		return err
	}); err != nil {
		return err
	}
	// Short circuit the backfill if the table has been deleted.
	if tableDesc.Dropped() {
		return nil
	}
	version := tableDesc.Version

	log.VEventf(ctx, 0, "Running backfill for %q, v=%d, m=%d",
		tableDesc.Name, tableDesc.Version, sc.mutationID)

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

	// First drop indexes, then add/drop columns, and only then add indexes.

	// Drop indexes.
	if err := sc.truncateIndexes(
		ctx, lease, version, droppedIndexDescs, droppedIndexMutationIdx,
	); err != nil {
		return err
	}

	// Add and drop columns.
	if err := sc.truncateAndBackfillColumns(
		ctx, lease, version, addedColumnDescs, droppedColumnDescs, columnMutationIdx,
	); err != nil {
		return err
	}

	// Add new indexes.
	if len(addedIndexDescs) > 0 {
		if err := sc.backfillIndexes(ctx, lease, version); err != nil {
			return err
		}
	}

	return nil
}

// getTableSpan returns a span stored at a checkpoint idx, or in the absence
// of a checkpoint, the span over all keys within a table.
func (sc *SchemaChanger) getTableSpan(ctx context.Context, mutationIdx int) (roachpb.Span, error) {
	var tableDesc *sqlbase.TableDescriptor
	if err := sc.db.Txn(ctx, func(txn *client.Txn) error {
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
	if len(tableDesc.Mutations[mutationIdx].ResumeSpans) > 0 {
		return tableDesc.Mutations[mutationIdx].ResumeSpans[0], nil
	}
	prefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID))
	return roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}, nil
}

func (sc *SchemaChanger) maybeWriteResumeSpan(
	txn *client.Txn,
	version sqlbase.DescriptorVersion,
	resume roachpb.Span,
	mutationIdx int,
	lastCheckpoint *time.Time,
) error {
	checkpointInterval := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		checkpointInterval = sc.testingKnobs.WriteCheckpointInterval
	}
	if timeutil.Since(*lastCheckpoint) < checkpointInterval {
		return nil
	}
	tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
	if err != nil {
		return err
	}
	if tableDesc.Version != version {
		return errors.Errorf("table version mismatch: %d, expected: %d", tableDesc.Version, version)
	}
	if len(tableDesc.Mutations[mutationIdx].ResumeSpans) > 0 {
		tableDesc.Mutations[mutationIdx].ResumeSpans[0] = resume
	} else {
		tableDesc.Mutations[mutationIdx].ResumeSpans = append(tableDesc.Mutations[mutationIdx].ResumeSpans, resume)
	}
	txn.SetSystemConfigTrigger()
	if err := txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.GetID()),
		sqlbase.WrapDescriptor(tableDesc)); err != nil {
		return err
	}
	*lastCheckpoint = timeutil.Now()
	return nil
}

func (sc *SchemaChanger) makePlanner(txn *client.Txn) *planner {
	return &planner{
		txn:      txn,
		leaseMgr: sc.leaseMgr,
		session: &Session{
			context: txn.Context,
		},
	}
}

func (sc *SchemaChanger) getTableLease(
	ctx context.Context, p *planner, version sqlbase.DescriptorVersion,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := p.getTableLeaseByID(ctx, sc.tableID)
	if err != nil {
		return nil, err
	}
	if version != tableDesc.Version {
		return nil, errors.Errorf("table version mismatch: %d, expected=%d", tableDesc.Version, version)
	}
	return tableDesc, nil
}

func (sc *SchemaChanger) truncateAndBackfillColumns(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
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
		sp, err := sc.getTableSpan(ctx, mutationIdx)
		if err != nil {
			return err
		}

		// Run through the entire table key space adding and deleting columns.
		chunkSize := sc.getChunkSize(columnTruncateAndBackfillChunkSize)
		// Evaluate default values.
		updateCols := append(added, dropped...)
		updateValues := make(parser.Datums, len(updateCols))
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
			if !col.Nullable && updateValues[j].Compare(&sc.evalCtx, parser.DNull) == 0 {
				nonNullViolationColumnName = col.Name
			}
		}
		for j := range dropped {
			updateValues[j+len(added)] = parser.DNull
		}
		lastCheckpoint := timeutil.Now()
		for row, done := int64(0), false; !done; row += chunkSize {
			// First extend the schema change lease.
			if err := sc.ExtendLease(lease); err != nil {
				return err
			}
			if log.V(2) {
				log.Infof(ctx, "column schema change (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, row, sp)
			}

			// Add and delete columns for a chunk of the key space.
			sp.Key, done, err = sc.truncateAndBackfillColumnsChunk(
				ctx, version, added, dropped, defaultExprs, sp,
				updateValues, nonNullViolationColumnName, chunkSize, mutationIdx, &lastCheckpoint)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// truncateAndBackfillColumnsChunk returns the next-key, done and an error.
// next-key and done are invalid if error != nil. next-key is invalid if done
// is true.
func (sc *SchemaChanger) truncateAndBackfillColumnsChunk(
	ctx context.Context,
	version sqlbase.DescriptorVersion,
	added []sqlbase.ColumnDescriptor,
	dropped []sqlbase.ColumnDescriptor,
	defaultExprs []parser.TypedExpr,
	sp roachpb.Span,
	updateValues parser.Datums,
	nonNullViolationColumnName string,
	chunkSize int64,
	mutationIdx int,
	lastCheckpoint *time.Time,
) (roachpb.Key, bool, error) {
	done := false
	var nextKey roachpb.Key
	err := sc.db.Txn(ctx, func(txn *client.Txn) error {
		if sc.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := sc.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if sc.testingKnobs.RunAfterBackfillChunk != nil {
			defer sc.testingKnobs.RunAfterBackfillChunk()
		}

		// TODO(vivek): See comment in backfillIndexesChunk.
		txn.SetSystemConfigTrigger()

		p := sc.makePlanner(txn)
		defer p.releaseLeases(ctx)
		tableDesc, err := sc.getTableLease(ctx, p, version)
		if err != nil {
			return err
		}

		updateCols := append(added, dropped...)
		fkTables := tablesNeededForFKs(*tableDesc, CheckUpdates)
		for k := range fkTables {
			table, err := p.getTableLeaseByID(ctx, k)
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
			tableDesc, colIDtoRowIndex, &tableDesc.PrimaryIndex,
			false /* reverse */, false, /* isSecondaryIndex */
			tableDesc.Columns, valNeededForCol, false, /* returnRangeInfo */
		); err != nil {
			return err
		}
		if err := rf.StartScan(
			txn, roachpb.Spans{sp}, true /* limit batches */, chunkSize,
		); err != nil {
			return err
		}

		oldValues := make(parser.Datums, len(ru.fetchCols))
		writeBatch := txn.NewBatch()
		rowLength := 0
		var lastRowSeen parser.Datums
		i := int64(0)
		for ; i < chunkSize; i++ {
			row, err := rf.NextRowDecoded()
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
			return distsqlrun.ConvertBackfillError(tableDesc, writeBatch)
		}
		if done = i < chunkSize; done {
			return nil
		}
		curIndexKey, _, err := sqlbase.EncodeIndexKey(
			tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, lastRowSeen,
			sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID))
		if err != nil {
			return err
		}
		resume := roachpb.Span{Key: roachpb.Key(curIndexKey).PrefixEnd(), EndKey: sp.EndKey}
		if err := sc.maybeWriteResumeSpan(txn, version, resume, mutationIdx, lastCheckpoint); err != nil {
			return err
		}
		nextKey = resume.Key
		return nil
	})
	return nextKey, done, err
}

func (sc *SchemaChanger) truncateIndexes(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
	dropped []sqlbase.IndexDescriptor,
	mutationIdx int,
) error {
	chunkSize := sc.getChunkSize(indexTruncateChunkSize)
	if sc.testingKnobs.BackfillChunkSize > 0 {
		chunkSize = sc.testingKnobs.BackfillChunkSize
	}
	for _, desc := range dropped {
		var resume roachpb.Span
		lastCheckpoint := timeutil.Now()
		for row, done := int64(0), false; !done; row += chunkSize {
			// First extend the schema change lease.
			if err := sc.ExtendLease(lease); err != nil {
				return err
			}

			resumeAt := resume
			if log.V(2) {
				log.Infof(ctx, "drop index (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, row, resume)
			}
			if err := sc.db.Txn(ctx, func(txn *client.Txn) error {
				if sc.testingKnobs.RunBeforeBackfillChunk != nil {
					if err := sc.testingKnobs.RunBeforeBackfillChunk(resume); err != nil {
						return err
					}
				}
				if sc.testingKnobs.RunAfterBackfillChunk != nil {
					defer sc.testingKnobs.RunAfterBackfillChunk()
				}

				// TODO(vivek): See comment in backfillIndexesChunk.
				txn.SetSystemConfigTrigger()

				p := sc.makePlanner(txn)
				defer p.releaseLeases(ctx)
				tableDesc, err := sc.getTableLease(ctx, p, version)
				if err != nil {
					return err
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
				if err := sc.maybeWriteResumeSpan(txn, version, resume, mutationIdx, &lastCheckpoint); err != nil {
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

type backfillType int

const (
	columnBackfill backfillType = iota
	indexBackfill
)

// backfillIndexesSpans returns the index spans that still have to be backfilled
// for the indexes being added as part of the first mutation enqueued on the
// table descriptor.
//
// Returns nil if the backfill for the indexes is complete (mutation no longer
// exists or there are no "ResumeSpans").
func (sc *SchemaChanger) backfillIndexesSpans(ctx context.Context) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	err := sc.db.Txn(ctx, func(txn *client.Txn) error {
		spans = nil
		tableDesc, err := sqlbase.GetTableDescFromID(txn, sc.tableID)
		if err != nil {
			return err
		}
		if len(tableDesc.Mutations) > 0 {
			mutationID := tableDesc.Mutations[0].MutationID
			for _, m := range tableDesc.Mutations {
				if m.MutationID != mutationID {
					break
				}
				if m.GetIndex() != nil && m.Direction == sqlbase.DescriptorMutation_ADD {
					spans = m.ResumeSpans
					break
				}
			}
		}
		return nil
	})
	return spans, err
}

func (sc *SchemaChanger) backfillIndexes(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
) error {
	duration := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		duration = sc.testingKnobs.WriteCheckpointInterval
	}
	chunkSize := sc.getChunkSize(indexBackfillChunkSize)
	spans, err := sc.backfillIndexesSpans(ctx)
	if err != nil {
		return err
	}

	for len(spans) > 0 {
		if err := sc.ExtendLease(lease); err != nil {
			return err
		}
		log.VEventf(ctx, 2, "index backfill: process %+v spans", spans)
		if err := sc.db.Txn(ctx, func(txn *client.Txn) error {
			p := sc.makePlanner(txn)
			// Use a leased table descriptor for the backfill.
			defer p.releaseLeases(ctx)
			tableDesc, err := sc.getTableLease(ctx, p, version)
			if err != nil {
				return err
			}
			recv := distSQLReceiver{}
			planCtx := sc.distSQLPlanner.NewPlanningCtx(ctx, txn)
			plan, err := sc.distSQLPlanner.CreateBackfiller(
				&planCtx, indexBackfill, *tableDesc, duration, chunkSize, spans,
			)
			if err != nil {
				return err
			}
			if err := sc.distSQLPlanner.Run(&planCtx, txn, &plan, &recv); err != nil {
				return err
			}
			return recv.err
		}); err != nil {
			return err
		}
		spans, err = sc.backfillIndexesSpans(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
