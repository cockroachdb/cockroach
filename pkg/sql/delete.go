// Copyright 2015 The Cockroach Authors.
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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var deleteNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteNode{}
	},
}

type deleteNode struct {
	source planNode

	// columns is set if this DELETE is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run deleteRun
}

// deleteNode implements the autoCommitNode interface.
var _ autoCommitNode = &deleteNode{}

// Delete removes rows from a table.
// Privileges: DELETE and SELECT on table. We currently always use a SELECT statement.
//   Notes: postgres requires DELETE. Also requires SELECT for "USING" and "WHERE" with tables.
//          mysql requires DELETE. Also requires SELECT if a table is used in the "WHERE" clause.
func (p *planner) Delete(
	ctx context.Context, n *tree.Delete, desiredTypes []*types.T,
) (result planNode, resultErr error) {
	// UX friendliness safeguard.
	if n.Where == nil && p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf("DELETE without WHERE clause")
	}

	// CTE analysis.
	resetter, err := p.initWith(ctx, n.With)
	if err != nil {
		return nil, err
	}
	if resetter != nil {
		defer func() {
			if cteErr := resetter(p); cteErr != nil && resultErr == nil {
				// If no error was found in the inner planning but a CTE error
				// is occurring during the final checks on the way back from
				// the recursion, use that error as final error for this
				// stage.
				resultErr = cteErr
				result = nil
			}
		}()
	}

	tracing.AnnotateTrace()

	// DELETE FROM xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	// Find which table we're working on, check the permissions.
	desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, desc, privilege.DELETE); err != nil {
		return nil, err
	}

	// Determine what are the foreign key tables that are involved in the deletion.
	fkTables, err := row.MakeFkMetadata(
		ctx,
		desc,
		row.CheckDeletes,
		p.LookupTableByID,
		p.CheckPrivilege,
		p.analyzeExpr,
		nil, /* checkHelper */
	)
	if err != nil {
		return nil, err
	}

	// rowsNeeded will help determine whether we can use the fast path
	// in startExec.
	rowsNeeded := resultsNeeded(n.Returning)

	// Also, rowsNeeded determines which rows of the source we need
	// in the table deleter.
	var requestedCols []sqlbase.ColumnDescriptor
	if rowsNeeded {
		// Note: in contrast to INSERT and UPDATE which also require the
		// data if there are CHECK expressions, DELETE does not care about
		// constraint checking (because the rows are being deleted after
		// all).

		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = desc.Columns
	}

	// Since all columns are being returned, use the 1:1 mapping. See todo above.
	rowIdxToRetIdx := mutationRowIdxToReturnIdx(requestedCols, requestedCols)

	// Create the table deleter, which does the bulk of the work.
	rd, err := row.MakeDeleter(
		p.txn, desc, fkTables, requestedCols, row.CheckFKs, p.EvalContext(), &p.alloc,
	)
	if err != nil {
		return nil, err
	}

	tracing.AnnotateTrace()

	// Determine the source for the deletion: the rows that are read,
	// filtered, limited, ordered, etc, prior to the deletion. One would
	// think there is only so much one wants to do with rows prior to a
	// deletion, but ORDER BY / LIMIT really determines which rows are
	// being deleted. Also RETURNING will expose this.
	rows, err := p.SelectClause(ctx, &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(rd.FetchCols, true /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{n.Table}},
		Where: n.Where,
	}, n.OrderBy, n.Limit, nil /*with*/, nil /*desiredTypes*/, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	var columns sqlbase.ResultColumns
	if rowsNeeded {
		columns = planColumns(rows)
		// If rowsNeeded is set, we have requested from the source above
		// all the columns from the descriptor. However, to ensure that
		// modified rows include all columns, the construction of the
		// source has used publicAndNonPublicColumns so the source may
		// contain additional columns for every newly dropped column not
		// visible.
		// We do not want these to be available for RETURNING below.
		//
		// In the case where rowsNeeded is true, the requested columns are
		// requestedCols. So we can truncate to the length of that to
		// only see public columns.
		columns = columns[:len(requestedCols)]
	}

	// Now make a delete node. We use a pool.
	dn := deleteNodePool.Get().(*deleteNode)
	*dn = deleteNode{
		source:  rows,
		columns: columns,
		run: deleteRun{
			td:                  tableDeleter{rd: rd, alloc: &p.alloc},
			rowsNeeded:          rowsNeeded,
			fastPathInterleaved: canDeleteFastInterleaved(desc, fkTables),
			rowIdxToRetIdx:      rowIdxToRetIdx,
		},
	}

	// Finally, handle RETURNING, if any.
	r, err := p.Returning(ctx, dn, n.Returning, desiredTypes, alias)
	if err != nil {
		// We close explicitly here to release the node to the pool.
		dn.Close(ctx)
	}
	return r, err
}

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	td         tableDeleter
	rowsNeeded bool

	// fastPathInterleaved indicates whether the delete operation can run
	// the interleaved fast path (all interleaved tables have no indexes and ON DELETE CASCADE).
	fastPathInterleaved bool

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call
	// to BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *rowcontainer.RowContainer

	// traceKV caches the current KV tracing flag.
	traceKV bool

	// rowIdxToRetIdx is the mapping from the columns returned by the deleter
	// to the columns in the resultRowBuffer. A value of -1 is used to indicate
	// that the column at that index is not part of the resultRowBuffer
	// of the mutation. Otherwise, the value at the i-th index refers to the
	// index of the resultRowBuffer where the i-th column is to be returned.
	rowIdxToRetIdx []int
}

// maxDeleteBatchSize is the max number of entries in the KV batch for
// the delete operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxDeleteBatchSize = 10000

func (d *deleteNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(d.run.td.tableDesc().GetID()); err != nil {
		return err
	}

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	d.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if d.run.rowsNeeded {
		d.run.rows = rowcontainer.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(d.columns), 0)
	}
	return d.run.td.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (d *deleteNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (d *deleteNode) BatchedNext(params runParams) (bool, error) {
	if d.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	d.run.rowCount = 0
	if d.run.rows != nil {
		d.run.rows.Clear(params.ctx)
	}
	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := d.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the deletion of the current source row,
		// potentially accumulating the result row for later.
		if err := d.processSourceRow(params, d.source.Values()); err != nil {
			return false, err
		}

		d.run.rowCount++

		// Are we done yet with the current batch?
		if d.run.td.curBatchSize() >= maxDeleteBatchSize {
			break
		}
	}

	if d.run.rowCount > 0 {
		if err := d.run.td.atBatchEnd(params.ctx, d.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := d.run.td.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := d.run.td.finalize(params.ctx, d.run.traceKV); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		d.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		d.run.td.tableDesc().ID,
		d.run.rowCount,
	)

	return d.run.rowCount > 0, nil
}

// processSourceRow processes one row from the source for deletion and, if
// result rows are needed, saves it in the result row container
func (d *deleteNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Queue the deletion in the KV batch.
	if err := d.run.td.row(params.ctx, sourceVals, d.run.traceKV); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if d.run.rows != nil {
		// The new values can include all columns, the construction of the
		// values has used publicAndNonPublicColumns so the values may
		// contain additional columns for every newly dropped column not
		// visible. We do not want them to be available for RETURNING.
		//
		// d.run.rows.NumCols() is guaranteed to only contain the requested
		// public columns.
		resultValues := make(tree.Datums, d.run.rows.NumCols())
		for i, retIdx := range d.run.rowIdxToRetIdx {
			if retIdx >= 0 {
				resultValues[retIdx] = sourceVals[i]
			}
		}

		if _, err := d.run.rows.AddRow(params.ctx, resultValues); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedCount() int { return d.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteNode) BatchedValues(rowIdx int) tree.Datums { return d.run.rows.At(rowIdx) }

func (d *deleteNode) Close(ctx context.Context) {
	d.source.Close(ctx)
	if d.run.rows != nil {
		d.run.rows.Close(ctx)
		d.run.rows = nil
	}
	d.run.td.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func canDeleteFastInterleaved(table *ImmutableTableDescriptor, fkTables row.FkTableMetadata) bool {
	// If there are no interleaved tables then don't take the fast path.
	// This avoids superfluous use of DelRange in cases where there isn't as much of a performance boost.
	hasInterleaved := false
	for _, idx := range table.AllNonDropIndexes() {
		if len(idx.InterleavedBy) > 0 {
			hasInterleaved = true
			break
		}
	}
	if !hasInterleaved {
		return false
	}

	// if the base table is interleaved in another table, fail
	for _, idx := range fkTables[table.ID].Desc.AllNonDropIndexes() {
		if len(idx.Interleave.Ancestors) > 0 {
			return false
		}
	}

	interleavedQueue := []sqlbase.ID{table.ID}
	// interleavedIdxs will contain all of the table and index IDs of the indexes
	// interleaved in any of the interleaved hierarchy of the input table.
	interleavedIdxs := make(map[sqlbase.ID]map[sqlbase.IndexID]struct{})
	for len(interleavedQueue) > 0 {
		tableID := interleavedQueue[0]
		interleavedQueue = interleavedQueue[1:]
		if _, ok := fkTables[tableID]; !ok {
			return false
		}
		tableDesc := fkTables[tableID].Desc
		if tableDesc == nil {
			return false
		}
		for _, idx := range tableDesc.AllNonDropIndexes() {
			// Don't allow any secondary indexes
			// TODO(emmanuel): identify the cases where secondary indexes can still work with the fast path and allow them
			if idx.ID != tableDesc.PrimaryIndex.ID {
				return false
			}

			for _, ref := range idx.InterleavedBy {
				if _, ok := interleavedIdxs[ref.Table]; !ok {
					interleavedIdxs[ref.Table] = make(map[sqlbase.IndexID]struct{})
				}
				interleavedIdxs[ref.Table][ref.Index] = struct{}{}

			}

			for _, ref := range idx.InterleavedBy {
				interleavedQueue = append(interleavedQueue, ref.Table)
			}

		}

		// The index can't be referenced by anything that's not the interleaved relationship.
		for _, fk := range tableDesc.InboundFKs {
			if _, ok := interleavedIdxs[fk.OriginTableID]; !ok {
				return false
			}
			for _, outboundFK := range fkTables[fk.OriginTableID].Desc.OutboundFKs {
				if outboundFK.ReferencedTableID != tableDesc.ID {
					continue
				}
				if sqlbase.ColumnIDs(outboundFK.ReferencedColumnIDs).EqualSets(fk.ReferencedColumnIDs) {
					// All of these references MUST be ON DELETE CASCADE.
					if outboundFK.OnDelete != sqlbase.ForeignKeyReference_CASCADE {
						return false
					}
					break
				}
			}
		}
	}
	return true
}

// enableAutoCommit is part of the autoCommitNode interface.
func (d *deleteNode) enableAutoCommit() {
	d.run.td.enableAutoCommit()
}
