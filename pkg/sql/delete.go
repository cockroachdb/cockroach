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

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var deleteNodePool = sync.Pool{
	New: func() interface{} {
		return &deleteNode{}
	},
}

type deleteNode struct {
	editNodeBase
	n *tree.Delete

	tw tableDeleter

	run        deleteRun
	autoCommit autoCommitOpt
}

// deleteNode implements the autoCommitNode interface.
var _ autoCommitNode = &deleteNode{}

// Delete removes rows from a table.
// Privileges: DELETE and SELECT on table. We currently always use a SELECT statement.
//   Notes: postgres requires DELETE. Also requires SELECT for "USING" and "WHERE" with tables.
//          mysql requires DELETE. Also requires SELECT if a table is used in the "WHERE" clause.
func (p *planner) Delete(
	ctx context.Context, n *tree.Delete, desiredTypes []types.T,
) (planNode, error) {
	resetter, err := p.initWith(ctx, n.With)
	if err != nil {
		return nil, err
	}
	if resetter != nil {
		defer resetter(p)
	}

	if n.Where == nil && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("DELETE without WHERE clause")
	}

	tn, alias, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	en, err := p.makeEditNode(ctx, tn, privilege.DELETE)
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if _, retExprs := n.Returning.(*tree.ReturningExprs); retExprs {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = en.tableDesc.Columns
	}

	fkTables, err := sqlbase.TablesNeededForFKs(
		ctx,
		*en.tableDesc,
		sqlbase.CheckDeletes,
		p.lookupFKTable,
		p.CheckPrivilege,
		p.analyzeExpr,
	)
	if err != nil {
		return nil, err
	}
	rd, err := sqlbase.MakeRowDeleter(
		p.txn, en.tableDesc, fkTables, requestedCols, sqlbase.CheckFKs, p.EvalContext(), &p.alloc,
	)
	if err != nil {
		return nil, err
	}
	tw := tableDeleter{rd: rd, alloc: &p.alloc}

	// TODO(knz): Until we split the creation of the node from Start()
	// for the SelectClause too, we cannot cache this. This is because
	// this node's initSelect() method both does type checking and also
	// performs index selection. We cannot perform index selection
	// properly until the placeholder values are known.
	rows, err := p.SelectClause(ctx, &tree.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(rd.FetchCols, true /* forUpdateOrDelete */),
		From:  &tree.From{Tables: []tree.TableExpr{n.Table}},
		Where: n.Where,
	}, n.OrderBy, n.Limit, nil, nil, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	dn := deleteNodePool.Get().(*deleteNode)
	*dn = deleteNode{
		n:            n,
		editNodeBase: en,
		tw:           tw,
	}

	if err := dn.run.initEditNode(
		ctx, &dn.editNodeBase, rows, &dn.tw, alias, n.Returning, desiredTypes); err != nil {
		return nil, err
	}

	return dn, nil
}

// deleteRun contains the run-time state of deleteNode during local execution.
type deleteRun struct {
	// The following fields are populated during Start().
	editNodeRun

	fastPath bool
}

func (d *deleteNode) startExec(params runParams) error {
	if err := d.run.startEditNode(params, &d.editNodeBase); err != nil {
		return err
	}
	// Check if we can avoid doing a round-trip to read the values and just
	// "fast-path" skip to deleting the key ranges without reading them first.
	// TODO(dt): We could probably be smarter when presented with an index-join,
	// but this goes away anyway once we push-down more of SQL.
	maybeScan := d.run.rows
	if sel, ok := maybeScan.(*renderNode); ok {
		maybeScan = sel.source.plan
	}
	if scan, ok := maybeScan.(*scanNode); ok && canDeleteWithoutScan(params.ctx, d.n, scan, &d.tw) {
		d.run.fastPath = true
		return d.fastDelete(params, scan)
	}

	return d.run.tw.init(d.p.txn, params.EvalContext())
}

func (d *deleteNode) Next(params runParams) (bool, error) {
	if d.run.fastPath {
		return false, nil
	}

	traceKV := d.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	next, err := d.run.rows.Next(params)
	if !next {
		if err == nil {
			if err := params.p.cancelChecker.Check(); err != nil {
				return false, err
			}
			// We're done. Finish the batch.
			_, err = d.tw.finalize(params.ctx, d.autoCommit, traceKV)
		}
		return false, err
	}

	rowVals := d.run.rows.Values()

	_, err = d.tw.row(params.ctx, rowVals, traceKV)
	if err != nil {
		return false, err
	}

	resultRow, err := d.rh.cookResultRow(rowVals)
	if err != nil {
		return false, err
	}
	d.run.resultRow = resultRow

	return true, nil
}

func (d *deleteNode) Values() tree.Datums {
	return d.run.resultRow
}

// requireSpool implements the planNodeRequireSpool interface.
func (d *deleteNode) requireSpool() {}

func (d *deleteNode) Close(ctx context.Context) {
	d.run.rows.Close(ctx)
	d.tw.close(ctx)
	*d = deleteNode{}
	deleteNodePool.Put(d)
}

func (d *deleteNode) FastPathResults() (int, bool) {
	if d.run.fastPath {
		return d.rh.rowCount, true
	}
	return 0, false
}

// Determine if the deletion of `rows` can be done without actually scanning them,
// i.e. if we do not need to know their values for filtering expressions or a
// RETURNING clause or for updating secondary indexes.
func canDeleteWithoutScan(
	ctx context.Context, n *tree.Delete, scan *scanNode, td *tableDeleter,
) bool {
	if !td.fastPathAvailable(ctx) {
		return false
	}
	if _, ok := n.Returning.(*tree.ReturningExprs); ok {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: values required for RETURNING")
		}
		return false
	}
	if scan.filter != nil {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: values required for filter (%s)", scan.filter)
		}
		return false
	}
	return true
}

// `fastDelete` skips the scan of rows and just deletes the ranges that
// `rows` would scan. Should only be used if `canDeleteWithoutScan` indicates
// that it is safe to do so.
func (d *deleteNode) fastDelete(params runParams, scan *scanNode) error {
	if err := scan.initScan(params); err != nil {
		return err
	}

	if err := d.tw.init(params.p.txn, params.EvalContext()); err != nil {
		return err
	}
	if err := params.p.cancelChecker.Check(); err != nil {
		return err
	}
	rowCount, err := d.tw.fastDelete(
		params.ctx, scan, d.autoCommit, params.extendedEvalCtx.Tracing.KVTracingEnabled())
	if err != nil {
		return err
	}
	d.rh.rowCount += rowCount
	return nil
}

// enableAutoCommit is part of the autoCommitNode interface.
func (d *deleteNode) enableAutoCommit() {
	d.autoCommit = autoCommitEnabled
}
