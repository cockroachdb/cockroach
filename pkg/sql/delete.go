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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type deleteNode struct {
	editNodeBase
	n *parser.Delete

	tw tableDeleter

	run struct {
		// The following fields are populated during Start().
		editNodeRun

		fastPath bool
	}
}

// Delete removes rows from a table.
// Privileges: DELETE and SELECT on table. We currently always use a SELECT statement.
//   Notes: postgres requires DELETE. Also requires SELECT for "USING" and "WHERE" with tables.
//          mysql requires DELETE. Also requires SELECT if a table is used in the "WHERE" clause.
func (p *planner) Delete(
	ctx context.Context, n *parser.Delete, desiredTypes []parser.Type,
) (planNode, error) {
	tn, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	en, err := p.makeEditNode(ctx, tn, privilege.DELETE)
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if _, retExprs := n.Returning.(*parser.ReturningExprs); retExprs {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = en.tableDesc.Columns
	}

	fkTables := sqlbase.TablesNeededForFKs(*en.tableDesc, sqlbase.CheckDeletes)
	if err := p.fillFKTableMap(ctx, fkTables); err != nil {
		return nil, err
	}
	rd, err := sqlbase.MakeRowDeleter(p.txn, en.tableDesc, fkTables, requestedCols, sqlbase.CheckFKs)
	if err != nil {
		return nil, err
	}
	tw := tableDeleter{rd: rd, autoCommit: p.autoCommit}

	// TODO(knz): Until we split the creation of the node from Start()
	// for the SelectClause too, we cannot cache this. This is because
	// this node's initSelect() method both does type checking and also
	// performs index selection. We cannot perform index selection
	// properly until the placeholder values are known.
	rows, err := p.SelectClause(ctx, &parser.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(rd.FetchCols),
		From:  &parser.From{Tables: []parser.TableExpr{n.Table}},
		Where: n.Where,
	}, nil, nil, nil, publicAndNonPublicColumns)
	if err != nil {
		return nil, err
	}

	dn := &deleteNode{
		n:            n,
		editNodeBase: en,
		tw:           tw,
	}

	if err := dn.run.initEditNode(
		ctx, &dn.editNodeBase, rows, &dn.tw, n.Returning, desiredTypes); err != nil {
		return nil, err
	}

	return dn, nil
}

func (d *deleteNode) Start(ctx context.Context) error {
	if err := d.run.startEditNode(ctx, &d.editNodeBase); err != nil {
		return err
	}

	if d.run.explain != explainDebug {
		// Check if we can avoid doing a round-trip to read the values and just
		// "fast-path" skip to deleting the key ranges without reading them first.
		// TODO(dt): We could probably be smarter when presented with an index-join,
		// but this goes away anyway once we push-down more of SQL.
		//
		// (When explain == explainDebug, we use the slow path so that
		// each debugVal gets a chance to be reported via Next().)
		maybeScan := d.run.rows
		if sel, ok := maybeScan.(*renderNode); ok {
			maybeScan = sel.source.plan
		}
		if scan, ok := maybeScan.(*scanNode); ok && canDeleteWithoutScan(ctx, d.n, scan, &d.tw) {
			d.run.fastPath = true
			err := d.fastDelete(ctx, scan)
			return err
		}
	}

	return d.run.tw.init(d.p.txn)
}

func (d *deleteNode) Close(ctx context.Context) {
	d.run.rows.Close(ctx)
}

func (d *deleteNode) FastPathResults() (int, bool) {
	if d.run.fastPath {
		return d.rh.rowCount, true
	}
	return 0, false
}

func (d *deleteNode) Next(ctx context.Context) (bool, error) {
	next, err := d.run.rows.Next(ctx)
	if !next {
		if err == nil {
			// We're done. Finish the batch.
			err = d.tw.finalize(ctx)
		}
		return false, err
	}

	if d.run.explain == explainDebug {
		return true, nil
	}

	rowVals := d.run.rows.Values()

	_, err = d.tw.row(ctx, rowVals)
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

// Determine if the deletion of `rows` can be done without actually scanning them,
// i.e. if we do not need to know their values for filtering expressions or a
// RETURNING clause or for updating secondary indexes.
func canDeleteWithoutScan(
	ctx context.Context, n *parser.Delete, scan *scanNode, td *tableDeleter,
) bool {
	if !td.fastPathAvailable(ctx) {
		return false
	}
	if _, ok := n.Returning.(*parser.ReturningExprs); ok {
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
func (d *deleteNode) fastDelete(ctx context.Context, scan *scanNode) error {
	if err := scan.initScan(ctx); err != nil {
		return err
	}

	if err := d.tw.init(d.p.txn); err != nil {
		return err
	}
	rowCount, err := d.tw.fastDelete(ctx, scan)
	if err != nil {
		return err
	}
	d.rh.rowCount += rowCount
	return nil
}

func (d *deleteNode) Columns() sqlbase.ResultColumns {
	return d.rh.columns
}

func (d *deleteNode) Values() parser.Datums {
	return d.run.resultRow
}

func (d *deleteNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	d.run.explain = mode
	d.run.rows.MarkDebug(mode)
}

func (d *deleteNode) DebugValues() debugValues {
	return d.run.rows.DebugValues()
}

func (d *deleteNode) Ordering() orderingInfo { return orderingInfo{} }

func (d *deleteNode) Spans(ctx context.Context) (reads, writes roachpb.Spans, err error) {
	return d.run.collectSpans(ctx)
}
