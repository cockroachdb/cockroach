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
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
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
func (p *planner) Delete(n *parser.Delete, desiredTypes []parser.Datum, autoCommit bool) (planNode, error) {
	en, err := p.makeEditNode(n.Table, autoCommit, privilege.DELETE)
	if err != nil {
		return nil, err
	}

	var requestedCols []sqlbase.ColumnDescriptor
	if len(n.Returning) > 0 {
		// TODO(dan): This could be made tighter, just the rows needed for RETURNING
		// exprs.
		requestedCols = en.tableDesc.Columns
	}

	fkTables := TablesNeededForFKs(*en.tableDesc, CheckDeletes)
	if err := p.fillFKTableMap(fkTables); err != nil {
		return nil, err
	}
	rd, err := makeRowDeleter(p.txn, en.tableDesc, fkTables, requestedCols, checkFKs)
	if err != nil {
		return nil, err
	}
	tw := tableDeleter{rd: rd, autoCommit: autoCommit}

	// TODO(knz): Until we split the creation of the node from Start()
	// for the SelectClause too, we cannot cache this. This is because
	// this node's initSelect() method both does type checking and also
	// performs index selection. We cannot perform index selection
	// properly until the placeholder values are known.
	rows, err := p.SelectClause(&parser.SelectClause{
		Exprs: sqlbase.ColumnsSelectors(rd.fetchCols),
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

	if err := dn.run.initEditNode(&dn.editNodeBase, rows, n.Returning, desiredTypes); err != nil {
		return nil, err
	}

	return dn, nil
}

func (d *deleteNode) expandPlan() error {
	return d.run.expandEditNodePlan(&d.editNodeBase, &d.tw)
}

func (d *deleteNode) Start() error {
	if err := d.run.startEditNode(); err != nil {
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
		sel := d.run.rows.(*selectTopNode).source.(*selectNode)
		if scan, ok := sel.source.plan.(*scanNode); ok && canDeleteWithoutScan(d.n, scan, &d.tw) {
			d.run.fastPath = true
			err := d.fastDelete(scan)
			return err
		}
	}

	if err := d.rh.startPlans(); err != nil {
		return err
	}

	return d.run.tw.init(d.p.txn)
}

func (d *deleteNode) FastPathResults() (int, bool) {
	if d.run.fastPath {
		return d.rh.rowCount, true
	}
	return 0, false
}

func (d *deleteNode) Next() (bool, error) {
	ctx := context.TODO()
	next, err := d.run.rows.Next()
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
func canDeleteWithoutScan(n *parser.Delete, scan *scanNode, td *tableDeleter) bool {
	ctx := context.TODO()
	if !td.fastPathAvailable(ctx) {
		return false
	}
	if n.Returning != nil {
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
func (d *deleteNode) fastDelete(scan *scanNode) error {
	if err := scan.initScan(); err != nil {
		return err
	}

	if err := d.tw.init(d.p.txn); err != nil {
		return err
	}
	rowCount, err := d.tw.fastDelete(context.TODO(), scan)
	if err != nil {
		return err
	}
	d.rh.rowCount += rowCount
	return nil
}

func (d *deleteNode) Columns() []ResultColumn {
	return d.rh.columns
}

func (d *deleteNode) Values() parser.DTuple {
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

func (d *deleteNode) Ordering() orderingInfo {
	return d.run.rows.Ordering()
}

func (d *deleteNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	if v {
		fmt.Fprintf(&buf, "from %s returning (", d.tableDesc.Name)
		for i, col := range d.rh.columns {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			fmt.Fprintf(&buf, "%s", col.Name)
		}
		fmt.Fprintf(&buf, ")")
	}

	subplans := []planNode{d.run.rows}
	for _, e := range d.rh.exprs {
		subplans = d.p.collectSubqueryPlans(e, subplans)
	}

	return "delete", buf.String(), subplans
}

func (d *deleteNode) ExplainTypes(regTypes func(string, string)) {
	cols := d.rh.columns
	for i, rexpr := range d.rh.exprs {
		regTypes(fmt.Sprintf("returning %s", cols[i].Name), parser.AsStringWithFlags(rexpr, parser.FmtShowTypes))
	}
}

func (d *deleteNode) SetLimitHint(numRows int64, soft bool) {}
