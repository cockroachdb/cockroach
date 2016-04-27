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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util"
)

type insertNode struct {
	// The following fields are populated during makePlan.
	rowCreatorNodeBase
	n          *parser.Insert
	qvals      qvalMap
	insertRows parser.SelectStatement
	checkExprs []parser.TypedExpr

	desiredTypes []parser.Datum // This will go away when we only type check once.

	run struct {
		// The following fields are populated during Start().
		editNodeRun

		ri             rowInserter
		rowIdxToRetIdx []int
		rowTemplate    parser.DTuple
	}
}

// Insert inserts rows into the database.
// Privileges: INSERT on table
//   Notes: postgres requires INSERT. No "on duplicate key update" option.
//          mysql requires INSERT. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
func (p *planner) Insert(
	n *parser.Insert, desiredTypes []parser.Datum, autoCommit bool,
) (planNode, *roachpb.Error) {
	en, pErr := p.makeEditNode(n.Table, n.Returning, desiredTypes, autoCommit, privilege.INSERT)
	if pErr != nil {
		return nil, pErr
	}

	var cols []ColumnDescriptor
	// Determine which columns we're inserting into.
	if n.DefaultValues() {
		cols = en.tableDesc.Columns
	} else {
		var err error
		if cols, err = p.processColumns(en.tableDesc, n.Columns); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	// Number of columns expecting an input. This doesn't include the
	// columns receiving a default value.
	numInputColumns := len(cols)

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, c := range cols {
		colIDtoRowIndex[c.ID] = i
	}

	// Add the column if it has a DEFAULT expression.
	addIfDefault := func(col ColumnDescriptor) {
		if col.DefaultExpr != nil {
			if _, ok := colIDtoRowIndex[col.ID]; !ok {
				colIDtoRowIndex[col.ID] = len(cols)
				cols = append(cols, col)
			}
		}
	}

	// Add any column that has a DEFAULT expression.
	for _, col := range en.tableDesc.Columns {
		addIfDefault(col)
	}
	// Also add any column in a mutation that is WRITE_ONLY and has
	// a DEFAULT expression.
	for _, m := range en.tableDesc.Mutations {
		if m.State != DescriptorMutation_WRITE_ONLY {
			continue
		}
		if col := m.GetColumn(); col != nil {
			addIfDefault(*col)
		}
	}

	rc, rcErr := p.makeRowCreatorNode(en, cols, colIDtoRowIndex, true)
	if rcErr != nil {
		return nil, rcErr
	}

	// Verify we have at least the columns that are part of the primary key.
	primaryKeyCols := map[ColumnID]struct{}{}
	for i, id := range en.tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDtoRowIndex[id]; !ok {
			return nil, roachpb.NewUErrorf("missing %q primary key column", en.tableDesc.PrimaryIndex.ColumnNames[i])
		}
		primaryKeyCols[id] = struct{}{}
	}

	// Replace any DEFAULT markers with the corresponding default expressions.
	insertRows, err := p.fillDefaults(rc.defaultExprs, cols, n)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Construct the check expressions. The returned slice will be nil if no
	// column in the table has a check expression.
	checkExprs, err := p.makeCheckExprs(cols)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Prepare the check expressions.
	var qvals qvalMap
	typedCheckExprs := make([]parser.TypedExpr, 0, len(checkExprs))
	if len(checkExprs) > 0 {
		qvals = make(qvalMap)
		table := tableInfo{
			columns: makeResultColumns(en.tableDesc.Columns),
		}
		for i := range checkExprs {
			expr, err := resolveQNames(checkExprs[i], &table, qvals, &p.qnameVisitor)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			typedExpr, err := parser.TypeCheck(expr, nil, parser.DummyBool)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			if typedExpr, err = p.parser.NormalizeExpr(p.evalCtx, typedExpr); err != nil {
				return nil, roachpb.NewError(err)
			}
			typedCheckExprs = append(typedCheckExprs, typedExpr)
		}
	}

	// Analyze the expressions for column information and typing.
	desiredTypesFromSelect := make([]parser.Datum, len(cols))
	for i, col := range cols {
		desiredTypesFromSelect[i] = getTypeForColumn(col)
	}
	rows, pErr := p.makePlan(insertRows, desiredTypesFromSelect, false)
	if pErr != nil {
		return nil, pErr
	}

	if expressions := len(rows.Columns()); expressions > numInputColumns {
		return nil, roachpb.NewUErrorf("INSERT has more expressions than target columns: %d/%d", expressions, numInputColumns)
	}

	// Type check the tuples, if any, to collect placeholder types.
	if values, ok := n.Rows.Select.(*parser.ValuesClause); ok {
		for _, tuple := range values.Tuples {
			for eIdx, val := range tuple.Exprs {
				if _, ok := val.(parser.DefaultVal); ok {
					continue
				}
				typedExpr, err := parser.TypeCheck(val, p.evalCtx.Args, desiredTypesFromSelect[eIdx])
				if err != nil {
					return nil, roachpb.NewError(err)
				}
				if err := checkColumnType(cols[eIdx], typedExpr.ReturnType(), p.evalCtx.Args); err != nil {
					return nil, roachpb.NewError(err)
				}
			}
		}
	}

	if pErr := en.rh.TypeCheck(); pErr != nil {
		return nil, pErr
	}

	return &insertNode{
		n:                  n,
		rowCreatorNodeBase: rc,
		checkExprs:         typedCheckExprs,
		qvals:              qvals,
		insertRows:         insertRows,
		desiredTypes:       desiredTypesFromSelect,
	}, nil
}

func (n *insertNode) Start() *roachpb.Error {
	// TODO(knz): We need to re-run makePlan here again
	// because that's when we can expand sub-queries.
	// This goes away when sub-query expansion is moved
	// to the Start() method of the insertRows object.

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	rows, pErr := n.p.makePlan(n.insertRows, n.desiredTypes, false)
	if pErr != nil {
		return pErr
	}

	if pErr := rows.Start(); pErr != nil {
		return pErr
	}

	ri, err := makeRowInserter(n.tableDesc, n.colIDtoRowIndex, n.cols)
	if err != nil {
		return roachpb.NewError(err)
	}
	n.run.ri = ri

	n.run.startEditNode(&n.editNodeBase, rows)

	// Prepare structures for building values to pass to rh.
	if n.rh.exprs != nil {
		// In some cases (e.g. `INSERT INTO t (a) ...`) rowVals does not contain all the table
		// columns. We need to pass values for all table columns to rh, in the correct order; we
		// will use rowTemplate for this. We also need a table that maps row indices to rowTemplate indices
		// to fill in the row values; any absent values will be NULLs.

		n.run.rowTemplate = make(parser.DTuple, len(n.tableDesc.Columns))
		for i := range n.run.rowTemplate {
			n.run.rowTemplate[i] = parser.DNull
		}

		colIDToRetIndex := map[ColumnID]int{}
		for i, col := range n.tableDesc.Columns {
			colIDToRetIndex[col.ID] = i
		}

		n.run.rowIdxToRetIdx = make([]int, len(n.cols))
		for i, col := range n.cols {
			n.run.rowIdxToRetIdx[i] = colIDToRetIndex[col.ID]
		}
	}

	return nil
}

func (n *insertNode) Next() bool {
	if n.run.done || n.run.pErr != nil {
		return false
	}

	if !n.run.rows.Next() {
		// We're done. Finish the batch.
		n.run.finalize(&n.editNodeBase, true)
		return false
	}

	rowVals := n.run.rows.Values()

	// The values for the row may be shorter than the number of columns being
	// inserted into. Generate default values for those columns using the
	// default expressions.
	for i := len(rowVals); i < len(n.cols); i++ {
		if n.defaultExprs == nil {
			rowVals = append(rowVals, parser.DNull)
			continue
		}
		d, err := n.defaultExprs[i].Eval(n.p.evalCtx)
		if err != nil {
			n.run.pErr = roachpb.NewError(err)
			return false
		}
		rowVals = append(rowVals, d)
	}

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range n.tableDesc.Columns {
		if !col.Nullable {
			if i, ok := n.colIDtoRowIndex[col.ID]; !ok || rowVals[i] == parser.DNull {
				n.run.pErr = roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
				return false
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := range rowVals {
		if err := checkValueWidth(n.cols[i], rowVals[i]); err != nil {
			n.run.pErr = roachpb.NewError(err)
			return false
		}
	}

	if len(n.checkExprs) > 0 {
		// Populate qvals.
		for ref, qval := range n.qvals {
			// The colIdx is 0-based, we need to change it to 1-based.
			ri, has := n.colIDtoRowIndex[ColumnID(ref.colIdx+1)]
			if !has {
				n.run.pErr = roachpb.NewUErrorf("failed to to find column %d in row", ColumnID(ref.colIdx+1))
				return false
			}
			qval.datum = rowVals[ri]
		}
		for _, expr := range n.checkExprs {
			if d, err := expr.Eval(n.p.evalCtx); err != nil {
				n.run.pErr = roachpb.NewError(err)
				return false
			} else if res, err := parser.GetBool(d); err != nil {
				n.run.pErr = roachpb.NewError(err)
				return false
			} else if !res {
				// Failed to satisfy CHECK constraint.
				n.run.pErr = roachpb.NewUErrorf("failed to satisfy CHECK constraint (%s)", expr.String())
				return false
			}
		}
	}

	n.run.pErr = n.run.ri.insertRow(n.run.b, rowVals)
	if n.run.pErr != nil {
		return false
	}

	for i, val := range rowVals {
		if n.run.rowTemplate != nil {
			n.run.rowTemplate[n.run.rowIdxToRetIdx[i]] = val
		}
	}

	resultRow, err := n.rh.cookResultRow(n.run.rowTemplate)
	if err != nil {
		n.run.pErr = roachpb.NewError(err)
		return false
	}
	n.run.resultRow = resultRow

	return true
}

func (p *planner) processColumns(tableDesc *TableDescriptor,
	node parser.QualifiedNames) ([]ColumnDescriptor, error) {
	if node == nil {
		// VisibleColumns is used here to prevent INSERT INTO <table> VALUES (...)
		// (as opposed to INSERT INTO <table> (...) VALUES (...)) from writing
		// hidden columns. At present, the only hidden column is the implicit rowid
		// primary key column.
		return tableDesc.VisibleColumns(), nil
	}

	cols := make([]ColumnDescriptor, len(node))
	colIDSet := make(map[ColumnID]struct{}, len(node))
	for i, n := range node {
		// TODO(pmattis): If the name is qualified, verify the table name matches
		// tableDesc.Name.
		if err := n.NormalizeColumnName(); err != nil {
			return nil, err
		}
		col, err := tableDesc.FindActiveColumnByName(n.Column())
		if err != nil {
			return nil, err
		}
		if _, ok := colIDSet[col.ID]; ok {
			return nil, fmt.Errorf("multiple assignments to same column \"%s\"", n.Column())
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = col
	}

	return cols, nil
}

func (p *planner) fillDefaults(defaultExprs []parser.TypedExpr,
	cols []ColumnDescriptor, n *parser.Insert) (parser.SelectStatement, error) {
	if n.DefaultValues() {
		row := make(parser.Exprs, 0, len(cols))
		for i := range cols {
			if defaultExprs == nil {
				row = append(row, parser.DNull)
				continue
			}
			row = append(row, defaultExprs[i])
		}
		return &parser.ValuesClause{Tuples: []*parser.Tuple{{Exprs: row}}}, nil
	}

	values, ok := n.Rows.Select.(*parser.ValuesClause)
	if !ok {
		return n.Rows.Select, nil
	}

	ret := values
	for tIdx, tuple := range values.Tuples {
		tupleCopied := false
		for eIdx, val := range tuple.Exprs {
			switch val.(type) {
			case parser.DefaultVal:
				if !tupleCopied {
					if ret == values {
						ret = &parser.ValuesClause{Tuples: append([]*parser.Tuple(nil), values.Tuples...)}
					}
					ret.Tuples[tIdx] =
						&parser.Tuple{Exprs: append([]parser.Expr(nil), tuple.Exprs...)}
					tupleCopied = true
				}
				if defaultExprs == nil {
					ret.Tuples[tIdx].Exprs[eIdx] = parser.DNull
				} else {
					ret.Tuples[tIdx].Exprs[eIdx] = defaultExprs[eIdx]
				}
			}
		}
	}
	return ret, nil
}

func makeDefaultExprs(
	cols []ColumnDescriptor, parse *parser.Parser, evalCtx parser.EvalContext,
) ([]parser.TypedExpr, error) {
	// Check to see if any of the columns have DEFAULT expressions. If there
	// are no DEFAULT expressions, we don't bother with constructing the
	// defaults map as the defaults are all NULL.
	haveDefaults := false
	for _, col := range cols {
		if col.DefaultExpr != nil {
			haveDefaults = true
			break
		}
	}
	if !haveDefaults {
		return nil, nil
	}

	// Build the default expressions map from the parsed SELECT statement.
	defaultExprs := make([]parser.TypedExpr, 0, len(cols))
	for _, col := range cols {
		if col.DefaultExpr == nil {
			defaultExprs = append(defaultExprs, parser.DNull)
			continue
		}
		expr, err := parser.ParseExprTraditional(*col.DefaultExpr)
		if err != nil {
			return nil, err
		}
		typedExpr, err := parser.TypeCheck(expr, nil, getTypeForColumn(col))
		if err != nil {
			return nil, err
		}
		if typedExpr, err = parse.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, err
		}
		if parser.ContainsVars(typedExpr) {
			return nil, util.Errorf("default expression contains variables")
		}
		defaultExprs = append(defaultExprs, typedExpr)
	}
	return defaultExprs, nil
}

func (p *planner) makeCheckExprs(cols []ColumnDescriptor) ([]parser.Expr, error) {
	// Check to see if any of the columns have CHECK expressions. If there are
	// no CHECK expressions, we don't bother with constructing it.
	numCheck := 0
	for _, col := range cols {
		if col.CheckExpr != nil {
			numCheck++
			break
		}
	}
	if numCheck == 0 {
		return nil, nil
	}

	checkExprs := make([]parser.Expr, 0, numCheck)
	for _, col := range cols {
		if col.CheckExpr == nil {
			continue
		}
		expr, err := parser.ParseExprTraditional(*col.CheckExpr)
		if err != nil {
			return nil, err
		}
		checkExprs = append(checkExprs, expr)
	}
	return checkExprs, nil
}

func (n *insertNode) Columns() []ResultColumn {
	return n.rh.columns
}

func (n *insertNode) Values() parser.DTuple {
	return n.run.resultRow
}

func (n *insertNode) MarkDebug(mode explainMode) {
	n.run.rows.MarkDebug(mode)
}

func (n *insertNode) DebugValues() debugValues {
	return n.run.rows.DebugValues()
}

func (n *insertNode) Ordering() orderingInfo {
	return n.run.rows.Ordering()
}

func (n *insertNode) PErr() *roachpb.Error {
	return n.run.pErr
}

func (n *insertNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	if v {
		fmt.Fprintf(&buf, "into %s (", n.tableDesc.Name)
		for i, col := range n.cols {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			fmt.Fprintf(&buf, "%s", col.Name)
		}
		fmt.Fprintf(&buf, ") returning (")
		for i, col := range n.rh.columns {
			if i > 0 {
				fmt.Fprintf(&buf, ", ")
			}
			fmt.Fprintf(&buf, "%s", col.Name)
		}
		fmt.Fprintf(&buf, ")")
	}
	return "insert", buf.String(), []planNode{n.run.rows}
}

func (n *insertNode) SetLimitHint(numRows int64, soft bool) {}
