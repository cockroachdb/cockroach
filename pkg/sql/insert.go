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
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var insertNodePool = sync.Pool{
	New: func() interface{} {
		return &insertNode{}
	},
}

var tableInserterPool = sync.Pool{
	New: func() interface{} {
		return &tableInserter{}
	},
}

var tableUpserterPool = sync.Pool{
	New: func() interface{} {
		return &tableUpserter{}
	},
}

type insertNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	defaultExprs []parser.TypedExpr
	n            *parser.Insert
	checkHelper  checkHelper

	insertCols            []sqlbase.ColumnDescriptor
	insertColIDtoRowIndex map[sqlbase.ColumnID]int
	tw                    tableWriter

	isUpsertReturning bool

	run struct {
		// The following fields are populated during Start().
		editNodeRun

		rowIdxToRetIdx []int
		rowTemplate    parser.Datums

		doneUpserting bool
		rowsUpserted  *sqlbase.RowContainer
	}
}

// Insert inserts rows into the database.
// Privileges: INSERT on table. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
//   Notes: postgres requires INSERT. No "on duplicate key update" option.
//          mysql requires INSERT. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
func (p *planner) Insert(
	ctx context.Context, n *parser.Insert, desiredTypes []parser.Type,
) (planNode, error) {
	tn, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	en, err := p.makeEditNode(ctx, tn, privilege.INSERT)
	if err != nil {
		return nil, err
	}
	isUpsertReturning := false
	if n.OnConflict != nil {
		if !n.OnConflict.DoNothing {
			if err := p.CheckPrivilege(en.tableDesc, privilege.UPDATE); err != nil {
				return nil, err
			}
		}
		if _, ok := n.Returning.(*parser.ReturningExprs); ok {
			isUpsertReturning = true
		}
	}

	var cols []sqlbase.ColumnDescriptor
	// Determine which columns we're inserting into.
	if n.DefaultValues() {
		cols = en.tableDesc.Columns
	} else {
		var err error
		if cols, err = p.processColumns(en.tableDesc, n.Columns); err != nil {
			return nil, err
		}
	}
	// Number of columns expecting an input. This doesn't include the
	// columns receiving a default value.
	numInputColumns := len(cols)

	cols, defaultExprs, err :=
		sqlbase.ProcessDefaultColumns(cols, en.tableDesc, &p.parser, &p.evalCtx)
	if err != nil {
		return nil, err
	}

	var insertRows parser.SelectStatement
	if n.DefaultValues() {
		insertRows = getDefaultValuesClause(defaultExprs, cols)
	} else {
		src, values, err := extractInsertSource(n.Rows)
		if err != nil {
			return nil, err
		}
		if values != nil {
			src = fillDefaults(defaultExprs, cols, values)
		}
		insertRows = src
	}

	// Analyze the expressions for column information and typing.
	desiredTypesFromSelect := make([]parser.Type, len(cols))
	for i, col := range cols {
		desiredTypesFromSelect[i] = col.Type.ToDatumType()
	}

	// Create the plan for the data source.
	// This performs type checking on source expressions, collecting
	// types for placeholders in the process.
	rows, err := p.newPlan(ctx, insertRows, desiredTypesFromSelect)
	if err != nil {
		return nil, err
	}

	if expressions := len(planColumns(rows)); expressions > numInputColumns {
		return nil, fmt.Errorf("INSERT error: table %s has %d columns but %d values were supplied", n.Table, numInputColumns, expressions)
	}

	fkTables := sqlbase.TablesNeededForFKs(*en.tableDesc, sqlbase.CheckInserts)
	if err := p.fillFKTableMap(ctx, fkTables); err != nil {
		return nil, err
	}
	ri, err := sqlbase.MakeRowInserter(p.txn, en.tableDesc, fkTables, cols,
		sqlbase.CheckFKs, &p.alloc)
	if err != nil {
		return nil, err
	}

	var tw tableWriter
	if n.OnConflict == nil {
		ti := tableInserterPool.Get().(*tableInserter)
		*ti = tableInserter{ri: ri, autoCommit: p.autoCommit}
		tw = ti
	} else {
		updateExprs, conflictIndex, err := upsertExprsAndIndex(en.tableDesc, *n.OnConflict, ri.InsertCols)
		if err != nil {
			return nil, err
		}

		if n.OnConflict.DoNothing {
			// TODO(dan): Postgres allows ON CONFLICT DO NOTHING without specifying a
			// conflict index, which means do nothing on any conflict. Support this if
			// someone needs it.
			tu := tableUpserterPool.Get().(*tableUpserter)
			*tu = tableUpserter{
				ri:            ri,
				autoCommit:    p.autoCommit,
				conflictIndex: *conflictIndex,
				alloc:         &p.alloc,
				mon:           &p.session.TxnState.mon,
				collectRows:   isUpsertReturning,
			}
			tw = tu
		} else {
			names, err := p.namesForExprs(updateExprs)
			if err != nil {
				return nil, err
			}
			// Also include columns that are inactive because they should be
			// updated.
			updateCols := make([]sqlbase.ColumnDescriptor, len(names))
			for i, n := range names {
				c, err := n.NormalizeUnqualifiedColumnItem()
				if err != nil {
					return nil, err
				}

				col, _, err := en.tableDesc.FindColumnByName(c.ColumnName)
				if err != nil {
					return nil, err
				}
				updateCols[i] = col
			}

			helper, err := p.makeUpsertHelper(
				ctx, tn, en.tableDesc, ri.InsertCols, updateCols, updateExprs, conflictIndex, n.OnConflict.Where,
			)
			if err != nil {
				return nil, err
			}

			fkTables := sqlbase.TablesNeededForFKs(*en.tableDesc, sqlbase.CheckUpdates)
			if err := p.fillFKTableMap(ctx, fkTables); err != nil {
				return nil, err
			}
			tu := tableUpserterPool.Get().(*tableUpserter)
			*tu = tableUpserter{
				ri:            ri,
				autoCommit:    p.autoCommit,
				alloc:         &p.alloc,
				mon:           &p.session.TxnState.mon,
				collectRows:   isUpsertReturning,
				fkTables:      fkTables,
				updateCols:    updateCols,
				conflictIndex: *conflictIndex,
				evaler:        helper,
				isUpsertAlias: n.OnConflict.IsUpsertAlias(),
			}
			tw = tu
		}
	}

	in := insertNodePool.Get().(*insertNode)
	*in = insertNode{
		n:                     n,
		editNodeBase:          en,
		defaultExprs:          defaultExprs,
		insertCols:            ri.InsertCols,
		insertColIDtoRowIndex: ri.InsertColIDtoRowIndex,
		isUpsertReturning:     isUpsertReturning,
		tw:                    tw,
	}

	if err := in.checkHelper.init(ctx, p, tn, en.tableDesc); err != nil {
		return nil, err
	}

	if err := in.run.initEditNode(
		ctx, &in.editNodeBase, rows, in.tw, tn, n.Returning, desiredTypes); err != nil {
		return nil, err
	}

	return in, nil
}

func (n *insertNode) Start(params runParams) error {
	// Prepare structures for building values to pass to rh.
	// TODO(couchand): Delete this, use tablewriter interface.
	if n.rh.exprs != nil {
		// In some cases (e.g. `INSERT INTO t (a) ...`) rowVals does not contain all the table
		// columns. We need to pass values for all table columns to rh, in the correct order; we
		// will use rowTemplate for this. We also need a table that maps row indices to rowTemplate indices
		// to fill in the row values; any absent values will be NULLs.

		n.run.rowTemplate = make(parser.Datums, len(n.tableDesc.Columns))
		for i := range n.run.rowTemplate {
			n.run.rowTemplate[i] = parser.DNull
		}

		colIDToRetIndex := map[sqlbase.ColumnID]int{}
		for i, col := range n.tableDesc.Columns {
			colIDToRetIndex[col.ID] = i
		}

		n.run.rowIdxToRetIdx = make([]int, len(n.insertCols))
		for i, col := range n.insertCols {
			n.run.rowIdxToRetIdx[i] = colIDToRetIndex[col.ID]
		}
	}

	if err := n.run.startEditNode(params, &n.editNodeBase); err != nil {
		return err
	}

	return n.run.tw.init(params.p.txn)
}

func (n *insertNode) Close(ctx context.Context) {
	n.tw.close(ctx)
	n.run.rows.Close(ctx)
	n.run.rows = nil
	if n.run.rowsUpserted != nil {
		n.run.rowsUpserted.Close(ctx)
		n.run.rowsUpserted = nil
	}
	switch t := n.tw.(type) {
	case *tableInserter:
		*t = tableInserter{}
		tableInserterPool.Put(t)
	case *tableUpserter:
		*t = tableUpserter{}
		tableUpserterPool.Put(t)
	}
	*n = insertNode{}
	insertNodePool.Put(n)
}

func (n *insertNode) Next(params runParams) (bool, error) {
	if n.isUpsertReturning {
		return n.drain(params)
	}

	return n.internalNext(params)
}

// Because TableUpserter batches the upserts, we need to completely drain the
// source and handle all the rows before returning from the first call to Next,
// so that we can return an upserted row from each call to Values.
func (n *insertNode) drain(params runParams) (bool, error) {
	for !n.run.doneUpserting {
		_, err := n.internalNext(params)
		if err != nil {
			return false, err
		}
	}
	hasRows := n.run.rowsUpserted.Len() > 0
	return hasRows, nil
}

func (n *insertNode) internalNext(params runParams) (bool, error) {
	if next, err := n.run.rows.Next(params); !next {
		if err == nil {
			if err := params.p.cancelChecker.Check(); err != nil {
				return false, err
			}
			// We're done. Finish the batch.
			rows, err := n.tw.finalize(params.ctx, params.p.session.Tracing.KVTracingEnabled())
			if err != nil {
				return false, err
			}

			if n.isUpsertReturning {
				n.run.rowsUpserted = sqlbase.NewRowContainer(
					params.p.session.TxnState.makeBoundAccount(),
					sqlbase.ColTypeInfoFromResCols(n.rh.columns),
					rows.Len(),
				)
				for i := 0; i < rows.Len(); i++ {
					cooked, err := n.rh.cookResultRow(rows.At(i))
					if err != nil {
						return false, err
					}
					_, err = n.run.rowsUpserted.AddRow(params.ctx, cooked)
					if err != nil {
						return false, err
					}
				}
				n.run.doneUpserting = true
			}
		}
		return false, err
	}

	rowVals, err := GenerateInsertRow(n.defaultExprs, n.insertColIDtoRowIndex, n.insertCols, params.p.evalCtx, n.tableDesc, n.run.rows.Values())
	if err != nil {
		return false, err
	}

	if err := n.checkHelper.loadRow(n.insertColIDtoRowIndex, rowVals, false); err != nil {
		return false, err
	}
	if err := n.checkHelper.check(&params.p.evalCtx); err != nil {
		return false, err
	}

	_, err = n.tw.row(params.ctx, rowVals, params.p.session.Tracing.KVTracingEnabled())
	if err != nil {
		return false, err
	}

	// Handle regular INSERT ... RETURNING without ON CONFLICT clause
	if !n.isUpsertReturning {
		for i, val := range rowVals {
			if n.run.rowTemplate != nil {
				n.run.rowTemplate[n.run.rowIdxToRetIdx[i]] = val
			}
		}

		resultRow, err := n.rh.cookResultRow(n.run.rowTemplate)
		if err != nil {
			return false, err
		}
		n.run.resultRow = resultRow
	}

	return true, nil
}

// GenerateInsertRow prepares a row tuple for insertion. It fills in default
// expressions, verifies non-nullable columns, and checks column widths.
func GenerateInsertRow(
	defaultExprs []parser.TypedExpr,
	insertColIDtoRowIndex map[sqlbase.ColumnID]int,
	insertCols []sqlbase.ColumnDescriptor,
	evalCtx parser.EvalContext,
	tableDesc *sqlbase.TableDescriptor,
	rowVals parser.Datums,
) (parser.Datums, error) {
	// The values for the row may be shorter than the number of columns being
	// inserted into. Generate default values for those columns using the
	// default expressions.

	if len(rowVals) < len(insertCols) {
		// It's not cool to append to the slice returned by a node; make a copy.
		oldVals := rowVals
		rowVals = make(parser.Datums, len(insertCols))
		copy(rowVals, oldVals)

		for i := len(oldVals); i < len(insertCols); i++ {
			if defaultExprs == nil {
				rowVals[i] = parser.DNull
				continue
			}
			d, err := defaultExprs[i].Eval(&evalCtx)
			if err != nil {
				return nil, err
			}
			rowVals[i] = d
		}
	}

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range tableDesc.Columns {
		if !col.Nullable {
			if i, ok := insertColIDtoRowIndex[col.ID]; !ok || rowVals[i] == parser.DNull {
				return nil, sqlbase.NewNonNullViolationError(col.Name)
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := range rowVals {
		if err := sqlbase.CheckValueWidth(insertCols[i], rowVals[i]); err != nil {
			return nil, err
		}
	}
	return rowVals, nil
}

func (p *planner) processColumns(
	tableDesc *sqlbase.TableDescriptor, node parser.UnresolvedNames,
) ([]sqlbase.ColumnDescriptor, error) {
	if node == nil {
		// VisibleColumns is used here to prevent INSERT INTO <table> VALUES (...)
		// (as opposed to INSERT INTO <table> (...) VALUES (...)) from writing
		// hidden columns. At present, the only hidden column is the implicit rowid
		// primary key column.
		return tableDesc.VisibleColumns(), nil
	}

	cols := make([]sqlbase.ColumnDescriptor, len(node))
	colIDSet := make(map[sqlbase.ColumnID]struct{}, len(node))
	for i, n := range node {
		c, err := n.NormalizeUnqualifiedColumnItem()
		if err != nil {
			return nil, err
		}

		if len(c.Selector) > 0 {
			return nil, pgerror.UnimplementedWithIssueErrorf(8318, "compound types not supported yet: %q", n)
		}

		col, err := tableDesc.FindActiveColumnByName(string(c.ColumnName))
		if err != nil {
			return nil, err
		}

		if _, ok := colIDSet[col.ID]; ok {
			return nil, fmt.Errorf("multiple assignments to the same column %q", n)
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = col
	}

	return cols, nil
}

// extractInsertSource removes the parentheses around the data source of an INSERT statement.
// If the data source is a VALUES clause not further qualified with LIMIT/OFFSET and ORDER BY,
// the 2nd return value is a pre-casted pointer to the VALUES clause.
func extractInsertSource(s *parser.Select) (parser.SelectStatement, *parser.ValuesClause, error) {
	wrapped := s.Select
	limit := s.Limit
	orderBy := s.OrderBy

	for s, ok := wrapped.(*parser.ParenSelect); ok; s, ok = wrapped.(*parser.ParenSelect) {
		wrapped = s.Select.Select
		if s.Select.OrderBy != nil {
			if orderBy != nil {
				return nil, nil, fmt.Errorf("multiple ORDER BY clauses not allowed")
			}
			orderBy = s.Select.OrderBy
		}
		if s.Select.Limit != nil {
			if limit != nil {
				return nil, nil, fmt.Errorf("multiple LIMIT clauses not allowed")
			}
			limit = s.Select.Limit
		}
	}

	if orderBy == nil && limit == nil {
		values, _ := wrapped.(*parser.ValuesClause)
		return wrapped, values, nil
	}
	return &parser.ParenSelect{
		Select: &parser.Select{Select: wrapped, OrderBy: orderBy, Limit: limit},
	}, nil, nil
}

func getDefaultValuesClause(
	defaultExprs []parser.TypedExpr, cols []sqlbase.ColumnDescriptor,
) parser.SelectStatement {
	row := make(parser.Exprs, 0, len(cols))
	for i := range cols {
		if defaultExprs == nil {
			row = append(row, parser.DNull)
			continue
		}
		row = append(row, defaultExprs[i])
	}
	return &parser.ValuesClause{Tuples: []*parser.Tuple{{Exprs: row}}}
}

func fillDefaults(
	defaultExprs []parser.TypedExpr, cols []sqlbase.ColumnDescriptor, values *parser.ValuesClause,
) *parser.ValuesClause {
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
				if defaultExprs == nil || eIdx >= len(defaultExprs) {
					// The case where eIdx is too large for defaultExprs will be
					// transformed into an error by the check on the number of
					// columns in Insert().
					ret.Tuples[tIdx].Exprs[eIdx] = parser.DNull
				} else {
					ret.Tuples[tIdx].Exprs[eIdx] = defaultExprs[eIdx]
				}
			}
		}
	}
	return ret
}

func (n *insertNode) Values() parser.Datums {
	if !n.isUpsertReturning {
		return n.run.resultRow
	}

	row := n.run.rowsUpserted.At(0)
	n.run.rowsUpserted.PopFirst()
	return row
}
