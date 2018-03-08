// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

var upsertNodePool = sync.Pool{
	New: func() interface{} {
		return &upsertNode{}
	},
}

type upsertNode struct {
	// The following fields are populated during makePlan.
	editNodeBase
	defaultExprs []tree.TypedExpr
	computeExprs []tree.TypedExpr
	checkHelper  *sqlbase.CheckHelper

	insertCols   []sqlbase.ColumnDescriptor
	computedCols []sqlbase.ColumnDescriptor
	tw           tableWriter

	run upsertRun

	autoCommit autoCommitOpt
}

// upsertNode implements the autoCommitNode interface.
var _ autoCommitNode = &upsertNode{}

func (p *planner) newUpsertNode(
	ctx context.Context,
	n *tree.Insert,
	en editNodeBase,
	ri sqlbase.RowInserter,
	tn, alias *tree.TableName,
	rows planNode,
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	computedCols []sqlbase.ColumnDescriptor,
	fkTables sqlbase.TableLookupsByID,
	desiredTypes []types.T,
) (res planNode, err error) {
	updateExprs, conflictIndex, err := upsertExprsAndIndex(en.tableDesc, *n.OnConflict, ri.InsertCols)
	if err != nil {
		return nil, err
	}

	needRows := false
	if _, ok := n.Returning.(*tree.ReturningExprs); ok {
		needRows = true
	}

	un := upsertNodePool.Get().(*upsertNode)
	*un = upsertNode{
		editNodeBase: en,
		defaultExprs: defaultExprs,
		computeExprs: computeExprs,
		insertCols:   ri.InsertCols,
		computedCols: computedCols,
		run: upsertRun{
			insertColIDtoRowIndex: ri.InsertColIDtoRowIndex,
		},
		checkHelper: fkTables[en.tableDesc.ID].CheckHelper,
	}
	defer func() {
		if err != nil {
			un.Close(ctx)
		}
	}()

	if n.OnConflict.DoNothing {
		// TODO(dan): Postgres allows ON CONFLICT DO NOTHING without specifying a
		// conflict index, which means do nothing on any conflict. Support this if
		// someone needs it.
		un.tw = &tableUpserter{
			ri:            ri,
			conflictIndex: *conflictIndex,
			collectRows:   needRows,
			alloc:         &p.alloc,
		}
	} else {
		names, err := p.namesForExprs(updateExprs)
		if err != nil {
			return nil, err
		}
		// Also include columns that are inactive because they should be
		// updated.
		updateCols := make([]sqlbase.ColumnDescriptor, len(names))
		for i, n := range names {
			col, _, err := en.tableDesc.FindColumnByName(n)
			if err != nil {
				return nil, err
			}
			updateCols[i] = col
		}

		// We also need to include any computed columns in the set of UpdateCols.
		// They can't have been set explicitly so there's no chance of
		// double-including a computed column.

		// TODO(justin): we should size updateCols appropriately beforehand,
		// but we'd need to know how many computed columns there are (and it
		// would be more complicated if we only updated computed columns when
		// necessary).
		updateCols = append(updateCols, computedCols...)

		helper, err := p.newUpsertHelper(
			ctx, tn, en.tableDesc, ri.InsertCols, updateCols, updateExprs, computeExprs, conflictIndex, n.OnConflict.Where,
		)
		if err != nil {
			return nil, err
		}

		un.tw = &tableUpserter{
			ri:            ri,
			alloc:         &p.alloc,
			anyComputed:   len(computeExprs) >= 0,
			fkTables:      fkTables,
			updateCols:    updateCols,
			conflictIndex: *conflictIndex,
			collectRows:   needRows,
			evaler:        helper,
			isUpsertAlias: n.OnConflict.IsUpsertAlias(),
		}
	}

	if err := un.run.initEditNode(
		ctx, &un.editNodeBase, rows, un.tw, alias, n.Returning, desiredTypes); err != nil {
		return nil, err
	}

	return un, nil
}

func (n *upsertNode) Close(ctx context.Context) {
	if n.tw != nil {
		n.tw.close(ctx)
	}
	if n.run.rows != nil {
		n.run.rows.Close(ctx)
	}
	if n.run.rowsUpserted != nil {
		n.run.rowsUpserted.Close(ctx)
		n.run.rowsUpserted = nil
	}
	*n = upsertNode{}
	upsertNodePool.Put(n)
}

// upsertRun contains the run-time state of upsertNode during local execution.
type upsertRun struct {
	// The following fields are populated during Start().
	editNodeRun

	insertColIDtoRowIndex map[sqlbase.ColumnID]int

	rowIdxToRetIdx []int
	rowTemplate    tree.Datums

	rowsUpserted *sqlbase.RowContainer

	numRows   int
	curRowIdx int
}

func (n *upsertNode) startExec(params runParams) error {
	if n.rh.exprs != nil {
		// In some cases (e.g. `INSERT INTO t (a) ...`) rowVals does not contain all the table
		// columns. We need to pass values for all table columns to rh, in the correct order; we
		// will use rowTemplate for this. We also need a table that maps row indices to rowTemplate indices
		// to fill in the row values; any absent values will be NULLs.
		n.run.rowTemplate = make(tree.Datums, len(n.tableDesc.Columns))
		for i := range n.run.rowTemplate {
			n.run.rowTemplate[i] = tree.DNull
		}

		colIDToRetIndex := map[sqlbase.ColumnID]int{}
		for i, col := range n.tableDesc.Columns {
			colIDToRetIndex[col.ID] = i
		}

		n.run.rowIdxToRetIdx = make([]int, len(n.insertCols))
		for i, col := range n.insertCols {
			n.run.rowIdxToRetIdx[i] = colIDToRetIndex[col.ID]
		}

		n.run.rowsUpserted = sqlbase.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(n.rh.columns),
			0,
		)
	}

	if err := n.run.startEditNode(params, &n.editNodeBase); err != nil {
		return err
	}

	if err := n.run.tw.init(params.p.txn, params.EvalContext()); err != nil {
		return err
	}

	// Do all the work upfront.
	for {
		next, err := n.internalNext(params)
		if err != nil {
			return err
		}
		if !next {
			break
		}
	}
	// Initialize the current row index for the first call to Next().
	n.run.curRowIdx = -1
	return nil
}

func (n *upsertNode) FastPathResults() (int, bool) {
	if n.run.rowsUpserted != nil {
		return 0, false
	}
	return n.run.numRows, true
}

func (n *upsertNode) Next(params runParams) (bool, error) {
	if n.run.rowsUpserted == nil {
		return false, nil
	}
	n.run.curRowIdx++
	return n.run.curRowIdx < n.run.rowsUpserted.Len(), nil
}

func (n *upsertNode) internalNext(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	if next, err := n.run.rows.Next(params); !next {
		if err != nil {
			return false, err
		}
		// We're done. Finish the batch.
		rows, err := n.tw.finalize(
			params.ctx, n.autoCommit, params.extendedEvalCtx.Tracing.KVTracingEnabled())
		if err != nil {
			return false, err
		}

		if n.run.rowsUpserted != nil {
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
		}
		return false, nil
	}

	n.run.numRows++
	rowVals, err := GenerateInsertRow(
		n.defaultExprs,
		n.computeExprs,
		n.run.insertColIDtoRowIndex,
		n.insertCols,
		n.computedCols,
		*params.EvalContext(),
		n.tableDesc,
		n.run.rows.Values(),
	)
	if err != nil {
		return false, err
	}

	if err := n.checkHelper.LoadRow(n.run.insertColIDtoRowIndex, rowVals, false); err != nil {
		return false, err
	}
	if err := n.checkHelper.Check(params.EvalContext()); err != nil {
		return false, err
	}

	_, err = n.tw.row(params.ctx, rowVals, params.extendedEvalCtx.Tracing.KVTracingEnabled())
	return err == nil, err
}

func (n *upsertNode) Values() tree.Datums {
	return n.run.rowsUpserted.At(n.run.curRowIdx)
}

// enableAutoCommit is part of the autoCommitNode interface.
func (n *upsertNode) enableAutoCommit() {
	n.autoCommit = autoCommitEnabled
}

// upsertExcludedTable is the name of a synthetic table used in an upsert's set
// expressions to refer to the values that would be inserted for a row if it
// didn't conflict.
// Example: `INSERT INTO kv VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = excluded.v`
var upsertExcludedTable = tree.MakeUnqualifiedTableName("excluded")

type upsertHelper struct {
	p                  *planner
	evalExprs          []tree.TypedExpr
	whereExpr          tree.TypedExpr
	sourceInfo         *sqlbase.DataSourceInfo
	excludedSourceInfo *sqlbase.DataSourceInfo
	curSourceRow       tree.Datums
	curExcludedRow     tree.Datums

	ivarHelper *tree.IndexedVarHelper

	ccIvarContainer *rowIndexedVarContainer
	ccIvarHelper    *tree.IndexedVarHelper
	computeExprs    []tree.TypedExpr

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

var _ tableUpsertEvaler = (*upsertHelper)(nil)

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.curExcludedRow[idx-numSourceColumns].Eval(ctx)
	}
	return uh.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarResolvedType(idx int) types.T {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.SourceColumns[idx-numSourceColumns].Typ
	}
	return uh.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (uh *upsertHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	numSourceColumns := len(uh.sourceInfo.SourceColumns)
	if idx >= numSourceColumns {
		return uh.excludedSourceInfo.NodeFormatter(idx - numSourceColumns)
	}
	return uh.sourceInfo.NodeFormatter(idx)
}

func (p *planner) newUpsertHelper(
	ctx context.Context,
	tn *tree.TableName,
	tableDesc *sqlbase.TableDescriptor,
	insertCols []sqlbase.ColumnDescriptor,
	updateCols []sqlbase.ColumnDescriptor,
	updateExprs tree.UpdateExprs,
	computeExprs []tree.TypedExpr,
	upsertConflictIndex *sqlbase.IndexDescriptor,
	whereClause *tree.Where,
) (*upsertHelper, error) {
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	untupledExprs := make(tree.Exprs, 0, len(updateExprs))
	i := 0
	for _, updateExpr := range updateExprs {
		if updateExpr.Tuple {
			if t, ok := updateExpr.Expr.(*tree.Tuple); ok {
				for _, e := range t.Exprs {
					e = fillDefault(e, i, defaultExprs)
					untupledExprs = append(untupledExprs, e)
					i++
				}
			}
		} else {
			e := fillDefault(updateExpr.Expr, i, defaultExprs)
			untupledExprs = append(untupledExprs, e)
			i++
		}
	}

	sourceInfo := sqlbase.NewSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.Columns),
	)
	excludedSourceInfo := sqlbase.NewSourceInfoForSingleTable(
		upsertExcludedTable, sqlbase.ResultColumnsFromColDescs(insertCols),
	)

	helper := &upsertHelper{
		p:                  p,
		sourceInfo:         sourceInfo,
		excludedSourceInfo: excludedSourceInfo,
	}

	var evalExprs []tree.TypedExpr
	ivarHelper := tree.MakeIndexedVarHelper(helper, len(sourceInfo.SourceColumns)+len(excludedSourceInfo.SourceColumns))
	sources := sqlbase.MultiSourceInfo{sourceInfo, excludedSourceInfo}
	for i, expr := range untupledExprs {
		typ := updateCols[i].Type.ToDatumType()
		normExpr, err := p.analyzeExpr(ctx, expr, sources, ivarHelper, typ, true, "ON CONFLICT")
		if err != nil {
			return nil, err
		}
		evalExprs = append(evalExprs, normExpr)
	}
	helper.ivarHelper = &ivarHelper
	helper.evalExprs = evalExprs

	// We need this IVarContainer to be able to evaluate the computed columns for each row.
	// Since we just have the entire row, this is just the identity mapping.
	mapping := make(map[sqlbase.ColumnID]int)
	for i, c := range tableDesc.Columns {
		mapping[c.ID] = i
	}
	helper.ccIvarContainer = &rowIndexedVarContainer{
		cols:    tableDesc.Columns,
		mapping: mapping,
	}
	ccIvarHelper := tree.MakeIndexedVarHelper(helper.ccIvarContainer, len(sourceInfo.SourceColumns))
	helper.ccIvarHelper = &ccIvarHelper
	helper.computeExprs = computeExprs

	if whereClause != nil {
		whereExpr, err := p.analyzeExpr(
			ctx, whereClause.Expr, sources, ivarHelper, types.Bool, true /* requireType */, "WHERE",
		)
		if err != nil {
			return nil, err
		}

		// Make sure there are no aggregation/window functions in the filter
		// (after subqueries have been expanded).
		if err := p.txCtx.AssertNoAggregationOrWindowing(
			whereExpr, "WHERE", p.SessionData().SearchPath,
		); err != nil {
			return nil, err
		}

		helper.whereExpr = whereExpr
	}

	return helper, nil
}

func (uh *upsertHelper) walkExprs(walk func(desc string, index int, expr tree.TypedExpr)) {
	for i, evalExpr := range uh.evalExprs {
		walk("eval", i, evalExpr)
	}
}

// eval returns the values for the update case of an upsert, given the row
// that would have been inserted and the existing (conflicting) values.
func (uh *upsertHelper) eval(insertRow tree.Datums, existingRow tree.Datums) (tree.Datums, error) {
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	var err error
	ret := make([]tree.Datum, len(uh.evalExprs))
	uh.p.extendedEvalCtx.PushIVarContainer(uh.ivarHelper.Container())
	defer func() { uh.p.extendedEvalCtx.PopIVarContainer() }()
	for i, evalExpr := range uh.evalExprs {
		ret[i], err = evalExpr.Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// evalComputedCols handles after we've figured out the values of all regular
// columns, what the values of any incoming computed columns should be.
// It then appends those new values to the end of a given slice.
func (uh *upsertHelper) evalComputedCols(
	updatedRow tree.Datums, appendTo tree.Datums,
) (tree.Datums, error) {
	uh.ccIvarContainer.curSourceRow = updatedRow
	uh.p.EvalContext().IVarContainer = uh.ccIvarContainer
	defer func() { uh.p.EvalContext().IVarContainer = nil }()
	for i := range uh.computeExprs {
		res, err := uh.computeExprs[i].Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
		appendTo = append(appendTo, res)
	}
	return appendTo, nil
}

// shouldUpdate returns the result of evaluating the WHERE clause of the
// ON CONFLICT ... DO UPDATE clause.
func (uh *upsertHelper) shouldUpdate(insertRow tree.Datums, existingRow tree.Datums) (bool, error) {
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	uh.p.extendedEvalCtx.PushIVarContainer(uh.ivarHelper.Container())
	defer func() { uh.p.extendedEvalCtx.PopIVarContainer() }()
	return sqlbase.RunFilter(uh.whereExpr, uh.p.EvalContext())
}

// upsertExprsAndIndex returns the upsert conflict index and the (possibly
// synthetic) SET expressions used when a row conflicts.
func upsertExprsAndIndex(
	tableDesc *sqlbase.TableDescriptor,
	onConflict tree.OnConflict,
	insertCols []sqlbase.ColumnDescriptor,
) (tree.UpdateExprs, *sqlbase.IndexDescriptor, error) {
	if onConflict.IsUpsertAlias() {
		// The UPSERT syntactic sugar is the same as the longhand specifying the
		// primary index as the conflict index and SET expressions for the columns
		// in insertCols minus any columns in the conflict index. Example:
		// `UPSERT INTO abc VALUES (1, 2, 3)` is syntactic sugar for
		// `INSERT INTO abc VALUES (1, 2, 3) ON CONFLICT a DO UPDATE SET b = 2, c = 3`.
		conflictIndex := &tableDesc.PrimaryIndex
		indexColSet := make(map[sqlbase.ColumnID]struct{}, len(conflictIndex.ColumnIDs))
		for _, colID := range conflictIndex.ColumnIDs {
			indexColSet[colID] = struct{}{}
		}
		updateExprs := make(tree.UpdateExprs, 0, len(insertCols))
		for _, c := range insertCols {
			if c.ComputeExpr != nil {
				continue
			}
			if _, ok := indexColSet[c.ID]; ok {
				continue
			}
			n := tree.Name(c.Name)
			expr := tree.NewColumnItem(&upsertExcludedTable, n)
			updateExprs = append(updateExprs, &tree.UpdateExpr{Names: tree.NameList{n}, Expr: expr})
		}
		return updateExprs, conflictIndex, nil
	}

	indexMatch := func(index sqlbase.IndexDescriptor) bool {
		if !index.Unique {
			return false
		}
		if len(index.ColumnNames) != len(onConflict.Columns) {
			return false
		}
		for i, colName := range index.ColumnNames {
			if colName != string(onConflict.Columns[i]) {
				return false
			}
		}
		return true
	}

	if indexMatch(tableDesc.PrimaryIndex) {
		return onConflict.Exprs, &tableDesc.PrimaryIndex, nil
	}
	for _, index := range tableDesc.Indexes {
		if indexMatch(index) {
			return onConflict.Exprs, &index, nil
		}
	}
	return nil, nil, fmt.Errorf("there is no unique or exclusion constraint matching the ON CONFLICT specification")
}
