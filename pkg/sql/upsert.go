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
	// Extract the index that will detect upsert conflicts
	// (conflictIndex) and the assignment expressions to use when
	// conflicts are detected (updateExprs).
	updateExprs, conflictIndex, err := upsertExprsAndIndex(en.tableDesc, *n.OnConflict, ri.InsertCols)
	if err != nil {
		return nil, err
	}

	// needRows determines whether we need to accumulate result rows.
	needRows := false
	if _, ok := n.Returning.(*tree.ReturningExprs); ok {
		needRows = true
	}

	// Instantiate the upsert node.
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
		// If anything below fails, we don't want to leak
		// resources. Ensure the thing is always closed and put back to
		// its alloc pool.
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
		// We're going to work on allocating an upsertHelper here, even
		// though it might end up not being used below in this fast
		// path. This is because this also performs a semantic check, that
		// the upsert column references are not ambiguous (in particular,
		// this rejects attempts to upsert into a table called "excluded"
		// without an AS clause).

		// Determine which columns are updated by the RHS of INSERT
		// ... ON CONFLICT DO UPDATE, or the non-PK columns in an
		// UPSERT.
		names, err := p.namesForExprs(updateExprs)
		if err != nil {
			return nil, err
		}

		// We use ensureColumns = false in processColumns, because
		// updateCols may be legitimately empty (when there is no DO
		// UPDATE clause). We set allowMutations because we need
		// to populate all columns even those that are being added.
		updateCols, err := p.processColumns(en.tableDesc, names,
			false /* ensureColumns */, true /* allowMutations */)
		if err != nil {
			return nil, err
		}

		// We also need to include any computed columns in the set of UpdateCols.
		// They can't have been set explicitly so there's no chance of
		// double-including a computed column.
		updateCols = append(updateCols, computedCols...)

		// Instantiate the helper that will take over the evaluation of
		// SQL expressions. As described above, this also performs a
		// semantic check, so it cannot be skipped on the fast path below.
		helper, err := p.newUpsertHelper(
			ctx, tn, en.tableDesc,
			ri.InsertCols,
			updateCols,
			updateExprs,
			computeExprs,
			conflictIndex,
			n.OnConflict.Where,
		)
		if err != nil {
			return nil, err
		}

		// Determine whether to use the fast path or the slow path.
		// TODO(dan): The fast path is currently only enabled when the UPSERT alias
		// is explicitly selected by the user. It's possible to fast path some
		// queries of the form INSERT ... ON CONFLICT, but the utility is low and
		// there are lots of edge cases (that caused real correctness bugs #13437
		// #13962). As a result, we've decided to remove this until after 1.0 and
		// re-enable it then. See #14482.
		enableFastPath := n.OnConflict.IsUpsertAlias() &&
			// Tables with secondary indexes are not eligible for fast path (it
			// would be easy to add the new secondary index entry but we can't clean
			// up the old one without the previous values).
			len(en.tableDesc.Indexes) == 0 &&
			// When adding or removing a column in a schema change (mutation), the user
			// can't specify it, which means we need to do a lookup and so we can't use
			// the fast path. When adding or removing an index, same result, so the fast
			// path is disabled during all mutations.
			len(en.tableDesc.Mutations) == 0 &&
			// For the fast path, all columns must be specified in the insert.
			len(ri.InsertCols) == len(en.tableDesc.Columns) &&
			// We cannot use the fast path if we also have a RETURNING clause, because
			// RETURNING wants to see only the updated rows.
			!needRows

		if enableFastPath {
			// We then use the super-simple, super-fast writer. There's not
			// much else to prepare.
			un.tw = &fastTableUpserter{
				ri: ri,
			}
		} else {
			// General/slow path.
			un.tw = &tableUpserter{
				ri:            ri,
				alloc:         &p.alloc,
				anyComputed:   len(computeExprs) >= 0,
				fkTables:      fkTables,
				updateCols:    updateCols,
				conflictIndex: *conflictIndex,
				collectRows:   needRows,
				evaler:        helper,
			}
		}
	}

	// Common initialization in any case, includes setting up the
	// returning helper.
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

// spooled implements the planNodeSpooled interface.
func (n *upsertNode) spooled() {}

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

// upsertHelper is the helper struct in charge of evaluating SQL
// expression during an UPSERT. Its main responsibilities are:
//
// - eval(): compute the UPDATE statements given the previous and new values,
//   for INSERT ... ON CONFLICT DO UPDATE SET ...
//
// - evalComputedCols(): evaluate and add the computed columns
//   to the current upserted row.
//
// - shouldUpdate(): evaluates the WHERE clause of an ON CONFLICT
//   ... DO UPDATE clause.
//
// See the tableUpsertEvaler interface definition in tablewriter.go.
type upsertHelper struct {
	// p is used to provide an evalContext.
	p *planner

	// evalExprs contains the individual columns of SET RHS in an ON
	// CONFLICT DO UPDATE clause.
	// This decomposes tuple assignments, e.g. DO UPDATE SET (a,b) = (x,y)
	// into their individual expressions, in this case x, y.
	//
	// Its entries correspond 1-to-1 to the columns in
	// tableUpserter.updateCols.
	//
	// This is the main input for eval().
	evalExprs []tree.TypedExpr

	// whereExpr is the RHS of an ON CONFLICT DO UPDATE SET ... WHERE <expr>.
	// This is the main input for shouldUpdate().
	whereExpr tree.TypedExpr

	// sourceInfo describes the columns provided by the table being
	// upserted into.
	sourceInfo *sqlbase.DataSourceInfo
	// excludedSourceInfo describes the columns provided by the values
	// being upserted. This may be fewer columns than the
	// table, for example:
	// INSERT INTO kv(k) VALUES (3) ON CONFLICT(k) DO UPDATE SET v = excluded.v;
	//        invalid, value v is not part of the source in VALUES   ^^^^^^^^^^
	excludedSourceInfo *sqlbase.DataSourceInfo

	// curSourceRow buffers the current values from the table during
	// an upsert conflict. Used as input when evaluating
	// evalExprs and whereExpr.
	curSourceRow tree.Datums
	// curExcludedRow buffers the current values being upserted
	// during an upsert conflict. Used as input when evaluating
	// evalExprs and whereExpr.
	curExcludedRow tree.Datums

	// computeExprs is the list of expressions to (re-)compute computed
	// columns.
	// This is the main input for evalComputedCols().
	computeExprs []tree.TypedExpr

	// ccIvarContainer buffers the current values after the upsert
	// conflict resolution, to serve as input while evaluating
	// computeExprs.
	ccIvarContainer rowIndexedVarContainer

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

// newUpsertHelper instantiates an upsertHelper based on the extracted
// metadata.
// It needs to prepare three things:
// - the RHS expressions of UPDATE SET ... (evalExprs)
// - the UPDATE WHERE clause, if any (whereExpr)
// - the computed expressions, if any (computeExprs).
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
	// Extract the parsed default expressions for every updated
	// column. This populates nil for every entry in updateCols which
	// has no default.
	defaultExprs, err := sqlbase.MakeDefaultExprs(
		updateCols, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// Flatten any tuple assignment in the UPDATE SET clause(s).
	//
	// For example, if we have two tuple assignments like SET (a,b) =
	// (c,d), (x,y) = (u,v), we turn this into four expressions
	// [c,d,u,v] in untupledExprs.
	//
	// If any of the RHS expressions is the special DEFAULT keyword,
	// we substitute the default expression computed above.
	//
	// This works because the LHS of each SET clause has already been
	// untupled by the caller, so that for the example above updateCols
	// contains the metadata for [a,b,c,d] already, so there's always
	// exactly one entry in defaultExprs for the untuplification to
	// find.
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

	// At this point, the entries in updateCols and untupledExprs match
	// 1-to-1.

	// Start preparing the helper.
	//
	// We need to allocate early because the ivarHelper below needs a
	// heap reference to inject in the resolved indexed vars.
	helper := &upsertHelper{p: p, computeExprs: computeExprs}

	// Now on to analyze the evalExprs and the whereExpr.  These can
	// refer to both the original table and the upserted values, so they
	// need a double dataSourceInfo.

	// sourceInfo describes the columns provided by the table.
	helper.sourceInfo = sqlbase.NewSourceInfoForSingleTable(
		*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.Columns),
	)
	// excludedSourceInfo describes the columns provided by the
	// insert/upsert data source. This will be used to resolve
	// expressions of the form `excluded.x`, which refer to the values
	// coming from the data source.
	helper.excludedSourceInfo = sqlbase.NewSourceInfoForSingleTable(
		upsertExcludedTable, sqlbase.ResultColumnsFromColDescs(insertCols),
	)

	// Name resolution needs a multi-source which knows both about the
	// table being upsert into which contains values pre-upsert, and a
	// pseudo-table "excluded" which will contain values from the upsert
	// data source.
	sources := sqlbase.MultiSourceInfo{helper.sourceInfo, helper.excludedSourceInfo}

	// Prepare the ivarHelper which connects the indexed vars
	// back to this helper.
	ivarHelper := tree.MakeIndexedVarHelper(helper,
		len(helper.sourceInfo.SourceColumns)+len(helper.excludedSourceInfo.SourceColumns))

	// Start with evalExprs, which will contain all the RHS expressions
	// of UPDATE SET clauses. Again, evalExprs will contain one entry
	// per column in updateCols and untupledExprs.
	helper.evalExprs = make([]tree.TypedExpr, len(untupledExprs))
	for i, expr := range untupledExprs {
		// Analyze the expression.

		// We require the type from the column descriptor.
		desiredType := updateCols[i].Type.ToDatumType()

		// Resolve names, type and normalize.
		normExpr, err := p.analyzeExpr(
			ctx, expr, sources, ivarHelper, desiredType, true /* requireType */, "ON CONFLICT")
		if err != nil {
			return nil, err
		}

		// Make sure there are no aggregation/window functions in the
		// expression (after subqueries have been expanded).
		if err := p.txCtx.AssertNoAggregationOrWindowing(
			normExpr, "ON CONFLICT", p.SessionData().SearchPath,
		); err != nil {
			return nil, err
		}

		helper.evalExprs[i] = normExpr
	}

	// If there's a conflict WHERE clause, analyze it.
	if whereClause != nil {
		// Resolve names, type and normalize.
		whereExpr, err := p.analyzeExpr(
			ctx, whereClause.Expr, sources, ivarHelper, types.Bool, true /* requireType */, "WHERE")
		if err != nil {
			return nil, err
		}

		// Make sure there are no aggregation/window functions in the filter
		// (after subqueries have been expanded).
		if err := p.txCtx.AssertNoAggregationOrWindowing(
			whereExpr, "ON CONFLICT...WHERE", p.SessionData().SearchPath,
		); err != nil {
			return nil, err
		}

		helper.whereExpr = whereExpr
	}

	// To (re-)compute computed columns, we use a 1-row buffer with an
	// IVarContainer interface (a rowIndexedVarContainer).
	//
	// This will use the layout from the table columns. The mapping from
	// column IDs to row datum positions is straightforward.
	helper.ccIvarContainer = rowIndexedVarContainer{
		cols:    tableDesc.Columns,
		mapping: sqlbase.ColIDtoRowIndexFromCols(tableDesc.Columns),
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
func (uh *upsertHelper) eval(insertRow, existingRow, resultRow tree.Datums) (tree.Datums, error) {
	// Buffer the rows.
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	// Evaluate the update expressions.
	uh.p.EvalContext().PushIVarContainer(uh)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
	for _, evalExpr := range uh.evalExprs {
		res, err := evalExpr.Eval(uh.p.EvalContext())
		if err != nil {
			return nil, err
		}
		resultRow = append(resultRow, res)
	}

	return resultRow, nil
}

// evalComputedCols handles after we've figured out the values of all regular
// columns, what the values of any incoming computed columns should be.
// It then appends those new values to the end of a given slice.
func (uh *upsertHelper) evalComputedCols(
	updatedRow tree.Datums, appendTo tree.Datums,
) (tree.Datums, error) {
	// Buffer the row.
	uh.ccIvarContainer.curSourceRow = updatedRow

	// Evaluate the computed columns.
	uh.p.EvalContext().PushIVarContainer(&uh.ccIvarContainer)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
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
	// Buffer the rows.
	uh.curSourceRow = existingRow
	uh.curExcludedRow = insertRow

	// Evaluate the predicate.
	uh.p.EvalContext().PushIVarContainer(uh)
	defer func() { uh.p.EvalContext().PopIVarContainer() }()
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
		// Construct a fake set of UPDATE SET expressions. This enables sharing
		// the same implementation for UPSERT and INSERT ... ON CONFLICT DO UPDATE.

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
				// Computed columns must not appear in the pseudo-SET
				// expression.
				continue
			}
			if _, ok := indexColSet[c.ID]; ok {
				// If the column is part of the PK, there is no need for a
				// pseudo-assignment.
				continue
			}
			n := tree.Name(c.Name)
			expr := tree.NewColumnItem(&upsertExcludedTable, n)
			updateExprs = append(updateExprs, &tree.UpdateExpr{Names: tree.NameList{n}, Expr: expr})
		}
		return updateExprs, conflictIndex, nil
	}

	// General case: INSERT with an ON CONFLICT clause.

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
