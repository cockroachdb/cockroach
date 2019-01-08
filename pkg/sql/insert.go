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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
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

type insertNode struct {
	source planNode

	// columns is set if this INSERT is returning any rows, to be
	// consumed by a renderNode upstream. This occurs when there is a
	// RETURNING clause with some scalar expressions.
	columns sqlbase.ResultColumns

	run insertRun
}

// insertNode implements the autoCommitNode interface.
var _ autoCommitNode = &insertNode{}

// Insert inserts rows into the database.
// Privileges: INSERT on table. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
//   Notes: postgres requires INSERT. No "on duplicate key update" option.
//          mysql requires INSERT. Also requires UPDATE on "ON DUPLICATE KEY UPDATE".
func (p *planner) Insert(
	ctx context.Context, n *tree.Insert, desiredTypes []types.T,
) (result planNode, resultErr error) {
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

	// INSERT INTO xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias, err := p.getAliasedTableName(n.Table)
	if err != nil {
		return nil, err
	}

	// Find which table we're working on, check the permissions.
	desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, desc, privilege.INSERT); err != nil {
		return nil, err
	}
	if n.OnConflict != nil {
		// UPSERT and INDEX ON CONFLICT will read from the table to check for duplicates.
		if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
			return nil, err
		}
		if !n.OnConflict.DoNothing {
			// UPSERT and INDEX ON CONFLICT DO UPDATE may modify rows.
			if err := p.CheckPrivilege(ctx, desc, privilege.UPDATE); err != nil {
				return nil, err
			}
		}
	}

	// Determine what are the foreign key tables that are involved in the update.
	var fkCheckType row.FKCheck
	if n.OnConflict == nil || n.OnConflict.DoNothing {
		fkCheckType = row.CheckInserts
	} else {
		fkCheckType = row.CheckUpdates
	}
	fkTables, err := row.TablesNeededForFKs(
		ctx,
		desc,
		fkCheckType,
		p.LookupTableByID,
		p.CheckPrivilege,
		p.analyzeExpr,
	)
	if err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	var insertCols []sqlbase.ColumnDescriptor
	if n.DefaultValues() {
		// No target column, select all columns in the table, including
		// hidden columns; these may have defaults too.
		//
		// Although this races with the backfill in case of UPSERT we do
		// not care: the backfill is also inserting defaults, and we do
		// not provide guarantees about the evaluation order of default
		// expressions.
		insertCols = desc.WritableColumns()
	} else {
		var err error
		if insertCols, err = p.processColumns(desc, n.Columns,
			true /* ensureColumns */, false /* allowMutations */); err != nil {
			return nil, err
		}
	}

	// maxInsertIdx is the highest column index we are allowed to insert into -
	// in the presence of computed columns, when we don't explicitly specify the
	// columns we're inserting into, we should allow inserts if and only if they
	// don't touch a computed column, and we only have the ordinal positions to
	// go by.
	maxInsertIdx := len(insertCols)
	for i, col := range insertCols {
		if col.IsComputed() {
			maxInsertIdx = i
			break
		}
	}

	// Number of columns expecting an input. This doesn't include the
	// columns receiving a default value, or computed columns.
	numInputColumns := len(insertCols)

	// We update the set of columns being inserted into with any computed columns.
	insertCols, computedCols, computeExprs, err :=
		sqlbase.ProcessComputedColumns(ctx, insertCols, tn, desc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// We update the set of columns being inserted into with any default values
	// for columns. This needs to happen after we process the computed columns,
	// because `defaultExprs` is expected to line up with the final set of
	// columns being inserted into.
	insertCols, defaultExprs, err :=
		sqlbase.ProcessDefaultColumns(insertCols, desc, &p.txCtx, p.EvalContext())
	if err != nil {
		return nil, err
	}

	// Now create the source data plan. For this we need an AST and as
	// list of required types. The AST comes from the Rows operand, the
	// required types from the inserted columns.

	// Analyze the expressions for column information and typing.
	requiredTypesFromSelect := make([]types.T, len(insertCols))
	for i, col := range insertCols {
		requiredTypesFromSelect[i] = col.Type.ToDatumType()
	}

	// Extract the AST for the data source.
	isUpsert := n.OnConflict.IsUpsertAlias()
	var insertRows tree.SelectStatement
	arityChecked := false
	colNames := make(tree.NameList, len(insertCols))
	for i := range insertCols {
		colNames[i] = tree.Name(insertCols[i].Name)
	}
	if n.DefaultValues() {
		insertRows = newDefaultValuesClause(defaultExprs, colNames)
	} else {
		src, values, err := extractInsertSource(colNames, n.Rows)
		if err != nil {
			return nil, err
		}
		if values != nil {
			if len(values.Rows) > 0 {
				// Check to make sure the values clause doesn't have too many or
				// too few expressions in each tuple.
				numExprs := len(values.Rows[0])
				if err := checkNumExprs(isUpsert, numExprs, numInputColumns, n.Columns != nil); err != nil {
					return nil, err
				}
				if numExprs > maxInsertIdx {
					// TODO(justin): this is too restrictive. It should
					// be possible to allow INSERT INTO (x) VALUES (DEFAULT)
					// if x is a computed column. See #22434.
					return nil, sqlbase.CannotWriteToComputedColError(insertCols[maxInsertIdx].Name)
				}
				arityChecked = true
			}
			src, err = fillDefaults(defaultExprs, insertCols, values)
			if err != nil {
				return nil, err
			}
		}
		insertRows = src
	}

	// Ready to create the plan for the data source; do it.
	// This performs type checking on source expressions, collecting
	// types for placeholders in the process.
	rows, err := p.newPlan(ctx, insertRows, requiredTypesFromSelect)
	if err != nil {
		return nil, err
	}

	srcCols := planColumns(rows)
	if !arityChecked {
		// If the insert source was not a VALUES clause, then we have not
		// already verified the arity of the operand is correct.
		// Do it now.
		numExprs := len(srcCols)
		if err := checkNumExprs(isUpsert, numExprs, numInputColumns, n.Columns != nil); err != nil {
			return nil, err
		}
		if numExprs > maxInsertIdx {
			return nil, sqlbase.CannotWriteToComputedColError(insertCols[maxInsertIdx].Name)
		}
	}

	// The required types may not have been matched exactly by the planning.
	// While this may be OK if the results were geared toward a client,
	// for INSERT/UPSERT we must have a direct match.
	for i, srcCol := range srcCols {
		if err := sqlbase.CheckDatumTypeFitsColumnType(
			insertCols[i], srcCol.Typ, &p.semaCtx.Placeholders); err != nil {
			return nil, err
		}
	}

	// Create the table insert, which does the bulk of the work.
	ri, err := row.MakeInserter(p.txn, desc, fkTables, insertCols,
		row.CheckFKs, &p.alloc)
	if err != nil {
		return nil, err
	}

	// rowsNeeded will help determine whether we need to allocate a
	// rowsContainer.
	rowsNeeded := resultsNeeded(n.Returning)

	// Determine the relational type of the generated insert node.
	// If rows are not needed, no columns are returned.
	var columns sqlbase.ResultColumns
	if rowsNeeded {
		columns = sqlbase.ResultColumnsFromColDescs(desc.Columns)
	}

	// At this point, everything is ready for either an insertNode or an upserNode.

	var node batchedPlanNode

	if n.OnConflict != nil {
		// This is an UPSERT, or INSERT ... ON CONFLICT.
		// The upsert path has a separate constructor.
		node, err = p.newUpsertNode(
			ctx, n, desc, ri, tn, alias, rows, rowsNeeded, columns,
			defaultExprs, computeExprs, computedCols, fkTables, desiredTypes)
		if err != nil {
			return nil, err
		}
	} else {
		// Regular path for INSERT.
		in := insertNodePool.Get().(*insertNode)
		*in = insertNode{
			source:  rows,
			columns: columns,
			run: insertRun{
				ti:           tableInserter{ri: ri},
				checkHelper:  fkTables[desc.ID].CheckHelper,
				rowsNeeded:   rowsNeeded,
				computedCols: computedCols,
				computeExprs: computeExprs,
				iVarContainerForComputedCols: sqlbase.RowIndexedVarContainer{
					Cols:    desc.Columns,
					Mapping: ri.InsertColIDtoRowIndex,
				},
				defaultExprs: defaultExprs,
				insertCols:   ri.InsertCols,
			},
		}
		node = in
	}

	// Finally, handle RETURNING, if any.
	r, err := p.Returning(ctx, node, n.Returning, desiredTypes, alias)
	if err != nil {
		// We close explicitly here to release the node to the pool.
		node.Close(ctx)
	}
	return r, err
}

// insertRun contains the run-time state of insertNode during local execution.
type insertRun struct {
	ti          tableInserter
	checkHelper *sqlbase.CheckHelper
	rowsNeeded  bool

	// insertCols are the columns being inserted into.
	insertCols []sqlbase.ColumnDescriptor

	// defaultExprs are the expressions used to generate default values.
	defaultExprs []tree.TypedExpr

	// computedCols are the columns that need to be (re-)computed as
	// the result of updating some of the columns in updateCols.
	computedCols []sqlbase.ColumnDescriptor
	// computeExprs are the expressions to evaluate to re-compute the
	// columns in computedCols.
	computeExprs []tree.TypedExpr
	// iVarContainerForComputedCols is used as a temporary buffer that
	// holds the updated values for every column in the source, to
	// serve as input for indexed vars contained in the computeExprs.
	iVarContainerForComputedCols sqlbase.RowIndexedVarContainer

	// rowCount is the number of rows in the current batch.
	rowCount int

	// done informs a new call to BatchedNext() that the previous call to
	// BatchedNext() has completed the work already.
	done bool

	// rows contains the accumulated result rows if rowsNeeded is set.
	rows *sqlbase.RowContainer

	// resultRowBuffer is used to prepare a result row for accumulation
	// into the row container above, when rowsNeeded is set.
	resultRowBuffer tree.Datums

	// rowIdxToRetIdx is the mapping from the ordering of rows in
	// insertCols to the ordering in the result rows, used when
	// rowsNeeded is set to populate resultRowBuffer and the row
	// container. The return index is -1 if the column for the row
	// index is not public.
	rowIdxToRetIdx []int

	// autoCommit indicates whether the last KV batch processed by
	// this update will also commit the KV txn.
	autoCommit autoCommitOpt

	// traceKV caches the current KV tracing flag.
	traceKV bool
}

// maxInsertBatchSize is the max number of entries in the KV batch for
// the insert operation (including secondary index updates, FK
// cascading updates, etc), before the current KV batch is executed
// and a new batch is started.
const maxInsertBatchSize = 10000

func (n *insertNode) startExec(params runParams) error {
	if err := params.p.maybeSetSystemConfig(n.run.ti.tableDesc().GetID()); err != nil {
		return err
	}

	// cache traceKV during execution, to avoid re-evaluating it for every row.
	n.run.traceKV = params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()

	if n.run.rowsNeeded {
		n.run.rows = sqlbase.NewRowContainer(
			params.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(n.columns), 0)

		// In some cases (e.g. `INSERT INTO t (a) ...`) the data source
		// does not provide all the table columns. However we do need to
		// produce result rows that contain values for all the table
		// columns, in the correct order.  This will be done by
		// re-ordering the data into resultRowBuffer.
		//
		// Also we need to re-order the values in the source, ordered by
		// insertCols, when writing them to resultRowBuffer, ordered by
		// n.columns. This uses the rowIdxToRetIdx mapping.

		n.run.resultRowBuffer = make(tree.Datums, len(n.columns))
		for i := range n.run.resultRowBuffer {
			n.run.resultRowBuffer[i] = tree.DNull
		}

		colIDToRetIndex := make(map[sqlbase.ColumnID]int)
		for i, col := range n.run.ti.tableDesc().Columns {
			colIDToRetIndex[col.ID] = i
		}

		n.run.rowIdxToRetIdx = make([]int, len(n.run.insertCols))
		for i, col := range n.run.insertCols {
			if idx, ok := colIDToRetIndex[col.ID]; !ok {
				// Column must be write only and not public.
				n.run.rowIdxToRetIdx[i] = -1
			} else {
				n.run.rowIdxToRetIdx[i] = idx
			}
		}
	}

	return n.run.ti.init(params.p.txn, params.EvalContext())
}

// Next is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *insertNode) Next(params runParams) (bool, error) { panic("not valid") }

// Values is required because batchedPlanNode inherits from planNode, but
// batchedPlanNode doesn't really provide it. See the explanatory comments
// in plan_batch.go.
func (n *insertNode) Values() tree.Datums { panic("not valid") }

// BatchedNext implements the batchedPlanNode interface.
func (n *insertNode) BatchedNext(params runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}

	tracing.AnnotateTrace()

	// Advance one batch. First, clear the current batch.
	n.run.rowCount = 0
	if n.run.rows != nil {
		n.run.rows.Clear(params.ctx)
	}

	// Now consume/accumulate the rows for this batch.
	lastBatch := false
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		// Advance one individual row.
		if next, err := n.source.Next(params); !next {
			lastBatch = true
			if err != nil {
				return false, err
			}
			break
		}

		// Process the insertion for the current source row, potentially
		// accumulating the result row for later.
		if err := n.processSourceRow(params, n.source.Values()); err != nil {
			return false, err
		}

		n.run.rowCount++

		// Are we done yet with the current batch?
		if n.run.ti.curBatchSize() >= maxInsertBatchSize {
			break
		}
	}

	if n.run.rowCount > 0 {
		if err := n.run.ti.atBatchEnd(params.ctx, n.run.traceKV); err != nil {
			return false, err
		}

		if !lastBatch {
			// We only run/commit the batch if there were some rows processed
			// in this batch.
			if err := n.run.ti.flushAndStartNewBatch(params.ctx); err != nil {
				return false, err
			}
		}
	}

	if lastBatch {
		if _, err := n.run.ti.finalize(params.ctx, n.run.autoCommit, n.run.traceKV); err != nil {
			return false, err
		}
		// Remember we're done for the next call to BatchedNext().
		n.run.done = true
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		&params.EvalContext().Settings.SV,
		n.run.ti.tableDesc().ID,
		n.run.rowCount,
	)

	return n.run.rowCount > 0, nil
}

// processSourceRow processes one row from the source for insertion and, if
// result rows are needed, saves it in the result row container.
func (n *insertNode) processSourceRow(params runParams, sourceVals tree.Datums) error {
	// Process the incoming row tuple and generate the full inserted
	// row. This fills in the defaults, computes computed columns, and
	// check the data width complies with the schema constraints.
	rowVals, err := GenerateInsertRow(
		n.run.defaultExprs,
		n.run.computeExprs,
		n.run.insertCols,
		n.run.computedCols,
		*params.EvalContext(),
		n.run.ti.tableDesc(),
		sourceVals,
		&n.run.iVarContainerForComputedCols,
	)
	if err != nil {
		return err
	}

	// Run the CHECK constraints, if any.
	if len(n.run.checkHelper.Exprs) > 0 {
		if err := n.run.checkHelper.LoadRow(n.run.ti.ri.InsertColIDtoRowIndex, rowVals, false); err != nil {
			return err
		}
		if err := n.run.checkHelper.Check(params.EvalContext()); err != nil {
			return err
		}
	}

	// Queue the insert in the KV batch.
	if err = n.run.ti.row(params.ctx, rowVals, n.run.traceKV); err != nil {
		return err
	}

	// If result rows need to be accumulated, do it.
	if n.run.rows != nil {
		for i, val := range rowVals {
			// The downstream consumer will want the rows in the order of
			// the table descriptor, not that of insertCols. Reorder them
			// and ignore non-public columns.
			if idx := n.run.rowIdxToRetIdx[i]; idx >= 0 {
				n.run.resultRowBuffer[idx] = val
			}
		}
		if _, err := n.run.rows.AddRow(params.ctx, n.run.resultRowBuffer); err != nil {
			return err
		}
	}

	return nil
}

// BatchedCount implements the batchedPlanNode interface.
func (n *insertNode) BatchedCount() int { return n.run.rowCount }

// BatchedCount implements the batchedPlanNode interface.
func (n *insertNode) BatchedValues(rowIdx int) tree.Datums { return n.run.rows.At(rowIdx) }

func (n *insertNode) Close(ctx context.Context) {
	n.source.Close(ctx)
	n.run.ti.close(ctx)
	if n.run.rows != nil {
		n.run.rows.Close(ctx)
	}
	*n = insertNode{}
	insertNodePool.Put(n)
}

// enableAutoCommit is part of the autoCommitNode interface.
func (n *insertNode) enableAutoCommit() {
	n.run.autoCommit = autoCommitEnabled
}

// GenerateInsertRow prepares a row tuple for insertion. It fills in default
// expressions, verifies non-nullable columns, and checks column widths.
//
// The result is a row tuple providing values for every column in insertCols.
// This results contains:
//
// - the values provided by rowVals, the tuple of source values. The
//   caller ensures this provides values 1-to-1 to the prefix of
//   insertCols that was specified explicitly in the INSERT statement.
// - the default values for any additional columns in insertCols that
//   have default values in defaultExprs.
// - the computed values for any additional columns in insertCols
//   that are computed. The mapping in rowContainerForComputedCols
//   maps the indexes of the comptuedCols/computeExpr slices
//   back into indexes in the result row tuple.
//
func GenerateInsertRow(
	defaultExprs []tree.TypedExpr,
	computeExprs []tree.TypedExpr,
	insertCols []sqlbase.ColumnDescriptor,
	computedCols []sqlbase.ColumnDescriptor,
	evalCtx tree.EvalContext,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	rowVals tree.Datums,
	rowContainerForComputedVals *sqlbase.RowIndexedVarContainer,
) (tree.Datums, error) {
	// The values for the row may be shorter than the number of columns being
	// inserted into. Generate default values for those columns using the
	// default expressions. This will not happen if the row tuple was produced
	// by a ValuesClause, because all default expressions will have been populated
	// already by fillDefaults.
	if len(rowVals) < len(insertCols) {
		// It's not cool to append to the slice returned by a node; make a copy.
		oldVals := rowVals
		rowVals = make(tree.Datums, len(insertCols))
		copy(rowVals, oldVals)

		for i := len(oldVals); i < len(insertCols); i++ {
			if defaultExprs == nil {
				rowVals[i] = tree.DNull
				continue
			}
			d, err := defaultExprs[i].Eval(&evalCtx)
			if err != nil {
				return nil, err
			}
			rowVals[i] = d
		}
	}

	// Generate the computed values, if needed.
	if len(computeExprs) > 0 {
		rowContainerForComputedVals.CurSourceRow = rowVals
		evalCtx.PushIVarContainer(rowContainerForComputedVals)
		for i := range computedCols {
			// Note that even though the row is not fully constructed at this point,
			// since we disallow computed columns from referencing other computed
			// columns, all the columns which could possibly be referenced *are*
			// available.
			d, err := computeExprs[i].Eval(&evalCtx)
			if err != nil {
				return nil, errors.Wrapf(err,
					"computed column %s", tree.ErrString((*tree.Name)(&computedCols[i].Name)))
			}
			rowVals[rowContainerForComputedVals.Mapping[computedCols[i].ID]] = d
		}
		evalCtx.PopIVarContainer()
	}

	// Check to see if NULL is being inserted into any non-nullable column.
	for _, col := range tableDesc.WritableColumns() {
		if !col.Nullable {
			if i, ok := rowContainerForComputedVals.Mapping[col.ID]; !ok || rowVals[i] == tree.DNull {
				return nil, sqlbase.NewNonNullViolationError(col.Name)
			}
		}
	}

	// Ensure that the values honor the specified column widths.
	for i := 0; i < len(insertCols); i++ {
		outVal, err := sqlbase.LimitValueWidth(insertCols[i].Type, rowVals[i], &insertCols[i].Name)
		if err != nil {
			return nil, err
		}
		rowVals[i] = outVal
	}
	return rowVals, nil
}

// processColumns returns the column descriptors identified by the
// given name list. It also checks that a given column name is only
// listed once. If no column names are given (special case for INSERT)
// and ensureColumns is set, the descriptors for all visible columns
// are returned. If allowMutations is set, even columns undergoing
// mutations are added.
func (p *planner) processColumns(
	tableDesc *sqlbase.ImmutableTableDescriptor,
	nameList tree.NameList,
	ensureColumns, allowMutations bool,
) ([]sqlbase.ColumnDescriptor, error) {
	if len(nameList) == 0 {
		if ensureColumns {
			// VisibleColumns is used here to prevent INSERT INTO <table> VALUES (...)
			// (as opposed to INSERT INTO <table> (...) VALUES (...)) from writing
			// hidden columns. At present, the only hidden column is the implicit rowid
			// primary key column.
			return tableDesc.VisibleColumns(), nil
		}
		return nil, nil
	}

	cols := make([]sqlbase.ColumnDescriptor, len(nameList))
	colIDSet := make(map[sqlbase.ColumnID]struct{}, len(nameList))
	for i, colName := range nameList {
		var col sqlbase.ColumnDescriptor
		var err error
		if allowMutations {
			col, _, err = tableDesc.FindColumnByName(colName)
		} else {
			col, err = tableDesc.FindActiveColumnByName(string(colName))
		}
		if err != nil {
			return nil, err
		}

		if _, ok := colIDSet[col.ID]; ok {
			return nil, fmt.Errorf("multiple assignments to the same column %q", &nameList[i])
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = col
	}

	return cols, nil
}

// extractInsertSource removes the parentheses around the data source of an INSERT statement.
// If the data source is a VALUES clause not further qualified with LIMIT/OFFSET and ORDER BY,
// the 3rd return value is a pre-casted pointer to the VALUES clause.
func extractInsertSource(
	colNames tree.NameList, s *tree.Select,
) (tree.SelectStatement, *tree.ValuesClauseWithNames, error) {
	wrapped := s.Select
	limit := s.Limit
	orderBy := s.OrderBy
	with := s.With

	// Be careful to not unwrap expressions with a WITH clause. These
	// need to be handled generically.
	for s, ok := wrapped.(*tree.ParenSelect); ok; s, ok = wrapped.(*tree.ParenSelect) {
		if s.Select.With != nil {
			if with != nil {
				// (WITH ... (WITH ...))
				// Currently we are unable to nest the scopes inside ParenSelect so we
				// must refuse the syntax so that the query does not get invalid results.
				return nil, nil, pgerror.UnimplementedWithIssueError(24303,
					"multiple WITH clauses in parentheses")
			}
			with = s.Select.With
		}

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

	if with == nil && orderBy == nil && limit == nil {
		values, _ := wrapped.(*tree.ValuesClause)
		if values != nil {
			return wrapped, &tree.ValuesClauseWithNames{ValuesClause: *values, Names: colNames}, nil
		}
		return wrapped, nil, nil
	}
	return &tree.ParenSelect{
		Select: &tree.Select{Select: wrapped, OrderBy: orderBy, Limit: limit, With: with},
	}, nil, nil
}

func newDefaultValuesClause(
	defaultExprs []tree.TypedExpr, colNames tree.NameList,
) tree.SelectStatement {
	row := make(tree.Exprs, 0, len(colNames))
	for i := range colNames {
		if defaultExprs == nil {
			row = append(row, tree.DNull)
			continue
		}
		row = append(row, defaultExprs[i])
	}
	return &tree.ValuesClauseWithNames{
		ValuesClause: tree.ValuesClause{Rows: []tree.Exprs{row}},
		Names:        colNames,
	}
}

// fillDefaults populates default expressions in the provided ValuesClause,
// returning a new ValuesClause with expressions for all columns in cols. Each
// default value in the Tuples will be replaced with either the corresponding
// column's default expressions if one exists, or NULL if one does not. There
// are two parts of a Tuple that fillDefaults will populate:
// - DefaultVal exprs (`VALUES (1, 2, DEFAULT)`) will be replaced by their
//   column's default expression (or NULL).
// - If tuples contain fewer elements than the number of columns, the missing
//   columns will be added with their default expressions (or NULL).
//
// The function returns a ValuesClause with defaults filled or an error.
func fillDefaults(
	defaultExprs []tree.TypedExpr,
	cols []sqlbase.ColumnDescriptor,
	values *tree.ValuesClauseWithNames,
) (*tree.ValuesClauseWithNames, error) {
	ret := values
	copyValues := func() {
		if ret == values {
			ret = &tree.ValuesClauseWithNames{ValuesClause: tree.ValuesClause{Rows: append([]tree.Exprs(nil), values.Rows...)}, Names: values.Names}
		}
	}

	defaultExpr := func(idx int) tree.Expr {
		if defaultExprs == nil || idx >= len(defaultExprs) {
			// The case where idx is too large for defaultExprs will be
			// transformed into an error by the check on the number of
			// columns in Insert().
			return tree.DNull
		}
		return defaultExprs[idx]
	}

	numColsOrig := len(ret.Rows[0])
	for tIdx, tuple := range ret.Rows {
		if a, e := len(tuple), numColsOrig; a != e {
			return nil, newValuesListLenErr(e, a)
		}

		tupleCopied := false
		copyTuple := func() {
			if !tupleCopied {
				copyValues()
				tuple = append(tree.Exprs(nil), tuple...)
				ret.Rows[tIdx] = tuple
				tupleCopied = true
			}
		}

		for eIdx, val := range tuple {
			switch val.(type) {
			case tree.DefaultVal:
				copyTuple()
				tuple[eIdx] = defaultExpr(eIdx)
			}
		}

		// The values for the row may be shorter than the number of columns being
		// inserted into. Populate default expressions for those columns.
		for i := len(tuple); i < len(cols); i++ {
			copyTuple()
			tuple = append(tuple, defaultExpr(len(tuple)))
			ret.Rows[tIdx] = tuple
		}
	}
	for i := numColsOrig; i < len(cols); i++ {
		ret.Names = append(ret.Names, tree.Name(cols[i].Name))
	}
	return ret, nil
}

func checkNumExprs(isUpsert bool, numExprs, numCols int, specifiedTargets bool) error {
	// It is ok to be missing exprs if !specifiedTargets, because the missing
	// columns will be filled in by DEFAULT expressions.
	extraExprs := numExprs > numCols
	missingExprs := specifiedTargets && numExprs < numCols
	if extraExprs || missingExprs {
		more, less := "expressions", "target columns"
		if missingExprs {
			more, less = less, more
		}
		kw := "INSERT"
		if isUpsert {
			kw = "UPSERT"
		}
		return pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"%s has more %s than %s, %d expressions for %d targets",
			kw, more, less, numExprs, numCols)
	}
	return nil
}
