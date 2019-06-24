// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// mutationBuilder is a helper struct that supports building Insert, Update,
// Upsert, and Delete operators in stages.
// TODO(andyk): Add support for Delete.
type mutationBuilder struct {
	b  *Builder
	md *opt.Metadata

	// op is InsertOp, UpdateOp, UpsertOp, or DeleteOp.
	op opt.Operator

	// tab is the target table.
	tab cat.Table

	// tabID is the metadata ID of the table.
	tabID opt.TableID

	// alias is the table alias specified in the mutation statement, or just the
	// resolved table name if no alias was specified.
	alias tree.TableName

	// outScope contains the current set of columns that are in scope, as well as
	// the output expression as it is incrementally built. Once the final mutation
	// expression is completed, it will be contained in outScope.expr. Columns,
	// when present, are arranged in this order:
	//
	//   +--------+-------+--------+--------+-------+
	//   | Insert | Fetch | Update | Upsert | Check |
	//   +--------+-------+--------+--------+-------+
	//
	// Each column is identified by its ordinal position in outScope, and those
	// ordinals are stored in the corresponding ScopeOrds fields (see below).
	outScope *scope

	// targetColList is an ordered list of IDs of the table columns into which
	// values will be inserted, or which will be updated with new values. It is
	// incrementally built as the mutation operator is built.
	targetColList opt.ColList

	// targetColSet contains the same column IDs as targetColList, but as a set.
	targetColSet opt.ColSet

	// insertOrds lists the outScope columns providing values to insert. Its
	// length is always equal to the number of columns in the target table,
	// including mutation columns. Table columns which will not have values
	// inserted are set to -1 (e.g. delete-only mutation columns). insertOrds
	// is empty if this is not an Insert/Upsert operator.
	insertOrds []scopeOrdinal

	// fetchOrds lists the outScope columns storing values which are fetched
	// from the target table in order to provide existing values that will form
	// lookup and update values. Its length is always equal to the number of
	// columns in the target table, including mutation columns. Table columns
	// which do not need to be fetched are set to -1. fetchOrds is empty if
	// this is an Insert operator.
	fetchOrds []scopeOrdinal

	// updateOrds lists the outScope columns providing update values. Its length
	// is always equal to the number of columns in the target table, including
	// mutation columns. Table columns which do not need to be updated are set
	// to -1.
	updateOrds []scopeOrdinal

	// upsertOrds lists the outScope columns that choose between an insert or
	// update column using a CASE expression:
	//
	//   CASE WHEN canary_col IS NULL THEN ins_col ELSE upd_col END
	//
	// These columns are used to compute constraints and to return result rows.
	// The length of upsertOrds is always equal to the number of columns in
	// the target table, including mutation columns. Table columns which do not
	// need to be updated are set to -1. upsertOrds is empty if this is not
	// an Upsert operator.
	upsertOrds []scopeOrdinal

	// checkOrds lists the outScope columns storing the boolean results of
	// evaluating check constraint expressions defined on the target table. Its
	// length is always equal to the number of check constraints on the table
	// (see opt.Table.CheckCount).
	checkOrds []scopeOrdinal

	// canaryColID is the ID of the column that is used to decide whether to
	// insert or update each row. If the canary column's value is null, then it's
	// an insert; otherwise it's an update.
	canaryColID opt.ColumnID

	// subqueries temporarily stores subqueries that were built during initial
	// analysis of SET expressions. They will be used later when the subqueries
	// are joined into larger LEFT OUTER JOIN expressions.
	subqueries []*scope

	// parsedExprs is a cached set of parsed default and computed expressions
	// from the table schema. These are parsed once and cached for reuse.
	parsedExprs []tree.Expr

	// checks contains foreign key check queries; see buildFKChecks.
	checks memo.FKChecksExpr
}

func (mb *mutationBuilder) init(b *Builder, op opt.Operator, tab cat.Table, alias tree.TableName) {
	mb.b = b
	mb.md = b.factory.Metadata()
	mb.op = op
	mb.tab = tab
	mb.alias = alias
	mb.targetColList = make(opt.ColList, 0, tab.DeletableColumnCount())

	// Allocate segmented array of scope column ordinals.
	n := tab.DeletableColumnCount()
	scopeOrds := make([]scopeOrdinal, n*4+tab.CheckCount())
	for i := range scopeOrds {
		scopeOrds[i] = -1
	}
	mb.insertOrds = scopeOrds[:n]
	mb.fetchOrds = scopeOrds[n : n*2]
	mb.updateOrds = scopeOrds[n*2 : n*3]
	mb.upsertOrds = scopeOrds[n*3 : n*4]
	mb.checkOrds = scopeOrds[n*4:]

	// Add the table and its columns (including mutation columns) to metadata.
	mb.tabID = mb.md.AddTableWithAlias(tab, &mb.alias)
}

// scopeOrdToColID returns the ID of the given scope column. If no scope column
// is defined, scopeOrdToColID returns 0.
func (mb *mutationBuilder) scopeOrdToColID(ord scopeOrdinal) opt.ColumnID {
	if ord == -1 {
		return 0
	}
	return mb.outScope.cols[ord].id
}

// insertColID is a convenience method that returns the ID of the input column
// that provides the insertion value for the given table column (specified by
// ordinal position in the table).
func (mb *mutationBuilder) insertColID(tabOrd int) opt.ColumnID {
	return mb.scopeOrdToColID(mb.insertOrds[tabOrd])
}

// buildInputForUpdateOrDelete constructs a Select expression from the fields in
// the Update or Delete operator, similar to this:
//
//   SELECT <cols>
//   FROM <table>
//   WHERE <where>
//   ORDER BY <order-by>
//   LIMIT <limit>
//
// All columns from the table to update are added to fetchColList.
// TODO(andyk): Do needed column analysis to project fewer columns if possible.
func (mb *mutationBuilder) buildInputForUpdateOrDelete(
	inScope *scope, where *tree.Where, limit *tree.Limit, orderBy tree.OrderBy,
) {
	// Fetch columns from different instance of the table metadata, so that it's
	// possible to remap columns, as in this example:
	//
	//   UPDATE abc SET a=b
	//

	// FROM
	mb.outScope = mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		nil, /* ordinals */
		nil, /* indexFlags */
		includeMutations,
		inScope,
	)

	// WHERE
	mb.b.buildWhere(where, mb.outScope)

	// SELECT + ORDER BY (which may add projected expressions)
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)
	orderByScope := mb.b.analyzeOrderBy(orderBy, mb.outScope, projectionsScope)
	mb.b.buildOrderBy(mb.outScope, projectionsScope, orderByScope)
	mb.b.constructProjectForScope(mb.outScope, projectionsScope)

	// LIMIT
	if limit != nil {
		mb.b.buildLimit(limit, inScope, projectionsScope)
	}

	mb.outScope = projectionsScope

	// Set list of columns that will be fetched by the input expression.
	for i := range mb.outScope.cols {
		mb.fetchOrds[i] = scopeOrdinal(i)
	}
}

// addTargetColsByName adds one target column for each of the names in the given
// list.
func (mb *mutationBuilder) addTargetColsByName(names tree.NameList) {
	for _, name := range names {
		// Determine the ordinal position of the named column in the table and
		// add it as a target column.
		if ord := cat.FindTableColumnByName(mb.tab, name); ord != -1 {
			mb.addTargetCol(ord)
			continue
		}
		panic(builderError{sqlbase.NewUndefinedColumnError(string(name))})
	}
}

// addTargetCol adds a target column by its ordinal position in the target
// table. It raises an error if a mutation or computed column is targeted, or if
// the same column is targeted multiple times.
func (mb *mutationBuilder) addTargetCol(ord int) {
	tabCol := mb.tab.Column(ord)

	// Don't allow targeting of mutation columns.
	if cat.IsMutationColumn(mb.tab, ord) {
		panic(builderError{makeBackfillError(tabCol.ColName())})
	}

	// Computed columns cannot be targeted with input values.
	if tabCol.IsComputed() {
		panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
	}

	// Ensure that the name list does not contain duplicates.
	colID := mb.tabID.ColumnID(ord)
	if mb.targetColSet.Contains(colID) {
		panic(pgerror.Newf(pgcode.Syntax,
			"multiple assignments to the same column %q", tabCol.ColName()))
	}
	mb.targetColSet.Add(colID)

	mb.targetColList = append(mb.targetColList, colID)
}

// extractValuesInput tests whether the given input is a VALUES clause with no
// WITH, ORDER BY, or LIMIT modifier. If so, it's returned, otherwise nil is
// returned.
func (mb *mutationBuilder) extractValuesInput(inputRows *tree.Select) *tree.ValuesClause {
	if inputRows == nil {
		return nil
	}

	// Only extract a simple VALUES clause with no modifiers.
	if inputRows.With != nil || inputRows.OrderBy != nil || inputRows.Limit != nil {
		return nil
	}

	// Discard parentheses.
	if parens, ok := inputRows.Select.(*tree.ParenSelect); ok {
		return mb.extractValuesInput(parens.Select)
	}

	if values, ok := inputRows.Select.(*tree.ValuesClause); ok {
		return values
	}

	return nil
}

// replaceDefaultExprs looks for DEFAULT specifiers in input value expressions
// and replaces them with the corresponding default value expression for the
// corresponding column. This is only possible when the input is a VALUES
// clause. For example:
//
//   INSERT INTO t (a, b) (VALUES (1, DEFAULT), (DEFAULT, 2))
//
// Here, the two DEFAULT specifiers are replaced by the default value expression
// for the a and b columns, respectively.
//
// replaceDefaultExprs returns a VALUES expression with replaced DEFAULT values,
// or just the unchanged input expression if there are no DEFAULT values.
func (mb *mutationBuilder) replaceDefaultExprs(inRows *tree.Select) (outRows *tree.Select) {
	values := mb.extractValuesInput(inRows)
	if values == nil {
		return inRows
	}

	// Ensure that the number of input columns exactly matches the number of
	// target columns.
	numCols := len(values.Rows[0])
	mb.checkNumCols(len(mb.targetColList), numCols)

	var newRows []tree.Exprs
	for irow, tuple := range values.Rows {
		if len(tuple) != numCols {
			reportValuesLenError(numCols, len(tuple))
		}

		// Scan list of tuples in the VALUES row, looking for DEFAULT specifiers.
		var newTuple tree.Exprs
		for itup, val := range tuple {
			if _, ok := val.(tree.DefaultVal); ok {
				// Found DEFAULT, so lazily create new rows and tuple lists.
				if newRows == nil {
					newRows = make([]tree.Exprs, irow, len(values.Rows))
					copy(newRows, values.Rows[:irow])
				}

				if newTuple == nil {
					newTuple = make(tree.Exprs, itup, numCols)
					copy(newTuple, tuple[:itup])
				}

				val = mb.parseDefaultOrComputedExpr(mb.targetColList[itup])
			}
			if newTuple != nil {
				newTuple = append(newTuple, val)
			}
		}

		if newRows != nil {
			if newTuple != nil {
				newRows = append(newRows, newTuple)
			} else {
				newRows = append(newRows, tuple)
			}
		}
	}

	if newRows != nil {
		return &tree.Select{Select: &tree.ValuesClause{Rows: newRows}}
	}
	return inRows
}

// addSynthesizedCols is a helper method for addDefaultAndComputedColsForInsert
// and addComputedColsForUpdate that scans the list of table columns, looking
// for any that do not yet have values provided by the input expression. New
// columns are synthesized for any missing columns, as long as the addCol
// callback function returns true for that column.
func (mb *mutationBuilder) addSynthesizedCols(
	scopeOrds []scopeOrdinal, addCol func(tabCol cat.Column) bool,
) {
	var projectionsScope *scope

	// Skip delete-only mutation columns, since they are ignored by all mutation
	// operators that synthesize columns.
	for i, n := 0, mb.tab.WritableColumnCount(); i < n; i++ {
		// Skip columns that are already specified.
		if scopeOrds[i] != -1 {
			continue
		}

		// Invoke addCol to determine whether column should be added.
		tabCol := mb.tab.Column(i)
		if !addCol(tabCol) {
			continue
		}

		// Construct a new Project operator that will contain the newly synthesized
		// column(s).
		if projectionsScope == nil {
			projectionsScope = mb.outScope.replace()
			projectionsScope.appendColumnsFromScope(mb.outScope)
		}
		tabColID := mb.tabID.ColumnID(i)
		expr := mb.parseDefaultOrComputedExpr(tabColID)
		texpr := mb.outScope.resolveAndRequireType(expr, tabCol.DatumType())
		scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr)
		mb.b.buildScalar(texpr, mb.outScope, projectionsScope, scopeCol, nil)

		// Assign name to synthesized column. Computed columns may refer to default
		// columns in the table by name.
		scopeCol.table = *mb.tab.Name()
		scopeCol.name = tabCol.ColName()

		// Remember ordinal position of the new scope column.
		scopeOrds[i] = scopeOrdinal(len(projectionsScope.cols) - 1)

		// Add corresponding target column.
		mb.targetColList = append(mb.targetColList, tabColID)
		mb.targetColSet.Add(tabColID)
	}

	if projectionsScope != nil {
		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// roundDecimalValues wraps each DECIMAL-related column (including arrays of
// decimals) with a call to the crdb_internal.round_decimal_values function, if
// column values may need to be rounded. This is necessary when mutating table
// columns that have a limited scale (e.g. DECIMAL(10, 1)). Here is the PG docs
// description:
//
//   http://www.postgresql.org/docs/9.5/static/datatype-numeric.html
//   "If the scale of a value to be stored is greater than
//   the declared scale of the column, the system will round the
//   value to the specified number of fractional digits. Then,
//   if the number of digits to the left of the decimal point
//   exceeds the declared precision minus the declared scale, an
//   error is raised."
//
// Note that this function only handles the rounding portion of that. The
// precision check is done by the execution engine. The rounding cannot be done
// there, since it needs to happen before check constraints are computed, and
// before UPSERT joins.
//
// if roundComputedCols is false, then don't wrap computed columns. If true,
// then only wrap computed columns. This is necessary because computed columns
// can depend on other columns mutated by the operation; it is necessary to
// first round those values, then evaluated the computed expression, and then
// round the result of the computation.
func (mb *mutationBuilder) roundDecimalValues(scopeOrds []scopeOrdinal, roundComputedCols bool) {
	var projectionsScope *scope

	for i, ord := range scopeOrds {
		if ord == -1 {
			// Column not mutated, so nothing to do.
			continue
		}

		// Include or exclude computed columns, depending on the value of
		// roundComputedCols.
		col := mb.tab.Column(i)
		if col.IsComputed() != roundComputedCols {
			continue
		}

		// Check whether the target column's type may require rounding of the
		// input value.
		props, overload := findRoundingFunction(col.DatumType(), col.ColTypePrecision())
		if props == nil {
			continue
		}
		private := &memo.FunctionPrivate{
			Name:       "crdb_internal.round_decimal_values",
			Typ:        mb.outScope.cols[ord].typ,
			Properties: props,
			Overload:   overload,
		}
		variable := mb.b.factory.ConstructVariable(mb.scopeOrdToColID(ord))
		scale := mb.b.factory.ConstructConstVal(tree.NewDInt(tree.DInt(col.ColTypeWidth())), types.Int)
		fn := mb.b.factory.ConstructFunction(memo.ScalarListExpr{variable, scale}, private)

		// Lazily create new scope and update the scope column to be rounded.
		if projectionsScope == nil {
			projectionsScope = mb.outScope.replace()
			projectionsScope.appendColumnsFromScope(mb.outScope)
		}
		mb.b.populateSynthesizedColumn(&projectionsScope.cols[ord], fn)
	}

	if projectionsScope != nil {
		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// findRoundingFunction returns the builtin function overload needed to round
// input values. This is only necessary for DECIMAL or DECIMAL[] types that have
// limited precision, such as:
//
//   DECIMAL(15, 1)
//   DECIMAL(10, 3)[]
//
// If an input decimal value has more than the required number of fractional
// digits, it must be rounded before being inserted into these types.
//
// NOTE: CRDB does not allow nested array storage types, so only one level of
// array nesting needs to be checked.
func findRoundingFunction(typ *types.T, precision int) (*tree.FunctionProperties, *tree.Overload) {
	if precision == 0 {
		// Unlimited precision decimal target type never needs rounding.
		return nil, nil
	}

	props, overloads := builtins.GetBuiltinProperties("crdb_internal.round_decimal_values")

	if typ.Equivalent(types.Decimal) {
		return props, &overloads[0]
	}
	if typ.Equivalent(types.DecimalArray) {
		return props, &overloads[1]
	}

	// Not DECIMAL or DECIMAL[].
	return nil, nil
}

// addCheckConstraintCols synthesizes a boolean output column for each check
// constraint defined on the target table. The mutation operator will report
// a constraint violation error if the value of the column is false.
func (mb *mutationBuilder) addCheckConstraintCols() {
	if mb.tab.CheckCount() > 0 {
		// Disambiguate names so that references in the constraint expression refer
		// to the correct columns.
		mb.disambiguateColumns()

		projectionsScope := mb.outScope.replace()
		projectionsScope.appendColumnsFromScope(mb.outScope)

		for i, n := 0, mb.tab.CheckCount(); i < n; i++ {
			expr, err := parser.ParseExpr(string(mb.tab.Check(i).Constraint))
			if err != nil {
				panic(builderError{err})
			}

			alias := fmt.Sprintf("check%d", i+1)
			texpr := mb.outScope.resolveAndRequireType(expr, types.Bool)
			scopeCol := mb.b.addColumn(projectionsScope, alias, texpr)

			// TODO(ridwanmsharif): Maybe we can avoid building constraints here
			// and instead use the constraints stored in the table metadata.
			mb.b.buildScalar(texpr, mb.outScope, projectionsScope, scopeCol, nil)
			mb.checkOrds[i] = scopeOrdinal(len(projectionsScope.cols) - 1)
		}

		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// disambiguateColumns ranges over the scope and ensures that at most one column
// has each table column name, and that name refers to the column with the final
// value that the mutation applies.
func (mb *mutationBuilder) disambiguateColumns() {
	// Determine the set of scope columns that will have their names preserved.
	var preserve util.FastIntSet
	for i, n := 0, mb.tab.DeletableColumnCount(); i < n; i++ {
		scopeOrd := mb.mapToReturnScopeOrd(i)
		if scopeOrd != -1 {
			preserve.Add(int(scopeOrd))
		}
	}

	// Clear names of all non-preserved columns. Set the fully qualified table
	// name of preserved columns, since computed column expressions will reference
	// table names, not alias names.
	for i := range mb.outScope.cols {
		if preserve.Contains(i) {
			mb.outScope.cols[i].table = *mb.tab.Name()
		} else {
			mb.outScope.cols[i].clearName()
		}
	}
}

// makeMutationPrivate builds a MutationPrivate struct containing the table and
// column metadata needed for the mutation operator.
func (mb *mutationBuilder) makeMutationPrivate(needResults bool) *memo.MutationPrivate {
	// Helper function to create a column list in the MutationPrivate.
	makeColList := func(scopeOrds []scopeOrdinal) opt.ColList {
		var colList opt.ColList
		for i := range scopeOrds {
			if scopeOrds[i] != -1 {
				if colList == nil {
					colList = make(opt.ColList, len(scopeOrds))
				}
				colList[i] = mb.scopeOrdToColID(scopeOrds[i])
			}
		}
		return colList
	}

	private := &memo.MutationPrivate{
		Table:      mb.tabID,
		InsertCols: makeColList(mb.insertOrds),
		FetchCols:  makeColList(mb.fetchOrds),
		UpdateCols: makeColList(mb.updateOrds),
		CanaryCol:  mb.canaryColID,
		CheckCols:  makeColList(mb.checkOrds),
	}

	if needResults {
		// Only non-mutation columns are output columns. ReturnCols needs to have
		// DeletableColumnCount entries, but only the first ColumnCount entries
		// can be defined (i.e. >= 0).
		private.ReturnCols = make(opt.ColList, mb.tab.DeletableColumnCount())
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			scopeOrd := mb.mapToReturnScopeOrd(i)
			if scopeOrd == -1 {
				panic(errors.AssertionFailedf("column %d is not available in the mutation input", i))
			}
			private.ReturnCols[i] = mb.outScope.cols[scopeOrd].id
		}
	}

	return private
}

// mapToReturnScopeOrd returns the ordinal of the scope column that provides the
// final value for the column at the given ordinal position in the table. This
// value might mutate the column, or it might be returned by the mutation
// statement, or it might not be used at all. Columns take priority in this
// order:
//
//   upsert, update, fetch, insert
//
// If an upsert column is available, then it already combines an update/fetch
// value with an insert value, so it takes priority. If an update column is
// available, then it overrides any fetch value. Finally, the relative priority
// of fetch and insert columns doesn't matter, since they're only used together
// in the upsert case where an upsert column would be available.
//
// If the column is never referenced by the statement, then mapToReturnScopeOrd
// returns 0. This would be the case for delete-only columns in an Insert
// statement, because they're neither fetched nor mutated.
func (mb *mutationBuilder) mapToReturnScopeOrd(tabOrd int) scopeOrdinal {
	switch {
	case mb.upsertOrds[tabOrd] != -1:
		return mb.upsertOrds[tabOrd]

	case mb.updateOrds[tabOrd] != -1:
		return mb.updateOrds[tabOrd]

	case mb.fetchOrds[tabOrd] != -1:
		return mb.fetchOrds[tabOrd]

	case mb.insertOrds[tabOrd] != -1:
		return mb.insertOrds[tabOrd]

	default:
		// Column is never referenced by the statement.
		return -1
	}
}

// buildReturning wraps the input expression with a Project operator that
// projects the given RETURNING expressions.
func (mb *mutationBuilder) buildReturning(returning tree.ReturningExprs) {
	// Handle case of no RETURNING clause.
	if returning == nil {
		mb.outScope = &scope{builder: mb.b, expr: mb.outScope.expr}
		return
	}

	// Start out by constructing a scope containing one column for each non-
	// mutation column in the target table, in the same order, and with the
	// same names. These columns can be referenced by the RETURNING clause.
	//
	//   1. Project only non-mutation columns.
	//   2. Alias columns to use table column names.
	//   3. Mark hidden columns.
	//   4. Project columns in same order as defined in table schema.
	//
	inScope := mb.outScope.replace()
	inScope.expr = mb.outScope.expr
	inScope.cols = make([]scopeColumn, 0, mb.tab.ColumnCount())
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		tabCol := mb.tab.Column(i)
		inScope.cols = append(inScope.cols, scopeColumn{
			name:   tabCol.ColName(),
			table:  mb.alias,
			typ:    tabCol.DatumType(),
			id:     mb.tabID.ColumnID(i),
			hidden: tabCol.IsHidden(),
		})
	}

	// Construct the Project operator that projects the RETURNING expressions.
	outScope := inScope.replace()
	mb.b.analyzeReturningList(returning, nil /* desiredTypes */, inScope, outScope)
	mb.b.buildProjectionList(inScope, outScope)
	mb.b.constructProjectForScope(inScope, outScope)
	mb.outScope = outScope
}

// checkNumCols raises an error if the expected number of columns does not match
// the actual number of columns.
func (mb *mutationBuilder) checkNumCols(expected, actual int) {
	if actual != expected {
		more, less := "expressions", "target columns"
		if actual < expected {
			more, less = less, more
		}

		var kw string
		if mb.op == opt.InsertOp {
			kw = "INSERT"
		} else {
			kw = "UPSERT"
		}
		panic(pgerror.Newf(pgcode.Syntax,
			"%s has more %s than %s, %d expressions for %d targets",
			kw, more, less, actual, expected))
	}
}

// parseDefaultOrComputedExpr parses the default (including nullable) or
// computed value expression for the given table column, and caches it for
// reuse.
func (mb *mutationBuilder) parseDefaultOrComputedExpr(colID opt.ColumnID) tree.Expr {
	if mb.parsedExprs == nil {
		mb.parsedExprs = make([]tree.Expr, mb.tab.DeletableColumnCount())
	}

	// Return expression from cache, if it was already parsed previously.
	ord := mb.tabID.ColumnOrdinal(colID)
	if mb.parsedExprs[ord] != nil {
		return mb.parsedExprs[ord]
	}

	var exprStr string
	tabCol := mb.tab.Column(ord)
	switch {
	case tabCol.IsComputed():
		exprStr = tabCol.ComputedExprStr()
	case tabCol.HasDefault():
		exprStr = tabCol.DefaultExprStr()
	default:
		return tree.DNull
	}

	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		panic(builderError{err})
	}

	mb.parsedExprs[ord] = expr
	return expr
}

// buildFKChecks populates mb.checks with queries that check the integrity of
// foreign key relations that involve modified rows.
func (mb *mutationBuilder) buildFKChecks() {
	if !mb.b.evalCtx.SessionData.OptimizerFKs {
		return
	}

	// TODO(radu): only insert supported for now.
	if mb.op != opt.InsertOp {
		return
	}

	for i, n := 0, mb.tab.OutboundForeignKeyCount(); i < n; i++ {
		fk := mb.tab.OutboundForeignKey(i)
		numCols := fk.ColumnCount()
		item := memo.FKChecksItem{FKChecksItemPrivate: memo.FKChecksItemPrivate{
			OriginTable: mb.tabID,
			FKOutbound:  true,
			FKOrdinal:   i,
		}}

		// Build an anti-join, with the origin FK columns on the left and the
		// referenced columns on the right.

		refTab, err := mb.b.catalog.ResolveDataSourceByID(mb.b.ctx, fk.ReferencedTableID())
		if err != nil {
			panic(builderError{err})
		}
		refOrdinals := make([]int, numCols)
		for j := range refOrdinals {
			refOrdinals[j] = fk.ReferencedColumnOrdinal(refTab.(cat.Table), j)
		}

		refTabMeta := mb.b.addTable(refTab.(cat.Table), refTab.Name())
		item.ReferencedTable = refTabMeta.MetaID
		scanScope := mb.b.buildScan(
			refTabMeta,
			refOrdinals,
			&tree.IndexFlags{IgnoreForeignKeys: true},
			includeMutations,
			mb.b.allocScope(),
		)

		inputProps := mb.outScope.expr.Relational()
		inputCols := make(opt.ColList, numCols)
		var notNullInputCols opt.ColSet
		for j := 0; j < numCols; j++ {
			ord := fk.OriginColumnOrdinal(mb.tab, j)
			inputColID := mb.insertColID(ord)
			if inputColID == 0 {
				// There shouldn't be any FK relations involving delete-only mutation
				// columns.
				panic(errors.AssertionFailedf("no value for FK column %d", ord))
			}
			inputCols[j] = inputColID

			// If a table column is not nullable, NULLs cannot be inserted (the
			// mutation will fail). So for the purposes of FK checks, we can treat
			// these columns as not null.
			if inputProps.NotNullCols.Contains(inputColID) || !mb.tab.Column(ord).IsNullable() {
				notNullInputCols.Add(inputColID)
			}
		}

		// Build the join filters:
		//   (origin_a = referenced_a) AND (origin_b = referenced_b) AND ...
		antiJoinFilters := make(memo.FiltersExpr, numCols)
		for j := 0; j < numCols; j++ {
			antiJoinFilters[j].Condition = mb.b.factory.ConstructEq(
				mb.b.factory.ConstructVariable(inputCols[j]),
				mb.b.factory.ConstructVariable(scanScope.cols[j].id),
			)
		}

		// TODO(radu): this is a major hack; we should be using a reference to a
		// buffer instead of reusing the expression directly. This is for
		// prototyping purposes (should work ok with constant inputs). Note that
		// the column IDs in the FK check query aren't exposed by the ForeignKeys
		// operator so using the same column IDs shouldn't cause issues.
		left := mb.b.factory.ConstructProject(
			mb.outScope.expr, memo.EmptyProjectionsExpr, inputCols.ToSet(),
		)
		item.KeyCols = inputCols

		if notNullInputCols.Len() < numCols {
			// The columns we are inserting might have NULLs. These require special
			// handling, depending on the match method:
			//  - MATCH SIMPLE: allows any column(s) to be NULL and the row doesn't
			//                  need to have a match in the referenced table.
			//  - MATCH FULL: only the case where *all* the columns are NULL is
			//                allowed, and the row doesn't need to have a match in the
			//                referenced table.
			//
			// Note that rows that have NULLs will never have a match in the anti
			// join and will generate errors. To handle these cases, we filter the
			// mutated rows (before the anti join) to remove those which don't need a
			// match.
			//
			// For SIMPLE, we filter out any rows which have a NULL. For FULL, we
			// filter out any rows where all the columns are NULL (rows which have
			// NULLs a subset of columns are let through and will generate FK errors
			// because they will never have a match in the anti join).
			switch m := fk.MatchMethod(); m {
			case tree.MatchSimple:
				// Filter out any rows which have a NULL; build filters of the form
				//   (a IS NOT NULL) AND (b IS NOT NULL) ...
				filters := make(memo.FiltersExpr, 0, numCols-notNullInputCols.Len())
				for _, col := range inputCols {
					if !notNullInputCols.Contains(col) {
						filters = append(filters, memo.FiltersItem{
							Condition: mb.b.factory.ConstructIsNot(
								mb.b.factory.ConstructVariable(col),
								memo.NullSingleton,
							),
						})
					}
				}
				left = mb.b.factory.ConstructSelect(left, filters)

			case tree.MatchFull:
				// Filter out any rows which have NULLs on all referencing columns.
				if !notNullInputCols.Empty() {
					// We statically know that some of the referencing columns can't be
					// NULL. In this case, we don't need to filter anything (the case
					// where all the origin columns are NULL is not possible).
					break
				}
				// Build a filter of the form
				//   (a IS NOT NULL) OR (b IS NOT NULL) ...
				var condition opt.ScalarExpr
				for _, col := range inputCols {
					is := mb.b.factory.ConstructIsNot(
						mb.b.factory.ConstructVariable(col),
						memo.NullSingleton,
					)
					if condition == nil {
						condition = is
					} else {
						condition = mb.b.factory.ConstructOr(condition, is)
					}
				}
				left = mb.b.factory.ConstructSelect(left, memo.FiltersExpr{{Condition: condition}})

			default:
				panic(errors.AssertionFailedf("match method %s not supported", m))
			}
		}

		item.Check = mb.b.factory.ConstructAntiJoin(
			left, scanScope.expr, antiJoinFilters, &memo.JoinPrivate{},
		)

		mb.checks = append(mb.checks, item)
	}
}

// findNotNullIndexCol finds the first not-null column in the given index and
// returns its ordinal position in the owner table. There must always be such a
// column, even if it turns out to be an implicit primary key column.
func findNotNullIndexCol(index cat.Index) int {
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		indexCol := index.Column(i)
		if !indexCol.IsNullable() {
			return indexCol.Ordinal
		}
	}
	panic(errors.AssertionFailedf("should have found not null column in index"))
}

// resultsNeeded determines whether a statement that might have a RETURNING
// clause needs to provide values for result rows for a downstream plan.
func resultsNeeded(r tree.ReturningClause) bool {
	switch t := r.(type) {
	case *tree.ReturningExprs:
		return true
	case *tree.ReturningNothing, *tree.NoReturningClause:
		return false
	default:
		panic(errors.AssertionFailedf("unexpected ReturningClause type: %T", t))
	}
}

// getAliasedTableName returns the underlying table name for a TableExpr that
// could be either an alias or a normal table name. It also returns the alias,
// if there is one.
//
// This is not meant to perform name resolution, but rather simply to extract
// the name indicated after FROM in DELETE/INSERT/UPDATE/UPSERT.
func getAliasedTableName(n tree.TableExpr) (*tree.TableName, *tree.TableName) {
	var alias *tree.TableName
	if ate, ok := n.(*tree.AliasedTableExpr); ok {
		n = ate.Expr
		// It's okay to ignore the As columns here, as they're not permitted in
		// DML aliases where this function is used. The grammar does not allow
		// them, so the parser would have reported an error if they were present.
		if ate.As.Alias != "" {
			alias = tree.NewUnqualifiedTableName(ate.As.Alias)
		}
	}
	tn, ok := n.(*tree.TableName)
	if !ok {
		panic(unimplemented.New(
			"complex table expression in UPDATE/DELETE",
			"cannot use a complex table name with DELETE/UPDATE"))
	}
	return tn, alias
}

// checkDatumTypeFitsColumnType verifies that a given scalar value type is valid
// to be stored in a column of the given column type.
//
// For the purpose of this analysis, column type aliases are not considered to
// be different (eg. TEXT and VARCHAR will fit the same scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func checkDatumTypeFitsColumnType(col cat.Column, typ *types.T) {
	if typ.Equivalent(col.DatumType()) {
		return
	}

	colName := string(col.ColName())
	panic(pgerror.Newf(pgcode.DatatypeMismatch,
		"value type %s doesn't match type %s of column %q",
		typ, col.DatumType(), tree.ErrNameString(colName)))
}
