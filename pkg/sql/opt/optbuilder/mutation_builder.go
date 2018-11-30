// Copyright 2018 The Cockroach Authors.
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

package optbuilder

import (
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// mutationBuilder is a helper struct that supports building Insert, Update,
// Upsert, and Delete operators in stages.
// TODO(andyk): Add support for Upsert and Delete.
type mutationBuilder struct {
	b  *Builder
	md *opt.Metadata

	// op is InsertOp, UpdateOp, UpsertOp, or DeleteOp.
	op opt.Operator

	// tab is the target table.
	tab opt.Table

	// tabID is the metadata ID of the table.
	tabID opt.TableID

	// alias is the table alias specified in the mutation statement, or just the
	// table name itself if no alias was specified.
	alias *tree.TableName

	// targetColList is an ordered list of IDs of the table columns into which
	// values will be inserted, or which will be updated with new values. It is
	// incrementally built as the operator is built.
	targetColList opt.ColList

	// targetColSet contains the same column IDs as targetColList, but as a set.
	targetColSet opt.ColSet

	// insertColList is an ordered list of IDs of input columns which provide
	// values to be inserted. Its length is always equal to the number of columns
	// in the target table, including mutation columns. It is empty if this is
	// not an Insert or Upsert operator.
	insertColList opt.ColList

	// parsedExprs is a cached set of parsed default and computed expressions
	// from the table schema. These are parsed once and cached for reuse.
	parsedExprs []tree.Expr

	// outScope contains the current set of columns that are in scope, as well as
	// the output expression as it is incrementally built. Once the final Insert
	// expression is completed, it will be contained in outScope.expr.
	outScope *scope
}

func (mb *mutationBuilder) init(b *Builder, op opt.Operator, tab opt.Table, alias *tree.TableName) {
	mb.b = b
	mb.md = b.factory.Metadata()
	mb.op = op
	mb.tab = tab
	mb.targetColList = make(opt.ColList, 0, tab.ColumnCount())

	if op == opt.InsertOp {
		mb.insertColList = make(opt.ColList, cap(mb.targetColList))
	}

	if alias != nil {
		mb.alias = alias
	} else {
		mb.alias = tab.Name()
	}

	// Add the table and its columns (including mutation columns) to metadata.
	mb.tabID = mb.md.AddTable(tab)
}

// addTargetNamedColsForInsert adds a list of user-specified column names to the
// list of table columns that are the target of the Insert operation.
func (mb *mutationBuilder) addTargetNamedColsForInsert(names tree.NameList) {
	if len(mb.targetColList) != 0 {
		panic("addTargetNamedColsForInsert cannot be called more than once")
	}

	for _, name := range names {
		found := false
		for ord, n := 0, mb.tab.ColumnCount(); ord < n; ord++ {
			tabCol := mb.tab.Column(ord)
			if tabCol.ColName() == name {
				colID := mb.tabID.ColumnID(ord)

				// Mutation columns cannot be targeted with input values.
				if opt.IsMutationColumn(mb.tab, ord) {
					break
				}

				// Computed columns cannot be targeted with input values.
				if tabCol.IsComputed() {
					panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
				}

				// Ensure that the name list does not contain duplicates.
				if mb.targetColSet.Contains(int(colID)) {
					panic(builderError{fmt.Errorf("multiple assignments to the same column %q", &name)})
				}
				mb.targetColSet.Add(int(colID))

				mb.targetColList = append(mb.targetColList, colID)
				found = true
				break
			}
		}
		if !found {
			panic(builderError{sqlbase.NewUndefinedColumnError(string(name))})
		}
	}

	// Ensure that primary key columns are in the target column list, or that
	// they have default values.
	mb.checkPrimaryKeyForInsert()

	// Ensure that foreign keys columns are in the target column list, or that
	// they have default values.
	mb.checkForeignKeysForInsert()
}

// checkPrimaryKeyForInsert ensures that the columns of the primary key are
// either assigned values by the INSERT statement, or else have default/computed
// values. If neither condition is true, checkPrimaryKeyForInsert raises an
// error.
func (mb *mutationBuilder) checkPrimaryKeyForInsert() {
	primary := mb.tab.Index(opt.PrimaryIndex)
	for i, n := 0, primary.KeyColumnCount(); i < n; i++ {
		col := primary.Column(i)
		if col.Column.HasDefault() || col.Column.IsComputed() {
			// The column has a default or computed value.
			continue
		}

		colID := mb.tabID.ColumnID(col.Ordinal)
		if mb.targetColSet.Contains(int(colID)) {
			// The column is explicitly specified in the target name list.
			continue
		}

		panic(builderError{fmt.Errorf(
			"missing %q primary key column", col.Column.ColName())})
	}
}

// checkForeignKeysForInsert ensures that all foreign key columns are either
// assigned values by the INSERT statement, or else have default/computed
// values.  Alternatively, all columns can be unspecified. If neither condition
// is true, checkForeignKeysForInsert raises an error. Here is an example:
//
//   CREATE TABLE orders (
//     id INT,
//     cust_id INT,
//     state STRING,
//     FOREIGN KEY (cust_id, state) REFERENCES customers (id, state)
//   )
//
//   INSERT INTO orders (cust_id) VALUES (1)
//
// This INSERT statement would trigger a static error, because only cust_id is
// specified in the INSERT statement. Either the state column must be specified
// as well, or else neither column can be specified.
//
// TODO(bram): add MATCH SIMPLE and fix MATCH FULL #30026
func (mb *mutationBuilder) checkForeignKeysForInsert() {
	for i, n := 0, mb.tab.IndexCount(); i < n; i++ {
		idx := mb.tab.Index(i)
		fkey, ok := idx.ForeignKey()
		if !ok {
			continue
		}

		var missingCols []string
		allMissing := true
		for j := 0; j < int(fkey.PrefixLen); j++ {
			indexCol := idx.Column(j)
			if indexCol.Column.HasDefault() || indexCol.Column.IsComputed() {
				// The column has a default value.
				allMissing = false
				continue
			}

			colID := mb.tabID.ColumnID(indexCol.Ordinal)
			if mb.targetColSet.Contains(int(colID)) {
				// The column is explicitly specified in the target name list.
				allMissing = false
				continue
			}

			missingCols = append(missingCols, string(indexCol.Column.ColName()))
		}
		if allMissing {
			continue
		}

		switch len(missingCols) {
		case 0:
			// Do nothing.

		case 1:
			panic(builderError{errors.Errorf(
				"missing value for column %q in multi-part foreign key", missingCols[0])})
		default:
			sort.Strings(missingCols)
			panic(builderError{errors.Errorf(
				"missing values for columns %q in multi-part foreign key", missingCols)})
		}
	}
}

// addTargetTableColsForInsert adds up to maxCols columns to the list of columns
// that will be set by an INSERT operation. Columns are added from the target
// table in the same order they appear in its schema. This method is used when
// the target columns are not explicitly specified in the INSERT statement:
//
//   INSERT INTO t VALUES (1, 2, 3)
//
// In this example, the first three columns of table t would be added as target
// columns.
func (mb *mutationBuilder) addTargetTableColsForInsert(maxCols int) {
	if len(mb.targetColList) != 0 {
		panic("addTargetTableCols cannot be called more than once")
	}

	numCols := 0
	for i, n := 0, mb.tab.ColumnCount(); i < n && numCols < maxCols; i++ {
		tabCol := mb.tab.Column(i)
		if tabCol.IsHidden() {
			continue
		}

		// Don't allow targeting of mutation columns.
		if opt.IsMutationColumn(mb.tab, i) {
			continue
		}

		// TODO(justin): this is too restrictive. It should be possible to allow
		// INSERT INTO (x) VALUES (DEFAULT) if x is a computed column. See #22434.
		if tabCol.IsComputed() {
			panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
		}

		colID := mb.tabID.ColumnID(i)
		mb.targetColList = append(mb.targetColList, colID)
		mb.targetColSet.Add(int(colID))
		numCols++
	}

	// Ensure that the number of input columns does not exceed the number of
	// target columns.
	mb.checkNumCols(len(mb.targetColList), maxCols)
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

// buildInputForInsert constructs the memo group for the input expression and
// constructs a new output scope containing that expression's output columns.
func (mb *mutationBuilder) buildInputForInsert(inScope *scope, inputRows *tree.Select) {
	// If there are already required target columns, then those will provide
	// desired input types. Otherwise, input columns are mapped to the table's
	// non-hidden columns by corresponding ordinal position. Exclude hidden
	// columns to prevent this statement from writing hidden columns:
	//
	//   INSERT INTO <table> VALUES (...)
	//
	// However, hidden columns can be written if the target columns were
	// explicitly specified:
	//
	//   INSERT INTO <table> (...) VALUES (...)
	//
	var desiredTypes []types.T
	if len(mb.targetColList) != 0 {
		desiredTypes = make([]types.T, len(mb.targetColList))
		for i, colID := range mb.targetColList {
			desiredTypes[i] = mb.md.ColumnType(colID)
		}
	} else {
		desiredTypes = make([]types.T, 0, mb.tab.ColumnCount())
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			tabCol := mb.tab.Column(i)
			if !tabCol.IsHidden() {
				desiredTypes = append(desiredTypes, tabCol.DatumType())
			}
		}
	}

	mb.outScope = mb.b.buildSelect(inputRows, desiredTypes, inScope)

	if len(mb.targetColList) != 0 {
		// Target columns already exist, so ensure that the number of input
		// columns exactly matches the number of target columns.
		mb.checkNumCols(len(mb.targetColList), len(mb.outScope.cols))
	} else {
		// No target columns have been added by previous steps, so add columns
		// that are implicitly targeted by the input expression.
		mb.addTargetTableColsForInsert(len(mb.outScope.cols))
	}

	// Loop over input columns and:
	//   1. Type check each column
	//   2. Assign name to each column
	//   3. Add id of each column to the insertColList
	for i := range mb.outScope.cols {
		inCol := &mb.outScope.cols[i]
		ord := mb.md.ColumnOrdinal(mb.targetColList[i])

		// Type check the input column against the corresponding table column.
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), inCol.typ)

		// Assign name of input column. Computed columns can refer to this column
		// by its name.
		inCol.name = tree.Name(mb.md.ColumnLabel(mb.targetColList[i]))

		// Map the ordinal position of each table column to the id of the input
		// column which will be inserted into that position.
		mb.insertColList[ord] = inCol.id
	}
}

// buildEmptyInput constructs a new output scope containing a single row VALUES
// expression with zero columns.
func (mb *mutationBuilder) buildEmptyInput(inScope *scope) {
	mb.outScope = inScope.push()
	mb.outScope.expr = mb.b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, opt.ColList{})
}

// addDefaultAndComputedColsForInsert wraps an Insert input expression with
// Project operator(s) containing any default (or nullable) and computed columns
// that are not yet part of the target column list. This includes mutation
// columns, since they must always have default or computed values.
//
// After this call, the input expression will provide values for every one of
// the target table columns, whether it was explicitly specified or implicitly
// added.
func (mb *mutationBuilder) addDefaultAndComputedColsForInsert() {
	// Add any missing default and nullable columns.
	mb.addSynthesizedCols(
		mb.insertColList,
		func(tabCol opt.Column) bool { return !tabCol.IsComputed() },
	)

	// Add any missing computed columns. This must be done after adding default
	// columns above, because computed columns can depend on default columns.
	mb.addSynthesizedCols(
		mb.insertColList,
		func(tabCol opt.Column) bool { return tabCol.IsComputed() },
	)
}

// addSynthesizedCols is a helper method for addDefaultAndComputedColsForInsert that
// scans the list of table columns, looking for any that do not yet have values
// provided by the input expression. New columns are synthesized for any missing
// columns, as long as the addCol callback function returns true for that
// column.
func (mb *mutationBuilder) addSynthesizedCols(
	colList opt.ColList, addCol func(tabCol opt.Column) bool,
) {
	var projectionsScope *scope

	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		// Skip columns that are already specified.
		if colList[i] != 0 {
			continue
		}

		// Skip delete-only mutation columns, since they are ignored by all
		// mutation operators that synthesize columns.
		if mut, ok := mb.tab.MutationColumn(i); ok && mut.IsDeleteOnly {
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
			projectionsScope.copyOrdering(mb.outScope)
		}

		tabColID := mb.tabID.ColumnID(i)
		expr := mb.parseDefaultOrComputedExpr(tabColID)
		texpr := mb.outScope.resolveType(expr, tabCol.DatumType())
		scopeCol := mb.b.addColumn(projectionsScope, "" /* label */, texpr)
		mb.b.buildScalar(texpr, mb.outScope, projectionsScope, scopeCol, nil)

		// Assign name to synthesized column. Computed columns may refer to default
		// columns in the table by name.
		scopeCol.name = tabCol.ColName()

		// Store id of newly synthesized column in corresponding list slot.
		colList[i] = scopeCol.id

		// Add corresponding target column.
		mb.targetColList = append(mb.targetColList, tabColID)
		mb.targetColSet.Add(int(tabColID))
	}

	if projectionsScope != nil {
		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// buildInsert constructs an Insert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause. Insert always
// returns columns in the same order and with the same names as the target
// table.
func (mb *mutationBuilder) buildInsert(returning tree.ReturningExprs) {
	private := memo.MutationPrivate{
		Table:       mb.tabID,
		InsertCols:  mb.insertColList,
		NeedResults: returning != nil,
	}
	private.Ordering.FromOrdering(mb.outScope.ordering)
	mb.outScope.expr = mb.b.factory.ConstructInsert(mb.outScope.expr, &private)

	if returning != nil {
		// 1. Project only non-mutation columns.
		// 2. Alias columns to use table column names.
		// 3. Mark hidden columns.
		inScope := mb.outScope.replace()
		inScope.expr = mb.outScope.expr
		inScope.cols = make([]scopeColumn, 0, mb.tab.ColumnCount())
		n := 0
		for i, col := range mb.insertColList {
			if opt.IsMutationColumn(mb.tab, i) {
				continue
			}

			outCol := mb.outScope.getColumn(col)
			inScope.cols = inScope.cols[:n+1]
			inScope.cols[n] = *outCol
			inScope.cols[n].table = *mb.alias
			inScope.cols[n].name = mb.tab.Column(i).ColName()
			inScope.cols[n].hidden = mb.tab.Column(i).IsHidden()
			n++
		}

		outScope := inScope.replace()
		mb.b.analyzeReturningList(returning, nil /* desiredTypes */, inScope, outScope)
		mb.b.buildProjectionList(inScope, outScope)
		mb.b.constructProjectForScope(inScope, outScope)
		mb.outScope = outScope
	} else {
		mb.outScope = &scope{builder: mb.b, expr: mb.outScope.expr}
	}
}

// checkNumCols raises an error if the expected number of columns does not match
// the actual number of columns.
func (mb *mutationBuilder) checkNumCols(expected, actual int) {
	if actual != expected {
		more, less := "expressions", "target columns"
		if actual < expected {
			more, less = less, more
		}

		// TODO(andyk): Add UpsertOp case.
		kw := "INSERT"
		panic(builderError{pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"%s has more %s than %s, %d expressions for %d targets",
			kw, more, less, actual, expected)})
	}
}

// parseDefaultOrComputedExpr parses the default (including nullable) or
// computed value expression for the given table column, and caches it for
// reuse.
func (mb *mutationBuilder) parseDefaultOrComputedExpr(colID opt.ColumnID) tree.Expr {
	if mb.parsedExprs == nil {
		mb.parsedExprs = make([]tree.Expr, mb.tab.ColumnCount())
	}

	// Return expression from cache, if it was already parsed previously.
	ord := mb.md.ColumnOrdinal(colID)
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
	case tabCol.IsNullable():
		return tree.DNull
	default:
		panic(builderError{sqlbase.NewNonNullViolationError(string(tabCol.ColName()))})
	}

	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		panic(builderError{err})
	}

	mb.parsedExprs[ord] = expr
	return mb.parsedExprs[ord]
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
		panic(errors.Errorf("unexpected ReturningClause type: %T", t))
	}
}

// getAliasedTableName returns the underlying table name for a TableExpr that
// could be either an alias or a normal table name. It also returns the original
// table name, which will be equal to the alias name if the input is an alias,
// or identical to the table name if the input is a normal table name.
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
		panic(builderError{pgerror.Unimplemented(
			"complex table expression in UPDATE/DELETE",
			"cannot use a complex table name with DELETE/UPDATE")})
	}
	if alias == nil {
		alias = tn
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
func checkDatumTypeFitsColumnType(col opt.Column, typ types.T) {
	if typ == types.Unknown || typ.Equivalent(col.DatumType()) {
		return
	}

	colName := string(col.ColName())
	panic(builderError{pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
		"value type %s doesn't match type %s of column %q",
		typ, col.ColTypeStr(), tree.ErrNameString(&colName))})
}
