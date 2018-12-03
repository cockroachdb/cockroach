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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
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
	// incrementally built as the mutation operator is built.
	targetColList opt.ColList

	// targetColSet contains the same column IDs as targetColList, but as a set.
	targetColSet opt.ColSet

	// insertColList is an ordered list of IDs of input columns which provide
	// values to be inserted. Its length is always equal to the number of columns
	// in the target table, including mutation columns. Table columns which will
	// not have values inserted are set to zero (e.g. delete-only mutation
	// columns). insertColList is empty if this is not an Insert operator.
	insertColList opt.ColList

	// fetchColList is an ordered list of IDs of input columns which are fetched
	// from a target table in order to provide existing values that will form
	// lookup and update values. Its length is always equal to the number of
	// columns in the target table, including mutation columns. Table columns
	// which do not need to be fetched are set to zero. fetchColList is empty if
	// this is an Insert operator with no ON CONFLICT clause.
	fetchColList opt.ColList

	// updateColList is an ordered list of IDs of input columns which contain new
	// updated values for columns in a target table. Its length is always equal
	// to the number of columns in the target table, including mutation columns.
	// Table columns which do not need to be updated are set to zero.
	// updateColList is empty if this is an Insert operator with no ON CONFLICT
	// clause.
	updateColList opt.ColList

	// subqueries temporarily stores subqueries that were built during initial
	// analysis of SET expressions. They will be used later when the subqueries
	// are joined into larger LEFT OUTER JOIN expressions.
	subqueries []*scope

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
	if op == opt.UpdateOp {
		mb.fetchColList = make(opt.ColList, cap(mb.targetColList))
		mb.updateColList = make(opt.ColList, cap(mb.targetColList))
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

	// Add target table columns by the names specified in the Insert statement.
	mb.addTargetColsByName(names)

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
		panic("addTargetTableColsForInsert cannot be called more than once")
	}

	numCols := 0
	for i, n := 0, mb.tab.ColumnCount(); i < n && numCols < maxCols; i++ {
		// Skip hidden columns.
		if mb.tab.Column(i).IsHidden() {
			continue
		}

		mb.addTargetCol(i)
		numCols++
	}

	// Ensure that the number of input columns does not exceed the number of
	// target columns.
	mb.checkNumCols(len(mb.targetColList), maxCols)
}

// addTargetColsForUpdate compiles the given SET expressions and adds the user-
// specified column names to the list of table columns that will be updated by
// the Update operation. Verify that the RHS of the SET expression provides
// exactly as many columns as are expected by the named SET columns.
func (mb *mutationBuilder) addTargetColsForUpdate(exprs tree.UpdateExprs) {
	if len(mb.targetColList) != 0 {
		panic("addTargetColsForUpdate cannot be called more than once")
	}

	for _, expr := range exprs {
		mb.addTargetColsByName(expr.Names)

		if expr.Tuple {
			n := -1
			switch t := expr.Expr.(type) {
			case *tree.Subquery:
				// Build the subquery in order to determine how many columns it
				// projects, and store it for later use in the addUpdateCols method.
				// Use the data types of the target columns to resolve expressions
				// with ambiguous types (e.g. should 1 be interpreted as an INT or
				// as a FLOAT).
				desiredTypes := make([]types.T, len(expr.Names))
				targetIdx := len(mb.targetColList) - len(expr.Names)
				for i := range desiredTypes {
					desiredTypes[i] = mb.md.ColumnType(mb.targetColList[targetIdx+i])
				}
				outScope := mb.b.buildSelectStmt(t.Select, desiredTypes, mb.outScope)
				mb.subqueries = append(mb.subqueries, outScope)
				n = len(outScope.cols)

			case *tree.Tuple:
				n = len(t.Exprs)
			}
			if n < 0 {
				panic(builderError{errors.Errorf("unsupported tuple assignment: %T", expr.Expr)})
			}
			if len(expr.Names) != n {
				panic(builderError{fmt.Errorf("number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n)})
			}
		}
	}
}

// addTargetColsByName adds one target column for each of the names in the given
// list.
func (mb *mutationBuilder) addTargetColsByName(names tree.NameList) {
	for _, name := range names {
		// Determine the ordinal position of the named column in the table and
		// add it as a target column.
		found := false
		for ord, n := 0, mb.tab.ColumnCount(); ord < n; ord++ {
			if mb.tab.Column(ord).ColName() == name {
				mb.addTargetCol(ord)
				found = true
				break
			}
		}
		if !found {
			panic(builderError{sqlbase.NewUndefinedColumnError(string(name))})
		}
	}
}

// addTargetCol adds a target column by its ordinal position in the target
// table. It raises an error if a mutation or computed column is targeted, or if
// the same column is targeted multiple times.
func (mb *mutationBuilder) addTargetCol(ord int) {
	tabCol := mb.tab.Column(ord)

	// Don't allow targeting of mutation columns.
	if opt.IsMutationColumn(mb.tab, ord) {
		panic(builderError{makeBackfillError(tabCol.ColName())})
	}

	// Computed columns cannot be targeted with input values.
	if tabCol.IsComputed() {
		panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
	}

	// Ensure that the name list does not contain duplicates.
	colID := mb.tabID.ColumnID(ord)
	if mb.targetColSet.Contains(int(colID)) {
		panic(builderError{fmt.Errorf("multiple assignments to the same column %q", tabCol.ColName())})
	}
	mb.targetColSet.Add(int(colID))

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

// buildInputForUpdate constructs a Select expression from the fields in the
// Update operator, similar to this:
//
//   SELECT <cols>
//   FROM <table>
//   WHERE <where>
//   ORDER BY <order-by>
//   LIMIT <limit>
//
// All columns from the table to update are added to fetchColList.
// TODO(andyk): Do needed column analysis to project fewer columns if possible.
func (mb *mutationBuilder) buildInputForUpdate(inScope *scope, upd *tree.Update) {
	// FROM
	mb.outScope = mb.b.buildScan(
		mb.tab,
		mb.tab.Name(),
		nil, /* ordinals */
		nil, /* indexFlags */
		includeMutations,
		inScope,
	)

	// Overwrite output properties with any alias information.
	mb.outScope.setTableAlias(mb.alias.TableName)

	// WHERE
	mb.b.buildWhere(upd.Where, mb.outScope)

	// SELECT + ORDER BY (which may add projected expressions)
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)
	orderByScope := mb.b.analyzeOrderBy(upd.OrderBy, mb.outScope, projectionsScope)
	mb.b.buildOrderBy(mb.outScope, projectionsScope, orderByScope)
	mb.b.constructProjectForScope(mb.outScope, projectionsScope)

	// LIMIT
	if upd.Limit != nil {
		mb.b.buildLimit(upd.Limit, inScope, projectionsScope)
	}

	mb.outScope = projectionsScope

	// Set list of columns that will be fetched by the input expression.
	for i := range mb.outScope.cols {
		mb.fetchColList[i] = mb.outScope.cols[i].id
	}
}

// addUpdateCols builds nested Project and LeftOuterJoin expressions that
// correspond to the given SET expressions:
//
//   SET a=1 (single-column SET)
//     Add as synthesized Project column:
//       SELECT <fetch-cols>, 1 FROM <input>
//
//   SET (a, b)=(1, 2) (tuple SET)
//     Add as multiple Project columns:
//       SELECT <fetch-cols>, 1, 2 FROM <input>
//
//   SET (a, b)=(SELECT 1, 2) (subquery)
//     Wrap input in Max1Row + LeftJoinApply expressions:
//       SELECT * FROM <fetch-cols> LEFT JOIN LATERAL (SELECT 1, 2) ON True
//
// Multiple subqueries result in multiple left joins successively wrapping the
// input. A final Project operator is built if any single-column or tuple SET
// expressions are present.
func (mb *mutationBuilder) addUpdateCols(exprs tree.UpdateExprs) {
	// SET expressions should reject aggregates, generators, etc.
	scalarProps := &mb.b.semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	mb.b.semaCtx.Properties.Require("UPDATE SET", tree.RejectSpecial)

	// UPDATE input columns are accessible to SET expressions.
	inScope := mb.outScope

	// Project additional column(s) for each update expression (can be multiple
	// columns in case of tuple assignment).
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)
	projectionsScope.copyOrdering(mb.outScope)

	checkCol := func(sourceCol *scopeColumn, targetColID opt.ColumnID) {
		// Type check the input expression against the corresponding table column.
		ord := mb.md.ColumnOrdinal(targetColID)
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), sourceCol.typ)

		// Add new column ID to the list of columns to update.
		mb.updateColList[ord] = sourceCol.id

		// "Shadow" the existing column of the same name, since any future
		// references (like computed expressions) should point to the new column
		// containing the updated value rather than the old column containing the
		// original value. The old columns need to be retained in the projection
		// because some original values are needed to formulate the update keys.
		projectionsScope.cols[ord].name = ""
		sourceCol.name = mb.tab.Column(ord).ColName()
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID) {
		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}

		// Add new column to the projections scope.
		desiredType := mb.md.ColumnType(targetColID)
		texpr := inScope.resolveType(expr, desiredType)
		scopeCol := mb.b.addColumn(projectionsScope, "" /* label */, texpr)
		mb.b.buildScalar(texpr, inScope, projectionsScope, scopeCol, nil)

		checkCol(scopeCol, targetColID)
	}

	n := 0
	subquery := 0
	for _, set := range exprs {
		if set.Tuple {
			switch t := set.Expr.(type) {
			case *tree.Subquery:
				// Get the subquery scope that was built by addTargetColsForUpdate.
				subqueryScope := mb.subqueries[subquery]
				subquery++

				// Type check and rename columns.
				for i := range subqueryScope.cols {
					checkCol(&subqueryScope.cols[i], mb.targetColList[n])
					n++
				}

				// Lazily create new scope to hold results of join.
				if mb.outScope == inScope {
					mb.outScope = inScope.replace()
					mb.outScope.appendColumnsFromScope(inScope)
					mb.outScope.expr = inScope.expr
				}

				// Wrap input with Max1Row + LOJ.
				mb.outScope.appendColumnsFromScope(subqueryScope)
				mb.outScope.expr = mb.b.factory.ConstructLeftJoinApply(
					mb.outScope.expr,
					mb.b.factory.ConstructMax1Row(subqueryScope.expr),
					memo.TrueFilter,
				)

				// Project all subquery output columns.
				projectionsScope.appendColumnsFromScope(subqueryScope)

			case *tree.Tuple:
				for _, expr := range t.Exprs {
					addCol(expr, mb.targetColList[n])
					n++
				}
			}
		} else {
			addCol(set.Expr, mb.targetColList[n])
			n++
		}
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope
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

// addComputedColsForUpdate wraps an Update input expression with a Project
// operator containing any computed columns that need to be updated. This
// includes write-only mutation columns that are computed.
func (mb *mutationBuilder) addComputedColsForUpdate() {
	// Allow mutation columns to be referenced by other computed mutation columns
	// (otherwise the scope will raise an error if a mutation column is
	// referenced). These do not need to be set back to true again because
	// mutation columns are not projected by the Update operator.
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].mutation = false
	}

	mb.addSynthesizedCols(
		mb.updateColList,
		func(tabCol opt.Column) bool { return tabCol.IsComputed() },
	)
}

// addSynthesizedCols is a helper method for addDefaultAndComputedColsForInsert
// and addComputedColsForUpdate that scans the list of table columns, looking
// for any that do not yet have values provided by the input expression. New
// columns are synthesized for any missing columns, as long as the addCol
// callback function returns true for that column.
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
		if mut, ok := mb.tab.Column(i).(*opt.MutationColumn); ok && mut.IsDeleteOnly {
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
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildInsert(returning tree.ReturningExprs) {
	private := memo.MutationPrivate{
		Table:       mb.tabID,
		InsertCols:  mb.insertColList,
		NeedResults: returning != nil,
	}
	private.Ordering.FromOrdering(mb.outScope.ordering)
	mb.outScope.expr = mb.b.factory.ConstructInsert(mb.outScope.expr, &private)

	mb.buildReturning(returning)
}

// buildUpdate constructs an Update operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpdate(returning tree.ReturningExprs) {
	private := memo.MutationPrivate{
		Table:       mb.tabID,
		FetchCols:   mb.fetchColList,
		UpdateCols:  mb.updateColList,
		NeedResults: returning != nil,
	}
	private.Ordering.FromOrdering(mb.outScope.ordering)
	mb.outScope.expr = mb.b.factory.ConstructUpdate(mb.outScope.expr, &private)

	mb.buildReturning(returning)
}

// buildReturning wraps the input expression with a Project operator that
// projects the given RETURNING expressions.
func (mb *mutationBuilder) buildReturning(returning tree.ReturningExprs) {
	if returning != nil {
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
			if opt.IsMutationColumn(mb.tab, i) {
				continue
			}

			// Derive ID of column projected by default by the mutation operator.
			var srcColID opt.ColumnID
			if mb.op == opt.InsertOp {
				srcColID = mb.insertColList[i]
			} else {
				// The Update operator returns the updated column if one exists, or
				// else the fetched column if not.
				if mb.updateColList[i] != 0 {
					srcColID = mb.updateColList[i]
				} else {
					srcColID = mb.fetchColList[i]
				}
			}

			// Copy column from the input scope to the returning scope, and update the
			// name and hidden attribute to correspond to the target table column.
			srcCol := mb.outScope.getColumn(srcColID)
			inScope.cols = inScope.cols[:len(inScope.cols)+1]
			dstCol := &inScope.cols[len(inScope.cols)-1]
			*dstCol = *srcCol
			dstCol.table = *mb.alias
			dstCol.name = mb.tab.Column(i).ColName()
			dstCol.hidden = mb.tab.Column(i).IsHidden()
		}

		// Construct the Project operator that projects the RETURNING expressions.
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
	default:
		return tree.DNull
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
