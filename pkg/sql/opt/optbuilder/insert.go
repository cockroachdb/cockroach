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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// buildInsert builds a memo group for an InsertOp expression. An input
// expression is constructed which outputs these columns:
//
//   1. Columns explicitly specified by the user in SELECT or VALUES expression.
//
//   2. Columns not specified by the user, but having a default value declared
//      in schema (or being nullable).
//
//   3. Computed columns.
//
//   4. Mutation columns which are being added or dropped by an online schema
//      change.
//
// buildInsert starts by constructing the input expression, and then wraps it
// with Project operators which add default, computed, and mutation columns. The
// final input expression will project values for all columns in the target
// table. For example, if this is the schema and INSERT statement:
//
//   CREATE TABLE abcd (
//     a INT PRIMARY KEY,
//     b INT,
//     c INT DEFAULT(10),
//     d INT AS (b+c) STORED
//   )
//   INSERT INTO abcd (a) VALUES (1)
//
// Then an input expression equivalent to this would be built:
//
//   INSERT INTO abcd (a, b, c, d)
//   SELECT aa, bb, cc, bb + cc AS dd
//   FROM (VALUES (1, NULL, 10)) AS t(aa, bb, cc)
//
func (b *Builder) buildInsert(ins *tree.Insert, inScope *scope) (outScope *scope) {
	if ins.OnConflict != nil {
		panic(unimplementedf("UPSERT is not supported"))
	}

	if ins.With != nil {
		inScope = b.buildCTE(ins.With.CTEList, inScope)
		defer b.checkCTEUsage(inScope)
	}

	// INSERT INTO xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(ins.Table)

	// Find which table we're working on, check the permissions.
	tab := b.resolveTable(tn, privilege.INSERT)

	// Table resolution checked the INSERT permission, but if OnConflict is
	// defined, then check the UPDATE permission as well. This has the side effect
	// of twice adding the table to the metadata dependency list.
	if ins.OnConflict != nil && !ins.OnConflict.DoNothing {
		b.checkPrivilege(tab, privilege.UPDATE)
	}

	var ib insertBuilder
	ib.init(b, opt.InsertOp, tab, alias)

	// Compute target columns in two cases:
	//
	//   1. When explicitly specified by name:
	//
	//        INSERT INTO <table> (<col1>, <col2>, ...) ...
	//
	//   2. When implicitly targeted by VALUES expression:
	//
	//        INSERT INTO <table> VALUES (...)
	//
	// Target columns for other cases can't be derived until the input expression
	// is built, at which time the number of input columns is known. At the same
	// time, the input expression cannot be built until DEFAULT expressions are
	// replaced and named target columns are known. So this step must come first.
	if len(ins.Columns) != 0 {
		// Target columns are explicitly specified by name.
		ib.addTargetNamedCols(ins.Columns)
	} else {
		values := ib.extractValuesInput(ins.Rows)
		if values != nil {
			// Target columns are implicitly targeted by VALUES expression in the
			// same order they appear in the target table schema.
			ib.addTargetTableCols(len(values.Rows[0]))
		}
	}

	// Build the input rows expression if one was specified:
	//
	//   INSERT INTO <table> VALUES ...
	//   INSERT INTO <table> SELECT ... FROM ...
	//
	// or initialize an empty input if inserting default values (default values
	// will be added later):
	//
	//   INSERT INTO <table> DEFAULT VALUES
	//
	if !ins.DefaultValues() {
		// Replace any DEFAULT expressions in the VALUES clause, if a VALUES clause
		// exists:
		//
		//   INSERT INTO <table> VALUES (..., DEFAULT, ...)
		//
		rows := ib.replaceDefaultExprs(ins.Rows)

		ib.buildInputRows(inScope, rows)
	} else {
		ib.buildEmptyInput(inScope)
	}

	// Add default and computed columns that were not explicitly specified by
	// name or implicitly targeted by input columns. This includes any columns
	// undergoing write mutations, as they must always have a default or computed
	// value.
	ib.addDefaultAndComputedCols()

	// Build the final insert statement, including any returned expressions.
	if resultsNeeded(ins.Returning) {
		ib.buildInsert(*ins.Returning.(*tree.ReturningExprs))
	} else {
		ib.buildInsert(nil /* returning */)
	}

	return ib.outScope
}

// insertBuilder is a helper struct that supports building an Insert operator in
// stages.
type insertBuilder struct {
	b  *Builder
	md *opt.Metadata

	// op is InsertOp or UpsertOp.
	op opt.Operator

	// tab is the target table.
	tab opt.Table

	// tabID is the metadata ID of the table.
	tabID opt.TableID

	// alias is the table alias specified in the INSERT statement, or just the
	// table name itself if no alias was specified.
	alias *tree.TableName

	// targetColList is an ordered list of IDs of the table columns into which
	// values will be inserted by the Insert operator. It is incrementally built
	// as the operator is built.
	targetColList opt.ColList

	// targetColSet contains the same column IDs as targetColList, but as a set.
	targetColSet opt.ColSet

	// parsedExprs is a cached set of parsed default and computed expressions
	// from the table schema. These are parsed once and cached for reuse.
	parsedExprs []tree.Expr

	// outScope contains the current set of columns that are in scope, as well as
	// the output expression as it is incrementally built. Once the final Insert
	// expression is completed, it will be contained in outScope.expr.
	outScope *scope
}

func (ib *insertBuilder) init(b *Builder, op opt.Operator, tab opt.Table, alias *tree.TableName) {
	ib.b = b
	ib.md = b.factory.Metadata()
	ib.op = op
	ib.tab = tab
	ib.targetColList = make(opt.ColList, 0, tab.ColumnCount())

	if alias != nil {
		ib.alias = alias
	} else {
		ib.alias = tab.Name()
	}

	// Add the table and its columns to metadata. Include columns undergoing write
	// mutations, since default values will need to be inserted into those.
	ib.tabID = ib.md.AddTableWithMutations(tab)
}

// addTargetNamedCols adds a list of user-specified column names to the list of
// table columns that are the target of the Insert operation.
func (ib *insertBuilder) addTargetNamedCols(names tree.NameList) {
	if len(ib.targetColList) != 0 {
		panic("addTargetNamedCols cannot be called more than once")
	}

	for _, name := range names {
		found := false
		for ord, n := 0, ib.tab.ColumnCount(); ord < n; ord++ {
			tabCol := ib.tab.Column(ord)
			if tabCol.ColName() == name {
				colID := ib.tabID.ColumnID(ord)

				// Computed columns cannot be targeted with input values.
				if tabCol.IsComputed() {
					panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
				}

				// Ensure that the name list does not contain duplicates.
				if ib.targetColSet.Contains(int(colID)) {
					panic(builderError{fmt.Errorf("multiple assignments to the same column %q", &name)})
				}
				ib.targetColSet.Add(int(colID))

				ib.targetColList = append(ib.targetColList, colID)
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
	ib.checkPrimaryKey()

	// Ensure that foreign keys columns are in the target column list, or that
	// they have default values.
	ib.checkForeignKeys()
}

// checkPrimaryKey ensures that the columns of the primary key are either
// assigned values by the INSERT statement, or else have default/computed
// values. If neither condition is true, checkPrimaryKey raises an error.
func (ib *insertBuilder) checkPrimaryKey() {
	primary := ib.tab.Index(opt.PrimaryIndex)
	for i, n := 0, primary.KeyColumnCount(); i < n; i++ {
		col := primary.Column(i)
		if col.Column.HasDefault() || col.Column.IsComputed() {
			// The column has a default or computed value.
			continue
		}

		colID := ib.tabID.ColumnID(col.Ordinal)
		if ib.targetColSet.Contains(int(colID)) {
			// The column is explicitly specified in the target name list.
			continue
		}

		panic(builderError{fmt.Errorf(
			"missing %q primary key column", col.Column.ColName())})
	}
}

// checkForeignKeys ensures that all foreign key columns are either assigned
// values by the INSERT statement, or else have default/computed values.
// Alternatively, all columns can be unspecified. If neither condition is true,
// checkForeignKeys raises an error. Here is an example:
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
func (ib *insertBuilder) checkForeignKeys() {
	for i, n := 0, ib.tab.IndexCount(); i < n; i++ {
		idx := ib.tab.Index(i)
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

			colID := ib.tabID.ColumnID(indexCol.Ordinal)
			if ib.targetColSet.Contains(int(colID)) {
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

// addTargetTableCols adds up to maxCols columns to the list of columns that
// will be set by an INSERT operation. Columns are added from the target table
// in the same order they appear in its schema. This method is used when the
// target columns are not explicitly specified in the INSERT statement:
//
//   INSERT INTO t VALUES (1, 2, 3)
//
// In this example, the first three columns of table t would be added as target
// columns.
func (ib *insertBuilder) addTargetTableCols(maxCols int) {
	if len(ib.targetColList) != 0 {
		panic("addTargetTableCols cannot be called more than once")
	}

	numCols := 0
	for i, n := 0, ib.tab.ColumnCount(); i < n && numCols < maxCols; i++ {
		tabCol := ib.tab.Column(i)
		if tabCol.IsHidden() {
			continue
		}

		// TODO(justin): this is too restrictive. It should be possible to allow
		// INSERT INTO (x) VALUES (DEFAULT) if x is a computed column. See #22434.
		if tabCol.IsComputed() {
			panic(builderError{sqlbase.CannotWriteToComputedColError(string(tabCol.ColName()))})
		}

		colID := ib.tabID.ColumnID(i)
		ib.targetColList = append(ib.targetColList, colID)
		ib.targetColSet.Add(int(colID))
		numCols++
	}

	// Ensure that the number of input columns does not exceed the number of
	// target columns.
	ib.checkNumCols(len(ib.targetColList), maxCols)
}

// extractValuesInput tests whether the given input is a VALUES clause with no
// WITH, ORDER BY, or LIMIT modifier. If so, it's returned, otherwise nil is
// returned.
func (ib *insertBuilder) extractValuesInput(inputRows *tree.Select) *tree.ValuesClause {
	if inputRows == nil {
		return nil
	}

	// Only extract a simple VALUES clause with no modifiers.
	if inputRows.With != nil || inputRows.OrderBy != nil || inputRows.Limit != nil {
		return nil
	}

	// Discard parentheses.
	if parens, ok := inputRows.Select.(*tree.ParenSelect); ok {
		return ib.extractValuesInput(parens.Select)
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
func (ib *insertBuilder) replaceDefaultExprs(inRows *tree.Select) (outRows *tree.Select) {
	values := ib.extractValuesInput(inRows)
	if values == nil {
		return inRows
	}

	// Ensure that the number of input columns exactly matches the number of
	// target columns.
	numCols := len(values.Rows[0])
	ib.checkNumCols(len(ib.targetColList), numCols)

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

				val = ib.parseDefaultOrComputedExpr(ib.targetColList[itup])
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

// buildInputRows constructs the memo group for the input expression and
// constructs a new output scope containing that expression's output columns.
func (ib *insertBuilder) buildInputRows(inScope *scope, inputRows *tree.Select) {
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
	if len(ib.targetColList) != 0 {
		desiredTypes = make([]types.T, len(ib.targetColList))
		for i, colID := range ib.targetColList {
			desiredTypes[i] = ib.md.ColumnType(colID)
		}
	} else {
		desiredTypes = make([]types.T, 0, ib.tab.ColumnCount())
		for i, n := 0, ib.tab.ColumnCount(); i < n; i++ {
			tabCol := ib.tab.Column(i)
			if !tabCol.IsHidden() {
				desiredTypes = append(desiredTypes, tabCol.DatumType())
			}
		}
	}

	ib.outScope = ib.b.buildSelect(inputRows, desiredTypes, inScope)

	if len(ib.targetColList) != 0 {
		// Target columns already exist, so ensure that the number of input
		// columns exactly matches the number of target columns.
		ib.checkNumCols(len(ib.targetColList), len(ib.outScope.cols))
	} else {
		// No target columns have been added by previous steps, so add columns
		// that are implicitly targeted by the input expression.
		ib.addTargetTableCols(len(ib.outScope.cols))
	}

	// Type check input columns.
	for i := range ib.outScope.cols {
		inCol := &ib.outScope.cols[i]
		tabCol := ib.tab.Column(ib.md.ColumnOrdinal(ib.targetColList[i]))
		checkDatumTypeFitsColumnType(tabCol, inCol.typ)
	}
}

// buildEmptyInput constructs a new output scope containing a single row VALUES
// expression with zero columns.
func (ib *insertBuilder) buildEmptyInput(inScope *scope) {
	ib.outScope = inScope.push()
	ib.outScope.expr = ib.b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, opt.ColList{})
}

// addDefaultAndComputedCols wraps the input expression with Project operator(s)
// containing any default (or nullable) and computed columns that are not yet
// part of the target column list. This includes mutation columns, since they
// must always have default or computed values.
//
// After this call, the input expression will provide values for every one of
// the target table columns, whether it was explicitly specified or implicitly
// added.
func (ib *insertBuilder) addDefaultAndComputedCols() {
	// Add any missing default and nullable columns.
	ib.addSynthesizedCols(func(tabCol opt.Column) bool { return !tabCol.IsComputed() })

	// Add any missing computed columns. This must be done after adding default
	// columns above, because computed columns can depend on default columns.
	ib.addSynthesizedCols(func(tabCol opt.Column) bool { return tabCol.IsComputed() })
}

// addSynthesizedCols is a helper method for addDefaultAndComputedCols that
// scans the list of table columns, looking for any that do not yet have values
// provided by the input expression. New columns are synthesized for any missing
// columns, as long as the addCol callback function returns true for that
// column.
func (ib *insertBuilder) addSynthesizedCols(addCol func(tabCol opt.Column) bool) {
	var projectionsScope *scope

	for i, n := 0, ib.tab.ColumnCount()+ib.tab.MutationColumnCount(); i < n; i++ {
		// Skip columns that are already specified.
		tabColID := ib.tabID.ColumnID(i)
		if ib.targetColSet.Contains(int(tabColID)) {
			continue
		}

		// Get column metadata, including any mutation columns.
		tabCol := tableColumnByOrdinal(ib.tab, i)

		// Invoke addCol to determine whether column should be added.
		if !addCol(tabCol) {
			continue
		}

		// Construct a new Project operator that will contain the newly synthesized
		// column(s).
		if projectionsScope == nil {
			projectionsScope = ib.outScope.replace()
			projectionsScope.appendColumnsFromScope(ib.outScope)
			projectionsScope.copyOrdering(ib.outScope)
		}

		expr := ib.parseDefaultOrComputedExpr(tabColID)
		texpr := ib.outScope.resolveType(expr, tabCol.DatumType())
		scopeCol := ib.b.addColumn(projectionsScope, "" /* label */, texpr)
		ib.b.buildScalar(texpr, ib.outScope, projectionsScope, scopeCol, nil)

		ib.targetColList = append(ib.targetColList, tabColID)
		ib.targetColSet.Add(int(tabColID))
	}

	if projectionsScope != nil {
		ib.b.constructProjectForScope(ib.outScope, projectionsScope)
		ib.outScope = projectionsScope
	}

	// Alias output columns using table column names. Computed columns may refer
	// to other columns in the table by name.
	for i := range ib.outScope.cols {
		ib.outScope.cols[i].name = tree.Name(ib.md.ColumnLabel(ib.targetColList[i]))
	}
}

// buildInsert constructs an Insert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause. Insert always
// returns columns in the same order and with the same names as the target
// table.
func (ib *insertBuilder) buildInsert(returning tree.ReturningExprs) {
	if len(ib.outScope.cols) != len(ib.targetColList) {
		panic("expected input column count to match table column coun")
	}

	// Map unordered input columns to order of target table columns.
	inputCols := make(opt.ColList, len(ib.outScope.cols))
	for i := range ib.outScope.cols {
		tabOrd := ib.md.ColumnOrdinal(ib.targetColList[i])
		inputCols[tabOrd] = ib.outScope.cols[i].id
	}

	private := memo.InsertPrivate{
		Table:       ib.tabID,
		InputCols:   inputCols,
		NeedResults: returning != nil,
	}
	private.Ordering.FromOrdering(ib.outScope.ordering)
	ib.outScope.expr = ib.b.factory.ConstructInsert(ib.outScope.expr, &private)

	if returning != nil {
		// 1. Project only non-mutation columns.
		// 2. Re-order columns so they're in same order as table columns.
		// 3. Alias columns to use table column names.
		// 4. Mark hidden columns.
		inScope := ib.outScope.replace()
		inScope.expr = ib.outScope.expr
		inScope.cols = make([]scopeColumn, ib.tab.ColumnCount())
		for i := range ib.outScope.cols {
			targetColID := ib.targetColList[i]
			ord := ib.md.ColumnOrdinal(targetColID)
			if ord >= ib.tab.ColumnCount() {
				// Exclude mutation columns.
				continue
			}

			outCol := &ib.outScope.cols[i]
			inScope.cols[ord] = *outCol
			inScope.cols[ord].table = *ib.alias
			inScope.cols[ord].name = ib.tab.Column(ord).ColName()

			if ib.tab.Column(ord).IsHidden() {
				inScope.cols[ord].hidden = true
			}
		}

		outScope := inScope.replace()
		ib.b.analyzeReturningList(returning, nil /* desiredTypes */, inScope, outScope)
		ib.b.buildProjectionList(inScope, outScope)
		ib.b.constructProjectForScope(inScope, outScope)
		ib.outScope = outScope
	} else {
		ib.outScope = &scope{builder: ib.b, expr: ib.outScope.expr}
	}
}

// checkNumCols raises an error if the expected number of columns does not match
// the actual number of columns.
func (ib *insertBuilder) checkNumCols(expected, actual int) {
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
func (ib *insertBuilder) parseDefaultOrComputedExpr(colID opt.ColumnID) tree.Expr {
	if ib.parsedExprs == nil {
		ib.parsedExprs = make([]tree.Expr, ib.tab.ColumnCount()+ib.tab.MutationColumnCount())
	}

	// Return expression from cache, if it was already parsed previously.
	ord := ib.md.ColumnOrdinal(colID)
	if ib.parsedExprs[ord] != nil {
		return ib.parsedExprs[ord]
	}

	var exprStr string
	tabCol := tableColumnByOrdinal(ib.tab, ord)
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

	ib.parsedExprs[ord] = expr
	return ib.parsedExprs[ord]
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

// tableColumnByOrdinal returns the table column with the given ordinal
// position, including any mutation columns, as if they were appended to end of
// regular column list.
func tableColumnByOrdinal(tab opt.Table, ord int) opt.Column {
	if ord < tab.ColumnCount() {
		return tab.Column(ord)
	}
	return tab.MutationColumn(ord - tab.ColumnCount())
}
