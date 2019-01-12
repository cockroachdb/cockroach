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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

// excludedTableName is the name of a special Upsert data source. When a row
// cannot be inserted due to a conflict, the "excluded" data source contains
// that row, so that its columns can be referenced in the conflict clause:
//
//   INSERT INTO ab VALUES (1, 2) ON CONFLICT (a) DO UPDATE b=excluded.b+1
//
// It is located in the special crdb_internal schema so that it never overlaps
// with user data sources.
var excludedTableName tree.TableName

func init() {
	// Clear explicit schema and catalog so that they're not printed in error
	// messages.
	excludedTableName = tree.MakeTableNameWithSchema("", "crdb_internal", "excluded")
	excludedTableName.ExplicitSchema = false
	excludedTableName.ExplicitCatalog = false
}

// buildInsert builds a memo group for an InsertOp or UpsertOp expression. To
// begin, an input expression is constructed which outputs these columns to
// insert into the target table:
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
//   SELECT ins_a, ins_b, ins_c, ins_b + ins_c AS ins_d
//   FROM (VALUES (1, NULL, 10)) AS t(ins_a, ins_b, ins_c)
//
// If an ON CONFLICT clause is present (or if it was an UPSERT statement), then
// additional columns are added to the input expression:
//
//   1. Columns containing existing values fetched from the target table and
//      used to detect conflicts and to formulate the key/value update commands.
//
//   2. Columns containing updated values to set when a conflict is detected, as
//      specified by the user.
//
//   3. Computed columns which will be updated when a conflict is detected and
//      that are dependent on one or more updated columns.
//
// In addition, the insert and update column expressions are merged into a
// single set of upsert column expressions that toggle between the insert and
// update values depending on whether the canary column is null.
//
// For example, if this is the schema and INSERT..ON CONFLICT statement:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   INSERT INTO abc VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b=10
//
// Then an input expression equivalent to this would be built:
//
//   SELECT
//     fetch_a,
//     fetch_b,
//     fetch_c,
//     CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//     CASE WHEN fetch_a IS NULL ins_b ELSE 10 END AS ups_b,
//     CASE WHEN fetch_a IS NULL ins_c ELSE fetch_c END AS ups_c,
//   FROM (VALUES (1, 2, NULL)) AS ins(ins_a, ins_b, ins_c)
//   LEFT OUTER JOIN abc AS fetch(fetch_a, fetch_b, fetch_c)
//   ON ins_a = fetch_a
//
// The CASE expressions will often prevent the unnecessary evaluation of the
// update expression in the case where an insertion needs to occur. In addition,
// it simplifies logical property calculation, since a 1:1 mapping to each
// target table column from a corresponding input column is maintained.
//
// If the ON CONFLICT clause contains a DO NOTHING clause, then each UNIQUE
// index on the target table requires its own LEFT OUTER JOIN to check whether a
// conflict exists. For example:
//
//   CREATE TABLE ab (a INT PRIMARY KEY, b INT)
//   INSERT INTO ab (a, b) VALUES (1, 2) ON CONFLICT DO NOTHING
//
// Then an input expression equivalent to this would be built:
//
//   SELECT x, y
//   FROM (VALUES (1, 2)) AS input(x, y)
//   LEFT OUTER JOIN ab
//   ON input.x = ab.a
//   WHERE ab.a IS NULL
//
// Note that an ordered input to the INSERT does not provide any guarantee about
// the order in which mutations are applied, or the order of any returned rows
// (i.e. it won't become a physical property required of the Insert or Upsert
// operator). Not propagating input orderings avoids an extra sort when the
// ON CONFLICT clause is present, since it joins a new set of rows to the input
// and thereby scrambles the input ordering.
func (b *Builder) buildInsert(ins *tree.Insert, inScope *scope) (outScope *scope) {
	// Put UPSERT behind feature flag.
	if ins.OnConflict != nil && !b.evalCtx.SessionData.OptimizerMutations {
		panic(unimplementedf("cost-based optimizer is not planning UPSERT statements"))
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

	if ins.OnConflict != nil {
		// UPSERT and INDEX ON CONFLICT will read from the table to check for
		// duplicates.
		b.checkPrivilege(tab, privilege.SELECT)

		if !ins.OnConflict.DoNothing {
			// UPSERT and INDEX ON CONFLICT DO UPDATE may modify rows if the
			// DO NOTHING clause is not present.
			b.checkPrivilege(tab, privilege.UPDATE)
		}
	}

	var mb mutationBuilder
	mb.init(b, opt.InsertOp, tab, alias)

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
		mb.addTargetNamedColsForInsert(ins.Columns)
	} else {
		values := mb.extractValuesInput(ins.Rows)
		if values != nil {
			// Target columns are implicitly targeted by VALUES expression in the
			// same order they appear in the target table schema.
			mb.addTargetTableColsForInsert(len(values.Rows[0]))
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
		rows := mb.replaceDefaultExprs(ins.Rows)

		mb.buildInputForInsert(inScope, rows)
	} else {
		mb.buildInputForInsert(inScope, nil /* rows */)
	}

	// Add default and computed columns that were not explicitly specified by
	// name or implicitly targeted by input columns. This includes any columns
	// undergoing write mutations, as they must always have a default or computed
	// value.
	mb.addDefaultAndComputedColsForInsert()

	var returning tree.ReturningExprs
	if resultsNeeded(ins.Returning) {
		returning = *ins.Returning.(*tree.ReturningExprs)
	}

	switch {
	// Case 1: Simple INSERT statement.
	case ins.OnConflict == nil:
		// Build the final insert statement, including any returned expressions.
		mb.buildInsert(returning)

	// Case 2: INSERT..ON CONFLICT DO NOTHING.
	case ins.OnConflict.DoNothing:
		// Wrap the input in one LEFT OUTER JOIN per UNIQUE index, and filter out
		// rows that have conflicts. See the buildInputForDoNothing comment for
		// more details.
		mb.buildInputForDoNothing(inScope, ins.OnConflict)

		// Since buildInputForDoNothing filters out rows with conflicts, always
		// insert rows that are not filtered.
		mb.buildInsert(returning)

	// Case 3: UPSERT statement.
	case ins.OnConflict.IsUpsertAlias():
		// Left-join each input row to the target table, using conflict columns
		// derived from the primary index as the join condition.
		mb.buildInputForUpsert(inScope, mb.getPrimaryKeyColumnNames(), nil)

		// Add columns which will be updated by the Upsert when a conflict occurs.
		// These are derived from the insert columns.
		mb.setUpsertCols(ins.Columns)

		// Add additional columns for computed expressions that may depend on any
		// updated columns.
		mb.addComputedColsForUpdate()

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning)

	// Case 4: INSERT..ON CONFLICT..DO UPDATE statement.
	default:
		// Left-join each input row to the target table, using the conflict columns
		// as the join condition.
		mb.buildInputForUpsert(inScope, ins.OnConflict.Columns, ins.OnConflict.Where)

		// Derive the columns that will be updated from the SET expressions.
		mb.addTargetColsForUpdate(ins.OnConflict.Exprs)

		// Build each of the SET expressions.
		mb.addUpdateCols(ins.OnConflict.Exprs)

		// Add additional columns for computed expressions that may depend on any
		// updated columns.
		mb.addComputedColsForUpdate()

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning)
	}

	return mb.outScope
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
	primary := mb.tab.Index(cat.PrimaryIndex)
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

		panic(builderError{pgerror.NewErrorf(pgerror.CodeInvalidForeignKeyError,
			"missing %q primary key column", col.Column.ColName())})
	}
}

// checkForeignKeysForInsert ensures that all composite foreign keys that
// specify the matching method as MATCH FULL have all of their columns assigned
// values by the INSERT statement, or else have default/computed values.
// Alternatively, all columns can be unspecified. If neither condition is true,
// checkForeignKeys raises an error. Here is an example:
//
//   CREATE TABLE orders (
//     id INT,
//     cust_id INT,
//     state STRING,
//     FOREIGN KEY (cust_id, state) REFERENCES customers (id, state) MATCH FULL
//   )
//
//   INSERT INTO orders (cust_id) VALUES (1)
//
// This INSERT statement would trigger a static error, because only cust_id is
// specified in the INSERT statement. Either the state column must be specified
// as well, or else neither column can be specified.
func (mb *mutationBuilder) checkForeignKeysForInsert() {
	for i, n := 0, mb.tab.IndexCount(); i < n; i++ {
		idx := mb.tab.Index(i)
		fkey, ok := idx.ForeignKey()
		if !ok {
			continue
		}

		// This check should only be performed on composite foreign keys that use
		// the MATCH FULL method.
		if fkey.Match != tree.MatchFull {
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

// buildInputForInsert constructs the memo group for the input expression and
// constructs a new output scope containing that expression's output columns.
func (mb *mutationBuilder) buildInputForInsert(inScope *scope, inputRows *tree.Select) {
	mb.insertColList = make(opt.ColList, cap(mb.targetColList))

	// Handle DEFAULT VALUES case by creating a single empty row as input.
	if inputRows == nil {
		mb.outScope = inScope.push()
		mb.outScope.expr = mb.b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, opt.ColList{})
		return
	}

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
			desiredTypes[i] = mb.md.ColumnMeta(colID).Type
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
		ord := mb.tabID.ColumnOrdinal(mb.targetColList[i])

		// Type check the input column against the corresponding table column.
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), inCol.typ)

		// Assign name of input column. Computed columns can refer to this column
		// by its name.
		inCol.table = *mb.tab.Name()
		inCol.name = tree.Name(mb.md.ColumnMeta(mb.targetColList[i]).Alias)

		// Map the ordinal position of each table column to the id of the input
		// column which will be inserted into that position.
		mb.insertColList[ord] = inCol.id
	}
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
		func(tabCol cat.Column) bool { return !tabCol.IsComputed() },
	)

	// Add any missing computed columns. This must be done after adding default
	// columns above, because computed columns can depend on default columns.
	mb.addSynthesizedCols(
		mb.insertColList,
		func(tabCol cat.Column) bool { return tabCol.IsComputed() },
	)
}

// buildInsert constructs an Insert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildInsert(returning tree.ReturningExprs) {
	private := memo.MutationPrivate{
		Table:       mb.tabID,
		InsertCols:  mb.insertColList,
		NeedResults: returning != nil,
	}
	mb.outScope.expr = mb.b.factory.ConstructInsert(mb.outScope.expr, &private)

	mb.buildReturning(returning)
}

// buildInputForDoNothing wraps the input expression in LEFT OUTER JOIN
// expressions, one for each UNIQUE index on the target table. It then adds a
// filter that discards rows that have a conflict (by checking a not-null table
// column to see if it was null-extended by the left join). See the comment
// header for Builder.buildInsert for an example.
func (mb *mutationBuilder) buildInputForDoNothing(inScope *scope, onConflict *tree.OnConflict) {
	// DO NOTHING clause does not require ON CONFLICT columns.
	var conflictIndex cat.Index
	if len(onConflict.Columns) != 0 {
		// Check that the ON CONFLICT columns reference at most one target row by
		// ensuring they match columns of a UNIQUE index. Using LEFT OUTER JOIN
		// to detect conflicts relies upon this being true (otherwise result
		// cardinality could increase). This is also a Postgres requirement.
		conflictIndex = mb.ensureUniqueConflictCols(onConflict.Columns)
	}

	insertColSet := mb.outScope.expr.Relational().OutputCols

	// Loop over each UNIQUE index, potentially creating a left join + filter for
	// each one.
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)
		if !index.IsUnique() {
			continue
		}

		// If conflict columns were explicitly specified, then only check for a
		// conflict on a single index. Otherwise, check on all indexes.
		if conflictIndex != nil && conflictIndex != index {
			continue
		}

		// Build the right side of the left outer join.
		tn := mb.tab.Name().TableName
		alias := tree.MakeUnqualifiedTableName(tree.Name(fmt.Sprintf("%s_%d", tn, idx+1)))
		scanScope := mb.b.buildScan(
			mb.tab,
			&alias,
			nil, /* ordinals */
			nil, /* indexFlags */
			excludeMutations,
			inScope,
		)

		// Add the scan columns to the current scope. It's OK to modify the current
		// scope because it contains only INSERT columns that were added by the
		// mutationBuilder, and which aren't needed for any other purpose.
		mb.outScope.appendColumnsFromScope(scanScope)

		// Remember the column ID of a scan column that is not null. This will be
		// used to detect whether a conflict was detected for a row. Such a column
		// must always exist, since the index always contains the primary key
		// columns, either explicitly or implicitly.
		notNullColID := scanScope.cols[findNotNullIndexCol(index)].id

		// Build the join condition by creating a conjunction of equality conditions
		// that test each conflict column:
		//
		//   ON ins.x = scan.a AND ins.y = scan.b
		//
		var on memo.FiltersExpr
		for i, n := 0, index.LaxKeyColumnCount(); i < n; i++ {
			indexCol := index.Column(i)
			scanColID := scanScope.cols[indexCol.Ordinal].id

			condition := mb.b.factory.ConstructEq(
				mb.b.factory.ConstructVariable(mb.insertColList[indexCol.Ordinal]),
				mb.b.factory.ConstructVariable(scanColID),
			)
			on = append(on, memo.FiltersItem{Condition: condition})
		}

		// Construct the left join + filter.
		// TODO(andyk): Convert this to use anti-join once we have support for
		// lookup anti-joins.
		mb.outScope.expr = mb.b.factory.ConstructProject(
			mb.b.factory.ConstructSelect(
				mb.b.factory.ConstructLeftJoin(
					mb.outScope.expr,
					scanScope.expr,
					on,
				),
				memo.FiltersExpr{memo.FiltersItem{
					Condition: mb.b.factory.ConstructIs(
						mb.b.factory.ConstructVariable(notNullColID),
						memo.NullSingleton,
					),
				}},
			),
			memo.EmptyProjectionsExpr,
			insertColSet,
		)
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.ColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// buildInputForUpsert assumes that the output scope already contains the insert
// columns. It left-joins each insert row to the target table, using the given
// conflict columns as the join condition. It also selects one of the table
// columns to be a "canary column" that can be tested to determine whether a
// given insert row conflicts with an existing row in the table. If it is null,
// then there is no conflict.
func (mb *mutationBuilder) buildInputForUpsert(
	inScope *scope, conflictCols tree.NameList, whereClause *tree.Where,
) {
	// Check that the ON CONFLICT columns reference at most one target row.
	// Using LEFT OUTER JOIN to detect conflicts relies upon this being true
	// (otherwise result cardinality could increase). This is also a Postgres
	// requirement.
	mb.ensureUniqueConflictCols(conflictCols)

	// Re-alias all INSERT columns so that they are accessible as if they were
	// part of a special data source named "crdb_internal.excluded".
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].table = excludedTableName
	}

	// Build the right side of the left outer join. Include mutation columns
	// because they can be used by computed update expressions.
	fetchScope := mb.b.buildScan(
		mb.tab,
		mb.alias,
		nil, /* ordinals */
		nil, /* indexFlags */
		includeMutations,
		inScope,
	)

	// Record a not-null "canary" column. After the left-join, this will be null
	// if no conflict has been detected, or not null otherwise. At least one not-
	// null column must exist, since primary key columns are not-null.
	canaryScopeCol := &fetchScope.cols[findNotNullIndexCol(mb.tab.Index(cat.PrimaryIndex))]
	mb.canaryColID = canaryScopeCol.id

	// Set list of columns that will be fetched by the input expression.
	mb.fetchColList = make(opt.ColList, cap(mb.targetColList))
	for i := range fetchScope.cols {
		mb.fetchColList[i] = fetchScope.cols[i].id
	}

	// Add the fetch columns to the current scope. It's OK to modify the current
	// scope because it contains only INSERT columns that were added by the
	// mutationBuilder, and which aren't needed for any other purpose.
	mb.outScope.appendColumnsFromScope(fetchScope)

	// Build the join condition by creating a conjunction of equality conditions
	// that test each conflict column:
	//
	//   ON ins.x = scan.a AND ins.y = scan.b
	//
	var on memo.FiltersExpr
	for _, name := range conflictCols {
		for i := range fetchScope.cols {
			fetchCol := &fetchScope.cols[i]
			if fetchCol.name == name {
				condition := mb.b.factory.ConstructEq(
					mb.b.factory.ConstructVariable(mb.insertColList[i]),
					mb.b.factory.ConstructVariable(fetchCol.id),
				)
				on = append(on, memo.FiltersItem{Condition: condition})
				break
			}
		}
	}

	// Construct the left join.
	mb.outScope.expr = mb.b.factory.ConstructLeftJoin(
		mb.outScope.expr,
		fetchScope.expr,
		on,
	)

	// Add a filter from the WHERE clause if one exists.
	if whereClause != nil {
		where := &tree.Where{
			Type: whereClause.Type,
			Expr: &tree.OrExpr{
				Left: &tree.ComparisonExpr{
					Operator: tree.IsNotDistinctFrom,
					Left:     canaryScopeCol,
					Right:    tree.DNull,
				},
				Right: whereClause.Expr,
			},
		}
		mb.b.buildWhere(where, mb.outScope)
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.ColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// setUpsertCols sets the list of columns to be updated in case of conflict.
// There are two cases to handle:
//
//   1. Target columns are explicitly specified:
//        UPSERT INTO abc (col1, col2, ...) <input-expr>
//
//   2. Target columns are implicitly derived:
//        UPSERT INTO abc <input-expr>
//
// In case #1, only the columns that were specified by the user and that are not
// primary key columns will be updated. In case #2, all columns in the table
// that are not primary key columns will be updated.
func (mb *mutationBuilder) setUpsertCols(insertCols tree.NameList) {
	mb.updateColList = make(opt.ColList, len(mb.insertColList))
	if len(insertCols) != 0 {
		for _, name := range insertCols {
			// Table column must exist, since existence of insertCols has already
			// been checked previously.
			ord := cat.FindTableColumnByName(mb.tab, name)
			mb.updateColList[ord] = mb.insertColList[ord]
		}
	} else {
		copy(mb.updateColList, mb.insertColList)
	}

	// Don't need to update primary key columns because the update only happens
	// when those columns are equal to the existing columns (i.e. they conflict).
	conflictIndex := mb.tab.Index(cat.PrimaryIndex)
	for i, n := 0, conflictIndex.KeyColumnCount(); i < n; i++ {
		mb.updateColList[conflictIndex.Column(i).Ordinal] = 0
	}
}

// buildUpsert constructs an Upsert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpsert(returning tree.ReturningExprs) {
	mb.projectUpsertColumns()

	private := memo.MutationPrivate{
		Table:       mb.tabID,
		InsertCols:  mb.insertColList,
		FetchCols:   mb.fetchColList,
		UpdateCols:  mb.updateColList,
		CanaryCol:   mb.canaryColID,
		NeedResults: returning != nil,
	}
	mb.outScope.expr = mb.b.factory.ConstructUpsert(mb.outScope.expr, &private)

	mb.buildReturning(returning)
}

// projectUpsertColumns projects a set of merged columns that will be either
// inserted into the target table, or else used to update an existing row,
// depending on whether the canary column is null. For example:
//
//   UPSERT INTO ab VALUES (ins_a, ins_b) ON CONFLICT (a) DO UPDATE SET b=upd_b
//
// will cause the columns to be projected:
//
//   SELECT
//     fetch_a,
//     fetch_b,
//     CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//     CASE WHEN fetch_b IS NULL ins_b ELSE upd_b END AS ups_b,
//   FROM (SELECT ins_a, ins_b, upd_b, fetch_a, fetch_b FROM ...)
//
// For each column, a CASE expression is created that toggles between the insert
// and update values depending on whether the canary column is null. These
// columns can then feed into any constraint checking expressions, which operate
// on the final result values.
func (mb *mutationBuilder) projectUpsertColumns() {
	projectionsScope := mb.outScope.replace()
	projectionsScope.cols = make([]scopeColumn, 0, len(mb.outScope.cols))

	addAnonymousColumn := func(id opt.ColumnID) *scopeColumn {
		projectionsScope.cols = append(projectionsScope.cols, scopeColumn{
			typ: mb.md.ColumnMeta(id).Type,
			id:  id,
		})
		return &projectionsScope.cols[len(projectionsScope.cols)-1]
	}

	// Pass through all fetch columns. This always includes the canary column.
	fetchColSet := mb.fetchColList.ToSet()
	for i := range mb.outScope.cols {
		col := &mb.outScope.cols[i]
		if fetchColSet.Contains(int(col.id)) {
			// Don't copy the column's name, since fetch columns can no longer be
			// referenced by expressions, such as any check constraints.
			addAnonymousColumn(col.id)
		}
	}

	// Project a column for each target table column that needs to be either
	// inserted or updated.
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		insertColID := mb.insertColList[i]
		updateColID := mb.updateColList[i]
		if updateColID == 0 {
			updateColID = mb.fetchColList[i]
		}

		var scopeCol *scopeColumn
		switch {
		case insertColID == 0 && updateColID == 0:
			// Neither insert nor update required for this column, so skip.
			continue

		case insertColID == 0:
			// No insert is required, so just pass through update column.
			scopeCol = addAnonymousColumn(updateColID)

		case updateColID == 0:
			// No update is required, so just pass through insert column.
			scopeCol = addAnonymousColumn(insertColID)

		default:
			// Generate CASE that toggles between insert and update column.
			caseExpr := mb.b.factory.ConstructCase(
				memo.TrueSingleton,
				memo.ScalarListExpr{
					mb.b.factory.ConstructWhen(
						mb.b.factory.ConstructIs(
							mb.b.factory.ConstructVariable(mb.canaryColID),
							memo.NullSingleton,
						),
						mb.b.factory.ConstructVariable(insertColID),
					),
				},
				mb.b.factory.ConstructVariable(updateColID),
			)

			alias := fmt.Sprintf("upsert_%s", mb.tab.Column(i).ColName())
			typ := mb.md.ColumnMeta(insertColID).Type
			scopeCol = mb.b.synthesizeColumn(projectionsScope, alias, typ, nil /* expr */, caseExpr)
		}

		// Assign name to synthesized column. Check constraint columns may refer
		// to columns in the table by name.
		scopeCol.table = *mb.tab.Name()
		scopeCol.name = mb.tab.Column(i).ColName()

		// Update the insert and update column list to point to the new upsert
		// column. This will be used by the Upsert operator in place of the
		// original columns.
		if mb.insertColList[i] != 0 {
			mb.insertColList[i] = scopeCol.id
		}
		if mb.updateColList[i] != 0 {
			mb.updateColList[i] = scopeCol.id
		}
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope
}

// ensureUniqueConflictCols tries to prove that the given list of column names
// correspond to the columns of at least one UNIQUE index on the target table.
// If true, then ensureUniqueConflictCols returns the matching index. Otherwise,
// it reports an error.
func (mb *mutationBuilder) ensureUniqueConflictCols(cols tree.NameList) cat.Index {
	for idx, idxCount := 0, mb.tab.IndexCount(); idx < idxCount; idx++ {
		index := mb.tab.Index(idx)

		// Skip non-unique indexes. Use lax key columns, which always contain
		// the minimum columns that ensure uniqueness. Null values are considered
		// to be *not* equal, but that's OK because the join condition rejects
		// nulls anyway.
		if !index.IsUnique() || index.LaxKeyColumnCount() != len(cols) {
			continue
		}

		found := true
		for col, colCount := 0, index.LaxKeyColumnCount(); col < colCount; col++ {
			if cols[col] != index.Column(col).Column.ColName() {
				found = false
				break
			}
		}

		if found {
			return index
		}
	}
	panic(builderError{errors.New(
		"there is no unique or exclusion constraint matching the ON CONFLICT specification")})
}

// getPrimaryKeyColumnNames returns the names of all primary key columns in the
// target table.
func (mb *mutationBuilder) getPrimaryKeyColumnNames() tree.NameList {
	pkIndex := mb.tab.Index(cat.PrimaryIndex)
	names := make(tree.NameList, pkIndex.KeyColumnCount())
	for i, n := 0, pkIndex.KeyColumnCount(); i < n; i++ {
		names[i] = pkIndex.Column(i).Column.ColName()
	}
	return names
}
