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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

// buildUpdate builds a memo group for an UpdateOp expression. First, an input
// expression is constructed that outputs the existing values for all rows from
// the target table that match the WHERE clause. Additional column(s) that
// provide updated values are projected for each of the SET expressions, as well
// as for any computed columns. For example:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   UPDATE abc SET b=1 WHERE a=2
//
// This would create an input expression similar to this SQL:
//
//   SELECT a AS oa, b AS ob, c AS oc, 1 AS nb FROM abc WHERE a=2
//
// The execution engine evaluates this relational expression and uses the
// resulting values to form the KV keys and values.
//
// Tuple SET expressions are decomposed into individual columns:
//
//   UPDATE abc SET (b, c)=(1, 2) WHERE a=3
//   =>
//   SELECT a AS oa, b AS ob, c AS oc, 1 AS nb, 2 AS nc FROM abc WHERE a=3
//
// Subqueries become correlated left outer joins:
//
//   UPDATE abc SET b=(SELECT y FROM xyz WHERE x=a)
//   =>
//   SELECT a AS oa, b AS ob, c AS oc, y AS nb
//   FROM abc
//   LEFT JOIN LATERAL (SELECT y FROM xyz WHERE x=a)
//   ON True
//
// Computed columns result in an additional wrapper projection that can depend
// on input columns.
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Update operator).
func (b *Builder) buildUpdate(upd *tree.Update, inScope *scope) (outScope *scope) {
	if !b.evalCtx.SessionData.OptimizerUpdates {
		panic(unimplementedf("cost-based optimizer is not planning UPDATE statements"))
	}

	if upd.OrderBy != nil && upd.Limit == nil {
		panic(builderError{errors.New("UPDATE statement requires LIMIT when ORDER BY is used")})
	}

	// UX friendliness safeguard.
	if upd.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(builderError{pgerror.NewDangerousStatementErrorf("UPDATE without WHERE clause")})
	}

	if upd.With != nil {
		inScope = b.buildCTE(upd.With.CTEList, inScope)
		defer b.checkCTEUsage(inScope)
	}

	// UPDATE xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(upd.Table)

	// Find which table we're working on, check the permissions.
	tab := b.resolveTable(tn, privilege.UPDATE)

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, opt.UpdateOp, tab, alias)

	// Build the input expression that selects the rows that will be updated:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the update table will be projected.
	mb.buildInputForUpdate(inScope, upd)

	// Derive the columns that will be updated from the SET expressions.
	mb.addTargetColsForUpdate(upd.Exprs)

	// Build each of the SET expressions.
	mb.addUpdateCols(upd.Exprs)

	// Add additional columns for computed expressions that may depend on the
	// updated columns.
	mb.addComputedColsForUpdate()

	// Build the final update statement, including any returned expressions.
	if resultsNeeded(upd.Returning) {
		mb.buildUpdate(*upd.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildUpdate(nil /* returning */)
	}

	return mb.outScope
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
					desiredTypes[i] = mb.md.ColumnMeta(mb.targetColList[targetIdx+i]).Type
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
		mb.alias,
		nil, /* ordinals */
		nil, /* indexFlags */
		includeMutations,
		inScope,
	)

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
	mb.fetchColList = make(opt.ColList, cap(mb.targetColList))
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
	mb.updateColList = make(opt.ColList, cap(mb.targetColList))

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

	checkCol := func(sourceCol *scopeColumn, targetColID opt.ColumnID) {
		// Type check the input expression against the corresponding table column.
		ord := mb.tabID.ColumnOrdinal(targetColID)
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), sourceCol.typ)

		// Add new column ID to the list of columns to update.
		mb.updateColList[ord] = sourceCol.id

		// Rename the column to match the target column being updated.
		sourceCol.name = mb.tab.Column(ord).ColName()
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID) {
		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}

		// Add new column to the projections scope.
		desiredType := mb.md.ColumnMeta(targetColID).Type
		texpr := inScope.resolveType(expr, desiredType)
		scopeCol := mb.b.addColumn(projectionsScope, "" /* alias */, texpr)
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

// addComputedColsForUpdate wraps an Update input expression with a Project
// operator containing any computed columns that need to be updated. This
// includes write-only mutation columns that are computed.
func (mb *mutationBuilder) addComputedColsForUpdate() {
	// Allow mutation columns to be referenced by other computed mutation
	// columns (otherwise the scope will raise an error if a mutation column
	// is referenced). These do not need to be set back to true again because
	// mutation columns are not projected by the Update operator.
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].mutation = false
	}

	// Disambiguate any existing columns of the same name so that computed
	// expressions will refer to the updated value rather than the old column
	// containing the original value (or even the column containing a value to
	// be inserted in case of upsert).
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		colName := mb.tab.Column(i).ColName()
		colID := mb.fetchColList[i]
		if mb.updateColList[i] != 0 {
			colID = mb.updateColList[i]
		}

		for i := range mb.outScope.cols {
			col := &mb.outScope.cols[i]
			if col.name == colName {
				if col.id == colID {
					// Use table name, not alias name, since computed column
					// expressions will not reference aliases.
					col.table = *mb.tab.Name()
				} else {
					// Clear name so that it will never match.
					col.table = tree.TableName{}
					col.name = ""
				}
			}
		}
	}

	mb.addSynthesizedCols(
		mb.updateColList,
		func(tabCol cat.Column) bool { return tabCol.IsComputed() },
	)
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
	mb.outScope.expr = mb.b.factory.ConstructUpdate(mb.outScope.expr, &private)

	mb.buildReturning(returning)
}
