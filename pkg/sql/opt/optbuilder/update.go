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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
	if upd.OrderBy != nil && upd.Limit == nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"UPDATE statement requires LIMIT when ORDER BY is used"))
	}

	// UX friendliness safeguard.
	if upd.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(pgerror.DangerousStatementf("UPDATE without WHERE clause"))
	}

	// Find which table we're working on, check the permissions.
	tab, depName, alias, refColumns := b.resolveTableForMutation(upd.Table, privilege.UPDATE)

	if refColumns != nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"cannot specify a list of column IDs with UPDATE"))
	}

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(depName, tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "update", tab, alias)

	// Build the input expression that selects the rows that will be updated:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the update table will be projected.
	mb.buildInputForUpdate(inScope, upd.Table, upd.From, upd.Where, upd.Limit, upd.OrderBy)

	// Derive the columns that will be updated from the SET expressions.
	mb.addTargetColsForUpdate(upd.Exprs)

	// Build each of the SET expressions.
	mb.addUpdateCols(upd.Exprs)

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
		panic(errors.AssertionFailedf("addTargetColsForUpdate cannot be called more than once"))
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
				desiredTypes := make([]*types.T, len(expr.Names))
				targetIdx := len(mb.targetColList) - len(expr.Names)
				for i := range desiredTypes {
					desiredTypes[i] = mb.md.ColumnMeta(mb.targetColList[targetIdx+i]).Type
				}
				outScope := mb.b.buildSelectStmt(t.Select, noRowLocking, desiredTypes, mb.outScope)
				mb.subqueries = append(mb.subqueries, outScope)
				n = len(outScope.cols)

			case *tree.Tuple:
				n = len(t.Exprs)
			}
			if n < 0 {
				panic(unimplementedWithIssueDetailf(35713, fmt.Sprintf("%T", expr.Expr),
					"source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression; not supported: %T", expr.Expr))
			}
			if len(expr.Names) != n {
				panic(pgerror.Newf(pgcode.Syntax,
					"number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n))
			}
		}
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

	checkCol := func(sourceCol *scopeColumn, targetColID opt.ColumnID) {
		// Type check the input expression against the corresponding table column.
		ord := mb.tabID.ColumnOrdinal(targetColID)
		checkDatumTypeFitsColumnType(mb.tab.Column(ord), sourceCol.typ)

		// Add source column ID to the list of columns to update.
		mb.updateColIDs[ord] = sourceCol.id
	}

	addCol := func(expr tree.Expr, targetColID opt.ColumnID) {
		// If the expression is already a scopeColumn, we can skip creating a
		// new scopeColumn and proceed with type checking and adding the column
		// to the list of source columns to update. The expression can be a
		// scopeColumn when addUpdateCols is called from the
		// onUpdateCascadeBuilder while building foreign key cascading updates.
		//
		// The input scopeColumn is a pointer to a column in mb.outScope. It was
		// copied by value to projectionsScope. The checkCol function mutates
		// the name of projected columns, so we must lookup the column in
		// projectionsScope so that the correct scopeColumn is renamed.
		if scopeCol, ok := expr.(*scopeColumn); ok {
			checkCol(projectionsScope.getColumn(scopeCol.id), targetColID)
			return
		}

		// Allow right side of SET to be DEFAULT.
		if _, ok := expr.(tree.DefaultVal); ok {
			expr = mb.parseDefaultOrComputedExpr(targetColID)
		}

		// Add new column to the projections scope.
		targetColMeta := mb.md.ColumnMeta(targetColID)
		desiredType := targetColMeta.Type
		texpr := inScope.resolveType(expr, desiredType)
		colName := scopeColName(tree.Name(targetColMeta.Alias)).WithMetadataName(
			targetColMeta.Alias + "_new",
		)
		scopeCol := projectionsScope.addColumn(colName, texpr)
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
					ord := mb.tabID.ColumnOrdinal(mb.targetColList[n])
					subqueryScope.cols[i].name = scopeColName(mb.tab.Column(ord).ColName())
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
					mb.b.factory.ConstructMax1Row(subqueryScope.expr, multiRowSubqueryErrText),
					memo.TrueFilter,
					memo.EmptyJoinPrivate,
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

	// Add additional columns for computed expressions that may depend on the
	// updated columns.
	mb.addSynthesizedColsForUpdate()
}

// addSynthesizedColsForUpdate wraps an Update input expression with a Project
// operator containing any computed columns that need to be updated. This
// includes write-only mutation columns that are computed.
func (mb *mutationBuilder) addSynthesizedColsForUpdate() {
	// Allow mutation columns to be referenced by other computed mutation
	// columns (otherwise the scope will raise an error if a mutation column
	// is referenced). These do not need to be set back to true again because
	// mutation columns are not projected by the Update operator.
	for i := range mb.outScope.cols {
		mb.outScope.cols[i].mutation = false
	}

	// Add non-computed columns that are being dropped or added (mutated) to the
	// table. These are not visible to queries, and will always be updated to
	// their default values. This is necessary because they may not yet have been
	// set by the backfiller.
	mb.addSynthesizedDefaultCols(mb.updateColIDs, false /* includeOrdinary */)

	// Possibly round DECIMAL-related columns containing update values. Do
	// this before evaluating computed expressions, since those may depend on
	// the inserted columns.
	mb.roundDecimalValues(mb.updateColIDs, false /* roundComputedCols */)

	// Disambiguate names so that references in the computed expression refer to
	// the correct columns.
	mb.disambiguateColumns()

	// Add all computed columns in case their values have changed.
	mb.addSynthesizedComputedCols(mb.updateColIDs, true /* restrict */)

	// Possibly round DECIMAL-related computed columns.
	mb.roundDecimalValues(mb.updateColIDs, true /* roundComputedCols */)
}

// buildUpdate constructs an Update operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpdate(returning tree.ReturningExprs) {
	// Disambiguate names so that references in any expressions, such as a
	// check constraint, refer to the correct columns.
	mb.disambiguateColumns()

	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols(true /* isUpdate */)

	// Add the partial index predicate expressions to the table metadata.
	// These expressions are used to prune fetch columns during
	// normalization.
	mb.b.addPartialIndexPredicatesForTable(mb.md.TableMeta(mb.tabID), nil /* scan */)

	// Project partial index PUT and DEL boolean columns.
	mb.projectPartialIndexPutAndDelCols()

	mb.buildUniqueChecksForUpdate()

	mb.buildFKChecksForUpdate()

	private := mb.makeMutationPrivate(returning != nil)
	for _, col := range mb.extraAccessibleCols {
		if col.id != 0 {
			private.PassthroughCols = append(private.PassthroughCols, col.id)
		}
	}
	mb.outScope.expr = mb.b.factory.ConstructUpdate(
		mb.outScope.expr, mb.uniqueChecks, mb.fkChecks, private,
	)
	mb.buildReturning(returning)
}
