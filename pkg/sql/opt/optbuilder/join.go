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
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// buildJoin builds a set of memo groups that represent the given join table
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildJoin(
	join *tree.JoinTableExpr, locking lockingSpec, inScope *scope,
) (outScope *scope) {
	leftScope := b.buildDataSource(join.Left, nil /* indexFlags */, locking, inScope)

	isLateral := false
	inScopeRight := inScope
	// If this is a lateral join, use leftScope as inScope for the right side.
	// The right side scope of a LATERAL join includes the columns produced by
	// the left side.
	if t, ok := join.Right.(*tree.AliasedTableExpr); ok && t.Lateral {
		telemetry.Inc(sqltelemetry.LateralJoinUseCounter)
		isLateral = true
		inScopeRight = leftScope
	}

	rightScope := b.buildDataSource(join.Right, nil /* indexFlags */, locking, inScopeRight)

	// Check that the same table name is not used on both sides.
	b.validateJoinTableNames(leftScope, rightScope)

	joinType := sqlbase.JoinTypeFromAstString(join.JoinType)
	var flags memo.JoinFlags
	switch join.Hint {
	case "":
	case tree.AstHash:
		telemetry.Inc(sqltelemetry.HashJoinHintUseCounter)
		flags = memo.AllowHashJoinStoreRight

	case tree.AstLookup:
		telemetry.Inc(sqltelemetry.LookupJoinHintUseCounter)
		flags = memo.AllowLookupJoinIntoRight
		if joinType != sqlbase.InnerJoin && joinType != sqlbase.LeftOuterJoin {
			panic(pgerror.Newf(pgcode.Syntax,
				"%s can only be used with INNER or LEFT joins", tree.AstLookup,
			))
		}

	case tree.AstMerge:
		telemetry.Inc(sqltelemetry.MergeJoinHintUseCounter)
		flags = memo.AllowMergeJoin

	default:
		panic(pgerror.Newf(
			pgcode.FeatureNotSupported, "join hint %s not supported", join.Hint,
		))
	}

	switch cond := join.Cond.(type) {
	case tree.NaturalJoinCond, *tree.UsingJoinCond:
		outScope = inScope.push()

		var jb usingJoinBuilder
		jb.init(b, joinType, flags, leftScope, rightScope, outScope)

		switch t := cond.(type) {
		case tree.NaturalJoinCond:
			jb.buildNaturalJoin(t)
		case *tree.UsingJoinCond:
			jb.buildUsingJoin(t)
		}
		return outScope

	case *tree.OnJoinCond, nil:
		// Append columns added by the children, as they are visible to the filter.
		outScope = inScope.push()
		outScope.appendColumnsFromScope(leftScope)
		outScope.appendColumnsFromScope(rightScope)

		var filters memo.FiltersExpr
		if on, ok := cond.(*tree.OnJoinCond); ok {
			// Do not allow special functions in the ON clause.
			b.semaCtx.Properties.Require("ON", tree.RejectSpecial)
			outScope.context = "ON"
			filter := b.buildScalar(
				outScope.resolveAndRequireType(on.Expr, types.Bool), outScope, nil, nil, nil,
			)
			filters = memo.FiltersExpr{b.factory.ConstructFiltersItem(filter)}
		} else {
			filters = memo.TrueFilter
		}

		left := leftScope.expr.(memo.RelExpr)
		right := rightScope.expr.(memo.RelExpr)
		outScope.expr = b.constructJoin(
			joinType, left, right, filters, &memo.JoinPrivate{Flags: flags}, isLateral,
		)
		return outScope

	default:
		panic(errors.AssertionFailedf("unsupported join condition %#v", cond))
	}
}

// validateJoinTableNames checks that table names are not repeated between the
// left and right sides of a join. leftTables contains a pre-built map of the
// tables from the left side of the join, and rightScope contains the
// scopeColumns (and corresponding table names) from the right side of the
// join.
func (b *Builder) validateJoinTableNames(leftScope, rightScope *scope) {
	// Try to derive smaller subset of columns which need to be validated.
	leftOrds := b.findJoinColsToValidate(leftScope)
	rightOrds := b.findJoinColsToValidate(rightScope)

	// Look for table name in left scope that exists in right scope.
	for left, ok := leftOrds.Next(0); ok; left, ok = leftOrds.Next(left + 1) {
		leftName := &leftScope.cols[left].table

		for right, ok := rightOrds.Next(0); ok; right, ok = rightOrds.Next(right + 1) {
			rightName := &rightScope.cols[right].table

			// Must match all name parts.
			if leftName.TableName != rightName.TableName ||
				leftName.SchemaName != rightName.SchemaName ||
				leftName.CatalogName != rightName.CatalogName {
				continue
			}

			panic(pgerror.Newf(
				pgcode.DuplicateAlias,
				"source name %q specified more than once (missing AS clause)",
				tree.ErrString(&leftName.TableName),
			))
		}
	}
}

// findJoinColsToValidate creates a FastIntSet containing the ordinal of each
// column that has a different table name than the previous column. This is a
// fast way of reducing the set of columns that need to checked for duplicate
// names by validateJoinTableNames.
func (b *Builder) findJoinColsToValidate(scope *scope) util.FastIntSet {
	var ords util.FastIntSet
	for i := range scope.cols {
		// Allow joins of sources that define columns with no
		// associated table name. At worst, the USING/NATURAL
		// detection code or expression analysis for ON will detect an
		// ambiguity later.
		if scope.cols[i].table.TableName == "" {
			continue
		}

		if i == 0 || scope.cols[i].table != scope.cols[i-1].table {
			ords.Add(i)
		}
	}
	return ords
}

var invalidLateralJoin = pgerror.New(pgcode.Syntax, "The combining JOIN type must be INNER or LEFT for a LATERAL reference")

func (b *Builder) constructJoin(
	joinType sqlbase.JoinType,
	left, right memo.RelExpr,
	on memo.FiltersExpr,
	private *memo.JoinPrivate,
	isLateral bool,
) memo.RelExpr {
	switch joinType {
	case sqlbase.InnerJoin:
		if isLateral {
			return b.factory.ConstructInnerJoinApply(left, right, on, private)
		}
		return b.factory.ConstructInnerJoin(left, right, on, private)
	case sqlbase.LeftOuterJoin:
		if isLateral {
			return b.factory.ConstructLeftJoinApply(left, right, on, private)
		}
		return b.factory.ConstructLeftJoin(left, right, on, private)
	case sqlbase.RightOuterJoin:
		if isLateral {
			panic(invalidLateralJoin)
		}
		return b.factory.ConstructRightJoin(left, right, on, private)
	case sqlbase.FullOuterJoin:
		if isLateral {
			panic(invalidLateralJoin)
		}
		return b.factory.ConstructFullJoin(left, right, on, private)
	default:
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"unsupported JOIN type %d", joinType))
	}
}

// usingJoinBuilder helps to build a USING join or natural join. It finds the
// columns in the left and right relations that match the columns provided in
// the names parameter (or names common to both sides in case of natural join),
// and creates equality predicate(s) with those columns. It also ensures that
// there is a single output column for each match name (other columns with the
// same name are hidden).
//
// -- Merged columns --
//
// With NATURAL JOIN or JOIN USING (a,b,c,...), SQL allows us to refer to the
// columns a,b,c directly; these columns have the following semantics:
//   a = IFNULL(left.a, right.a)
//   b = IFNULL(left.b, right.b)
//   c = IFNULL(left.c, right.c)
//   ...
//
// Furthermore, a star has to resolve the columns in the following order:
// merged columns, non-equality columns from the left table, non-equality
// columns from the right table. To perform this rearrangement, we use a
// projection on top of the join. Note that the original columns must
// still be accessible via left.a, right.a (they will just be hidden).
//
// For inner or left outer joins, a is always the same as left.a.
//
// For right outer joins, a is always equal to right.a; but for some types
// (like collated strings), this doesn't mean it is the same as right.a. In
// this case we must still use the IFNULL construct.
//
// Example:
//
//  left has columns (a,b,x)
//  right has columns (a,b,y)
//
//  - SELECT * FROM left JOIN right USING(a,b)
//
//  join has columns:
//    1: left.a
//    2: left.b
//    3: left.x
//    4: right.a
//    5: right.b
//    6: right.y
//
//  projection has columns and corresponding variable expressions:
//    1: a aka left.a        @1
//    2: b aka left.b        @2
//    3: left.x              @3
//    4: right.a (hidden)    @4
//    5: right.b (hidden)    @5
//    6: right.y             @6
//
// If the join was a FULL OUTER JOIN, the columns would be:
//    1: a                   IFNULL(@1,@4)
//    2: b                   IFNULL(@2,@5)
//    3: left.a (hidden)     @1
//    4: left.b (hidden)     @2
//    5: left.x              @3
//    6: right.a (hidden)    @4
//    7: right.b (hidden)    @5
//    8: right.y             @6
//
type usingJoinBuilder struct {
	b          *Builder
	joinType   sqlbase.JoinType
	joinFlags  memo.JoinFlags
	filters    memo.FiltersExpr
	leftScope  *scope
	rightScope *scope
	outScope   *scope

	// hideCols contains the ids of join columns which are hidden in the result
	// expression.
	hideCols opt.ColSet

	// showCols contains the ids of join columns which are not hidden in the
	// resultexpression.
	showCols opt.ColSet

	// ifNullCols contains the ids of each synthesized column which performs the
	// IFNULL check for a pair of join columns.
	ifNullCols opt.ColSet
}

func (jb *usingJoinBuilder) init(
	b *Builder,
	joinType sqlbase.JoinType,
	flags memo.JoinFlags,
	leftScope, rightScope, outScope *scope,
) {
	jb.b = b
	jb.joinType = joinType
	jb.joinFlags = flags
	jb.leftScope = leftScope
	jb.rightScope = rightScope
	jb.outScope = outScope
}

// buildUsingJoin constructs a Join operator with join columns matching the
// the names in the given join condition.
func (jb *usingJoinBuilder) buildUsingJoin(using *tree.UsingJoinCond) {
	var seenCols opt.ColSet
	for _, name := range using.Cols {
		// Find left and right USING columns in the scopes.
		leftCol := jb.findUsingColumn(jb.leftScope.cols, name)
		if leftCol == nil {
			jb.raiseUndefinedColError(name, "left")
		}
		if seenCols.Contains(leftCol.id) {
			// Same name exists more than once in USING column name list.
			panic(pgerror.Newf(pgcode.DuplicateColumn,
				"column %q appears more than once in USING clause", tree.ErrString(&name)))
		}
		seenCols.Add(leftCol.id)

		rightCol := jb.findUsingColumn(jb.rightScope.cols, name)
		if rightCol == nil {
			jb.raiseUndefinedColError(name, "right")
		}

		jb.addEqualityCondition(leftCol, rightCol)
	}

	jb.finishBuild()
}

// buildNaturalJoin constructs a Join operator with join columns derived from
// matching names in the left and right inputs.
func (jb *usingJoinBuilder) buildNaturalJoin(natural tree.NaturalJoinCond) {
	// Only add equality conditions for non-hidden columns with matching name in
	// both the left and right inputs.
	var seenCols opt.ColSet
	for i := range jb.leftScope.cols {
		leftCol := &jb.leftScope.cols[i]
		if leftCol.hidden {
			continue
		}
		if seenCols.Contains(leftCol.id) {
			jb.raiseDuplicateColError(leftCol.name)
		}
		seenCols.Add(leftCol.id)

		rightCol := jb.findUsingColumn(jb.rightScope.cols, leftCol.name)
		if rightCol != nil {
			jb.addEqualityCondition(leftCol, rightCol)
		}
	}

	jb.finishBuild()
}

// finishBuild adds any non-join columns to the output scope and then constructs
// the Join operator. If at least one "if null" column exists, the join must be
// wrapped in a Project operator that performs the required IFNULL checks.
func (jb *usingJoinBuilder) finishBuild() {
	jb.addRemainingCols(jb.leftScope.cols)
	jb.addRemainingCols(jb.rightScope.cols)

	jb.outScope.expr = jb.b.constructJoin(
		jb.joinType,
		jb.leftScope.expr.(memo.RelExpr),
		jb.rightScope.expr.(memo.RelExpr),
		jb.filters,
		&memo.JoinPrivate{Flags: jb.joinFlags},
		false, /* isLateral */
	)

	if !jb.ifNullCols.Empty() {
		// Wrap in a projection to include the merged columns and ensure that all
		// remaining columns are passed through unchanged.
		for i := range jb.outScope.cols {
			col := &jb.outScope.cols[i]
			if !jb.ifNullCols.Contains(col.id) {
				// Mark column as passthrough.
				col.scalar = nil
			}
		}

		jb.outScope.expr = jb.b.constructProject(jb.outScope.expr.(memo.RelExpr), jb.outScope.cols)
	}
}

// addRemainingCols iterates through each of the columns in cols and performs
// one of the following actions:
// (1) If the column is part of the hideCols set, then it is a join column that
//     needs to be added to output scope, with the hidden attribute set to true.
// (2) If the column is part of the showCols set, then it is a join column that
//     has already been added to the output scope by addEqualityCondition, so
//     skip it now.
// (3) All other columns are added to the scope without modification.
func (jb *usingJoinBuilder) addRemainingCols(cols []scopeColumn) {
	for i := range cols {
		col := &cols[i]
		switch {
		case jb.hideCols.Contains(col.id):
			jb.outScope.cols = append(jb.outScope.cols, *col)
			jb.outScope.cols[len(jb.outScope.cols)-1].hidden = true

		case !jb.showCols.Contains(col.id):
			jb.outScope.cols = append(jb.outScope.cols, *col)
		}
	}
}

// findUsingColumn finds the column in cols that has the given name. If no such
// column exists, findUsingColumn returns nil. If multiple columns with the name
// exist, then findUsingColumn raises an error.
func (jb *usingJoinBuilder) findUsingColumn(cols []scopeColumn, name tree.Name) *scopeColumn {
	var foundCol *scopeColumn
	for i := range cols {
		col := &cols[i]
		if !col.hidden && col.name == name {
			if foundCol != nil {
				jb.raiseDuplicateColError(name)
			}
			foundCol = col
		}
	}
	return foundCol
}

// addEqualityCondition constructs a new Eq expression comparing the given left
// and right columns. In addition, it adds a new column to the output scope that
// represents the "merged" value of the left and right columns. This could be
// either the left or right column value, or, in the case of a FULL JOIN, an
// IFNULL(left, right) expression.
func (jb *usingJoinBuilder) addEqualityCondition(leftCol, rightCol *scopeColumn) {
	// First, check if the comparison would even be valid.
	if !leftCol.typ.Equivalent(rightCol.typ) {
		if _, found := tree.FindEqualComparisonFunction(leftCol.typ, rightCol.typ); !found {
			panic(pgerror.Newf(pgcode.DatatypeMismatch,
				"JOIN/USING types %s for left and %s for right cannot be matched for column %q",
				leftCol.typ, rightCol.typ, tree.ErrString(&leftCol.name)))
		}
	}

	// Construct the predicate.
	leftVar := jb.b.factory.ConstructVariable(leftCol.id)
	rightVar := jb.b.factory.ConstructVariable(rightCol.id)
	eq := jb.b.factory.ConstructEq(leftVar, rightVar)
	jb.filters = append(jb.filters, jb.b.factory.ConstructFiltersItem(eq))

	// Add the merged column to the scope, constructing a new column if needed.
	if jb.joinType == sqlbase.InnerJoin || jb.joinType == sqlbase.LeftOuterJoin {
		// The merged column is the same as the corresponding column from the
		// left side.
		jb.outScope.cols = append(jb.outScope.cols, *leftCol)
		jb.showCols.Add(leftCol.id)
		jb.hideCols.Add(rightCol.id)
	} else if jb.joinType == sqlbase.RightOuterJoin &&
		!sqlbase.DatumTypeHasCompositeKeyEncoding(leftCol.typ) {
		// The merged column is the same as the corresponding column from the
		// right side.
		jb.outScope.cols = append(jb.outScope.cols, *rightCol)
		jb.showCols.Add(rightCol.id)
		jb.hideCols.Add(leftCol.id)
	} else {
		// Construct a new merged column to represent IFNULL(left, right).
		var typ *types.T
		if leftCol.typ.Family() != types.UnknownFamily {
			typ = leftCol.typ
		} else {
			typ = rightCol.typ
		}
		texpr := tree.NewTypedCoalesceExpr(tree.TypedExprs{leftCol, rightCol}, typ)
		merged := jb.b.factory.ConstructCoalesce(memo.ScalarListExpr{leftVar, rightVar})
		col := jb.b.synthesizeColumn(jb.outScope, string(leftCol.name), typ, texpr, merged)
		jb.ifNullCols.Add(col.id)
		jb.hideCols.Add(leftCol.id)
		jb.hideCols.Add(rightCol.id)
	}
}

func (jb *usingJoinBuilder) raiseDuplicateColError(name tree.Name) {
	panic(pgerror.Newf(pgcode.DuplicateColumn,
		"duplicate column name: %q", tree.ErrString(&name)))
}

func (jb *usingJoinBuilder) raiseUndefinedColError(name tree.Name, context string) {
	panic(pgerror.Newf(pgcode.UndefinedColumn,
		"column \"%s\" specified in USING clause does not exist in %s table", name, context))
}
