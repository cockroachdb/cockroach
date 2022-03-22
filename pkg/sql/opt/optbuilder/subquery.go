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
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const multiRowSubqueryErrText = "more than one row returned by a subquery used as an expression"

// subquery represents a subquery expression in an expression tree
// after it has been type-checked and added to the memo.
type subquery struct {
	// Subquery is the AST Subquery expression.
	*tree.Subquery

	// cols contains the output columns of the subquery.
	cols []scopeColumn

	// node is the top level memo node of the subquery.
	node memo.RelExpr

	// ordering is the ordering requested by the subquery.
	// It is only consulted in certain cases, however (such as the
	// ArrayFlatten operation).
	ordering opt.Ordering

	// wrapInTuple is true if the subquery return type should be wrapped in a
	// tuple. This is true for subqueries that may return multiple rows in
	// comparison expressions (e.g., IN, ANY, ALL) and EXISTS expressions.
	wrapInTuple bool

	// typ is the lazily resolved type of the subquery.
	typ *types.T

	// outerCols stores the set of outer columns in the subquery. These are
	// columns which are referenced within the subquery but are bound in an
	// outer scope.
	outerCols opt.ColSet

	// desiredNumColumns specifies the desired number of columns for the subquery.
	// Specifying -1 for desiredNumColumns allows the subquery to return any
	// number of columns and is used when the normal type checking machinery will
	// verify that the correct number of columns is returned.
	desiredNumColumns int

	// extraColsAllowed indicates that extra columns built from the subquery
	// (such as columns for which orderings have been requested) will not be
	// stripped away.
	extraColsAllowed bool

	// scope is the input scope of the subquery. It is needed to lazily build
	// the subquery in TypeCheck.
	scope *scope
}

// isMultiRow returns whether the subquery can return multiple rows.
func (s *subquery) isMultiRow() bool {
	return s.wrapInTuple && !s.Exists
}

// Walk is part of the tree.Expr interface.
func (s *subquery) Walk(v tree.Visitor) tree.Expr {
	return s
}

// TypeCheck is part of the tree.Expr interface.
func (s *subquery) TypeCheck(
	_ context.Context, _ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	if s.typ != nil {
		return s, nil
	}

	// Convert desired to an array of desired types for building the subquery.
	desiredTypes := desired.TupleContents()

	// Build the subquery. We cannot build the subquery earlier because we do
	// not know the desired types until TypeCheck is called.
	s.buildSubquery(desiredTypes)

	// The typing for subqueries is complex, but regular.
	//
	// * If the subquery is part of an EXISTS statement:
	//
	//   The type of the subquery is always "bool".
	//
	// * If the subquery is used in a single-row context:
	//
	//   - If the subquery returns a single column with type "U", the type of the
	//     subquery is the type of the column "U". For example:
	//
	//       SELECT 1 = (SELECT 1)
	//
	//     The type of the subquery is "int".
	//
	//   - If the subquery returns multiple columns, the type of the subquery is
	//     "tuple{C}" where "C" expands to all of the types of the columns of the
	//     subquery. For example:
	//
	//       SELECT (1, 'a') = (SELECT 1, 'a')
	//
	//     The type of the subquery is "tuple{int,string}"
	//
	// * If the subquery is used in a multi-row context:
	//
	//   - If the subquery returns a single column with type "U", the type of the
	//     subquery is the singleton tuple of type "U": "tuple{U}". For example:
	//
	//       SELECT 1 IN (SELECT 1)
	//
	//     The type of the subquery's columns is "int" and the type of the
	//     subquery is "tuple{int}".
	//
	//   - If the subquery returns multiple columns, the type of the subquery is
	//     "tuple{tuple{C}}" where "C expands to all of the types of the columns
	//     of the subquery. For example:
	//
	//       SELECT (1, 'a') IN (SELECT 1, 'a')
	//
	//     The types of the subquery's columns are "int" and "string". These are
	//     wrapped into "tuple{int,string}" to form the row type. And these are
	//     wrapped again to form the subquery type "tuple{tuple{int,string}}".
	//
	// Note that these rules produce a somewhat surprising equivalence:
	//
	//   SELECT (SELECT 1, 2) = (SELECT (1, 2))
	//
	// A subquery which returns a single column tuple is equivalent to a subquery
	// which returns the elements of the tuple as individual columns. While
	// surprising, this is necessary for regularity and in order to handle:
	//
	//   SELECT 1 IN (SELECT 1)
	//
	// Without that auto-unwrapping of single-column subqueries, this query would
	// type check as "<int> IN <tuple{tuple{int}}>" which would fail.

	if s.Exists {
		s.typ = types.Bool
		return s, nil
	}

	if len(s.cols) == 1 {
		s.typ = s.cols[0].typ
	} else {
		contents := make([]*types.T, len(s.cols))
		labels := make([]string, len(s.cols))
		for i := range s.cols {
			contents[i] = s.cols[i].typ
			labels[i] = string(s.cols[i].name.ReferenceName())
		}
		s.typ = types.MakeLabeledTuple(contents, labels)
	}

	if s.wrapInTuple {
		// The subquery is in a multi-row context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		//
		// Wrap the type in a tuple.
		//
		// TODO(peter): Using a tuple type to represent a multi-row
		// subquery works with the current type checking code, but seems
		// semantically incorrect. A tuple represents a fixed number of
		// elements. Instead, we should introduce a new vtuple type.
		s.typ = types.MakeTuple([]*types.T{s.typ})
	}

	return s, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (s *subquery) ResolvedType() *types.T {
	return s.typ
}

// Eval is part of the tree.TypedExpr interface.
func (s *subquery) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("subquery must be replaced before evaluation"))
}

// buildSubquery builds a relational expression that represents this subquery.
// It stores the resulting relational expression in s.node, and also updates
// s.cols and s.ordering with the output columns and ordering of the subquery.
func (s *subquery) buildSubquery(desiredTypes []*types.T) {
	if s.scope.replaceSRFs {
		// We need to save and restore the previous value of the replaceSRFs field in
		// case we are recursively called within a subquery context.
		defer func() { s.scope.replaceSRFs = true }()
		s.scope.replaceSRFs = false
	}

	// Save and restore the previous value of s.builder.subquery in case we are
	// recursively called within a subquery context.
	outer := s.scope.builder.subquery
	defer func() { s.scope.builder.subquery = outer }()
	s.scope.builder.subquery = s

	// We must push() here so that the columns in s.scope are correctly identified
	// as outer columns.
	outScope := s.scope.builder.buildStmt(s.Subquery.Select, desiredTypes, s.scope.push())
	ord := outScope.ordering

	// Treat the subquery result as an anonymous data source (i.e. column names
	// are not qualified). Remove hidden columns, as they are not accessible
	// outside the subquery.
	outScope.setTableAlias("")
	outScope.removeHiddenCols()

	if s.desiredNumColumns > 0 && len(outScope.cols) != s.desiredNumColumns {
		n := len(outScope.cols)
		switch s.desiredNumColumns {
		case 1:
			panic(pgerror.Newf(pgcode.Syntax,
				"subquery must return only one column, found %d", n))
		default:
			panic(pgerror.Newf(pgcode.Syntax,
				"subquery must return %d columns, found %d", s.desiredNumColumns, n))
		}
	}

	if len(outScope.extraCols) > 0 && !s.extraColsAllowed {
		// We need to add a projection to remove the extra columns.
		projScope := outScope.push()
		projScope.appendColumnsFromScope(outScope)
		projScope.expr = s.scope.builder.constructProject(outScope.expr, projScope.cols)
		outScope = projScope
	}

	s.cols = outScope.cols
	s.node = outScope.expr
	s.ordering = ord
}

// buildSubqueryProjection ensures that a subquery returns exactly one column.
// If the original subquery has more than one column, buildSubqueryProjection
// wraps it in a projection which has a single tuple column containing all the
// original columns: tuple{col1, col2...}.
func (b *Builder) buildSubqueryProjection(
	s *subquery, inScope *scope,
) (out memo.RelExpr, outScope *scope) {
	out = s.node
	outScope = inScope.replace()

	switch len(s.cols) {
	case 0:
		// This can be obtained with:
		// CREATE TABLE t(x INT); ALTER TABLE t DROP COLUMN x;
		// SELECT (SELECT * FROM t) = (SELECT * FROM t);
		panic(pgerror.Newf(pgcode.Syntax,
			"subquery must return only one column"))

	case 1:
		outScope.cols = append(outScope.cols, s.cols[0])

	default:
		// Wrap the subquery in a projection with a single column.
		// col1, col2... from the subquery becomes tuple{col1, col2...} in the
		// projection.
		cols := make(tree.Exprs, len(s.cols))
		els := make(memo.ScalarListExpr, len(s.cols))
		contents := make([]*types.T, len(s.cols))
		for i := range s.cols {
			cols[i] = &s.cols[i]
			contents[i] = s.cols[i].ResolvedType()
			els[i] = b.factory.ConstructVariable(s.cols[i].id)
		}
		typ := types.MakeTuple(contents)

		texpr := tree.NewTypedTuple(typ, cols)
		tup := b.factory.ConstructTuple(els, typ)
		col := b.synthesizeColumn(outScope, scopeColName(""), texpr.ResolvedType(), texpr, tup)
		out = b.constructProject(out, []scopeColumn{*col})
	}

	telemetry.Inc(sqltelemetry.SubqueryUseCounter)

	return out, outScope
}

// buildSingleRowSubquery builds a set of memo groups that represent the given
// subquery. This function should only be called for subqueries in a single-row
// context, such as `SELECT (1, 'a') = (SELECT 1, 'a')`.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildSingleRowSubquery(
	s *subquery, inScope *scope,
) (out opt.ScalarExpr, outScope *scope) {
	subqueryPrivate := memo.SubqueryPrivate{OriginalExpr: s.Subquery}
	if s.Exists {
		return b.factory.ConstructExists(s.node, &subqueryPrivate), inScope
	}

	var input memo.RelExpr
	input, outScope = b.buildSubqueryProjection(s, inScope)

	// Wrap the subquery in a Max1Row operator to enforce that it should return
	// at most one row. Max1Row may be removed by the optimizer later if it can
	// prove statically that the subquery always returns at most one row.
	input = b.factory.ConstructMax1Row(input, multiRowSubqueryErrText)

	out = b.factory.ConstructSubquery(input, &subqueryPrivate)
	return out, outScope
}

// buildMultiRowSubquery transforms a multi-row subquery into a single-row
// subquery for IN, NOT IN, ANY, SOME and ALL expressions. It performs the
// transformation using the Any operator, which returns true if any of the
// values returned by the subquery are true, else returns NULL if any of the
// values are NULL, else returns false.
//
// We use the following transformations:
//
//   <var> IN (<subquery>)
//     ==> ConstructAny(<subquery>, <var>, EqOp)
//
//   <var> NOT IN (<subquery>)
//    ==> ConstructNot(ConstructAny(<subquery>, <var>, EqOp))
//
//   <var> <comp> {SOME|ANY}(<subquery>)
//     ==> ConstructAny(<subquery>, <var>, <comp>)
//
//   <var> <comp> ALL(<subquery>)
//     ==> ConstructNot(ConstructAny(<subquery>, <var>, Negate(<comp>)))
//
func (b *Builder) buildMultiRowSubquery(
	c *tree.ComparisonExpr, inScope *scope, colRefs *opt.ColSet,
) (out opt.ScalarExpr, outScope *scope) {
	var input memo.RelExpr
	s := c.Right.(*subquery)
	input, outScope = b.buildSubqueryProjection(s, inScope)

	scalar := b.buildScalar(c.TypedLeft(), inScope, nil, nil, colRefs)
	outScope = outScope.replace()

	var cmp opt.Operator
	switch c.Operator.Symbol {
	case treecmp.In, treecmp.NotIn:
		// <var> = x
		cmp = opt.EqOp

	case treecmp.Any, treecmp.Some, treecmp.All:
		// <var> <comp> x
		cmp = opt.ComparisonOpMap[c.SubOperator.Symbol]
		if c.Operator.Symbol == treecmp.All {
			// NOT(<var> <comp> x)
			cmp = opt.NegateOpMap[cmp]
		}

	default:
		panic(errors.AssertionFailedf(
			"buildMultiRowSubquery called with operator %v", c.Operator,
		))
	}

	// Construct the outer Any(...) operator.
	out = b.factory.ConstructAny(input, scalar, &memo.SubqueryPrivate{
		Cmp:          cmp,
		OriginalExpr: s.Subquery,
	})
	switch c.Operator.Symbol {
	case treecmp.NotIn, treecmp.All:
		// NOT Any(...)
		out = b.factory.ConstructNot(out)
	}

	return out, outScope
}

var _ tree.Expr = &subquery{}
var _ tree.TypedExpr = &subquery{}

// SubqueryExpr implements the SubqueryExpr interface.
func (*subquery) SubqueryExpr() {}

var _ tree.SubqueryExpr = &subquery{}
