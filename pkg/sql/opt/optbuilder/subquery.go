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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// subquery represents a subquery expression in an expression tree
// after it has been type-checked and added to the memo.
type subquery struct {
	// Subquery is the AST Subquery expression.
	*tree.Subquery

	// cols contains the output columns of the subquery.
	cols []scopeColumn

	// group is the top level memo GroupID of the subquery.
	group memo.GroupID

	// ordering is the ordering requested by the subquery.
	// It is only consulted in certain cases, however (such as the
	// ArrayFlatten operation).
	ordering opt.Ordering

	// wrapInTuple is true if the subquery return type should be wrapped in a
	// tuple. This is true for subqueries that may return multiple rows in
	// comparison expressions (e.g., IN, ANY, ALL) and EXISTS expressions.
	wrapInTuple bool

	// typ is the lazily resolved type of the subquery.
	typ types.T

	// outerCols stores the set of outer columns in the subquery. These are
	// columns which are referenced within the subquery but are bound in an
	// outer scope.
	outerCols opt.ColSet
}

// Walk is part of the tree.Expr interface.
func (s *subquery) Walk(v tree.Visitor) tree.Expr {
	return s
}

// TypeCheck is part of the tree.Expr interface.
func (s *subquery) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	if s.typ != nil {
		return s, nil
	}

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
		t := types.TTuple{
			Types:  make([]types.T, len(s.cols)),
			Labels: make([]string, len(s.cols)),
		}
		for i := range s.cols {
			t.Types[i] = s.cols[i].typ
			t.Labels[i] = string(s.cols[i].name)
		}
		s.typ = t
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
		s.typ = types.TTuple{Types: []types.T{s.typ}}
	}

	return s, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (s *subquery) ResolvedType() types.T {
	return s.typ
}

// Eval is part of the tree.TypedExpr interface.
func (s *subquery) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("subquery must be replaced before evaluation")
}

// buildSubqueryProjection ensures that a subquery returns exactly one column.
// If the original subquery has more than one column, buildSubqueryProjection
// wraps it in a projection which has a single tuple column containing all the
// original columns: tuple{col1, col2...}.
func (b *Builder) buildSubqueryProjection(
	s *subquery, inScope *scope,
) (out memo.GroupID, outScope *scope) {
	out = s.group
	outScope = inScope.replace()

	switch len(s.cols) {
	case 0:
		panic("subquery returned 0 columns")

	case 1:
		outScope.cols = append(outScope.cols, s.cols[0])

	default:
		// Wrap the subquery in a projection with a single column.
		// col1, col2... from the subquery becomes tuple{col1, col2...} in the
		// projection.
		cols := make(tree.Exprs, len(s.cols))
		colGroups := make([]memo.GroupID, len(s.cols))
		typ := types.TTuple{
			Types: make([]types.T, len(s.cols)),
		}
		for i := range s.cols {
			cols[i] = &s.cols[i]
			typ.Types[i] = s.cols[i].ResolvedType()
			colGroups[i] = b.factory.ConstructVariable(b.factory.InternColumnID(s.cols[i].id))
		}

		texpr := tree.NewTypedTuple(typ, cols)
		tup := b.factory.ConstructTuple(b.factory.InternList(colGroups), b.factory.InternType(typ))
		col := b.synthesizeColumn(outScope, "", texpr.ResolvedType(), texpr, tup)
		out = b.constructProject(out, []scopeColumn{*col})
	}

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
) (out memo.GroupID, outScope *scope) {
	def := b.factory.InternSubqueryDef(&memo.SubqueryDef{OriginalExpr: s.Subquery})
	if s.Exists {
		return b.factory.ConstructExists(s.group, def), inScope
	}

	out, outScope = b.buildSubqueryProjection(s, inScope)

	// Wrap the subquery in a Max1Row operator to enforce that it should return
	// at most one row. Max1Row may be removed by the optimizer later if it can
	// prove statically that the subquery always returns at most one row.
	out = b.factory.ConstructMax1Row(out)

	out = b.factory.ConstructSubquery(out, def)
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
) (out memo.GroupID, outScope *scope) {
	s := c.Right.(*subquery)
	out, outScope = b.buildSubqueryProjection(s, inScope)

	scalar := b.buildScalar(c.TypedLeft(), inScope, nil, nil, colRefs)
	outScope = outScope.replace()

	var cmp opt.Operator
	switch c.Operator {
	case tree.In, tree.NotIn:
		// <var> = x
		cmp = opt.EqOp

	case tree.Any, tree.Some, tree.All:
		// <var> <comp> x
		cmp = opt.ComparisonOpMap[c.SubOperator]
		if c.Operator == tree.All {
			// NOT(<var> <comp> x)
			cmp = opt.NegateOpMap[cmp]
		}

	default:
		panic(fmt.Errorf(
			"buildMultiRowSubquery called with operator %v", c.Operator,
		))
	}

	// Construct the outer Any(...) operator.
	out = b.factory.ConstructAny(
		out,
		scalar,
		b.factory.InternSubqueryDef(&memo.SubqueryDef{
			OriginalExpr: s.Subquery,
			Cmp:          cmp,
		}),
	)
	switch c.Operator {
	case tree.NotIn, tree.All:
		// NOT Any(...)
		out = b.factory.ConstructNot(out)
	}

	return out, outScope
}

var _ tree.Expr = &subquery{}
var _ tree.TypedExpr = &subquery{}
