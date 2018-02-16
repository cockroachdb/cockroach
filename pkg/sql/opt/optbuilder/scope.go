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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// scope is used for the build process and maintains the variables that have
// been bound within the current scope as columnProps. Variables bound in the
// parent scope are also visible in this scope.
//
// See builder.go for more details.
type scope struct {
	builder *Builder
	parent  *scope
	cols    []columnProps
	groupby groupby
	// TODO(rytaft): add ordering to scope.
}

// groupByStrSet is a set of stringified GROUP BY expressions.
type groupByStrSet map[string]struct{}

// exists is the 0-byte value of each element in groupByStrSet.
var exists = struct{}{}

type groupby struct {
	// groupingsScope refers to another scope that groups columns in this
	// scope. Any aggregate functions which contain column references to this
	// scope trigger the creation of new grouping columns in the grouping
	// scope. In addition, if an aggregate function contains no column
	// references, then the aggregate will be added to the "nearest" grouping
	// scope. For example:
	//   SELECT MAX(1) FROM t1
	groupingsScope *scope

	// aggs contains all aggregation expressions that were extracted from the
	// query and which will become columns in this scope.
	aggs []opt.GroupID

	// groupings contains all group by expressions that were extracted from the
	// query and which will become columns in this scope.
	groupings []opt.GroupID

	// groupStrs contains a string representation of each GROUP BY expression
	// using symbolic notation. These strings are used to determine if SELECT
	// and HAVING expressions contain sub-expressions matching a GROUP BY
	// expression. This enables queries such as:
	//    SELECT x+y FROM t GROUP BY x+y
	// but not:
	//    SELECT x+y FROM t GROUP BY y+x
	groupStrs groupByStrSet

	// inAgg is true within the body of an aggregate function. inAgg is used
	// to ensure that nested aggregates are disallowed.
	inAgg bool

	// varsUsed is only utilized when groupingsScope is not nil.
	// It keeps track of variables that are encountered by the builder that are:
	//   (1) not explicit GROUP BY columns,
	//   (2) not part of a sub-expression that matches a GROUP BY expression, and
	//   (3) not contained in an aggregate.
	//
	// varsUsed is a slice rather than a set because the builder appends
	// variables found in each sub-expression, and variables from individual
	// sub-expresssions may be removed if they are found to match a GROUP BY
	// expression. If any variables remain in varsUsed when the builder is
	// done building a SELECT expression or HAVING clause, the builder throws
	// an error.
	//
	// For example, consider this query:
	//   SELECT COUNT(*), v/(k+v) FROM t.kv GROUP BY k+v
	// When building the expression v/(k+v), varsUsed will contain the variables
	// shown in brackets after each of the following expressions is built (in
	// order of completed recursive calls):
	//
	//  1.   Build v [v]
	//          \
	//  2.       \  Build k [v, k]
	//            \    \
	//  3.         \    \  Build v [v, k, v]
	//              \    \   /
	//  4.           \  Build (k+v) [v]  <- truncate varsUsed since (k+v) matches
	//                \   /                 a GROUP BY expression
	//  5.          Build v/(k+v) [v] <- error - build is complete and varsUsed
	//                                   is not empty
	varsUsed []opt.ColumnIndex

	// refScope is the scope to which all column references contained by the
	// aggregate function must refer. This is used to detect illegal cases
	// where the aggregate contains column references that point to
	// different scopes. For example:
	//   SELECT a
	//   FROM t1
	//   GROUP BY a
	//   HAVING EXISTS
	//   (
	//     SELECT MAX(t1.a+t2.b)
	//     FROM t2
	//   )
	refScope *scope
}

// inGroupingContext returns true when the groupingsScope is not nil. This is
// the case when the builder is building expressions in a SELECT list, and
// aggregates, GROUP BY, or HAVING are present. This is also true when the
// builder is building expressions inside the HAVING clause. When
// inGroupingContext returns true, varsUsed will be utilized to enforce scoping
// rules. See the comment above varsUsed for more details.
func (s *scope) inGroupingContext() bool {
	return s.groupby.groupingsScope != nil
}

// push creates a new scope with this scope as its parent.
func (s *scope) push() *scope {
	return &scope{builder: s.builder, parent: s}
}

// replace creates a new scope with the parent of this scope as its parent.
func (s *scope) replace() *scope {
	return &scope{builder: s.builder, parent: s.parent}
}

// appendColumns adds newly bound variables to this scope.
func (s *scope) appendColumns(src *scope) {
	s.cols = append(s.cols, src.cols...)
}

// resolveType converts the given expr to a tree.TypedExpr. As part of the
// conversion, it performs name resolution and replaces unresolved column names
// with columnProps.
func (s *scope) resolveType(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr, _ = tree.WalkExpr(s, expr)
	texpr, err := tree.TypeCheck(expr, &s.builder.semaCtx, desired)
	if err != nil {
		panic(builderError{err})
	}

	return texpr
}

// hasColumn returns true if the given column index is found within this scope.
func (s *scope) hasColumn(index opt.ColumnIndex) bool {
	for curr := s; curr != nil; curr = curr.parent {
		for i := range curr.cols {
			col := &curr.cols[i]
			if col.index == index {
				return true
			}
		}
	}

	return false
}

// getAggregateCols returns the columns in this scope corresponding
// to aggregate functions.
func (s *scope) getAggregateCols() []columnProps {
	// Aggregates are always clustered at the end of the column list, in the
	// same order as s.groupby.aggs.
	return s.cols[len(s.cols)-len(s.groupby.aggs):]
}

// findAggregate finds the given aggregate among the bound variables
// in this scope. Returns nil if the aggregate is not found.
func (s *scope) findAggregate(agg opt.GroupID) *columnProps {
	for i, a := range s.groupby.aggs {
		if a == agg {
			// Aggregate already exists, so return information about the
			// existing column that computes it.
			return &s.getAggregateCols()[i]
		}
	}

	return nil
}

// findGrouping finds the given grouping expression among the bound variables
// in the groupingsScope. Returns nil if the grouping is not found.
func (s *scope) findGrouping(grouping opt.GroupID) *columnProps {
	for i, g := range s.groupby.groupings {
		if g == grouping {
			// Grouping already exists, so return information about the
			// existing column that computes it. Columns in the groupingsScope are
			// always listed in the same order as s.groupby.groupings.
			return &s.groupby.groupingsScope.cols[i]
		}
	}

	return nil
}

// startAggFunc is called when the builder starts building an aggregate
// function. It is used to disallow nested aggregates and ensure that aggregate
// functions are only used in a groupings scope.
func (s *scope) startAggFunc() {
	var found bool
	for curr := s; curr != nil; curr = curr.parent {
		if curr.groupby.inAgg {
			panic(errorf("aggregate function cannot be nested within another aggregate function"))
		}

		if curr.groupby.groupingsScope != nil {
			// The aggregate will be added to the innermost groupings scope.
			// TODO(rytaft): This will not work for subqueries. We should wait to set
			// the refScope until the variables inside the aggregate are resolved.
			s.groupby.refScope = curr.groupby.groupingsScope
			found = true
			break
		}
	}

	if !found {
		panic(errorf("aggregate function is not allowed in this context"))
	}

	s.groupby.inAgg = true
}

// endAggFunc is called when the builder finishes building an aggregate
// function. It is used in combination with startAggFunc to disallow nested
// aggregates and ensure that aggregate functions are only used in a groupings
// scope. It returns the reference scope to which the new aggregate should be
// added.
func (s *scope) endAggFunc() (refScope *scope) {
	if !s.groupby.inAgg {
		panic(errorf("mismatched calls to start/end aggFunc"))
	}

	refScope = s.groupby.refScope
	if refScope == nil {
		panic(errorf("not in grouping scope"))
	}

	s.groupby.inAgg = false
	return refScope
}

// scope implements the tree.Visitor interface so that it can walk through
// a tree.Expr tree, perform name resolution, and replace unresolved column
// names with a columnProps. The info stored in columnProps is necessary for
// Builder.buildScalar to construct a "variable" memo expression.
var _ tree.Visitor = &scope{}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*scope) ColumnSourceMeta() {}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*columnProps) ColumnSourceMeta() {}

// ColumnResolutionResult implements the tree.ColumnResolutionResult interface.
func (*columnProps) ColumnResolutionResult() {}

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colNameName tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	colName := optbase.ColumnName(colNameName)
	for ; s != nil; s = s.parent {
		for i := range s.cols {
			col := &s.cols[i]
			if col.matches("", colName) {
				// TODO(whomever): source names in a FROM clause also have a
				// catalog/schema prefix and it matters.
				return tree.NewUnqualifiedTableName(tree.Name(col.table)), col, int(col.index), nil
			}
		}
	}
	return nil, nil, -1, fmt.Errorf("unknown column %s", colName)
}

// FindSourceMatchingName is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (
	res tree.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	err error,
) {
	tblName := optbase.TableName(tn.Table())
	for ; s != nil; s = s.parent {
		for i := range s.cols {
			col := &s.cols[i]
			// TODO(whomever): this improperly disregards the catalog/schema prefix.
			if col.table == tblName {
				// TODO(whomever): this improperly fails to recognize when a source table
				// is ambiguous, e.g. SELECT kv.k FROM db1.kv, db2.kv
				return tree.ExactlyOne, tree.NewUnqualifiedTableName(tree.Name(col.table)), s, nil
			}
		}
	}
	return tree.NoResults, nil, nil, nil
}

// Resolve is part of the tree.ColumnItemResolver interface.
func (s *scope) Resolve(
	_ context.Context,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	colHint int,
	colNameName tree.Name,
) (tree.ColumnResolutionResult, error) {
	if colHint >= 0 {
		// Column was found by FindSourceProvidingColumn above.
		return srcMeta.(*columnProps), nil
	}
	// Otherwise, a table is known but not the column yet.
	inScope := srcMeta.(*scope)
	tblName := optbase.TableName(prefix.Table())
	colName := optbase.ColumnName(colNameName)
	for i := range inScope.cols {
		col := &s.cols[i]
		if col.matches(tblName, colName) {
			return col, nil
		}
	}

	return nil, fmt.Errorf("unknown column %s", columnProps{name: colName, table: tblName})
}

// VisitPre is part of the Visitor interface.
//
// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		colI, err := t.Resolve(context.TODO(), s)
		if err != nil {
			panic(err)
		}
		return false, colI.(*columnProps)

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(s.builder.semaCtx.SearchPath)
		if err != nil {
			panic(builderError{err})
		}
		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(tree.VarName)
		if !ok {
			break
		}
		vn, err = vn.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		t.Exprs[0] = vn

		if strings.EqualFold(def.Name, "count") && t.Type == 0 {
			if _, ok := vn.(tree.UnqualifiedStar); ok {
				// Special case handling for COUNT(*). This is a special construct to
				// count the number of rows; in this case * does NOT refer to a set of
				// columns. A * is invalid elsewhere (and will be caught by TypeCheck()).
				// Replace the function with COUNT_ROWS (which doesn't take any
				// arguments).
				e := &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: &tree.UnresolvedName{
							NumParts: 1, Parts: tree.NameParts{"count_rows"},
						},
					},
				}
				// We call TypeCheck to fill in FuncExpr internals. This is a fixed
				// expression; we should not hit an error here.
				if _, err := e.TypeCheck(&tree.SemaContext{}, types.Any); err != nil {
					panic(builderError{err})
				}
				e.Filter = t.Filter
				e.WindowDef = t.WindowDef
				return true, e
			}
			// TODO(rytaft): Add handling for tree.AllColumnsSelector to support
			// expressions like SELECT COUNT(kv.*) FROM kv
			// Similar to the work done in PR #17833.
		}

		// TODO(rytaft): Implement subquery replacement.
	}

	return true, expr
}

// VisitPost is part of the Visitor interface.
func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}
