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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// expandStarAndResolveType expands expr into a list of columns if expr
// corresponds to a "*" or "<table>.*". Otherwise, expandStarAndResolveType
// resolves the type of expr and returns it as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	// NB: The case statements are sorted lexicographically.
	switch t := expr.(type) {
	case *tree.AllColumnsSelector:
		tn, err := tree.NormalizeTableName(&t.TableName)
		if err != nil {
			panic(builderError{err})
		}

		numRes, src, _, err := inScope.FindSourceMatchingName(b.ctx, tn)
		if err != nil {
			panic(builderError{err})
		}
		if numRes == tree.NoResults {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"no data source named %q", tree.ErrString(&tn))})
		}

		for i := range inScope.cols {
			col := inScope.cols[i]
			if col.table == *src && !col.hidden {
				exprs = append(exprs, &col)
			}
		}

	case tree.UnqualifiedStar:
		for i := range inScope.cols {
			col := inScope.cols[i]
			if !col.hidden {
				exprs = append(exprs, &col)
			}
		}
		if len(exprs) == 0 {
			panic(errorf("failed to expand *"))
		}

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		return b.expandStarAndResolveType(vn, inScope)

	default:
		texpr := inScope.resolveType(t, types.Any)
		exprs = []tree.TypedExpr{texpr}
	}

	return exprs
}

// synthesizeColumn is used to synthesize new columns. This is needed for
// operations such as projection of scalar expressions and aggregations. For
// example, the query `SELECT (x + 1) AS "x_incr" FROM t` has a projection with
// a synthesized column "x_incr".
//
// scope  The scope is passed in so it can can be updated with the newly bound
//        variable.
// label  This is an optional label for the new column (e.g., if specified with
//        the AS keyword).
// typ    The type of the column.
//
// The new column is returned as a columnProps object.
func (b *Builder) synthesizeColumn(scope *scope, label string, typ types.T) *columnProps {
	if label == "" {
		label = fmt.Sprintf("column%d", len(b.colMap))
	}

	colIndex := b.factory.Metadata().AddColumn(label, typ)
	col := columnProps{name: tree.Name(label), typ: typ, index: colIndex}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

// constructList invokes the factory to create one of the operators that contain
// a list of groups: ProjectionsOp and AggregationsOp.
func (b *Builder) constructList(
	op opt.Operator, items []opt.GroupID, cols []columnProps,
) opt.GroupID {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].index
	}

	list := b.factory.InternList(items)
	private := b.factory.InternPrivate(&colList)

	switch op {
	case opt.ProjectionsOp:
		return b.factory.ConstructProjections(list, private)
	case opt.AggregationsOp:
		return b.factory.ConstructAggregations(list, private)
	}

	panic(fmt.Sprintf("unexpected operator: %s", op))
}

// colIndex takes an expression that refers to a column using an integer,
// verifies it refers to a valid target in the SELECT list, and returns the
// corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first item in the SELECT list, "a". The returned index
// is 0.
func colIndex(numOriginalCols int, expr tree.Expr, context string) int {
	ord := int64(-1)
	switch i := expr.(type) {
	case *tree.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				panic(builderError{err})
			}
			ord = val
		} else {
			panic(errorf("non-integer constant in %s: %s", context, expr))
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		panic(errorf("non-integer constant in %s: %s", context, expr))
	case tree.Datum:
		panic(errorf("non-integer constant in %s: %s", context, expr))
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			panic(errorf("%s position %s is not in select list", context, expr))
		}
		ord--
	}
	return int(ord)
}

// flattenTuples extracts the members of tuples into a list of columns.
func flattenTuples(exprs []tree.TypedExpr) []tree.TypedExpr {
	// We want to avoid allocating new slices unless strictly necessary.
	var newExprs []tree.TypedExpr
	for i, e := range exprs {
		if t, ok := e.(*tree.Tuple); ok {
			if newExprs == nil {
				// All right, it was necessary to allocate the slices after all.
				newExprs = make([]tree.TypedExpr, i, len(exprs))
				copy(newExprs, exprs[:i])
			}

			newExprs = flattenTuple(t, newExprs)
		} else if newExprs != nil {
			newExprs = append(newExprs, e)
		}
	}
	if newExprs != nil {
		return newExprs
	}
	return exprs
}

// flattenTuple recursively extracts the members of a tuple into a list of
// expressions.
func flattenTuple(t *tree.Tuple, exprs []tree.TypedExpr) []tree.TypedExpr {
	for _, e := range t.Exprs {
		if eT, ok := e.(*tree.Tuple); ok {
			exprs = flattenTuple(eT, exprs)
		} else {
			expr := e.(tree.TypedExpr)
			exprs = append(exprs, expr)
		}
	}
	return exprs
}

// symbolicExprStr returns a string representation of the expression using
// symbolic notation. Because the symbolic notation disambiguates columns, this
// string can be used to determine if two expressions are equivalent.
func symbolicExprStr(expr tree.Expr) string {
	return tree.AsStringWithFlags(expr, tree.FmtCheckEquivalence)
}

func colsToColList(cols []columnProps) opt.ColList {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].index
	}
	return colList
}
