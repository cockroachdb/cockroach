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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// expandStar expands expr into a list of columns if expr
// corresponds to a "*" or "<table>.*".
func (b *Builder) expandStar(expr tree.Expr, inScope *scope) (exprs []tree.TypedExpr) {
	if len(inScope.cols) == 0 {
		panic(builderError{pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", tree.ErrString(expr))})
	}

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
	}

	return exprs
}

// expandStarAndResolveType expands expr into a list of columns if expr
// corresponds to a "*" or "<table>.*". Otherwise, expandStarAndResolveType
// resolves the type of expr and returns it as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector, tree.UnqualifiedStar:
		exprs = b.expandStar(expr, inScope)

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
// expr   The expression this column refers to (if any).
// group  The memo group ID of this column/expression (if any). This parameter
//        is optional and can be set later in the returned scopeColumn.
//
// The new column is returned as a columnProps object.
func (b *Builder) synthesizeColumn(
	scope *scope, label string, typ types.T, expr tree.TypedExpr, group memo.GroupID,
) *scopeColumn {
	if label == "" {
		label = fmt.Sprintf("column%d", len(b.colMap))
	}

	name := tree.Name(label)
	colID := b.factory.Metadata().AddColumn(label, typ)
	col := scopeColumn{
		origName: name,
		name:     name,
		typ:      typ,
		id:       colID,
		expr:     expr,
		group:    group,
	}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

// constructList invokes the factory to create one of the operators that contain
// a list of groups: ProjectionsOp and AggregationsOp.
func (b *Builder) constructList(op opt.Operator, cols []scopeColumn) memo.GroupID {
	colList := make(opt.ColList, 0, len(cols))
	itemList := make([]memo.GroupID, 0, len(cols))

	// Deduplicate the lists. We only need to project each column once.
	colSet := opt.ColSet{}
	for i := range cols {
		id := cols[i].id
		if !colSet.Contains(int(id)) {
			colList = append(colList, id)
			itemList = append(itemList, cols[i].group)
			colSet.Add(int(id))
		}
	}

	list := b.factory.InternList(itemList)
	private := b.factory.InternColList(colList)

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
// Here "1" refers to the first item in the SELECT list, "a". The returned
// index is 0.
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

// colIdxByProjectionAlias returns the corresponding index in columns of an expression
// that may refer to a column alias.
// If there are no aliases in columns that expr refers to, then -1 is returned.
// This method is pertinent to ORDER BY and DISTINCT ON clauses that may refer
// to a column alias.
func colIdxByProjectionAlias(expr tree.Expr, op string, scope *scope) int {
	index := -1

	if vBase, ok := expr.(tree.VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}

		if c, ok := v.(*tree.ColumnItem); ok && c.TableName.Parts[0] == "" {
			// Look for an output column that matches the name. This
			// handles cases like:
			//
			//   SELECT a AS b FROM t ORDER BY b
			//   SELECT DISTINCT ON (b) a AS b FROM t
			target := c.ColumnName
			for j, col := range scope.cols {
				if col.name == target {
					if index != -1 {
						// There is more than one projection alias that matches the clause.
						// Here, SQL92 is specific as to what should be done: if the
						// underlying expression is known and it is equivalent, then just
						// accept that and ignore the ambiguity. This plays nice with
						// `SELECT b, * FROM t ORDER BY b`. Otherwise, reject with an
						// ambiguity error.
						if scope.cols[j].getExprStr() != scope.cols[index].getExprStr() {
							panic(builderError{pgerror.NewErrorf(
								pgerror.CodeAmbiguousAliasError, "%s \"%s\" is ambiguous", op, target,
							)})
						}
						// Use the index of the first matching column.
						continue
					}
					index = j
				}
			}
		}
	}

	return index
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

func colsToColList(cols []scopeColumn) opt.ColList {
	colList := make(opt.ColList, len(cols))
	for i := range cols {
		colList[i] = cols[i].id
	}
	return colList
}

func findColByIndex(cols []scopeColumn, id opt.ColumnID) *scopeColumn {
	for i := range cols {
		col := &cols[i]
		if col.id == id {
			return col
		}
	}

	return nil
}

func makePresentation(cols []scopeColumn) memo.Presentation {
	presentation := make(memo.Presentation, 0, len(cols))
	for i := range cols {
		col := &cols[i]
		if !col.hidden {
			presentation = append(presentation, opt.LabeledColumn{Label: string(col.name), ID: col.id})
		}
	}
	return presentation
}

func (b *Builder) assertNoAggregationOrWindowing(expr tree.Expr, op string) {
	exprTransformCtx := transform.ExprTransformContext{}
	if exprTransformCtx.AggregateInExpr(expr, b.semaCtx.SearchPath) {
		panic(builderError{
			pgerror.NewErrorf(pgerror.CodeGroupingError, "aggregate functions are not allowed in %s", op),
		})
	}
	if exprTransformCtx.WindowFuncInExpr(expr) {
		panic(builderError{
			pgerror.NewErrorf(pgerror.CodeWindowingError, "window functions are not allowed in %s", op),
		})
	}
}
