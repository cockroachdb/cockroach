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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

func checkFrom(expr tree.Expr, inScope *scope) {
	if len(inScope.cols) == 0 {
		panic(builderError{pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", tree.ErrString(expr))})
	}
}

// expandStar expands expr into a list of columns if expr
// corresponds to a "*", "<table>.*" or "(Expr).*".
func (b *Builder) expandStar(
	expr tree.Expr, inScope *scope,
) (labels []string, exprs []tree.TypedExpr) {
	switch t := expr.(type) {
	case *tree.TupleStar:
		texpr := inScope.resolveType(t.Expr, types.Any)
		typ := texpr.ResolvedType()
		tType, ok := typ.(types.TTuple)
		if !ok || tType.Labels == nil {
			panic(builderError{tree.NewTypeIsNotCompositeError(typ)})
		}

		// If the sub-expression is a tuple constructor, we'll de-tuplify below.
		// Otherwise we'll re-evaluate the expression multiple times.
		//
		// The following query generates a tuple constructor:
		//     SELECT (kv.*).* FROM kv
		//     -- the inner star expansion (scope.VisitPre) first expands to
		//     SELECT (((kv.k, kv.v) as k,v)).* FROM kv
		//     -- then the inner tuple constructor detuplifies here to:
		//     SELECT kv.k, kv.v FROM kv
		//
		// The following query generates a scalar var with tuple type that
		// is not a tuple constructor:
		//
		//     SELECT (SELECT pg_get_keywords() AS x LIMIT 1).*
		//     -- does not detuplify, one gets instead:
		//     SELECT (SELECT pg_get_keywords() AS x LIMIT 1).word,
		//            (SELECT pg_get_keywords() AS x LIMIT 1).catcode,
		//            (SELECT pg_get_keywords() AS x LIMIT 1).catdesc
		//     -- (and we hope a later opt will merge the subqueries)
		tTuple, isTuple := texpr.(*tree.Tuple)

		labels = tType.Labels
		exprs = make([]tree.TypedExpr, len(tType.Types))
		for i := range tType.Types {
			if isTuple {
				// De-tuplify: ((a,b,c)).* -> a, b, c
				exprs[i] = tTuple.Exprs[i].(tree.TypedExpr)
			} else {
				// Can't de-tuplify: (Expr).* -> (Expr).a, (Expr).b, (Expr).c
				exprs[i] = tree.NewTypedColumnAccessExpr(texpr, tType.Labels[i], i)
			}
		}

	case *tree.AllColumnsSelector:
		checkFrom(expr, inScope)
		src, _, err := t.Resolve(b.ctx, inScope)
		if err != nil {
			panic(builderError{err})
		}
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		labels = make([]string, 0, len(inScope.cols))
		for i := range inScope.cols {
			col := &inScope.cols[i]
			if col.table == *src && !col.hidden {
				exprs = append(exprs, col)
				labels = append(labels, string(col.name))
			}
		}

	case tree.UnqualifiedStar:
		checkFrom(expr, inScope)
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		labels = make([]string, 0, len(inScope.cols))
		for i := range inScope.cols {
			col := &inScope.cols[i]
			if !col.hidden {
				exprs = append(exprs, col)
				labels = append(labels, string(col.name))
			}
		}

	default:
		panic(fmt.Sprintf("unhandled type: %T", expr))
	}

	return labels, exprs
}

// expandStarAndResolveType expands expr into a list of columns if
// expr corresponds to a "*", "<table>.*" or "(Expr).*". Otherwise,
// expandStarAndResolveType resolves the type of expr and returns it
// as a []TypedExpr.
func (b *Builder) expandStarAndResolveType(
	expr tree.Expr, inScope *scope,
) (exprs []tree.TypedExpr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector, tree.UnqualifiedStar, *tree.TupleStar:
		_, exprs = b.expandStar(expr, inScope)

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
		label = fmt.Sprintf("column%d", b.factory.Metadata().NumColumns()+1)
	}

	name := tree.Name(label)
	colID := b.factory.Metadata().AddColumn(label, typ)
	scope.cols = append(scope.cols, scopeColumn{
		origName: name,
		name:     name,
		typ:      typ,
		id:       colID,
		expr:     expr,
		group:    group,
	})
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) synthesizeResultColumns(scope *scope, cols sqlbase.ResultColumns) {
	for i := range cols {
		c := b.synthesizeColumn(scope, cols[i].Name, cols[i].Typ, nil /* expr */, 0 /* group */)
		if cols[i].Hidden {
			c.hidden = true
		}
	}
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
			panic(builderError{pgerror.NewErrorf(
				pgerror.CodeSyntaxError,
				"non-integer constant in %s: %s", context, expr,
			)})
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		panic(builderError{pgerror.NewErrorf(
			pgerror.CodeSyntaxError, "non-integer constant in %s: %s", context, expr,
		)})
	case tree.Datum:
		panic(builderError{pgerror.NewErrorf(
			pgerror.CodeSyntaxError, "non-integer constant in %s: %s", context, expr,
		)})
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			panic(builderError{pgerror.NewErrorf(
				pgerror.CodeInvalidColumnReferenceError,
				"%s position %s is not in select list", context, expr,
			)})
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
			for j := range scope.cols {
				col := &scope.cols[j]
				if col.name == target {
					if index != -1 {
						// There is more than one projection alias that matches the clause.
						// Here, SQL92 is specific as to what should be done: if the
						// underlying expression is known and it is equivalent, then just
						// accept that and ignore the ambiguity. This plays nice with
						// `SELECT b, * FROM t ORDER BY b`. Otherwise, reject with an
						// ambiguity error.
						if scope.cols[j].getExprStr() != scope.cols[index].getExprStr() {
							panic(builderError{pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
								"%s \"%s\" is ambiguous", op, target)})
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

func (b *Builder) assertNoAggregationOrWindowing(expr tree.Expr, op string) {
	if b.exprTransformCtx.AggregateInExpr(expr, b.semaCtx.SearchPath) {
		panic(builderError{
			pgerror.NewErrorf(pgerror.CodeGroupingError, "aggregate functions are not allowed in %s", op),
		})
	}
	if b.exprTransformCtx.WindowFuncInExpr(expr) {
		panic(builderError{
			pgerror.NewErrorf(pgerror.CodeWindowingError, "window functions are not allowed in %s", op),
		})
	}
}

// resolveDataSource returns the data source in the catalog with the given name.
// If the name does not resolve to a table, or if the current user does not have
// the right privileges, then resolveDataSource raises an error.
func (b *Builder) resolveDataSource(tn *tree.TableName, priv privilege.Kind) opt.DataSource {
	ds, err := b.catalog.ResolveDataSource(b.ctx, tn)
	if err != nil {
		panic(builderError{err})
	}

	if !b.skipSelectPrivilegeChecks {
		err := ds.CheckPrivilege(b.ctx, priv)
		if err != nil {
			panic(builderError{err})
		}
	}

	return ds
}

// resolveDataSourceFromRef returns the data source in the catalog that matches
// the given TableRef spec. If no data source matches, or if the current user
// does not have the right privileges, then resolveDataSourceFromRef raises an
// error.
func (b *Builder) resolveDataSourceRef(ref *tree.TableRef, priv privilege.Kind) opt.DataSource {
	ds, err := b.catalog.ResolveDataSourceByID(b.ctx, ref.TableID)
	if err != nil {
		panic(builderError{errors.Wrapf(err, "%s", tree.ErrString(ref))})
	}

	if !b.skipSelectPrivilegeChecks {
		err := ds.CheckPrivilege(b.ctx, priv)
		if err != nil {
			panic(builderError{err})
		}
	}

	return ds
}
