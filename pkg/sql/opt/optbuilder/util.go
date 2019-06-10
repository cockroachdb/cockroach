// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func checkFrom(expr tree.Expr, inScope *scope) {
	if len(inScope.cols) == 0 {
		panic(pgerror.Newf(pgerror.CodeInvalidNameError,
			"cannot use %q without a FROM clause", tree.ErrString(expr)))
	}
}

// expandStar expands expr into a list of columns if expr
// corresponds to a "*", "<table>.*" or "(Expr).*".
func (b *Builder) expandStar(
	expr tree.Expr, inScope *scope,
) (aliases []string, exprs []tree.TypedExpr) {
	switch t := expr.(type) {
	case *tree.TupleStar:
		texpr := inScope.resolveType(t.Expr, types.Any)
		typ := texpr.ResolvedType()
		if typ.Family() != types.TupleFamily || typ.TupleLabels() == nil {
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

		aliases = typ.TupleLabels()
		exprs = make([]tree.TypedExpr, len(typ.TupleContents()))
		for i := range typ.TupleContents() {
			if isTuple {
				// De-tuplify: ((a,b,c)).* -> a, b, c
				exprs[i] = tTuple.Exprs[i].(tree.TypedExpr)
			} else {
				// Can't de-tuplify: (Expr).* -> (Expr).a, (Expr).b, (Expr).c
				exprs[i] = tree.NewTypedColumnAccessExpr(texpr, typ.TupleLabels()[i], i)
			}
		}

	case *tree.AllColumnsSelector:
		checkFrom(expr, inScope)
		src, _, err := t.Resolve(b.ctx, inScope)
		if err != nil {
			panic(builderError{err})
		}
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		aliases = make([]string, 0, len(inScope.cols))
		for i := range inScope.cols {
			col := &inScope.cols[i]
			if col.table == *src && !col.hidden {
				exprs = append(exprs, col)
				aliases = append(aliases, string(col.name))
			}
		}

	case tree.UnqualifiedStar:
		checkFrom(expr, inScope)
		exprs = make([]tree.TypedExpr, 0, len(inScope.cols))
		aliases = make([]string, 0, len(inScope.cols))
		for i := range inScope.cols {
			col := &inScope.cols[i]
			if !col.hidden {
				exprs = append(exprs, col)
				aliases = append(aliases, string(col.name))
			}
		}

	default:
		panic(pgerror.AssertionFailedf("unhandled type: %T", expr))
	}

	return aliases, exprs
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
// alias  This is an optional alias for the new column (e.g., if specified with
//        the AS keyword).
// typ    The type of the column.
// expr   The expression this column refers to (if any).
// group  The memo group ID of this column/expression (if any). This parameter
//        is optional and can be set later in the returned scopeColumn.
//
// The new column is returned as a scopeColumn object.
func (b *Builder) synthesizeColumn(
	scope *scope, alias string, typ *types.T, expr tree.TypedExpr, scalar opt.ScalarExpr,
) *scopeColumn {
	name := tree.Name(alias)
	colID := b.factory.Metadata().AddColumn(alias, typ)
	scope.cols = append(scope.cols, scopeColumn{
		name:   name,
		typ:    typ,
		id:     colID,
		expr:   expr,
		scalar: scalar,
	})
	return &scope.cols[len(scope.cols)-1]
}

// populateSynthesizedColumn is similar to synthesizeColumn, but it fills in
// the given existing column rather than allocating a new one.
func (b *Builder) populateSynthesizedColumn(col *scopeColumn, scalar opt.ScalarExpr) {
	colID := b.factory.Metadata().AddColumn(string(col.name), col.typ)
	col.id = colID
	col.scalar = scalar
}

// projectColumn projects src by copying its column ID to dst. projectColumn
// also copies src.name to dst if an alias is not already set in dst. No other
// fields are copied, for the following reasons:
// - We don't copy group, as dst becomes a pass-through column in the new
//   scope. dst already has group=0, so keep it as-is.
// - We don't copy hidden, because projecting a column makes it visible.
//   dst already has hidden=false, so keep it as-is.
// - We don't copy table, since the table becomes anonymous in the new scope.
// - We don't copy descending, since we don't want to overwrite dst.descending
//   if dst is an ORDER BY column.
// - expr, exprStr and typ in dst already correspond to the expression and type
//   of the src column.
func (b *Builder) projectColumn(dst *scopeColumn, src *scopeColumn) {
	if dst.name == "" {
		dst.name = src.name
	}
	dst.id = src.id
}

// addColumn adds a column to scope with the given alias, type, and
// expression. It returns a pointer to the new column. The column ID and group
// are left empty so they can be filled in later.
func (b *Builder) addColumn(scope *scope, alias string, expr tree.TypedExpr) *scopeColumn {
	name := tree.Name(alias)
	scope.cols = append(scope.cols, scopeColumn{
		name: name,
		typ:  expr.ResolvedType(),
		expr: expr,
	})
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) synthesizeResultColumns(scope *scope, cols sqlbase.ResultColumns) {
	for i := range cols {
		c := b.synthesizeColumn(scope, cols[i].Name, cols[i].Typ, nil /* expr */, nil /* scalar */)
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
			panic(pgerror.Newf(
				pgerror.CodeSyntaxError,
				"non-integer constant in %s: %s", context, expr,
			))
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		panic(pgerror.Newf(
			pgerror.CodeSyntaxError, "non-integer constant in %s: %s", context, expr,
		))
	case tree.Datum:
		panic(pgerror.Newf(
			pgerror.CodeSyntaxError, "non-integer constant in %s: %s", context, expr,
		))
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			panic(pgerror.Newf(
				pgerror.CodeInvalidColumnReferenceError,
				"%s position %s is not in select list", context, expr,
			))
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

		if c, ok := v.(*tree.ColumnItem); ok && c.TableName == nil {
			// Look for an output column that matches the name. This
			// handles cases like:
			//
			//   SELECT a AS b FROM t ORDER BY b
			//   SELECT DISTINCT ON (b) a AS b FROM t
			target := c.ColumnName
			for j := range scope.cols {
				col := &scope.cols[j]
				if col.name != target {
					continue
				}

				if col.mutation {
					panic(builderError{makeBackfillError(col.name)})
				}

				if index != -1 {
					// There is more than one projection alias that matches the clause.
					// Here, SQL92 is specific as to what should be done: if the
					// underlying expression is known and it is equivalent, then just
					// accept that and ignore the ambiguity. This plays nice with
					// `SELECT b, * FROM t ORDER BY b`. Otherwise, reject with an
					// ambiguity error.
					if scope.cols[j].getExprStr() != scope.cols[index].getExprStr() {
						panic(pgerror.Newf(pgerror.CodeAmbiguousAliasError,
							"%s \"%s\" is ambiguous", op, target))
					}
					// Use the index of the first matching column.
					continue
				}
				index = j
			}
		}
	}

	return index
}

// makeBackfillError returns an error indicating that the column of the given
// name is currently being backfilled and cannot be referenced.
func makeBackfillError(name tree.Name) error {
	return pgerror.Newf(pgerror.CodeInvalidColumnReferenceError,
		"column %q is being backfilled", tree.ErrString(&name))
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
			pgerror.Newf(pgerror.CodeGroupingError, "aggregate functions are not allowed in %s", op),
		})
	}
	if b.exprTransformCtx.WindowFuncInExpr(expr) {
		panic(builderError{
			pgerror.Newf(pgerror.CodeWindowingError, "window functions are not allowed in %s", op),
		})
	}
}

// resolveSchemaForCreate returns the schema that will contain a newly created
// catalog object with the given name. If the current user does not have the
// CREATE privilege, then resolveSchemaForCreate raises an error.
func (b *Builder) resolveSchemaForCreate(name *tree.TableName) (cat.Schema, cat.SchemaName) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	sch, resName, err := b.catalog.ResolveSchema(b.ctx, flags, &name.TableNamePrefix)
	if err != nil {
		// Remap invalid schema name error text so that it references the catalog
		// object that could not be created.
		if code := pgerror.GetPGCode(err); code == pgcode.InvalidSchemaName {
			var newErr error
			newErr = pgerror.Newf(pgcode.InvalidSchemaName,
				"cannot create %q because the target database or schema does not exist",
				tree.ErrString(name))
			newErr = errors.WithSecondaryError(newErr, err)
			newErr = errors.WithHint(newErr, "verify that the current database and search_path are valid and/or the target database exists")
			panic(newErr)
		}
		panic(builderError{err})
	}

	// Only allow creation of objects in the public schema.
	if resName.Schema() != tree.PublicSchema {
		panic(pgerror.Newf(pgerror.CodeInvalidNameError,
			"schema cannot be modified: %q", tree.ErrString(&resName)))
	}

	if err := b.catalog.CheckPrivilege(b.ctx, sch, privilege.CREATE); err != nil {
		panic(builderError{err})
	}

	// Add dependency on this schema to the metadata, so that the metadata can be
	// cached and later checked for freshness.
	b.factory.Metadata().AddSchemaDependency(&name.TableNamePrefix, sch, privilege.CREATE)
	return sch, resName
}

// resolveTable returns the data source in the catalog with the given name. If
// the name does not resolve to a table, or if the current user does not have
// the given privilege, then resolveTable raises an error.
func (b *Builder) resolveTable(
	tn *tree.TableName, priv privilege.Kind,
) (cat.Table, tree.TableName) {
	ds, resName := b.resolveDataSource(tn, priv)
	tab, ok := ds.(cat.Table)
	if !ok {
		panic(builderError{sqlbase.NewWrongObjectTypeError(tn, "table")})
	}
	return tab, resName
}

// resolveDataSource returns the data source in the catalog with the given name.
// If the name does not resolve to a table, or if the current user does not have
// the given privilege, then resolveDataSource raises an error.
func (b *Builder) resolveDataSource(
	tn *tree.TableName, priv privilege.Kind,
) (cat.DataSource, cat.DataSourceName) {
	ds, resName, err := b.catalog.ResolveDataSource(b.ctx, cat.Flags{}, tn)
	if err != nil {
		panic(builderError{err})
	}
	b.checkPrivilege(tn, ds, priv)
	return ds, resName
}

// resolveDataSourceFromRef returns the data source in the catalog that matches
// the given TableRef spec. If no data source matches, or if the current user
// does not have the given privilege, then resolveDataSourceFromRef raises an
// error.
func (b *Builder) resolveDataSourceRef(ref *tree.TableRef, priv privilege.Kind) cat.DataSource {
	ds, err := b.catalog.ResolveDataSourceByID(b.ctx, cat.StableID(ref.TableID))
	if err != nil {
		panic(builderError{pgerror.Wrapf(err, pgerror.CodeUndefinedObjectError,
			"%s", tree.ErrString(ref))})
	}
	b.checkPrivilege(ds.Name(), ds, priv)
	return ds
}

// checkPrivilege ensures that the current user has the privilege needed to
// access the given object in the catalog. If not, then checkPrivilege raises an
// error. It also adds the object and it's original unresolved name as a
// dependency to the metadata, so that the privileges can be re-checked on reuse
// of the memo.
func (b *Builder) checkPrivilege(
	origName *cat.DataSourceName, ds cat.DataSource, priv privilege.Kind,
) {
	if !(priv == privilege.SELECT && b.skipSelectPrivilegeChecks) {
		err := b.catalog.CheckPrivilege(b.ctx, ds, priv)
		if err != nil {
			panic(builderError{err})
		}
	} else {
		// The check is skipped, so don't recheck when dependencies are checked.
		priv = 0
	}

	// Add dependency on this object to the metadata, so that the metadata can be
	// cached and later checked for freshness.
	b.factory.Metadata().AddDataSourceDependency(origName, ds, priv)
}
