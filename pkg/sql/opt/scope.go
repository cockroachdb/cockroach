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

package opt

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// scope implements the tree.Visitor interface, which allows us to
// walk through the query tree to perform name resolution and type
// checking.
type scope struct {
	parent *scope
	cols   []columnProps
	state  *queryState
}

// push adds a new child scope below scope s.
func (s *scope) push() *scope {
	return &scope{parent: s, state: s.state}
}

// resolve performs name resolution and converts expr to a TypedExpr tree.
// All column references in the expression tree are converted to well-typed
// columnProps objects, allowing each column to be uniquely identified and
// accessed throughout the query tree.
func (s *scope) resolve(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr, _ = tree.WalkExpr(s, expr)
	texpr, err := tree.TypeCheck(expr, &s.state.semaCtx, desired)
	if err != nil {
		panic(err)
	}

	return texpr
}

// FindSourceMatchingName implements the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colName tree.Name,
) (prefix *tree.TableName, srcMeta interface{}, colHint int, err error) {
	for ; s != nil; s = s.parent {
		for i := range s.cols {
			col := &s.cols[i]
			if col.hasColumn("", columnName(colName)) {
				// TODO(peter): source names in a FROM clause also have a catalog/schema prefix and it matters.
				return tree.NewUnqualifiedTableName(tree.Name(col.table)), col, col.index, nil
			}
		}
	}
	return nil, nil, -1, fmt.Errorf("unknown column %s", colName)
}

// FindSourceMatchingName implements the tree.ColumnItemResolver interface.
func (s *scope) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (res tree.NumResolutionResults, prefix *tree.TableName, srcMeta interface{}, err error) {
	for ; s != nil; s = s.parent {
		for i := range s.cols {
			col := &s.cols[i]
			// TODO(whomever): this improperly disregards the catalog/schema prefix.
			if col.table == tableName(tn.Table()) {
				// TODO(whomever): this improperly fails to recognize when a source table
				// is ambiguous, e.g. SELECT kv.k FROM db1.kv, db2.kv
				return tree.ExactlyOne, tree.NewUnqualifiedTableName(tree.Name(col.table)), s, nil
			}
		}
	}
	return tree.NoResults, nil, nil, nil
}

// Resolve implements the tree.ColumnItemResolver interface.
func (s *scope) Resolve(
	_ context.Context, prefix *tree.TableName, srcMeta interface{}, colHint int, colName tree.Name,
) (interface{}, error) {
	if colHint >= 0 {
		// Column was found by FindSourceProvidingColumn above.
		return srcMeta.(*columnProps), nil
	}
	// Otherwise, a table is known but not the column yet.
	inScope := srcMeta.(*scope)
	for i := range inScope.cols {
		col := &s.cols[i]
		if col.hasColumn(tableName(prefix.Table()), columnName(colName)) {
			return col, nil
		}
	}
	return nil, fmt.Errorf("unknown column %s", colName)
}

// VisitPre is part of the tree.Visitor interface.
// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		colI, err := t.Resolve(context.TODO(), s)
		if err != nil {
			panic(err)
		}
		return false, colI.(*columnProps)

		// TODO(rytaft): Implement function expressions and subquery replacement.
	}

	return true, expr
}

// VisitPost is part of the tree.Visitor interface.
func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

var _ tree.Expr = &columnProps{}

// Format is part of the tree.Expr interface.
func (c *columnProps) Format(ctx *tree.FmtCtx) {
	ctx.Printf("@%d", c.index+1)
}

// Walk is part of the tree.Expr interface.
func (c *columnProps) Walk(v tree.Visitor) tree.Expr {
	return c
}

// TypeCheck is part of the tree.Expr interface.
func (c *columnProps) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return c, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (c *columnProps) ResolvedType() types.T {
	return c.typ
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*columnProps) Variable() {}

// Eval is part of the tree.TypedExpr interface.
func (*columnProps) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("columnProps must be replaced before evaluation"))
}
