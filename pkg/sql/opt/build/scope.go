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

package build

import (
	"context"
	"fmt"

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
	// TODO(rytaft): Add group by and ordering to scope.
}

// push creates a new scope with this scope as its parent.
func (s *scope) push() *scope {
	return &scope{builder: s.builder, parent: s}
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
		panic(err)
	}

	return texpr
}

// scope implements the tree.Visitor interface so that it can walk through
// a tree.Expr tree, perform name resolution, and replace unresolved column
// names with a columnProps. The info stored in columnProps is necessary for
// Builder.buildScalar to construct a "variable" memo expression.
var _ tree.Visitor = &scope{}

// FindSourceMatchingName implements the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colNameName tree.Name,
) (prefix *tree.TableName, srcMeta interface{}, colHint int, err error) {
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

// FindSourceMatchingName implements the tree.ColumnItemResolver interface.
func (s *scope) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (res tree.NumResolutionResults, prefix *tree.TableName, srcMeta interface{}, err error) {
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

// Resolve implements the tree.ColumnItemResolver interface.
func (s *scope) Resolve(
	_ context.Context, prefix *tree.TableName, srcMeta interface{}, colHint int, colNameName tree.Name,
) (interface{}, error) {
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

// VisitPost is part of the Visitor interface.
func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}
