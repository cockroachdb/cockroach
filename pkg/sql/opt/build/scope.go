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
		tblName := optbase.TableName(t.TableName.Table())
		colName := optbase.ColumnName(t.ColumnName)

		for curr := s; curr != nil; curr = curr.parent {
			for i := range curr.cols {
				col := &curr.cols[i]
				if col.matches(tblName, colName) {
					if tblName == "" && col.table != "" {
						// TODO(andy): why is this necessary??
						t.TableName.TableName = tree.Name(col.table)
						t.TableName.OmitSchemaNameDuringFormatting = true
					}
					return false, col
				}
			}
		}

		panic(errorf("unknown column %s", columnProps{name: colName, table: tblName}))

		// TODO(rytaft): Implement function expressions and subquery replacement.
	}

	return true, expr
}

// VisitPost is part of the Visitor interface.
func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}
