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
		tblName := tableName(t.TableName.Table())
		colName := columnName(t.ColumnName)

		for ; s != nil; s = s.parent {
			for i := range s.cols {
				col := &s.cols[i]
				if col.hasColumn(tblName, colName) {
					// TODO(peter): what is this doing?
					if tblName == "" && col.table != "" {
						t.TableName.TableName = tree.Name(col.table)
						t.TableName.OmitDBNameDuringFormatting = true
					}
					return false, col
				}
			}
		}
		panic(fmt.Sprintf("unknown column %s", t))

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
