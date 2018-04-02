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

// scopeColumn holds per-column information that is scoped to a particular
// relational expression. Note that scopeColumn implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a scopeColumn.
type scopeColumn struct {
	// origName is the original name of this column, either in its origin table,
	// or when it was first synthesized.
	origName tree.Name

	// name is the current name of this column. It is usually the same as
	// origName, unless this column was renamed with an AS expression.
	name  tree.Name
	table tree.TableName
	typ   types.T

	// id is an identifier for this column, which is unique across all the
	// columns in the query.
	id     opt.ColumnID
	hidden bool

	// group is the memo.GroupID of the column, which is updated after the
	// column is built in the memo.
	group memo.GroupID

	// expr is the expression that this column refers to, if any. expr is nil if
	// the column does not refer to an expression.
	expr tree.TypedExpr

	// exprStr contains a stringified representation of expr, or the original
	// column name if expr is nil. It is populated lazily inside getExprStr().
	exprStr string
}

// getExprStr gets a stringified representation of the expression that this
// column refers to, or the original column name if the column does not refer
// to an expression. It caches the result in exprStr.
func (c *scopeColumn) getExprStr() string {
	if c.exprStr == "" {
		if c.expr == nil {
			if tableStr := c.table.String(); tableStr != "" {
				c.exprStr = fmt.Sprintf("%s.%s", tableStr, c.origName)
			} else {
				c.exprStr = string(c.origName)
			}
		} else {
			c.exprStr = symbolicExprStr(c.expr)
		}
	}
	return c.exprStr
}

var _ tree.Expr = &scopeColumn{}
var _ tree.TypedExpr = &scopeColumn{}
var _ tree.VariableExpr = &scopeColumn{}

func (c *scopeColumn) String() string {
	return tree.AsString(c)
}

// Format implements the NodeFormatter interface.
func (c *scopeColumn) Format(ctx *tree.FmtCtx) {
	if c.table.TableName != "" {
		if c.table.ExplicitSchema && c.table.SchemaName != "" {
			if c.table.ExplicitCatalog && c.table.CatalogName != "" {
				ctx.FormatNode(&c.table.CatalogName)
				ctx.WriteByte('.')
			}
			ctx.FormatNode(&c.table.SchemaName)
			ctx.WriteByte('.')
		}

		ctx.FormatNode(&c.table.TableName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.name)
}

// Walk is part of the tree.Expr interface.
func (c *scopeColumn) Walk(v tree.Visitor) tree.Expr {
	return c
}

// TypeCheck is part of the tree.Expr interface.
func (c *scopeColumn) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return c, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (c *scopeColumn) ResolvedType() types.T {
	return c.typ
}

// Eval is part of the tree.TypedExpr interface.
func (*scopeColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("scopeColumn must be replaced before evaluation"))
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*scopeColumn) Variable() {}
