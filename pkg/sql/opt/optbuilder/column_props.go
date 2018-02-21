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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// columnProps holds per-column information that is scoped to a particular
// relational expression. Note that columnProps implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a columnProps.
type columnProps struct {
	name  tree.Name
	table tree.TableName
	typ   types.T

	// index is an identifier for this column, which is unique across all the
	// columns in the query.
	index  opt.ColumnIndex
	hidden bool
}

var _ tree.Expr = &columnProps{}
var _ tree.TypedExpr = &columnProps{}
var _ tree.VariableExpr = &columnProps{}

func (c *columnProps) String() string {
	return tree.AsString(c)
}

// Format implements the NodeFormatter interface.
func (c *columnProps) Format(ctx *tree.FmtCtx) {
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

// Eval is part of the tree.TypedExpr interface.
func (*columnProps) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("columnProps must be replaced before evaluation"))
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*columnProps) Variable() {}
