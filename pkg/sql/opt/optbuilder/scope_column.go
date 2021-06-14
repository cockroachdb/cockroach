// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// scopeColumn holds per-column information that is scoped to a particular
// relational expression. Note that scopeColumn implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a scopeColumn.
type scopeColumn struct {
	// name is the current name of this column. It is usually the same as
	// the original name, unless this column was renamed with an AS expression.
	name  tree.Name
	table tree.TableName
	typ   *types.T

	// id is an identifier for this column, which is unique across all the
	// columns in the query.
	id opt.ColumnID

	visibility columnVisibility

	// tableOrdinal is set to the table ordinal corresponding to this column, if
	// this is a column from a scan.
	tableOrdinal int

	// mutation is true if the column is in the process of being dropped or added
	// to the table. It should not be visible to variable references.
	mutation bool

	// kind of the table column, if this is a column from a scan.
	kind cat.ColumnKind

	// descending indicates whether this column is sorted in descending order.
	// This field is only used for ordering columns.
	descending bool

	// scalar is the scalar expression associated with this column. If it is nil,
	// then the column is a passthrough from an inner scope or a table column.
	scalar opt.ScalarExpr

	// expr is the AST expression that this column refers to, if any.
	// expr is nil if the column does not refer to an expression.
	expr tree.TypedExpr

	// exprStr contains a stringified representation of expr, or the original
	// column name if expr is nil. It is populated lazily inside getExprStr().
	exprStr string
}

// columnVisibility is an extension of cat.ColumnVisibility.
// cat.ColumnVisibility values can be converted directly.
type columnVisibility uint8

const (
	// visible columns are part of the presentation of a query. Visible columns
	// contribute to star expansions.
	visible = columnVisibility(cat.Visible)

	// accessibleByQualifiedStar columns are accessible by name or by a qualified
	// star "<table>.*". This is a rare case, occurring in joins:
	//   ab NATURAL LEFT JOIN ac
	// Here ac.a is not visible (or part of the unqualified star expansion) but it
	// is accessible via ac.*.
	accessibleByQualifiedStar = columnVisibility(10)

	// accessibleByName columns are accessible by name but are otherwise not
	// visible; they do not contribute to star expansions.
	accessibleByName = columnVisibility(cat.Hidden)

	// inaccessible columns cannot be referred to by name (or any other means).
	inaccessible = columnVisibility(cat.Inaccessible)
)

func (cv columnVisibility) String() string {
	switch cv {
	case visible:
		return "visible"
	case accessibleByQualifiedStar:
		return "accessible-by-qualified-star"
	case accessibleByName:
		return "accessible-by-name"
	case inaccessible:
		return "inaccessible"
	default:
		return "invalid-column-visibility"
	}
}

// clearName sets the empty table and column name. This is used to make the
// column anonymous so that it cannot be referenced, but will still be
// projected.
func (c *scopeColumn) clearName() {
	c.name = ""
	c.table = tree.TableName{}
}

// getExpr returns the expression that this column refers to, or the column
// itself if the column does not refer to an expression.
func (c *scopeColumn) getExpr() tree.TypedExpr {
	if c.expr == nil {
		return c
	}
	return c.expr
}

// getExprStr gets a stringified representation of the expression that this
// column refers to.
func (c *scopeColumn) getExprStr() string {
	if c.exprStr == "" {
		c.exprStr = symbolicExprStr(c.getExpr())
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
	// FmtCheckEquivalence is used by getExprStr when comparing expressions for
	// equivalence. If that flag is present, then use the unique column id to
	// differentiate this column from other columns.
	if ctx.HasFlags(tree.FmtCheckEquivalence) {
		// Use double @ to distinguish from Cockroach column ordinals.
		ctx.Printf("@@%d", c.id)
		return
	}

	if ctx.HasFlags(tree.FmtShowTableAliases) && c.table.ObjectName != "" {
		if c.table.ExplicitSchema && c.table.SchemaName != "" {
			if c.table.ExplicitCatalog && c.table.CatalogName != "" {
				ctx.FormatNode(&c.table.CatalogName)
				ctx.WriteByte('.')
			}
			ctx.FormatNode(&c.table.SchemaName)
			ctx.WriteByte('.')
		}

		ctx.FormatNode(&c.table.ObjectName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.name)
}

// Walk is part of the tree.Expr interface.
func (c *scopeColumn) Walk(v tree.Visitor) tree.Expr {
	return c
}

// TypeCheck is part of the tree.Expr interface.
func (c *scopeColumn) TypeCheck(
	_ context.Context, _ *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	return c, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (c *scopeColumn) ResolvedType() *types.T {
	return c.typ
}

// Eval is part of the tree.TypedExpr interface.
func (*scopeColumn) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("scopeColumn must be replaced before evaluation"))
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*scopeColumn) Variable() {}
