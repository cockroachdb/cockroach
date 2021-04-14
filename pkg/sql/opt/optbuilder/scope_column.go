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
	// name is the current name of this column in the scope. It is used to
	// resolve column references when building expressions. It is usually the
	// same as the original name, unless this column was renamed with an AS
	// expression.
	name scopeColumnName

	table tree.TableName
	typ   *types.T

	// id is an identifier for this column, which is unique across all the
	// columns in the query.
	id opt.ColumnID

	visibility cat.ColumnVisibility

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

// clearName sets the empty table and column name. This is used to make the
// column anonymous so that it cannot be referenced, but will still be
// projected.
// TODO(mgartner): Do we still need this?
func (c *scopeColumn) clearName() {
	c.name.Anonymize()
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
	colName := c.name.ReferenceName()
	ctx.FormatNode(&colName)
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

// scopeColumnName represents the name of a scopeColumn. The struct tracks two
// names: refName represents the name that can reference the scopeColumn, while
// metadataName is the name used when adding the scopeColumn to the metadata.
//
// Separating these names allows a column to be referenced by one name while
// optbuilder builds expressions, but added to the metadata and displayed in opt
// trees with another. This is useful for:
//
//   1. Creating more descriptive metadata names, while having descriptive
//      refNames that are required for column resolution while building a
//      expressions. This is particularly useful in mutations where there
//      are multiple versions of target table columns for fetching,
//      inserting, and updating that must be referenced by the same name.
//   2. Creating descriptive metadata names for anonymous columns that
//      cannot be referenced. This is useful for columns like synthesized
//      check constraint columns and partial index columns which cannot be
//      referenced by other expressions. Prior to the creation of
//      scopeColumnName, the same descriptive name added to the metadata
//      could be referenced, making optbuilder vulnerable to "ambiguous
//      column" bugs when a user table had a column with the same name.
type scopeColumnName struct {
	// refName is the name used when resolving columns while building an
	// expression. If it is empty, the column is anonymous and cannot be
	// referenced.
	refName string

	// metadataName is the name used when adding the column to the metadata, if
	// present. If metadataName is blank, refName should be used when adding the
	// column to the metadata.
	metadataName string
}

// scopeColName creates a scopeColumnName that both be referenced by the given
// name and will be added to the metadata with the given name.
func scopeColName(name string) scopeColumnName {
	return scopeColumnName{
		refName: name,
	}
}

// scopeColNameWithMetadataName creates a scopeColumnName that can be referenced
// by name and will be added to the metadata with the given metadataName. This
// is useful for columns which must be referenced by a particular name, but
// would benefit from a more descriptive name in the metadata.
func scopeColNameWithMetadataName(name, metadataName string) scopeColumnName {
	return scopeColumnName{
		refName:      name,
		metadataName: metadataName,
	}
}

// anonymousScopeColName creates an anonymous scopeColumnName that cannot be
// referenced. It will be added to the metadata with a name of the form
// "column<ID>".
func anonymousScopeColName() scopeColumnName {
	return scopeColumnName{}
}

// anonymousScopeColNameWithMetadataName creates an anonymous scopeColumnName
// that cannot be referenced. It will be added to the metadata with the given
// metadataName. This is useful for columns like synthesized check constraint
// columns and partial index columns, which cannot be referenced but benefit
// from having descriptive names
func anonymousScopeColNameWithMetadataName(metadataName string) scopeColumnName {
	return scopeColumnName{
		metadataName: metadataName,
	}
}

// Anonymize makes the scopeColumnName unable to be referenced.
func (s *scopeColumnName) Anonymize() {
	s.refName = ""
}

// IsAnonymous returns true if the scopeColumnName is a name that cannot be
// referenced.
func (s *scopeColumnName) IsAnonymous() bool {
	return s.refName == ""
}

// MatchesReferenceName returns true if the given name references the
// scopeColumn with this scopeColumnName.
func (s *scopeColumnName) MatchesReferenceName(ref tree.Name) bool {
	return s.refName == string(ref)
}

// MatchesReferenceString returns true if the given string references the
// scopeColumn with this scopeColumnName.
func (s *scopeColumnName) MatchesReferenceString(ref string) bool {
	return s.refName == ref
}

// ReferenceName returns the name that the scopeColumn with this scopeColumnName
// can be referenced by.
func (s *scopeColumnName) ReferenceName() tree.Name {
	return tree.Name(s.refName)
}

// ReferenceString returns the string that the scopeColumn with this
// scopeColumnName can be referenced by.
func (s *scopeColumnName) ReferenceString() string {
	return s.refName
}

// SetReferenceName sets the name that the scopeColumn with this scopeColumnName
// can be referenced by.
func (s *scopeColumnName) SetReferenceName(ref tree.Name) {
	s.refName = string(ref)
}

// MetadataName returns the string to use when adding the scopeColumn with this
// scopeColumnName to metadata. It returns a metadata-specific name, if present.
// Otherwise, it returns the same name that is used to reference the
// scopeColumn.
func (s *scopeColumnName) MetadataName() string {
	if s.metadataName != "" {
		return s.metadataName
	}
	return s.refName
}
