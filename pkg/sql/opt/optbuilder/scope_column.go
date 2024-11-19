// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	// resolve column references when building expressions and to populate the
	// metadata with a descriptive name.
	name scopeColumnName

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

	// paramOrd is the 1-based ordinal of the parameter of the function that
	// the column corresponds to. It is used to resolve placeholders (e.g., $1)
	// in function bodies that are references to function arguments. If the
	// column does not represent a function parameter, then paramOrd is the zero
	// value.
	paramOrd funcParamOrd

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

// maxFuncParams is the maximum number of parameters allowed in a function.
const maxFuncParams = 100

// funcParamOrd is a 1-based ordinal of a function parameter.
type funcParamOrd int8

// setParamOrd sets the column's 1-based function parameter ordinal to the given
// 0-based ordinal. Panics if the given ordinal is not in the range
// [0, maxFuncParams).
func (c *scopeColumn) setParamOrd(ord int) {
	if ord < 0 {
		panic(errors.AssertionFailedf("expected non-negative argument ordinal"))
	}
	if ord >= maxFuncParams {
		panic(pgerror.New(pgcode.TooManyArguments, "functions cannot have more than 100 arguments"))
	}
	c.paramOrd = funcParamOrd(ord + 1)
}

// getParamOrd retrieves the 0-based ordinal from the column's 1-based function
// parameter ordinal. Panics if the function ordinal is unset.
func (c *scopeColumn) getParamOrd() int {
	if c.paramOrd < 1 {
		panic(errors.AssertionFailedf("expected non-negative argument ordinal"))
	}
	return int(c.paramOrd) - 1
}

// funcParamReferencedBy returns true if the scopeColumn is a function parameter
// column that can be referenced by the given placeholder.
func (c *scopeColumn) funcParamReferencedBy(idx tree.PlaceholderIdx) bool {
	return c.paramOrd > 0 && tree.PlaceholderIdx(c.paramOrd-1) == idx
}

// clearName sets the empty table and column name. This is used to make the
// column anonymous so that it cannot be referenced, but will still be
// projected.
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

func (c *scopeColumn) Eval(ctx context.Context, v tree.ExprEvaluator) (tree.Datum, error) {
	panic(errors.AssertionFailedf("scopeColumn must be replaced before evaluation"))
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
//  1. Creating more descriptive metadata names, while having refNames that are
//     required for column resolution while building expressions. This is
//     particularly useful in mutations where there are multiple versions of
//     target table columns for fetching, inserting, and updating that must be
//     referenced by the same name.
//
//  2. Creating descriptive metadata names for anonymous columns that
//     cannot be referenced. This is useful for columns like synthesized
//     check constraint columns and partial index columns which cannot be
//     referenced by other expressions. Prior to the creation of
//     scopeColumnName, the same descriptive name added to the metadata
//     could be referenced, making optbuilder vulnerable to "ambiguous
//     column" bugs when a user table had a column with the same name.
type scopeColumnName struct {
	// refName is the name used when resolving columns while building an
	// expression. If it is empty, the column is anonymous and cannot be
	// referenced. It is usually the same as the original column name, unless
	// this column was renamed with an AS expression.
	refName tree.Name

	// metadataName is the name used when adding the column to the metadata. It
	// is inconsequential while building expressions; it only makes plans more
	// readable.
	metadataName string
}

// scopeColName creates a scopeColumnName that can be referenced by the given
// name and will be added to the metadata with the given name. If name is an
// empty string, the returned scopeColumnName is anonymous; it cannot be
// referenced in expressions and it will be added to the metadata with a name of
// the form "column<ID>".
func scopeColName(name tree.Name) scopeColumnName {
	return scopeColumnName{
		refName:      name,
		metadataName: string(name),
	}
}

// funcParamColName creates a scopeColumnName that can be referenced by the given
// name and will be added to the metadata with the given name, if the given name
// is not empty. If the given name is empty, the returned scopeColumnName
// represents an anonymous function argument that cannot be referenced, and it
// will be added to the metadata with the descriptive name "arg<ord>".
// TODO(119502): unnamed parameters can be referenced via $i notation (for SQL
// routines only IN / INOUT parameters can be referenced this way, for PLpgSQL
// routines all parameters can).
func funcParamColName(name tree.Name, ord int) scopeColumnName {
	alias := string(name)
	if alias == "" {
		alias = fmt.Sprintf("arg%d", ord+1)
	}
	return scopeColName(name).WithMetadataName(alias)
}

// WithMetadataName returns a copy of s with the metadata name set to the given
// name. This only affects the name of the column in the metadata. It does not
// change the name by which the column can be referenced.
func (s scopeColumnName) WithMetadataName(name string) scopeColumnName {
	s.metadataName = name
	return s
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
	return s.refName == ref
}

// ReferenceName returns the name that the scopeColumn with this scopeColumnName
// can be referenced by.
func (s *scopeColumnName) ReferenceName() tree.Name {
	return s.refName
}

// MetadataName returns the string to use when adding the scopeColumn with this
// scopeColumnName to metadata.
func (s *scopeColumnName) MetadataName() string {
	return s.metadataName
}
