// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// VarName occurs inside scalar expressions.
//
// Immediately after parsing, the following types can occur:
//
// - UnqualifiedStar: a naked star as argument to a function, e.g. count(*),
//   or at the top level of a SELECT clause.
//   See also uses of StarExpr() and StarSelectExpr() in the grammar.
//
// - UnresolvedName: other names of the form `a.b....e` or `a.b...e.*`.
//
// Consumers of variable names do not like UnresolvedNames and instead
// expect either AllColumnsSelector or ColumnItem. Use
// NormalizeVarName() for this.
//
// After a ColumnItem is available, it should be further resolved, for this
// the Resolve() method should be used; see name_resolution.go.
type VarName interface {
	TypedExpr

	// NormalizeVarName() guarantees to return a variable name
	// that is not an UnresolvedName. This converts the UnresolvedName
	// to an AllColumnsSelector or ColumnItem as necessary.
	NormalizeVarName() (VarName, error)
}

var _ VarName = &UnresolvedName{}
var _ VarName = UnqualifiedStar{}
var _ VarName = &AllColumnsSelector{}
var _ VarName = &TupleStar{}
var _ VarName = &ColumnItem{}

// UnqualifiedStar corresponds to a standalone '*' in a scalar
// expression.
type UnqualifiedStar struct{}

// Format implements the NodeFormatter interface.
func (UnqualifiedStar) Format(ctx *FmtCtx) { ctx.WriteByte('*') }
func (u UnqualifiedStar) String() string   { return AsString(u) }

// NormalizeVarName implements the VarName interface.
func (u UnqualifiedStar) NormalizeVarName() (VarName, error) { return u, nil }

var singletonStarName VarName = UnqualifiedStar{}

// StarExpr is a convenience function that represents an unqualified "*".
func StarExpr() VarName { return singletonStarName }

// ResolvedType implements the TypedExpr interface.
func (UnqualifiedStar) ResolvedType() *types.T {
	panic(errors.AssertionFailedf("unqualified stars ought to be replaced before this point"))
}

// Variable implements the VariableExpr interface.
func (UnqualifiedStar) Variable() {}

// UnresolvedName is defined in name_part.go. It also implements the
// VarName interface, and thus TypedExpr too.

// ResolvedType implements the TypedExpr interface.
func (*UnresolvedName) ResolvedType() *types.T {
	panic(errors.AssertionFailedf("unresolved names ought to be replaced before this point"))
}

// Variable implements the VariableExpr interface.  Although, the
// UnresolvedName ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (*UnresolvedName) Variable() {}

// NormalizeVarName implements the VarName interface.
func (n *UnresolvedName) NormalizeVarName() (VarName, error) {
	return classifyColumnItem(n)
}

// AllColumnsSelector corresponds to a selection of all
// columns in a table when used in a SELECT clause.
// (e.g. `table.*`).
type AllColumnsSelector struct {
	// TableName corresponds to the table prefix, before the star.
	TableName *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (a *AllColumnsSelector) Format(ctx *FmtCtx) {
	ctx.FormatNode(a.TableName)
	ctx.WriteString(".*")
}
func (a *AllColumnsSelector) String() string { return AsString(a) }

// NormalizeVarName implements the VarName interface.
func (a *AllColumnsSelector) NormalizeVarName() (VarName, error) { return a, nil }

// Variable implements the VariableExpr interface.  Although, the
// AllColumnsSelector ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (a *AllColumnsSelector) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (*AllColumnsSelector) ResolvedType() *types.T {
	panic(errors.AssertionFailedf("all-columns selectors ought to be replaced before this point"))
}

// ColumnItem corresponds to the name of a column in an expression.
type ColumnItem struct {
	// TableName holds the table prefix, if the name refers to a column. It is
	// optional.
	//
	// This uses UnresolvedObjectName because we need to preserve the
	// information about which parts were initially specified in the SQL
	// text. ColumnItems are intermediate data structures anyway, that
	// still need to undergo name resolution.
	TableName *UnresolvedObjectName
	// ColumnName names the designated column.
	ColumnName Name
}

// Format implements the NodeFormatter interface.
// If this is updated, then dummyColumnItem.Format should be updated as well.
func (c *ColumnItem) Format(ctx *FmtCtx) {
	if c.TableName != nil {
		ctx.FormatNode(c.TableName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.ColumnName)
}
func (c *ColumnItem) String() string { return AsString(c) }

// NormalizeVarName implements the VarName interface.
func (c *ColumnItem) NormalizeVarName() (VarName, error) { return c, nil }

// Column retrieves the unqualified column name.
func (c *ColumnItem) Column() string {
	return string(c.ColumnName)
}

// Variable implements the VariableExpr interface.
//
// Note that in common uses, ColumnItem ought to be replaced to an
// IndexedVar prior to evaluation.
func (c *ColumnItem) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (c *ColumnItem) ResolvedType() *types.T {
	if presetTypesForTesting == nil {
		return nil
	}
	return presetTypesForTesting[c.String()]
}

// NewColumnItem constructs a column item from an already valid
// TableName. This can be used for e.g. pretty-printing.
func NewColumnItem(tn *TableName, colName Name) *ColumnItem {
	c := MakeColumnItem(tn, colName)
	return &c
}

// MakeColumnItem constructs a column item from an already valid
// TableName. This can be used for e.g. pretty-printing.
func MakeColumnItem(tn *TableName, colName Name) ColumnItem {
	c := ColumnItem{ColumnName: colName}
	if tn.Table() != "" {
		numParts := 1
		if tn.ExplicitCatalog {
			numParts = 3
		} else if tn.ExplicitSchema {
			numParts = 2
		}

		c.TableName = &UnresolvedObjectName{
			NumParts: numParts,
			Parts:    [3]string{tn.Table(), tn.Schema(), tn.Catalog()},
		}
	}
	return c
}
