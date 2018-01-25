// Copyright 2016 The Cockroach Authors.
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

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Variable names are used in multiples places in SQL:
//
// - if the context is a direct select target, then the name may end
//   with '*' for a column group, with optional database and table prefix.
//
//   Syntax: [ [ [ <database-name> '.' ] <schema-name> '.' ] <table-name> '.' ] '*'
//
//   Represented by: UnqualifiedStar, *AllColumnsSelector (VarName)
//   Found by: NormalizeVarName()
//
// - elsewhere, the name is for a optionally-qualified column name.
//
//   Syntax: [ [ [ <database-name> '.' ] <schema-name> '.' ] <table-name> '.' ]  <column-name>
//
//   Represented by: ColumnItem (VarName)
//   Found by: NormalizeVarName()
//
// The common type in expression contexts is VarName. This extends
// TypedExpr and VariableExpr so it can be used in expression trees
// directly.  During parsing a name in expressions always begins as an
// UnresolvedName instance.  During either IndexedVar substitution, type
// checking or render target expansion (render node) this is
// normalized and replaced by either *ColumnItem, UnqualifiedStar or
// AllColumnsSelector using the NormalizeVarName() method.

// VarName is the common interface to UnresolvedName,
// ColumnItem and AllColumnsSelector for use in expression contexts.
type VarName interface {
	TypedExpr

	NormalizeVarName() (VarName, error)
}

var _ VarName = &UnresolvedName{}
var _ VarName = UnqualifiedStar{}
var _ VarName = &AllColumnsSelector{}
var _ VarName = &ColumnItem{}

// NormalizeVarName is a no-op for UnqualifiedStar (already normalized)
func (u UnqualifiedStar) NormalizeVarName() (VarName, error) { return u, nil }

var singletonStarName VarName = UnqualifiedStar{}

// StarExpr is a convenience function that represents an unqualified "*".
func StarExpr() VarName { return singletonStarName }

// ResolvedType implements the TypedExpr interface.
func (UnqualifiedStar) ResolvedType() types.T {
	panic("unqualified stars ought to be replaced before this point")
}

// Variable implements the VariableExpr interface.
func (UnqualifiedStar) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (*UnresolvedName) ResolvedType() types.T {
	panic("unresolved names ought to be replaced before this point")
}

// Variable implements the VariableExpr interface.  Although, the
// UnresolvedName ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (*UnresolvedName) Variable() {}

// AllColumnsSelector corresponds to a selection of all
// columns in a table when used in a SELECT clause.
// (e.g. `table.*`)
type AllColumnsSelector struct {
	TableName
}

// Format implements the NodeFormatter interface.
func (a *AllColumnsSelector) Format(ctx *FmtCtx) {
	if !a.TableName.OmitSchemaNameDuringFormatting {
		ctx.FormatNode(&a.TableName.SchemaName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&a.TableName.TableName)
	ctx.WriteString(".*")
}
func (a *AllColumnsSelector) String() string { return AsString(a) }

// NormalizeVarName is a no-op for AllColumnsSelector (already normalized)
func (a *AllColumnsSelector) NormalizeVarName() (VarName, error) { return a, nil }

// Variable implements the VariableExpr interface.  Although, the
// AllColumnsSelector ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (a *AllColumnsSelector) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (*AllColumnsSelector) ResolvedType() types.T {
	panic("all-columns selectors ought to be replaced before this point")
}

// ColumnItem corresponds to the name of a column or sub-item
// of a column in an expression.
type ColumnItem struct {
	// TableName holds the table prefix, if the name refers to a column.
	TableName TableName
	// ColumnName names the designated column.
	ColumnName Name

	// This column is a selector column expression used in a SELECT
	// for an UPDATE/DELETE.
	// TODO(vivek): Do not artificially create such expressions
	// when scanning columns for an UPDATE/DELETE.
	ForUpdateOrDelete bool
}

// Format implements the NodeFormatter interface.
func (c *ColumnItem) Format(ctx *FmtCtx) {
	if c.TableName.TableName != "" {
		if !c.TableName.OmitSchemaNameDuringFormatting {
			ctx.FormatNode(&c.TableName.SchemaName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&c.TableName.TableName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&c.ColumnName)
}
func (c *ColumnItem) String() string { return AsString(c) }

// NormalizeVarName is a no-op for ColumnItem (already normalized)
func (c *ColumnItem) NormalizeVarName() (VarName, error) { return c, nil }

// Column retrieves the unqualified column name.
func (c *ColumnItem) Column() string {
	return string(c.ColumnName)
}

// Variable implements the VariableExpr interface.  Although, the
// ColumnItem ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (c *ColumnItem) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (c *ColumnItem) ResolvedType() types.T {
	if presetTypesForTesting == nil {
		return nil
	}
	return presetTypesForTesting[c.String()]
}

func newInvColRef(fmt string, args ...interface{}) error {
	return pgerror.NewErrorf(pgerror.CodeInvalidColumnReferenceError, fmt, args...)
}

// NormalizeVarName normalizes a UnresolvedName for all the forms it can have
// inside an expression context.
func (n *UnresolvedName) NormalizeVarName() (VarName, error) {
	var tn TableName
	if n.NumParts > 1 {
		tnPart := UnresolvedName{
			NumParts: n.NumParts - 1,
			Parts:    NameParts{n.Parts[1], n.Parts[2], n.Parts[3]},
		}
		var err error
		if tn, err = tnPart.normalizeTableNameAsValue(); err != nil {
			return nil, err
		}
	}
	if n.Star {
		return &AllColumnsSelector{tn}, nil
	}
	if len(n.Parts[0]) == 0 {
		return nil, newInvColRef("empty column name: %q", ErrString(n))
	}
	return &ColumnItem{TableName: tn, ColumnName: Name(n.Parts[0])}, nil
}
