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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

// Variable names are used in multiples places in SQL:
//
// - if the context is the LHS of an UPDATE, then the name is for an
//   unqualified column or part of a column.
//
//   Syntax: <column-name> [ . <subfield-name> | '[' <index> ']' ]*
//   (the name always *starts* with a column name)
//
//   Represented by: ColumnItem
//   Found by: NormalizeUnqualifiedColumnItem()
//
// - if the context is a direct select target, then the name may end
//   with '*' for a column group, with optional database and table prefix.
//
//   Syntax: [ [ <database-name> '.' ] <table-name> '.' ] '*'
//
//   Represented by: UnqualifiedStar, *AllColumnsSelector (VarName)
//   Found by: NormalizeVarName()
//
// - elsewhere, the name is for a optionally-qualified column name
//   with optional array subscript followed by additional optional
//   subfield or array subscripts.
//
//   Syntax: [ [ <database-name> '.' ] <table-name> '.' ]
//              <column-name>
//           [ '[' <index> ']' [ '[' <index> ']' | '.' <subfield> ] * ]
//   (either there is no array subscript and the qualified name *ends*
//   with a column name; or there is an array subscript and the
//   supporting column's name is the last unqualified name before the first
//   array subscript).
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
//
// In the context of UpdateExprs, UnresolvedNames are translated to
// ColumnItem directly by NormalizeUnqualifiedColumnItem() without
// going through the VarName interface at all.

// VarName is the common interface to UnresolvedName,
// ColumnItem and AllColumnsSelector for use in expression contexts.
type VarName interface {
	TypedExpr

	NormalizeVarName() (VarName, error)
}

var _ VarName = UnresolvedName{}
var _ VarName = UnqualifiedStar{}
var _ VarName = &AllColumnsSelector{}
var _ VarName = &ColumnItem{}

// NormalizeVarName is a no-op for UnqualifiedStar (already normalized)
func (u UnqualifiedStar) NormalizeVarName() (VarName, error) { return u, nil }

var singletonStarName VarName = UnqualifiedStar{}

// StarExpr is a convenience function that represents an unqualified "*".
func StarExpr() VarName { return singletonStarName }

// ResolvedType implements the TypedExpr interface.
func (UnqualifiedStar) ResolvedType() Type {
	panic("unqualified stars ought to be replaced before this point")
}

// Variable implements the VariableExpr interface.
func (UnqualifiedStar) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (UnresolvedName) ResolvedType() Type {
	panic("unresolved names ought to be replaced before this point")
}

// Variable implements the VariableExpr interface.  Although, the
// UnresolvedName ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (UnresolvedName) Variable() {}

// AllColumnsSelector corresponds to a selection of all
// columns in a table when used in a SELECT clause.
// (e.g. `table.*`)
type AllColumnsSelector struct {
	TableName
}

// Format implements the NodeFormatter interface.
func (a *AllColumnsSelector) Format(buf *bytes.Buffer, f FmtFlags) {
	if a.TableName.DatabaseName != "" {
		FormatNode(buf, f, a.TableName.DatabaseName)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, a.TableName.TableName)
	buf.WriteString(".*")
}
func (a *AllColumnsSelector) String() string { return AsString(a) }

// NormalizeVarName is a no-op for AllColumnsSelector (already normalized)
func (a *AllColumnsSelector) NormalizeVarName() (VarName, error) { return a, nil }

// Variable implements the VariableExpr interface.  Although, the
// AllColumnsSelector ought to be replaced to an IndexedVar before the points the
// VariableExpr interface is used.
func (a *AllColumnsSelector) Variable() {}

// ResolvedType implements the TypedExpr interface.
func (*AllColumnsSelector) ResolvedType() Type {
	panic("all-columns selectors ought to be replaced before this point")
}

// ColumnItem corresponds to the name of a column or sub-item
// of a column in an expression.
type ColumnItem struct {
	// TableName holds the table prefix, if the name refers to a column.
	TableName TableName
	// ColumnName names the designated column.
	ColumnName Name
	// Selector defines which sub-part of the variable is being
	// accessed.
	Selector NameParts
}

// Format implements the NodeFormatter interface.
func (c *ColumnItem) Format(buf *bytes.Buffer, f FmtFlags) {
	if c.TableName.TableName != "" {
		if c.TableName.DatabaseName != "" {
			FormatNode(buf, f, c.TableName.DatabaseName)
			buf.WriteByte('.')
		}
		FormatNode(buf, f, c.TableName.TableName)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, c.ColumnName)
	if len(c.Selector) > 0 {
		if _, ok := c.Selector[0].(*ArraySubscript); !ok {
			buf.WriteByte('.')
		}
		FormatNode(buf, f, c.Selector)
	}
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
func (c *ColumnItem) ResolvedType() Type {
	if presetTypesForTesting == nil {
		return nil
	}
	return presetTypesForTesting[c.String()]
}

// NormalizeVarName normalizes a UnresolvedName for all the forms it can have
// inside an expression context.
func (n UnresolvedName) NormalizeVarName() (VarName, error) {
	if len(n) == 0 {
		return nil, fmt.Errorf("invalid name: %q", n)
	}

	if s, isStar := n[len(n)-1].(UnqualifiedStar); isStar {
		// Either a single '*' or a name of the form [db.]table.*

		if len(n) == 1 {
			return s, nil
		}

		// The prefix before the star must be a valid table name.  Use the
		// existing normalize code to enforce that, since we can reuse the
		// resulting TableName.
		t, err := n[:len(n)-1].normalizeTableNameAsValue()
		if err != nil {
			return nil, err
		}

		return &AllColumnsSelector{t}, nil
	}

	// In the remaining case, we have an optional table name prefix,
	// followed by a column name, followed by some additional selector.

	// Find the first array subscript, if any.
	i := len(n)
	for j, p := range n {
		if _, ok := p.(*ArraySubscript); ok {
			i = j
			break
		}
	}
	// The element at position i - 1 must be the column name.
	// (We don't support record types yet.)
	if i == 0 {
		return nil, fmt.Errorf("invalid column name: %q", n)
	}
	colName, ok := n[i-1].(Name)
	if !ok {
		return nil, fmt.Errorf("invalid column name: %q", n[:i])
	}
	if len(colName) == 0 {
		return nil, fmt.Errorf("empty column name: %q", n)
	}

	// Everything afterwards is the selector.
	res := &ColumnItem{ColumnName: colName, Selector: NameParts(n[i:])}

	if i-1 > 0 {
		// What's before must be a valid table name.  Use the existing
		// normalize code to enforce that, since we can reuse the
		// resulting TableName.
		t, err := n[:i-1].normalizeTableNameAsValue()
		if err != nil {
			return nil, err
		}
		res.TableName = t
	}

	return res, nil
}

// NormalizeUnqualifiedColumnItem normalizes a UnresolvedName for all
// the forms it can have inside a context that requires an unqualified
// column item (e.g. UPDATE LHS, INSERT, etc.).
func (n UnresolvedName) NormalizeUnqualifiedColumnItem() (*ColumnItem, error) {
	if len(n) == 0 {
		return nil, fmt.Errorf("invalid column name: %q", n)
	}

	colName, ok := n[0].(Name)
	if !ok {
		return nil, fmt.Errorf("invalid column name: %q", n)
	}

	if colName == "" {
		return nil, fmt.Errorf("empty column name: %q", n)
	}

	// Remainder is a selector.
	return &ColumnItem{ColumnName: colName, Selector: NameParts(n[1:])}, nil
}
