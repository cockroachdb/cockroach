// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Column stores information about table columns, exposing only the information
// needed by the query optimizer.
type Column struct {
	// The fields in this struct correspond to the getter methods below. Refer to
	// those for documentation.
	//
	// Warning! If any fields are added here, make sure both Init methods below
	// set all fields (even if they are the empty value).
	stableID                    StableID
	name                        tree.Name
	kind                        ColumnKind
	datumType                   *types.T
	colTypePrecision            int32
	colTypeWidth                int32
	nullable                    bool
	hidden                      bool
	defaultExpr                 *string
	computedExpr                *string
	invertedSourceColumnOrdinal int
}

// ColID is the unique, stable identifier for this column within its table.
// Each new column in the table will be assigned a new ID that is different than
// every column allocated before or after. This is true even if a column is
// dropped and then re-added with the same name; the new column will have a
// different ID. See the comment for StableID for more detail.
//
// Virtual columns don't have stable IDs; for these columns ColID() must not be
// called.
func (c *Column) ColID() StableID {
	if c.kind == Virtual {
		panic(errors.AssertionFailedf("virtual columns have no StableID"))
	}
	return c.stableID
}

// ColName returns the name of the column.
func (c *Column) ColName() tree.Name {
	return c.name
}

// Kind returns what kind of column this is.
func (c *Column) Kind() ColumnKind {
	return c.kind
}

// DatumType returns the data type of the column.
func (c *Column) DatumType() *types.T {
	return c.datumType
}

// ColTypePrecision returns the precision of the column's SQL data type. This is
// only defined for the Decimal data type and represents the max number of
// decimal digits in the decimal (including fractional digits). If precision is
// 0, then the decimal has no max precision.
func (c *Column) ColTypePrecision() int {
	return int(c.colTypePrecision)
}

// ColTypeWidth returns the width of the column's SQL data type. This has
// different meanings depending on the data type:
//
//   Decimal  : scale
//   Int      : # bits (16, 32, 64, etc)
//   Bit Array: # bits
//   String   : rune count
//
// TODO(andyk): Switch calling code to use DatumType.
func (c *Column) ColTypeWidth() int {
	return int(c.colTypeWidth)
}

// IsNullable returns true if the column is nullable.
func (c *Column) IsNullable() bool {
	return c.nullable
}

// IsHidden returns true if the column is hidden (e.g., there is always a hidden
// column called rowid if there is no primary key on the table).
func (c *Column) IsHidden() bool {
	return c.hidden
}

// HasDefault returns true if the column has a default value. DefaultExprStr
// will be set to the SQL expression string in that case.
func (c *Column) HasDefault() bool {
	return c.defaultExpr != nil
}

// DefaultExprStr is set to the SQL expression string that describes the
// column's default value. It is used when the user does not provide a value for
// the column when inserting a row. Default values cannot depend on other
// columns.
func (c *Column) DefaultExprStr() string {
	return *c.defaultExpr
}

// IsComputed returns true if the column is a computed value. ComputedExprStr
// will be set to the SQL expression string in that case.
func (c *Column) IsComputed() bool {
	return c.computedExpr != nil
}

// ComputedExprStr is set to the SQL expression string that describes the
// column's computed value. It is always used to provide the column's value when
// inserting or updating a row. Computed values cannot depend on other computed
// columns, but they can depend on all other columns, including columns with
// default values.
func (c *Column) ComputedExprStr() string {
	return *c.computedExpr
}

// InvertedSourceColumnOrdinal is used for virtual columns that are part
// of inverted indexes. It returns the ordinal of the table column from which
// the inverted column is derived.
//
// For example, if we have an inverted index on a JSON column `j`, the index is
// on a virtual `j_inverted` column and calling InvertedSourceColumnOrdinal() on
// `j_inverted` returns the ordinal of the `j` column.
//
// Must not be called if this is not a virtual column.
func (c *Column) InvertedSourceColumnOrdinal() int {
	return c.invertedSourceColumnOrdinal
}

// ColumnKind differentiates between different kinds of table columns.
type ColumnKind uint8

const (
	// Ordinary columns are "regular" table columns (including hidden columns
	// like `rowid`).
	Ordinary ColumnKind = iota
	// WriteOnly columns are mutation columns that have to be updated on writes
	// (inserts, updates, deletes) and cannot be otherwise accessed.
	WriteOnly
	// DeleteOnly columns are mutation columns that have to be updated only on
	// deletes and cannot be otherwise accessed.
	DeleteOnly
	// System columns are implicit columns that every physical table
	// contains. These columns can only be read from and must not be included
	// as part of mutations. They also cannot be part of the lax or key columns
	// for indexes. System columns are not members of any column family.
	System
	// Virtual columns are implicit columns that are used by inverted indexes (and
	// later, expression-based indexes).
	Virtual
)

// IsMutation is a convenience method that returns true if the column kind is
// a mutation column.
func (kind ColumnKind) IsMutation() bool {
	return kind == WriteOnly || kind == DeleteOnly
}

// IsSelectable is a convenience method that returns true if the column
// kind is a selectable column.
func (kind ColumnKind) IsSelectable() bool {
	return kind == Ordinary || kind == System
}

// IsMutationColumn is a convenience function that returns true if the column at
// the given ordinal position is a mutation column.
func IsMutationColumn(table Table, ord int) bool {
	return table.ColumnKind(ord).IsMutation()
}

// IsSystemColumn is a convenience function that returns true if the column at
// the given ordinal position is a system column.
func IsSystemColumn(table Table, ord int) bool {
	return table.ColumnKind(ord) == System
}

// IsSelectableColumn is a convenience function that returns true if the column
// at the given ordinal position is a selectable column.
func IsSelectableColumn(table Table, ord int) bool {
	return table.ColumnKind(ord).IsSelectable()
}

// IsVirtualColumn is a convenience function that returns true if the column at
// the given ordinal position is a virtual column.
func IsVirtualColumn(table Table, ord int) bool {
	return table.ColumnKind(ord) == Virtual
}

// InitNonVirtual is used by catalog implementations to populate a non-virtual
// Column. It should not be used anywhere else.
func (c *Column) InitNonVirtual(
	stableID StableID,
	name tree.Name,
	kind ColumnKind,
	datumType *types.T,
	nullable bool,
	hidden bool,
	defaultExpr *string,
	computedExpr *string,
) {
	if kind == Virtual {
		panic(errors.AssertionFailedf("incorrect init method"))
	}
	c.stableID = stableID
	c.name = name
	c.kind = kind
	c.initType(datumType)
	c.nullable = nullable
	c.hidden = hidden
	c.defaultExpr = defaultExpr
	c.computedExpr = computedExpr
	c.invertedSourceColumnOrdinal = -1
}

// InitVirtual is used by catalog implementations to populate a virtual Column.
// It should not be used anywhere else.
func (c *Column) InitVirtual(
	name tree.Name, datumType *types.T, nullable bool, invertedSourceColumnOrdinal int,
) {
	c.stableID = 0
	c.name = name
	c.kind = Virtual
	c.initType(datumType)
	c.nullable = nullable
	c.hidden = true
	c.defaultExpr = nil
	c.computedExpr = nil
	c.invertedSourceColumnOrdinal = invertedSourceColumnOrdinal
}

// initType initializes the datumType, colTypePrecision, and colTypeWidth
// fields.
func (c *Column) initType(t *types.T) {
	c.datumType = t

	if t.Family() == types.ArrayFamily {
		if t.ArrayContents().Family() == types.ArrayFamily {
			panic(errors.AssertionFailedf("column type should never be a nested array"))
		}
		t = t.ArrayContents()
	}
	c.colTypePrecision = t.Precision()
	c.colTypeWidth = t.Width()
}
