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
	ordinal                     int
	stableID                    StableID
	name                        tree.Name
	kind                        ColumnKind
	datumType                   *types.T
	nullable                    bool
	hidden                      bool
	defaultExpr                 string
	computedExpr                string
	invertedSourceColumnOrdinal int
}

// Ordinal returns the position of the column in its table. The following always
// holds:
//   tab.Column(i).Ordinal() == i
func (c *Column) Ordinal() int {
	return c.ordinal
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
	if c.kind.IsVirtual() {
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

// IsMutation returns true if this is a mutation column (based on its Kind).
func (c *Column) IsMutation() bool {
	return c.kind == WriteOnly || c.kind == DeleteOnly
}

// IsSelectable returns true if this column should be accessible from user
// queries (based on its Kind).
func (c *Column) IsSelectable() bool {
	return c.kind == Ordinary || c.kind == System
}

// DatumType returns the data type of the column.
func (c *Column) DatumType() *types.T {
	return c.datumType
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
	return c.defaultExpr != ""
}

// DefaultExprStr is set to the SQL expression string that describes the
// column's default value. It is used when the user does not provide a value for
// the column when inserting a row. Default values cannot depend on other
// columns.
func (c *Column) DefaultExprStr() string {
	return c.defaultExpr
}

// IsComputed returns true if the column is a computed value. ComputedExprStr
// will be set to the SQL expression string in that case.
func (c *Column) IsComputed() bool {
	return c.computedExpr != ""
}

// ComputedExprStr is set to the SQL expression string that describes the
// column's computed value. It is always used to provide the column's value when
// inserting or updating a row. Computed values cannot depend on other computed
// columns, but they can depend on all other columns, including columns with
// default values.
func (c *Column) ComputedExprStr() string {
	return c.computedExpr
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
	if c.kind != VirtualInverted {
		panic(errors.AssertionFailedf("non-virtual columns have no inverted source column ordinal"))
	}
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
	// VirtualInverted columns are implicit columns that are used by inverted
	// indexes.
	VirtualInverted
	// VirtualComputed columns are non-stored computed columns that are used by
	// expression-based indexes.
	VirtualComputed
)

// IsVirtual returns true if the column kind is VirtualInverted or
// VirtualComputed.
func (k ColumnKind) IsVirtual() bool {
	return k == VirtualInverted || k == VirtualComputed
}

// InitNonVirtual is used by catalog implementations to populate a non-virtual
// Column. It should not be used anywhere else.
func (c *Column) InitNonVirtual(
	ordinal int,
	stableID StableID,
	name tree.Name,
	kind ColumnKind,
	datumType *types.T,
	nullable bool,
	hidden bool,
	defaultExpr *string,
	computedExpr *string,
) {
	if kind.IsVirtual() {
		panic(errors.AssertionFailedf("incorrect init method"))
	}
	c.ordinal = ordinal
	c.stableID = stableID
	c.name = name
	c.kind = kind
	c.datumType = datumType
	c.nullable = nullable
	c.hidden = hidden
	if defaultExpr != nil {
		c.defaultExpr = *defaultExpr
	} else {
		c.defaultExpr = ""
	}
	if computedExpr != nil {
		c.computedExpr = *computedExpr
	} else {
		c.computedExpr = ""
	}
	c.invertedSourceColumnOrdinal = -1
}

// InitVirtualInverted is used by catalog implementations to populate a
// VirtualInverted Column. It should not be used anywhere else.
func (c *Column) InitVirtualInverted(
	ordinal int, name tree.Name, datumType *types.T, nullable bool, invertedSourceColumnOrdinal int,
) {
	c.ordinal = ordinal
	c.stableID = 0
	c.name = name
	c.kind = VirtualInverted
	c.datumType = datumType
	c.nullable = nullable
	c.hidden = true
	c.defaultExpr = ""
	c.computedExpr = ""
	c.invertedSourceColumnOrdinal = invertedSourceColumnOrdinal
}

// InitVirtualComputed is used by catalog implementations to populate a
// VirtualComputed Column. It should not be used anywhere else.
func (c *Column) InitVirtualComputed(
	ordinal int, name tree.Name, datumType *types.T, nullable bool, computedExpr string,
) {
	c.ordinal = ordinal
	c.stableID = 0
	c.name = name
	c.kind = VirtualComputed
	c.datumType = datumType
	c.nullable = nullable
	c.hidden = true
	c.defaultExpr = ""
	c.computedExpr = computedExpr
	c.invertedSourceColumnOrdinal = -1
}

// Quiet the linter until this is used.
var _ = (*Column).InitVirtualComputed
