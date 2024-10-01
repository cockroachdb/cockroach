// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	ordinal                           int
	stableID                          StableID
	name                              tree.Name
	datumType                         *types.T
	kind                              ColumnKind
	nullable                          bool
	visibility                        ColumnVisibility
	virtualComputed                   bool
	defaultExpr                       string
	computedExpr                      string
	onUpdateExpr                      string
	invertedSourceColumnOrdinal       int
	generatedAsIdentityType           GeneratedAsIdentityType
	generatedAsIdentitySequenceOption string
}

// Ordinal returns the position of the column in its table. The following always
// holds:
//
//	tab.Column(i).Ordinal() == i
func (c *Column) Ordinal() int {
	return c.ordinal
}

// ColID is the unique, stable identifier for this column within its table.
// Each new column in the table will be assigned a new ID that is different than
// every column allocated before or after. This is true even if a column is
// dropped and then re-added with the same name; the new column will have a
// different ID. See the comment for StableID for more detail.
//
// Inverted columns don't have stable IDs; for these columns ColID() must not be
// called.
func (c *Column) ColID() StableID {
	if c.kind == Inverted {
		panic(errors.AssertionFailedf("inverted columns have no StableID"))
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

// DatumType returns the data type of the column.
func (c *Column) DatumType() *types.T {
	return c.datumType
}

// IsNullable returns true if the column is nullable.
func (c *Column) IsNullable() bool {
	return c.nullable
}

// Visibility returns the column visibility.
func (c *Column) Visibility() ColumnVisibility {
	return c.visibility
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

// HasOnUpdate returns true if the column has an ON UPDATE expression.
func (c *Column) HasOnUpdate() bool {
	return c.onUpdateExpr != ""
}

// UseOnUpdate returns true if there is an ON UPDATE expression which should be
// updated as part of the session. This is to allow gating ON UPDATE
// rehome_row() from triggering when on_update_rehome_row_enabled = false.
func (c *Column) UseOnUpdate(sd *sessiondata.SessionData) bool {
	return c.onUpdateExpr != "" &&
		(sd.OnUpdateRehomeRowEnabled || !strings.HasPrefix(c.onUpdateExpr, "rehome_row()"))
}

// OnUpdateExprStr is set to the SQL expression string that describes the
// column's ON UPDATE expression. It is used when the user does not provide a
// value for the column when updating a row. ON UPDATE expressions cannot depend
// on other columns.
func (c *Column) OnUpdateExprStr() string {
	return c.onUpdateExpr
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

// IsVirtualComputed returns true if this is a virtual computed column.
func (c *Column) IsVirtualComputed() bool {
	return c.virtualComputed
}

// InvertedSourceColumnOrdinal is used for inverted columns that are part of
// inverted indexes. It returns the ordinal of the table column from which the
// inverted column is derived.
//
// For example, if we have an inverted index on a JSON column `j`, then the
// index contains keys of an implicit inverted `j_inverted` column. Calling
// InvertedSourceColumnOrdinal() on `j_inverted` returns the ordinal of the `j`
// column.
//
// Must not be called if this is not an inverted column.
func (c *Column) InvertedSourceColumnOrdinal() int {
	if c.kind != Inverted {
		panic(errors.AssertionFailedf("non-inverted columns have no inverted source column ordinal"))
	}
	return c.invertedSourceColumnOrdinal
}

// ColumnKind differentiates between different kinds of table columns.
type ColumnKind uint8

const (
	// Ordinary columns are "regular" table columns (including hidden columns
	// like `rowid` and virtual computed columns).
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
	// Inverted columns are implicit columns that represent the keys of inverted
	// indexes.
	Inverted
)

// ColumnVisibility controls if a column is visible for queries and if it is
// part of the star expansion.
type ColumnVisibility uint8

const (
	// Visible columns are visible to queries and are part of the star expansion
	// (e.g. SELECT * FROM t).
	Visible ColumnVisibility = iota

	// Hidden columns are visible to queries by name, but are not part of the star
	// expansion (e.g. implicit PK column "rowid").
	Hidden

	// Inaccessible columns are not visible to queries in any way.
	Inaccessible
)

// GeneratedAsIdentityType reflects how the creation of this column
// is associated with the GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY
// syntax.
// The GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY generates an IDENTITY
// column, which is based on an underlying sequence, and is auto-incremented
// in a new row. For an Identity column, it is not allowed to use SET DEFAULT
// syntax under ALTER TABLE ... ALTER COLUMN syntax.
// The options {ALWAYS | BY DEFAULT} determine the access to this column
// in INSERT and UPDATE statement.
type GeneratedAsIdentityType uint8

const (
	// NotGeneratedAsIdentity column is created without
	// the GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY syntax.
	NotGeneratedAsIdentity GeneratedAsIdentityType = iota

	// GeneratedAlwaysAsIdentity column is created with
	// the GENERATED ALWAYS AS IDENTITY syntax.
	// It cannot be overwritten without OVERRIDING SYSTEM VALUE
	// clause in the INSERT/UPSERT statement.
	// TODO(janexing): implement support for OVERRIDING SYSTEM VALUE
	// for INSERT/UPSERT/UPDATE statement.
	GeneratedAlwaysAsIdentity

	// GeneratedByDefaultAsIdentity column is created with
	// the GENERATED BY DEFAULT AS IDENTITY syntax, which
	// can be overwritten.
	GeneratedByDefaultAsIdentity
)

// HasGeneratedAsIdentitySequenceOption returns true if there is a sequence
// option for a `GENERATED AS IDENTITY` column.
func (c *Column) HasGeneratedAsIdentitySequenceOption() bool {
	return c.generatedAsIdentitySequenceOption != ""
}

// GeneratedAsIdentitySequenceOption is set to the SQL expression string that
// specify the sequence option for a `GENERATED AS IDENTITY` column.
// A `GENERATED AS IDENTITY` column is an auto-incremented column based on an
// underlying sequence, for which users can customize the options.
func (c *Column) GeneratedAsIdentitySequenceOption() string {
	return strings.TrimSpace(c.generatedAsIdentitySequenceOption)
}

// MaybeHidden is a helper constructor for either Visible or Hidden, depending
// on a flag.
func MaybeHidden(hidden bool) ColumnVisibility {
	if hidden {
		return Hidden
	}
	return Visible
}

// GeneratedAsIdentityType returns how
// the creation of this column is associated with
// the GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY syntax.
func (c *Column) GeneratedAsIdentityType() GeneratedAsIdentityType {
	return c.generatedAsIdentityType
}

// Init is used by catalog implementations to populate a non-inverted and
// non-virtual Column. It should not be used anywhere else.
func (c *Column) Init(
	ordinal int,
	stableID StableID,
	name tree.Name,
	kind ColumnKind,
	datumType *types.T,
	nullable bool,
	visibility ColumnVisibility,
	defaultExpr *string,
	computedExpr *string,
	onUpdateExpr *string,
	generatedAsIdentityType GeneratedAsIdentityType,
	generatedAsIdentitySequenceOption *string,
) {
	if kind == Inverted {
		panic(errors.AssertionFailedf("incorrect init method"))
	}
	if (kind == WriteOnly || kind == DeleteOnly) && visibility != Inaccessible {
		panic(errors.AssertionFailedf("mutation columns should always be inaccessible"))
	}
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = Column{
		ordinal:                     ordinal,
		stableID:                    stableID,
		name:                        name,
		kind:                        kind,
		datumType:                   datumType,
		nullable:                    nullable,
		visibility:                  visibility,
		invertedSourceColumnOrdinal: -1,
		generatedAsIdentityType:     generatedAsIdentityType,
	}
	if defaultExpr != nil {
		c.defaultExpr = *defaultExpr
	}
	if computedExpr != nil {
		if generatedAsIdentityType != NotGeneratedAsIdentity {
			panic(errors.AssertionFailedf("both generated identity and computed expression specified for column %q", name))
		}
		c.computedExpr = *computedExpr
	}
	if onUpdateExpr != nil {
		c.onUpdateExpr = *onUpdateExpr
	}
	if generatedAsIdentityType != NotGeneratedAsIdentity {
		if generatedAsIdentitySequenceOption != nil {
			c.generatedAsIdentitySequenceOption = *generatedAsIdentitySequenceOption
		}
	}
}

// InitInverted is used by catalog implementations to populate a
// Inverted Column. It should not be used anywhere else.
func (c *Column) InitInverted(
	ordinal int, name tree.Name, datumType *types.T, nullable bool, invertedSourceColumnOrdinal int,
) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = Column{
		ordinal:                     ordinal,
		stableID:                    0,
		name:                        name,
		kind:                        Inverted,
		datumType:                   datumType,
		nullable:                    nullable,
		visibility:                  Inaccessible,
		invertedSourceColumnOrdinal: invertedSourceColumnOrdinal,
	}
}

// InitVirtualComputed is used by catalog implementations to populate a
// virtual computed Column. It should not be used anywhere else.
func (c *Column) InitVirtualComputed(
	ordinal int,
	stableID StableID,
	name tree.Name,
	kind ColumnKind,
	datumType *types.T,
	nullable bool,
	visibility ColumnVisibility,
	computedExpr string,
) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = Column{
		ordinal:                     ordinal,
		stableID:                    stableID,
		name:                        name,
		kind:                        kind,
		datumType:                   datumType,
		nullable:                    nullable,
		visibility:                  visibility,
		computedExpr:                computedExpr,
		virtualComputed:             true,
		invertedSourceColumnOrdinal: -1,
	}
}

// IsGeneratedAlwaysAsIdentity returns true
// if the column is created with the GENERATED ALWAYS AS IDENTITY syntax
// and hence is not allowed for explicit write
// (write without any additional tokens).
func (c *Column) IsGeneratedAlwaysAsIdentity() bool {
	return c.generatedAsIdentityType == GeneratedAlwaysAsIdentity
}
