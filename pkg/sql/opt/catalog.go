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

package opt

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// This file contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.

// ColumnName is the type of a column name.
type ColumnName string

// PrimaryIndex selects the primary index of a table when calling the
// Table.Index method. Every table is guaranteed to have a unique primary
// index, even if it meant adding a hidden unique rowid column.
const PrimaryIndex = 0

// Column is an interface to a table column, exposing only the information
// needed by the query optimizer.
type Column interface {
	// ColName returns the name of the column.
	ColName() ColumnName

	// DatumType returns the data type of the column.
	DatumType() types.T

	// IsNullable returns true if the column is nullable.
	IsNullable() bool

	// IsHidden returns true if the column is hidden (e.g., there is always a
	// hidden column called rowid if there is no primary key on the table).
	IsHidden() bool
}

// IndexColumn describes a single column that is part of an index definition.
type IndexColumn struct {
	// Column is a reference to the column returned by Table.Column, given the
	// column ordinal.
	Column Column

	// Ordinal is the ordinal position of the indexed column in the table being
	// indexed. It is always >= 0 and < Table.ColumnCount.
	Ordinal int

	// Descending is true if the index is ordered from greatest to least on
	// this column, rather than least to greatest.
	Descending bool
}

// Index is an interface to a database index, exposing only the information
// needed by the query optimizer. Every index is treated as unique by the
// optimizer. If an index was declared as non-unique, then the system will add
// implicit columns from the primary key in order to make it unique (and even
// add an implicit primary key based on a hidden rowid column if a primary key
// was not explicitly declared).
type Index interface {
	// IdxName is the name of the index.
	IdxName() string

	// IsInverted returns true if this is a JSON inverted index.
	IsInverted() bool

	// ColumnCount returns the number of columns in the index. This includes
	// columns that were part of the index definition (including the STORING
	// clause), as well as implicitly added primary key columns.
	ColumnCount() int

	// KeyColumnCount returns the number of columns in the index that are part
	// of its unique key. No two rows in the index will have the same values for
	// those columns (where NULL values are treated as equal). Every index has a
	// set of key columns, regardless of how it was defined, because Cockroach
	// will implicitly add primary key columns to any index which would not
	// otherwise have a key, like when:
	//
	//   1. Index was not declared as UNIQUE.
	//   2. Index was UNIQUE, but one or more columns have NULL values.
	//
	// The second case is subtle, because UNIQUE indexes treat NULL values as if
	// they are *not* equal to one another. For example, this is allowed with a
	// unique index, even though it appears there are duplicate rows:
	//
	//   b     c
	//   -------
	//   NULL  1
	//   NULL  1
	//
	// Since keys treat NULL values as if they *are* equal to each other,
	// Cockroach must append the primary key columns in order to ensure this
	// index has a key. If the primary key of this table was column (a), then
	// the key of this secondary index would be (b,c,a).
	//
	// The key columns are always a prefix of the full column list, where
	// KeyColumnCount <= ColumnCount.
	KeyColumnCount() int

	// LaxKeyColumnCount returns the number of columns in the index that are
	// part of its "lax" key. Lax keys follow the same rules as keys (sometimes
	// referred to as "strict" keys), except that NULL values are treated as
	// *not* equal to one another, as in the case for UNIQUE indexes. This means
	// that two rows can appear to have duplicate values when one of those values
	// is NULL. See the KeyColumnCount comment for more details and an example.
	//
	// The lax key columns are always a prefix of the key columns, where
	// LaxKeyColumnCount <= KeyColumnCount. However, it is not required that an
	// index have a separate lax key, in which case LaxKeyColumnCount equals
	// KeyColumnCount. Here are the cases:
	//
	//   PRIMARY KEY                : lax key cols = key cols
	//   INDEX (not unique)         : lax key cols = key cols
	//   UNIQUE INDEX, not-null cols: lax key cols = key cols
	//   UNIQUE INDEX, nullable cols: lax key cols < key cols
	//
	// In the first three cases, all strict key columns (and thus all lax key
	// columns as well) are guaranteed to be encoded in the row's key (as opposed
	// to in its value). However, for a UNIQUE INDEX with at least one NULL-able
	// column, only the lax key columns are guaranteed to be encoded in the row's
	// key. The strict key columns are only encoded in the row's key when at least
	// one of the lax key columns has a NULL value. Therefore, whether the row's
	// key contains all the strict key columns is data-dependent, not schema-
	// dependent.
	LaxKeyColumnCount() int

	// Column returns the ith IndexColumn within the index definition, where
	// i < ColumnCount.
	Column(i int) IndexColumn
}

// TableStatistic is an interface to a table statistic. Each statistic is
// associated with a set of columns.
type TableStatistic interface {
	// CreatedAt indicates when the statistic was generated.
	CreatedAt() time.Time

	// ColumnCount is the number of columns the statistic pertains to.
	ColumnCount() int

	// ColumnOrdinal returns the column ordinal (see Table.Column) of the ith
	// column in this statistic, with 0 <= i < ColumnCount.
	ColumnOrdinal(i int) int

	// RowCount returns the estimated number of rows in the table.
	RowCount() uint64

	// DistinctCount returns the estimated number of distinct values on the
	// columns of the statistic. If there are multiple columns, each "value" is a
	// tuple with the values on each column. Rows where any statistic column have
	// a NULL don't contribute to this count.
	DistinctCount() uint64

	// NullCount returns the estimated number of rows which have a NULL value on
	// any column in the statistic.
	NullCount() uint64

	// TODO(radu): add Histogram().
}

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
type Table interface {
	// TabName returns the fully normalized, fully qualified, and fully resolved
	// name of the table. The ExplicitCatalog and ExplicitSchema fields will
	// always be true, since both names are always specified.
	TabName() *tree.TableName

	// IsVirtualTable returns true if this table is a special system table that
	// constructs its rows "on the fly" when it's queried. An example is the
	// information_schema tables.
	IsVirtualTable() bool

	// ColumnCount returns the number of columns in the table.
	ColumnCount() int

	// Column returns a Column interface to the column at the ith ordinal
	// position within the table, where i < ColumnCount.
	Column(i int) Column

	// IndexCount returns the number of indexes defined on this table. This
	// includes the primary index, so the count is always >= 1.
	IndexCount() int

	// Index returns the ith index, where i < IndexCount. The table's primary
	// index is always the 0th index, and is always present (use the
	// opt.PrimaryIndex to select it). The primary index corresponds to the
	// table's primary key. If a primary key was not explicitly specified, then
	// the system implicitly creates one based on a hidden rowid column.
	Index(i int) Index

	// StatisticCount returns the number of statistics available for the table.
	StatisticCount() int

	// Statistic returns the ith statistic, where i < StatisticCount.
	Statistic(i int) TableStatistic
}

// Catalog is an interface to a database catalog, exposing only the information
// needed by the query optimizer.
type Catalog interface {
	// FindTable returns a Table interface for the database table matching the
	// given table name.  Returns an error if the table does not exist.
	FindTable(ctx context.Context, name *tree.TableName) (Table, error)
}

// FormatCatalogTable nicely formats a catalog table using a treeprinter for
// debugging and testing.
func FormatCatalogTable(tab Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tab.TabName().TableName)

	var buf bytes.Buffer
	for i := 0; i < tab.ColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), &buf)
		child.Child(buf.String())
	}

	for i := 0; i < tab.IndexCount(); i++ {
		formatCatalogIndex(tab.Index(i), i == PrimaryIndex, child)
	}
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(idx Index, isPrimary bool, tp treeprinter.Node) {
	inverted := ""
	if idx.IsInverted() {
		inverted = "INVERTED "
	}
	child := tp.Childf("%sINDEX %s", inverted, idx.IdxName())

	var buf bytes.Buffer
	colCount := idx.ColumnCount()
	if isPrimary {
		// Omit the "stored" columns from the primary index.
		colCount = idx.KeyColumnCount()
	}

	for i := 0; i < colCount; i++ {
		buf.Reset()

		idxCol := idx.Column(i)
		formatColumn(idxCol.Column, &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		child.Child(buf.String())
	}
}

func formatColumn(col Column, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s %s", col.ColName(), col.DatumType())
	if !col.IsNullable() {
		fmt.Fprintf(buf, " not null")
	}
	if col.IsHidden() {
		fmt.Fprintf(buf, " (hidden)")
	}
}
