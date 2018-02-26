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

package optbase

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// This file contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.

// ColumnName is the type of a column name.
type ColumnName string

// TableName is the type of a table name.
type TableName string

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

	// ColumnCount returns the number of columns in the index. This includes
	// columns that were part of the index definition (including the STORING
	// clause), as well as implicitly added primary key columns.
	ColumnCount() int

	// UniqueColumnCount returns the number of columns in the index that are
	// part of its unique key. Every index has a set of unique columns, even if
	// it was not originally declared unique, due to implicitly added primary
	// key columns. The unique columns are always a prefix of the full column
	// list, where UniqueColumnCount <= ColumnCount.
	UniqueColumnCount() int

	// Column returns the ith IndexColumn within the index definition, where
	// i < ColumnCount.
	Column(i int) IndexColumn
}

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
type Table interface {
	// TabName returns the name of the table.
	TabName() TableName

	// ColumnCount returns the number of columns in the table.
	ColumnCount() int

	// Column returns a Column interface to the column at the ith ordinal
	// position within the table, where i < ColumnCount.
	Column(i int) Column

	// Primary returns the unique index that specifies the primary ordering of
	// the rows in the table. It corresponds to the table's primary key, and is
	// always present. If a primary key was not explicitly specified, then the
	// system implicitly creates one based on a hidden rowid column.
	Primary() Index

	// SecondaryCount returns the number of secondary indexes defined on this
	// table.
	SecondaryCount() int

	// Secondary returns the ith secondary index, where i < SecondaryCount.
	Secondary(i int) Index
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
func FormatCatalogTable(tbl Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tbl.TabName())

	var buf bytes.Buffer
	for i := 0; i < tbl.ColumnCount(); i++ {
		buf.Reset()
		formatColumn(tbl.Column(i), &buf)
		child.Child(buf.String())
	}

	formatCatalogIndex(tbl.Primary(), child)

	for i := 0; i < tbl.SecondaryCount(); i++ {
		formatCatalogIndex(tbl.Secondary(i), child)
	}
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(idx Index, tp treeprinter.Node) {
	child := tp.Childf("INDEX %s", idx.IdxName())

	var buf bytes.Buffer
	for i := 0; i < idx.ColumnCount(); i++ {
		buf.Reset()

		idxCol := idx.Column(i)
		formatColumn(idxCol.Column, &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.UniqueColumnCount() {
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
