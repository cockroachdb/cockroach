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

package cat

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
type Table interface {
	DataSource

	// IsVirtualTable returns true if this table is a special system table that
	// constructs its rows "on the fly" when it's queried. An example is the
	// information_schema tables.
	IsVirtualTable() bool

	// ColumnCount returns the number of columns in the table.
	ColumnCount() int

	// Column returns a Column interface to the column at the ith ordinal
	// position within the table, where i < ColumnCount. Note that the Columns
	// collection includes mutation columns, if present. Mutation columns are in
	// the process of being added or dropped from the table, and may need to have
	// default or computed values set when inserting or updating rows. See this
	// RFC for more details:
	//
	//   cockroachdb/cockroach/docs/RFCS/20151014_online_schema_change.md
	//
	// To determine if the column is a mutation column, try to cast it to
	// *MutationColumn.
	Column(i int) Column

	// IndexCount returns the number of indexes defined on this table. This
	// includes the primary index, so the count is always >= 1.
	IndexCount() int

	// Index returns the ith index, where i < IndexCount. The table's primary
	// index is always the 0th index, and is always present (use cat.PrimaryIndex
	// to select it). The primary index corresponds to the table's primary key.
	// If a primary key was not explicitly specified, then the system implicitly
	// creates one based on a hidden rowid column.
	Index(i int) Index

	// StatisticCount returns the number of statistics available for the table.
	StatisticCount() int

	// Statistic returns the ith statistic, where i < StatisticCount.
	Statistic(i int) TableStatistic
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

// ForeignKeyReference is a struct representing an outbound foreign key reference.
// It has accessors for table and index IDs, as well as the prefix length.
type ForeignKeyReference struct {
	// Table contains the referenced table's stable identifier.
	TableID StableID

	// Index contains the stable identifier of the index that represents the
	// destination table's side of the foreign key relation.
	IndexID StableID

	// PrefixLen contains the length of columns that form the foreign key
	// relation in the current and destination indexes.
	PrefixLen int32

	// Match contains the method used for comparing composite foreign keys.
	Match tree.CompositeKeyMatchMethod
}

// FindTableColumnByName returns the ordinal of the column having the given
// name, if one exists in the given table. Otherwise, it returns -1.
func FindTableColumnByName(tab Table, name tree.Name) int {
	for ord, n := 0, tab.ColumnCount(); ord < n; ord++ {
		if tab.Column(ord).ColName() == name {
			return ord
		}
	}
	return -1
}

// FormatCatalogTable nicely formats a catalog table using a treeprinter for
// debugging and testing.
func FormatCatalogTable(cat Catalog, tab Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tab.Name().TableName)

	var buf bytes.Buffer
	for i := 0; i < tab.ColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), &buf)
		child.Child(buf.String())
	}

	for i := 0; i < tab.IndexCount(); i++ {
		formatCatalogIndex(tab.Index(i), i == PrimaryIndex, child)
	}

	for i := 0; i < tab.IndexCount(); i++ {
		fkRef, ok := tab.Index(i).ForeignKey()

		if ok {
			formatCatalogFKRef(cat, tab, tab.Index(i), fkRef, child)
		}
	}
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(idx Index, isPrimary bool, tp treeprinter.Node) {
	inverted := ""
	if idx.IsInverted() {
		inverted = "INVERTED "
	}
	child := tp.Childf("%sINDEX %s", inverted, idx.Name())

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

// formatColPrefix returns a string representation of the first prefixLen columns of idx.
func formatColPrefix(idx Index, prefixLen int) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i := 0; i < prefixLen; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		colName := idx.Column(i).Column.ColName()
		buf.WriteString(colName.String())
	}
	buf.WriteByte(')')

	return buf.String()
}

// formatCatalogFKRef nicely formats a catalog foreign key reference using a
// treeprinter for debugging and testing.
func formatCatalogFKRef(
	cat Catalog, tab Table, idx Index, fkRef ForeignKeyReference, tp treeprinter.Node,
) {
	ds, err := cat.ResolveDataSourceByID(context.TODO(), fkRef.TableID)
	if err != nil {
		panic(err)
	}

	fkTable := ds.(Table)

	var fkIndex Index
	for j, cnt := 0, fkTable.IndexCount(); j < cnt; j++ {
		if fkTable.Index(j).ID() == fkRef.IndexID {
			fkIndex = fkTable.Index(j)
			break
		}
	}

	tp.Childf(
		"FOREIGN KEY %s REFERENCES %v %s",
		formatColPrefix(idx, int(fkRef.PrefixLen)),
		ds.Name(),
		formatColPrefix(fkIndex, int(fkRef.PrefixLen)),
	)
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
