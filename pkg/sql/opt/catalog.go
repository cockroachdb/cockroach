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

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// This file contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.

// PrimaryIndex selects the primary index of a table when calling the
// Table.Index method. Every table is guaranteed to have a unique primary
// index, even if it meant adding a hidden unique rowid column.
const PrimaryIndex = 0

// Fingerprint uniquely identifies a catalog data source. If the schema of the
// data source changes in any way, then the fingerprint must also change. This
// enables cached data sources (or other data structures dependent on the data
// sources) to be invalidated when their schema changes.
//
// For sqlbase data sources, the fingerprint is simply the concatenation of the
// 32-bit descriptor ID and version fields. The version is incremented each time
// a change to a table/view/sequence occurs, including changes to any associated
// indexes.
type Fingerprint uint64

// Catalog is an interface to a database catalog, exposing only the information
// needed by the query optimizer.
type Catalog interface {
	// ResolveDataSource locates a data source with the given name and returns it.
	// If no such data source exists, then ResolveDataSource returns an error. As
	// a side effect, the name parameter is updated to be fully qualified if it
	// was not before (i.e. to include catalog and schema names).
	ResolveDataSource(ctx context.Context, name *tree.TableName) (DataSource, error)

	// ResolveDataSourceByID is similar to ResolveDataSource, except that it
	// locates a data source by its unique identifier in the database. This id
	// is stable as long as the data source exists.
	ResolveDataSourceByID(ctx context.Context, dataSourceID int64) (DataSource, error)

	// CheckPrivilege verifies that the current user has the given privilege on
	// the given data source. If not, then CheckPrivilege returns an error.
	CheckPrivilege(ctx context.Context, ds DataSource, priv privilege.Kind) error
}

// DataSource is an interface to a database object that provides rows, like a
// table, a view, or a sequence.
type DataSource interface {
	// Fingerprint uniquely identifies this data source. If the schema of this
	// data source changes, then so will the value of this fingerprint. If two
	// data sources have the same fingerprint, then they are identical.
	Fingerprint() Fingerprint

	// Name returns the fully normalized, fully qualified, and fully resolved
	// name of the data source. The ExplicitCatalog and ExplicitSchema fields
	// will always be true, since all parts of the name are always specified.
	Name() *tree.TableName
}

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
type Table interface {
	DataSource

	// IsVirtualTable returns true if this table is a special system table that
	// constructs its rows "on the fly" when it's queried. An example is the
	// information_schema tables.
	IsVirtualTable() bool

	// InternalID returns the table's globally-unique ID.
	InternalID() uint64

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

	// LookupColumnOrdinal returns the ordinal of the column with the given ID.
	// Note that this takes the internal column ID, and has no relation to
	// ColumnIDs in the optimizer.
	LookupColumnOrdinal(colID uint32) (int, error)

	// IndexCount returns the number of indexes defined on this table. This
	// includes the primary index, so the count is always >= 1.
	IndexCount() int

	// Index returns the ith index, where i < IndexCount. The table's primary
	// index is always the 0th index, and is always present (use opt.PrimaryIndex
	// to select it). The primary index corresponds to the table's primary key.
	// If a primary key was not explicitly specified, then the system implicitly
	// creates one based on a hidden rowid column.
	Index(i int) Index

	// StatisticCount returns the number of statistics available for the table.
	StatisticCount() int

	// Statistic returns the ith statistic, where i < StatisticCount.
	Statistic(i int) TableStatistic
}

// View is an interface to a database view, exposing only the information needed
// by the query optimizer.
type View interface {
	DataSource

	// Query returns the SQL text that specifies the SELECT query that constitutes
	// this view.
	Query() string

	// ColumnNameCount returns the number of column names specified in the view.
	// If zero, then the columns are not aliased. Otherwise, it will match the
	// number of columns in the view.
	ColumnNameCount() int

	// ColumnNames returns the name of the column at the ith ordinal position
	// within the view, where i < ColumnNameCount.
	ColumnName(i int) tree.Name
}

// Column is an interface to a table column, exposing only the information
// needed by the query optimizer.
type Column interface {
	// ColName returns the name of the column.
	ColName() tree.Name

	// DatumType returns the data type of the column.
	DatumType() types.T

	// ColTypeStr returns the SQL data type of the column, as a string. Note that
	// this is sometimes different than DatumType().String(), since datum types
	// are a subset of column types.
	ColTypeStr() string

	// IsNullable returns true if the column is nullable.
	IsNullable() bool

	// IsHidden returns true if the column is hidden (e.g., there is always a
	// hidden column called rowid if there is no primary key on the table).
	IsHidden() bool

	// HasDefault returns true if the column has a default value. DefaultExprStr
	// will be set to the SQL expression string in that case.
	HasDefault() bool

	// DefaultExprStr is set to the SQL expression string that describes the
	// column's default value. It is used when the user does not provide a value
	// for the column when inserting a row. Default values cannot depend on other
	// columns.
	DefaultExprStr() string

	// IsComputed returns true if the column is a computed value. ComputedExprStr
	// will be set to the SQL expression string in that case.
	IsComputed() bool

	// ComputedExprStr is set to the SQL expression string that describes the
	// column's computed value. It is always used to provide the column's value
	// when inserting or updating a row. Computed values cannot depend on other
	// computed columns, but they can depend on all other columns, including
	// columns with default values.
	ComputedExprStr() string
}

// MutationColumn describes a single column that is being added to a table or
// dropped from a table. Mutation columns require special handling by mutation
// operators like Insert, Update, and Delete.
type MutationColumn struct {
	// Column is the Table.Column that is being added or dropped.
	Column

	// IsDeleteOnly is true if the column only needs special handling by Delete
	// operations; Update and Insert operations can ignore the column. See
	// sqlbase.DescriptorMutation_DELETE_ONLY for more information.
	IsDeleteOnly bool
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

	// InternalID returns the internal identifier of the index. Only used
	// when the query contains a numeric index reference.
	InternalID() uint64

	// Table returns a reference to the table this index is based on.
	Table() Table

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

	// ForeignKey returns a ForeignKeyReference if this index is part
	// of an outbound foreign key relation. Returns false for the second
	// return value if there is no foreign key reference on this index.
	ForeignKey() (ForeignKeyReference, bool)
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
	// Table contains the referenced table's internal ID.
	TableID uint64

	// Index contains the ID of the index that represents the
	// destination table's side of the foreign key relation.
	IndexID uint64

	// PrefixLen contains the length of columns that form the foreign key
	// relation in the current and destination indexes.
	PrefixLen int32

	// Match contains the method used for comparing composite foreign keys.
	Match tree.CompositeKeyMatchMethod
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
	ds, err := cat.ResolveDataSourceByID(context.TODO(), int64(fkRef.TableID))
	if err != nil {
		panic(err)
	}

	fkTable := ds.(Table)

	var fkIndex Index
	for j, cnt := 0, fkTable.IndexCount(); j < cnt; j++ {
		if fkTable.Index(j).InternalID() == fkRef.IndexID {
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

// FormatCatalogView nicely formats a catalog view using a treeprinter for
// debugging and testing.
func FormatCatalogView(view View, tp treeprinter.Node) {
	var buf bytes.Buffer
	if view.ColumnNameCount() > 0 {
		buf.WriteString(" (")
		for i := 0; i < view.ColumnNameCount(); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(string(view.ColumnName(i)))
		}
		buf.WriteString(")")
	}

	child := tp.Childf("VIEW %s%s", view.Name().TableName, buf.String())

	child.Child(view.Query())
}

// IsMutationColumn is a convenience function that returns true if the column at
// the given ordinal position is a mutation column.
func IsMutationColumn(table Table, i int) bool {
	_, ok := table.Column(i).(*MutationColumn)
	return ok
}
