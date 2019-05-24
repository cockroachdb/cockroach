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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
//
// Both columns and indexes are grouped into three sets: public, write-only, and
// delete-only. When a column or index is added or dropped, it proceeds through
// each of the three states as that schema change is incrementally rolled out to
// the cluster without blocking ongoing queries. In the public state, reads,
// writes, and deletes are allowed. In the write-only state, only writes and
// deletes are allowed. Finally, in the delete-only state, only deletes are
// allowed. Further details about "online schema change" can be found in:
//
//   docs/RFCS/20151014_online_schema_change.md
//
// Calling code must take care to use the right collection of columns or
// indexes. Usually this should be the public collections, since most usages are
// read-only, but mutation operators generally need to consider non-public
// columns and indexes.
type Table interface {
	DataSource

	// IsVirtualTable returns true if this table is a special system table that
	// constructs its rows "on the fly" when it's queried. An example is the
	// information_schema tables.
	IsVirtualTable() bool

	// IsInterleaved returns true if any of this table's indexes are interleaved
	// with index(es) from other table(s).
	IsInterleaved() bool

	// IsReferenced returns true if this table is referenced by at least one
	// foreign key defined on another table (or this one if self-referential).
	IsReferenced() bool

	// ColumnCount returns the number of public columns in the table. Public
	// columns are not currently being added or dropped from the table. This
	// method should be used when mutation columns can be ignored (the common
	// case).
	ColumnCount() int

	// WritableColumnCount returns the number of public and write-only columns in
	// the table. Although write-only columns are not visible, any inserts and
	// updates must still set them. WritableColumnCount is always >= ColumnCount.
	WritableColumnCount() int

	// DeletableColumnCount returns the number of public, write-only, and
	// delete- only columns in the table. DeletableColumnCount is always >=
	// WritableColumnCount.
	DeletableColumnCount() int

	// Column returns a Column interface to the column at the ith ordinal
	// position within the table, where i < ColumnCount. Note that the Columns
	// collection includes mutation columns, if present. Mutation columns are in
	// the process of being added or dropped from the table, and may need to have
	// default or computed values set when inserting or updating rows. See this
	// RFC for more details:
	//
	//   cockroachdb/cockroach/docs/RFCS/20151014_online_schema_change.md
	//
	// Writable columns are always situated after public columns, and are followed
	// by deletable columns.
	Column(i int) Column

	// IndexCount returns the number of public indexes defined on this table.
	// Public indexes are not currently being added or dropped from the table.
	// This method should be used when mutation columns can be ignored (the common
	// case). The returned indexes include the primary index, so the count is
	// always >= 1.
	IndexCount() int

	// WritableIndexCount returns the number of public and write-only indexes
	// defined on this table. Although write-only indexes are not visible, any
	// table mutation operations must still be applied to them. WritableIndexCount
	// is always >= IndexCount.
	WritableIndexCount() int

	// DeletableIndexCount returns the number of public, write-only, and
	// delete-onlyindexes defined on this table. DeletableIndexCount is always
	// >= WritableIndexCount.
	DeletableIndexCount() int

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

	// CheckCount returns the number of check constraints present on the table.
	CheckCount() int

	// Check returns the ith check constraint, where i < CheckCount.
	Check(i int) CheckConstraint

	// FamilyCount returns the number of column families present on the table.
	// There is always at least one primary family (always family 0) where columns
	// go if they are not explicitly assigned to another family. The primary
	// family is the first family that was explicitly specified by the user, or
	// is a synthesized family if no families were explicitly specified.
	FamilyCount() int

	// Family returns the interface for the ith column family, where
	// i < FamilyCount.
	Family(i int) Family
}

// CheckConstraint is the SQL text for a check constraint on a table. Check
// constraints are user-defined restrictions on the content of each row in a
// table. For example, this check constraint ensures that only values greater
// than zero can be inserted into the table:
//
//   CREATE TABLE a (a INT CHECK (a > 0))
//
type CheckConstraint string

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

	// Validated is true if the reference is validated (i.e. we know that the
	// existing data satisfies the constraint). It is possible to set up a foreign
	// key constraint on existing tables without validating it, in which case we
	// cannot make any assumptions about the data.
	Validated bool

	// Match contains the method used for comparing composite foreign keys.
	Match tree.CompositeKeyMatchMethod
}
