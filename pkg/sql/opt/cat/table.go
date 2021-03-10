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

	// IsMaterializedView returns true if this table is actually a materialized
	// view. Materialized views are the same as tables in all aspects, other than
	// that they cannot be mutated.
	IsMaterializedView() bool

	// ColumnCount returns the number of columns in the table. This includes
	// public columns, write-only columns, etc.
	ColumnCount() int

	// Column returns a Column interface to the column at the ith ordinal
	// position within the table, where i < ColumnCount. The Columns collections
	// is the union of all columns in all indexes. It may include mutation
	// columns. Mutation columns are in the process of being added or dropped
	// from the table, and may need to have default or computed values set when
	// inserting or updating rows. See this RFC for more details:
	//
	//   cockroachdb/cockroach/docs/RFCS/20151014_online_schema_change.md
	//
	Column(i int) *Column

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
	// delete-only indexes defined on this table. DeletableIndexCount is always
	// >= WritableIndexCount.
	DeletableIndexCount() int

	// Index returns the ith index, where i < DeletableIndexCount. The table's
	// primary index is always the 0th index, and is always present (use
	// cat.PrimaryIndex to select it). The primary index corresponds to the
	// table's primary key. If a primary key was not explicitly specified, then
	// the system implicitly creates one based on a hidden rowid column. For
	// virtual tables, the primary index contains a single, synthesized column.
	Index(i IndexOrdinal) Index

	// StatisticCount returns the number of statistics available for the table.
	StatisticCount() int

	// Statistic returns the ith statistic, where i < StatisticCount.
	// The statistics must be ordered from new to old, according to the
	// CreatedAt() times.
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

	// OutboundForeignKeyCount returns the number of outbound foreign key
	// references (where this is the origin table).
	OutboundForeignKeyCount() int

	// OutboundForeignKeyCount returns the ith outbound foreign key reference.
	OutboundForeignKey(i int) ForeignKeyConstraint

	// InboundForeignKeyCount returns the number of inbound foreign key references
	// (where this is the referenced table).
	InboundForeignKeyCount() int

	// InboundForeignKey returns the ith inbound foreign key reference.
	InboundForeignKey(i int) ForeignKeyConstraint

	// UniqueCount returns the number of unique constraints defined on this table.
	// Includes any unique constraints implied by unique indexes.
	UniqueCount() int

	// Unique returns the ith unique constraint defined on this table, where
	// i < UniqueCount.
	Unique(i UniqueOrdinal) UniqueConstraint
}

// CheckConstraint contains the SQL text and the validity status for a check
// constraint on a table. Check constraints are user-defined restrictions
// on the content of each row in a table. For example, this check constraint
// ensures that only values greater than zero can be inserted into the table:
//
//   CREATE TABLE a (a INT CHECK (a > 0))
//
type CheckConstraint struct {
	Constraint string
	Validated  bool
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
	// tuple with the values on each column.
	DistinctCount() uint64

	// NullCount returns the estimated number of rows which have a NULL value on
	// any column in the statistic.
	NullCount() uint64

	// Histogram returns a slice of histogram buckets, sorted by UpperBound.
	// It is only used for single-column stats (i.e., when ColumnCount() = 1),
	// and it represents the distribution of values for that column.
	// See HistogramBucket for more details.
	Histogram() []HistogramBucket
}

// HistogramBucket contains the data for a single histogram bucket. Note
// that NumEq, NumRange, and DistinctRange are floats so the statisticsBuilder
// can apply filters to the histogram.
type HistogramBucket struct {
	// NumEq is the estimated number of values equal to UpperBound.
	NumEq float64

	// NumRange is the estimated number of values between the upper bound of the
	// previous bucket and UpperBound (both boundaries are exclusive).
	// The first bucket should always have NumRange=0.
	NumRange float64

	// DistinctRange is the estimated number of distinct values between the upper
	// bound of the previous bucket and UpperBound (both boundaries are
	// exclusive).
	DistinctRange float64

	// UpperBound is the upper bound of the bucket.
	UpperBound tree.Datum
}

// ForeignKeyConstraint represents a foreign key constraint. A foreign key
// constraint has an origin (or referencing) side and a referenced side. For
// example:
//   ALTER TABLE o ADD CONSTRAINT fk FOREIGN KEY (a,b) REFERENCES r(a,b)
// Here o is the origin table, r is the referenced table, and we have two pairs
// of columns: (o.a,r.a) and (o.b,r.b).
type ForeignKeyConstraint interface {
	// Name of the foreign key constraint.
	Name() string

	// OriginTableID returns the referencing table's stable identifier.
	OriginTableID() StableID

	// ReferencedTableID returns the referenced table's stable identifier.
	ReferencedTableID() StableID

	// ColumnCount returns the number of column pairs in this FK reference.
	ColumnCount() int

	// OriginColumnOrdinal returns the ith column ordinal (see Table.Column) in
	// the origin (referencing) table. The ID() of originTable must equal
	// OriginTable().
	OriginColumnOrdinal(originTable Table, i int) int

	// ReferencedColumnOrdinal returns the ith column ordinal (see Table.Column)
	// in the referenced table. The ID() of referencedTable must equal
	// ReferencedTable().
	ReferencedColumnOrdinal(referencedTable Table, i int) int

	// Validated is true if the reference is validated (i.e. we know that the
	// existing data satisfies the constraint). It is possible to set up a foreign
	// key constraint on existing tables without validating it, in which case we
	// cannot make any assumptions about the data. An unvalidated constraint still
	// needs to be enforced on new mutations.
	Validated() bool

	// MatchMethod returns the method used for comparing composite foreign keys.
	MatchMethod() tree.CompositeKeyMatchMethod

	// DeleteReferenceAction returns the action to be performed if the foreign key
	// constraint would be violated by a delete.
	DeleteReferenceAction() tree.ReferenceAction

	// UpdateReferenceAction returns the action to be performed if the foreign key
	// constraint would be violated by an update.
	UpdateReferenceAction() tree.ReferenceAction
}

// UniqueConstraint represents a uniqueness constraint. UniqueConstraints may
// or may not be enforced with a unique index. For example, the following
// statement creates a unique constraint on column a without a unique index:
//   ALTER TABLE t ADD CONSTRAINT u UNIQUE WITHOUT INDEX (a);
// In order to enforce this uniqueness constraint, the optimizer must add
// a uniqueness check as a postquery to any query that inserts into or updates
// column a.
type UniqueConstraint interface {
	// Name of the unique constraint.
	Name() string

	// TableID returns the stable identifier of the table on which this unique
	// constraint is defined.
	TableID() StableID

	// ColumnCount returns the number of columns in this constraint.
	ColumnCount() int

	// ColumnOrdinal returns the table column ordinal of the ith column in this
	// constraint.
	ColumnOrdinal(tab Table, i int) int

	// Predicate returns the partial predicate expression and true if the
	// constraint is a partial unique constraint. If it is not, the empty string
	// and false are returned.
	Predicate() (string, bool)

	// WithoutIndex is true if this unique constraint is not enforced by an index.
	WithoutIndex() bool

	// Validated is true if the constraint is validated (i.e. we know that the
	// existing data satisfies the constraint). It is possible to set up a unique
	// constraint on existing tables without validating it, in which case we
	// cannot make any assumptions about the data. An unvalidated constraint still
	// needs to be enforced on new mutations.
	Validated() bool
}

// UniqueOrdinal identifies a unique constraint (in the context of a Table).
type UniqueOrdinal = int

// UniqueOrdinals identifies a list of unique constraints (in the context of
// a Table).
type UniqueOrdinals = []UniqueOrdinal
