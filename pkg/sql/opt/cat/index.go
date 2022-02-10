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
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IndexOrdinal identifies an index (in the context of a Table).
type IndexOrdinal = int

// IndexOrdinals identifies a list of indexes (in the context of a Table).
type IndexOrdinals = []IndexOrdinal

// PrimaryIndex selects the primary index of a table when calling the
// Table.Index method. Every table is guaranteed to have a unique primary
// index, even if it meant adding a hidden unique rowid column.
const PrimaryIndex IndexOrdinal = 0

// Index is an interface to a database index, exposing only the information
// needed by the query optimizer. Every index is treated as unique by the
// optimizer. If an index was declared as non-unique, then the system will add
// implicit columns from the primary key in order to make it unique (and even
// add an implicit primary key based on a hidden rowid column if a primary key
// was not explicitly declared).
type Index interface {
	// ID is the stable identifier for this index that is guaranteed to be
	// unique within the owning table. See the comment for StableID for more
	// detail.
	ID() StableID

	// Name is the name of the index.
	Name() tree.Name

	// Table returns a reference to the table this index is based on.
	Table() Table

	// Ordinal returns the ordinal of this index within the context of its Table.
	// Specifically idx = Table().Index(idx.Ordinal).
	Ordinal() int

	// IsUnique returns true if this index is declared as UNIQUE in the schema.
	IsUnique() bool

	// IsInverted returns true if this is an inverted index.
	IsInverted() bool

	// ColumnCount returns the number of columns in the index. This includes
	// columns that were part of the index definition (including the STORING
	// clause), as well as implicitly added primary key columns. It also contains
	// implicit system columns, which are placed after all physical columns in
	// the table.
	ColumnCount() int

	// ExplicitColumnCount returns the number of key columns that are explicitly
	// specified in the index definition. This does not include stored columns.
	ExplicitColumnCount() int

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
	// unique (b, c) index, even though it appears there are duplicate rows:
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
	// to in its value). Note that the third case, the UNIQUE INDEX columns are
	// sufficient to form a strict key without needing to append the primary key
	// columns (which are stored in the value).
	//
	// For the last case of a UNIQUE INDEX with at least one NULL-able column,
	// only the lax key columns are guaranteed to be encoded in the row's key.
	// The strict key columns (the primary key columns in this case) are only
	// encoded in the row's key when at least one of the lax key columns has a
	// NULL value. Therefore, whether the row's key contains all the strict key
	// columns is data-dependent, not schema-dependent.
	LaxKeyColumnCount() int

	// NonInvertedPrefixColumnCount returns the number of non-inverted columns
	// in the inverted index. An inverted index only has non-inverted columns if
	// it is a multi-column inverted index. Therefore, a non-zero value is only
	// returned for multi-column inverted indexes. This function panics if the
	// index is not an inverted index.
	NonInvertedPrefixColumnCount() int

	// Column returns the ith IndexColumn within the index definition, where
	// i < ColumnCount.
	Column(i int) IndexColumn

	// InvertedColumn returns the inverted IndexColumn of the index. Panics if
	// the index is not an inverted index.
	InvertedColumn() IndexColumn

	// Predicate returns the partial index predicate expression and true if the
	// index is a partial index. If it is not a partial index, the empty string
	// and false are returned.
	Predicate() (string, bool)

	// Zone returns the zone which constrains placement of the index's range
	// replicas. If the index was not explicitly assigned to a zone, then it
	// inherits the zone of its owning table (which in turn inherits from its
	// owning database or the default zone). In addition, any unspecified zone
	// information will also be inherited.
	//
	// NOTE: This zone always applies to the entire index and never to any
	// particular partition of the index.
	Zone() Zone

	// Span returns the KV span associated with the index.
	Span() roachpb.Span

	// ImplicitColumnCount returns the number of implicit columns at the front of
	// the index including implicit partitioning columns and shard columns of hash
	// sharded indexes. For example, consider the following table:
	//
	// CREATE TABLE t (
	//   x INT,
	//   y INT,
	//   z INT,
	//   INDEX (y),
	//   INDEX (z) USING HASH
	// ) PARTITION ALL BY LIST (x) (
	//   PARTITION p1 VALUES IN (1)
	// );
	//
	// In this case, the number of implicit columns in the index on y is 1, since
	// x is implicitly added to the front of the index. The number of implicit
	// columns in the index on z is 2, since x and a shard column are implicitly
	// added to the front of the index.
	//
	// The implicit partitioning columns are always a prefix of the full column
	// list, and ImplicitColumnCount < LaxKeyColumnCount.
	ImplicitColumnCount() int

	// GeoConfig returns a geospatial index configuration. If non-nil, it
	// describes the configuration for this geospatial inverted index.
	GeoConfig() *geoindex.Config

	// Version returns the IndexDescriptorVersion of the index.
	Version() descpb.IndexDescriptorVersion

	// PartitionCount returns the number of PARTITION BY LIST partitions defined
	// on this index.
	PartitionCount() int

	// Partition returns the ith PARTITION BY LIST partition within the index
	// definition, where i < PartitionCount.
	Partition(i int) Partition
}

// IndexColumn describes a single column that is part of an index definition.
type IndexColumn struct {
	// Column is a reference to the column returned by Table.Column, given the
	// column ordinal.
	*Column

	// Descending is true if the index is ordered from greatest to least on
	// this column, rather than least to greatest.
	Descending bool
}

// IsMutationIndex is a convenience function that returns true if the index at
// the given ordinal position is a mutation index.
func IsMutationIndex(table Table, ord IndexOrdinal) bool {
	return ord >= table.IndexCount()
}

// Partition is an interface to a PARTITION BY LIST partition of an index. The
// intended use is to support planning of scans or lookup joins that will use
// locality optimized search. Locality optimized search can be planned when the
// maximum number of rows returned by a scan or lookup join is known, but the
// specific region in which the rows are located is unknown. In this case, the
// optimizer will plan a scan or lookup join in which local nodes (i.e., nodes
// in the gateway region) are searched for matching rows before remote nodes, in
// the hope that the execution engine can avoid visiting remote nodes.
type Partition interface {
	// Name is the name of this partition.
	Name() string

	// Zone returns the zone which constrains placement of this partition's
	// replicas. If this partition does not have an associated zone, the returned
	// zone is empty, but non-nil.
	Zone() Zone

	// PartitionByListPrefixes returns the values of this partition. Specifically,
	// it returns a list of tuples where each tuple contains values for a prefix
	// of index columns (indicating the region of the index covered by this
	// partition).
	//
	// Example:
	//
	// CREATE INDEX idx ON t(region,subregion,val) PARTITION BY LIST (region,subregion) (
	//     PARTITION westcoast VALUES IN (('us', 'seattle'), ('us', 'cali')),
	//     PARTITION us VALUES IN (('us', DEFAULT)),
	//     PARTITION eu VALUES IN (('eu', DEFAULT)),
	//     PARTITION default VALUES IN (DEFAULT)
	// );
	//
	// If this is the westcoast partition, PartitionByListPrefixes() returns
	//  ('us', 'seattle'),
	//  ('us', 'cali')
	//
	// If this is the us partition, PartitionByListPrefixes() cuts off the DEFAULT
	// value and just returns
	//  ('us')
	//
	// Finally, if this is the default partition, PartitionByListPrefixes()
	// returns an empty slice.
	//
	// In addition to supporting locality optimized search as described above,
	// this function can be used to support index skip scans. To support index
	// skip scans, we collect the PartitionByListPrefixes for all partitions in
	// the index. Each tuple corresponds to a region of the index that we can
	// constrain further. In the example above: if we have a val=1 filter, instead
	// of a full index scan we can skip most of the data under /us/cali and
	// /us/seattle by scanning spans:
	//   [                 - /us/cali      )
	//   [ /us/cali/1      - /us/cali/1    ]
	//   [ /us/cali\x00    - /us/seattle   )
	//   [ /us/seattle/1   - /us/seattle/1 ]
	//   [ /us/seattle\x00 -               ]
	//
	PartitionByListPrefixes() []tree.Datums
}
