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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IndexOrdinal identifies an index (in the context of a Table).
type IndexOrdinal = int

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

	// Column returns the ith IndexColumn within the index definition, where
	// i < ColumnCount.
	Column(i int) IndexColumn

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

	// PartitionByListPrefixes returns values that correspond to PARTITION BY LIST
	// values. Specifically, it returns a list of tuples where each tuple contains
	// values for a prefix of index columns (indicating a region of the index).
	// Each tuple corresponds to a configured partition or subpartition.
	//
	// Note: this function decodes and allocates datums; use sparingly.
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
	// PartitionByListPrefixes() returns
	//  ('us', 'seattle'),
	//  ('us', 'cali'),
	//  ('us'),
	//  ('eu').
	//
	// The intended use of this function is for index skip scans. Each tuple
	// corresponds to a region of the index that we can constrain further. In the
	// example above: if we have a val=1 filter, instead of a full index scan we
	// can skip most of the data under /us/cali and /us/seattle by scanning spans:
	//   [                 - /us/cali      )
	//   [ /us/cali/1      - /us/cali/1    ]
	//   [ /us/cali\x00    - /us/seattle   )
	//   [ /us/seattle/1   - /us/seattle/1 ]
	//   [ /us/seattle\x00 -               ]
	//
	PartitionByListPrefixes() []tree.Datums
}

// IndexColumn describes a single column that is part of an index definition.
type IndexColumn struct {
	// Column is a reference to the column returned by Table.Column, given the
	// column ordinal.
	Column

	// Ordinal is the ordinal position of the indexed column in the table being
	// indexed. It is always >= 0 and < Table.ColumnCount.
	Ordinal int

	// Descending is true if the index is ordered from greatest to least on
	// this column, rather than least to greatest.
	Descending bool
}

// IsMutationIndex is a convenience function that returns true if the index at
// the given ordinal position is a mutation index.
func IsMutationIndex(table Table, ord IndexOrdinal) bool {
	return ord >= table.IndexCount()
}
