// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idxtype

// CanBePrimary is true if this index type can be the primary index that always
// contains unique keys sorted according to the primary ordering of the table.
// Secondary indexes refer to rows in the primary index by unique key value.
func (t T) CanBePrimary() bool {
	return t == FORWARD
}

// CanBeUnique is true if this index type can be declared as UNIQUE, meaning it
// never contains duplicate keys.
func (t T) CanBeUnique() bool {
	return t == FORWARD
}

// AllowExplicitDirection is true if this index type allows all of its columns
// to specify an explicit ascending or descending direction. For example,
// inverted and vector indexes do not allow the last column in the index to
// specify an explicit direction.
func (t T) AllowExplicitDirection() bool {
	return t == FORWARD
}

// AllowsPrefixColumns is true if this index type allows other columns from the
// table to act as a key prefix that separates indexed values into distinct
// groupings, e.g. by user or customer.
func (t T) AllowsPrefixColumns() bool {
	return t == INVERTED || t == VECTOR
}

// SupportsSharding is true if this index can be hash sharded, meaning that its
// rows are grouped according to a hash value and spread across the keyspace.
func (t T) SupportsSharding() bool {
	return t == FORWARD
}

// SupportsStoring is true if this index allows STORING values, which are
// un-indexed columns from the table that are stored directly in the index for
// faster retrieval.
func (t T) SupportsStoring() bool {
	return t == FORWARD
}

// SupportsOpClass is true if this index allows columns to specify an operator
// class, which defines an alternate set of operators used when sorting and
// querying those columns.
// NOTE: Currently, only inverted indexes support operator classes, and only on
// the last column of the index.
func (t T) SupportsOpClass() bool {
	return t == INVERTED
}
