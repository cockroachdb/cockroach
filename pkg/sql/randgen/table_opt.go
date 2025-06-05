// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// TableOption represents a single option for table generation.
type TableOption func(*tableOptions)

type tableOptions struct {
	primaryIndexRequired       bool
	skipColumnFamilyMutations  bool
	multiRegion                bool
	allowPartiallyVisibleIndex bool
	crazyNames                 bool
	columnFilter               func(*tree.ColumnTableDef) bool
	primaryIndexFilter         func(*tree.IndexTableDef, []*tree.ColumnTableDef) bool
	indexFilter                func(tree.TableDef, []*tree.ColumnTableDef) bool
}

// WithPrimaryIndexRequired requires that the table has a primary index.
func WithPrimaryIndexRequired() TableOption {
	return func(o *tableOptions) {
		o.primaryIndexRequired = true
	}
}

// WithSkipColumnFamilyMutations skips column family mutations.
func WithSkipColumnFamilyMutations() TableOption {
	return func(o *tableOptions) {
		o.skipColumnFamilyMutations = true
	}
}

// WithMultiRegion enables multi-region table generation.
func WithMultiRegion() TableOption {
	return func(o *tableOptions) {
		o.multiRegion = true
	}
}

// WithAllowPartiallyVisibleIndex allows partially visible indexes.
func WithAllowPartiallyVisibleIndex() TableOption {
	return func(o *tableOptions) {
		o.allowPartiallyVisibleIndex = true
	}
}

// WithCrazyNames enables generation of crazy table and column names.
func WithCrazyNames() TableOption {
	return func(o *tableOptions) {
		o.crazyNames = true
	}
}

// WithColumnFilter sets a filter function for columns. The filter function
// should return false if the generated column should be excluded.
func WithColumnFilter(filter func(column *tree.ColumnTableDef) bool) TableOption {
	return func(o *tableOptions) {
		o.columnFilter = filter
	}
}

// WithPrimaryIndexFilter sets a filter function for primary indexes. The filter
// function should return false if the generated primary index should be
// excluded.
func WithPrimaryIndexFilter(
	filter func(index *tree.IndexTableDef, columns []*tree.ColumnTableDef) bool,
) TableOption {
	return func(o *tableOptions) {
		o.primaryIndexFilter = filter
	}
}

// WithIndexFilter sets a filter function for secondary indexes. The filter
// function should return false if the generated index should be excluded.
func WithIndexFilter(
	filter func(index tree.TableDef, columns []*tree.ColumnTableDef) bool,
) TableOption {
	return func(o *tableOptions) {
		o.indexFilter = filter
	}
}

// applyOptions applies the given options to a tableOptions struct.
func applyOptions(opts []TableOption) tableOptions {
	var o tableOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
