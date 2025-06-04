// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

// TableOption represents a single option for table generation.
type TableOption func(*tableOptions)

type tableOptions struct {
	primaryIndexRequired       bool
	skipColumnFamilyMutations  bool
	multiRegion                bool
	allowPartiallyVisibleIndex bool
	crazyNames                 bool
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

// applyOptions applies the given options to a tableOptions struct.
func applyOptions(opts []TableOption) tableOptions {
	var o tableOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
