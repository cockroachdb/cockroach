// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// DescriptorInterface provides table information for results from a name
// lookup.
//
// TODO(ajwerner): Move this back to catalog after sqlbase has been
// appropriately cleaned up.
type DescriptorInterface interface {
	tree.NameResolutionResult

	// DatabaseDesc returns the underlying database descriptor, or nil if the
	// descriptor is not a table backed object.
	DatabaseDesc() *DatabaseDescriptor

	// SchemaDesc returns the underlying schema descriptor, or nil if the
	// descriptor is not a table backed object.
	SchemaDesc() *SchemaDescriptor

	// TableDesc returns the underlying table descriptor, or nil if the
	// descriptor is not a table backed object.
	TableDesc() *TableDescriptor

	// TypeDesc returns the underlying type descriptor, or nil if the
	// descriptor is not a type backed object.
	TypeDesc() *TypeDescriptor
}
