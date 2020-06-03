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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// DescriptorInterface provides table information for results from a name
// lookup.
//
// TODO(ajwerner): Move this back to catalog after sqlbase has been
// appropriately cleaned up. Furthermore, reconsider whether this interface
// actually makes much sense. It may make more sense to instead type assert into
// the individual descriptor type interfaces which we'll be introducing.
type DescriptorInterface interface {
	BaseDescriptorInterface

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

// BaseDescriptorInterface is an interface to be shared by individual descriptor
// types. Perhaps this should be the actual DescriptorInterface.
type BaseDescriptorInterface interface {
	tree.NameResolutionResult

	GetPrivileges() *PrivilegeDescriptor
	GetID() ID
	TypeName() string
	GetName() string
	GetAuditMode() TableDescriptor_AuditMode

	// DescriptorProto prepares this descriptor for serialization.
	DescriptorProto() *Descriptor
}

// UnwrapDescriptor is a hack to construct a DescriptorInterface from a
// Descriptor.
//
// TODO(ajwerner): In practice, the function which retrieved the value should
// be unwrapping it.
func UnwrapDescriptor(desc *Descriptor) DescriptorInterface {
	if typDesc := desc.GetType(); typDesc != nil {
		return NewImmutableTypeDescriptor(*typDesc)
	}
	if tbDesc := desc.Table(hlc.Timestamp{}); tbDesc != nil {
		// TODO(ajwerner): Fix the constructor here to take desc.
		return NewImmutableTableDescriptor(*tbDesc)
	}
	if schemaDesc := desc.GetSchema(); schemaDesc != nil {
		return schemaDesc
	}
	if dbDesc := desc.GetDatabase(); dbDesc != nil {
		return NewImmutableDatabaseDescriptor(*dbDesc)
	}
	panic(errors.Errorf("unknown descriptor type %+v", desc))
}
