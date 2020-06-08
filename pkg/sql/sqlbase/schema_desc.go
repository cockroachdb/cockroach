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

// SchemaDescriptorInterface will eventually be called dbdesc.Descriptor.
// It is implemented by ImmutableSchemaDescriptor.
type SchemaDescriptorInterface interface {
	BaseDescriptorInterface
	SchemaDesc() *SchemaDescriptor
}

var _ SchemaDescriptorInterface = (*ImmutableSchemaDescriptor)(nil)
var _ SchemaDescriptorInterface = (*MutableSchemaDescriptor)(nil)

// ImmutableSchemaDescriptor wraps a Schema descriptor and provides methods
// on it.
type ImmutableSchemaDescriptor struct {
	SchemaDescriptor
}

// MutableSchemaDescriptor is a mutable reference to a SchemaDescriptor.
//
// Note: Today this isn't actually ever mutated but rather exists for a future
// where we anticipate having a mutable copy of Schema descriptors. There's a
// large amount of space to question this `Mutable|Immutable` version of each
// descriptor type. Maybe it makes no sense but we're running with it for the
// moment. This is an intermediate state on the road to descriptors being
// handled outside of the catalog entirely as interfaces.
type MutableSchemaDescriptor struct {
	ImmutableSchemaDescriptor

	ClusterVersion *ImmutableSchemaDescriptor
}

// NewImmutableSchemaDescriptor makes a new Schema descriptor.
func NewImmutableSchemaDescriptor(desc SchemaDescriptor) *ImmutableSchemaDescriptor {
	return &ImmutableSchemaDescriptor{
		SchemaDescriptor: desc,
	}
}

// Reference these functions to defeat the linter.
var (
	_ = NewImmutableSchemaDescriptor
	_ = NewInitialSchemaDescriptor
)

// NewInitialSchemaDescriptor constructs a new SchemaDescriptor for an
// initial version from an id and name.
func NewInitialSchemaDescriptor(id ID, name string) *ImmutableSchemaDescriptor {
	return &ImmutableSchemaDescriptor{
		SchemaDescriptor: SchemaDescriptor{
			ID:         id,
			Name:       name,
			Version:    1,
			Privileges: NewDefaultPrivilegeDescriptor(),
		},
	}
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *SchemaDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// TypeName implements the DescriptorProto interface.
func (desc *SchemaDescriptor) TypeName() string {
	return "schema"
}

// DatabaseDesc implements the ObjectDescriptor interface.
func (desc *SchemaDescriptor) DatabaseDesc() *DatabaseDescriptor {
	return nil
}

// SchemaDesc implements the ObjectDescriptor interface.
func (desc *SchemaDescriptor) SchemaDesc() *SchemaDescriptor {
	return desc
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *SchemaDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TypeDesc implements the ObjectDescriptor interface.
func (desc *SchemaDescriptor) TypeDesc() *TypeDescriptor {
	return nil
}

// DescriptorProto wraps a SchemaDescriptor in a Descriptor.
//
// TODO(ajwerner): Lift this into the SchemaDescriptorInterface
// implementations.
func (desc *SchemaDescriptor) DescriptorProto() *Descriptor {
	return wrapDescriptor(desc)
}

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *SchemaDescriptor) NameResolutionResult() {}
