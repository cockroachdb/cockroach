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

type ResolvedSchemaKind int

// TODO (rohany): Generate a string for this?
const (
	SchemaPublic ResolvedSchemaKind = iota
	SchemaVirtual
	SchemaUserDefined
)

// TODO (rohany): Should this live here?
// TODO (rohany): I want it to be clear at what level of resolution a user
//  can expect different kinds of schemas. It seems like logical and above
//  you can get anything, and below that you can get user defined or public.
type ResolvedSchema struct {
	ID   ID
	Kind ResolvedSchemaKind

	// can be nil
	Desc SchemaDescriptorInterface
}

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

func makeImmutableSchemaDescriptor(desc SchemaDescriptor) ImmutableSchemaDescriptor {
	return ImmutableSchemaDescriptor{SchemaDescriptor: desc}
}

// Reference these functions to defeat the linter.
var (
	_ = NewImmutableSchemaDescriptor
)

// NewMutableCreatedSchemaDescriptor returns a MutableSchemaDescriptor from the
// given SchemaDescriptor with the cluster version being the zero schema. This
// is for a schema that is created within the current transaction.
func NewMutableCreatedSchemaDescriptor(desc SchemaDescriptor) *MutableSchemaDescriptor {
	return &MutableSchemaDescriptor{
		ImmutableSchemaDescriptor: makeImmutableSchemaDescriptor(desc),
	}
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *ImmutableSchemaDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// TypeName implements the DescriptorProto interface.
func (desc *ImmutableSchemaDescriptor) TypeName() string {
	return "schema"
}

// DatabaseDesc implements the ObjectDescriptor interface.
func (desc *ImmutableSchemaDescriptor) DatabaseDesc() *DatabaseDescriptor {
	return nil
}

// SchemaDesc implements the ObjectDescriptor interface.
func (desc *ImmutableSchemaDescriptor) SchemaDesc() *SchemaDescriptor {
	return &desc.SchemaDescriptor
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *ImmutableSchemaDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TypeDesc implements the ObjectDescriptor interface.
func (desc *ImmutableSchemaDescriptor) TypeDesc() *TypeDescriptor {
	return nil
}

// DescriptorProto wraps a SchemaDescriptor in a Descriptor.
func (desc *ImmutableSchemaDescriptor) DescriptorProto() *Descriptor {
	return &Descriptor{
		Union: &Descriptor_Schema{
			Schema: &desc.SchemaDescriptor,
		},
	}
}

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *ImmutableSchemaDescriptor) NameResolutionResult() {}
