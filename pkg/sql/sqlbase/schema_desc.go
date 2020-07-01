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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

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

// NewMutableExistingSchemaDescriptor returns a MutableSchemaDescriptor from the
// given schema descriptor with the cluster version also set to the descriptor.
// This is for schemas that already exist.
func NewMutableExistingSchemaDescriptor(desc SchemaDescriptor) *MutableSchemaDescriptor {
	return &MutableSchemaDescriptor{
		ImmutableSchemaDescriptor: makeImmutableSchemaDescriptor(*protoutil.Clone(&desc).(*SchemaDescriptor)),
		ClusterVersion:            NewImmutableSchemaDescriptor(desc),
	}
}

// NewImmutableSchemaDescriptor makes a new Schema descriptor.
func NewImmutableSchemaDescriptor(desc SchemaDescriptor) *ImmutableSchemaDescriptor {
	m := makeImmutableSchemaDescriptor(desc)
	return &m
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

// GetParentSchemaID implements the BaseDescriptorInterface interface.
func (desc *ImmutableSchemaDescriptor) GetParentSchemaID() ID {
	return keys.RootNamespaceID
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

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableSchemaDescriptor) Immutable() DescriptorInterface {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableSchemaDescriptor(*protoutil.Clone(desc.SchemaDesc()).(*SchemaDescriptor))
}
