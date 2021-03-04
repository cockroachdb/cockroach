// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package typedesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TypeDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for type descriptors.
type TypeDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableType() *Immutable
	BuildExistingMutableType() *Mutable
	BuildCreatedMutableType() *Mutable
}

type typeDescriptorBuilder struct {
	original *descpb.TypeDescriptor
}

var _ TypeDescriptorBuilder = &typeDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// type descriptors.
func NewBuilder(desc *descpb.TypeDescriptor) TypeDescriptorBuilder {
	return &typeDescriptorBuilder{
		original: protoutil.Clone(desc).(*descpb.TypeDescriptor),
	}
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (tdb *typeDescriptorBuilder) RunPostDeserializationChanges(
	_ context.Context, _ catalog.DescGetter,
) error {
	return nil
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return tdb.BuildImmutableType()
}

// BuildImmutableType returns an immutable type descriptor.
func (tdb *typeDescriptorBuilder) BuildImmutableType() *Immutable {
	imm := makeImmutable(tdb.original)
	return &imm
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return tdb.BuildExistingMutableType()
}

// BuildExistingMutableType returns a mutable descriptor for a type
// which already exists.
func (tdb *typeDescriptorBuilder) BuildExistingMutableType() *Mutable {
	clusterVersion := makeImmutable(protoutil.Clone(tdb.original).(*descpb.TypeDescriptor))
	return &Mutable{Immutable: makeImmutable(tdb.original), ClusterVersion: &clusterVersion}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return tdb.BuildCreatedMutableType()
}

// BuildCreatedMutableType returns a mutable descriptor for a type
// which is in the process of being created.
func (tdb *typeDescriptorBuilder) BuildCreatedMutableType() *Mutable {
	return &Mutable{Immutable: makeImmutable(tdb.original)}
}

func makeImmutable(desc *descpb.TypeDescriptor) Immutable {
	immutDesc := Immutable{TypeDescriptor: *desc}

	// Initialize metadata specific to the TypeDescriptor kind.
	switch immutDesc.Kind {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		immutDesc.logicalReps = make([]string, len(desc.EnumMembers))
		immutDesc.physicalReps = make([][]byte, len(desc.EnumMembers))
		immutDesc.readOnlyMembers = make([]bool, len(desc.EnumMembers))
		for i := range desc.EnumMembers {
			member := &desc.EnumMembers[i]
			immutDesc.logicalReps[i] = member.LogicalRepresentation
			immutDesc.physicalReps[i] = member.PhysicalRepresentation
			immutDesc.readOnlyMembers[i] =
				member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY
		}
	}

	return immutDesc
}
