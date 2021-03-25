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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	originalType
}

var _ TypeDescriptorBuilder = &typeDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// type descriptors.
func NewBuilder(desc *descpb.TypeDescriptor) TypeDescriptorBuilder {
	return &typeDescriptorBuilder{originalType: newOriginalType(desc)}
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
	desc := descpb.TypeDescriptor{}
	tdb.deepCopyInto(&desc)
	imm := makeImmutable(&desc)
	return &imm
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return tdb.BuildExistingMutableType()
}

// BuildExistingMutableType returns a mutable descriptor for a type
// which already exists.
func (tdb *typeDescriptorBuilder) BuildExistingMutableType() *Mutable {
	desc := descpb.TypeDescriptor{}
	tdb.deepCopyInto(&desc)
	clusterVersion := makeImmutable(&desc)
	m := &Mutable{ClusterVersion: &clusterVersion}
	tdb.deepCopyInto(&m.TypeDescriptor)
	return m
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return tdb.BuildCreatedMutableType()
}

// BuildCreatedMutableType returns a mutable descriptor for a type
// which is in the process of being created.
func (tdb *typeDescriptorBuilder) BuildCreatedMutableType() *Mutable {
	desc := descpb.TypeDescriptor{}
	tdb.deepCopyInto(&desc)
	return &Mutable{Immutable: makeImmutable(&desc)}
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

type originalType struct {
	desc *descpb.TypeDescriptor
	pb   []byte
}

func (o *originalType) deepCopyInto(dst *descpb.TypeDescriptor) {
	if o.pb != nil {
		err := protoutil.Unmarshal(o.pb, dst)
		if err != nil {
			o.pb = nil
		}
	}
	if o.pb == nil {
		*dst = *protoutil.Clone(o.desc).(*descpb.TypeDescriptor)
	}
	// Deep-copy type metadata that's not part of the proto message.
	deepCopyTypeMeta(dst.Alias, o.desc.Alias)
}

func deepCopyTypeMeta(dst, src *types.T) {
	if src == nil {
		return
	}
	dst.TypeMeta = src.TypeMeta
	deepCopyTypeMeta(dst.InternalType.ArrayContents, src.InternalType.ArrayContents)
}

func newOriginalType(desc *descpb.TypeDescriptor) originalType {
	pb, err := protoutil.Marshal(desc)
	if err != nil {
		pb = nil
	}
	return originalType{desc: desc, pb: pb}
}
