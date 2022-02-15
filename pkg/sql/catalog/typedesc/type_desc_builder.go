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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TypeDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for type descriptors.
type TypeDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableType() catalog.TypeDescriptor
	BuildExistingMutableType() *Mutable
	BuildCreatedMutableType() *Mutable
}

type typeDescriptorBuilder struct {
	original      *descpb.TypeDescriptor
	maybeModified *descpb.TypeDescriptor

	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
}

var _ TypeDescriptorBuilder = &typeDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// type descriptors.
func NewBuilder(desc *descpb.TypeDescriptor) TypeDescriptorBuilder {
	return newBuilder(desc, false, /* isUncommitedVersion */
		catalog.PostDeserializationChanges{})
}

func newBuilder(
	desc *descpb.TypeDescriptor,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) TypeDescriptorBuilder {
	b := &typeDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.TypeDescriptor),
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}
	return b
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (tdb *typeDescriptorBuilder) RunPostDeserializationChanges() {
	tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TypeDescriptor)
	fixedPrivileges := catprivilege.MaybeFixPrivileges(
		&tdb.maybeModified.Privileges,
		tdb.maybeModified.GetParentID(),
		tdb.maybeModified.GetParentSchemaID(),
		privilege.Type,
		tdb.maybeModified.GetName(),
	)
	addedGrantOptions := catprivilege.MaybeUpdateGrantOptions(tdb.maybeModified.Privileges)
	if fixedPrivileges || addedGrantOptions {
		tdb.changes.Add(catalog.UpgradedPrivileges)
	}
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) RunRestoreChanges(_ func(id descpb.ID) catalog.Descriptor) error {
	return nil
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return tdb.BuildImmutableType()
}

// BuildImmutableType returns an immutable type descriptor.
func (tdb *typeDescriptorBuilder) BuildImmutableType() catalog.TypeDescriptor {
	desc := tdb.maybeModified
	if desc == nil {
		desc = tdb.original
	}
	imm := makeImmutable(desc, tdb.isUncommittedVersion, tdb.changes)
	return &imm
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return tdb.BuildExistingMutableType()
}

// BuildExistingMutableType returns a mutable descriptor for a type
// which already exists.
func (tdb *typeDescriptorBuilder) BuildExistingMutableType() *Mutable {
	if tdb.maybeModified == nil {
		tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TypeDescriptor)
	}
	clusterVersion := makeImmutable(tdb.original, false, /* isUncommitedVersion */
		catalog.PostDeserializationChanges{})
	return &Mutable{
		immutable:      makeImmutable(tdb.maybeModified, false /* isUncommitedVersion */, tdb.changes),
		ClusterVersion: &clusterVersion,
	}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return tdb.BuildCreatedMutableType()
}

// BuildCreatedMutableType returns a mutable descriptor for a type
// which is in the process of being created.
func (tdb *typeDescriptorBuilder) BuildCreatedMutableType() *Mutable {
	return &Mutable{
		immutable: makeImmutable(tdb.original, tdb.isUncommittedVersion, tdb.changes),
	}
}

func makeImmutable(
	desc *descpb.TypeDescriptor,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) immutable {
	immutDesc := immutable{
		TypeDescriptor:       *desc,
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}

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
