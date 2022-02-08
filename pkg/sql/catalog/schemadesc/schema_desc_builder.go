// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemadesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// SchemaDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for schema descriptors.
type SchemaDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableSchema() catalog.SchemaDescriptor
	BuildExistingMutableSchema() *Mutable
	BuildCreatedMutableSchema() *Mutable
}

type schemaDescriptorBuilder struct {
	original             *descpb.SchemaDescriptor
	maybeModified        *descpb.SchemaDescriptor
	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
}

var _ SchemaDescriptorBuilder = &schemaDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// schema descriptors.
func NewBuilder(desc *descpb.SchemaDescriptor) SchemaDescriptorBuilder {
	return newBuilder(desc, false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{})
}

func newBuilder(
	desc *descpb.SchemaDescriptor,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) SchemaDescriptorBuilder {
	return &schemaDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.SchemaDescriptor),
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Schema
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (sdb *schemaDescriptorBuilder) RunPostDeserializationChanges() {
	sdb.maybeModified = protoutil.Clone(sdb.original).(*descpb.SchemaDescriptor)
	privsChanged := catprivilege.MaybeFixPrivileges(
		&sdb.maybeModified.Privileges,
		sdb.maybeModified.GetParentID(),
		descpb.InvalidID,
		privilege.Schema,
		sdb.maybeModified.GetName(),
	)
	addedGrantOptions := catprivilege.MaybeUpdateGrantOptions(sdb.maybeModified.Privileges)
	if privsChanged || addedGrantOptions {
		sdb.changes.Add(catalog.UpgradedPrivileges)
	}
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) RunRestoreChanges(
	_ func(id descpb.ID) catalog.Descriptor,
) error {
	return nil
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return sdb.BuildImmutableSchema()
}

// BuildImmutableSchema returns an immutable schema descriptor.
func (sdb *schemaDescriptorBuilder) BuildImmutableSchema() catalog.SchemaDescriptor {
	desc := sdb.maybeModified
	if desc == nil {
		desc = sdb.original
	}
	return &immutable{
		SchemaDescriptor:     *desc,
		changes:              sdb.changes,
		isUncommittedVersion: sdb.isUncommittedVersion,
	}
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return sdb.BuildExistingMutableSchema()
}

// BuildExistingMutableSchema returns a mutable descriptor for a schema
// which already exists.
func (sdb *schemaDescriptorBuilder) BuildExistingMutableSchema() *Mutable {
	if sdb.maybeModified == nil {
		sdb.maybeModified = protoutil.Clone(sdb.original).(*descpb.SchemaDescriptor)
	}
	return &Mutable{
		immutable: immutable{
			SchemaDescriptor:     *sdb.maybeModified,
			changes:              sdb.changes,
			isUncommittedVersion: sdb.isUncommittedVersion,
		},
		ClusterVersion: &immutable{SchemaDescriptor: *sdb.original},
	}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return sdb.BuildCreatedMutableSchema()
}

// BuildCreatedMutableSchema returns a mutable descriptor for a schema
// which is in the process of being created.
func (sdb *schemaDescriptorBuilder) BuildCreatedMutableSchema() *Mutable {
	return &Mutable{
		immutable: immutable{
			SchemaDescriptor: *sdb.original,
			changes:          sdb.changes,
		},
	}
}
