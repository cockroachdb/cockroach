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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// SchemaDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for schema descriptors.
type SchemaDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableSchema() *Immutable
	BuildExistingMutableSchema() *Mutable
	BuildCreatedMutableSchema() *Mutable
}

type schemaDescriptorBuilder struct {
	original *descpb.SchemaDescriptor
}

var _ SchemaDescriptorBuilder = &schemaDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// schema descriptors.
func NewBuilder(desc *descpb.SchemaDescriptor) SchemaDescriptorBuilder {
	return &schemaDescriptorBuilder{
		original: protoutil.Clone(desc).(*descpb.SchemaDescriptor),
	}
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Schema
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (sdb *schemaDescriptorBuilder) RunPostDeserializationChanges(
	_ context.Context, _ catalog.DescGetter,
) error {
	privDesc := sdb.original.GetPrivileges()
	descpb.MaybeFixSchemaPrivileges(&privDesc)
	return nil
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return sdb.BuildImmutableSchema()
}

// BuildImmutableSchema returns an immutable schema descriptor.
func (sdb *schemaDescriptorBuilder) BuildImmutableSchema() *Immutable {
	return &Immutable{SchemaDescriptor: *sdb.original}
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return sdb.BuildExistingMutableSchema()
}

// BuildExistingMutableSchema returns a mutable descriptor for a schema
// which already exists.
func (sdb *schemaDescriptorBuilder) BuildExistingMutableSchema() *Mutable {
	desc := protoutil.Clone(sdb.original).(*descpb.SchemaDescriptor)
	return &Mutable{
		Immutable:      Immutable{SchemaDescriptor: *desc},
		ClusterVersion: &Immutable{SchemaDescriptor: *sdb.original},
	}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return sdb.BuildCreatedMutableSchema()
}

// BuildCreatedMutableSchema returns a mutable descriptor for a schema
// which is in the process of being created.
func (sdb *schemaDescriptorBuilder) BuildCreatedMutableSchema() *Mutable {
	return &Mutable{Immutable: Immutable{SchemaDescriptor: *sdb.original}}
}
