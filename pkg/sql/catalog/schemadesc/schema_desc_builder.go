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
	originalSchema
}

var _ SchemaDescriptorBuilder = &schemaDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// schema descriptors.
func NewBuilder(desc *descpb.SchemaDescriptor) SchemaDescriptorBuilder {
	return &schemaDescriptorBuilder{originalSchema: newOriginalSchema(desc)}
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
	return nil
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return sdb.BuildImmutableSchema()
}

// BuildImmutableSchema returns an immutable schema descriptor.
func (sdb *schemaDescriptorBuilder) BuildImmutableSchema() *Immutable {
	imm := &Immutable{}
	sdb.deepCopyInto(&imm.SchemaDescriptor)
	return imm
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return sdb.BuildExistingMutableSchema()
}

// BuildExistingMutableSchema returns a mutable descriptor for a schema
// which already exists.
func (sdb *schemaDescriptorBuilder) BuildExistingMutableSchema() *Mutable {
	clusterVersion := &Immutable{}
	sdb.deepCopyInto(&clusterVersion.SchemaDescriptor)
	m := &Mutable{ClusterVersion: clusterVersion}
	sdb.deepCopyInto(&m.SchemaDescriptor)
	return m
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return sdb.BuildCreatedMutableSchema()
}

// BuildCreatedMutableSchema returns a mutable descriptor for a schema
// which is in the process of being created.
func (sdb *schemaDescriptorBuilder) BuildCreatedMutableSchema() *Mutable {
	m := &Mutable{}
	sdb.deepCopyInto(&m.SchemaDescriptor)
	return m
}

type originalSchema struct {
	desc *descpb.SchemaDescriptor
	pb   []byte
}

func (o *originalSchema) deepCopyInto(dst *descpb.SchemaDescriptor) {
	if o.pb != nil {
		err := protoutil.Unmarshal(o.pb, dst)
		if err != nil {
			o.pb = nil
		}
	}
	if o.pb == nil {
		*dst = *protoutil.Clone(o.desc).(*descpb.SchemaDescriptor)
	}
}

func newOriginalSchema(desc *descpb.SchemaDescriptor) originalSchema {
	pb, err := protoutil.Marshal(desc)
	if err != nil {
		pb = nil
	}
	return originalSchema{desc: desc, pb: pb}
}
