// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type FunctionDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableFunction() catalog.FunctionDescriptor
	BuildExistingMutableFunction() *Mutable
	BuildCreatedMutableFunction() *Mutable
}

var _ FunctionDescriptorBuilder = &functionDescriptorBuilder{}

func NewBuilder(desc *descpb.FunctionDescriptor) FunctionDescriptorBuilder {
	return newBuilder(desc, false /* isUncommittedVersion */, catalog.PostDeserializationChanges{})
}

func newBuilder(
	desc *descpb.FunctionDescriptor,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) FunctionDescriptorBuilder {
	return &functionDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.FunctionDescriptor),
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}
}

type functionDescriptorBuilder struct {
	original      *descpb.FunctionDescriptor
	maybeModified *descpb.FunctionDescriptor

	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
}

func (f functionDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Function
}

func (f functionDescriptorBuilder) RunPostDeserializationChanges() error {
	return nil
}

func (f functionDescriptorBuilder) RunRestoreChanges(
	descLookupFn func(id descpb.ID) catalog.Descriptor,
) error {
	return nil
}

func (f functionDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return f.BuildImmutableFunction()
}

func (f functionDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return f.BuildExistingMutableFunction()
}

func (f functionDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return f.BuildCreatedMutableFunction()
}

func (f functionDescriptorBuilder) BuildImmutableFunction() catalog.FunctionDescriptor {
	desc := f.maybeModified
	if desc == nil {
		desc = f.original
	}
	return &immutable{
		FunctionDescriptor:   *desc,
		isUncommittedVersion: f.isUncommittedVersion,
		changes:              f.changes,
	}
}

func (f functionDescriptorBuilder) BuildExistingMutableFunction() *Mutable {
	if f.maybeModified == nil {
		f.maybeModified = protoutil.Clone(f.original).(*descpb.FunctionDescriptor)
	}
	return &Mutable{
		immutable: immutable{
			FunctionDescriptor:   *f.maybeModified,
			isUncommittedVersion: f.isUncommittedVersion,
			changes:              f.changes,
		},
		ClusterVersion: &immutable{FunctionDescriptor: *f.original},
	}
}

func (f functionDescriptorBuilder) BuildCreatedMutableFunction() *Mutable {
	desc := f.maybeModified
	if desc == nil {
		desc = f.original
	}
	return &Mutable{
		immutable: immutable{
			FunctionDescriptor:   *desc,
			isUncommittedVersion: f.isUncommittedVersion,
			changes:              f.changes,
		},
	}
}
