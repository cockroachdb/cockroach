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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// FunctionDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for function descriptors.
type FunctionDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableFunction() catalog.FunctionDescriptor
	BuildExistingMutableFunction() *Mutable
	BuildCreatedMutableFunction() *Mutable
}

var _ FunctionDescriptorBuilder = &functionDescriptorBuilder{}

// NewBuilder returns a new FunctionDescriptorBuilder instance by delegating to
// NewBuilderWithMVCCTimestamp with an empty MVCC timestamp.
//
// Callers must assume that the given protobuf has already been treated with the
// MVCC timestamp beforehand.
func NewBuilder(desc *descpb.FunctionDescriptor) FunctionDescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// NewBuilderWithMVCCTimestamp creates a new FunctionDescriptorBuilder instance
// for building function descriptors.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.FunctionDescriptor, mvccTimestamp hlc.Timestamp,
) FunctionDescriptorBuilder {
	return newBuilder(
		desc,
		mvccTimestamp,
		false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{},
	)
}

func newBuilder(
	desc *descpb.FunctionDescriptor,
	mvccTimestamp hlc.Timestamp,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) FunctionDescriptorBuilder {
	return &functionDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.FunctionDescriptor),
		mvccTimestamp:        mvccTimestamp,
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}
}

type functionDescriptorBuilder struct {
	original             *descpb.FunctionDescriptor
	maybeModified        *descpb.FunctionDescriptor
	mvccTimestamp        hlc.Timestamp
	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
	// This is the raw bytes (tag + data) of the function descriptor in storage.
	rawBytesInStorage []byte
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Function
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (fdb *functionDescriptorBuilder) RunPostDeserializationChanges() (err error) {
	defer func() {
		err = errors.Wrapf(err, "function %q (%d)", fdb.original.Name, fdb.original.ID)
	}()
	// Set the ModificationTime field before doing anything else.
	// Other changes may depend on it.
	mustSetModTime, err := descpb.MustSetModificationTime(
		fdb.original.ModificationTime, fdb.mvccTimestamp, fdb.original.Version,
	)
	if err != nil {
		return err
	}
	fdb.maybeModified = protoutil.Clone(fdb.original).(*descpb.FunctionDescriptor)
	if mustSetModTime {
		fdb.maybeModified.ModificationTime = fdb.mvccTimestamp
		fdb.changes.Add(catalog.SetModTimeToMVCCTimestamp)
	}
	return nil
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) error {
	// Upgrade the declarative schema changer state.
	if scpb.MigrateDescriptorState(version, fdb.maybeModified.DeclarativeSchemaChangerState) {
		fdb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}
	return nil
}

// SetRawBytesInStorage implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) SetRawBytesInStorage(rawBytes []byte) {
	fdb.rawBytesInStorage = append([]byte(nil), rawBytes...) // deep-copy
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return fdb.BuildImmutableFunction()
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return fdb.BuildExistingMutableFunction()
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return fdb.BuildCreatedMutableFunction()
}

// BuildImmutableFunction implements the FunctionDescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) BuildImmutableFunction() catalog.FunctionDescriptor {
	desc := fdb.maybeModified
	if desc == nil {
		desc = fdb.original
	}
	return &immutable{
		FunctionDescriptor:   *desc,
		isUncommittedVersion: fdb.isUncommittedVersion,
		changes:              fdb.changes,
		rawBytesInStorage:    append([]byte(nil), fdb.rawBytesInStorage...), // deep-copy
	}
}

// BuildExistingMutableFunction implements the FunctionDescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) BuildExistingMutableFunction() *Mutable {
	if fdb.maybeModified == nil {
		fdb.maybeModified = protoutil.Clone(fdb.original).(*descpb.FunctionDescriptor)
	}
	return &Mutable{
		immutable: immutable{
			FunctionDescriptor:   *fdb.maybeModified,
			isUncommittedVersion: fdb.isUncommittedVersion,
			changes:              fdb.changes,
			rawBytesInStorage:    append([]byte(nil), fdb.rawBytesInStorage...), // deep-copy
		},
		clusterVersion: &immutable{FunctionDescriptor: *fdb.original},
	}
}

// BuildCreatedMutableFunction implements the FunctionDescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) BuildCreatedMutableFunction() *Mutable {
	desc := fdb.maybeModified
	if desc == nil {
		desc = fdb.original
	}
	return &Mutable{
		immutable: immutable{
			FunctionDescriptor:   *desc,
			isUncommittedVersion: fdb.isUncommittedVersion,
			changes:              fdb.changes,
			rawBytesInStorage:    append([]byte(nil), fdb.rawBytesInStorage...), // deep-copy
		},
	}
}
