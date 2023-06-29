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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
	mvccTimestamp        hlc.Timestamp
	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
	// This is the raw bytes (tag + data) of the schema descriptor in storage.
	rawBytesInStorage []byte
}

var _ SchemaDescriptorBuilder = &schemaDescriptorBuilder{}

// NewBuilder returns a new SchemaDescriptorBuilder instance by delegating to
// NewBuilderWithMVCCTimestamp with an empty MVCC timestamp.
//
// Callers must assume that the given protobuf has already been treated with the
// MVCC timestamp beforehand.
func NewBuilder(desc *descpb.SchemaDescriptor) SchemaDescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// NewBuilderWithMVCCTimestamp creates a new SchemaDescriptorBuilder instance
// for building table descriptors.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.SchemaDescriptor, mvccTimestamp hlc.Timestamp,
) SchemaDescriptorBuilder {
	return newBuilder(
		desc,
		mvccTimestamp,
		false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{},
	)
}

func newBuilder(
	desc *descpb.SchemaDescriptor,
	mvccTimestamp hlc.Timestamp,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) SchemaDescriptorBuilder {
	return &schemaDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.SchemaDescriptor),
		mvccTimestamp:        mvccTimestamp,
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
func (sdb *schemaDescriptorBuilder) RunPostDeserializationChanges() (err error) {
	defer func() {
		err = errors.Wrapf(err, "schema %q (%d)", sdb.original.Name, sdb.original.ID)
	}()
	// Set the ModificationTime field before doing anything else.
	// Other changes may depend on it.
	mustSetModTime, err := descpb.MustSetModificationTime(
		sdb.original.ModificationTime, sdb.mvccTimestamp, sdb.original.Version,
	)
	if err != nil {
		return err
	}
	sdb.maybeModified = protoutil.Clone(sdb.original).(*descpb.SchemaDescriptor)
	if mustSetModTime {
		sdb.maybeModified.ModificationTime = sdb.mvccTimestamp
		sdb.changes.Add(catalog.SetModTimeToMVCCTimestamp)
	}
	if privsChanged, err := catprivilege.MaybeFixPrivileges(
		&sdb.maybeModified.Privileges,
		sdb.maybeModified.GetParentID(),
		descpb.InvalidID,
		privilege.Schema,
		sdb.maybeModified.GetName(),
	); err != nil {
		return err
	} else if privsChanged {
		sdb.changes.Add(catalog.UpgradedPrivileges)
	}
	return nil
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) error {
	// Upgrade the declarative schema changer state.
	if scpb.MigrateDescriptorState(version, sdb.maybeModified.ParentID, sdb.maybeModified.DeclarativeSchemaChangerState) {
		sdb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}
	return nil
}

// StripDanglingBackReferences implements the catalog.DescriptorBuilder
// interface.
func (sdb *schemaDescriptorBuilder) StripDanglingBackReferences(
	descIDMightExist func(id descpb.ID) bool, nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
) error {
	// There's nothing we can do for schemas here.
	return nil
}

// SetRawBytesInStorage implements the catalog.DescriptorBuilder interface.
func (sdb *schemaDescriptorBuilder) SetRawBytesInStorage(rawBytes []byte) {
	sdb.rawBytesInStorage = append([]byte(nil), rawBytes...) // deep-copy
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
		rawBytesInStorage:    append([]byte(nil), sdb.rawBytesInStorage...), // deep-copy
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
			rawBytesInStorage:    append([]byte(nil), sdb.rawBytesInStorage...), // deep-copy
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
	desc := sdb.maybeModified
	if desc == nil {
		desc = sdb.original
	}
	return &Mutable{
		immutable: immutable{
			SchemaDescriptor:     *desc,
			isUncommittedVersion: sdb.isUncommittedVersion,
			changes:              sdb.changes,
			rawBytesInStorage:    append([]byte(nil), sdb.rawBytesInStorage...), // deep-copy
		},
	}
}
