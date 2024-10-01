// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package typedesc

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
	original             *descpb.TypeDescriptor
	maybeModified        *descpb.TypeDescriptor
	mvccTimestamp        hlc.Timestamp
	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
	// This is the raw bytes (tag + data) of the type descriptor in storage.
	rawBytesInStorage []byte
}

var _ TypeDescriptorBuilder = &typeDescriptorBuilder{}

// NewBuilder returns a new TypeDescriptorBuilder instance by delegating to
// NewBuilderWithMVCCTimestamp with an empty MVCC timestamp.
//
// Callers must assume that the given protobuf has already been treated with the
// MVCC timestamp beforehand.
func NewBuilder(desc *descpb.TypeDescriptor) TypeDescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// NewBuilderWithMVCCTimestamp creates a new TypeDescriptorBuilder instance
// for building table descriptors.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.TypeDescriptor, mvccTimestamp hlc.Timestamp,
) TypeDescriptorBuilder {
	return newBuilder(
		desc,
		mvccTimestamp,
		false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{},
	)
}

func newBuilder(
	desc *descpb.TypeDescriptor,
	mvccTimestamp hlc.Timestamp,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) TypeDescriptorBuilder {
	b := &typeDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.TypeDescriptor),
		mvccTimestamp:        mvccTimestamp,
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
func (tdb *typeDescriptorBuilder) RunPostDeserializationChanges() (err error) {
	defer func() {
		err = errors.Wrapf(err, "type %q (%d)", tdb.original.Name, tdb.original.ID)
	}()
	// Set the ModificationTime field before doing anything else.
	// Other changes may depend on it.
	mustSetModTime, err := descpb.MustSetModificationTime(
		tdb.original.ModificationTime, tdb.mvccTimestamp, tdb.original.Version, tdb.original.State,
	)
	if err != nil {
		return err
	}
	tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TypeDescriptor)
	if mustSetModTime {
		tdb.maybeModified.ModificationTime = tdb.mvccTimestamp
		tdb.changes.Add(catalog.SetModTimeToMVCCTimestamp)
	}
	if fixedPrivileges, err := catprivilege.MaybeFixPrivileges(
		&tdb.maybeModified.Privileges,
		tdb.maybeModified.GetParentID(),
		tdb.maybeModified.GetParentSchemaID(),
		privilege.Type,
		tdb.maybeModified.GetName(),
	); err != nil {
		return err
	} else if fixedPrivileges {
		tdb.changes.Add(catalog.UpgradedPrivileges)
	}
	return nil
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) error {
	// Upgrade the declarative schema changer state
	if scpb.MigrateDescriptorState(version, tdb.maybeModified.ParentID, tdb.maybeModified.DeclarativeSchemaChangerState) {
		tdb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}
	return nil
}

// StripDanglingBackReferences implements the catalog.DescriptorBuilder
// interface.
func (tdb *typeDescriptorBuilder) StripDanglingBackReferences(
	descIDMightExist func(id descpb.ID) bool, nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
) error {
	sliceIdx := 0
	for _, id := range tdb.maybeModified.ReferencingDescriptorIDs {
		tdb.maybeModified.ReferencingDescriptorIDs[sliceIdx] = id
		if descIDMightExist(id) {
			sliceIdx++
		}
	}
	if sliceIdx < len(tdb.maybeModified.ReferencingDescriptorIDs) {
		tdb.maybeModified.ReferencingDescriptorIDs = tdb.maybeModified.ReferencingDescriptorIDs[:sliceIdx]
		tdb.changes.Add(catalog.StrippedDanglingBackReferences)
	}
	return nil
}

// StripNonExistentRoles implements the catalog.DescriptorBuilder
// interface.
func (tdb *typeDescriptorBuilder) StripNonExistentRoles(
	roleExists func(role username.SQLUsername) bool,
) error {
	// If the owner doesn't exist, change the owner to admin.
	if !roleExists(tdb.maybeModified.GetPrivileges().Owner()) {
		tdb.maybeModified.Privileges.OwnerProto = username.AdminRoleName().EncodeProto()
		tdb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	// Remove any non-existent roles from the privileges.
	newPrivs := make([]catpb.UserPrivileges, 0, len(tdb.maybeModified.Privileges.Users))
	for _, priv := range tdb.maybeModified.Privileges.Users {
		exists := roleExists(priv.UserProto.Decode())
		if exists {
			newPrivs = append(newPrivs, priv)
		}
	}
	if len(newPrivs) != len(tdb.maybeModified.Privileges.Users) {
		tdb.maybeModified.Privileges.Users = newPrivs
		tdb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	return nil
}

// SetRawBytesInStorage implements the catalog.DescriptorBuilder interface.
func (tdb *typeDescriptorBuilder) SetRawBytesInStorage(rawBytes []byte) {
	tdb.rawBytesInStorage = append([]byte(nil), rawBytes...) // deep-copy
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
	imm.rawBytesInStorage = append([]byte(nil), tdb.rawBytesInStorage...) // deep-copy
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
	mutableType := makeImmutable(tdb.maybeModified,
		false /* isUncommitedVersion */, tdb.changes)
	mutableType.rawBytesInStorage = append([]byte(nil), tdb.rawBytesInStorage...) // deep-copy
	clusterVersion := makeImmutable(tdb.original,
		false /* isUncommitedVersion */, catalog.PostDeserializationChanges{})
	return &Mutable{
		immutable:      mutableType,
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
	desc := tdb.maybeModified
	if desc == nil {
		desc = tdb.original
	}
	createdType := makeImmutable(desc, tdb.isUncommittedVersion, tdb.changes)
	createdType.rawBytesInStorage = append([]byte(nil), tdb.rawBytesInStorage...) // deep-copy
	return &Mutable{
		immutable: createdType,
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
