// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package funcdesc

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
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
		fdb.original.ModificationTime, fdb.mvccTimestamp, fdb.original.Version, fdb.original.State,
	)
	if err != nil {
		return err
	}
	fdb.maybeModified = protoutil.Clone(fdb.original).(*descpb.FunctionDescriptor)
	if mustSetModTime {
		fdb.maybeModified.ModificationTime = fdb.mvccTimestamp
		fdb.changes.Add(catalog.SetModTimeToMVCCTimestamp)
	}
	desc := fdb.maybeModified
	if desc.GetPrivileges().Version < catpb.Version23_2 {
		// Grant EXECUTE privilege on the function for the public role if the
		// descriptor was created before v23.2. This matches the default
		// privilege for newly created functions.
		desc.GetPrivileges().Grant(
			username.PublicRoleName(),
			privilege.List{privilege.EXECUTE},
			false, /* withGrantOption */
		)
		desc.GetPrivileges().SetVersion(catpb.Version23_2)
		fdb.changes.Add(catalog.GrantExecuteOnFunctionToPublicRole)
	}
	return nil
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (fdb *functionDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) error {
	// Upgrade the declarative schema changer state.
	if scpb.MigrateDescriptorState(version, fdb.maybeModified.ParentID, fdb.maybeModified.DeclarativeSchemaChangerState) {
		fdb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}
	return nil
}

// StripDanglingBackReferences implements the catalog.DescriptorBuilder
// interface.
func (fdb *functionDescriptorBuilder) StripDanglingBackReferences(
	descIDMightExist func(id descpb.ID) bool, nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
) error {
	sliceIdx := 0
	for _, backref := range fdb.maybeModified.DependedOnBy {
		fdb.maybeModified.DependedOnBy[sliceIdx] = backref
		if descIDMightExist(backref.ID) {
			sliceIdx++
		}
	}
	if sliceIdx < len(fdb.maybeModified.DependedOnBy) {
		fdb.maybeModified.DependedOnBy = fdb.maybeModified.DependedOnBy[:sliceIdx]
		fdb.changes.Add(catalog.StrippedDanglingBackReferences)
	}
	return nil
}

// StripNonExistentRoles implements the catalog.DescriptorBuilder
// interface.
func (fdb *functionDescriptorBuilder) StripNonExistentRoles(
	roleExists func(role username.SQLUsername) bool,
) error {
	// If the owner doesn't exist, change the owner to admin.
	if !roleExists(fdb.maybeModified.GetPrivileges().Owner()) {
		fdb.maybeModified.Privileges.OwnerProto = username.AdminRoleName().EncodeProto()
		fdb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	// Remove any non-existent roles from the privileges.
	newPrivs := make([]catpb.UserPrivileges, 0, len(fdb.maybeModified.Privileges.Users))
	for _, priv := range fdb.maybeModified.Privileges.Users {
		exists := roleExists(priv.UserProto.Decode())
		if exists {
			newPrivs = append(newPrivs, priv)
		}
	}
	if len(newPrivs) != len(fdb.maybeModified.Privileges.Users) {
		fdb.maybeModified.Privileges.Users = newPrivs
		fdb.changes.Add(catalog.StrippedNonExistentRoles)
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
