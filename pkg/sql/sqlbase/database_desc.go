// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// DatabaseDescriptor will eventually be called dbdesc.Descriptor.
// It is implemented by ImmutableDatabaseDescriptor.
type DatabaseDescriptor interface {
	Descriptor

	// Note: Prior to user-defined schemas, databases were the schema meta for
	// objects.
	//
	// TODO(ajwerner): Remove this in the 20.2 cycle as part of user-defined
	// schemas.
	tree.SchemaMeta
	DatabaseDesc() *descpb.DatabaseDescriptor
}

var _ DatabaseDescriptor = (*ImmutableDatabaseDescriptor)(nil)

// ImmutableDatabaseDescriptor wraps a database descriptor and provides methods
// on it.
type ImmutableDatabaseDescriptor struct {
	descpb.DatabaseDescriptor
}

// MutableDatabaseDescriptor wraps a database descriptor and provides methods
// on it. It can be mutated and generally has not been committed.
type MutableDatabaseDescriptor struct {
	ImmutableDatabaseDescriptor

	ClusterVersion *ImmutableDatabaseDescriptor
}

// NewInitialDatabaseDescriptor constructs a new DatabaseDescriptor for an
// initial version from an id and name.
func NewInitialDatabaseDescriptor(
	id descpb.ID, name string, owner string,
) *MutableDatabaseDescriptor {
	return NewInitialDatabaseDescriptorWithPrivileges(id, name,
		descpb.NewDefaultPrivilegeDescriptor(owner))
}

// NewInitialDatabaseDescriptorWithPrivileges constructs a new DatabaseDescriptor for an
// initial version from an id and name.
func NewInitialDatabaseDescriptorWithPrivileges(
	id descpb.ID, name string, privileges *descpb.PrivilegeDescriptor,
) *MutableDatabaseDescriptor {
	return NewMutableCreatedDatabaseDescriptor(descpb.DatabaseDescriptor{
		Name:       name,
		ID:         id,
		Version:    1,
		Privileges: privileges,
	})
}

func makeImmutableDatabaseDescriptor(desc descpb.DatabaseDescriptor) ImmutableDatabaseDescriptor {
	return ImmutableDatabaseDescriptor{DatabaseDescriptor: desc}
}

// NewImmutableDatabaseDescriptor makes a new database descriptor.
func NewImmutableDatabaseDescriptor(desc descpb.DatabaseDescriptor) *ImmutableDatabaseDescriptor {
	ret := makeImmutableDatabaseDescriptor(desc)
	return &ret
}

// NewMutableCreatedDatabaseDescriptor returns a MutableDatabaseDescriptor from
// the given database descriptor with a nil cluster version. This is for a
// database that is created in the same transaction.
func NewMutableCreatedDatabaseDescriptor(
	desc descpb.DatabaseDescriptor,
) *MutableDatabaseDescriptor {
	return &MutableDatabaseDescriptor{
		ImmutableDatabaseDescriptor: makeImmutableDatabaseDescriptor(desc),
	}
}

// NewMutableExistingDatabaseDescriptor returns a MutableDatabaseDescriptor from the
// given database descriptor with the cluster version also set to the descriptor.
// This is for databases that already exist.
func NewMutableExistingDatabaseDescriptor(
	desc descpb.DatabaseDescriptor,
) *MutableDatabaseDescriptor {
	return &MutableDatabaseDescriptor{
		ImmutableDatabaseDescriptor: makeImmutableDatabaseDescriptor(*protoutil.Clone(&desc).(*descpb.DatabaseDescriptor)),
		ClusterVersion:              NewImmutableDatabaseDescriptor(desc),
	}
}

// TypeName returns the plain type of this descriptor.
func (desc *ImmutableDatabaseDescriptor) TypeName() string {
	return "database"
}

// DatabaseDesc implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) DatabaseDesc() *descpb.DatabaseDescriptor {
	return &desc.DatabaseDescriptor
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentID implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) GetParentID() descpb.ID {
	return keys.RootNamespaceID
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// NameResolutionResult implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) NameResolutionResult() {}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *ImmutableDatabaseDescriptor) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Adding implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) Offline() bool {
	return false
}

// GetOfflineReason implements the Descriptor interface.
func (desc *ImmutableDatabaseDescriptor) GetOfflineReason() string {
	return ""
}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *ImmutableDatabaseDescriptor) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Database{
			Database: &desc.DatabaseDescriptor,
		},
	}
}

// SetName sets the name on the descriptor.
func (desc *MutableDatabaseDescriptor) SetName(name string) {
	desc.Name = name
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *ImmutableDatabaseDescriptor) Validate() error {
	if err := validateName(desc.GetName(), "descriptor"); err != nil {
		return err
	}
	if desc.GetID() == 0 {
		return fmt.Errorf("invalid database ID %d", desc.GetID())
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	descpb.MaybeFixPrivileges(desc.GetID(), desc.Privileges)

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) OriginalID() descpb.ID {
	if desc.ClusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) OriginalVersion() descpb.DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) Immutable() Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableDatabaseDescriptor(*protoutil.Clone(desc.DatabaseDesc()).(*descpb.DatabaseDescriptor))
}

// IsNew implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) IsNew() bool {
	return desc.ClusterVersion == nil
}

// AddDrainingName adds a draining name to the DatabaseDescriptor's slice of
// draining names.
func (desc *MutableDatabaseDescriptor) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
}
