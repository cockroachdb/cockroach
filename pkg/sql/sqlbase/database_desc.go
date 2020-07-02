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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// DatabaseDescriptorInterface will eventually be called dbdesc.Descriptor.
// It is implemented by ImmutableDatabaseDescriptor.
type DatabaseDescriptorInterface interface {
	BaseDescriptorInterface
	DatabaseDesc() *DatabaseDescriptor
}

var _ DatabaseDescriptorInterface = (*ImmutableDatabaseDescriptor)(nil)

// ImmutableDatabaseDescriptor wraps a database descriptor and provides methods
// on it.
type ImmutableDatabaseDescriptor struct {
	DatabaseDescriptor
}

// MutableDatabaseDescriptor wraps a database descriptor and provides methods
// on it. It can be mutated and generally has not been committed.
type MutableDatabaseDescriptor struct {
	ImmutableDatabaseDescriptor

	ClusterVersion *ImmutableDatabaseDescriptor
}

// NewInitialDatabaseDescriptor constructs a new DatabaseDescriptor for an
// initial version from an id and name.
func NewInitialDatabaseDescriptor(id ID, name string) *ImmutableDatabaseDescriptor {
	return NewInitialDatabaseDescriptorWithPrivileges(id, name,
		NewDefaultPrivilegeDescriptor())
}

// NewInitialDatabaseDescriptorWithPrivileges constructs a new DatabaseDescriptor for an
// initial version from an id and name.
func NewInitialDatabaseDescriptorWithPrivileges(
	id ID, name string, privileges *PrivilegeDescriptor,
) *ImmutableDatabaseDescriptor {
	return NewImmutableDatabaseDescriptor(DatabaseDescriptor{
		Name:       name,
		ID:         id,
		Version:    1,
		Privileges: privileges,
	})
}

func makeImmutableDatabaseDescriptor(desc DatabaseDescriptor) ImmutableDatabaseDescriptor {
	return ImmutableDatabaseDescriptor{DatabaseDescriptor: desc}
}

// NewImmutableDatabaseDescriptor makes a new database descriptor.
func NewImmutableDatabaseDescriptor(desc DatabaseDescriptor) *ImmutableDatabaseDescriptor {
	ret := makeImmutableDatabaseDescriptor(desc)
	return &ret
}

// NewMutableExistingDatabaseDescriptor returns a MutableDatabaseDescriptor from the
// given database descriptor with the cluster version also set to the descriptor.
// This is for databases that already exist.
func NewMutableExistingDatabaseDescriptor(desc DatabaseDescriptor) *MutableDatabaseDescriptor {
	return &MutableDatabaseDescriptor{
		ImmutableDatabaseDescriptor: makeImmutableDatabaseDescriptor(*protoutil.Clone(&desc).(*DatabaseDescriptor)),
		ClusterVersion:              NewImmutableDatabaseDescriptor(desc),
	}
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// DatabaseDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) DatabaseDesc() *DatabaseDescriptor {
	return desc
}

// SchemaDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) SchemaDesc() *SchemaDescriptor {
	return nil
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TypeDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) TypeDesc() *TypeDescriptor {
	return nil
}

// GetParentID implements the BaseDescriptorInterface interface.
func (desc *ImmutableDatabaseDescriptor) GetParentID() ID {
	return keys.RootNamespaceID
}

// GetParentSchemaID implements the BaseDescriptorInterface interface.
func (desc *ImmutableDatabaseDescriptor) GetParentSchemaID() ID {
	return keys.RootNamespaceID
}

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *ImmutableDatabaseDescriptor) NameResolutionResult() {}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *ImmutableDatabaseDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *ImmutableDatabaseDescriptor) DescriptorProto() *Descriptor {
	return &Descriptor{
		Union: &Descriptor_Database{
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
	desc.Privileges.MaybeFixPrivileges(desc.GetID())

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableDatabaseDescriptor) Immutable() DescriptorInterface {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableDatabaseDescriptor(*protoutil.Clone(desc.DatabaseDesc()).(*DatabaseDescriptor))
}
