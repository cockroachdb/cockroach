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

	ClusterVersion *DatabaseDescriptor
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

func makeImmutableDatabaseDesc(desc DatabaseDescriptor) ImmutableDatabaseDescriptor {
	return ImmutableDatabaseDescriptor{DatabaseDescriptor: desc}
}

// NewImmutableDatabaseDescriptor makes a new database descriptor.
func NewImmutableDatabaseDescriptor(desc DatabaseDescriptor) *ImmutableDatabaseDescriptor {
	ret := makeImmutableDatabaseDesc(desc)
	return &ret
}

// NewMutableDatabaseDescriptor creates a new MutableDatabaseDescriptor. The
// version of the returned descriptor will be the successor of the descriptor
// from which it was constructed.
func NewMutableDatabaseDescriptor(mutationOf DatabaseDescriptor) *MutableDatabaseDescriptor {
	mut := &MutableDatabaseDescriptor{
		ImmutableDatabaseDescriptor: makeImmutableDatabaseDesc(*protoutil.Clone(&mutationOf).(*DatabaseDescriptor)),
		ClusterVersion:              &mutationOf,
	}
	mut.Version++
	return mut
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

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *ImmutableDatabaseDescriptor) NameResolutionResult() {}

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

// SetID sets the id on the descriptor.
func (desc *MutableDatabaseDescriptor) SetID(id ID) {
	desc.ID = id
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *DatabaseDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid database ID %d", desc.ID)
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID())

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *DatabaseDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}
