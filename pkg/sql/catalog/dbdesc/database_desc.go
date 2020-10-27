// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package dbdesc contains the concrete implementations of
// catalog.DatabaseDescriptor.
package dbdesc

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/redact"
)

var _ catalog.DatabaseDescriptor = (*Immutable)(nil)
var _ catalog.DatabaseDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// Immutable wraps a database descriptor and provides methods
// on it.
type Immutable struct {
	descpb.DatabaseDescriptor

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

// Mutable wraps a database descriptor and provides methods
// on it. It can be mutated and generally has not been committed.
type Mutable struct {
	Immutable

	ClusterVersion *Immutable
}

// NewInitial constructs a new Mutable for an initial version from an id and
// name with default privileges.
func NewInitial(id descpb.ID, name string, owner security.SQLUsername) *Mutable {
	return NewInitialWithPrivileges(id, name,
		descpb.NewDefaultPrivilegeDescriptor(owner))
}

// NewInitialWithPrivileges constructs a new Mutable for an initial version
// from an id and name and custom privileges.
func NewInitialWithPrivileges(
	id descpb.ID, name string, privileges *descpb.PrivilegeDescriptor,
) *Mutable {
	return NewCreatedMutable(descpb.DatabaseDescriptor{
		Name:       name,
		ID:         id,
		Version:    1,
		Privileges: privileges,
	})
}

func makeImmutable(desc descpb.DatabaseDescriptor) Immutable {
	return Immutable{DatabaseDescriptor: desc}
}

// NewImmutable makes a new immutable database descriptor.
func NewImmutable(desc descpb.DatabaseDescriptor) *Immutable {
	ret := makeImmutable(desc)
	return &ret
}

// NewCreatedMutable returns a Mutable from the given database descriptor with
// a nil cluster version. This is for a database that is created in the same
// transaction.
func NewCreatedMutable(desc descpb.DatabaseDescriptor) *Mutable {
	return &Mutable{
		Immutable: makeImmutable(desc),
	}
}

// NewExistingMutable returns a Mutable from the given database descriptor with
// the cluster version also set to the descriptor. This is for databases that
// already exist.
func NewExistingMutable(desc descpb.DatabaseDescriptor) *Mutable {
	return &Mutable{
		Immutable:      makeImmutable(*protoutil.Clone(&desc).(*descpb.DatabaseDescriptor)),
		ClusterVersion: NewImmutable(desc),
	}
}

// SafeMessage makes Immutable a SafeMessager.
func (desc *Immutable) SafeMessage() string {
	return formatSafeMessage("dbdesc.Immutable", desc)
}

// SafeMessage makes Mutable a SafeMessager.
func (desc *Mutable) SafeMessage() string {
	return formatSafeMessage("dbdesc.Mutable", desc)
}

func formatSafeMessage(typeName string, desc catalog.DatabaseDescriptor) string {
	var buf redact.StringBuilder
	buf.Print(typeName + ": {")
	catalog.FormatSafeDescriptorProperties(&buf, desc)
	buf.Print("}")
	return buf.String()
}

// TypeName returns the plain type of this descriptor.
func (desc *Immutable) TypeName() string {
	return "database"
}

// DatabaseDesc implements the Descriptor interface.
func (desc *Immutable) DatabaseDesc() *descpb.DatabaseDescriptor {
	return &desc.DatabaseDescriptor
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetParentID implements the Descriptor interface.
func (desc *Immutable) GetParentID() descpb.ID {
	return keys.RootNamespaceID
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// GetParentSchemaID implements the Descriptor interface.
func (desc *Immutable) GetParentSchemaID() descpb.ID {
	return keys.RootNamespaceID
}

// NameResolutionResult implements the Descriptor interface.
func (desc *Immutable) NameResolutionResult() {}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *Immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the Descriptor interface.
func (desc *Immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

// Adding implements the Descriptor interface.
func (desc *Immutable) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *Immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

// Dropped implements the Descriptor interface.
func (desc *Immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *Immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Database{
			Database: &desc.DatabaseDescriptor,
		},
	}
}

// SetName sets the name on the descriptor.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *Immutable) Validate() error {
	if err := catalog.ValidateName(desc.GetName(), "descriptor"); err != nil {
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
	return desc.Privileges.Validate(desc.GetID(), privilege.Database)
}

// SchemaMeta implements the tree.SchemaMeta interface.
// TODO (rohany): I don't want to keep this here, but it seems to be used
//  by backup only for the fake resolution that occurs in backup. Is it possible
//  to have this implementation only visible there? Maybe by creating a type
//  alias for database descriptor in the backupccl package, and then defining
//  SchemaMeta on it?
func (desc *Immutable) SchemaMeta() {}

// LookupSchema returns a descpb.DatabaseDescriptor_SchemaInfo for the schema
// with the provided name, if a schema with that name belongs to the database.
func (desc *Immutable) LookupSchema(
	name string,
) (schemaInfo descpb.DatabaseDescriptor_SchemaInfo, found bool) {
	schemaInfo, found = desc.Schemas[name]
	return schemaInfo, found
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	if desc.ClusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	imm := NewImmutable(*protoutil.Clone(desc.DatabaseDesc()).(*descpb.DatabaseDescriptor))
	imm.isUncommittedVersion = desc.IsUncommittedVersion()
	return imm
}

// IsNew implements the MutableDescriptor interface.
func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion == nil
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.GetVersion() != desc.ClusterVersion.GetVersion()
}

// SetPublic implements the MutableDescriptor interface.
func (desc *Mutable) SetPublic() {
	desc.State = descpb.DescriptorState_PUBLIC
	desc.OfflineReason = ""
}

// SetDropped implements the MutableDescriptor interface.
func (desc *Mutable) SetDropped() {
	desc.State = descpb.DescriptorState_DROP
	desc.OfflineReason = ""
}

// SetOffline implements the MutableDescriptor interface.
func (desc *Mutable) SetOffline(reason string) {
	desc.State = descpb.DescriptorState_OFFLINE
	desc.OfflineReason = reason
}

// AddDrainingName adds a draining name to the DatabaseDescriptor's slice of
// draining names.
func (desc *Mutable) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
}
