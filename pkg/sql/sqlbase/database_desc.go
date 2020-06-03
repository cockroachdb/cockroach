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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

// MakeImmutableDatabaseDescriptor constructs a DatabaseDescriptor from an AST node.
func MakeImmutableDatabaseDescriptor(id ID, p *tree.CreateDatabase) ImmutableDatabaseDescriptor {
	return makeImmutableDatabaseDesc(DatabaseDescriptor{
		Name:       string(p.Name),
		ID:         id,
		Privileges: NewDefaultPrivilegeDescriptor(),
	})
}

func makeImmutableDatabaseDesc(desc DatabaseDescriptor) ImmutableDatabaseDescriptor {
	return ImmutableDatabaseDescriptor{DatabaseDescriptor: desc}
}

// Defeat the linter.

var _ = NewImmutableDatabaseDescriptor

// NewImmutableDatabaseDescriptor makes a new database descriptor.
func NewImmutableDatabaseDescriptor(desc DatabaseDescriptor) *ImmutableDatabaseDescriptor {
	ret := makeImmutableDatabaseDesc(desc)
	return &ret
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// SetName implements the DescriptorProto interface.
func (desc *DatabaseDescriptor) SetName(name string) {
	desc.Name = name
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
func (desc *DatabaseDescriptor) NameResolutionResult() {}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *DatabaseDescriptor) DescriptorProto() *Descriptor {
	return wrapDescriptor(desc)
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
