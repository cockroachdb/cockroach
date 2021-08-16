// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catprivilege

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var _ catalog.DefaultPrivilegeDescriptor = &immutable{}
var _ catalog.DefaultPrivilegeDescriptor = &Mutable{}

// immutable is a wrapper for a DefaultPrivilegeDescriptor
// that only exposes getters.
type immutable struct {
	defaultPrivilegeDescriptor *descpb.DefaultPrivilegeDescriptor
}

// Mutable is a wrapper for a DefaultPrivilegeDescriptor
// that exposes getters and setters.
type Mutable struct {
	immutable
}

// MakeDefaultPrivileges returns an immutable
// given a defaultPrivilegeDescriptor.
func MakeDefaultPrivileges(
	defaultPrivilegeDescriptor *descpb.DefaultPrivilegeDescriptor,
) catalog.DefaultPrivilegeDescriptor {
	return &immutable{
		defaultPrivilegeDescriptor: defaultPrivilegeDescriptor,
	}
}

// NewMutableDefaultPrivileges returns a Mutable
// given a defaultPrivilegeDescriptor.
func NewMutableDefaultPrivileges(
	defaultPrivilegeDescriptor *descpb.DefaultPrivilegeDescriptor,
) *Mutable {
	return &Mutable{
		immutable{
			defaultPrivilegeDescriptor: defaultPrivilegeDescriptor,
		}}
}

// GrantDefaultPrivileges grants privileges for the specified users.
func (d *Mutable) GrantDefaultPrivileges(
	role descpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees []security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivilegesPerObject := d.defaultPrivilegeDescriptor.
		FindOrCreateUser(role).DefaultPrivilegesPerObject
	for _, grantee := range grantees {
		defaultPrivileges := defaultPrivilegesPerObject[targetObject]
		defaultPrivileges.Grant(
			grantee,
			privileges,
		)
		defaultPrivilegesPerObject[targetObject] = defaultPrivileges
	}
}

// RevokeDefaultPrivileges revokes privileges for the specified users.
func (d *Mutable) RevokeDefaultPrivileges(
	role descpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees []security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivilegesPerObject := d.defaultPrivilegeDescriptor.
		FindOrCreateUser(role).DefaultPrivilegesPerObject
	for _, grantee := range grantees {
		defaultPrivileges := defaultPrivilegesPerObject[targetObject]
		defaultPrivileges.Revoke(
			grantee,
			privileges,
			targetObject.ToPrivilegeObjectType(),
		)

		defaultPrivilegesPerObject[targetObject] = defaultPrivileges
	}

	// If ForAllRoles was specified, we do not have to remove any users.
	if role.ForAllRoles {
		return
	}
	// Check if there are any default privileges remaining on the descriptor.
	// If empty we will remove the map entry.
	for _, defaultPrivs := range defaultPrivilegesPerObject {
		if len(defaultPrivs.Users) != 0 {
			return
		}
	}
	// There no entries remaining, remove the entry for the role.
	d.defaultPrivilegeDescriptor.RemoveUser(role)
}

// CreatePrivilegesFromDefaultPrivileges implements the
// catalog.DefaultPrivilegeDescriptor interface.
// CreatePrivilegesFromDefaultPrivileges creates privileges for a
// the specified object with the corresponding default privileges and
// the appropriate owner (node for system, the restoring user otherwise.)
func (d *immutable) CreatePrivilegesFromDefaultPrivileges(
	dbID descpb.ID,
	user security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	databasePrivileges *descpb.PrivilegeDescriptor,
) *descpb.PrivilegeDescriptor {
	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	if dbID == keys.SystemDatabaseID {
		return descpb.NewDefaultPrivilegeDescriptor(security.NodeUserName())
	}

	// The privileges for the object are the union of the default privileges
	// defined for the object for the object creator and the default privileges
	// defined for all roles.
	newPrivs := descpb.NewDefaultPrivilegeDescriptor(user)
	defaultPrivilegesForAllRoles, found := d.GetDefaultPrivilegesForRole(
		descpb.DefaultPrivilegesRole{
			ForAllRoles: true,
		},
	)
	if found {
		defaultPrivileges, descriptorExists := defaultPrivilegesForAllRoles.DefaultPrivilegesPerObject[targetObject]
		if descriptorExists {
			for _, user := range defaultPrivileges.Users {
				newPrivs.Grant(
					user.UserProto.Decode(),
					privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
				)
			}
		}
	}

	defaultPrivilegesForCreator, defaultPrivilegesDefinedForCreator := d.GetDefaultPrivilegesForRole(
		descpb.DefaultPrivilegesRole{
			Role: user,
		})
	if defaultPrivilegesDefinedForCreator {
		defaultPrivileges, descriptorExists := defaultPrivilegesForCreator.DefaultPrivilegesPerObject[targetObject]
		if descriptorExists {
			for _, user := range defaultPrivileges.Users {
				newPrivs.Grant(
					user.UserProto.Decode(),
					privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
				)
			}
		}
	}

	newPrivs.SetOwner(user)
	newPrivs.Version = descpb.Version21_2

	// TODO(richardjcai): Remove this depending on how we handle the migration.
	//   For backwards compatibility, also "inherit" privileges from the dbDesc.
	//   Issue #67378.
	if targetObject == tree.Tables || targetObject == tree.Sequences {
		for _, u := range databasePrivileges.Users {
			newPrivs.Grant(u.UserProto.Decode(), privilege.ListFromBitField(u.Privileges, privilege.Table))
		}
	} else if targetObject == tree.Schemas {
		for _, u := range databasePrivileges.Users {
			newPrivs.Grant(u.UserProto.Decode(), privilege.ListFromBitField(u.Privileges, privilege.Schema))
		}
	}
	return newPrivs
}

// ForEachDefaultPrivilegeForRole implements the
// catalog.DefaultPrivilegeDescriptor interface.
// ForEachDefaultPrivilegeForRole loops through the DefaultPrivilegeDescriptior's
// DefaultPrivilegePerRole entry and calls f on it.
func (d *immutable) ForEachDefaultPrivilegeForRole(
	f func(defaultPrivilegesForRole descpb.DefaultPrivilegesForRole) error,
) error {
	if d.defaultPrivilegeDescriptor == nil {
		return nil
	}
	for _, defaultPrivilegesForRole := range d.defaultPrivilegeDescriptor.DefaultPrivilegesPerRole {
		if err := f(defaultPrivilegesForRole); err != nil {
			return err
		}
	}
	return nil
}

// GetDefaultPrivilegesForRole implements the
// catalog.DefaultPrivilegeDescriptor interface.
// GetDefaultPrivilegesForRole looks for a specific user in the list.
// Returns (nil, false) if not found, or (ptr, true) if found.
func (d *immutable) GetDefaultPrivilegesForRole(
	role descpb.DefaultPrivilegesRole,
) (*descpb.DefaultPrivilegesForRole, bool) {
	idx := d.defaultPrivilegeDescriptor.FindUserIndex(role)
	if idx == -1 {
		return nil, false
	}
	return &d.defaultPrivilegeDescriptor.DefaultPrivilegesPerRole[idx], true
}
