// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package defaultprivilegedesc

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// defaultPrivileges is an interface for default privileges to ensure
// DefaultPrivilegeDescriptor protos are not accessed and interacted
// with directly.
type defaultPrivileges interface {
	CreatePrivilegesFromDefaultPrivileges(
		dbID descpb.ID,
		user security.SQLUsername,
		targetObject tree.AlterDefaultPrivilegesTargetObject,
		databasePrivileges *descpb.PrivilegeDescriptor,
	) *descpb.PrivilegeDescriptor
	GetDefaultPrivilegesForRole(descpb.DefaultPrivilegesRole) (*descpb.DefaultPrivilegesForRole, bool)
	ForEachDefaultPrivilegeForRole(func(descpb.DefaultPrivilegesForRole) error) error
}

// ImmutableDefaultPrivileges is a wrapper for a DefaultPrivilegeDescriptor
// that only exposes getters.
type ImmutableDefaultPrivileges struct {
	defaultPrivileges
	defaultPrivilegeDescriptor *descpb.DefaultPrivilegeDescriptor
}

// MutableDefaultPrivileges is a wrapper for a DefaultPrivilegeDescriptor
// that exposes getters and setters.
type MutableDefaultPrivileges struct {
	ImmutableDefaultPrivileges
}

// MakeImmutableDefaultPrivileges returns an ImmutableDefaultPrivileges
// given a defaultPrivilegeDescriptor.
func MakeImmutableDefaultPrivileges(
	defaultPrivilegeDescriptor *descpb.DefaultPrivilegeDescriptor,
) ImmutableDefaultPrivileges {
	return ImmutableDefaultPrivileges{
		defaultPrivilegeDescriptor: defaultPrivilegeDescriptor,
	}
}

// MakeMutableDefaultPrivileges returns a MutableDefaultPrivileges
// given a defaultPrivilegeDescriptor.
func MakeMutableDefaultPrivileges(
	defaultPrivilegeDescriptor *descpb.DefaultPrivilegeDescriptor,
) MutableDefaultPrivileges {
	return MutableDefaultPrivileges{
		MakeImmutableDefaultPrivileges(defaultPrivilegeDescriptor),
	}
}

// GrantDefaultPrivileges grants privileges for the specified users.
func (d *MutableDefaultPrivileges) GrantDefaultPrivileges(
	role descpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees tree.NameList,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivilegesPerObject := d.defaultPrivilegeDescriptor.
		FindOrCreateUser(role).DefaultPrivilegesPerObject
	for _, grantee := range grantees {
		defaultPrivileges := defaultPrivilegesPerObject[targetObject]
		defaultPrivileges.Grant(
			security.MakeSQLUsernameFromPreNormalizedString(string(grantee)),
			privileges,
		)
		defaultPrivilegesPerObject[targetObject] = defaultPrivileges
	}
}

// RevokeDefaultPrivileges revokes privileges for the specified users.
func (d *MutableDefaultPrivileges) RevokeDefaultPrivileges(
	role descpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees tree.NameList,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivilegesPerObject := d.defaultPrivilegeDescriptor.
		FindOrCreateUser(role).DefaultPrivilegesPerObject
	for _, grantee := range grantees {
		defaultPrivileges := defaultPrivilegesPerObject[targetObject]
		defaultPrivileges.Revoke(
			security.MakeSQLUsernameFromPreNormalizedString(string(grantee)),
			privileges,
			targetObject.ToPrivilegeObjectType(),
		)

		defaultPrivilegesPerObject[targetObject] = defaultPrivileges
	}
}

// CreatePrivilegesFromDefaultPrivileges creates privileges for a
// the specified object with the corresponding default privileges and
// the appropriate owner (node for system, the restoring user otherwise.)
func (d *MutableDefaultPrivileges) CreatePrivilegesFromDefaultPrivileges(
	dbID descpb.ID,
	user security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	databasePrivileges *descpb.PrivilegeDescriptor,
) *descpb.PrivilegeDescriptor {
	defaultPrivileges := d.defaultPrivilegeDescriptor
	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	if dbID == keys.SystemDatabaseID {
		return descpb.NewDefaultPrivilegeDescriptor(security.NodeUserName())
	}

	if defaultPrivileges == nil {
		defaultPrivileges = descpb.InitDefaultPrivilegeDescriptor()
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

// ForEachDefaultPrivilegeForRole loops through the DefaultPrivilegeDescriptior's
// DefaultPrivilegePerRole entry and calls f on it.
func (d *ImmutableDefaultPrivileges) ForEachDefaultPrivilegeForRole(
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

// GetDefaultPrivilegesForRole looks for a specific user in the list.
// Returns (empty struct, false) if not found, or (obj, true) if found.
func (d *ImmutableDefaultPrivileges) GetDefaultPrivilegesForRole(
	role descpb.DefaultPrivilegesRole,
) (descpb.DefaultPrivilegesForRole, bool) {
	idx := d.defaultPrivilegeDescriptor.FindUserIndex(role)
	if idx == -1 {
		return descpb.DefaultPrivilegesForRole{}, false
	}
	return d.defaultPrivilegeDescriptor.DefaultPrivilegesPerRole[idx], true
}
