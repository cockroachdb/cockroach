// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catprivilege

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// PrivilegeDescriptorBuilder used to potentially mutate privilege descriptors,
// only if a change exists to avoid copying.
type PrivilegeDescriptorBuilder interface {
	// GetReadOnlyPrivilege gets an immutable privilege descriptor
	// reference. This copy should *never* be modified.
	GetReadOnlyPrivilege() *catpb.PrivilegeDescriptor
	// GetMutablePrivilege gets a mutable privilege descriptor
	// that is safe to modify.
	GetMutablePrivilege() *catpb.PrivilegeDescriptor
}

// simplePrivilegeDescriptorBuilder implements PrivilegeDescriptorBuilder with
// the assumption that the privilege descriptor is mutable.
type simplePrivilegeDescriptorBuilder struct {
	p **catpb.PrivilegeDescriptor
}

// GetReadOnlyPrivilege implements PrivilegeDescriptorBuilder.
func (d simplePrivilegeDescriptorBuilder) GetReadOnlyPrivilege() *catpb.PrivilegeDescriptor {
	if *d.p == nil {
		*d.p = &catpb.PrivilegeDescriptor{}
	}
	return *d.p
}

// GetMutablePrivilege implements PrivilegeDescriptorBuilder.g
func (d simplePrivilegeDescriptorBuilder) GetMutablePrivilege() *catpb.PrivilegeDescriptor {
	return d.GetReadOnlyPrivilege()
}

func MaybeFixPrivileges(
	ptr **catpb.PrivilegeDescriptor,
	parentID, parentSchemaID descpb.ID,
	objectType privilege.ObjectType,
	objectName string,
) (bool, error) {
	return MaybeFixPrivilegesWithBuilder(simplePrivilegeDescriptorBuilder{
		p: ptr,
	},
		parentID,
		parentSchemaID,
		objectType,
		objectName)
}

// MaybeFixPrivilegesWithBuilder fixes the privilege descriptor if
// needed, including:
// * adding default privileges for the "admin" role
// * fixing default privileges for the "root" user
// * fixing maximum privileges for users.
// * populating the owner field if previously empty.
// * updating version field older than 21.2 to Version21_2.
// MaybeFixPrivileges can be removed after v22.2.
func MaybeFixPrivilegesWithBuilder(
	reader PrivilegeDescriptorBuilder,
	parentID, parentSchemaID descpb.ID,
	objectType privilege.ObjectType,
	objectName string,
) (bool, error) {
	// This privilege descriptor is meant to be read only,
	// and may not show any changes applied below.
	readOnlyPriv := reader.GetReadOnlyPrivilege()
	privList, err := privilege.GetValidPrivilegesForObject(objectType)
	if err != nil {
		return false, err
	}
	allowedPrivilegesBits := privList.ToBitField()
	systemPrivs := SystemSuperuserPrivileges(descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           objectName,
	})
	if systemPrivs != nil {
		// System databases and tables have custom maximum allowed privileges.
		allowedPrivilegesBits = systemPrivs.ToBitField()
	}

	changed := false

	fixSuperUser := func(user username.SQLUsername) {
		privs, found := readOnlyPriv.FindUser(user)
		if !found {
			privs = reader.GetMutablePrivilege().FindOrCreateUser(user)
		}
		oldPrivilegeBits := privs.Privileges
		if privilege.ALL.IsSetIn(allowedPrivilegesBits) {
			if oldPrivilegeBits != privilege.ALL.Mask() {
				privs, _ = reader.GetMutablePrivilege().FindUser(user)
				privs.Privileges = privilege.ALL.Mask()
				changed = true
			}
		} else if oldPrivilegeBits != allowedPrivilegesBits {
			privs, _ = reader.GetMutablePrivilege().FindUser(user)
			privs.Privileges = allowedPrivilegesBits
			changed = true
		}
	}

	// Check "root" user and "admin" role.
	fixSuperUser(username.RootUserName())
	fixSuperUser(username.AdminRoleName())

	for i := range readOnlyPriv.Users {
		// Users is a slice of values, we need pointers to make them mutable.
		u := &readOnlyPriv.Users[i]
		if u.User().IsRootUser() || u.User().IsAdminRole() {
			// we've already checked super users.
			continue
		}

		if u.Privileges&allowedPrivilegesBits != u.Privileges {
			changed = true
			reader.GetMutablePrivilege().Users[i].Privileges &= allowedPrivilegesBits
		}
	}

	if readOnlyPriv.Owner().Undefined() {
		if systemPrivs != nil {
			reader.GetMutablePrivilege().SetOwner(username.NodeUserName())
		} else {
			reader.GetMutablePrivilege().SetOwner(username.RootUserName())
		}
		changed = true
	}

	if readOnlyPriv.Version < catpb.Version21_2 {
		reader.GetMutablePrivilege().SetVersion(catpb.Version21_2)
		changed = true
	}
	return changed, nil
}
