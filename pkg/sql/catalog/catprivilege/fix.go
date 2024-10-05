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

// MaybeFixPrivileges fixes the privilege descriptor if needed, including:
// * adding default privileges for the "admin" role
// * fixing default privileges for the "root" user
// * fixing maximum privileges for users.
// * populating the owner field if previously empty.
// * updating version field older than 21.2 to Version21_2.
// MaybeFixPrivileges can be removed after v22.2.
func MaybeFixPrivileges(
	ptr **catpb.PrivilegeDescriptor,
	parentID, parentSchemaID descpb.ID,
	objectType privilege.ObjectType,
	objectName string,
) (bool, error) {
	if *ptr == nil {
		*ptr = &catpb.PrivilegeDescriptor{}
	}
	p := *ptr
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
		privs := p.FindOrCreateUser(user)
		oldPrivilegeBits := privs.Privileges
		if oldPrivilegeBits != allowedPrivilegesBits {
			if privilege.ALL.IsSetIn(allowedPrivilegesBits) {
				privs.Privileges = privilege.ALL.Mask()
			} else {
				privs.Privileges = allowedPrivilegesBits
			}
			changed = (privs.Privileges != oldPrivilegeBits) || changed
		}
	}

	// Check "root" user and "admin" role.
	fixSuperUser(username.RootUserName())
	fixSuperUser(username.AdminRoleName())

	for i := range p.Users {
		// Users is a slice of values, we need pointers to make them mutable.
		u := &p.Users[i]
		if u.User().IsRootUser() || u.User().IsAdminRole() {
			// we've already checked super users.
			continue
		}

		if u.Privileges&allowedPrivilegesBits != u.Privileges {
			changed = true
		}
		u.Privileges &= allowedPrivilegesBits
	}

	if p.Owner().Undefined() {
		if systemPrivs != nil {
			p.SetOwner(username.NodeUserName())
		} else {
			p.SetOwner(username.RootUserName())
		}
		changed = true
	}

	if p.Version < catpb.Version21_2 {
		p.SetVersion(catpb.Version21_2)
		changed = true
	}
	return changed, nil
}
