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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// MaybeFixUsagePrivForTablesAndDBs fixes cases where privilege descriptors
// with ZONECONFIG were corrupted after upgrading from 20.1 to 20.2.
// USAGE was mistakenly added in the privilege bitfield above ZONECONFIG
// causing privilege descriptors with ZONECONFIG in 20.1 to have USAGE privilege
// instead of ZONECONFIG.
// Fortunately ZONECONFIG was only valid on TABLES/DB while USAGE is not valid
// on either so we know if the descriptor was corrupted.
func MaybeFixUsagePrivForTablesAndDBs(ptr **catpb.PrivilegeDescriptor) bool {
	if *ptr == nil {
		*ptr = &catpb.PrivilegeDescriptor{}
	}
	p := *ptr

	if p.Version > catpb.InitialVersion {
		// InitialVersion is for descriptors that were created in versions 20.1 and
		// earlier. If the privilege descriptor was created after 20.1, then we
		// do not have to fix it. Furthermore privilege descriptor versions are
		// currently never updated so we're guaranteed to only have this issue
		// on privilege descriptors that are on "InitialVersion".
		return false
	}

	modified := false
	for i := range p.Users {
		// Users is a slice of values, we need pointers to make them mutable.
		userPrivileges := &p.Users[i]
		// Tables and Database should not have USAGE privilege in 20.2 onwards.
		// The only reason they would have USAGE privilege is because they had
		// ZoneConfig in 20.1 and upgrading to 20.2 where USAGE was added
		// in the privilege bitfield where ZONECONFIG previously was.
		if privilege.USAGE.Mask()&userPrivileges.Privileges != 0 {
			// Remove USAGE privilege and add ZONECONFIG. The privilege was
			// originally ZONECONFIG in 20.1 but got changed to USAGE in 20.2
			// due to changing the bitfield values.
			userPrivileges.Privileges = (userPrivileges.Privileges - privilege.USAGE.Mask()) | privilege.ZONECONFIG.Mask()
			modified = true
		}
	}

	return modified
}

// MaybeFixPrivileges fixes the privilege descriptor if needed, including:
// * adding default privileges for the "admin" role
// * fixing default privileges for the "root" user
// * fixing maximum privileges for users.
// * populating the owner field if previously empty.
// * updating version field to Version21_2.
// MaybeFixPrivileges can be removed after v21.2.
func MaybeFixPrivileges(
	ptr **catpb.PrivilegeDescriptor,
	parentID, parentSchemaID descpb.ID,
	objectType privilege.ObjectType,
	objectName string,
) bool {
	if *ptr == nil {
		*ptr = &catpb.PrivilegeDescriptor{}
	}
	p := *ptr
	allowedPrivilegesBits := privilege.GetValidPrivilegesForObject(objectType).ToBitField()
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

	fixSuperUser := func(user security.SQLUsername) {
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
	fixSuperUser(security.RootUserName())
	fixSuperUser(security.AdminRoleName())

	if objectType == privilege.Table || objectType == privilege.Database {
		changed = MaybeFixUsagePrivForTablesAndDBs(&p) || changed
	}

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
			p.SetOwner(security.NodeUserName())
		} else {
			p.SetOwner(security.RootUserName())
		}
		changed = true
	}

	if p.Version < catpb.Version21_2 {
		p.SetVersion(catpb.Version21_2)
		changed = true
	}
	return changed
}

// MaybeUpdateGrantOptions iterates over the users of the descriptor and checks
// if they have the GRANT privilege - if so, then set the user's grant option
// bits equal to the privilege bits.
func MaybeUpdateGrantOptions(p *catpb.PrivilegeDescriptor) bool {
	// If admin has grant option bits set, then we know the descriptor was
	// created by a new binary, so all the other grant options are already
	// correct. Note that admin always has SELECT on *every* table including
	// system tables.
	if p.CheckGrantOptions(security.AdminRoleName(), privilege.List{privilege.SELECT}) {
		return false
	}

	changed := false
	for i := range p.Users {
		u := &p.Users[i]
		if privilege.ALL.IsSetIn(u.Privileges) {
			if !privilege.ALL.IsSetIn(u.WithGrantOption) {
				changed = true
			}
			u.WithGrantOption = privilege.ALL.Mask()
			continue
		}
		if privilege.GRANT.IsSetIn(u.Privileges) {
			if u.Privileges != u.WithGrantOption {
				changed = true
			}
			u.WithGrantOption |= u.Privileges
		}
	}

	return changed
}
