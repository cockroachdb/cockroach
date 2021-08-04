// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DefaultPrivilegesRole represents the creator role that the default privileges
// are being altered for.
// Either:
//     role should be populated
//     forAllRoles should be true.
type DefaultPrivilegesRole struct {
	Role        security.SQLUsername
	ForAllRoles bool
}

// GrantDefaultPrivileges grants privileges for the specified users.
func (p *DefaultPrivilegeDescriptor) GrantDefaultPrivileges(
	role DefaultPrivilegesRole,
	privileges privilege.List,
	grantees tree.NameList,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivilegesForRole := p.findOrCreateUser(role)
	defaultPrivilegesPerObject := defaultPrivilegesForRole.DefaultPrivilegesPerObject
	for _, grantee := range grantees {
		granteeSQLUsername := security.MakeSQLUsernameFromPreNormalizedString(string(grantee))
		defaultPrivileges := defaultPrivilegesPerObject[targetObject]
		defaultPrivilegesForRole.expandPrivileges(role, &defaultPrivileges, targetObject)
		defaultPrivileges.Grant(
			granteeSQLUsername,
			privileges,
		)
		defaultPrivilegesForRole.foldPrivileges(role, &defaultPrivileges, targetObject)
		defaultPrivilegesPerObject[targetObject] = defaultPrivileges
	}
}

// RevokeDefaultPrivileges revokes privileges for the specified users.
func (p *DefaultPrivilegeDescriptor) RevokeDefaultPrivileges(
	role DefaultPrivilegesRole,
	privileges privilege.List,
	grantees tree.NameList,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivilegesForRole := p.findOrCreateUser(role)
	defaultPrivilegesPerObject := defaultPrivilegesForRole.DefaultPrivilegesPerObject
	for _, grantee := range grantees {
		defaultPrivileges := defaultPrivilegesPerObject[targetObject]
		granteeSQLUsername := security.MakeSQLUsernameFromPreNormalizedString(string(grantee))
		defaultPrivilegesForRole.expandPrivileges(role, &defaultPrivileges, targetObject)
		defaultPrivileges.Revoke(
			granteeSQLUsername,
			privileges,
			targetObject.ToPrivilegeObjectType(),
		)
		defaultPrivilegesForRole.foldPrivileges(role, &defaultPrivileges, targetObject)
		defaultPrivilegesPerObject[targetObject] = defaultPrivileges
	}
}

func (p *DefaultPrivilegesForRole) foldPrivileges(
	role DefaultPrivilegesRole,
	privileges *PrivilegeDescriptor,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if privileges.hasAllPrivileges(role.Role, targetObject.ToPrivilegeObjectType()) {
		switch targetObject {
		// When granting, if the flag is set that the role has all privileges
		// do nothing since there are no privileges to be granted.
		case tree.Tables:
			p.RoleHasAllPrivilegesOnTables = true
		case tree.Sequences:
			p.RoleHasAllPrivilegesOnSequences = true
		case tree.Schemas:
			p.RoleHasAllPrivilegesOnSchemas = true
		case tree.Types:
			p.RoleHasAllPrivilegesOnTypes = true
		}
		privileges.removeUser(role.Role)
	}

	if targetObject == tree.Types &&
		privileges.CheckPrivilege(security.PublicRoleName(), privilege.USAGE) {
		p.PublicHasUsageOnTypes = true
		privileges.Revoke(
			security.PublicRoleName(),
			privilege.List{privilege.USAGE},
			privilege.Type,
		)
	}
}

func (p *DefaultPrivilegesForRole) expandPrivileges(
	role DefaultPrivilegesRole,
	privileges *PrivilegeDescriptor,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if targetObject == tree.Types && p.PublicHasUsageOnTypes {
		privileges.Grant(security.PublicRoleName(), privilege.List{privilege.USAGE})
		p.PublicHasUsageOnTypes = false
	}
	// ForAllRoles cannot be a grantee, nothing to expand.
	if role.ForAllRoles {
		return
	}
	switch targetObject {
	case tree.Tables:
		if p.RoleHasAllPrivilegesOnTables {
			privileges.Grant(p.GetUserProto().Decode(), privilege.List{privilege.ALL})
			p.RoleHasAllPrivilegesOnTables = false
		}
	case tree.Sequences:
		if p.RoleHasAllPrivilegesOnSequences {
			privileges.Grant(p.GetUserProto().Decode(), privilege.List{privilege.ALL})
			p.RoleHasAllPrivilegesOnSequences = false
		}
	case tree.Schemas:
		if p.RoleHasAllPrivilegesOnSchemas {
			privileges.Grant(p.GetUserProto().Decode(), privilege.List{privilege.ALL})
			p.RoleHasAllPrivilegesOnSchemas = false
		}
	case tree.Types:
		if p.RoleHasAllPrivilegesOnTypes {
			privileges.Grant(p.GetUserProto().Decode(), privilege.List{privilege.ALL})
			p.RoleHasAllPrivilegesOnTypes = false
		}
	}
}

// CreatePrivilegesFromDefaultPrivileges creates privileges for a
// the specified object with the corresponding default privileges and
// the appropriate owner (node for system, the restoring user otherwise.)
func CreatePrivilegesFromDefaultPrivileges(
	dbID ID,
	defaultPrivileges *DefaultPrivilegeDescriptor,
	user security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	databasePrivileges *PrivilegeDescriptor,
) *PrivilegeDescriptor {
	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	if dbID == keys.SystemDatabaseID {
		return NewDefaultPrivilegeDescriptor(security.NodeUserName())
	}

	if defaultPrivileges == nil {
		defaultPrivileges = InitDefaultPrivilegeDescriptor()
	}

	// The privileges for the object are the union of the default privileges
	// defined for the object for the object creator and the default privileges
	// defined for all roles.
	newPrivs := NewDefaultPrivilegeDescriptor(user)
	defaultPrivilegesForAllRoles := defaultPrivileges.GetDefaultPrivilegesForRole(
		DefaultPrivilegesRole{
			ForAllRoles: true,
		},
	)

	for _, user := range defaultPrivilegesForAllRoles.GetUserPrivilegesForObject(targetObject) {
		newPrivs.Grant(
			user.UserProto.Decode(),
			privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
		)
	}

	defaultPrivilegesForCreator := defaultPrivileges.GetDefaultPrivilegesForRole(DefaultPrivilegesRole{
		Role: user,
	})
	for _, user := range defaultPrivilegesForCreator.GetUserPrivilegesForObject(targetObject) {
		newPrivs.Grant(
			user.UserProto.Decode(),
			privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
		)
	}

	newPrivs.SetOwner(user)
	newPrivs.Version = Version21_2

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

// ToDefaultPrivilegesRole returns the DefaultPrivilegesRole corresponding to
// DefaultPrivilegesForRole.
func (u *DefaultPrivilegesForRole) ToDefaultPrivilegesRole() DefaultPrivilegesRole {
	if u.GetForAllRoles() {
		return DefaultPrivilegesRole{
			ForAllRoles: true,
		}
	}
	return DefaultPrivilegesRole{
		Role: u.GetUserProto().Decode(),
	}
}

// LessThan returns whether r is less than other.
// The DefaultPrivilegesRole with ForAllRoles set is always considered
// larger. Only one of r or other should have ForAllRoles set since there
// should only ever be one entry for all roles.
// If ForAllRoles is set for neither, we do a string comparison on the username.
func (r DefaultPrivilegesRole) LessThan(other DefaultPrivilegesRole) bool {
	// Defined such that ForAllRoles is never less than.
	if r.ForAllRoles {
		return false
	}
	if other.ForAllRoles {
		return true
	}

	return r.Role.LessThan(other.Role)
}

// GetDefaultPrivilegesForRole looks for a specific user in the list.
// Returns the default set of default privileges for a role if not in the list.
func (p *DefaultPrivilegeDescriptor) GetDefaultPrivilegesForRole(
	role DefaultPrivilegesRole,
) DefaultPrivilegesForRole {
	idx := p.findUserIndex(role)
	if idx == -1 {
		return InitDefaultPrivilegesForRole(role)
	}
	return p.DefaultPrivilegesPerRole[idx]
}

// GetUserPrivilegesForObject returns the set of []UserPrivileges constructed
// from the DefaultPrivilegesForRole.
func (p DefaultPrivilegesForRole) GetUserPrivilegesForObject(
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) []UserPrivileges {
	var userPrivileges []UserPrivileges
	if privileges, ok := p.DefaultPrivilegesPerObject[targetObject]; ok {
		userPrivileges = privileges.Users
	}
	if p.PublicHasUsageOnTypes && targetObject == tree.Types {
		userPrivileges = append(userPrivileges, UserPrivileges{
			UserProto:  security.PublicRoleName().EncodeProto(),
			Privileges: privilege.USAGE.Mask(),
		})
	}
	// If ForAllRoles is specified, we can return early.
	// ForAllRoles is not a real role and does not have implicit default privileges
	// for itself.
	if p.GetForAllRoles() {
		return userPrivileges
	}
	userProto := p.GetUserProto()
	switch targetObject {
	case tree.Tables:
		if p.RoleHasAllPrivilegesOnTables {
			return append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	case tree.Sequences:
		if p.RoleHasAllPrivilegesOnSequences {
			return append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	case tree.Schemas:
		if p.RoleHasAllPrivilegesOnSchemas {
			return append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	case tree.Types:
		if p.RoleHasAllPrivilegesOnTypes {
			userPrivileges = append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	}
	return userPrivileges
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p *DefaultPrivilegeDescriptor) findUserIndex(role DefaultPrivilegesRole) int {
	idx := sort.Search(len(p.DefaultPrivilegesPerRole), func(i int) bool {
		return !p.DefaultPrivilegesPerRole[i].ToDefaultPrivilegesRole().LessThan(role)
	})
	if idx < len(p.DefaultPrivilegesPerRole) &&
		p.DefaultPrivilegesPerRole[idx].ToDefaultPrivilegesRole() == role {
		return idx
	}
	return -1
}

// findOrCreateUser looks for a specific user in the list, creating it if needed.
// If a new user is created, it must be added in the correct sorted order
// in the list.
func (p *DefaultPrivilegeDescriptor) findOrCreateUser(
	role DefaultPrivilegesRole,
) *DefaultPrivilegesForRole {
	idx := sort.Search(len(p.DefaultPrivilegesPerRole), func(i int) bool {
		return !p.DefaultPrivilegesPerRole[i].ToDefaultPrivilegesRole().LessThan(role)
	})
	if idx == len(p.DefaultPrivilegesPerRole) {
		// Not found but should be inserted at the end.
		p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole,
			InitDefaultPrivilegesForRole(role),
		)
	} else if p.DefaultPrivilegesPerRole[idx].ToDefaultPrivilegesRole() == role {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole, DefaultPrivilegesForRole{})
		copy(p.DefaultPrivilegesPerRole[idx+1:], p.DefaultPrivilegesPerRole[idx:])
		p.DefaultPrivilegesPerRole[idx] = InitDefaultPrivilegesForRole(role)
	}
	return &p.DefaultPrivilegesPerRole[idx]
}

// InitDefaultPrivilegesForRole creates the default DefaultPrivilegesForRole
// for a user.
func InitDefaultPrivilegesForRole(role DefaultPrivilegesRole) DefaultPrivilegesForRole {
	var defaultPrivilegesRole isDefaultPrivilegesForRole_Role
	if role.ForAllRoles {
		defaultPrivilegesRole = &DefaultPrivilegesForRole_ForAllRoles{ForAllRoles: true}
		return DefaultPrivilegesForRole{
			Role:                       defaultPrivilegesRole,
			DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
		}
	} else {
		defaultPrivilegesRole = &DefaultPrivilegesForRole_UserProto{
			UserProto: role.Role.EncodeProto(),
		}
		return DefaultPrivilegesForRole{
			Role:                            defaultPrivilegesRole,
			DefaultPrivilegesPerObject:      map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
			PublicHasUsageOnTypes:           true,
			RoleHasAllPrivilegesOnTables:    true,
			RoleHasAllPrivilegesOnSequences: true,
			RoleHasAllPrivilegesOnSchemas:   true,
			RoleHasAllPrivilegesOnTypes:     true,
		}
	}
}

// Validate returns an assertion error if the default privilege descriptor
// is invalid.
func (p *DefaultPrivilegeDescriptor) Validate() error {
	entryForAllRolesFound := false
	for i, defaultPrivilegesForRole := range p.DefaultPrivilegesPerRole {
		if defaultPrivilegesForRole.GetForAllRoles() {
			if entryForAllRolesFound {
				return errors.AssertionFailedf("multiple entries found in map for all roles")
			}
			entryForAllRolesFound = true
		}
		if i+1 < len(p.DefaultPrivilegesPerRole) &&
			!defaultPrivilegesForRole.ToDefaultPrivilegesRole().LessThan(p.DefaultPrivilegesPerRole[i+1].ToDefaultPrivilegesRole()) {
			return errors.AssertionFailedf("default privilege list is not sorted")
		}
		for objectType, defaultPrivileges := range defaultPrivilegesForRole.DefaultPrivilegesPerObject {
			privilegeObjectType := objectType.ToPrivilegeObjectType()
			valid, u, remaining := defaultPrivileges.IsValidPrivilegesForObjectType(privilegeObjectType)
			if !valid {
				return errors.AssertionFailedf("user %s must not have %s privileges on %s",
					u.User(), privilege.ListFromBitField(remaining, privilege.Any), privilegeObjectType)
			}
		}
	}

	return nil
}

// InitDefaultPrivilegeDescriptor returns a new DefaultPrivilegeDescriptor.
func InitDefaultPrivilegeDescriptor() *DefaultPrivilegeDescriptor {
	var defaultPrivilegesForRole []DefaultPrivilegesForRole
	return &DefaultPrivilegeDescriptor{
		DefaultPrivilegesPerRole: defaultPrivilegesForRole,
	}
}
