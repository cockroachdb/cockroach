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

// Grant adds new privileges to this descriptor for a given list of users.
func (p *DefaultPrivilegesForRole) Grant(
	role DefaultPrivilegesRole,
	user security.SQLUsername,
	privList privilege.List,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivileges := p.DefaultPrivilegesPerObject[targetObject]
	// expandPrivileges turns flags on the DefaultPrivilegesForRole representing
	// special privilege cases into real privileges on the PrivilegeDescriptor.
	// foldPrivileges converts the real privileges back into flags.
	p.expandPrivileges(role, &defaultPrivileges, targetObject)
	defaultPrivileges.Grant(user, privList)
	p.foldPrivileges(role, &defaultPrivileges, targetObject)
	p.DefaultPrivilegesPerObject[targetObject] = defaultPrivileges
}

// Revoke removes privileges from this descriptor for a given list of users.
func (p *DefaultPrivilegesForRole) Revoke(
	role DefaultPrivilegesRole,
	user security.SQLUsername,
	privList privilege.List,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivileges := p.DefaultPrivilegesPerObject[targetObject]
	// expandPrivileges turns flags on the DefaultPrivilegesForRole representing
	// special privilege cases into real privileges on the PrivilegeDescriptor.
	// foldPrivileges converts the real privileges back into flags.
	p.expandPrivileges(role, &defaultPrivileges, targetObject)
	defaultPrivileges.Revoke(user, privList, targetObject.ToPrivilegeObjectType())
	p.foldPrivileges(role, &defaultPrivileges, targetObject)

	p.DefaultPrivilegesPerObject[targetObject] = defaultPrivileges
}

// foldPrivileges folds ALL privileges for role and USAGE on public into
// the corresponding flag on the DefaultPrivilegesForRole object.
// For example, if after a Grant operation, role has ALL on tables, ALL
// privilege is removed from the UserPrivileges object and instead
// RoleHasAllPrivilegesOnTable is set to true.
// This is necessary as role having ALL privileges on tables is the default state
// and should not prevent the role from being dropped if it has ALL privileges.
func (p *DefaultPrivilegesForRole) foldPrivileges(
	role DefaultPrivilegesRole,
	privileges *PrivilegeDescriptor,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if targetObject == tree.Types &&
		privileges.CheckPrivilege(security.PublicRoleName(), privilege.USAGE) {
		p.setPublicHasUsageOnTypes(true)
		privileges.Revoke(
			security.PublicRoleName(),
			privilege.List{privilege.USAGE},
			privilege.Type,
		)
	}
	// ForAllRoles cannot be a grantee, nothing left to do.
	if role.ForAllRoles {
		return
	}
	if privileges.hasAllPrivileges(role.Role, targetObject.ToPrivilegeObjectType()) {
		switch targetObject {
		// When granting, if the flag is set that the role has all privileges
		// do nothing since there are no privileges to be granted.
		case tree.Tables:
			p.GetExplicitRole().RoleHasAllPrivilegesOnTables = true
		case tree.Sequences:
			p.GetExplicitRole().RoleHasAllPrivilegesOnSequences = true
		case tree.Schemas:
			p.GetExplicitRole().RoleHasAllPrivilegesOnSchemas = true
		case tree.Types:
			p.GetExplicitRole().RoleHasAllPrivilegesOnTypes = true
		}
		privileges.removeUser(role.Role)
	}
}

// expandPrivileges expands the pseudo privilege flags on
// DefaultPrivilegesForRole into real privileges on the UserPrivileges object.
// After expandPrivileges, UserPrivileges can be Granted/Revoked from normally.
// For example - if RoleHasAllPrivilegesOnTables is true, ALL privilege is added
// into the UserPrivileges array for the Role.
func (p *DefaultPrivilegesForRole) expandPrivileges(
	role DefaultPrivilegesRole,
	privileges *PrivilegeDescriptor,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if targetObject == tree.Types && p.GetPublicHasUsageOnTypes() {
		privileges.Grant(security.PublicRoleName(), privilege.List{privilege.USAGE})
		p.setPublicHasUsageOnTypes(false)
	}
	// ForAllRoles cannot be a grantee, nothing left to do.
	if role.ForAllRoles {
		return
	}
	switch targetObject {
	case tree.Tables:
		if p.GetRoleHasAllPrivilegesOnTables() {
			privileges.Grant(p.GetExplicitRole().UserProto.Decode(), privilege.List{privilege.ALL})
			p.GetExplicitRole().RoleHasAllPrivilegesOnTables = false
		}
	case tree.Sequences:
		if p.GetRoleHasAllPrivilegesOnSequences() {
			privileges.Grant(p.GetExplicitRole().UserProto.Decode(), privilege.List{privilege.ALL})
			p.GetExplicitRole().RoleHasAllPrivilegesOnSequences = false
		}
	case tree.Schemas:
		if p.GetRoleHasAllPrivilegesOnSchemas() {
			privileges.Grant(p.GetExplicitRole().UserProto.Decode(), privilege.List{privilege.ALL})
			p.GetExplicitRole().RoleHasAllPrivilegesOnSchemas = false
		}
	case tree.Types:
		if p.GetRoleHasAllPrivilegesOnTypes() {
			privileges.Grant(p.GetExplicitRole().UserProto.Decode(), privilege.List{privilege.ALL})
			p.GetExplicitRole().RoleHasAllPrivilegesOnTypes = false
		}
	}
}

// ToDefaultPrivilegesRole returns the DefaultPrivilegesRole corresponding to
// DefaultPrivilegesForRole.
func (p *DefaultPrivilegesForRole) ToDefaultPrivilegesRole() DefaultPrivilegesRole {
	if p.IsExplicitRole() {
		return DefaultPrivilegesRole{
			Role: p.GetExplicitRole().UserProto.Decode(),
		}
	}
	return DefaultPrivilegesRole{
		ForAllRoles: true,
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

// GetUserPrivilegesForObject returns the set of []UserPrivileges constructed
// from the DefaultPrivilegesForRole.
func (p DefaultPrivilegesForRole) GetUserPrivilegesForObject(
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) []UserPrivileges {
	var userPrivileges []UserPrivileges
	if privileges, ok := p.DefaultPrivilegesPerObject[targetObject]; ok {
		userPrivileges = privileges.Users
	}
	if p.GetPublicHasUsageOnTypes() && targetObject == tree.Types {
		userPrivileges = append(userPrivileges, UserPrivileges{
			UserProto:  security.PublicRoleName().EncodeProto(),
			Privileges: privilege.USAGE.Mask(),
		})
	}
	// If ForAllRoles is specified, we can return early.
	// ForAllRoles is not a real role and does not have implicit default privileges
	// for itself.
	if !p.IsExplicitRole() {
		return userPrivileges
	}
	userProto := p.GetExplicitRole().UserProto
	switch targetObject {
	case tree.Tables:
		if p.GetRoleHasAllPrivilegesOnTables() {
			return append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	case tree.Sequences:
		if p.GetRoleHasAllPrivilegesOnSequences() {
			return append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	case tree.Schemas:
		if p.GetRoleHasAllPrivilegesOnSchemas() {
			return append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	case tree.Types:
		if p.GetRoleHasAllPrivilegesOnTypes() {
			userPrivileges = append(userPrivileges, UserPrivileges{
				UserProto:  userProto,
				Privileges: privilege.ALL.Mask(),
			})
		}
	}
	return userPrivileges
}

// FindUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p *DefaultPrivilegeDescriptor) FindUserIndex(role DefaultPrivilegesRole) int {
	idx := sort.Search(len(p.DefaultPrivilegesPerRole), func(i int) bool {
		return !p.DefaultPrivilegesPerRole[i].ToDefaultPrivilegesRole().LessThan(role)
	})
	if idx < len(p.DefaultPrivilegesPerRole) &&
		p.DefaultPrivilegesPerRole[idx].ToDefaultPrivilegesRole() == role {
		return idx
	}
	return -1
}

// FindOrCreateUser looks for a specific user in the list, creating it if needed.
// If a new user is created, it must be added in the correct sorted order
// in the list.
func (p *DefaultPrivilegeDescriptor) FindOrCreateUser(
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
		defaultPrivilegesRole = &DefaultPrivilegesForRole_ForAllRoles{
			ForAllRoles: &DefaultPrivilegesForRoleForAllRolesMessage{
				PublicHasUsageOnTypes: true,
			},
		}
		return DefaultPrivilegesForRole{
			Role:                       defaultPrivilegesRole,
			DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
		}
	}
	defaultPrivilegesRole = &DefaultPrivilegesForRole_ExplicitRole{
		ExplicitRole: &DefaultPrivilegesForRoleRoleMessage{
			UserProto:                       role.Role.EncodeProto(),
			PublicHasUsageOnTypes:           true,
			RoleHasAllPrivilegesOnTables:    true,
			RoleHasAllPrivilegesOnSequences: true,
			RoleHasAllPrivilegesOnSchemas:   true,
			RoleHasAllPrivilegesOnTypes:     true,
		},
	}
	return DefaultPrivilegesForRole{
		Role:                       defaultPrivilegesRole,
		DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
	}
}

// RemoveUser looks for a given user in the list and removes it if present.
func (p *DefaultPrivilegeDescriptor) RemoveUser(role DefaultPrivilegesRole) {
	idx := p.FindUserIndex(role)
	if idx == -1 {
		// Not found.
		return
	}
	p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole[:idx], p.DefaultPrivilegesPerRole[idx+1:]...)
}

// Validate returns an assertion error if the default privilege descriptor
// is invalid.
func (p *DefaultPrivilegeDescriptor) Validate() error {
	entryForAllRolesFound := false
	for i, defaultPrivilegesForRole := range p.DefaultPrivilegesPerRole {
		if !defaultPrivilegesForRole.IsExplicitRole() {
			if entryForAllRolesFound {
				return errors.AssertionFailedf("multiple entries found in map for all roles")
			}
			entryForAllRolesFound = true
		}
		if i+1 < len(p.DefaultPrivilegesPerRole) &&
			!defaultPrivilegesForRole.ToDefaultPrivilegesRole().
				LessThan(p.DefaultPrivilegesPerRole[i+1].ToDefaultPrivilegesRole()) {
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

// GetPublicHasUsageOnTypes returns whether Public has Usage privilege on types.
func (p *DefaultPrivilegesForRole) GetPublicHasUsageOnTypes() bool {
	if p.IsExplicitRole() {
		return p.GetExplicitRole().PublicHasUsageOnTypes
	}
	return p.GetForAllRoles().PublicHasUsageOnTypes
}

// GetRoleHasAllPrivilegesOnTables returns whether the role has all privileges
// on tables.
func (p *DefaultPrivilegesForRole) GetRoleHasAllPrivilegesOnTables() bool {
	if p.IsExplicitRole() {
		return p.GetExplicitRole().RoleHasAllPrivilegesOnTables
	}
	return false
}

// GetRoleHasAllPrivilegesOnSequences returns whether the role has all privileges
// on sequences.
func (p *DefaultPrivilegesForRole) GetRoleHasAllPrivilegesOnSequences() bool {
	if p.IsExplicitRole() {
		return p.GetExplicitRole().RoleHasAllPrivilegesOnSequences
	}
	return false
}

// GetRoleHasAllPrivilegesOnSchemas returns whether the role has all privileges
// on schemas.
func (p *DefaultPrivilegesForRole) GetRoleHasAllPrivilegesOnSchemas() bool {
	if p.IsExplicitRole() {
		return p.GetExplicitRole().RoleHasAllPrivilegesOnSchemas
	}
	return false
}

// GetRoleHasAllPrivilegesOnTypes returns whether the role has all privileges
// on types.
func (p *DefaultPrivilegesForRole) GetRoleHasAllPrivilegesOnTypes() bool {
	if p.IsExplicitRole() {
		return p.GetExplicitRole().RoleHasAllPrivilegesOnTypes
	}
	return false
}

func (p *DefaultPrivilegesForRole) setPublicHasUsageOnTypes(publicHasUsageOnTypes bool) {
	if p.IsExplicitRole() {
		p.GetExplicitRole().PublicHasUsageOnTypes = publicHasUsageOnTypes
	} else {
		p.GetForAllRoles().PublicHasUsageOnTypes = publicHasUsageOnTypes
	}
}

// IsExplicitRole returns if DefaultPrivilegesForRole is defined for
// an explicit role.
func (p *DefaultPrivilegesForRole) IsExplicitRole() bool {
	return p.GetExplicitRole() != nil
}
