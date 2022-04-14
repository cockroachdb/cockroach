// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catpb

import (
	github_com_lib_pq_oid "github.com/lib/pq/oid"
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
	Role        security.SQLUserInfo
	ForAllRoles bool
}

// toDefaultPrivilegesRole returns the DefaultPrivilegesRole corresponding to
// DefaultPrivilegesForRole.
func (p *DefaultPrivilegesForRole) toDefaultPrivilegesRole() DefaultPrivilegesRole {
	if p.IsExplicitRole() {
		return DefaultPrivilegesRole{
			Role: security.SQLUserInfo{Username: p.GetExplicitRole().UserProto.Decode(), UserID: p.GetExplicitRole().UserId},
		}
	}
	return DefaultPrivilegesRole{
		ForAllRoles: true,
	}
}

// lessThan returns whether r is less than other.
// The DefaultPrivilegesRole with ForAllRoles set is always considered
// larger. Only one of r or other should have ForAllRoles set since there
// should only ever be one entry for all roles.
// If ForAllRoles is set for neither, we do a string comparison on the username.
func (r DefaultPrivilegesRole) lessThan(other DefaultPrivilegesRole) bool {
	// Defined such that ForAllRoles is never less than.
	if r.ForAllRoles {
		return false
	}
	if other.ForAllRoles {
		return true
	}

	return r.Role.Username.LessThan(other.Role.Username)
}

// FindUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p *DefaultPrivilegeDescriptor) FindUserIndex(role DefaultPrivilegesRole) int {
	idx := sort.Search(len(p.DefaultPrivilegesPerRole), func(i int) bool {
		return !p.DefaultPrivilegesPerRole[i].toDefaultPrivilegesRole().lessThan(role)
	})
	if idx < len(p.DefaultPrivilegesPerRole) &&
		p.DefaultPrivilegesPerRole[idx].toDefaultPrivilegesRole() == role {
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
	if p.DefaultPrivilegesMap == nil {
		p.DefaultPrivilegesMap = make(map[github_com_lib_pq_oid.Oid]DefaultPrivilegesForRole)
	}
	if _, ok := p.DefaultPrivilegesMap[role.Role.UserID]; !ok {
		p.DefaultPrivilegesMap[role.Role.UserID] = InitDefaultPrivilegesForRole(role, p.Type)
	}
	idx := sort.Search(len(p.DefaultPrivilegesPerRole), func(i int) bool {
		return !p.DefaultPrivilegesPerRole[i].toDefaultPrivilegesRole().lessThan(role)
	})
	if idx == len(p.DefaultPrivilegesPerRole) {
		// Not found but should be inserted at the end.
		p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole,
			InitDefaultPrivilegesForRole(role, p.Type),
		)
	} else if p.DefaultPrivilegesPerRole[idx].toDefaultPrivilegesRole() == role {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole, DefaultPrivilegesForRole{})
		copy(p.DefaultPrivilegesPerRole[idx+1:], p.DefaultPrivilegesPerRole[idx:])
		p.DefaultPrivilegesPerRole[idx] = InitDefaultPrivilegesForRole(role, p.Type)
	}
	return &p.DefaultPrivilegesPerRole[idx]
}

// InitDefaultPrivilegesForRole creates the default DefaultPrivilegesForRole
// for a user.
func InitDefaultPrivilegesForRole(
	role DefaultPrivilegesRole,
	defaultPrivilegeDescType DefaultPrivilegeDescriptor_DefaultPrivilegeDescriptorType,
) DefaultPrivilegesForRole {
	var defaultPrivilegesRole isDefaultPrivilegesForRole_Role
	if role.ForAllRoles {
		defaultPrivilegesRole = &DefaultPrivilegesForRole_ForAllRoles{
			ForAllRoles: &DefaultPrivilegesForRole_ForAllRolesPseudoRole{
				PublicHasUsageOnTypes: true,
			},
		}
		return DefaultPrivilegesForRole{
			Role:                       defaultPrivilegesRole,
			DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
		}
	}

	if defaultPrivilegeDescType == DefaultPrivilegeDescriptor_DATABASE {
		defaultPrivilegesRole = &DefaultPrivilegesForRole_ExplicitRole_{
			ExplicitRole: &DefaultPrivilegesForRole_ExplicitRole{
				UserProto:                       role.Role.Username.EncodeProto(),
				UserId:                          role.Role.UserID,
				PublicHasUsageOnTypes:           true,
				RoleHasAllPrivilegesOnTables:    true,
				RoleHasAllPrivilegesOnSequences: true,
				RoleHasAllPrivilegesOnSchemas:   true,
				RoleHasAllPrivilegesOnTypes:     true,
			},
		}
	} else {
		// If the default privilege descriptor is on a schema, there are no
		// defaults set.
		defaultPrivilegesRole = &DefaultPrivilegesForRole_ExplicitRole_{
			ExplicitRole: &DefaultPrivilegesForRole_ExplicitRole{
				UserProto: role.Role.Username.EncodeProto(),
				UserId:    role.Role.UserID,
			},
		}
	}
	return DefaultPrivilegesForRole{
		Role:                       defaultPrivilegesRole,
		DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
	}
}

// RemoveUser looks for a given user in the list and removes it if present.
func (p *DefaultPrivilegeDescriptor) RemoveUser(role DefaultPrivilegesRole) {
	if _, ok := p.DefaultPrivilegesMap[role.Role.UserID]; ok {
		delete(p.DefaultPrivilegesMap, role.Role.UserID)
	}
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
			!defaultPrivilegesForRole.toDefaultPrivilegesRole().
				lessThan(p.DefaultPrivilegesPerRole[i+1].toDefaultPrivilegesRole()) {
			return errors.AssertionFailedf("default privilege list is not sorted")
		}
		for objectType, defaultPrivileges := range defaultPrivilegesForRole.DefaultPrivilegesPerObject {
			privilegeObjectType := objectType.ToPrivilegeObjectType()
			valid, u, remaining := defaultPrivileges.IsValidPrivilegesForObjectType(privilegeObjectType)
			if !valid {
				return errors.AssertionFailedf("user %s must not have %s privileges on %s",
					u.User().Username, privilege.ListFromBitField(remaining, privilege.Any), privilegeObjectType)
			}
		}
	}

	return nil
}

// IsExplicitRole returns if DefaultPrivilegesForRole is defined for
// an explicit role.
func (p DefaultPrivilegesForRole) IsExplicitRole() bool {
	return p.GetExplicitRole() != nil
}
