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
	var defaultPrivilegeRole isDefaultPrivilegesForRole_Role
	if role.ForAllRoles {
		defaultPrivilegeRole = &DefaultPrivilegesForRole_ForAllRoles{ForAllRoles: true}
	} else {
		defaultPrivilegeRole = &DefaultPrivilegesForRole_UserProto{UserProto: role.Role.EncodeProto()}
	}
	if idx == len(p.DefaultPrivilegesPerRole) {
		// Not found but should be inserted at the end.
		p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole,
			DefaultPrivilegesForRole{
				Role:                       defaultPrivilegeRole,
				DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
			},
		)
	} else if p.DefaultPrivilegesPerRole[idx].ToDefaultPrivilegesRole() == role {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.DefaultPrivilegesPerRole = append(p.DefaultPrivilegesPerRole, DefaultPrivilegesForRole{})
		copy(p.DefaultPrivilegesPerRole[idx+1:], p.DefaultPrivilegesPerRole[idx:])
		p.DefaultPrivilegesPerRole[idx] = DefaultPrivilegesForRole{
			Role:                       defaultPrivilegeRole,
			DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
		}
	}
	return &p.DefaultPrivilegesPerRole[idx]
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
