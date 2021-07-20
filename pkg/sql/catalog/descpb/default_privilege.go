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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var targetObjectToPrivilegeObject = map[tree.AlterDefaultPrivilegesTargetObject]privilege.ObjectType{
	tree.Tables:    privilege.Table,
	tree.Sequences: privilege.Table,
	tree.Schemas:   privilege.Schema,
	tree.Types:     privilege.Type,
}

// GrantDefaultPrivileges grants privileges for the specified users.
func (p *DefaultPrivilegeDescriptor) GrantDefaultPrivileges(
	role security.SQLUsername,
	privileges privilege.List,
	grantees tree.NameList,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivileges := p.findOrCreateUser(role)
	for _, grantee := range grantees {
		defaultPrivilegesPerObject := defaultPrivileges.DefaultPrivilegesPerObject[targetObject]
		defaultPrivilegesPerObject.Grant(
			security.MakeSQLUsernameFromPreNormalizedString(string(grantee)),
			privileges,
		)

		defaultPrivileges.DefaultPrivilegesPerObject[targetObject] = defaultPrivilegesPerObject
	}
}

// RevokeDefaultPrivileges revokes privileges for the specified users.
func (p *DefaultPrivilegeDescriptor) RevokeDefaultPrivileges(
	role security.SQLUsername,
	privileges privilege.List,
	grantees tree.NameList,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	defaultPrivileges := p.findOrCreateUser(role)
	for _, grantee := range grantees {
		defaultPrivilegesPerObject := defaultPrivileges.DefaultPrivilegesPerObject[targetObject]
		defaultPrivilegesPerObject.Revoke(
			security.MakeSQLUsernameFromPreNormalizedString(string(grantee)),
			privileges,
			targetObjectToPrivilegeObject[targetObject],
		)

		defaultPrivileges.DefaultPrivilegesPerObject[targetObject] = defaultPrivilegesPerObject
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

	defaultPrivilegesForRole, found := defaultPrivileges.GetDefaultPrivilegesForRole(user)
	var newPrivs *PrivilegeDescriptor
	if !found {
		newPrivs = NewDefaultPrivilegeDescriptor(user)
	} else {
		defaultPrivs := defaultPrivilegesForRole.DefaultPrivilegesPerObject[targetObject]
		newPrivs = protoutil.Clone(&defaultPrivs).(*PrivilegeDescriptor)
		newPrivs.Grant(security.AdminRoleName(), DefaultSuperuserPrivileges)
		newPrivs.Grant(security.RootUserName(), DefaultSuperuserPrivileges)
		newPrivs.SetOwner(user)
		newPrivs.Version = Version21_2
	}

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

// User accesses the role field.
func (u *DefaultPrivilegesForRole) User() security.SQLUsername {
	return u.UserProto.Decode()
}

// GetDefaultPrivilegesForRole looks for a specific user in the list.
// Returns (nil, false) if not found, or (obj, true) if found.
func (p *DefaultPrivilegeDescriptor) GetDefaultPrivilegesForRole(
	user security.SQLUsername,
) (*DefaultPrivilegesForRole, bool) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		return nil, false
	}
	return &p.DefaultPrivileges[idx], true
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p *DefaultPrivilegeDescriptor) findUserIndex(user security.SQLUsername) int {
	idx := sort.Search(len(p.DefaultPrivileges), func(i int) bool {
		return !p.DefaultPrivileges[i].User().LessThan(user)
	})
	if idx < len(p.DefaultPrivileges) && p.DefaultPrivileges[idx].User() == user {
		return idx
	}
	return -1
}

// findOrCreateUser looks for a specific user in the list, creating it if needed.
// If a new user is created, it must be added in the correct sorted order
// in the list.
func (p *DefaultPrivilegeDescriptor) findOrCreateUser(
	user security.SQLUsername,
) *DefaultPrivilegesForRole {
	idx := sort.Search(len(p.DefaultPrivileges), func(i int) bool {
		return !p.DefaultPrivileges[i].User().LessThan(user)
	})
	if idx == len(p.DefaultPrivileges) {
		// Not found but should be inserted at the end.
		p.DefaultPrivileges = append(p.DefaultPrivileges,
			DefaultPrivilegesForRole{
				UserProto:                  user.EncodeProto(),
				DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
			},
		)
	} else if p.DefaultPrivileges[idx].User() == user {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.DefaultPrivileges = append(p.DefaultPrivileges, DefaultPrivilegesForRole{})
		copy(p.DefaultPrivileges[idx+1:], p.DefaultPrivileges[idx:])
		p.DefaultPrivileges[idx] = DefaultPrivilegesForRole{
			UserProto:                  user.EncodeProto(),
			DefaultPrivilegesPerObject: map[tree.AlterDefaultPrivilegesTargetObject]PrivilegeDescriptor{},
		}
	}
	return &p.DefaultPrivileges[idx]
}

// Validate returns an assertion error if the default privilege descriptor
// is invalid.
func (p *DefaultPrivilegeDescriptor) Validate() error {
	for i, defaultPrivilegesForRole := range p.DefaultPrivileges {
		if i+1 < len(p.DefaultPrivileges) &&
			!defaultPrivilegesForRole.User().LessThan(p.DefaultPrivileges[i+1].User()) {
			return errors.AssertionFailedf("default privilege list is not sorted")
		}
		for objectType, defaultPrivileges := range defaultPrivilegesForRole.DefaultPrivilegesPerObject {
			privilegeObjectType := targetObjectToPrivilegeObject[objectType]
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
		DefaultPrivileges: defaultPrivilegesForRole,
	}
}
