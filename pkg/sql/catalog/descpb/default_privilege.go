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
		defaultPrivilegesPerObject := defaultPrivileges.DefaultPrivilegesPerObject[targetObject.ToUInt32()]
		defaultPrivilegesPerObject.Grant(
			security.MakeSQLUsernameFromPreNormalizedString(string(grantee)),
			privileges,
		)

		defaultPrivileges.DefaultPrivilegesPerObject[targetObject.ToUInt32()] = defaultPrivilegesPerObject
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
		defaultPrivilegesPerObject := defaultPrivileges.DefaultPrivilegesPerObject[uint32(targetObject)]
		defaultPrivilegesPerObject.Revoke(
			security.MakeSQLUsernameFromPreNormalizedString(string(grantee)),
			privileges,
			targetObjectToPrivilegeObject[targetObject],
		)

		defaultPrivileges.DefaultPrivilegesPerObject[targetObject.ToUInt32()] = defaultPrivilegesPerObject
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

	defaultPrivilegesForRole, found := defaultPrivileges.GetDefaultPrivilegesForRole(user)
	var newPrivs *PrivilegeDescriptor
	if !found {
		newPrivs = NewDefaultPrivilegeDescriptor(user)
	} else {
		defaultPrivs := defaultPrivilegesForRole.DefaultPrivilegesPerObject[targetObject.ToUInt32()]
		newPrivs = protoutil.Clone(&defaultPrivs).(*PrivilegeDescriptor)
		newPrivs.Grant(security.AdminRoleName(), DefaultSuperuserPrivileges)
		newPrivs.Grant(security.RootUserName(), DefaultSuperuserPrivileges)
		newPrivs.SetOwner(user)
		newPrivs.Version = OwnerVersion
	}
	// TODO (richardjcai): We also have to handle the "public role" for types.
	//   "Public" has USAGE by default but it can be revoked.
	//    We need to use a Version field to determine what a missing Public role
	//    entry in the DefaultPrivileges map means.

	// TODO(richardjcai): Remove this depending on how we handle the migration.
	//   For backwards compatibility, also "inherit" privileges from the dbDesc.
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
func (u DefaultPrivilegesForRole) User() security.SQLUsername {
	return u.UserProto.Decode()
}

// findUser looks for a specific user in the list.
// Returns (nil, false) if not found, or (obj, true) if found.
func (p DefaultPrivilegeDescriptor) GetDefaultPrivilegesForRole(
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
func (p DefaultPrivilegeDescriptor) findUserIndex(user security.SQLUsername) int {
	idx := sort.Search(len(p.DefaultPrivileges), func(i int) bool {
		return !p.DefaultPrivileges[i].User().LessThan(user)
	})
	if idx < len(p.DefaultPrivileges) && p.DefaultPrivileges[idx].User() == user {
		return idx
	}
	return -1
}

// findOrCreateUser looks for a specific user in the list, creating it if needed.
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
				DefaultPrivilegesPerObject: map[uint32]PrivilegeDescriptor{},
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
			DefaultPrivilegesPerObject: map[uint32]PrivilegeDescriptor{},
		}
	}
	return &p.DefaultPrivileges[idx]
}
