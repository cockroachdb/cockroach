// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

// PrivilegeDescVersion is a custom type for PrivilegeDescriptor versions.
//go:generate stringer -type=PrivilegeDescVersion
type PrivilegeDescVersion uint32

const (
	// InitialVersion corresponds to all descriptors created before 20.1.
	// These descriptors may not have owners explicitly set.
	InitialVersion PrivilegeDescVersion = iota

	// OwnerVersion corresponds to descriptors created 20.2 and onward.
	// These descriptors should always have owner set.
	OwnerVersion

	// Version21_2 corresponds to descriptors created in 21.2 and onwards.
	// These descriptors should have all the correct privileges and the owner field
	// explicitly set. These descriptors should be strictly validated.
	Version21_2
)

// Owner accesses the owner field.
func (p PrivilegeDescriptor) Owner() security.SQLUsername {
	return p.OwnerProto.Decode()
}

// User accesses the owner field.
func (u UserPrivileges) User() security.SQLUsername {
	return u.UserProto.Decode()
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p PrivilegeDescriptor) findUserIndex(user security.SQLUsername) int {
	idx := sort.Search(len(p.Users), func(i int) bool {
		return !p.Users[i].User().LessThan(user)
	})
	if idx < len(p.Users) && p.Users[idx].User() == user {
		return idx
	}
	return -1
}

// FindUser looks for a specific user in the list.
// Returns (nil, false) if not found, or (obj, true) if found.
func (p PrivilegeDescriptor) FindUser(user security.SQLUsername) (*UserPrivileges, bool) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		return nil, false
	}
	return &p.Users[idx], true
}

// FindOrCreateUser looks for a specific user in the list, creating it if needed.
func (p *PrivilegeDescriptor) FindOrCreateUser(user security.SQLUsername) *UserPrivileges {
	idx := sort.Search(len(p.Users), func(i int) bool {
		return !p.Users[i].User().LessThan(user)
	})
	if idx == len(p.Users) {
		// Not found but should be inserted at the end.
		p.Users = append(p.Users, UserPrivileges{UserProto: user.EncodeProto()})
	} else if p.Users[idx].User() == user {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.Users = append(p.Users, UserPrivileges{})
		copy(p.Users[idx+1:], p.Users[idx:])
		p.Users[idx] = UserPrivileges{UserProto: user.EncodeProto()}
	}
	return &p.Users[idx]
}

// RemoveUser looks for a given user in the list and removes it if present.
func (p *PrivilegeDescriptor) RemoveUser(user security.SQLUsername) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		// Not found.
		return
	}
	p.Users = append(p.Users[:idx], p.Users[idx+1:]...)
}

// NewCustomSuperuserPrivilegeDescriptor returns a privilege descriptor for the root user
// and the admin role with specified privileges.
func NewCustomSuperuserPrivilegeDescriptor(
	priv privilege.List, owner security.SQLUsername,
) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		OwnerProto: owner.EncodeProto(),
		Users: []UserPrivileges{
			{
				UserProto:       security.AdminRoleName().EncodeProto(),
				Privileges:      priv.ToBitField(),
				WithGrantOption: priv.ToBitField(),
			},
			{
				UserProto:       security.RootUserName().EncodeProto(),
				Privileges:      priv.ToBitField(),
				WithGrantOption: priv.ToBitField(),
			},
		},
		Version: Version21_2,
	}
}

// NewVirtualTablePrivilegeDescriptor is used to construct a privilege descriptor
// owned by the node user which has SELECT privilege for the public role. It is
// used for virtual tables.
func NewVirtualTablePrivilegeDescriptor() *PrivilegeDescriptor {
	return NewPrivilegeDescriptor(
		security.PublicRoleName(), privilege.List{privilege.SELECT}, privilege.List{}, security.NodeUserName(),
	)
}

// NewVirtualSchemaPrivilegeDescriptor is used to construct a privilege descriptor
// owned by the node user which has USAGE privilege for the public role. It is
// used for virtual schemas.
func NewVirtualSchemaPrivilegeDescriptor() *PrivilegeDescriptor {
	return NewPrivilegeDescriptor(
		security.PublicRoleName(), privilege.List{privilege.USAGE}, privilege.List{}, security.NodeUserName(),
	)
}

// NewTemporarySchemaPrivilegeDescriptor is used to construct a privilege
// descriptor owned by the admin user which has CREATE and USAGE privilege for
// the public role. It is used for temporary schemas.
func NewTemporarySchemaPrivilegeDescriptor() *PrivilegeDescriptor {
	p := NewBasePrivilegeDescriptor(security.AdminRoleName())
	p.Grant(security.PublicRoleName(), privilege.List{privilege.CREATE, privilege.USAGE}, false /* withGrantOption */)
	return p
}

// NewPrivilegeDescriptor returns a privilege descriptor for the given
// user with the specified list of privileges.
func NewPrivilegeDescriptor(
	user security.SQLUsername,
	priv privilege.List,
	grantOption privilege.List,
	owner security.SQLUsername,
) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		OwnerProto: owner.EncodeProto(),
		Users: []UserPrivileges{
			{
				UserProto:       user.EncodeProto(),
				Privileges:      priv.ToBitField(),
				WithGrantOption: grantOption.ToBitField(),
			},
		},
		Version: Version21_2,
	}
}

// DefaultSuperuserPrivileges is the list of privileges for super users
// on non-system objects.
var DefaultSuperuserPrivileges = privilege.List{privilege.ALL}

// NewBasePrivilegeDescriptor returns a privilege descriptor
// with ALL privileges for the root user and admin role.
// NOTE: use NewBaseDatabasePrivilegeDescriptor for databases.
func NewBasePrivilegeDescriptor(owner security.SQLUsername) *PrivilegeDescriptor {
	return NewCustomSuperuserPrivilegeDescriptor(DefaultSuperuserPrivileges, owner)
}

// NewBaseDatabasePrivilegeDescriptor creates defaults privileges for a database.
// Here we also add the CONNECT privilege for the database.
func NewBaseDatabasePrivilegeDescriptor(owner security.SQLUsername) *PrivilegeDescriptor {
	p := NewBasePrivilegeDescriptor(owner)
	p.Grant(security.PublicRoleName(), privilege.List{privilege.CONNECT}, false /* withGrantOption */)
	return p
}

// NewPublicSchemaPrivilegeDescriptor is used to construct a privilege
// descriptor owned by the admin user which has CREATE and USAGE privilege for
// the public role, and ALL privileges for superusers. It is used for the
// public schema.
func NewPublicSchemaPrivilegeDescriptor() *PrivilegeDescriptor {
	// In postgres, the user "postgres" is the owner of the public schema in a
	// newly created db. In CockroachDB, admin is our substitute for the postgres
	// user.
	p := NewBasePrivilegeDescriptor(security.AdminRoleName())
	// By default, everyone has USAGE and CREATE on the public schema.
	// Once https://github.com/cockroachdb/cockroach/issues/70266 is resolved,
	// the public role will no longer have CREATE privileges.
	p.Grant(security.PublicRoleName(), privilege.List{privilege.CREATE, privilege.USAGE}, false)
	return p
}

// CheckGrantOptions returns false if the user tries to grant a privilege that
// it does not possess grant options for
func (p *PrivilegeDescriptor) CheckGrantOptions(
	user security.SQLUsername, privList privilege.List,
) bool {
	userPriv, exists := p.FindUser(user)
	if !exists {
		return false
	}

	// User has ALL WITH GRANT OPTION so they can grant anything.
	if privilege.ALL.IsSetIn(userPriv.WithGrantOption) {
		return true
	}

	for _, priv := range privList {
		if userPriv.WithGrantOption&priv.Mask() == 0 {
			return false
		}
	}

	return true
}

// Grant adds new privileges to this descriptor for a given list of users.
func (p *PrivilegeDescriptor) Grant(
	user security.SQLUsername, privList privilege.List, withGrantOption bool,
) {
	userPriv := p.FindOrCreateUser(user)
	if privilege.ALL.IsSetIn(userPriv.WithGrantOption) && privilege.ALL.IsSetIn(userPriv.Privileges) {
		// User already has 'ALL' privilege: no-op.
		// If userPriv.WithGrantOption has ALL, then userPriv.Privileges must also have ALL.
		// It is possible however for userPriv.Privileges to have ALL but userPriv.WithGrantOption to not have ALL
		return
	}

	if privilege.ALL.IsSetIn(userPriv.Privileges) && !withGrantOption {
		// A user can hold all privileges but not all grant options.
		// If a user holds all privileges but withGrantOption is False,
		// there is nothing left to be done
		return
	}

	bits := privList.ToBitField()
	if privilege.ALL.IsSetIn(bits) {
		// Granting 'ALL' privilege: overwrite.
		// TODO(marc): the grammar does not allow it, but we should
		// check if other privileges are being specified and error out.
		userPriv.Privileges = privilege.ALL.Mask()
		if withGrantOption {
			userPriv.WithGrantOption = privilege.ALL.Mask()
		}
		return
	}

	if withGrantOption {
		userPriv.WithGrantOption |= bits
	}
	userPriv.Privileges |= bits
}

// Revoke removes privileges from this descriptor for a given list of users.
func (p *PrivilegeDescriptor) Revoke(
	user security.SQLUsername,
	privList privilege.List,
	objectType privilege.ObjectType,
	grantOptionFor bool,
) {
	userPriv, ok := p.FindUser(user)
	if !ok || userPriv.Privileges == 0 {
		// Removing privileges from a user without privileges is a no-op.
		return
	}

	bits := privList.ToBitField()
	if privilege.ALL.IsSetIn(bits) {
		userPriv.WithGrantOption = 0
		if !grantOptionFor {
			// Revoking 'ALL' privilege: remove user.
			// TODO(marc): the grammar does not allow it, but we should
			// check if other privileges are being specified and error out.
			p.RemoveUser(user)
		} else if privilege.ALL.IsSetIn(userPriv.Privileges) {
			// fold sub-privileges into ALL
			userPriv.Privileges = privilege.ALL.Mask()
		}
		return
	}

	if privilege.ALL.IsSetIn(userPriv.Privileges) && !grantOptionFor {
		// User has 'ALL' privilege. Remove it and set
		// all other privileges one.
		validPrivs := privilege.GetValidPrivilegesForObject(objectType)
		userPriv.Privileges = 0
		for _, v := range validPrivs {
			if v != privilege.ALL {
				userPriv.Privileges |= v.Mask()
			}
		}
	}

	// We will always revoke the grant options regardless of the flag.
	if privilege.ALL.IsSetIn(userPriv.WithGrantOption) {
		// User has 'ALL' grant option. Remove it and set
		// all other grant options to one.
		validPrivs := privilege.GetValidPrivilegesForObject(objectType)
		userPriv.WithGrantOption = 0
		for _, v := range validPrivs {
			if v != privilege.ALL {
				userPriv.WithGrantOption |= v.Mask()
			}
		}
	}

	// One doesn't see "AND NOT" very often.
	// We will always revoke the grant options regardless of the flag.
	userPriv.WithGrantOption &^= bits
	if !grantOptionFor {
		userPriv.Privileges &^= bits

		if userPriv.Privileges == 0 {
			p.RemoveUser(user)
		}
	}
}

// GrantPrivilegeToGrantOptions adjusts a user's grant option bits based on whether the GRANT or ALL
// privilege was just granted or revoked. If GRANT/ALL was just granted, the user should obtain grant
// options for each privilege it currently has. If GRANT/ALL was just revoked, the user should lose
// grant options for each privilege it has
// TODO(jackcwu): delete this function once the GRANT privilege is finally removed
func (p *PrivilegeDescriptor) GrantPrivilegeToGrantOptions(
	user security.SQLUsername, isGrant bool,
) {
	if isGrant {
		userPriv := p.FindOrCreateUser(user)
		userPriv.WithGrantOption = userPriv.Privileges
	} else {
		userPriv, ok := p.FindUser(user)
		if !ok || userPriv.Privileges == 0 {
			// Removing privileges from a user without privileges is a no-op.
			return
		}
		userPriv.WithGrantOption = 0
	}
}

// ValidateSuperuserPrivileges ensures that superusers have exactly the maximum
// allowed privilege set for the object.
// It requires the ID of the descriptor it is applied on to determine whether
// it is is a system descriptor, because superusers do not always have full
// privileges for those.
// It requires the objectType to determine the superset of privileges allowed
// for regular users.
func (p PrivilegeDescriptor) ValidateSuperuserPrivileges(
	parentID catid.DescID,
	objectType privilege.ObjectType,
	objectName string,
	allowedSuperuserPrivileges privilege.List,
) error {
	if parentID == catid.InvalidDescID && objectType != privilege.Database {
		// Special case for virtual objects.
		return nil
	}
	for _, user := range []security.SQLUsername{
		// Check "root" user.
		security.RootUserName(),
		// We expect an "admin" role. Check that it has desired superuser permissions.
		security.AdminRoleName(),
	} {
		superPriv, ok := p.FindUser(user)
		if !ok {
			return fmt.Errorf(
				"user %s does not have privileges over %s",
				user,
				privilegeObject(parentID, objectType, objectName),
			)
		}

		// The super users must match the allowed privilege set exactly.
		if superPriv.Privileges != allowedSuperuserPrivileges.ToBitField() {
			return fmt.Errorf(
				"user %s must have exactly %s privileges on %s",
				user,
				allowedSuperuserPrivileges,
				privilegeObject(parentID, objectType, objectName),
			)
		}
	}
	return nil
}

// Validate returns an assertion error if the privilege descriptor is invalid.
func (p PrivilegeDescriptor) Validate(
	parentID catid.DescID,
	objectType privilege.ObjectType,
	objectName string,
	allowedSuperuserPrivileges privilege.List,
) error {
	if err := p.ValidateSuperuserPrivileges(parentID, objectType, objectName, allowedSuperuserPrivileges); err != nil {
		return errors.HandleAsAssertionFailure(err)
	}

	if p.Version >= OwnerVersion {
		if p.Owner().Undefined() {
			return errors.AssertionFailedf("found no owner for %s", privilegeObject(parentID, objectType, objectName))
		}
	}

	valid, u, remaining := p.IsValidPrivilegesForObjectType(objectType)
	if !valid {
		return errors.AssertionFailedf(
			"user %s must not have %s privileges on %s",
			u.User(),
			privilege.ListFromBitField(remaining, privilege.Any),
			privilegeObject(parentID, objectType, objectName),
		)
	}

	return nil
}

// IsValidPrivilegesForObjectType checks if the privileges on the descriptor
// is valid for the given object type.
// If the privileges are invalid, it returns false along with the first user
// found to have invalid privileges and the bits representing the invalid
// privileges.
func (p PrivilegeDescriptor) IsValidPrivilegesForObjectType(
	objectType privilege.ObjectType,
) (bool, UserPrivileges, uint32) {
	allowedPrivilegesBits := privilege.GetValidPrivilegesForObject(objectType).ToBitField()

	// Validate can be called during the fix_privileges_migration introduced in
	// 21.2. It is possible for have invalid privileges prior to 21.2 in certain
	// cases due to bugs. We can strictly check privileges in 21.2 and onwards.
	if p.Version < Version21_2 {
		if objectType == privilege.Schema {
			// Prior to 21_2, it was possible for a schema to have some database
			// privileges on it. This was temporarily fixed by an upgrade on read
			// but in 21.2 onwards, it should be permanently fixed with a migration.
			allowedPrivilegesBits |= privilege.GetValidPrivilegesForObject(privilege.Database).ToBitField()
		}
		if objectType == privilege.Table || objectType == privilege.Database {
			// Prior to 21_2, it was possible for a table or database to have USAGE
			// privilege on it due to a bug when upgrading from 20.1 to 20.2.
			// In 21.2 onwards, it should be permanently fixed with a migration.
			allowedPrivilegesBits |= privilege.USAGE.Mask()
		}
	}

	// For all non-super users, privileges must not exceed the allowed privileges.
	// Also the privileges must be valid on the object type.
	for _, u := range p.Users {
		if u.User().IsRootUser() || u.User().IsAdminRole() {
			// We've already checked super users.
			continue
		}

		if remaining := u.Privileges &^ allowedPrivilegesBits; remaining != 0 {
			return false, u, remaining
		}
	}

	return true, UserPrivileges{}, 0
}

// UserPrivilege represents a User and its Privileges
type UserPrivilege struct {
	User       security.SQLUsername
	Privileges []privilege.Privilege
}

// Show returns the list of {username, privileges} sorted by username.
// 'privileges' is a string of comma-separated sorted privilege names.
func (p PrivilegeDescriptor) Show(objectType privilege.ObjectType) []UserPrivilege {
	ret := make([]UserPrivilege, 0, len(p.Users))
	for _, userPriv := range p.Users {
		privileges := privilege.PrivilegesFromBitFields(userPriv.Privileges, userPriv.WithGrantOption, objectType)
		sort.Slice(privileges, func(i, j int) bool {
			return strings.Compare(privileges[i].Kind.String(), privileges[j].Kind.String()) < 0
		})
		ret = append(ret, UserPrivilege{
			User:       userPriv.User(),
			Privileges: privileges,
		})
	}
	return ret
}

// CheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
func (p PrivilegeDescriptor) CheckPrivilege(user security.SQLUsername, priv privilege.Kind) bool {
	userPriv, ok := p.FindUser(user)
	if !ok {
		// User "node" has all privileges.
		return user.IsNodeUser()
	}

	if privilege.ALL.IsSetIn(userPriv.Privileges) {
		return true
	}
	return priv.IsSetIn(userPriv.Privileges)
}

// AnyPrivilege returns true if 'user' has any privilege on this descriptor.
func (p PrivilegeDescriptor) AnyPrivilege(user security.SQLUsername) bool {
	if p.Owner() == user {
		return true
	}
	userPriv, ok := p.FindUser(user)
	if !ok {
		return false
	}
	return userPriv.Privileges != 0
}

// HasAllPrivileges returns whether the user has ALL privileges either through
// ALL or having every privilege possible on the object.
func (p PrivilegeDescriptor) HasAllPrivileges(
	user security.SQLUsername, objectType privilege.ObjectType,
) bool {
	if p.CheckPrivilege(user, privilege.ALL) {
		return true
	}
	// If ALL is not set, check if all other privileges would add up to all.
	validPrivileges := privilege.GetValidPrivilegesForObject(objectType)
	for _, priv := range validPrivileges {
		if priv == privilege.ALL {
			continue
		}
		if !p.CheckPrivilege(user, priv) {
			return false
		}
	}

	return true
}

// SetOwner sets the owner of the privilege descriptor to the provided string.
func (p *PrivilegeDescriptor) SetOwner(owner security.SQLUsername) {
	p.OwnerProto = owner.EncodeProto()
}

// SetVersion sets the version of the privilege descriptor.
func (p *PrivilegeDescriptor) SetVersion(version PrivilegeDescVersion) {
	p.Version = version
}

// privilegeObject is a helper function for privilege errors.
func privilegeObject(
	parentID catid.DescID, objectType privilege.ObjectType, objectName string,
) string {
	if parentID == keys.SystemDatabaseID ||
		(parentID == catid.InvalidDescID && objectName == catconstants.SystemDatabaseName) {
		return fmt.Sprintf("system %s %q", objectType, objectName)
	}
	return fmt.Sprintf("%s %q", objectType, objectName)
}
