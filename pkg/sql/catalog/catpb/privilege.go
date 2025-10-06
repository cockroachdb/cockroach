// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

// PrivilegeDescVersion is a custom type for PrivilegeDescriptor versions.
//
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

	// Version23_2 corresponds to descriptors created in 23.2 and onwards. These
	// descriptors that are function descriptions should have EXECUTE privileges
	// for the public role.
	Version23_2
)

// Owner accesses the owner field.
func (p PrivilegeDescriptor) Owner() username.SQLUsername {
	return p.OwnerProto.Decode()
}

// User accesses the owner field.
func (u UserPrivileges) User() username.SQLUsername {
	return u.UserProto.Decode()
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p PrivilegeDescriptor) findUserIndex(user username.SQLUsername) int {
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
func (p PrivilegeDescriptor) FindUser(user username.SQLUsername) (*UserPrivileges, bool) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		return nil, false
	}
	return &p.Users[idx], true
}

// FindOrCreateUser looks for a specific user in the list, creating it if needed.
func (p *PrivilegeDescriptor) FindOrCreateUser(user username.SQLUsername) *UserPrivileges {
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
func (p *PrivilegeDescriptor) RemoveUser(user username.SQLUsername) {
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
	priv privilege.List, owner username.SQLUsername,
) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		OwnerProto: owner.EncodeProto(),
		Users: []UserPrivileges{
			{
				UserProto:       username.AdminRoleName().EncodeProto(),
				Privileges:      priv.ToBitField(),
				WithGrantOption: priv.ToBitField(),
			},
			{
				UserProto:       username.RootUserName().EncodeProto(),
				Privileges:      priv.ToBitField(),
				WithGrantOption: priv.ToBitField(),
			},
		},
		Version: Version23_2,
	}
}

// NewVirtualSchemaPrivilegeDescriptor is used to construct a privilege descriptor
// owned by the node user which has USAGE privilege for the public role. It is
// used for virtual schemas.
func NewVirtualSchemaPrivilegeDescriptor() *PrivilegeDescriptor {
	return NewPrivilegeDescriptor(
		username.PublicRoleName(), privilege.List{privilege.USAGE}, privilege.List{}, username.NodeUserName(),
	)
}

// NewTemporarySchemaPrivilegeDescriptor is used to construct a privilege
// descriptor owned by the admin user which has CREATE and USAGE privilege for
// the public role. It is used for temporary schemas.
func NewTemporarySchemaPrivilegeDescriptor() *PrivilegeDescriptor {
	p := NewBasePrivilegeDescriptor(username.AdminRoleName())
	p.Grant(username.PublicRoleName(), privilege.List{privilege.CREATE, privilege.USAGE}, false /* withGrantOption */)
	return p
}

// NewPrivilegeDescriptor returns a privilege descriptor for the given
// user with the specified list of privileges.
func NewPrivilegeDescriptor(
	user username.SQLUsername,
	priv privilege.List,
	grantOption privilege.List,
	owner username.SQLUsername,
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
		Version: Version23_2,
	}
}

// DefaultSuperuserPrivileges is the list of privileges for super users
// on non-system objects.
var DefaultSuperuserPrivileges = privilege.List{privilege.ALL}

// NewBasePrivilegeDescriptor returns a privilege descriptor
// with ALL privileges for the root user and admin role.
// NOTE: use NewBaseDatabasePrivilegeDescriptor for databases.
func NewBasePrivilegeDescriptor(owner username.SQLUsername) *PrivilegeDescriptor {
	return NewCustomSuperuserPrivilegeDescriptor(DefaultSuperuserPrivileges, owner)
}

// NewBaseDatabasePrivilegeDescriptor creates defaults privileges for a database.
// Here we also add the CONNECT privilege for the database.
func NewBaseDatabasePrivilegeDescriptor(owner username.SQLUsername) *PrivilegeDescriptor {
	p := NewBasePrivilegeDescriptor(owner)
	p.Grant(username.PublicRoleName(), privilege.List{privilege.CONNECT}, false /* withGrantOption */)
	return p
}

// NewPublicSchemaPrivilegeDescriptor is used to construct a privilege
// descriptor owned by the given user which has USAGE privilege, and optionally
// CREATE privilege, for the public role, and ALL privileges for superusers. It
// is used for the public schema.
func NewPublicSchemaPrivilegeDescriptor(
	owner username.SQLUsername, includeCreatePriv bool,
) *PrivilegeDescriptor {
	p := NewBasePrivilegeDescriptor(owner)
	if includeCreatePriv {
		p.Grant(username.PublicRoleName(), privilege.List{privilege.CREATE, privilege.USAGE}, false)
	} else {
		p.Grant(username.PublicRoleName(), privilege.List{privilege.USAGE}, false)
	}
	return p
}

// NewBaseFunctionPrivilegeDescriptor returns a privilege descriptor
// with default privileges for a function descriptor.
func NewBaseFunctionPrivilegeDescriptor(owner username.SQLUsername) *PrivilegeDescriptor {
	p := NewBasePrivilegeDescriptor(owner)
	p.Grant(username.PublicRoleName(), privilege.List{privilege.EXECUTE}, false)
	return p
}

// CheckGrantOptions returns false if the user tries to grant a privilege that
// it does not possess grant options for
func (p *PrivilegeDescriptor) CheckGrantOptions(
	user username.SQLUsername, privList privilege.List,
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
	user username.SQLUsername, privList privilege.List, withGrantOption bool,
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
	user username.SQLUsername,
	privList privilege.List,
	objectType privilege.ObjectType,
	grantOptionFor bool,
) error {
	userPriv, ok := p.FindUser(user)
	if !ok || userPriv.Privileges == 0 {
		// Removing privileges from a user without privileges is a no-op.
		return nil
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
		return nil
	}

	if privilege.ALL.IsSetIn(userPriv.Privileges) && !grantOptionFor {
		// User has 'ALL' privilege. Remove it and set
		// all other privileges one.
		validPrivs, err := privilege.GetValidPrivilegesForObject(objectType)
		if err != nil {
			return err
		}
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
		validPrivs, err := privilege.GetValidPrivilegesForObject(objectType)
		if err != nil {
			return err
		}
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
	return nil
}

// ValidateSuperuserPrivileges ensures that superusers have exactly the maximum
// allowed privilege set for the object.
// It requires the ID of the descriptor it is applied on to determine whether it
// is a system descriptor, because superusers do not always have full privileges
// for those.
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
	for _, user := range []username.SQLUsername{
		// Check "root" user.
		username.RootUserName(),
		// We expect an "admin" role. Check that it has desired superuser permissions.
		username.AdminRoleName(),
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
				allowedSuperuserPrivileges.SortedDisplayNames(),
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

	valid, u, remaining, err := p.IsValidPrivilegesForObjectType(objectType)
	if err != nil {
		return err
	}
	if !valid {
		privList, err := privilege.ListFromBitField(remaining, privilege.Any)
		if err != nil {
			return err
		}
		return errors.AssertionFailedf(
			"user %s must not have %s privileges on %s",
			u.User(),
			privList.SortedDisplayNames(),
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
) (bool, UserPrivileges, uint64, error) {
	validPrivs, err := privilege.GetValidPrivilegesForObject(objectType)
	if err != nil {
		return false, UserPrivileges{}, 0, err
	}
	allowedPrivilegesBits := validPrivs.ToBitField()

	// For all non-super users, privileges must not exceed the allowed privileges.
	// Also the privileges must be valid on the object type.
	for _, u := range p.Users {
		if u.User().IsRootUser() || u.User().IsAdminRole() {
			// We've already checked super users.
			continue
		}

		if remaining := u.Privileges &^ allowedPrivilegesBits; remaining != 0 {
			return false, u, remaining, nil
		}
	}

	return true, UserPrivileges{}, 0, nil
}

// UserPrivilege represents a User and its Privileges
type UserPrivilege struct {
	User       username.SQLUsername
	Privileges []privilege.Privilege
}

// Show returns the list of {username, privileges} sorted by username.
// 'privileges' is a string of comma-separated sorted privilege names.
// The owner always implicitly receives ALL privileges with the GRANT OPTION. If
// showImplicitOwnerPrivs is true, then that implicit privilege is shown here.
// Otherwise, only the privileges that were explicitly granted to the owner are
// shown.
func (p PrivilegeDescriptor) Show(
	objectType privilege.ObjectType, showImplicitOwnerPrivs bool,
) ([]UserPrivilege, error) {
	ret := make([]UserPrivilege, 0, len(p.Users))
	sawOwner := false
	for _, userPriv := range p.Users {
		privBits := userPriv.Privileges
		grantOptionBits := userPriv.WithGrantOption
		if userPriv.User() == p.Owner() {
			sawOwner = true
			if showImplicitOwnerPrivs {
				privBits = privilege.ALL.Mask()
				grantOptionBits = privilege.ALL.Mask()
			}
		}
		privileges, err := privilege.PrivilegesFromBitFields(privBits, grantOptionBits, objectType)
		if err != nil {
			return nil, err
		}
		sort.Slice(privileges, func(i, j int) bool {
			return privileges[i].Kind.DisplayName() < privileges[j].Kind.DisplayName()
		})
		ret = append(ret, UserPrivilege{
			User:       userPriv.User(),
			Privileges: privileges,
		})
	}

	// The node user owns system tables, but since it's just an internal reserved
	// name, we don't show node's privileges here.
	if showImplicitOwnerPrivs && !sawOwner && !p.Owner().IsNodeUser() && !p.Owner().Undefined() {
		ret = append(ret, UserPrivilege{
			User:       p.Owner(),
			Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}},
		})
	}
	return ret, nil
}

// CheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
func (p PrivilegeDescriptor) CheckPrivilege(user username.SQLUsername, priv privilege.Kind) bool {
	if p.Owner() == user {
		return true
	}
	userPriv, ok := p.FindUser(user)
	if !ok {
		// User "node" has all privileges.
		return user.IsNodeUser()
	}

	if privilege.ALL.IsSetIn(userPriv.Privileges) && priv != privilege.NOSQLLOGIN {
		// Since NOSQLLOGIN is a "negative" privilege, it's ignored for the ALL
		// check. It's poor UX for someone with ALL privileges to not be able to
		// log in.
		return true
	}
	return priv.IsSetIn(userPriv.Privileges)
}

// AnyPrivilege returns true if 'user' has any privilege on this descriptor.
func (p PrivilegeDescriptor) AnyPrivilege(user username.SQLUsername) bool {
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
	user username.SQLUsername, objectType privilege.ObjectType,
) (bool, error) {
	if p.CheckPrivilege(user, privilege.ALL) {
		return true, nil
	}
	// If ALL is not set, check if all other privileges would add up to all.
	validPrivileges, err := privilege.GetValidPrivilegesForObject(objectType)
	if err != nil {
		return false, err
	}
	for _, priv := range validPrivileges {
		if priv == privilege.ALL {
			continue
		}
		if !p.CheckPrivilege(user, priv) {
			return false, nil
		}
	}

	return true, nil
}

// SetOwner sets the owner of the privilege descriptor to the provided string.
func (p *PrivilegeDescriptor) SetOwner(owner username.SQLUsername) {
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
