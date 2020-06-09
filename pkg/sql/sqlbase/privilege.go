// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

func isPrivilegeSet(bits uint32, priv privilege.Kind) bool {
	return bits&priv.Mask() != 0
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (p PrivilegeDescriptor) findUserIndex(user string) int {
	idx := sort.Search(len(p.Users), func(i int) bool {
		return p.Users[i].User >= user
	})
	if idx < len(p.Users) && p.Users[idx].User == user {
		return idx
	}
	return -1
}

// findUser looks for a specific user in the list.
// Returns (nil, false) if not found, or (obj, true) if found.
func (p PrivilegeDescriptor) findUser(user string) (*UserPrivileges, bool) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		return nil, false
	}
	return &p.Users[idx], true
}

// findOrCreateUser looks for a specific user in the list, creating it if needed.
func (p *PrivilegeDescriptor) findOrCreateUser(user string) *UserPrivileges {
	idx := sort.Search(len(p.Users), func(i int) bool {
		return p.Users[i].User >= user
	})
	if idx == len(p.Users) {
		// Not found but should be inserted at the end.
		p.Users = append(p.Users, UserPrivileges{User: user})
	} else if p.Users[idx].User == user {
		// Found.
	} else {
		// New element to be inserted at idx.
		p.Users = append(p.Users, UserPrivileges{})
		copy(p.Users[idx+1:], p.Users[idx:])
		p.Users[idx] = UserPrivileges{User: user}
	}
	return &p.Users[idx]
}

// removeUser looks for a given user in the list and removes it if present.
func (p *PrivilegeDescriptor) removeUser(user string) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		// Not found.
		return
	}
	p.Users = append(p.Users[:idx], p.Users[idx+1:]...)
}

// NewCustomSuperuserPrivilegeDescriptor returns a privilege descriptor for the root user
// and the admin role with specified privileges.
func NewCustomSuperuserPrivilegeDescriptor(priv privilege.List) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		Users: []UserPrivileges{
			{
				User:       AdminRole,
				Privileges: priv.ToBitField(),
			},
			{
				User:       security.RootUser,
				Privileges: priv.ToBitField(),
			},
		},
	}
}

// NewPrivilegeDescriptor returns a privilege descriptor for the given
// user with the specified list of privileges.
func NewPrivilegeDescriptor(user string, priv privilege.List) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		Users: []UserPrivileges{
			{
				User:       user,
				Privileges: priv.ToBitField(),
			},
		},
	}
}

// DefaultSuperuserPrivileges is the list of privileges for super users
// on non-system objects.
var DefaultSuperuserPrivileges = privilege.List{privilege.ALL}

// NewDefaultPrivilegeDescriptor returns a privilege descriptor
// with ALL privileges for the root user and admin role.
func NewDefaultPrivilegeDescriptor() *PrivilegeDescriptor {
	return NewCustomSuperuserPrivilegeDescriptor(DefaultSuperuserPrivileges)
}

// Grant adds new privileges to this descriptor for a given list of users.
// TODO(marc): if all privileges other than ALL are set, should we collapse
// them into ALL?
func (p *PrivilegeDescriptor) Grant(user string, privList privilege.List) {
	userPriv := p.findOrCreateUser(user)
	if isPrivilegeSet(userPriv.Privileges, privilege.ALL) {
		// User already has 'ALL' privilege: no-op.
		return
	}

	bits := privList.ToBitField()
	if isPrivilegeSet(bits, privilege.ALL) {
		// Granting 'ALL' privilege: overwrite.
		// TODO(marc): the grammar does not allow it, but we should
		// check if other privileges are being specified and error out.
		userPriv.Privileges = privilege.ALL.Mask()
		return
	}
	userPriv.Privileges |= bits
}

// Revoke removes privileges from this descriptor for a given list of users.
func (p *PrivilegeDescriptor) Revoke(user string, privList privilege.List) {
	userPriv, ok := p.findUser(user)
	if !ok || userPriv.Privileges == 0 {
		// Removing privileges from a user without privileges is a no-op.
		return
	}

	bits := privList.ToBitField()
	if isPrivilegeSet(bits, privilege.ALL) {
		// Revoking 'ALL' privilege: remove user.
		// TODO(marc): the grammar does not allow it, but we should
		// check if other privileges are being specified and error out.
		p.removeUser(user)
		return
	}

	if isPrivilegeSet(userPriv.Privileges, privilege.ALL) {
		// User has 'ALL' privilege. Remove it and set
		// all other privileges one.
		userPriv.Privileges = 0
		for _, v := range privilege.ByValue {
			if v != privilege.ALL {
				userPriv.Privileges |= v.Mask()
			}
		}
	}

	// One doesn't see "AND NOT" very often.
	userPriv.Privileges &^= bits

	if userPriv.Privileges == 0 {
		p.removeUser(user)
	}
}

// MaybeFixPrivileges fixes the privilege descriptor if needed, including:
// * adding default privileges for the "admin" role
// * fixing default privileges for the "root" user
// * fixing maximum privileges for users.
// Returns true if the privilege descriptor was modified.
//
// TODO(ajwerner): Figure out whether this is still needed. It seems like
// perhaps it was intended only for the 2.0 release but then somehow we got
// bad descriptors with bad initial permissions into later versions or we didn't
// properly bake this migration in.
func (p *PrivilegeDescriptor) MaybeFixPrivileges(id ID) bool {
	allowedPrivilegesBits := privilege.ALL.Mask()
	if IsReservedID(id) {
		// System databases and tables have custom maximum allowed privileges.
		allowedPrivilegesBits = SystemAllowedPrivileges[id].ToBitField()
	}

	var modified bool

	fixSuperUser := func(user string) {
		privs := p.findOrCreateUser(user)
		if privs.Privileges != allowedPrivilegesBits {
			privs.Privileges = allowedPrivilegesBits
			modified = true
		}
	}

	// Check "root" user and "admin" role.
	fixSuperUser(security.RootUser)
	fixSuperUser(AdminRole)

	if isPrivilegeSet(allowedPrivilegesBits, privilege.ALL) {
		// ALL privileges allowed, we can skip regular users.
		return modified
	}

	for i := range p.Users {
		// Users is a slice of values, we need pointers to make them mutable.
		u := &p.Users[i]
		if u.User == security.RootUser || u.User == AdminRole {
			// we've already checked super users.
			continue
		}

		if (u.Privileges &^ allowedPrivilegesBits) != 0 {
			// User has disallowed privileges: bitwise AND with allowed privileges.
			u.Privileges &= allowedPrivilegesBits
			modified = true
		}
	}

	return modified
}

// Validate is called when writing a database or table descriptor.
// It takes the descriptor ID which is used to determine if
// it belongs to a system descriptor, in which case the maximum
// set of allowed privileges is looked up and applied.
func (p PrivilegeDescriptor) Validate(id ID) error {
	allowedPrivileges := privilege.List{privilege.ALL}
	if IsReservedID(id) {
		var ok bool
		allowedPrivileges, ok = SystemAllowedPrivileges[id]
		if !ok {
			return fmt.Errorf("no allowed privileges found for system object with ID=%d", id)
		}
	}

	// Check "root" user.
	if err := p.validateRequiredSuperuser(id, allowedPrivileges, security.RootUser); err != nil {
		return err
	}

	// We expect an "admin" role. Check that it has desired superuser permissions.
	if err := p.validateRequiredSuperuser(id, allowedPrivileges, AdminRole); err != nil {
		return err
	}

	allowedPrivilegesBits := allowedPrivileges.ToBitField()
	if isPrivilegeSet(allowedPrivilegesBits, privilege.ALL) {
		// ALL privileges allowed, we can skip regular users.
		return nil
	}

	// For all non-super users, privileges must not exceed the allowed privileges.
	for _, u := range p.Users {
		if u.User == security.RootUser || u.User == AdminRole {
			// We've already checked super users.
			continue
		}

		if remaining := u.Privileges &^ allowedPrivilegesBits; remaining != 0 {
			return fmt.Errorf("user %s must not have %s privileges on system object with ID=%d",
				u.User, privilege.ListFromBitField(remaining), id)
		}
	}

	return nil
}

func (p PrivilegeDescriptor) validateRequiredSuperuser(
	id ID, allowedPrivileges privilege.List, user string,
) error {
	superPriv, ok := p.findUser(user)
	if !ok {
		return fmt.Errorf("user %s does not have privileges over system object with ID=%d", user, id)
	}

	// The super users must match the allowed privilege set exactly.
	if superPriv.Privileges != allowedPrivileges.ToBitField() {
		return fmt.Errorf("user %s must have exactly %s privileges on system object with ID=%d",
			user, allowedPrivileges, id)
	}

	return nil
}

// UserPrivilegeString is a pair of strings describing the
// privileges for a given user.
type UserPrivilegeString struct {
	User       string
	Privileges []string
}

// PrivilegeString returns a string of comma-separted privilege names.
func (u UserPrivilegeString) PrivilegeString() string {
	return strings.Join(u.Privileges, ",")
}

// Show returns the list of {username, privileges} sorted by username.
// 'privileges' is a string of comma-separated sorted privilege names.
func (p PrivilegeDescriptor) Show() []UserPrivilegeString {
	ret := make([]UserPrivilegeString, 0, len(p.Users))
	for _, userPriv := range p.Users {
		ret = append(ret, UserPrivilegeString{
			User:       userPriv.User,
			Privileges: privilege.ListFromBitField(userPriv.Privileges).SortedNames(),
		})
	}
	return ret
}

// CheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
func (p PrivilegeDescriptor) CheckPrivilege(user string, priv privilege.Kind) bool {
	userPriv, ok := p.findUser(user)
	if !ok {
		// User "node" has all privileges.
		return user == security.NodeUser
	}
	// ALL is always good.
	if isPrivilegeSet(userPriv.Privileges, privilege.ALL) {
		return true
	}
	return isPrivilegeSet(userPriv.Privileges, priv)
}

// AnyPrivilege returns true if 'user' has any privilege on this descriptor.
func (p PrivilegeDescriptor) AnyPrivilege(user string) bool {
	userPriv, ok := p.findUser(user)
	if !ok {
		return false
	}
	return userPriv.Privileges != 0
}
