// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/privilege"
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

// NewDefaultPrivilegeDescriptor returns a privilege descriptor
// with ALL privileges for the root user.
func NewDefaultPrivilegeDescriptor() *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		Users: []UserPrivileges{
			{
				User:       security.RootUser,
				Privileges: privilege.ALL.Mask(),
			},
		},
	}
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

// Validate is called when writing a database or table descriptor.
// It takes the descriptor ID which is used to determine if
// it belongs to a system descriptor, in which case the maximum
// set of allowed privileges is looked up and applied.
func (p PrivilegeDescriptor) Validate(id ID) error {
	userPriv, ok := p.findUser(security.RootUser)
	if !ok {
		return fmt.Errorf("user %s does not have privileges", security.RootUser)
	}
	if isSystemConfigID(id) {
		// System databases and tables have custom maximum allowed privileges.
		objectPrivileges, ok := SystemAllowedPrivileges[id]
		if !ok {
			return fmt.Errorf("no allowed privileges found for system object with ID=%d", id)
		}

		// The root user must have all the allowed privileges.
		allowedPrivileges := objectPrivileges.ToBitField()
		if userPriv.Privileges&allowedPrivileges != allowedPrivileges {
			return fmt.Errorf("user %s must have %s privileges on system objects",
				security.RootUser, privilege.ListFromBitField(allowedPrivileges))
		}

		// For all users (root included), no other privileges must be granted.
		for _, u := range p.Users {
			if remaining := u.Privileges &^ allowedPrivileges; remaining != 0 {
				return fmt.Errorf("user %s must not have %s privileges on system objects",
					u.User, privilege.ListFromBitField(remaining))
			}
		}
	} else if !isPrivilegeSet(userPriv.Privileges, privilege.ALL) {
		// Non-system databases and tables must preserve ALL
		// privileges for the root user.
		return fmt.Errorf("user %s does not have ALL privileges", security.RootUser)
	}
	return nil
}

// UserPrivilegeString is a pair of strings describing the
// privileges for a given user.
type UserPrivilegeString struct {
	User       string
	Privileges string
}

// Show returns the list of {username, privileges} sorted by username.
// 'privileges' is a string of comma-separated sorted privilege names.
func (p PrivilegeDescriptor) Show() []UserPrivilegeString {
	ret := make([]UserPrivilegeString, 0, len(p.Users))
	for _, userPriv := range p.Users {
		ret = append(ret, UserPrivilegeString{
			User:       userPriv.User,
			Privileges: privilege.ListFromBitField(userPriv.Privileges).SortedString(),
		})
	}
	return ret
}

// CheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
func (p PrivilegeDescriptor) CheckPrivilege(user string, priv privilege.Kind) bool {
	userPriv, ok := p.findUser(user)
	if !ok {
		return false
	}
	// ALL is always good.
	if isPrivilegeSet(userPriv.Privileges, privilege.ALL) {
		return true
	}
	return isPrivilegeSet(userPriv.Privileges, priv)
}
