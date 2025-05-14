// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

import (
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

type safePriv struct {
	sync.RWMutex
}

var (
	safePrivInstance *safePriv
	safePrivOnce     sync.Once
)

// getSafePriv returns the singleton instance of safePriv.
func getSafePriv() *safePriv {
	safePrivOnce.Do(func() {
		safePrivInstance = &safePriv{}
	})
	return safePrivInstance
}

// findUserIndex looks for a given user and returns its
// index in the User array if found. Returns -1 otherwise.
func (sp *safePriv) atomicFindUserIndex(user username.SQLUsername, p PrivilegeDescriptor) int {
	sp.RLock()
	defer sp.RUnlock()
	idx := sort.Search(len(p.Users), func(i int) bool {
		return !p.Users[i].User().LessThan(user)
	})
	if idx < len(p.Users) && p.Users[idx].User() == user {
		return idx
	}
	return -1
}

// atomicCheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
// It uses a read lock to make sure this check is thread-safe.
func (sp *safePriv) atomicCheckPrivilege(
	p PrivilegeDescriptor, user username.SQLUsername, priv privilege.Kind,
) bool {
	sp.RLock()
	defer sp.RUnlock()
	if p.Owner() == user {
		return true
	}
	userPriv, ok := p.findUser(user)
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

func (sp *safePriv) atomicGrant(
	user username.SQLUsername, privList privilege.List, withGrantOption bool, p *PrivilegeDescriptor,
) {
	sp.Lock()
	defer sp.Unlock()

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
