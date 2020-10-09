// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
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
)

func isPrivilegeSet(bits uint32, priv privilege.Kind) bool {
	return bits&priv.Mask() != 0
}

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

// findUser looks for a specific user in the list.
// Returns (nil, false) if not found, or (obj, true) if found.
func (p PrivilegeDescriptor) findUser(user security.SQLUsername) (*UserPrivileges, bool) {
	idx := p.findUserIndex(user)
	if idx == -1 {
		return nil, false
	}
	return &p.Users[idx], true
}

// findOrCreateUser looks for a specific user in the list, creating it if needed.
func (p *PrivilegeDescriptor) findOrCreateUser(user security.SQLUsername) *UserPrivileges {
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

// removeUser looks for a given user in the list and removes it if present.
func (p *PrivilegeDescriptor) removeUser(user security.SQLUsername) {
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
				UserProto:  security.AdminRoleName().EncodeProto(),
				Privileges: priv.ToBitField(),
			},
			{
				UserProto:  security.RootUserName().EncodeProto(),
				Privileges: priv.ToBitField(),
			},
		},
		Version: OwnerVersion,
	}
}

// NewPrivilegeDescriptor returns a privilege descriptor for the given
// user with the specified list of privileges.
func NewPrivilegeDescriptor(
	user security.SQLUsername, priv privilege.List, owner security.SQLUsername,
) *PrivilegeDescriptor {
	return &PrivilegeDescriptor{
		OwnerProto: owner.EncodeProto(),
		Users: []UserPrivileges{
			{
				UserProto:  user.EncodeProto(),
				Privileges: priv.ToBitField(),
			},
		},
		Version: OwnerVersion,
	}
}

// DefaultSuperuserPrivileges is the list of privileges for super users
// on non-system objects.
var DefaultSuperuserPrivileges = privilege.List{privilege.ALL}

// NewDefaultPrivilegeDescriptor returns a privilege descriptor
// with ALL privileges for the root user and admin role.
func NewDefaultPrivilegeDescriptor(owner security.SQLUsername) *PrivilegeDescriptor {
	return NewCustomSuperuserPrivilegeDescriptor(DefaultSuperuserPrivileges, owner)
}

// Grant adds new privileges to this descriptor for a given list of users.
// TODO(marc): if all privileges other than ALL are set, should we collapse
// them into ALL?
func (p *PrivilegeDescriptor) Grant(user security.SQLUsername, privList privilege.List) {
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
func (p *PrivilegeDescriptor) Revoke(
	user security.SQLUsername, privList privilege.List, objectType privilege.ObjectType,
) {
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
		validPrivs := privilege.GetValidPrivilegesForObject(objectType)
		userPriv.Privileges = 0
		for _, v := range validPrivs {
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
func MaybeFixPrivileges(id ID, p *PrivilegeDescriptor) bool {
	allowedPrivilegesBits := privilege.ALL.Mask()
	if IsReservedID(id) {
		// System databases and tables have custom maximum allowed privileges.
		allowedPrivilegesBits = SystemAllowedPrivileges[id].ToBitField()
	}

	var modified bool

	fixSuperUser := func(user security.SQLUsername) {
		privs := p.findOrCreateUser(user)
		if privs.Privileges != allowedPrivilegesBits {
			privs.Privileges = allowedPrivilegesBits
			modified = true
		}
	}

	// Check "root" user and "admin" role.
	fixSuperUser(security.RootUserName())
	fixSuperUser(security.AdminRoleName())

	if isPrivilegeSet(allowedPrivilegesBits, privilege.ALL) {
		// ALL privileges allowed, we can skip regular users.
		return modified
	}

	for i := range p.Users {
		// Users is a slice of values, we need pointers to make them mutable.
		u := &p.Users[i]
		if u.User().IsRootUser() || u.User().IsAdminRole() {
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

// Validate is called when writing a database, table or type descriptor.
// It takes the descriptor ID which is used to determine if
// it belongs to a system descriptor, in which case the maximum
// set of allowed privileges is looked up and applied.
func (p PrivilegeDescriptor) Validate(id ID, objectType privilege.ObjectType) error {
	allowedPrivileges := DefaultSuperuserPrivileges

	if IsReservedID(id) {
		var ok bool
		allowedPrivileges, ok = SystemAllowedPrivileges[id]
		if !ok {
			return fmt.Errorf("no allowed privileges found for system %s with ID=%d",
				objectType, id)
		}
	}

	// Check "root" user.
	if err := p.validateRequiredSuperuser(id, allowedPrivileges, security.RootUserName(), objectType); err != nil {
		return err
	}

	// We expect an "admin" role. Check that it has desired superuser permissions.
	if err := p.validateRequiredSuperuser(id, allowedPrivileges, security.AdminRoleName(), objectType); err != nil {
		return err
	}

	if p.Version >= OwnerVersion {
		if p.Owner().Undefined() {
			return errors.AssertionFailedf("found no owner for system %s with ID=%d",
				objectType, id)
		}
	}

	allowedPrivilegesBits := privilege.GetValidPrivilegesForObject(objectType).ToBitField()

	// For all non-super users, privileges must not exceed the allowed privileges.
	// Also the privileges must be valid on the object type.
	for _, u := range p.Users {
		if u.User().IsRootUser() || u.User().IsAdminRole() {
			// We've already checked super users.
			continue
		}

		if remaining := u.Privileges &^ allowedPrivilegesBits; remaining != 0 {
			return fmt.Errorf("user %s must not have %s privileges on system %s with ID=%d",
				u.User(), privilege.ListFromBitField(remaining, privilege.Any), objectType, id)
		}
		// Get all the privilege bits set on the descriptor even if they're not valid.
		privs := privilege.ListFromBitField(u.Privileges, privilege.Any)
		if err := privilege.ValidatePrivileges(
			privs, objectType,
		); err != nil {
			return err
		}
	}

	return nil
}

func (p PrivilegeDescriptor) validateRequiredSuperuser(
	id ID,
	allowedPrivileges privilege.List,
	user security.SQLUsername,
	objectType privilege.ObjectType,
) error {
	superPriv, ok := p.findUser(user)
	if !ok {
		return fmt.Errorf("user %s does not have privileges over system %s with ID=%d",
			user, objectType, id)
	}

	// The super users must match the allowed privilege set exactly.
	if superPriv.Privileges != allowedPrivileges.ToBitField() {
		return fmt.Errorf("user %s must have exactly %s privileges on system %s with ID=%d",
			user, allowedPrivileges, objectType, id)
	}

	return nil
}

// UserPrivilegeString is a pair of strings describing the
// privileges for a given user.
type UserPrivilegeString struct {
	User       security.SQLUsername
	Privileges []string
}

// PrivilegeString returns a string of comma-separted privilege names.
func (u UserPrivilegeString) PrivilegeString() string {
	return strings.Join(u.Privileges, ",")
}

// Show returns the list of {username, privileges} sorted by username.
// 'privileges' is a string of comma-separated sorted privilege names.
func (p PrivilegeDescriptor) Show(objectType privilege.ObjectType) []UserPrivilegeString {
	ret := make([]UserPrivilegeString, 0, len(p.Users))
	for _, userPriv := range p.Users {
		ret = append(ret, UserPrivilegeString{
			User:       userPriv.User(),
			Privileges: privilege.ListFromBitField(userPriv.Privileges, objectType).SortedNames(),
		})
	}
	return ret
}

// CheckPrivilege returns true if 'user' has 'privilege' on this descriptor.
func (p PrivilegeDescriptor) CheckPrivilege(user security.SQLUsername, priv privilege.Kind) bool {
	userPriv, ok := p.findUser(user)
	if !ok {
		// User "node" has all privileges.
		return user.IsNodeUser()
	}

	if isPrivilegeSet(userPriv.Privileges, privilege.ALL) {
		return true
	}
	return isPrivilegeSet(userPriv.Privileges, priv)
}

// AnyPrivilege returns true if 'user' has any privilege on this descriptor.
func (p PrivilegeDescriptor) AnyPrivilege(user security.SQLUsername) bool {
	if p.Owner() == user {
		return true
	}
	userPriv, ok := p.findUser(user)
	if !ok {
		return false
	}
	return userPriv.Privileges != 0
}

// SystemAllowedPrivileges describes the allowable privilege list for each
// system object. Super users (root and admin) must have exactly the specified privileges,
// other users must not exceed the specified privileges.
var SystemAllowedPrivileges = map[ID]privilege.List{
	keys.SystemDatabaseID:           privilege.ReadData,
	keys.NamespaceTableID:           privilege.ReadData,
	keys.DeprecatedNamespaceTableID: privilege.ReadData,
	keys.DescriptorTableID:          privilege.ReadData,
	keys.UsersTableID:               privilege.ReadWriteData,
	keys.RoleOptionsTableID:         privilege.ReadWriteData,
	keys.ZonesTableID:               privilege.ReadWriteData,
	// We eventually want to migrate the table to appear read-only to force the
	// the use of a validating, logging accessor, so we'll go ahead and tolerate
	// read-only privs to make that migration possible later.
	keys.SettingsTableID:   privilege.ReadWriteData,
	keys.DescIDSequenceID:  privilege.ReadData,
	keys.TenantsTableID:    privilege.ReadData,
	keys.LeaseTableID:      privilege.ReadWriteData,
	keys.EventLogTableID:   privilege.ReadWriteData,
	keys.RangeEventTableID: privilege.ReadWriteData,
	keys.UITableID:         privilege.ReadWriteData,
	// IMPORTANT: CREATE|DROP|ALL privileges should always be denied or database
	// users will be able to modify system tables' schemas at will. CREATE and
	// DROP privileges are allowed on the above system tables for backwards
	// compatibility reasons only!
	keys.JobsTableID:                          privilege.ReadWriteData,
	keys.WebSessionsTableID:                   privilege.ReadWriteData,
	keys.TableStatisticsTableID:               privilege.ReadWriteData,
	keys.LocationsTableID:                     privilege.ReadWriteData,
	keys.RoleMembersTableID:                   privilege.ReadWriteData,
	keys.CommentsTableID:                      privilege.ReadWriteData,
	keys.ReplicationConstraintStatsTableID:    privilege.ReadWriteData,
	keys.ReplicationCriticalLocalitiesTableID: privilege.ReadWriteData,
	keys.ReplicationStatsTableID:              privilege.ReadWriteData,
	keys.ReportsMetaTableID:                   privilege.ReadWriteData,
	keys.ProtectedTimestampsMetaTableID:       privilege.ReadData,
	keys.ProtectedTimestampsRecordsTableID:    privilege.ReadData,
	keys.StatementBundleChunksTableID:         privilege.ReadWriteData,
	keys.StatementDiagnosticsRequestsTableID:  privilege.ReadWriteData,
	keys.StatementDiagnosticsTableID:          privilege.ReadWriteData,
	keys.ScheduledJobsTableID:                 privilege.ReadWriteData,
	keys.SqllivenessID:                        privilege.ReadWriteData,
}

// SetOwner sets the owner of the privilege descriptor to the provided string.
func (p *PrivilegeDescriptor) SetOwner(owner security.SQLUsername) {
	p.OwnerProto = owner.EncodeProto()
}
