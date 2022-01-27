// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catprivilege

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var _ catalog.DefaultPrivilegeDescriptor = &immutable{}
var _ catalog.DefaultPrivilegeDescriptor = &Mutable{}

// immutable is a wrapper for a DefaultPrivilegeDescriptor
// that only exposes getters.
type immutable struct {
	defaultPrivilegeDescriptor *catpb.DefaultPrivilegeDescriptor
}

// Mutable is a wrapper for a DefaultPrivilegeDescriptor
// that exposes getters and setters.
type Mutable struct {
	immutable
}

// MakeDefaultPrivilegeDescriptor returns a new DefaultPrivilegeDescriptor for
// the specified DefaultPrivilegeDescriptorType.
func MakeDefaultPrivilegeDescriptor(
	typ catpb.DefaultPrivilegeDescriptor_DefaultPrivilegeDescriptorType,
) *catpb.DefaultPrivilegeDescriptor {
	var defaultPrivilegesForRole []catpb.DefaultPrivilegesForRole
	return &catpb.DefaultPrivilegeDescriptor{
		DefaultPrivilegesPerRole: defaultPrivilegesForRole,
		Type:                     typ,
	}
}

// MakeDefaultPrivileges returns an immutable
// given a defaultPrivilegeDescriptor.
func MakeDefaultPrivileges(
	defaultPrivilegeDescriptor *catpb.DefaultPrivilegeDescriptor,
) catalog.DefaultPrivilegeDescriptor {
	return &immutable{
		defaultPrivilegeDescriptor: defaultPrivilegeDescriptor,
	}
}

// NewMutableDefaultPrivileges returns a Mutable
// given a defaultPrivilegeDescriptor.
func NewMutableDefaultPrivileges(
	defaultPrivilegeDescriptor *catpb.DefaultPrivilegeDescriptor,
) *Mutable {
	return &Mutable{
		immutable{
			defaultPrivilegeDescriptor: defaultPrivilegeDescriptor,
		}}
}

// grantOrRevokeDefaultPrivilegesHelper calls expandPrivileges before calling
// the function passed in and calls foldPrivileges after calling grant/revoke
// if the DefaultPrivilegeDescriptor is for a database.
// If the DefaultPrivilegeDescriptor is for a schema we simply
// call grant/revoke.
func (d *immutable) grantOrRevokeDefaultPrivilegesHelper(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole,
	role catpb.DefaultPrivilegesRole,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	grantee security.SQLUsername,
	privList privilege.List,
	withGrantOption bool,
	isGrant bool,
	deprecateGrant bool,
) {
	defaultPrivileges := defaultPrivilegesForRole.DefaultPrivilegesPerObject[targetObject]
	// expandPrivileges turns flags on the DefaultPrivilegesForRole representing
	// special privilege cases into real privileges on the PrivilegeDescriptor.
	// foldPrivileges converts the real privileges back into flags.
	// We only need to do this if the default privileges are defined globally
	// (for the database) as schemas do not have this special case.
	if d.IsDatabaseDefaultPrivilege() {
		expandPrivileges(defaultPrivilegesForRole, role, &defaultPrivileges, targetObject)
	}
	if isGrant {
		defaultPrivileges.Grant(grantee, privList, withGrantOption)
	} else {
		defaultPrivileges.Revoke(grantee, privList, targetObject.ToPrivilegeObjectType(), withGrantOption)
	}

	if deprecateGrant {
		defaultPrivileges.GrantPrivilegeToGrantOptions(grantee, isGrant)
	}

	if d.IsDatabaseDefaultPrivilege() {
		foldPrivileges(defaultPrivilegesForRole, role, &defaultPrivileges, targetObject)
	}
	defaultPrivilegesForRole.DefaultPrivilegesPerObject[targetObject] = defaultPrivileges
}

// GrantDefaultPrivileges grants privileges for the specified users.
func (d *Mutable) GrantDefaultPrivileges(
	role catpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees []security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	withGrantOption bool,
	deprecateGrant bool,
) {
	defaultPrivilegesForRole := d.defaultPrivilegeDescriptor.FindOrCreateUser(role)
	for _, grantee := range grantees {
		d.grantOrRevokeDefaultPrivilegesHelper(defaultPrivilegesForRole, role, targetObject, grantee, privileges, withGrantOption, true /* isGrant */, deprecateGrant)
	}
}

// RevokeDefaultPrivileges revokes privileges for the specified users.
func (d *Mutable) RevokeDefaultPrivileges(
	role catpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees []security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	grantOptionFor bool,
	deprecateGrant bool,
) {
	defaultPrivilegesForRole := d.defaultPrivilegeDescriptor.FindOrCreateUser(role)
	for _, grantee := range grantees {
		d.grantOrRevokeDefaultPrivilegesHelper(defaultPrivilegesForRole, role, targetObject, grantee, privileges, grantOptionFor, false /* isGrant */, deprecateGrant)
	}

	defaultPrivilegesPerObject := defaultPrivilegesForRole.DefaultPrivilegesPerObject
	// Check if there are any default privileges remaining on the descriptor.
	// If there are no privileges left remaining and the descriptor is in the
	// default state, we can remove it.
	for _, defaultPrivs := range defaultPrivilegesPerObject {
		if len(defaultPrivs.Users) != 0 {
			return
		}
	}

	// If the DefaultPrivilegeDescriptor is defined on a schema, the flags are
	// not used and have no meaning.
	if defaultPrivilegesForRole.IsExplicitRole() && d.IsDatabaseDefaultPrivilege() &&
		(!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, tree.Tables) ||
			!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, tree.Sequences) ||
			!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, tree.Types) ||
			!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, tree.Schemas)) ||
		!GetPublicHasUsageOnTypes(defaultPrivilegesForRole) {
		return
	}

	// There no entries remaining, remove the entry for the role.
	d.defaultPrivilegeDescriptor.RemoveUser(role)
}

// CreatePrivilegesFromDefaultPrivileges creates privileges for a
// the specified object with the corresponding default privileges and
// the appropriate owner (node for system, the restoring user otherwise.)
// schemaDefaultPrivilegeDescriptor can be nil in the case where the object
// being created is itself a schema.
func CreatePrivilegesFromDefaultPrivileges(
	dbDefaultPrivilegeDescriptor catalog.DefaultPrivilegeDescriptor,
	schemaDefaultPrivilegeDescriptor catalog.DefaultPrivilegeDescriptor,
	dbID descpb.ID,
	user security.SQLUsername,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
	databasePrivileges *catpb.PrivilegeDescriptor,
) *catpb.PrivilegeDescriptor {
	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	if dbID == keys.SystemDatabaseID {
		return catpb.NewBasePrivilegeDescriptor(security.NodeUserName())
	}

	defaultPrivilegeDescriptors := []catalog.DefaultPrivilegeDescriptor{
		dbDefaultPrivilegeDescriptor,
	}

	if schemaDefaultPrivilegeDescriptor != nil {
		defaultPrivilegeDescriptors = append(defaultPrivilegeDescriptors, schemaDefaultPrivilegeDescriptor)
	}

	newPrivs := catpb.NewBasePrivilegeDescriptor(user)
	role := catpb.DefaultPrivilegesRole{Role: user}
	for _, d := range defaultPrivilegeDescriptors {
		if defaultPrivilegesForRole, found := d.GetDefaultPrivilegesForRole(role); !found {
			// If default privileges are not defined for the creator role, we handle
			// it as the case where the user has all privileges.
			defaultPrivilegesForCreatorRole := catpb.InitDefaultPrivilegesForRole(role, d.GetDefaultPrivilegeDescriptorType())
			for _, user := range GetUserPrivilegesForObject(defaultPrivilegesForCreatorRole, targetObject) {
				applyDefaultPrivileges(
					newPrivs,
					user.UserProto.Decode(),
					privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
					privilege.ListFromBitField(user.WithGrantOption, targetObject.ToPrivilegeObjectType()),
				)
			}
		} else {
			// If default privileges were defined for the role, we create privileges
			// using the default privileges.
			for _, user := range GetUserPrivilegesForObject(*defaultPrivilegesForRole, targetObject) {
				applyDefaultPrivileges(
					newPrivs,
					user.UserProto.Decode(),
					privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
					privilege.ListFromBitField(user.WithGrantOption, targetObject.ToPrivilegeObjectType()),
				)
			}
		}

		// The privileges for the object are the union of the default privileges
		// defined for the object for the object creator and the default privileges
		// defined for all roles.
		defaultPrivilegesForAllRoles, found := d.GetDefaultPrivilegesForRole(catpb.DefaultPrivilegesRole{ForAllRoles: true})
		if found {
			for _, user := range GetUserPrivilegesForObject(*defaultPrivilegesForAllRoles, targetObject) {
				applyDefaultPrivileges(
					newPrivs,
					user.UserProto.Decode(),
					privilege.ListFromBitField(user.Privileges, targetObject.ToPrivilegeObjectType()),
					privilege.ListFromBitField(user.WithGrantOption, targetObject.ToPrivilegeObjectType()),
				)
			}
		}
	}

	newPrivs.Version = catpb.Version21_2

	// TODO(richardjcai): Remove this depending on how we handle the migration.
	//   For backwards compatibility, also "inherit" privileges from the dbDesc.
	//   Issue #67378.
	if targetObject == tree.Tables || targetObject == tree.Sequences {
		for _, u := range databasePrivileges.Users {
			applyDefaultPrivileges(
				newPrivs,
				u.UserProto.Decode(),
				privilege.ListFromBitField(u.Privileges, privilege.Table),
				privilege.ListFromBitField(u.WithGrantOption, privilege.Table),
			)
		}
	} else if targetObject == tree.Schemas {
		for _, u := range databasePrivileges.Users {
			applyDefaultPrivileges(
				newPrivs,
				u.UserProto.Decode(),
				privilege.ListFromBitField(u.Privileges, privilege.Schema),
				privilege.ListFromBitField(u.WithGrantOption, privilege.Schema),
			)
		}
	}
	return newPrivs
}

// ForEachDefaultPrivilegeForRole implements the
// catalog.DefaultPrivilegeDescriptor interface.
// ForEachDefaultPrivilegeForRole loops through the DefaultPrivilegeDescriptior's
// DefaultPrivilegePerRole entry and calls f on it.
func (d *immutable) ForEachDefaultPrivilegeForRole(
	f func(defaultPrivilegesForRole catpb.DefaultPrivilegesForRole) error,
) error {
	if d.defaultPrivilegeDescriptor == nil {
		return nil
	}
	for _, defaultPrivilegesForRole := range d.defaultPrivilegeDescriptor.DefaultPrivilegesPerRole {
		if err := f(defaultPrivilegesForRole); err != nil {
			return err
		}
	}
	return nil
}

// GetDefaultPrivilegesForRole implements the
// catalog.DefaultPrivilegeDescriptor interface.
// GetDefaultPrivilegesForRole looks for a specific user in the list.
// Returns (nil, false) if not found, or (ptr, true) if found.
func (d *immutable) GetDefaultPrivilegesForRole(
	role catpb.DefaultPrivilegesRole,
) (*catpb.DefaultPrivilegesForRole, bool) {
	idx := d.defaultPrivilegeDescriptor.FindUserIndex(role)
	if idx == -1 {
		return nil, false
	}
	return &d.defaultPrivilegeDescriptor.DefaultPrivilegesPerRole[idx], true
}

// GetDefaultPrivilegeDescriptorType the Type of the default privilege descriptor.
func (d *immutable) GetDefaultPrivilegeDescriptorType() catpb.DefaultPrivilegeDescriptor_DefaultPrivilegeDescriptorType {
	return d.defaultPrivilegeDescriptor.Type
}

// IsDatabaseDefaultPrivilege returns whether the Type of the default privilege
// descriptor is for databases.
func (d *immutable) IsDatabaseDefaultPrivilege() bool {
	return d.defaultPrivilegeDescriptor.Type == catpb.DefaultPrivilegeDescriptor_DATABASE
}

// foldPrivileges folds ALL privileges for role and USAGE on public into
// the corresponding flag on the DefaultPrivilegesForRole object.
// For example, if after a Grant operation, role has ALL on tables, ALL
// privilege is removed from the UserPrivileges object and instead
// RoleHasAllPrivilegesOnTable is set to true.
// This is necessary as role having ALL privileges on tables is the default state
// and should not prevent the role from being dropped if it has ALL privileges.
func foldPrivileges(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole,
	role catpb.DefaultPrivilegesRole,
	privileges *catpb.PrivilegeDescriptor,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if targetObject == tree.Types &&
		privileges.CheckPrivilege(security.PublicRoleName(), privilege.USAGE) {
		publicUser, ok := privileges.FindUser(security.PublicRoleName())
		if ok {
			if !privilege.USAGE.IsSetIn(publicUser.WithGrantOption) {
				setPublicHasUsageOnTypes(defaultPrivilegesForRole, true)
				privileges.Revoke(
					security.PublicRoleName(),
					privilege.List{privilege.USAGE},
					privilege.Type,
					false, /* grantOptionFor */
				)
			}
		}
	}
	// ForAllRoles cannot be a grantee, nothing left to do.
	if role.ForAllRoles {
		return
	}
	if privileges.HasAllPrivileges(role.Role, targetObject.ToPrivilegeObjectType()) {
		user := privileges.FindOrCreateUser(role.Role)
		if user.WithGrantOption == 0 {
			setRoleHasAllOnTargetObject(defaultPrivilegesForRole, true, targetObject)
			privileges.RemoveUser(role.Role)
		}
	}
}

// expandPrivileges expands the pseudo privilege flags on
// DefaultPrivilegesForRole into real privileges on the UserPrivileges object.
// After expandPrivileges, UserPrivileges can be Granted/Revoked from normally.
// For example - if RoleHasAllPrivilegesOnTables is true, ALL privilege is added
// into the UserPrivileges array for the Role.
func expandPrivileges(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole,
	role catpb.DefaultPrivilegesRole,
	privileges *catpb.PrivilegeDescriptor,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if targetObject == tree.Types && GetPublicHasUsageOnTypes(defaultPrivilegesForRole) {
		privileges.Grant(security.PublicRoleName(), privilege.List{privilege.USAGE}, false /* withGrantOption */)
		setPublicHasUsageOnTypes(defaultPrivilegesForRole, false)
	}
	// ForAllRoles cannot be a grantee, nothing left to do.
	if role.ForAllRoles {
		return
	}
	if GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, targetObject) {
		privileges.Grant(defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode(), privilege.List{privilege.ALL}, false /* withGrantOption */)
		setRoleHasAllOnTargetObject(defaultPrivilegesForRole, false, targetObject)
	}
}

// GetUserPrivilegesForObject returns the set of []UserPrivileges constructed
// from the DefaultPrivilegesForRole.
func GetUserPrivilegesForObject(
	p catpb.DefaultPrivilegesForRole, targetObject tree.AlterDefaultPrivilegesTargetObject,
) []catpb.UserPrivileges {
	var userPrivileges []catpb.UserPrivileges
	if privileges, ok := p.DefaultPrivilegesPerObject[targetObject]; ok {
		userPrivileges = privileges.Users
	}
	if GetPublicHasUsageOnTypes(&p) && targetObject == tree.Types {
		userPrivileges = append(userPrivileges, catpb.UserPrivileges{
			UserProto:  security.PublicRoleName().EncodeProto(),
			Privileges: privilege.USAGE.Mask(),
		})
	}
	// If ForAllRoles is specified, we can return early.
	// ForAllRoles is not a real role and does not have implicit default privileges
	// for itself.
	if !p.IsExplicitRole() {
		return userPrivileges
	}
	userProto := p.GetExplicitRole().UserProto
	if GetRoleHasAllPrivilegesOnTargetObject(&p, targetObject) {
		return append(userPrivileges, catpb.UserPrivileges{
			UserProto:  userProto,
			Privileges: privilege.ALL.Mask(),
		})
	}
	return userPrivileges
}

// GetPublicHasUsageOnTypes returns whether Public has Usage privilege on types.
func GetPublicHasUsageOnTypes(defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole) bool {
	if defaultPrivilegesForRole.IsExplicitRole() {
		return defaultPrivilegesForRole.GetExplicitRole().PublicHasUsageOnTypes
	}
	return defaultPrivilegesForRole.GetForAllRoles().PublicHasUsageOnTypes
}

// GetRoleHasAllPrivilegesOnTargetObject returns whether the creator role
// has all privileges on the default privileges target object.
func GetRoleHasAllPrivilegesOnTargetObject(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) bool {
	if !defaultPrivilegesForRole.IsExplicitRole() {
		// ForAllRoles is a pseudo role and does not actually have privileges on it.
		return false
	}
	switch targetObject {
	case tree.Tables:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTables
	case tree.Sequences:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSequences
	case tree.Types:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTypes
	case tree.Schemas:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSchemas
	default:
		panic(fmt.Sprintf("unknown target object %s", targetObject))
	}
}

// setPublicHasUsageOnTypes sets PublicHasUsageOnTypes to publicHasUsageOnTypes.
func setPublicHasUsageOnTypes(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole, publicHasUsageOnTypes bool,
) {
	if defaultPrivilegesForRole.IsExplicitRole() {
		defaultPrivilegesForRole.GetExplicitRole().PublicHasUsageOnTypes = publicHasUsageOnTypes
	} else {
		defaultPrivilegesForRole.GetForAllRoles().PublicHasUsageOnTypes = publicHasUsageOnTypes
	}
}

// applyDefaultPrivileges adds new privileges to this descriptor and new grant options which
// could be different from the privileges. Unlike the normal grant, the privileges
// and the grant options being granted could be different
func applyDefaultPrivileges(
	p *catpb.PrivilegeDescriptor,
	user security.SQLUsername,
	privList privilege.List,
	grantOptionList privilege.List,
) {
	userPriv := p.FindOrCreateUser(user)
	if privilege.ALL.IsSetIn(userPriv.WithGrantOption) && privilege.ALL.IsSetIn(userPriv.Privileges) {
		// User already has 'ALL' privilege: no-op.
		// If userPriv.WithGrantOption has ALL, then userPriv.Privileges must also have ALL.
		// It is possible however for userPriv.Privileges to have ALL but userPriv.WithGrantOption to not have ALL
		return
	}

	privBits := privList.ToBitField()
	grantBits := grantOptionList.ToBitField()

	// Should not be possible for a privilege to be in grantOptionList that is not in privList.
	if !privilege.ALL.IsSetIn(privBits) {
		for _, grantOption := range grantOptionList {
			if privBits&grantOption.Mask() == 0 {
				return
			}
		}
	}

	if privilege.ALL.IsSetIn(privBits) {
		userPriv.Privileges = privilege.ALL.Mask()
	} else {
		if !privilege.ALL.IsSetIn(userPriv.Privileges) {
			userPriv.Privileges |= privBits
		}
	}

	if privilege.ALL.IsSetIn(grantBits) {
		userPriv.WithGrantOption = privilege.ALL.Mask()
	} else {
		if !privilege.ALL.IsSetIn(userPriv.WithGrantOption) {
			userPriv.WithGrantOption |= grantBits
		}
	}
}

func setRoleHasAllOnTargetObject(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole,
	roleHasAll bool,
	targetObject tree.AlterDefaultPrivilegesTargetObject,
) {
	if !defaultPrivilegesForRole.IsExplicitRole() {
		// ForAllRoles is a pseudo role and does not actually have privileges on it.
		panic("DefaultPrivilegesForRole must be for an explicit role")
	}
	switch targetObject {
	case tree.Tables:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTables = roleHasAll
	case tree.Sequences:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSequences = roleHasAll
	case tree.Types:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTypes = roleHasAll
	case tree.Schemas:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSchemas = roleHasAll
	default:
		panic(fmt.Sprintf("unknown target object %s", targetObject))
	}
}
