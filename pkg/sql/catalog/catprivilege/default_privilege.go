// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catprivilege

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
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
	targetObject privilege.TargetObjectType,
	grantee username.SQLUsername,
	privList privilege.List,
	withGrantOption bool,
	isGrant bool,
) error {
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
		if err := defaultPrivileges.Revoke(grantee, privList, targetObject.ToObjectType(), withGrantOption); err != nil {
			return err
		}
	}

	if d.IsDatabaseDefaultPrivilege() {
		if err := foldPrivileges(defaultPrivilegesForRole, role, &defaultPrivileges, targetObject); err != nil {
			return err
		}
	}
	defaultPrivilegesForRole.DefaultPrivilegesPerObject[targetObject] = defaultPrivileges
	return nil
}

// GrantDefaultPrivileges grants privileges for the specified users.
func (d *Mutable) GrantDefaultPrivileges(
	role catpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees []username.SQLUsername,
	targetObject privilege.TargetObjectType,
	withGrantOption bool,
) error {
	defaultPrivilegesForRole := d.defaultPrivilegeDescriptor.FindOrCreateUser(role)
	for _, grantee := range grantees {
		if err := d.grantOrRevokeDefaultPrivilegesHelper(defaultPrivilegesForRole, role, targetObject, grantee, privileges, withGrantOption, true /* isGrant */); err != nil {
			return err
		}
	}
	return nil
}

// RevokeDefaultPrivileges revokes privileges for the specified users.
func (d *Mutable) RevokeDefaultPrivileges(
	role catpb.DefaultPrivilegesRole,
	privileges privilege.List,
	grantees []username.SQLUsername,
	targetObject privilege.TargetObjectType,
	grantOptionFor bool,
) error {
	defaultPrivilegesForRole := d.defaultPrivilegeDescriptor.FindOrCreateUser(role)
	for _, grantee := range grantees {
		if err := d.grantOrRevokeDefaultPrivilegesHelper(defaultPrivilegesForRole, role, targetObject, grantee, privileges, grantOptionFor, false /* isGrant */); err != nil {
			return err
		}
	}

	defaultPrivilegesPerObject := defaultPrivilegesForRole.DefaultPrivilegesPerObject
	// Check if there are any default privileges remaining on the descriptor.
	// If there are no privileges left remaining and the descriptor is in the
	// default state, we can remove it.
	for _, defaultPrivs := range defaultPrivilegesPerObject {
		if len(defaultPrivs.Users) != 0 {
			return nil
		}
	}

	// If the DefaultPrivilegeDescriptor is defined on a schema, the flags are
	// not used and have no meaning.
	if defaultPrivilegesForRole.IsExplicitRole() && d.IsDatabaseDefaultPrivilege() &&
		(!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, privilege.Tables) ||
			!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, privilege.Sequences) ||
			!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, privilege.Types) ||
			!GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, privilege.Schemas) ||
			!GetPublicHasUsageOnTypes(defaultPrivilegesForRole) ||
			!GetPublicHasExecuteOnFunctions(defaultPrivilegesForRole)) {
		return nil
	}

	// There no entries remaining, remove the entry for the role.
	d.defaultPrivilegeDescriptor.RemoveUser(role)
	return nil
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
	user username.SQLUsername,
	targetObject privilege.TargetObjectType,
) (*catpb.PrivilegeDescriptor, error) {
	// If a new system table is being created (which should only be doable by
	// an internal user account), make sure it gets the correct privileges.
	if dbID == keys.SystemDatabaseID {
		return catpb.NewBasePrivilegeDescriptor(username.NodeUserName()), nil
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
				privList, err := privilege.ListFromBitField(user.Privileges, targetObject.ToObjectType())
				if err != nil {
					return nil, err
				}
				grantOptionList, err := privilege.ListFromBitField(user.WithGrantOption, targetObject.ToObjectType())
				if err != nil {
					return nil, err
				}

				applyDefaultPrivileges(
					newPrivs,
					user.UserProto.Decode(),
					privList,
					grantOptionList,
				)
			}
		} else {
			// If default privileges were defined for the role, we create privileges
			// using the default privileges.
			for _, user := range GetUserPrivilegesForObject(*defaultPrivilegesForRole, targetObject) {
				privList, err := privilege.ListFromBitField(user.Privileges, targetObject.ToObjectType())
				if err != nil {
					return nil, err
				}
				grantOptionList, err := privilege.ListFromBitField(user.WithGrantOption, targetObject.ToObjectType())
				if err != nil {
					return nil, err
				}
				applyDefaultPrivileges(
					newPrivs,
					user.UserProto.Decode(),
					privList,
					grantOptionList,
				)
			}
		}

		// The privileges for the object are the union of the default privileges
		// defined for the object for the object creator and the default privileges
		// defined for all roles.
		defaultPrivilegesForAllRoles, found := d.GetDefaultPrivilegesForRole(catpb.DefaultPrivilegesRole{ForAllRoles: true})
		if found {
			for _, user := range GetUserPrivilegesForObject(*defaultPrivilegesForAllRoles, targetObject) {
				privList, err := privilege.ListFromBitField(user.Privileges, targetObject.ToObjectType())
				if err != nil {
					return nil, err
				}
				grantOptionList, err := privilege.ListFromBitField(user.WithGrantOption, targetObject.ToObjectType())
				if err != nil {
					return nil, err
				}
				applyDefaultPrivileges(
					newPrivs,
					user.UserProto.Decode(),
					privList,
					grantOptionList,
				)
			}
		}
	}

	newPrivs.Version = catpb.Version23_2
	return newPrivs, nil
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
	targetObject privilege.TargetObjectType,
) error {
	if targetObject == privilege.Types &&
		privileges.CheckPrivilege(username.PublicRoleName(), privilege.USAGE) {
		setPublicHasUsageOnTypes(defaultPrivilegesForRole, true)
		if err := privileges.Revoke(
			username.PublicRoleName(),
			privilege.List{privilege.USAGE},
			privilege.Type,
			false, /* grantOptionFor */
		); err != nil {
			return err
		}
	}
	if targetObject == privilege.Routines &&
		privileges.CheckPrivilege(username.PublicRoleName(), privilege.EXECUTE) {
		setPublicHasExecuteOnFunctions(defaultPrivilegesForRole, true)
		if err := privileges.Revoke(
			username.PublicRoleName(),
			privilege.List{privilege.EXECUTE},
			privilege.Routine,
			false, /* grantOptionFor */
		); err != nil {
			return err
		}
	}

	// ForAllRoles cannot be a grantee, nothing left to do.
	if role.ForAllRoles {
		return nil
	}
	if hasAll, err := privileges.HasAllPrivileges(role.Role, targetObject.ToObjectType()); err != nil {
		return err
	} else if hasAll {
		// Even though the owner's ALL privileges are implicit, we still need this
		// because it's possible to modify the default privileges to be more
		// fine-grained than ALL.
		user := privileges.FindOrCreateUser(role.Role)
		if user.WithGrantOption == 0 {
			setRoleHasAllOnTargetObject(defaultPrivilegesForRole, true, targetObject)
			privileges.RemoveUser(role.Role)
		}
	}
	return nil
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
	targetObject privilege.TargetObjectType,
) {
	if targetObject == privilege.Types && GetPublicHasUsageOnTypes(defaultPrivilegesForRole) {
		privileges.Grant(username.PublicRoleName(), privilege.List{privilege.USAGE}, false /* withGrantOption */)
		setPublicHasUsageOnTypes(defaultPrivilegesForRole, false)
	}
	if targetObject == privilege.Routines && GetPublicHasExecuteOnFunctions(defaultPrivilegesForRole) {
		privileges.Grant(username.PublicRoleName(), privilege.List{privilege.EXECUTE}, false /* withGrantOption */)
		setPublicHasExecuteOnFunctions(defaultPrivilegesForRole, false)
	}
	// ForAllRoles cannot be a grantee, nothing left to do.
	if role.ForAllRoles {
		return
	}
	if GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, targetObject) {
		// Even though the owner's ALL privileges are implicit, we still need this
		// because it's possible to modify the default privileges to be more
		// fine-grained than ALL.
		privileges.Grant(defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode(), privilege.List{privilege.ALL}, false /* withGrantOption */)
		setRoleHasAllOnTargetObject(defaultPrivilegesForRole, false, targetObject)
	}
}

// GetUserPrivilegesForObject returns the set of []UserPrivileges constructed
// from the DefaultPrivilegesForRole.
func GetUserPrivilegesForObject(
	p catpb.DefaultPrivilegesForRole, targetObject privilege.TargetObjectType,
) []catpb.UserPrivileges {
	var userPrivileges []catpb.UserPrivileges
	if privileges, ok := p.DefaultPrivilegesPerObject[targetObject]; ok {
		userPrivileges = privileges.Users
	}
	if GetPublicHasUsageOnTypes(&p) && targetObject == privilege.Types {
		userPrivileges = append(userPrivileges, catpb.UserPrivileges{
			UserProto:  username.PublicRoleName().EncodeProto(),
			Privileges: privilege.USAGE.Mask(),
		})
	}
	if GetPublicHasExecuteOnFunctions(&p) && targetObject == privilege.Routines {
		userPrivileges = append(userPrivileges, catpb.UserPrivileges{
			UserProto:  username.PublicRoleName().EncodeProto(),
			Privileges: privilege.EXECUTE.Mask(),
		})
	}
	return userPrivileges
}

// GetPublicHasUsageOnTypes returns whether Public has USAGE privilege on types.
func GetPublicHasUsageOnTypes(defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole) bool {
	if defaultPrivilegesForRole.IsExplicitRole() {
		return defaultPrivilegesForRole.GetExplicitRole().PublicHasUsageOnTypes
	}
	return defaultPrivilegesForRole.GetForAllRoles().PublicHasUsageOnTypes
}

// GetPublicHasExecuteOnFunctions returns whether Public has EXECUTE privilege on functions.
func GetPublicHasExecuteOnFunctions(defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole) bool {
	if defaultPrivilegesForRole.IsExplicitRole() {
		return defaultPrivilegesForRole.GetExplicitRole().PublicHasExecuteOnFunctions
	}
	return defaultPrivilegesForRole.GetForAllRoles().PublicHasExecuteOnFunctions
}

// GetRoleHasAllPrivilegesOnTargetObject returns whether the creator role
// has all privileges on the default privileges target object.
func GetRoleHasAllPrivilegesOnTargetObject(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole, targetObject privilege.TargetObjectType,
) bool {
	if !defaultPrivilegesForRole.IsExplicitRole() {
		// ForAllRoles is a pseudo role and does not actually have privileges on it.
		return false
	}
	switch targetObject {
	case privilege.Tables:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTables
	case privilege.Sequences:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSequences
	case privilege.Types:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTypes
	case privilege.Schemas:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSchemas
	case privilege.Routines:
		return defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnFunctions
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

// setPublicHasExecuteOnFunctions sets PublicHasExecuteOnFunctions to publicHasExecuteOnFunctions.
func setPublicHasExecuteOnFunctions(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole, publicHasExecuteOnFunctions bool,
) {
	if defaultPrivilegesForRole.IsExplicitRole() {
		defaultPrivilegesForRole.GetExplicitRole().PublicHasExecuteOnFunctions = publicHasExecuteOnFunctions
	} else {
		defaultPrivilegesForRole.GetForAllRoles().PublicHasExecuteOnFunctions = publicHasExecuteOnFunctions
	}
}

// applyDefaultPrivileges adds new privileges to this descriptor and new grant options which
// could be different from the privileges. Unlike the normal grant, the privileges
// and the grant options being granted could be different
func applyDefaultPrivileges(
	p *catpb.PrivilegeDescriptor,
	user username.SQLUsername,
	privList privilege.List,
	grantOptionList privilege.List,
) {
	userPriv := p.FindOrCreateUser(user)
	newUserPrivs, newUserGrantOptions := ApplyDefaultPrivileges(
		userPriv.Privileges, userPriv.WithGrantOption, privList, grantOptionList,
	)
	userPriv.Privileges = newUserPrivs
	userPriv.WithGrantOption = newUserGrantOptions
}

func ApplyDefaultPrivileges(
	userPrivs uint64,
	userGrantOptions uint64,
	defaultPrivs privilege.List,
	defaultGrantOptions privilege.List,
) (newUserPrivs uint64, newUserGrantOptions uint64) {
	if privilege.ALL.IsSetIn(userGrantOptions) && privilege.ALL.IsSetIn(userPrivs) {
		// User already has 'ALL' privilege: no-op.
		// If userPriv.WithGrantOption has ALL, then userPriv.Privileges must also have ALL.
		// It is possible however for userPriv.Privileges to have ALL but userPriv.WithGrantOption to not have ALL
		return userPrivs, userGrantOptions
	}

	privBits := defaultPrivs.ToBitField()
	grantBits := defaultGrantOptions.ToBitField()

	// Should not be possible for a privilege to be in grantOptionList that is not in privList.
	if !privilege.ALL.IsSetIn(privBits) {
		for _, grantOption := range defaultGrantOptions {
			if privBits&grantOption.Mask() == 0 {
				return userPrivs, userGrantOptions
			}
		}
	}

	if privilege.ALL.IsSetIn(privBits) {
		userPrivs = privilege.ALL.Mask()
	} else {
		if !privilege.ALL.IsSetIn(userPrivs) {
			userPrivs |= privBits
		}
	}

	if privilege.ALL.IsSetIn(grantBits) {
		userGrantOptions = privilege.ALL.Mask()
	} else {
		if !privilege.ALL.IsSetIn(userGrantOptions) {
			userGrantOptions |= grantBits
		}
	}

	return userPrivs, userGrantOptions
}

func setRoleHasAllOnTargetObject(
	defaultPrivilegesForRole *catpb.DefaultPrivilegesForRole,
	roleHasAll bool,
	targetObject privilege.TargetObjectType,
) {
	if !defaultPrivilegesForRole.IsExplicitRole() {
		// ForAllRoles is a pseudo role and does not actually have privileges on it.
		panic("DefaultPrivilegesForRole must be for an explicit role")
	}
	switch targetObject {
	case privilege.Tables:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTables = roleHasAll
	case privilege.Sequences:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSequences = roleHasAll
	case privilege.Types:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnTypes = roleHasAll
	case privilege.Schemas:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnSchemas = roleHasAll
	case privilege.Routines:
		defaultPrivilegesForRole.GetExplicitRole().RoleHasAllPrivilegesOnFunctions = roleHasAll
	default:
		panic(fmt.Sprintf("unknown target object %s", targetObject))
	}
}
