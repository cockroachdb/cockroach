// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// DropRoleNode deletes entries from the system.users table.
// This is called from DROP USER and DROP ROLE.
type DropRoleNode struct {
	ifExists bool
	isRole   bool
	names    func() ([]string, error)
}

// DropRole represents a DROP ROLE statement.
// Privileges: CREATEROLE privilege.
func (p *planner) DropRole(ctx context.Context, n *tree.DropRole) (planNode, error) {
	return p.DropRoleNode(ctx, n.Names, n.IfExists, n.IsRole, "DROP ROLE")
}

// DropRoleNode creates a "drop user" plan node. This can be called from DROP USER or DROP ROLE.
func (p *planner) DropRoleNode(
	ctx context.Context, namesE tree.Exprs, ifExists bool, isRole bool, opName string,
) (*DropRoleNode, error) {
	if err := p.CheckRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	names, err := p.TypeAsStringArray(ctx, namesE, opName)
	if err != nil {
		return nil, err
	}

	return &DropRoleNode{
		ifExists: ifExists,
		isRole:   isRole,
		names:    names,
	}, nil
}

type objectType string

const (
	database         objectType = "database"
	table            objectType = "table"
	schema           objectType = "schema"
	typeObject       objectType = "type"
	defaultPrivilege objectType = "default_privilege"
)

type objectAndType struct {
	ObjectType   objectType
	ObjectName   string
	ErrorMessage error
}

func (n *DropRoleNode) startExec(params runParams) error {
	var opName string
	if n.isRole {
		sqltelemetry.IncIAMDropCounter(sqltelemetry.Role)
		opName = "drop-role"
	} else {
		sqltelemetry.IncIAMDropCounter(sqltelemetry.User)
		opName = "drop-user"
	}

	names, err := n.names()
	if err != nil {
		return err
	}

	// Now check whether the user still has permission or ownership on any
	// object in the database.

	// userNames maps users to the objects they own
	userNames := make(map[security.SQLUsername][]objectAndType)
	for i := range names {
		name := names[i]
		normalizedUsername, err := NormalizeAndValidateUsername(name)
		if err != nil {
			return err
		}

		// Update the name in the names slice since we will re-use the name later.
		names[i] = normalizedUsername.Normalized()
		userNames[normalizedUsername] = make([]objectAndType, 0)
	}

	// Non-admin users cannot drop admins.
	hasAdmin, err := params.p.HasAdminRole(params.ctx)
	if err != nil {
		return err
	}
	if !hasAdmin {
		for i := range names {
			// Normalized above already.
			name := security.MakeSQLUsernameFromPreNormalizedString(names[i])
			targetIsAdmin, err := params.p.UserHasAdminRole(params.ctx, name)
			if err != nil {
				return err
			}
			if targetIsAdmin {
				return pgerror.New(pgcode.InsufficientPrivilege, "must be superuser to drop superusers")
			}
		}
	}

	privilegeObjectFormatter := tree.NewFmtCtx(tree.FmtSimple)
	defer privilegeObjectFormatter.Close()

	// First check all the databases.
	if err := forEachDatabaseDesc(params.ctx, params.p, nil /*nil prefix = all databases*/, true, /* requiresPrivileges */
		func(db catalog.DatabaseDescriptor) error {
			if _, ok := userNames[db.GetPrivileges().Owner()]; ok {
				userNames[db.GetPrivileges().Owner()] = append(
					userNames[db.GetPrivileges().Owner()],
					objectAndType{
						ObjectType: database,
						ObjectName: db.GetName(),
					})
			}
			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User()]; ok {
					if privilegeObjectFormatter.Len() > 0 {
						privilegeObjectFormatter.WriteString(", ")
					}
					privilegeObjectFormatter.FormatName(db.GetName())
					break
				}
			}
			return accumulateDependentDefaultPrivileges(db, userNames)
		}); err != nil {
		return err
	}

	// Then check all the tables.
	//
	// We need something like forEachTableAll here, however we can't use
	// the predefined forEachTableAll() function because we need to look
	// at all _visible_ descriptors, not just those on which the current
	// user has permission.
	descs, err := params.p.Descriptors().GetAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(params.ctx, descs, nil /*prefix - we want all descriptors */, nil /* fallback */)
	// privileges are added.
	for _, tbID := range lCtx.tbIDs {
		tableDescriptor := lCtx.tbDescs[tbID]
		if !descriptorIsVisible(tableDescriptor, true /*allowAdding*/) {
			continue
		}
		if _, ok := userNames[tableDescriptor.GetPrivileges().Owner()]; ok {
			tn, err := getTableNameFromTableDescriptor(lCtx, tableDescriptor, "")
			if err != nil {
				return err
			}
			userNames[tableDescriptor.GetPrivileges().Owner()] = append(
				userNames[tableDescriptor.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: table,
					ObjectName: tn.String(),
				})
		}
		for _, u := range tableDescriptor.GetPrivileges().Users {
			if _, ok := userNames[u.User()]; ok {
				if privilegeObjectFormatter.Len() > 0 {
					privilegeObjectFormatter.WriteString(", ")
				}
				parentName := lCtx.getDatabaseName(tableDescriptor)
				schemaName := lCtx.getSchemaName(tableDescriptor)
				tn := tree.MakeTableNameWithSchema(tree.Name(parentName), tree.Name(schemaName), tree.Name(tableDescriptor.GetName()))
				privilegeObjectFormatter.FormatNode(&tn)
				break
			}
		}
	}
	for _, schemaDesc := range lCtx.schemaDescs {
		if !descriptorIsVisible(schemaDesc, true /* allowAdding */) {
			continue
		}
		// TODO(arul): Ideally this should be the fully qualified name of the schema,
		// but at the time of writing there doesn't seem to be a clean way of doing
		// this.
		if _, ok := userNames[schemaDesc.GetPrivileges().Owner()]; ok {
			userNames[schemaDesc.GetPrivileges().Owner()] = append(
				userNames[schemaDesc.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: schema,
					ObjectName: schemaDesc.GetName(),
				})
		}
	}
	for _, typDesc := range lCtx.typDescs {
		if _, ok := userNames[typDesc.GetPrivileges().Owner()]; ok {
			if !descriptorIsVisible(typDesc, true /* allowAdding */) {
				continue
			}
			tn, err := getTypeNameFromTypeDescriptor(lCtx, typDesc)
			if err != nil {
				return err
			}
			userNames[typDesc.GetPrivileges().Owner()] = append(
				userNames[typDesc.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: typeObject,
					ObjectName: tn.String(),
				})
		}
	}

	// Was there any object depending on that user?
	if privilegeObjectFormatter.Len() > 0 {
		fnl := tree.NewFmtCtx(tree.FmtSimple)
		defer fnl.Close()
		for i, name := range names {
			if i > 0 {
				fnl.WriteString(", ")
			}
			fnl.FormatName(name)
		}
		return pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop role%s/user%s %s: grants still exist on %s",
			util.Pluralize(int64(len(names))), util.Pluralize(int64(len(names))),
			fnl.String(), privilegeObjectFormatter.String(),
		)
	}

	hasDependentDefaultPrivilege := false
	for i := range names {
		// Name already normalized above.
		name := security.MakeSQLUsernameFromPreNormalizedString(names[i])
		// Did the user own any objects?
		dependentObjects := userNames[name]
		if len(dependentObjects) > 0 {
			objectsMsg := tree.NewFmtCtx(tree.FmtSimple)
			for _, obj := range dependentObjects {
				switch obj.ObjectType {
				case database, table, schema, typeObject:
					objectsMsg.WriteString(fmt.Sprintf("\nowner of %s %s", obj.ObjectType, obj.ObjectName))
				case defaultPrivilege:
					hasDependentDefaultPrivilege = true
					objectsMsg.WriteString(fmt.Sprintf("\n%s", obj.ErrorMessage))
				}
			}
			objects := objectsMsg.CloseAndGetString()
			err := pgerror.Newf(pgcode.DependentObjectsStillExist,
				"role %s cannot be dropped because some objects depend on it%s",
				name, objects)
			if hasDependentDefaultPrivilege {
				err = errors.WithHint(err,
					"use SHOW DEFAULT PRIVILEGES FOR ROLE to find existing default privileges"+
						" and execute ALTER DEFAULT PRIVILEGES {FOR ROLE ... / FOR ALL ROLES} "+
						"REVOKE ... ON ... FROM ... to remove them"+
						"\nsee: SHOW DEFAULT PRIVILEGES and ALTER DEFAULT PRIVILEGES",
				)
			}
			return err
		}
	}

	// All safe - do the work.
	var numRoleMembershipsDeleted, numRoleSettingsRowsDeleted int
	for normalizedUsername := range userNames {
		// Specifically reject special users and roles. Some (root, admin) would fail with
		// "privileges still exist" first.
		if normalizedUsername.IsAdminRole() || normalizedUsername.IsPublicRole() {
			return pgerror.Newf(
				pgcode.InvalidParameterValue, "cannot drop special role %s", normalizedUsername)
		}
		if normalizedUsername.IsRootUser() {
			return pgerror.Newf(
				pgcode.InvalidParameterValue, "cannot drop special user %s", normalizedUsername)
		}

		// Check if user owns any scheduled jobs.
		numSchedulesRow, err := params.ExecCfg().InternalExecutor.QueryRow(
			params.ctx,
			"check-user-schedules",
			params.p.txn,
			"SELECT count(*) FROM system.scheduled_jobs WHERE owner=$1",
			normalizedUsername,
		)
		if err != nil {
			return err
		}
		if numSchedulesRow == nil {
			return errors.New("failed to check user schedules")
		}
		numSchedules := int64(tree.MustBeDInt(numSchedulesRow[0]))
		if numSchedules > 0 {
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"cannot drop role/user %s; it owns %d scheduled jobs.",
				normalizedUsername, numSchedules)
		}

		numUsersDeleted, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.txn,
			`DELETE FROM system.users WHERE username=$1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		if numUsersDeleted == 0 && !n.ifExists {
			return errors.Errorf("role/user %s does not exist", normalizedUsername)
		}

		// Drop all role memberships involving the user/role.
		rowsDeleted, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}
		numRoleMembershipsDeleted += rowsDeleted

		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.txn,
			fmt.Sprintf(
				`DELETE FROM %s WHERE username=$1`,
				sessioninit.RoleOptionsTableName,
			),
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		// TODO(rafi): Remove this condition in 21.2.
		if params.EvalContext().Settings.Version.IsActive(params.ctx, clusterversion.DatabaseRoleSettings) {
			rowsDeleted, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
				params.ctx,
				opName,
				params.p.txn,
				fmt.Sprintf(
					`DELETE FROM %s WHERE role_name = $1`,
					sessioninit.DatabaseRoleSettingsTableName,
				),
				normalizedUsername,
			)
			if err != nil {
				return err
			}
			numRoleSettingsRowsDeleted += rowsDeleted
		}
	}

	// Bump role-related table versions to force a refresh of membership/auth
	// caches.
	if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
		if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
			return err
		}
		if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
			return err
		}
		if numRoleSettingsRowsDeleted > 0 &&
			params.EvalContext().Settings.Version.IsActive(params.ctx, clusterversion.DatabaseRoleSettings) {
			if err := params.p.bumpDatabaseRoleSettingsTableVersion(params.ctx); err != nil {
				return err
			}
		}
	}
	if numRoleMembershipsDeleted > 0 {
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	sort.Strings(names)
	for _, name := range names {
		if err := params.p.logEvent(params.ctx,
			0, /* no target */
			&eventpb.DropRole{RoleName: name}); err != nil {
			return err
		}
	}
	return nil
}

// Next implements the planNode interface.
func (*DropRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*DropRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*DropRoleNode) Close(context.Context) {}

// accumulateDependentDefaultPrivileges checks for any default privileges
// that the users in userNames have and append them to the objectAndType array.
func accumulateDependentDefaultPrivileges(
	db catalog.DatabaseDescriptor, userNames map[security.SQLUsername][]objectAndType,
) error {
	addDependentPrivileges := func(object tree.AlterDefaultPrivilegesTargetObject, defaultPrivs descpb.PrivilegeDescriptor, role descpb.DefaultPrivilegesRole) {
		var objectType string
		switch object {
		case tree.Tables:
			objectType = "relations"
		case tree.Sequences:
			objectType = "sequences"
		case tree.Types:
			objectType = "types"
		case tree.Schemas:
			objectType = "schemas"
		}

		for _, privs := range defaultPrivs.Users {
			if !role.ForAllRoles {
				if _, ok := userNames[role.Role]; ok {
					userNames[role.Role] = append(userNames[role.Role],
						objectAndType{
							ObjectType: defaultPrivilege,
							ErrorMessage: errors.Newf(
								"owner of default privileges on new %s belonging to role %s",
								objectType, role.Role,
							),
						})
				}
			}
			grantee := privs.User()
			if _, ok := userNames[grantee]; ok {
				var err error
				if role.ForAllRoles {
					err = errors.Newf(
						"privileges for default privileges on new %s for all roles",
						objectType,
					)
				} else {
					err = errors.Newf(
						"privileges for default privileges on new %s belonging to role %s",
						objectType, role.Role,
					)
				}
				userNames[grantee] = append(userNames[grantee],
					objectAndType{
						ObjectType:   defaultPrivilege,
						ErrorMessage: err,
					})
			}
		}
	}
	// No error is returned.
	return db.GetDefaultPrivilegeDescriptor().ForEachDefaultPrivilegeForRole(func(
		defaultPrivilegesForRole descpb.DefaultPrivilegesForRole) error {
		role := descpb.DefaultPrivilegesRole{}
		if defaultPrivilegesForRole.IsExplicitRole() {
			role.Role = defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode()
		} else {
			role.ForAllRoles = true
		}
		for object, defaultPrivs := range defaultPrivilegesForRole.DefaultPrivilegesPerObject {
			addDependentPrivileges(object, defaultPrivs, role)
		}
		return nil
	})
}
