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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
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
	ifExists  bool
	isRole    bool
	roleNames []security.SQLUsername
}

// DropRole represents a DROP ROLE statement.
// Privileges: CREATEROLE privilege.
func (p *planner) DropRole(ctx context.Context, n *tree.DropRole) (planNode, error) {
	return p.DropRoleNode(ctx, n.Names, n.IfExists, n.IsRole, "DROP ROLE")
}

// DropRoleNode creates a "drop user" plan node. This can be called from DROP USER or DROP ROLE.
func (p *planner) DropRoleNode(
	ctx context.Context, roleSpecs tree.RoleSpecList, ifExists bool, isRole bool, opName string,
) (*DropRoleNode, error) {
	if err := p.CheckRoleOption(ctx, roleoption.CREATEROLE); err != nil {
		return nil, err
	}

	for _, r := range roleSpecs {
		if r.RoleSpecType != tree.RoleName {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "cannot use special role specifier in DROP ROLE")
		}
	}
	roleNames, err := roleSpecs.ToSQLUsernames(p.SessionData(), security.UsernameCreation)
	if err != nil {
		return nil, err
	}

	return &DropRoleNode{
		ifExists:  ifExists,
		isRole:    isRole,
		roleNames: roleNames,
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

	hasAdmin, err := params.p.HasAdminRole(params.ctx)
	if err != nil {
		return err
	}

	// Now check whether the user still has permission or ownership on any
	// object in the database.

	userNames := make(map[security.SQLUsername][]objectAndType)
	for i, name := range n.roleNames {
		// userNames maps users to the objects they own
		userNames[n.roleNames[i]] = make([]objectAndType, 0)
		if name.IsReserved() {
			return pgerror.Newf(pgcode.ReservedName, "role name %q is reserved", name.Normalized())
		}
		// Non-admin users cannot drop admins.
		if !hasAdmin {
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
			return accumulateDependentDefaultPrivileges(db.GetDefaultPrivilegeDescriptor(), userNames, db.GetName(), "" /* schemaName */)
		}); err != nil {
		return err
	}

	// Then check all the tables.
	//
	// We need something like forEachTableAll here, however we can't use
	// the predefined forEachTableAll() function because we need to look
	// at all _visible_ descriptors, not just those on which the current
	// user has permission.
	all, err := params.p.Descriptors().GetAllDescriptors(params.ctx, params.p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(all.OrderedDescriptors(), nil /*prefix - we want all descriptors */)
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

		dbDesc, err := lCtx.getDatabaseByID(schemaDesc.GetParentID())
		if err != nil {
			return err
		}

		if err := accumulateDependentDefaultPrivileges(schemaDesc.GetDefaultPrivilegeDescriptor(), userNames, dbDesc.GetName(), schemaDesc.GetName()); err != nil {
			return err
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
		for i, name := range n.roleNames {
			if i > 0 {
				fnl.WriteString(", ")
			}
			fnl.FormatName(name.Normalized())
		}
		return pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot drop role%s/user%s %s: grants still exist on %s",
			util.Pluralize(int64(len(n.roleNames))), util.Pluralize(int64(len(n.roleNames))),
			fnl.String(), privilegeObjectFormatter.String(),
		)
	}

	hasDependentDefaultPrivilege := false
	for _, name := range n.roleNames {
		// Did the user own any objects?
		dependentObjects := userNames[name]

		// Sort the slice so we're guaranteed the same ordering on errors.
		sort.SliceStable(dependentObjects, func(i int, j int) bool {
			if dependentObjects[i].ObjectType != dependentObjects[j].ObjectType {
				return dependentObjects[i].ObjectType < dependentObjects[j].ObjectType
			}

			if dependentObjects[i].ObjectName != dependentObjects[j].ObjectName {
				return dependentObjects[i].ObjectName < dependentObjects[j].ObjectName
			}

			return dependentObjects[i].ErrorMessage.Error() < dependentObjects[j].ErrorMessage.Error()
		})
		var hints []string
		if len(dependentObjects) > 0 {
			objectsMsg := tree.NewFmtCtx(tree.FmtSimple)
			for _, obj := range dependentObjects {
				switch obj.ObjectType {
				case database, table, schema, typeObject:
					objectsMsg.WriteString(fmt.Sprintf("\nowner of %s %s", obj.ObjectType, obj.ObjectName))
				case defaultPrivilege:
					hasDependentDefaultPrivilege = true
					objectsMsg.WriteString(fmt.Sprintf("\n%s", obj.ErrorMessage))
					hints = append(hints, errors.GetAllHints(obj.ErrorMessage)...)
				}
			}
			objects := objectsMsg.CloseAndGetString()
			err := pgerror.Newf(pgcode.DependentObjectsStillExist,
				"role %s cannot be dropped because some objects depend on it%s",
				name, objects)
			if hasDependentDefaultPrivilege {
				err = errors.WithHint(err,
					strings.Join(hints, "\n"),
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

	// Bump role-related table versions to force a refresh of membership/auth
	// caches.
	if sessioninit.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
		if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
			return err
		}
		if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
			return err
		}
		if numRoleSettingsRowsDeleted > 0 {
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

	normalizedNames := make([]string, len(n.roleNames))
	for i, name := range n.roleNames {
		normalizedNames[i] = name.Normalized()
	}
	sort.Strings(normalizedNames)
	for _, name := range normalizedNames {
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
	defaultPrivilegeDescriptor catalog.DefaultPrivilegeDescriptor,
	userNames map[security.SQLUsername][]objectAndType,
	dbName string,
	schemaName string,
) error {
	// The func we pass into ForEachDefaultPrivilegeForRole will never
	// err so no err will be returned.
	return defaultPrivilegeDescriptor.ForEachDefaultPrivilegeForRole(func(
		defaultPrivilegesForRole catpb.DefaultPrivilegesForRole) error {
		role := catpb.DefaultPrivilegesRole{}
		if defaultPrivilegesForRole.IsExplicitRole() {
			role.Role = defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode()
		} else {
			role.ForAllRoles = true
		}
		for object, defaultPrivs := range defaultPrivilegesForRole.DefaultPrivilegesPerObject {
			addDependentPrivileges(object, defaultPrivs, role, userNames, dbName, schemaName)
		}
		return nil
	})
}

func addDependentPrivileges(
	object tree.AlterDefaultPrivilegesTargetObject,
	defaultPrivs catpb.PrivilegeDescriptor,
	role catpb.DefaultPrivilegesRole,
	userNames map[security.SQLUsername][]objectAndType,
	dbName string,
	schemaName string,
) {
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

	inSchemaMsg := ""
	if schemaName != "" {
		inSchemaMsg = fmt.Sprintf(" in schema %s", schemaName)
	}

	createHint := func(
		role catpb.DefaultPrivilegesRole,
		grantee security.SQLUsername,
	) string {

		roleString := "ALL ROLES"
		if !role.ForAllRoles {
			roleString = fmt.Sprintf("ROLE %s", role.Role.SQLIdentifier())
		}

		return fmt.Sprintf("USE %s; ALTER DEFAULT PRIVILEGES FOR %s%s REVOKE ALL ON %s FROM %s;",
			dbName, roleString, strings.ToUpper(inSchemaMsg), strings.ToUpper(object.String()), grantee.SQLIdentifier())
	}

	for _, privs := range defaultPrivs.Users {
		grantee := privs.User()
		if !role.ForAllRoles {
			if _, ok := userNames[role.Role]; ok {
				hint := createHint(role, grantee)
				userNames[role.Role] = append(userNames[role.Role],
					objectAndType{
						ObjectType: defaultPrivilege,
						ErrorMessage: errors.WithHint(
							errors.Newf(
								"owner of default privileges on new %s belonging to role %s in database %s%s",
								objectType, role.Role, dbName, inSchemaMsg,
							), hint),
					})
			}
		}
		if _, ok := userNames[grantee]; ok {
			hint := createHint(role, grantee)
			var err error
			if role.ForAllRoles {
				err = errors.Newf(
					"privileges for default privileges on new %s for all roles in database %s%s",
					objectType, dbName, inSchemaMsg,
				)
			} else {
				err = errors.Newf(
					"privileges for default privileges on new %s belonging to role %s in database %s%s",
					objectType, role.Role, dbName, inSchemaMsg,
				)
			}
			userNames[grantee] = append(userNames[grantee],
				objectAndType{
					ObjectType:   defaultPrivilege,
					ErrorMessage: errors.WithHint(err, hint),
				})
		}
	}
}
