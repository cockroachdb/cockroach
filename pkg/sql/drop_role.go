// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// DropRoleNode deletes entries from the system.users table.
// This is called from DROP USER and DROP ROLE.
type DropRoleNode struct {
	zeroInputPlanNode
	ifExists  bool
	isRole    bool
	roleNames []username.SQLUsername
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
	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEROLE); err != nil {
		return nil, err
	}

	for _, r := range roleSpecs {
		if r.RoleSpecType != tree.RoleName {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "cannot use special role specifier in DROP ROLE")
		}
	}
	roleNames, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeCreation, roleSpecs,
	)
	if err != nil {
		return nil, err
	}

	return &DropRoleNode{
		ifExists:  ifExists,
		isRole:    isRole,
		roleNames: roleNames,
	}, nil
}

type objectAndType struct {
	ObjectType         privilege.ObjectType
	ObjectName         string
	IsDefaultPrivilege bool
	IsGlobalPrivilege  bool
	ErrorMessage       error
}

func (n *DropRoleNode) startExec(params runParams) error {
	var opName redact.RedactableString
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

	userNames := make(map[username.SQLUsername][]objectAndType)
	for i, name := range n.roleNames {
		// userNames maps users to the objects they own
		userNames[n.roleNames[i]] = make([]objectAndType, 0)
		if name.IsReserved() {
			return pgerror.Newf(pgcode.ReservedName, "role name %q is reserved", name.Normalized())
		}

		// Non-admin users cannot drop admins.
		if !hasAdmin {
			if n.ifExists {
				// If `IF EXISTS` was specified, then a non-existing role should be
				// skipped without causing any error.
				roleExists, err := RoleExists(params.ctx, params.p.InternalSQLTxn(), name)
				if err != nil {
					return err
				}
				if !roleExists {
					// If the role does not exist, we can skip the check for targetIsAdmin.
					continue
				}
			}

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
		func(ctx context.Context, db catalog.DatabaseDescriptor) error {
			if _, ok := userNames[db.GetPrivileges().Owner()]; ok {
				userNames[db.GetPrivileges().Owner()] = append(
					userNames[db.GetPrivileges().Owner()],
					objectAndType{
						ObjectType: privilege.Database,
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
		if !descriptorIsVisible(tableDescriptor, true /*allowAdding*/, false /* includeDropped */) {
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
					ObjectType: privilege.Table,
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
		// Check that any of the roles we are dropping aren't referenced in any of
		// the row-level security policies defined on this table.
		for _, p := range tableDescriptor.GetPolicies() {
			for _, rn := range p.RoleNames {
				roleName := username.MakeSQLUsernameFromPreNormalizedString(rn)
				if _, found := userNames[roleName]; found {
					return errors.WithDetailf(
						pgerror.Newf(pgcode.DependentObjectsStillExist,
							"role %q cannot be dropped because some objects depend on it",
							roleName),
						"target of policy %q on table %q", p.Name, tableDescriptor.GetName())
				}
			}
		}
	}
	for _, schemaDesc := range lCtx.schemaDescs {
		if !descriptorIsVisible(schemaDesc, true /* allowAdding */, false /* includeDropped */) {
			continue
		}
		if _, ok := userNames[schemaDesc.GetPrivileges().Owner()]; ok {
			sn, err := getSchemaNameFromSchemaDescriptor(lCtx, schemaDesc)
			if err != nil {
				return err
			}
			userNames[schemaDesc.GetPrivileges().Owner()] = append(
				userNames[schemaDesc.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: privilege.Schema,
					ObjectName: sn.String(),
				})
		}

		dbDesc, err := lCtx.getDatabaseByID(schemaDesc.GetParentID())
		if err != nil {
			return err
		}

		for _, u := range schemaDesc.GetPrivileges().Users {
			if _, ok := userNames[u.User()]; ok {
				if privilegeObjectFormatter.Len() > 0 {
					privilegeObjectFormatter.WriteString(", ")
				}
				sn := tree.ObjectNamePrefix{
					ExplicitCatalog: true,
					CatalogName:     tree.Name(dbDesc.GetName()),
					ExplicitSchema:  true,
					SchemaName:      tree.Name(schemaDesc.GetName()),
				}
				privilegeObjectFormatter.FormatNode(&sn)
				break
			}
		}

		if err := accumulateDependentDefaultPrivileges(schemaDesc.GetDefaultPrivilegeDescriptor(), userNames, dbDesc.GetName(), schemaDesc.GetName()); err != nil {
			return err
		}
	}
	for _, typDesc := range lCtx.typDescs {
		if _, ok := userNames[typDesc.GetPrivileges().Owner()]; ok {
			if !descriptorIsVisible(typDesc, true /* allowAdding */, false /* includeDropped */) {
				continue
			}
			tn, err := getTypeNameFromTypeDescriptor(lCtx, typDesc)
			if err != nil {
				return err
			}
			userNames[typDesc.GetPrivileges().Owner()] = append(
				userNames[typDesc.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: privilege.Type,
					ObjectName: tn.String(),
				})
		}
		for _, u := range typDesc.GetPrivileges().Users {
			if _, ok := userNames[u.User()]; ok {
				tn, err := getTypeNameFromTypeDescriptor(lCtx, typDesc)
				if err != nil {
					return err
				}
				if privilegeObjectFormatter.Len() > 0 {
					privilegeObjectFormatter.WriteString(", ")
				}
				privilegeObjectFormatter.FormatNode(&tn)
				break
			}
		}
	}
	for _, fnDesc := range lCtx.fnDescs {
		if _, ok := userNames[fnDesc.GetPrivileges().Owner()]; ok {
			if !descriptorIsVisible(fnDesc, true /* allowAdding */, false /* includeDropped */) {
				continue
			}
			name, err := getFunctionNameFromFunctionDescriptor(lCtx, fnDesc)
			if err != nil {
				return err
			}
			userNames[fnDesc.GetPrivileges().Owner()] = append(
				userNames[fnDesc.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: privilege.Routine,
					ObjectName: name.String(),
				},
			)
		}

		for _, u := range fnDesc.GetPrivileges().Users {
			if _, ok := userNames[u.User()]; ok {
				name, err := getFunctionNameFromFunctionDescriptor(lCtx, fnDesc)
				if err != nil {
					return err
				}
				if privilegeObjectFormatter.Len() > 0 {
					privilegeObjectFormatter.WriteString(", ")
				}
				privilegeObjectFormatter.FormatNode(&name)
				break
			}
		}
	}

	if err := addDependentPrivilegesFromSystemPrivileges(params.ctx, n.roleNames, params.p, privilegeObjectFormatter, userNames); err != nil {
		return err
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
			if dependentObjects[j].ErrorMessage == nil {
				return false
			}
			if dependentObjects[i].ErrorMessage == nil {
				return true
			}
			return dependentObjects[i].ErrorMessage.Error() < dependentObjects[j].ErrorMessage.Error()
		})
		var hints []string
		if len(dependentObjects) > 0 {
			objectsMsg := tree.NewFmtCtx(tree.FmtSimple)
			for _, obj := range dependentObjects {
				if obj.IsDefaultPrivilege {
					hasDependentDefaultPrivilege = true
					objectsMsg.WriteString(fmt.Sprintf("\n%s", obj.ErrorMessage))
					hints = append(hints, errors.GetAllHints(obj.ErrorMessage)...)
				} else if obj.IsGlobalPrivilege {
					objectsMsg.WriteString(fmt.Sprintf("\n%s", obj.ErrorMessage))
				} else {
					objectsMsg.WriteString(fmt.Sprintf("\nowner of %s %s", obj.ObjectType, obj.ObjectName))
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
	var numRoleSettingsRowsDeleted int
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
		numSchedulesRow, err := params.p.InternalSQLTxn().QueryRowEx(
			params.ctx,
			"check-user-schedules",
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
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

		numUsersDeleted, err := params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.users WHERE username=$1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		if numUsersDeleted == 0 && !n.ifExists {
			return sqlerrors.NewUndefinedUserError(normalizedUsername)
		}

		// Drop all role memberships involving the user/role.
		if _, err = params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		); err != nil {
			return err
		}

		_, err = params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(
				`DELETE FROM system.public.%s WHERE username=$1`,
				catconstants.RoleOptionsTableName,
			),
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		if rowsDeleted, err := params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(
				`DELETE FROM system.public.%s WHERE role_name = $1`,
				catconstants.DatabaseRoleSettingsTableName,
			),
			normalizedUsername,
		); err != nil {
			return err
		} else {
			numRoleSettingsRowsDeleted += rowsDeleted
		}

		_, err = params.p.InternalSQLTxn().ExecEx(
			params.ctx,
			opName,
			params.p.txn,
			sessiondata.NodeUserSessionDataOverride,
			`UPDATE system.web_sessions SET "revokedAt" = now() WHERE username = $1 AND "revokedAt" IS NULL;`,
			normalizedUsername,
		)
		if err != nil {
			return err
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
		if numRoleSettingsRowsDeleted > 0 {
			if err := params.p.bumpDatabaseRoleSettingsTableVersion(params.ctx); err != nil {
				return err
			}
		}
	}
	if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
		return err
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
	userNames map[username.SQLUsername][]objectAndType,
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
	object privilege.TargetObjectType,
	defaultPrivs catpb.PrivilegeDescriptor,
	role catpb.DefaultPrivilegesRole,
	userNames map[username.SQLUsername][]objectAndType,
	dbName string,
	schemaName string,
) {
	var objectType string
	switch object {
	case privilege.Tables:
		objectType = "relations"
	case privilege.Sequences:
		objectType = "sequences"
	case privilege.Types:
		objectType = "types"
	case privilege.Schemas:
		objectType = "schemas"
	}

	inSchemaMsg := ""
	if schemaName != "" {
		inSchemaMsg = fmt.Sprintf(" in schema %s", schemaName)
	}

	createHint := func(
		role catpb.DefaultPrivilegesRole,
		grantee username.SQLUsername,
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
						IsDefaultPrivilege: true,
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
					IsDefaultPrivilege: true,
					ErrorMessage:       errors.WithHint(err, hint),
				})
		}
	}
}

func addDependentPrivilegesFromSystemPrivileges(
	ctx context.Context,
	usernames []username.SQLUsername,
	p *planner,
	privilegeObjectFormatter *tree.FmtCtx,
	userNamesToDependentPrivileges map[username.SQLUsername][]objectAndType,
) (retErr error) {
	names := make([]string, len(usernames))
	for i, username := range usernames {
		names[i] = username.Normalized()
	}
	rows, err := p.QueryIteratorEx(ctx, `drop-role-get-system-privileges`, sessiondata.NodeUserSessionDataOverride,
		`SELECT DISTINCT username, path, privileges FROM system.privileges WHERE username = ANY($1) ORDER BY 1, 2`, names)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, rows.Close())
	}()
	for {
		ok, err := rows.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		name := tree.MustBeDString(rows.Cur()[0])
		sqlUsername := username.MakeSQLUsernameFromPreNormalizedString(string(name))
		path := tree.MustBeDString(rows.Cur()[1])
		privileges := tree.MustBeDArray(rows.Cur()[2])
		obj, err := syntheticprivilege.Parse(string(path))
		if err != nil {
			return err
		}

		if obj.GetObjectType() == privilege.Global {
			for _, priv := range privileges.Array {
				userNamesToDependentPrivileges[sqlUsername] = append(
					userNamesToDependentPrivileges[sqlUsername],
					objectAndType{
						IsGlobalPrivilege: true,
						ErrorMessage: errors.Newf(
							"%s has global %s privilege", sqlUsername, priv.String(),
						)})
			}
			continue
		}
		if privilegeObjectFormatter.Len() > 0 {
			privilegeObjectFormatter.WriteString(", ")
		}
		privilegeObjectFormatter.WriteString(string(obj.GetObjectType()))
		if obj.GetName() != "" {
			privilegeObjectFormatter.WriteString(" ")
			privilegeObjectFormatter.FormatName(obj.GetName())
		}
	}
	return nil
}
