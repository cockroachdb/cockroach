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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/authentication"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	type objectAndType struct {
		ObjectType string
		ObjectName string
	}

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

	f := tree.NewFmtCtx(tree.FmtSimple)
	defer f.Close()

	// First check all the databases.
	if err := forEachDatabaseDesc(params.ctx, params.p, nil /*nil prefix = all databases*/, true, /* requiresPrivileges */
		func(db catalog.DatabaseDescriptor) error {
			if _, ok := userNames[db.GetPrivileges().Owner()]; ok {
				userNames[db.GetPrivileges().Owner()] = append(
					userNames[db.GetPrivileges().Owner()],
					objectAndType{
						ObjectType: "database",
						ObjectName: db.GetName(),
					})
			}
			for _, u := range db.GetPrivileges().Users {
				if _, ok := userNames[u.User()]; ok {
					if f.Len() > 0 {
						f.WriteString(", ")
					}
					f.FormatName(db.GetName())
					break
				}
			}
			return nil
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
		table := lCtx.tbDescs[tbID]
		if !descriptorIsVisible(table, true /*allowAdding*/) {
			continue
		}
		if _, ok := userNames[table.GetPrivileges().Owner()]; ok {
			tn, err := getTableNameFromTableDescriptor(lCtx, table, "")
			if err != nil {
				return err
			}
			userNames[table.GetPrivileges().Owner()] = append(
				userNames[table.GetPrivileges().Owner()],
				objectAndType{
					ObjectType: "table",
					ObjectName: tn.String(),
				})
		}
		for _, u := range table.GetPrivileges().Users {
			if _, ok := userNames[u.User()]; ok {
				if f.Len() > 0 {
					f.WriteString(", ")
				}
				parentName := lCtx.getDatabaseName(table)
				schemaName := lCtx.getSchemaName(table)
				tn := tree.MakeTableNameWithSchema(tree.Name(parentName), tree.Name(schemaName), tree.Name(table.GetName()))
				f.FormatNode(&tn)
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
					ObjectType: "schema",
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
					ObjectType: "type",
					ObjectName: tn.String(),
				})
		}
	}

	// Was there any object depending on that user?
	if f.Len() > 0 {
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
			fnl.String(), f.String(),
		)
	}

	for i := range names {
		// Name already normalized above.
		name := security.MakeSQLUsernameFromPreNormalizedString(names[i])
		// Did the user own any objects?
		ownedObjects := userNames[name]
		if len(ownedObjects) > 0 {
			objectsMsg := tree.NewFmtCtx(tree.FmtSimple)
			for _, obj := range ownedObjects {
				objectsMsg.WriteString(fmt.Sprintf("\nowner of %s %s", obj.ObjectType, obj.ObjectName))
			}
			objects := objectsMsg.CloseAndGetString()
			return pgerror.Newf(pgcode.DependentObjectsStillExist,
				"role %s cannot be dropped because some objects depend on it%s",
				name, objects)
		}
	}

	// All safe - do the work.
	var numRoleMembershipsDeleted int
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
		numRoleMembershipsDeleted, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-role-membership",
			params.p.txn,
			`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
			normalizedUsername,
		)
		if err != nil {
			return err
		}

		_, err = params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			opName,
			params.p.txn,
			fmt.Sprintf(
				`DELETE FROM %s WHERE username=$1`,
				authentication.RoleOptionsTableName,
			),
			normalizedUsername,
		)
		if err != nil {
			return err
		}
	}

	// Bump role-related table versions to force a refresh of membership/password
	// caches.
	if authentication.CacheEnabled.Get(&params.p.ExecCfg().Settings.SV) {
		if err := params.p.bumpUsersTableVersion(params.ctx); err != nil {
			return err
		}
		if err := params.p.bumpRoleOptionsTableVersion(params.ctx); err != nil {
			return err
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
