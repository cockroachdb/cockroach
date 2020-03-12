// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package roleccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func createRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	createRole, ok := stmt.(*tree.CreateRole)
	if !ok {
		return nil, nil
	}

	cfg := p.ExecCfg()
	if err := utilccl.CheckEnterpriseEnabled(
		cfg.Settings, cfg.ClusterID(), cfg.Organization(), "CREATE ROLE",
	); err != nil {
		return nil, err
	}

	// Call directly into the OSS code.
	return p.CreateRoleNode(ctx, createRole.Name, createRole.IfNotExists, true, /* isRole */
		"CREATE ROLE", createRole.KVOptions)
}

func alterRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	alterRole, ok := stmt.(*tree.AlterRole)
	if !ok {
		return nil, nil
	}

	cfg := p.ExecCfg()
	if err := utilccl.CheckEnterpriseEnabled(
		cfg.Settings, cfg.ClusterID(), cfg.Organization(), "ALTER ROLE",
	); err != nil {
		return nil, err
	}

	// Call directly into the OSS code.
	return p.AlterRoleNode(ctx, alterRole.Name, alterRole.IfExists, true, /* isRole */
		"CREATE ROLE", alterRole.KVOptions)
}

func dropRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	dropRole, ok := stmt.(*tree.DropRole)
	if !ok {
		return nil, nil
	}

	cfg := p.ExecCfg()
	if err := utilccl.CheckEnterpriseEnabled(
		cfg.Settings, cfg.ClusterID(), cfg.Organization(), "DROP ROLE",
	); err != nil {
		return nil, err
	}

	// Call directly into the OSS code.
	return p.DropRoleNode(ctx, dropRole.Names, dropRole.IfExists, true, /* isRole */
		"DROP ROLE")
}

func grantRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (
	fn sql.PlanHookRowFn,
	header sqlbase.ResultColumns,
	subplans []sql.PlanNode,
	avoidBuffering bool,
	err error,
) {
	grant, ok := stmt.(*tree.GrantRole)
	if !ok {
		return nil, nil, nil, false, nil
	}

	sqltelemetry.IncIAMGrantCounter(grant.AdminOption)

	fn = func(ctx context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		// Note: we do not check the license for GRANT <role>, only for
		// CREATE/DROP <role>. This is because we want to allow
		// non-licensed users to add/remove users from the admin role, so
		// they can grant administrative privileges to user accounts that
		// are not superusers like "root".

		hasAdminRole, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		// Check permissions on each role.
		allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
		if err != nil {
			return err
		}
		for _, r := range grant.Roles {
			// If the user is an admin, don't check if the user is allowed to add/drop
			// roles in the role. However, if the role being modified is the admin role, then
			// make sure the user is an admin with the admin option.
			if hasAdminRole && string(r) != sqlbase.AdminRole {
				continue
			}
			if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
				errMsg := "%s is not a superuser or role admin for role %s"
				if string(r) == sqlbase.AdminRole {
					errMsg = "%s is not a role admin for role %s"
				}
				return pgerror.Newf(pgcode.InsufficientPrivilege, errMsg, p.User(), r)
			}
		}

		// Check that roles exist.
		// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all roles.
		// This is wasteful when we have a LOT of roles compared to the number of roles being operated on.
		roles, err := p.GetAllRoles(ctx)
		if err != nil {
			return err
		}

		// NOTE: membership manipulation involving the "public" pseudo-role fails with
		// "role public does not exist". This matches postgres behavior.

		for _, r := range grant.Roles {
			if _, ok := roles[string(r)]; !ok {
				return pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r)
			}
		}

		for _, m := range grant.Members {
			if _, ok := roles[string(m)]; !ok {
				return pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", m)
			}
		}

		// Given an acyclic directed membership graph, adding a new edge (grant.Member ∈ grant.Role)
		// means checking whether we have an expanded relationship (grant.Role ∈ ... ∈ grant.Member)
		// For each grant.Role, we lookup all the roles it is a member of.
		// After adding a given edge (grant.Member ∈ grant.Role), we add the edge to the list as well.
		allRoleMemberships := make(map[string]map[string]bool)
		for _, rawR := range grant.Roles {
			r := string(rawR)
			allRoles, err := p.MemberOfWithAdminOption(ctx, r)
			if err != nil {
				return err
			}
			allRoleMemberships[r] = allRoles
		}

		// Since we perform no queries here, check all role/member pairs for cycles.
		// Only if there are no errors do we proceed to write them.
		for _, rawR := range grant.Roles {
			r := string(rawR)
			for _, rawM := range grant.Members {
				m := string(rawM)
				if r == m {
					// self-cycle.
					return pgerror.Newf(pgcode.InvalidGrantOperation, "%s cannot be a member of itself", m)
				}
				// Check if grant.Role ∈ ... ∈ grant.Member
				if memberOf, ok := allRoleMemberships[r]; ok {
					if _, ok = memberOf[m]; ok {
						return pgerror.Newf(pgcode.InvalidGrantOperation,
							"making %s a member of %s would create a cycle", m, r)
					}
				}
				// Add the new membership. We don't care about the actual bool value.
				if _, ok := allRoleMemberships[m]; !ok {
					allRoleMemberships[m] = make(map[string]bool)
				}
				allRoleMemberships[m][r] = false
			}
		}

		// Add memberships. Existing memberships are allowed.
		// If admin option is false, we do not remove it from existing memberships.
		memberStmt := `INSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, $3) ON CONFLICT ("role", "member")`
		if grant.AdminOption {
			// admin option: true, set "isAdmin" even if the membership exists.
			memberStmt += ` DO UPDATE SET "isAdmin" = true`
		} else {
			// admin option: false, do not clear it from existing memberships.
			memberStmt += ` DO NOTHING`
		}

		var rowsAffected int
		for _, r := range grant.Roles {
			for _, m := range grant.Members {
				affected, err := p.ExecCfg().InternalExecutor.Exec(
					ctx, "grant-role", p.Txn(),
					memberStmt,
					r, m, grant.AdminOption,
				)
				if err != nil {
					return err
				}

				rowsAffected += affected
			}
		}

		// We need to bump the table version to trigger a refresh if anything changed.
		if rowsAffected > 0 {
			if err := p.BumpRoleMembershipTableVersion(ctx); err != nil {
				return err
			}
		}

		return nil
	}

	return fn, sqlbase.ResultColumns{}, nil, false, nil
}

func revokeRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (
	fn sql.PlanHookRowFn,
	header sqlbase.ResultColumns,
	subplans []sql.PlanNode,
	avoidBuffering bool,
	err error,
) {
	revoke, ok := stmt.(*tree.RevokeRole)
	if !ok {
		return nil, nil, nil, false, nil
	}

	sqltelemetry.IncIAMRevokeCounter(revoke.AdminOption)

	fn = func(ctx context.Context, _ []sql.PlanNode, _ chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		// Note: we do not check the license for REVOKE <role>, only for
		// CREATE/DROP <role>. This is because we want to allow
		// non-licensed users to add/remove users from the admin role, so
		// they can grant administrative privileges to user accounts that
		// are not superusers like "root".

		hasAdminRole, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		// check permissions on each role.
		allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
		if err != nil {
			return err
		}
		for _, r := range revoke.Roles {
			// If the user is an admin, don't check if the user is allowed to add/drop
			// roles in the role. However, if the role being modified is the admin role, then
			// make sure the user is an admin with the admin option.
			if hasAdminRole && string(r) != sqlbase.AdminRole {
				continue
			}
			if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
				errMsg := "%s is not a superuser or role admin for role %s"
				if string(r) == sqlbase.AdminRole {
					errMsg = "%s is not a role admin for role %s"
				}
				return pgerror.Newf(pgcode.InsufficientPrivilege, errMsg, p.User(), r)
			}
		}

		// Check that roles exist.
		// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all roles.
		// This is wasteful when we have a LOT of roles compared to the number of roles being operated on.
		roles, err := p.GetAllRoles(ctx)
		if err != nil {
			return err
		}

		for _, r := range revoke.Roles {
			if _, ok := roles[string(r)]; !ok {
				return pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r)
			}
		}

		for _, m := range revoke.Members {
			if _, ok := roles[string(m)]; !ok {
				return pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", m)
			}
		}

		var memberStmt string
		if revoke.AdminOption {
			// ADMIN OPTION FOR is specified, we don't remove memberships just remove the admin option.
			memberStmt = `UPDATE system.role_members SET "isAdmin" = false WHERE "role" = $1 AND "member" = $2`
		} else {
			// Admin option not specified: remove membership if it exists.
			memberStmt = `DELETE FROM system.role_members WHERE "role" = $1 AND "member" = $2`
		}

		var rowsAffected int
		for _, r := range revoke.Roles {
			for _, m := range revoke.Members {
				if string(r) == sqlbase.AdminRole && string(m) == security.RootUser {
					// We use CodeObjectInUseError which is what happens if you tried to delete the current user in pg.
					return pgerror.Newf(pgcode.ObjectInUse,
						"role/user %s cannot be removed from role %s or lose the ADMIN OPTION",
						security.RootUser, sqlbase.AdminRole)
				}
				affected, err := p.ExecCfg().InternalExecutor.Exec(
					ctx, "revoke-role", p.Txn(),
					memberStmt,
					r, m,
				)
				if err != nil {
					return err
				}

				rowsAffected += affected
			}
		}

		// We need to bump the table version to trigger a refresh if anything changed.
		if rowsAffected > 0 {
			if err := p.BumpRoleMembershipTableVersion(ctx); err != nil {
				return err
			}
		}

		return nil
	}

	return fn, sqlbase.ResultColumns{}, nil, false, nil
}

func init() {
	sql.AddWrappedPlanHook(createRolePlanHook)
	sql.AddWrappedPlanHook(alterRolePlanHook)
	sql.AddWrappedPlanHook(dropRolePlanHook)
	sql.AddPlanHook(grantRolePlanHook)
	sql.AddPlanHook(revokeRolePlanHook)
}
