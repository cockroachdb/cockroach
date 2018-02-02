// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	return p.CreateUserNode(ctx, createRole.Name, nil /* password */, createRole.IfNotExists, true /* isRole */, "CREATE ROLE")
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
	return p.DropUserNode(ctx, dropRole.Names, dropRole.IfExists, true /* isRole */, "DROP ROLE")
}

func grantRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	grant, ok := stmt.(*tree.GrantRole)
	if !ok {
		return nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "GRANT <role>",
	); err != nil {
		return nil, err
	}

	if err := p.RequireSuperUser(ctx, "grant role"); err != nil {
		// Not a superuser: check permissions on each role.
		allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
		if err != nil {
			return nil, err
		}
		for _, r := range grant.Roles {
			if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
				return nil, pgerror.NewErrorf(pgerror.CodeInsufficientPrivilegeError,
					"%s is not a superuser or role admin for role %s", p.User(), r)
			}
		}
	}

	// Check that users and roles exist.
	// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all users.
	// This is wasteful when we have a LOT of users compared to the number of users being operated on.
	users, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return nil, err
	}

	// Check roles: these have to be roles.
	for _, r := range grant.Roles {
		if isRole, ok := users[string(r)]; !ok || !isRole {
			return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError, "role %s does not exist", r)
		}
	}

	// Check grantees: these can be users or roles.
	for _, m := range grant.Members {
		if _, ok := users[string(m)]; !ok {
			return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError, "user or role %s does not exist", m)
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
			return nil, err
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
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidGrantOperationError, "%s cannot be a member of itself", m)
			}
			// Check if grant.Role ∈ ... ∈ grant.Member
			if memberOf, ok := allRoleMemberships[r]; ok {
				if _, ok = memberOf[m]; ok {
					return nil, pgerror.NewErrorf(pgerror.CodeInvalidGrantOperationError,
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
	memberStmt := `INSERT INTO system.public.role_members ("role", "member", "isAdmin") VALUES ($1, $2, $3) ON CONFLICT ("role", "member")`
	if grant.AdminOption {
		// admin option: true, set "isAdmin" even if the membership exists.
		memberStmt += ` DO UPDATE SET "isAdmin" = true`
	} else {
		// admin option: false, do not clear it from existing memberships.
		memberStmt += ` DO NOTHING`
	}

	internalExecutor := sql.InternalExecutor{ExecCfg: p.ExecCfg()}
	for _, r := range grant.Roles {
		for _, m := range grant.Members {
			_, err := internalExecutor.ExecuteStatementInTransaction(
				ctx,
				"grant-role",
				p.Txn(),
				memberStmt,
				r, m, grant.AdminOption,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	return &sql.ZeroNode{}, nil
}

func revokeRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	revoke, ok := stmt.(*tree.RevokeRole)
	if !ok {
		return nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "REVOKE <role>",
	); err != nil {
		return nil, err
	}

	if err := p.RequireSuperUser(ctx, "revoke role"); err != nil {
		// Not a superuser: check permissions on each role.
		allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
		if err != nil {
			return nil, err
		}
		for _, r := range revoke.Roles {
			if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
				return nil, pgerror.NewErrorf(pgerror.CodeInsufficientPrivilegeError,
					"%s is not a superuser or role admin for role %s", p.User(), r)
			}
		}
	}

	// Check that users and roles exist.
	// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all users.
	// This is wasteful when we have a LOT of users compared to the number of users being operated on.
	users, err := p.GetAllUsersAndRoles(ctx)
	if err != nil {
		return nil, err
	}

	// Check roles: these have to be roles.
	for _, r := range revoke.Roles {
		if isRole, ok := users[string(r)]; !ok || !isRole {
			return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError, "role %s does not exist", r)
		}
	}

	// Check members: these can be users or roles.
	for _, m := range revoke.Members {
		if _, ok := users[string(m)]; !ok {
			return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError, "user or role %s does not exist", m)
		}
	}

	var memberStmt string
	if revoke.AdminOption {
		// ADMIN OPTION FOR is specified, we don't remove memberships just remove the admin option.
		memberStmt = `UPDATE system.public.role_members SET "isAdmin" = false WHERE "role" = $1 AND "member" = $2`
	} else {
		// Admin option not specified: remove membership if it exists.
		memberStmt = `DELETE FROM system.public.role_members WHERE "role" = $1 AND "member" = $2`
	}

	internalExecutor := sql.InternalExecutor{ExecCfg: p.ExecCfg()}
	for _, r := range revoke.Roles {
		for _, m := range revoke.Members {
			if string(r) == sqlbase.AdminRole && string(m) == security.RootUser {
				// We use CodeObjectInUseError which is what happens if you tried to delete the current user in pg.
				return nil, pgerror.NewErrorf(pgerror.CodeObjectInUseError,
					"user %s cannot be removed from role %s or lose the ADMIN OPTION",
					security.RootUser, sqlbase.AdminRole)
			}
			_, err := internalExecutor.ExecuteStatementInTransaction(
				ctx,
				"revoke-role",
				p.Txn(),
				memberStmt,
				r, m,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	return &sql.ZeroNode{}, nil
}

func init() {
	sql.AddWrappedPlanHook(createRolePlanHook)
	sql.AddWrappedPlanHook(dropRolePlanHook)
	sql.AddWrappedPlanHook(grantRolePlanHook)
	sql.AddWrappedPlanHook(revokeRolePlanHook)
}
