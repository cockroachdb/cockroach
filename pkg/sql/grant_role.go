// Copyright 2020 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// GrantRoleNode creates entries in the system.role_members table.
// This is called from GRANT <ROLE>
type GrantRoleNode struct {
	roles       tree.NameList
	members     tree.NameList
	adminOption bool

	run grantRoleRun
}

type grantRoleRun struct {
	rowsAffected int
}

// GrantRole represents a GRANT ROLE statement.
func (p *planner) GrantRole(ctx context.Context, n *tree.GrantRole) (planNode, error) {
	return p.GrantRoleNode(ctx, n)
}

func (p *planner) GrantRoleNode(ctx context.Context, n *tree.GrantRole) (*GrantRoleNode, error) {
	sqltelemetry.IncIAMGrantCounter(n.AdminOption)

	ctx, span := tracing.ChildSpan(ctx, n.StatementTag())
	defer tracing.FinishSpan(span)

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	// Check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	for _, r := range n.Roles {
		// If the user is an admin, don't check if the user is allowed to add/drop
		// roles in the role. However, if the role being modified is the admin role, then
		// make sure the user is an admin with the admin option.
		if hasAdminRole && string(r) != sqlbase.AdminRole {
			continue
		}
		if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
			if string(r) == sqlbase.AdminRole {
				return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
					"%s is not a role admin for role %s", p.User(), r)
			}
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"%s is not a superuser or role admin for role %s", p.User(), r)
		}
	}

	// Check that roles exist.
	// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all roles.
	// This is wasteful when we have a LOT of roles compared to the number of roles being operated on.
	roles, err := p.GetAllRoles(ctx)
	if err != nil {
		return nil, err
	}

	// NOTE: membership manipulation involving the "public" pseudo-role fails with
	// "role public does not exist". This matches postgres behavior.

	for _, r := range n.Roles {
		if _, ok := roles[string(r)]; !ok {
			for name := range roleoption.ByName {
				if uppercase := strings.ToUpper(string(r)); uppercase == name {
					return nil, errors.WithHintf(
						pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r),
						"%s is a role option, try using ALTER ROLE to change a role's options.", uppercase)
				}
			}
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r)
		}
	}

	for _, m := range n.Members {
		if _, ok := roles[string(m)]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", m)
		}
	}

	// Given an acyclic directed membership graph, adding a new edge (grant.Member ∈ grant.Role)
	// means checking whether we have an expanded relationship (grant.Role ∈ ... ∈ grant.Member)
	// For each grant.Role, we lookup all the roles it is a member of.
	// After adding a given edge (grant.Member ∈ grant.Role), we add the edge to the list as well.
	allRoleMemberships := make(map[string]map[string]bool)
	for _, rawR := range n.Roles {
		r := string(rawR)
		allRoles, err := p.MemberOfWithAdminOption(ctx, r)
		if err != nil {
			return nil, err
		}
		allRoleMemberships[r] = allRoles
	}

	// Since we perform no queries here, check all role/member pairs for cycles.
	// Only if there are no errors do we proceed to write them.
	for _, rawR := range n.Roles {
		r := string(rawR)
		for _, rawM := range n.Members {
			m := string(rawM)
			if r == m {
				// self-cycle.
				return nil, pgerror.Newf(pgcode.InvalidGrantOperation, "%s cannot be a member of itself", m)
			}
			// Check if grant.Role ∈ ... ∈ grant.Member
			if memberOf, ok := allRoleMemberships[r]; ok {
				if _, ok = memberOf[m]; ok {
					return nil, pgerror.Newf(pgcode.InvalidGrantOperation,
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

	return &GrantRoleNode{
		roles:       n.Roles,
		members:     n.Members,
		adminOption: n.AdminOption,
	}, nil
}

func (n *GrantRoleNode) startExec(params runParams) error {
	opName := "grant-role"
	// Add memberships. Existing memberships are allowed.
	// If admin option is false, we do not remove it from existing memberships.
	memberStmt := `INSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, $3) ON CONFLICT ("role", "member")`
	if n.adminOption {
		// admin option: true, set "isAdmin" even if the membership exists.
		memberStmt += ` DO UPDATE SET "isAdmin" = true`
	} else {
		// admin option: false, do not clear it from existing memberships.
		memberStmt += ` DO NOTHING`
	}

	var rowsAffected int
	for _, r := range n.roles {
		for _, m := range n.members {
			affected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				memberStmt,
				r, m, n.adminOption,
			)
			if err != nil {
				return err
			}

			rowsAffected += affected
		}
	}

	// We need to bump the table version to trigger a refresh if anything changed.
	if rowsAffected > 0 {
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	n.run.rowsAffected += rowsAffected

	return nil
}

// Next implements the planNode interface.
func (*GrantRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*GrantRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*GrantRoleNode) Close(context.Context) {}
