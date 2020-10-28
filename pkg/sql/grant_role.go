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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
	defer span.Finish()

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	// Check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	for i := range n.Roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in
		// n.Roles, but we want SQL usernames. Do we normalize or not? For
		// reference, REASSIGN / OWNER TO do normalize.  Related:
		// https://github.com/cockroachdb/cockroach/issues/54696
		r := security.MakeSQLUsernameFromPreNormalizedString(string(n.Roles[i]))

		// If the user is an admin, don't check if the user is allowed to add/drop
		// roles in the role. However, if the role being modified is the admin role, then
		// make sure the user is an admin with the admin option.
		if hasAdminRole && !r.IsAdminRole() {
			continue
		}
		if isAdmin, ok := allRoles[r]; !ok || !isAdmin {
			if r.IsAdminRole() {
				return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
					"%s is not a role admin for role %s", p.User(), n.Roles[i])
			}
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"%s is not a superuser or role admin for role %s", p.User(), n.Roles[i])
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

	for i := range n.Roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in
		// n.Roles, but we want SQL usernames. Do we normalize or not? For
		// reference, REASSIGN / OWNER TO do normalize.  Related:
		// https://github.com/cockroachdb/cockroach/issues/54696
		r := security.MakeSQLUsernameFromPreNormalizedString(string(n.Roles[i]))

		if _, ok := roles[r]; !ok {
			maybeOption := strings.ToUpper(r.Normalized())
			for name := range roleoption.ByName {
				if maybeOption == name {
					return nil, errors.WithHintf(
						pgerror.Newf(pgcode.UndefinedObject,
							"role/user %s does not exist", n.Roles[i]),
						"%s is a role option, try using ALTER ROLE to change a role's options.", maybeOption)
				}
			}
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", n.Roles[i])
		}
	}

	for i := range n.Members {
		// TODO(solon): there are SQL identifiers (tree.Name) in
		// n.Members but we want SQL usernames. Do we normalize or not? For
		// reference, REASSIGN / OWNER TO do normalize.  Related:
		// https://github.com/cockroachdb/cockroach/issues/54696
		m := security.MakeSQLUsernameFromPreNormalizedString(string(n.Members[i]))

		if _, ok := roles[m]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject,
				"role/user %s does not exist", n.Members[i])
		}
	}

	// Given an acyclic directed membership graph, adding a new edge (grant.Member ∈ grant.Role)
	// means checking whether we have an expanded relationship (grant.Role ∈ ... ∈ grant.Member)
	// For each grant.Role, we lookup all the roles it is a member of.
	// After adding a given edge (grant.Member ∈ grant.Role), we add the edge to the list as well.
	allRoleMemberships := make(map[security.SQLUsername]map[security.SQLUsername]bool)
	for _, rawR := range n.Roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in
		// n.Roles but we want SQL usernames. Do we normalize or not? For
		// reference, REASSIGN / OWNER TO do normalize.  Related:
		// https://github.com/cockroachdb/cockroach/issues/54696
		r := security.MakeSQLUsernameFromPreNormalizedString(string(rawR))

		allRoles, err := p.MemberOfWithAdminOption(ctx, r)
		if err != nil {
			return nil, err
		}
		allRoleMemberships[r] = allRoles
	}

	// Since we perform no queries here, check all role/member pairs for cycles.
	// Only if there are no errors do we proceed to write them.
	for _, rawR := range n.Roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in
		// n.Roles but we want SQL usernames. Do we normalize or not? For
		// reference, REASSIGN / OWNER TO do normalize.  Related:
		// https://github.com/cockroachdb/cockroach/issues/54696
		r := security.MakeSQLUsernameFromPreNormalizedString(string(rawR))
		for _, rawM := range n.Members {
			// TODO(solon): ditto above, names in n.Members.
			m := security.MakeSQLUsernameFromPreNormalizedString(string(rawM))
			if r == m {
				// self-cycle.
				return nil, pgerror.Newf(pgcode.InvalidGrantOperation, "%s cannot be a member of itself", rawM)
			}
			// Check if grant.Role ∈ ... ∈ grant.Member
			if memberOf, ok := allRoleMemberships[r]; ok {
				if _, ok = memberOf[m]; ok {
					return nil, pgerror.Newf(pgcode.InvalidGrantOperation,
						"making %s a member of %s would create a cycle", rawM, rawR)
				}
			}
			// Add the new membership. We don't care about the actual bool value.
			if _, ok := allRoleMemberships[m]; !ok {
				allRoleMemberships[m] = make(map[security.SQLUsername]bool)
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
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
