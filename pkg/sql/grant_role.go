// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// GrantRoleNode creates entries in the system.role_members table.
// This is called from GRANT <ROLE>
type GrantRoleNode struct {
	zeroInputPlanNode
	roles       []username.SQLUsername
	members     []username.SQLUsername
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

	hasCreateRolePriv, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEROLE)
	if err != nil {
		return nil, err
	}

	// Check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	grantingRoleHasAdminOptionOnAdmin := allRoles[username.AdminRoleName()]

	inputRoles, err := decodeusername.FromNameList(n.Roles)
	if err != nil {
		return nil, err
	}
	inputMembers, err := decodeusername.FromRoleSpecList(
		p.SessionData(), username.PurposeValidation, n.Members,
	)
	if err != nil {
		return nil, err
	}

	for _, r := range inputRoles {
		// If the current user has CREATEROLE, and the role being granted is not an
		// admin there is no need to check if the user is allowed to grant/revoke
		// membership in the role. However, if the role being granted is an admin,
		// then make sure the current user also has the admin option for that role.
		grantedRoleIsAdmin, err := p.UserHasAdminRole(ctx, r)
		if err != nil {
			return nil, err
		}
		if hasCreateRolePriv && !grantedRoleIsAdmin {
			continue
		}
		if hasAdminOption := allRoles[r]; !hasAdminOption && !grantingRoleHasAdminOptionOnAdmin {
			if grantedRoleIsAdmin {
				return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
					"%s must have admin option on role %q", p.User(), r)
			}
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"%s must have CREATEROLE or have admin option on role %q", p.User(), r)
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

	for _, r := range inputRoles {
		if _, ok := roles[r]; !ok {
			maybeOption := strings.ToUpper(r.Normalized())
			for name := range roleoption.ByName {
				if maybeOption == name {
					return nil, errors.WithHintf(
						sqlerrors.NewUndefinedUserError(r),
						"%s is a role option, try using ALTER ROLE to change a role's options.", maybeOption)
				}
			}
			return nil, sqlerrors.NewUndefinedUserError(r)
		}
	}

	for _, m := range inputMembers {
		if _, ok := roles[m]; !ok {
			return nil, sqlerrors.NewUndefinedUserError(m)
		}
	}

	// Given an acyclic directed membership graph, adding a new edge (grant.Member ∈ grant.Role)
	// means checking whether we have an expanded relationship (grant.Role ∈ ... ∈ grant.Member)
	// For each grant.Role, we lookup all the roles it is a member of.
	// After adding a given edge (grant.Member ∈ grant.Role), we add the edge to the list as well.
	allRoleMemberships := make(map[username.SQLUsername]map[username.SQLUsername]bool)
	for _, r := range inputRoles {
		allRoles, err := p.MemberOfWithAdminOption(ctx, r)
		if err != nil {
			return nil, err
		}
		allRoleMemberships[r] = allRoles
	}

	// Since we perform no queries here, check all role/member pairs for cycles.
	// Only if there are no errors do we proceed to write them.
	for _, r := range inputRoles {
		for _, m := range inputMembers {
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
				allRoleMemberships[m] = make(map[username.SQLUsername]bool)
			}
			allRoleMemberships[m][r] = false
		}
	}

	return &GrantRoleNode{
		roles:       inputRoles,
		members:     inputMembers,
		adminOption: n.AdminOption,
	}, nil
}

func (n *GrantRoleNode) startExec(params runParams) error {
	var rowsAffected int

	// Add memberships. Existing memberships are allowed.
	// If admin option is false, we do not remove it from existing memberships.
	memberStmt := `
INSERT INTO system.role_members ("role", "member", "isAdmin", role_id, member_id)
VALUES ($1, $2, $3, (SELECT user_id FROM system.users WHERE username = $1), (SELECT user_id FROM system.users WHERE username = $2))
ON CONFLICT ("role", "member")`
	if n.adminOption {
		// admin option: true, set "isAdmin" even if the membership exists.
		memberStmt += ` DO UPDATE SET "isAdmin" = true WHERE role_members."isAdmin" IS NOT true`
	} else {
		// admin option: false, do not clear it from existing memberships.
		memberStmt += ` DO NOTHING`
	}

	for _, r := range n.roles {
		for _, m := range n.members {
			memberStmtRowsAffected, err := params.p.InternalSQLTxn().ExecEx(
				params.ctx, "grant-role", params.p.Txn(),
				sessiondata.NodeUserSessionDataOverride,
				memberStmt,
				r.Normalized(),
				m.Normalized(),
				n.adminOption,
			)
			if err != nil {
				return err
			}
			rowsAffected += memberStmtRowsAffected
		}
	}

	// We need to bump the table version to trigger a refresh if anything changed.
	if rowsAffected > 0 {
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	n.run.rowsAffected += rowsAffected

	sqlUsernameToStrings := func(sqlUsernames []username.SQLUsername) []string {
		strings := make([]string, len(sqlUsernames))
		for i, sqlUsername := range sqlUsernames {
			strings[i] = sqlUsername.Normalized()
		}
		return strings
	}

	return params.p.logEvent(params.ctx,
		0, /* no target */
		&eventpb.GrantRole{GranteeRoles: sqlUsernameToStrings(n.roles), Members: sqlUsernameToStrings(n.members)})
}

// Next implements the planNode interface.
func (*GrantRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*GrantRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*GrantRoleNode) Close(context.Context) {}
