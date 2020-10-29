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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// RevokeRoleNode removes entries from the system.role_members table.
// This is called from REVOKE <ROLE>
type RevokeRoleNode struct {
	roles       tree.NameList
	members     tree.NameList
	adminOption bool

	run revokeRoleRun
}

type revokeRoleRun struct {
	rowsAffected int
}

// RevokeRole represents a GRANT ROLE statement.
func (p *planner) RevokeRole(ctx context.Context, n *tree.RevokeRole) (planNode, error) {
	return p.RevokeRoleNode(ctx, n)
}

func (p *planner) RevokeRoleNode(ctx context.Context, n *tree.RevokeRole) (*RevokeRoleNode, error) {
	sqltelemetry.IncIAMRevokeCounter(n.AdminOption)

	ctx, span := tracing.ChildSpan(ctx, n.StatementTag())
	defer span.Finish()

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	// check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	for i := range n.Roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in n.Roles,
		// but we want SQL usernames. Do we normalize or not? For reference,
		// REASSIGN / OWNER TO do normalize.
		// Related: https://github.com/cockroachdb/cockroach/issues/54696
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

	for i := range n.Roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in n.Roles,
		// but we want SQL usernames. Do we normalize or not? For reference,
		// REASSIGN / OWNER TO do normalize.
		// Related: https://github.com/cockroachdb/cockroach/issues/54696
		r := security.MakeSQLUsernameFromPreNormalizedString(string(n.Roles[i]))

		if _, ok := roles[r]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", n.Roles[i])
		}
	}

	for i := range n.Members {
		// TODO(solon): there are SQL identifiers (tree.Name) in n.Roles,
		// but we want SQL usernames. Do we normalize or not? For reference,
		// REASSIGN / OWNER TO do normalize.
		// Related: https://github.com/cockroachdb/cockroach/issues/54696
		m := security.MakeSQLUsernameFromPreNormalizedString(string(n.Members[i]))

		if _, ok := roles[m]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", n.Members[i])
		}
	}

	return &RevokeRoleNode{
		roles:       n.Roles,
		members:     n.Members,
		adminOption: n.AdminOption,
	}, nil
}

func (n *RevokeRoleNode) startExec(params runParams) error {
	opName := "revoke-role"

	var memberStmt string
	if n.adminOption {
		// ADMIN OPTION FOR is specified, we don't remove memberships just remove the admin option.
		memberStmt = `UPDATE system.role_members SET "isAdmin" = false WHERE "role" = $1 AND "member" = $2`
	} else {
		// Admin option not specified: remove membership if it exists.
		memberStmt = `DELETE FROM system.role_members WHERE "role" = $1 AND "member" = $2`
	}

	var rowsAffected int
	for i := range n.roles {
		// TODO(solon): there are SQL identifiers (tree.Name) in
		// n.Roles, but we want SQL usernames. Do we normalize or not? For
		// reference, REASSIGN / OWNER TO do normalize.  Related:
		// https://github.com/cockroachdb/cockroach/issues/54696
		r := security.MakeSQLUsernameFromPreNormalizedString(string(n.roles[i]))

		for j := range n.members {
			// TODO(solon): ditto above, names in n.members.
			m := security.MakeSQLUsernameFromPreNormalizedString(string(n.members[j]))

			if r.IsAdminRole() && m.IsRootUser() {
				// We use CodeObjectInUseError which is what happens if you tried to delete the current user in pg.
				return pgerror.Newf(pgcode.ObjectInUse,
					"role/user %s cannot be removed from role %s or lose the ADMIN OPTION",
					security.RootUser, security.AdminRole)
			}
			affected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				memberStmt,
				r.Normalized(), m.Normalized(),
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
func (*RevokeRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*RevokeRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*RevokeRoleNode) Close(context.Context) {}
