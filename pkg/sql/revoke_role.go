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

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// RevokeRoleNode removes entries from the system.role_members table.
// This is called from REVOKE <ROLE>
type RevokeRoleNode struct {
	roles       []username.SQLUsername
	members     []username.SQLUsername
	adminOption bool
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

	for _, r := range inputRoles {
		if _, ok := roles[r]; !ok {
			return nil, sqlerrors.NewUndefinedUserError(r)
		}
	}

	for _, m := range inputMembers {
		if _, ok := roles[m]; !ok {
			return nil, sqlerrors.NewUndefinedUserError(m)
		}
	}

	return &RevokeRoleNode{
		roles:       inputRoles,
		members:     inputMembers,
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
	for _, r := range n.roles {
		for _, m := range n.members {
			if r.IsAdminRole() && m.IsRootUser() {
				// We use CodeObjectInUseError which is what happens if you tried to delete the current user in pg.
				return pgerror.Newf(pgcode.ObjectInUse,
					"role/user %s cannot be removed from role %s or lose the ADMIN OPTION",
					username.RootUser, username.AdminRole)
			}
			affected, err := params.p.InternalSQLTxn().ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sessiondata.RootUserSessionDataOverride,
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

	return nil
}

// Next implements the planNode interface.
func (*RevokeRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*RevokeRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*RevokeRoleNode) Close(context.Context) {}
