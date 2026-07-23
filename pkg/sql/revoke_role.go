// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/redact"
)

// RevokeRoleNode removes entries from the system.role_members table.
// This is called from REVOKE <ROLE>
type RevokeRoleNode struct {
	zeroInputPlanNode
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

	hasCreateRolePriv, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEROLE)
	if err != nil {
		return nil, err
	}

	// check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	revokingRoleHasAdminOptionOnAdmin := allRoles[username.AdminRoleName()]

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
		// If the current user has CREATEROLE, and the role being revoked is not an
		// admin there is no need to check if the user is allowed to grant/revoke
		// membership in the role. However, if the role being revoked is an admin,
		// then make sure the current user also has the admin option for that role.
		revokedRoleIsAdmin, err := p.UserHasAdminRole(ctx, r)
		if err != nil {
			return nil, err
		}
		if hasCreateRolePriv && !revokedRoleIsAdmin {
			continue
		}
		if hasAdminOption := allRoles[r]; !hasAdminOption && !revokingRoleHasAdminOptionOnAdmin {
			if revokedRoleIsAdmin {
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
	var opName redact.RedactableString = "revoke-role"

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
				sessiondata.NodeUserSessionDataOverride,
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
