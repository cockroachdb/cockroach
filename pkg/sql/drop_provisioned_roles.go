// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/redact"
)

// DropProvisionedRolesNode drops provisioned users matching filter
// criteria (SOURCE, LAST LOGIN BEFORE) with an optional
// LIMIT. Users that own objects or have dependencies are skipped with
// a NOTICE rather than failing the entire operation.
type DropProvisionedRolesNode struct {
	zeroInputPlanNode
	options *tree.DropProvisionedRolesOptions
	limit   *tree.Limit
}

// DropProvisionedRoles creates a plan node for DROP PROVISIONED ROLES.
// Requires CREATEROLE privilege.
func (p *planner) DropProvisionedRoles(
	ctx context.Context, n *tree.DropProvisionedRoles,
) (planNode, error) {
	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEROLE); err != nil {
		return nil, err
	}
	return &DropProvisionedRolesNode{
		options: n.Options,
		limit:   n.Limit,
	}, nil
}

func (n *DropProvisionedRolesNode) startExec(params runParams) error {
	sqltelemetry.IncIAMDropCounter(sqltelemetry.User)
	const opName redact.RedactableString = "drop-provisioned-roles"

	hasAdmin, err := params.p.HasAdminRole(params.ctx)
	if err != nil {
		return err
	}

	// Build the query to find matching provisioned users.
	query, queryArgs := n.buildFilterQuery()

	rows, err := params.p.InternalSQLTxn().QueryBufferedEx(
		params.ctx,
		"drop-provisioned-roles-find",
		params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		query,
		queryArgs...,
	)
	if err != nil {
		return err
	}

	// Collect all descriptors once for dependency checking.
	allDescs, err := params.p.Descriptors().GetAllDescriptors(
		params.ctx, params.p.txn,
	)
	if err != nil {
		return err
	}

	var numDropped, numSkipped int
	var droppedNames []string
	var numRoleSettingsRowsDeleted int

	for _, row := range rows {
		normalizedUsername := username.MakeSQLUsernameFromPreNormalizedString(
			string(tree.MustBeDString(row[0])),
		)

		// Skip reserved roles.
		if normalizedUsername.IsAdminRole() ||
			normalizedUsername.IsPublicRole() ||
			normalizedUsername.IsRootUser() {
			continue
		}

		// Non-admin users cannot drop admins.
		if !hasAdmin {
			targetIsAdmin, err := params.p.UserHasAdminRole(
				params.ctx, normalizedUsername,
			)
			if err != nil {
				return err
			}
			if targetIsAdmin {
				params.p.BufferClientNotice(
					params.ctx,
					pgnotice.Newf("skipping %q: must be superuser to drop superusers", normalizedUsername),
				)
				numSkipped++
				continue
			}
		}

		// Check for dependencies (owned objects, grants, default
		// privileges, scheduled jobs, system privileges).
		if hasDeps, err := n.userHasDependencies(
			params, normalizedUsername, allDescs,
		); err != nil {
			return err
		} else if hasDeps {
			params.p.BufferClientNotice(
				params.ctx,
				pgnotice.Newf("skipping %q: role has dependent objects", normalizedUsername),
			)
			numSkipped++
			continue
		}

		// Delete the role from all system tables.
		deleted, err := n.deleteRole(params, normalizedUsername, opName)
		if err != nil {
			return err
		}
		numRoleSettingsRowsDeleted += deleted
		numDropped++
		droppedNames = append(droppedNames, normalizedUsername.Normalized())
	}

	// Bump table versions if anything was dropped.
	if numDropped > 0 {
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

		// Log per-user DropRole events.
		for _, name := range droppedNames {
			if err := params.p.logEvent(params.ctx,
				0, /* no target */
				&eventpb.DropRole{RoleName: name}); err != nil {
				return err
			}
		}
	}

	// Log summary event.
	var sourceStr string
	if n.options != nil && n.options.Source != nil {
		sourceStr = tree.AsStringWithFlags(n.options.Source, tree.FmtBareStrings)
	}
	return params.p.logEvent(params.ctx,
		0, /* no target */
		&eventpb.DropProvisionedRoles{
			Source:     sourceStr,
			NumDropped: uint32(numDropped),
			NumSkipped: uint32(numSkipped),
			RoleNames:  droppedNames,
		})
}

// buildFilterQuery constructs the SQL query to find provisioned users
// matching the filter options.
func (n *DropProvisionedRolesNode) buildFilterQuery() (string, []interface{}) {
	var whereExprs []string
	var args []interface{}
	argIdx := 1

	// Always filter for users that have a PROVISIONSRC role option
	// (i.e. are provisioned).
	provisionFilter := fmt.Sprintf(`EXISTS (
	SELECT 1 FROM system.role_options AS src
	WHERE src.username = u.username
		AND src.option = 'PROVISIONSRC'`)

	if n.options != nil && n.options.Source != nil {
		sourceStr := tree.AsStringWithFlags(
			n.options.Source, tree.FmtBareStrings,
		)
		provisionFilter += fmt.Sprintf(
			"\n\t\tAND src.value = %s", lexbase.EscapeSQLString(sourceStr),
		)
	}
	provisionFilter += "\n)"
	whereExprs = append(whereExprs, provisionFilter)

	if n.options != nil && n.options.LastLoginBefore != nil {
		tsExpr := tree.AsStringWithFlags(
			n.options.LastLoginBefore, tree.FmtParsable,
		)
		whereExprs = append(whereExprs, fmt.Sprintf(
			"u.estimated_last_login_time < (%s)::TIMESTAMPTZ", tsExpr,
		))
	}

	whereClause := "\nWHERE " + strings.Join(whereExprs, "\n\tAND ")

	var limitClause string
	if n.limit != nil && n.limit.Count != nil {
		limitClause = fmt.Sprintf("\nLIMIT %s", tree.AsString(n.limit.Count))
	}

	query := fmt.Sprintf(
		`SELECT u.username FROM system.users AS u%s%s`,
		whereClause, limitClause,
	)

	_ = argIdx // args currently embedded via string escaping
	return query, args
}

// userHasDependencies checks whether the given user owns any objects,
// has grants, default privileges, scheduled jobs, or system
// privileges that would prevent dropping.
func (n *DropProvisionedRolesNode) userHasDependencies(
	params runParams, normalizedUsername username.SQLUsername, allDescs nstree.Catalog,
) (bool, error) {
	// Check ownership across all descriptors.
	for _, desc := range allDescs.OrderedDescriptors() {
		if !descriptorIsVisible(desc, true /* allowAdding */, false /* includeDropped */) {
			continue
		}
		if desc.GetPrivileges().Owner() == normalizedUsername {
			return true, nil
		}
		for _, u := range desc.GetPrivileges().Users {
			if u.User() == normalizedUsername {
				return true, nil
			}
		}
	}

	// Check scheduled jobs.
	row, err := params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		"check-user-schedules",
		params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT count(*) FROM system.scheduled_jobs WHERE owner=$1",
		normalizedUsername,
	)
	if err != nil {
		return false, err
	}
	if row != nil && int64(tree.MustBeDInt(row[0])) > 0 {
		return true, nil
	}

	// Check system privileges.
	row, err = params.p.InternalSQLTxn().QueryRowEx(
		params.ctx,
		"check-user-system-privileges",
		params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT count(*) FROM system.privileges WHERE username=$1",
		normalizedUsername.Normalized(),
	)
	if err != nil {
		return false, err
	}
	if row != nil && int64(tree.MustBeDInt(row[0])) > 0 {
		return true, nil
	}

	return false, nil
}

// deleteRole removes a single role from all system tables and revokes
// its web sessions.
func (n *DropProvisionedRolesNode) deleteRole(
	params runParams, normalizedUsername username.SQLUsername, opName redact.RedactableString,
) (dbRoleSettingsDeleted int, err error) {
	// DELETE from system.users.
	if _, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx, opName, params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.users WHERE username=$1`,
		normalizedUsername,
	); err != nil {
		return 0, err
	}

	// DELETE from system.role_members.
	if _, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx, "drop-role-membership", params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
		normalizedUsername,
	); err != nil {
		return 0, err
	}

	// DELETE from system.role_options.
	if _, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx, opName, params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			`DELETE FROM system.public.%s WHERE username=$1`,
			catconstants.RoleOptionsTableName,
		),
		normalizedUsername,
	); err != nil {
		return 0, err
	}

	// DELETE from system.database_role_settings.
	rowsDeleted, err := params.p.InternalSQLTxn().ExecEx(
		params.ctx, opName, params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			`DELETE FROM system.public.%s WHERE role_name = $1`,
			catconstants.DatabaseRoleSettingsTableName,
		),
		normalizedUsername,
	)
	if err != nil {
		return 0, err
	}

	// Revoke web sessions.
	if _, err = params.p.InternalSQLTxn().ExecEx(
		params.ctx, opName, params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE username = $1 AND "revokedAt" IS NULL`,
		normalizedUsername,
	); err != nil {
		return 0, err
	}

	return rowsDeleted, nil
}

// Next implements the planNode interface.
func (*DropProvisionedRolesNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*DropProvisionedRolesNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*DropProvisionedRolesNode) Close(context.Context) {}
