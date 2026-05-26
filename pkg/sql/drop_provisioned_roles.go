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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/redact"
)

// maxDropProvisionedRolesLimit is the maximum number of roles that can
// be dropped in a single DROP PROVISIONED ROLES statement. Each role
// requires dependency checks across all descriptors plus multiple
// system table mutations, so we cap the batch size to keep
// transactions bounded.
const maxDropProvisionedRolesLimit = 1024

// DropProvisionedRolesNode drops provisioned users matching filter
// criteria (SOURCE, LAST LOGIN BEFORE) with a required LIMIT. Users
// that own objects or have dependencies are skipped with a NOTICE
// rather than failing the entire operation.
type DropProvisionedRolesNode struct {
	zeroInputPlanNode
	source          tree.TypedExpr // nil if no SOURCE filter
	lastLoginBefore tree.TypedExpr // nil if no LAST LOGIN BEFORE filter
	limitCount      int64
}

// DropProvisionedRoles creates a plan node for DROP PROVISIONED ROLES.
// Requires CREATEROLE privilege.
func (p *planner) DropProvisionedRoles(
	ctx context.Context, n *tree.DropProvisionedRoles,
) (planNode, error) {
	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.CREATEROLE); err != nil {
		return nil, err
	}
	// LIMIT is required to prevent accidentally dropping an unbounded
	// number of roles in a single transaction.
	if n.Limit == nil || n.Limit.Count == nil {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"LIMIT is required for DROP PROVISIONED ROLES")
	}
	// Validate that the LIMIT expression is a constant integer to
	// prevent subqueries or other expressions from being smuggled
	// into the internal query.
	numVal, ok := n.Limit.Count.(*tree.NumVal)
	if !ok {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"LIMIT must be a constant integer expression")
	}
	limitInt, err := numVal.AsInt64()
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.InvalidParameterValue,
			"LIMIT must be an integer")
	}
	if limitInt <= 0 || limitInt > maxDropProvisionedRolesLimit {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"LIMIT must be between 1 and %d", maxDropProvisionedRolesLimit)
	}

	// Type-check SOURCE and LAST LOGIN BEFORE expressions so that
	// expressions like now() - '7d'::interval are properly evaluated
	// at execution time rather than being pretty-printed as text.
	node := &DropProvisionedRolesNode{limitCount: limitInt}
	var dummyHelper tree.IndexedVarHelper

	if n.Options != nil && n.Options.Source != nil {
		expr := paramparse.UnresolvedNameToStrVal(n.Options.Source)
		typed, err := p.analyzeExpr(
			ctx, expr, dummyHelper, types.String, true, /* requireType */
			"DROP PROVISIONED ROLES SOURCE",
		)
		if err != nil {
			return nil, err
		}
		node.source = typed
	}

	if n.Options != nil && n.Options.LastLoginBefore != nil {
		expr := paramparse.UnresolvedNameToStrVal(n.Options.LastLoginBefore)
		typed, err := p.analyzeExpr(
			ctx, expr, dummyHelper, types.TimestampTZ, true, /* requireType */
			"DROP PROVISIONED ROLES LAST LOGIN BEFORE",
		)
		if err != nil {
			return nil, err
		}
		node.lastLoginBefore = typed
	}

	return node, nil
}

func (n *DropProvisionedRolesNode) startExec(params runParams) error {
	sqltelemetry.IncIAMDropCounter(sqltelemetry.Role)
	const opName redact.RedactableString = "drop-provisioned-roles"

	hasAdmin, err := params.p.HasAdminRole(params.ctx)
	if err != nil {
		return err
	}

	// Evaluate filter expressions and build the query.
	sourceVal, lastLoginVal, err := n.evalFilterExprs(params)
	if err != nil {
		return err
	}
	query, queryArgs := buildProvisionedRolesQuery(sourceVal, lastLoginVal, n.limitCount)

	rows, err := params.p.InternalSQLTxn().QueryBufferedEx(
		params.ctx,
		"drop-provisioned-roles-find",
		params.p.txn,
		sessiondata.InternalExecutorOverride{User: params.p.User()},
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

		// Skip reserved roles (public, node, pg_*, crdb_internal_*),
		// root, and admin.
		if normalizedUsername.IsReserved() ||
			normalizedUsername.IsRootUser() ||
			normalizedUsername.IsAdminRole() {
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

	return nil
}

// evalFilterExprs evaluates the typed SOURCE and LAST LOGIN BEFORE
// expressions using the session's EvalContext, returning concrete
// Go values suitable for parameterized query arguments.
func (n *DropProvisionedRolesNode) evalFilterExprs(
	params runParams,
) (sourceVal *string, lastLoginVal *tree.DTimestampTZ, err error) {
	if n.source != nil {
		d, err := eval.Expr(params.ctx, params.EvalContext(), n.source)
		if err != nil {
			return nil, nil, err
		}
		s := string(tree.MustBeDString(d))
		sourceVal = &s
	}
	if n.lastLoginBefore != nil {
		d, err := eval.Expr(params.ctx, params.EvalContext(), n.lastLoginBefore)
		if err != nil {
			return nil, nil, err
		}
		v := tree.MustBeDTimestampTZ(d)
		lastLoginVal = &v
	}
	return sourceVal, lastLoginVal, nil
}

// buildProvisionedRolesQuery constructs the parameterized SQL query
// to find provisioned users matching the given filter values.
func buildProvisionedRolesQuery(
	sourceVal *string, lastLoginVal *tree.DTimestampTZ, limitCount int64,
) (string, []any) {
	var whereExprs []string
	var args []any
	argIdx := 1

	// Always filter for users that have a PROVISIONSRC role option
	// (i.e. are provisioned).
	provisionFilter := "EXISTS (SELECT 1 FROM system.role_options AS src" +
		" WHERE src.username = u.username AND src.option = 'PROVISIONSRC'"

	if sourceVal != nil {
		provisionFilter += fmt.Sprintf(" AND src.value = $%d", argIdx)
		args = append(args, *sourceVal)
		argIdx++
	}
	provisionFilter += ")"
	whereExprs = append(whereExprs, provisionFilter)

	if lastLoginVal != nil {
		whereExprs = append(whereExprs, fmt.Sprintf(
			"(u.estimated_last_login_time IS NULL OR u.estimated_last_login_time < $%d)", argIdx,
		))
		args = append(args, lastLoginVal.Time)
		argIdx++
	}

	whereClause := " WHERE " + strings.Join(whereExprs, " AND ")

	limitClause := fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limitCount)

	query := fmt.Sprintf(
		`SELECT u.username FROM system.users AS u%s%s`,
		whereClause, limitClause,
	)

	return query, args
}

// userHasDependencies checks whether the given user owns any objects,
// has grants, default privileges, row-level security policies,
// scheduled jobs, or system privileges that would prevent dropping.
func (n *DropProvisionedRolesNode) userHasDependencies(
	params runParams, normalizedUsername username.SQLUsername, allDescs nstree.Catalog,
) (bool, error) {
	// Check ownership, grants, default privileges, and RLS policies
	// across all descriptors.
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

		// Check default privileges on databases and schemas.
		var defaultPrivs catalog.DefaultPrivilegeDescriptor
		if dbDesc, ok := desc.(catalog.DatabaseDescriptor); ok {
			defaultPrivs = dbDesc.GetDefaultPrivilegeDescriptor()
		} else if schemaDesc, ok := desc.(catalog.SchemaDescriptor); ok {
			defaultPrivs = schemaDesc.GetDefaultPrivilegeDescriptor()
		}
		if defaultPrivs != nil {
			hasDep := false
			if err := defaultPrivs.ForEachDefaultPrivilegeForRole(
				func(dpForRole catpb.DefaultPrivilegesForRole) error {
					if dpForRole.IsExplicitRole() &&
						dpForRole.GetExplicitRole().UserProto.Decode() == normalizedUsername {
						hasDep = true
					}
					for _, privs := range dpForRole.DefaultPrivilegesPerObject {
						for _, u := range privs.Users {
							if u.User() == normalizedUsername {
								hasDep = true
							}
						}
					}
					return nil
				},
			); err != nil {
				return false, err
			}
			if hasDep {
				return true, nil
			}
		}

		// Check row-level security policies on tables.
		if tblDesc, ok := desc.(catalog.TableDescriptor); ok {
			for _, p := range tblDesc.GetPolicies() {
				for _, rn := range p.RoleNames {
					if username.MakeSQLUsernameFromPreNormalizedString(rn) == normalizedUsername {
						return true, nil
					}
				}
			}
		}
	}

	// Check scheduled jobs. Use NodeUserSessionDataOverride because
	// CREATEROLE users cannot read system.scheduled_jobs directly.
	// This is safe since the query is hardcoded with only a
	// parameterized username — no user-controlled SQL expressions.
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

	// Check system privileges. Same as above — use node privileges
	// for the hardcoded parameterized query.
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
) (int, error) {
	// DELETE from system.users.
	if _, err := params.p.InternalSQLTxn().ExecEx(
		params.ctx, opName, params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.users WHERE username=$1`,
		normalizedUsername,
	); err != nil {
		return 0, err
	}

	// DELETE from system.role_members.
	if _, err := params.p.InternalSQLTxn().ExecEx(
		params.ctx, "drop-role-membership", params.p.txn,
		sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.role_members WHERE "role" = $1 OR "member" = $1`,
		normalizedUsername,
	); err != nil {
		return 0, err
	}

	// DELETE from system.role_options.
	if _, err := params.p.InternalSQLTxn().ExecEx(
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
	if _, err := params.p.InternalSQLTxn().ExecEx(
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
