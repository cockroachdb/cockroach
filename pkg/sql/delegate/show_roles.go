// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// delegateShowRoles implements SHOW ROLES which returns all the roles.
// Privileges: SELECT on system.users.
func (d *delegator) delegateShowRoles() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Roles)
	selectClause := `
SELECT
	u.username,
	COALESCE(array_remove(array_agg(o.option || COALESCE('=' || o.value, '') ORDER BY o.option), NULL), ARRAY[]::STRING[]) AS options,
	ARRAY (SELECT role FROM system.role_members AS rm WHERE rm.member = u.username ORDER BY 1) AS member_of`
	selectLastLoginTime := `,
	u.estimated_last_login_time`
	endingClauses := `
FROM
	system.users AS u LEFT JOIN system.role_options AS o ON u.username = o.username
GROUP BY
	u.username
ORDER BY 1;
`
	d.evalCtx.ClientNoticeSender.BufferClientNotice(d.ctx, pgnotice.Newf(
		"estimated_last_login_time is computed on a best effort basis; it is not guaranteed to capture every login event"))
	return d.parse(selectClause + selectLastLoginTime + endingClauses)
}

// delegateShowRolesExtended implements SHOW ROLES with optional
// provisioning filter clauses (SOURCE, LAST LOGIN BEFORE)
// and LIMIT. When no options are specified, it falls back to
// delegateShowRoles.
//
// TODO(sourav): Wire this into the delegate dispatch for ShowUsers;
// currently only exercised by unit tests.
func (d *delegator) delegateShowRolesExtended(n *tree.ShowUsers) (tree.Statement, error) {
	if (n.Options == nil || n.Options.IsDefault()) && n.Limit == nil {
		return d.delegateShowRoles()
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.Roles)

	var whereExprs []string
	if n.Options != nil {
		if n.Options.Source != nil {
			sourceStr := tree.AsStringWithFlags(n.Options.Source, tree.FmtBareStrings)
			whereExprs = append(whereExprs, fmt.Sprintf(
				`EXISTS (
	SELECT 1 FROM system.role_options AS src
	WHERE src.username = u.username
		AND src.option = 'PROVISIONSRC'
		AND src.value = %s
)`, lexbase.EscapeSQLString(sourceStr)))
		}
		if n.Options.LastLoginBefore != nil {
			// Users with a NULL estimated_last_login_time are excluded from
			// the result since NULL comparisons evaluate to NULL, not true.
			tsExpr := tree.AsStringWithFlags(n.Options.LastLoginBefore, tree.FmtParsable)
			whereExprs = append(whereExprs, fmt.Sprintf(
				"u.estimated_last_login_time < (%s)::TIMESTAMPTZ",
				tsExpr))
		}
	}

	var whereClause string
	if len(whereExprs) > 0 {
		whereClause = fmt.Sprintf(
			"\nWHERE %s", strings.Join(whereExprs, "\n\tAND "))
	}

	var limitClause string
	if n.Limit != nil && n.Limit.Count != nil {
		limitClause = fmt.Sprintf("\nLIMIT %s", tree.AsString(n.Limit.Count))
	}

	query := fmt.Sprintf(`
SELECT
	u.username,
	COALESCE(array_remove(array_agg(o.option || COALESCE('=' || o.value, '') ORDER BY o.option), NULL), ARRAY[]::STRING[]) AS options,
	ARRAY (SELECT role FROM system.role_members AS rm WHERE rm.member = u.username ORDER BY 1) AS member_of,
	u.estimated_last_login_time
FROM
	system.users AS u LEFT JOIN system.role_options AS o ON u.username = o.username%s
GROUP BY
	u.username
ORDER BY 1%s;
`, whereClause, limitClause)

	d.evalCtx.ClientNoticeSender.BufferClientNotice(d.ctx, pgnotice.Newf(
		"estimated_last_login_time is computed on a best effort basis; it is not guaranteed to capture every login event"))
	return d.parse(query)
}
