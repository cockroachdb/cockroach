// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowRoleGrants returns role membership details for the specified roles and grantees.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW GRANTS ON ROLES statement.
func (d *delegator) delegateShowRoleGrants(n *tree.ShowRoleGrants) (tree.Statement, error) {

	var roles, grantees []string

	if n.Roles != nil {
		sqlUsernames, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Roles,
		)
		if err != nil {
			return nil, err
		}
		for _, r := range sqlUsernames {
			roles = append(roles, lexbase.EscapeSQLString(r.Normalized()))
		}
	}

	if n.Grantees != nil {
		granteeSQLUsernames, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Grantees,
		)
		if err != nil {
			return nil, err
		}
		for _, g := range granteeSQLUsernames {
			grantees = append(grantees, lexbase.EscapeSQLString(g.Normalized()))
		}
	}

	whereClause := "true"
	joinedRoles := strings.Join(roles, ",")
	joinedGrantees := strings.Join(grantees, ",")
	if len(roles) > 0 && len(grantees) > 0 {
		whereClause = fmt.Sprintf(`role_name IN (%s) AND member IN (%s)`,
			joinedRoles, joinedGrantees)
	} else if len(roles) > 0 {
		whereClause = fmt.Sprintf(`role_name IN (%s)`,
			joinedRoles)
	} else if len(grantees) > 0 {
		whereClause = fmt.Sprintf(`implicit_grantee IN (%s)`,
			joinedGrantees)
	}

	// This query relies on two CTEs:
	//   - `explicit` lists all explicit role memberships. We need this because
	//     we want the (role_name, member) pairs produced by SHOW GRANTS ON ROLE
	//     to correspond to earlier `GRANT <role_name> TO <member>` statements.
	//   - `u` joins the above with the set of implicit grantees (plus the member
	//     itself), because we need to filter on those when a FOR clause is
	//     present in the SHOW GRANTS ON ROLE statement.
	query := fmt.Sprintf(`
WITH
	explicit
		AS (
			SELECT
				"role" AS role_name, inheriting_member AS member, member_is_admin AS is_admin
			FROM
				"".crdb_internal.kv_inherited_role_members
			WHERE
				member_is_explicit
		),
	u
		AS (
			SELECT
				e.*, irm.inheriting_member AS implicit_grantee
			FROM
				explicit AS e
				INNER JOIN "".crdb_internal.kv_inherited_role_members AS irm ON e.member = irm.role
			UNION ALL
				SELECT
					*, member
				FROM
					explicit
		)
SELECT DISTINCT
	role_name, member, is_admin
FROM
	u
WHERE
	%s
ORDER BY
	1, 2, 3
  `, whereClause)

	return d.parse(query)
}
