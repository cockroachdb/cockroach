// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowRoleGrants returns role membership details for the specified roles and grantees.
// Privileges: SELECT on system.role_members.
//
//	Notes: postgres does not have a SHOW GRANTS ON ROLES statement.
func (d *delegator) delegateShowRoleGrants(n *tree.ShowRoleGrants) (tree.Statement, error) {
	const selectQuery = `
SELECT role AS role_name,
       member,
       "isAdmin" AS is_admin
 FROM system.role_members`

	var query bytes.Buffer
	query.WriteString(selectQuery)

	if n.Roles != nil {
		var roles []string
		sqlUsernames, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Roles,
		)
		if err != nil {
			return nil, err
		}
		for _, r := range sqlUsernames {
			roles = append(roles, lexbase.EscapeSQLString(r.Normalized()))
		}
		fmt.Fprintf(&query, ` WHERE "role" IN (%s)`, strings.Join(roles, ","))
	}

	if n.Grantees != nil {
		if n.Roles == nil {
			// No roles specified: we need a WHERE clause.
			query.WriteString(" WHERE ")
		} else {
			// We have a WHERE clause for roles.
			query.WriteString(" AND ")
		}

		var grantees []string
		granteeSQLUsernames, err := decodeusername.FromRoleSpecList(
			d.evalCtx.SessionData(), username.PurposeValidation, n.Grantees,
		)
		if err != nil {
			return nil, err
		}
		for _, g := range granteeSQLUsernames {
			grantees = append(grantees, lexbase.EscapeSQLString(g.Normalized()))
		}
		fmt.Fprintf(&query, ` member IN (%s)`, strings.Join(grantees, ","))

	}

	return d.parse(query.String())
}
