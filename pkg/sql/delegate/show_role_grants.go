// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package delegate

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowRoleGrants returns role membership details for the specified roles and grantees.
// Privileges: SELECT on system.role_members.
//   Notes: postgres does not have a SHOW GRANTS ON ROLES statement.
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
		for _, r := range n.Roles.ToStrings() {
			roles = append(roles, lex.EscapeSQLString(r))
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
		for _, g := range n.Grantees.ToStrings() {
			grantees = append(grantees, lex.EscapeSQLString(g))
		}
		fmt.Fprintf(&query, ` member IN (%s)`, strings.Join(grantees, ","))

	}

	return parse(query.String())
}
