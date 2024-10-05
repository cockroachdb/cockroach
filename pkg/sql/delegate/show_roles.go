// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// delegateShowRoles implements SHOW ROLES which returns all the roles.
// Privileges: SELECT on system.users.
func (d *delegator) delegateShowRoles() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Roles)
	return d.parse(`
SELECT
	u.username,
	IFNULL(string_agg(o.option || COALESCE('=' || o.value, ''), ', ' ORDER BY o.option), '') AS options,
	ARRAY (SELECT role FROM system.role_members AS rm WHERE rm.member = u.username ORDER BY 1) AS member_of
FROM
	system.users AS u LEFT JOIN system.role_options AS o ON u.username = o.username
GROUP BY
	u.username
ORDER BY 1;
`)
}
