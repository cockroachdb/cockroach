// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// delegateShowRoles implements SHOW ROLES which returns all the roles.
// Privileges: SELECT on system.users.
func (d *delegator) delegateShowRoles(n *tree.ShowRoles) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Roles)
	return parse(`SELECT username AS role_name FROM system.users WHERE "isRole" = true ORDER BY 1`)
}
