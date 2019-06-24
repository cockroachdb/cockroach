// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// ShowUsers returns all the users.
// Privileges: SELECT on system.users.
func (d *delegator) delegateShowUsers(n *tree.ShowUsers) (tree.Statement, error) {
	return parse(`SELECT username AS user_name FROM system.users WHERE "isRole" = false ORDER BY 1`)
}
