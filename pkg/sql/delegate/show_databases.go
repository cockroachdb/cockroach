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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

func (d *delegator) delegateShowDatabases(stmt *tree.ShowDatabases) (tree.Statement, error) {
	query := `SELECT
	name AS database_name
`

	if stmt.WithComment {
		query += `,
  shobj_description(oid, 'pg_database') AS comment`
	}

	query += `
FROM
  "".crdb_internal.databases
ORDER BY
	database_name`

	return parse(query)
}
