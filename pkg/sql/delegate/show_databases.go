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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var NodeUserOid *tree.DOid

func (d *delegator) delegateShowDatabases(stmt *tree.ShowDatabases) (tree.Statement, error) {
	// The 'node' role is internal-only (not exposed in the pg_roles table), so we have to check for it explicitly here
	getDatabasesQuery := `SELECT
	datname AS database_name,
  CASE WHEN d.datdba = %v 
  THEN '%s'
  ELSE rl.rolname
  END
  AS owner
`

	if stmt.WithComment {
		getDatabasesQuery += `, comment`
	}

	getDatabasesQuery += `
FROM
  pg_catalog.pg_database d
  LEFT JOIN pg_catalog.pg_roles AS rl on (d.datdba = rl.oid)
`
	if stmt.WithComment {
		getDatabasesQuery += fmt.Sprintf(`
LEFT JOIN
	(
		SELECT 
			object_id, type, comment
		FROM
			system.comments
		WHERE
			type = %d
	) c
ON
	c.object_id = d.oid`, keys.DatabaseCommentType)
	}

	getDatabasesQuery += `
ORDER BY
	database_name`

	query := fmt.Sprintf(getDatabasesQuery, NodeUserOid, security.NodeUser)

	return parse(query)
}
