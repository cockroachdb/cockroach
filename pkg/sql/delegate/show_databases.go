// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowDatabases(stmt *tree.ShowDatabases) (tree.Statement, error) {
	query := `SELECT
	name AS database_name, owner, primary_region, secondary_region, regions, survival_goal`

	if stmt.WithComment {
		query += `, comment`
	}

	query += `
FROM
  "".crdb_internal.databases d
`
	if stmt.WithComment {
		query += fmt.Sprintf(`
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
	c.object_id = d.id`, catalogkeys.DatabaseCommentType)
	}

	query += `
ORDER BY
	database_name`

	return d.parse(query)
}
