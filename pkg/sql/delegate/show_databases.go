// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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
			objoid, description as comment
		FROM
			"".crdb_internal.kv_catalog_comments c
		WHERE
			classoid = %d
	) c
ON
	c.objoid = d.id`, catconstants.PgCatalogDatabaseTableID)
	}

	query += `
ORDER BY
	database_name`

	return d.parse(query)
}
