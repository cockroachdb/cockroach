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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowDatabases(stmt *tree.ShowDatabases) (tree.Statement, error) {
	query := `SELECT
	name AS database_name, owner, primary_region, regions, survival_goal`

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
	c.object_id = d.id`, keys.DatabaseCommentType)
	}

	query += `
ORDER BY
	database_name`

	return parse(query)
}
