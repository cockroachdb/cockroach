// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowDatabases(stmt *tree.ShowDatabases) (tree.Statement, error) {

	commentColumn, commentJoin := ``, ``
	if stmt.WithComment {
		commentTableName := lexbase.EscapeSQLIdent(d.evalCtx.SessionData().Database) + ".pg_catalog.pg_shdescription"
		commentColumn, commentJoin = d.getCommentQuery(commentTableName, catconstants.PgCatalogDatabaseTableID, "d.id")
	}

	query := fmt.Sprintf(`
			SELECT
				name AS database_name, 
				owner, 
				primary_region, 
				secondary_region, 
				regions,
				survival_goal %s
			FROM "".crdb_internal.databases d %s
			ORDER BY database_name`, commentColumn, commentJoin)

	return d.parse(query)
}
