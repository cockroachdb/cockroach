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

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// delegateShowTables implements SHOW TABLES which returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (d *delegator) delegateShowTables(n *tree.ShowTables) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	_, name, err := d.catalog.ResolveSchema(d.ctx, flags, &n.TableNamePrefix)
	if err != nil {
		return nil, err
	}

	var query string
	schema := lex.EscapeSQLString(name.Schema())
	if name.Schema() == sessiondata.PgTempSchemaName {
		schema = lex.EscapeSQLString(d.evalCtx.SessionData.SearchPath.GetTemporarySchemaName())
	}

	if n.WithComment {
		const getTablesQuery = `
SELECT
	pc.relname AS table_name,
  COALESCE(pd.description, '') AS comment
 FROM %[1]s.pg_catalog.pg_class       AS pc
 JOIN %[1]s.pg_catalog.pg_namespace   AS ns ON (ns.oid = pc.relnamespace)
LEFT JOIN %[1]s.pg_catalog.pg_description AS pd ON (pc.oid = pd.objoid AND pd.objsubid = 0)
WHERE ns.nspname = %[2]s
  AND pc.relkind IN ('r', 'v')`

		query = fmt.Sprintf(
			getTablesQuery,
			&name.CatalogName,
			schema,
		)

	} else {
		const getTablesQuery = `
  SELECT table_name
    FROM %[1]s.information_schema.tables
   WHERE table_schema = %[2]s
ORDER BY table_name`

		query = fmt.Sprintf(
			getTablesQuery,
			&name.CatalogName,
			schema,
		)
	}

	return parse(query)
}
