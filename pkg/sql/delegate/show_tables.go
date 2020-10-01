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
	_, name, err := d.catalog.ResolveSchema(d.ctx, flags, &n.ObjectNamePrefix)
	if err != nil {
		return nil, err
	}

	var schemaClause string
	if n.ExplicitSchema {
		schema := lex.EscapeSQLString(name.Schema())
		if name.Schema() == sessiondata.PgTempSchemaName {
			schema = lex.EscapeSQLString(d.evalCtx.SessionData.SearchPath.GetTemporarySchemaName())
		}
		schemaClause = fmt.Sprintf("AND ns.nspname = %s", schema)
	} else {
		// These must be custom defined until the sql <-> sql/delegate cyclic dependency
		// is resolved. When we have that, we should read the names off "virtualSchemas" instead.
		schemaClause = "AND ns.nspname NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')"
	}

	const getTablesQuery = `
SELECT ns.nspname AS schema_name,
       pc.relname AS table_name,
       CASE
       WHEN pc.relkind = 'v' THEN 'view'
       WHEN pc.relkind = 'm' THEN 'materialized view'
       WHEN pc.relkind = 'S' THEN 'sequence'
       ELSE 'table'
       END AS type,
       rl.rolname AS owner,
       s.estimated_row_count AS estimated_row_count
       %[3]s
  FROM %[1]s.pg_catalog.pg_class AS pc
  LEFT JOIN %[1]s.pg_catalog.pg_roles AS rl on (pc.relowner = rl.oid)
  JOIN %[1]s.pg_catalog.pg_namespace AS ns ON (ns.oid = pc.relnamespace)
  LEFT
  JOIN %[1]s.pg_catalog.pg_description AS pd ON (pc.oid = pd.objoid AND pd.objsubid = 0)
  LEFT
  JOIN crdb_internal.table_row_statistics AS s on (s.table_id = pc.oid::INT8)
 WHERE pc.relkind IN ('r', 'v', 'S', 'm') %[2]s
 ORDER BY schema_name, table_name
`
	var comment string
	if n.WithComment {
		comment = `, COALESCE(pd.description, '')       AS comment`
	}
	query := fmt.Sprintf(
		getTablesQuery,
		&name.CatalogName,
		schemaClause,
		comment,
	)
	return parse(query)
}
