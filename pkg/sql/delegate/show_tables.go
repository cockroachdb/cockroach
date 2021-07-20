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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var showEstimatedRowCountClusterSetting = settings.RegisterBoolSetting(
	"sql.show_tables.estimated_row_count.enabled",
	"whether the estimated_row_count is shown on SHOW TABLES. Turning this off "+
		"will improve SHOW TABLES performance.",
	true,
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
	// If we're resolved a one-part name into <db>.public (which is the behavior
	// of ResolveSchema, not for any obviously good reason), rework the resolved
	// name to have an explicit catalog but no explicit schema. This would arise
	// when doing SHOW TABLES FROM <db>. Without this logic, we would not show the
	// tables from other schemas than public.
	if name.ExplicitSchema && name.ExplicitCatalog && name.SchemaName == tree.PublicSchemaName &&
		n.ExplicitSchema && !n.ExplicitCatalog && n.SchemaName == name.CatalogName {
		name.SchemaName, name.ExplicitSchema = "", false
	}
	var schemaClause string
	if name.ExplicitSchema {
		schema := lex.EscapeSQLString(name.Schema())
		if name.Schema() == catconstants.PgTempSchemaName {
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
			 %[5]s
       ct.locality AS locality
       %[3]s
FROM %[1]s.pg_catalog.pg_class AS pc
LEFT JOIN %[1]s.pg_catalog.pg_roles AS rl on (pc.relowner = rl.oid)
JOIN %[1]s.pg_catalog.pg_namespace AS ns ON (ns.oid = pc.relnamespace)
%[4]s
%[6]s
LEFT JOIN crdb_internal.tables AS ct ON (pc.oid::int8 = ct.table_id)
WHERE pc.relkind IN ('r', 'v', 'S', 'm') %[2]s
ORDER BY schema_name, table_name
`
	var estimatedRowCount string
	var estimatedRowCountJoin string
	if showEstimatedRowCountClusterSetting.Get(&d.evalCtx.Settings.SV) {
		estimatedRowCount = "s.estimated_row_count AS estimated_row_count, "
		estimatedRowCountJoin = fmt.Sprintf(
			`LEFT JOIN %[1]s.crdb_internal.table_row_statistics AS s on (s.table_id = pc.oid::INT8)`,
			&name.CatalogName,
		)
	}
	var descJoin string
	var comment string
	if n.WithComment {
		descJoin = fmt.Sprintf(
			`LEFT JOIN %s.pg_catalog.pg_description AS pd ON (pc.oid = pd.objoid AND pd.objsubid = 0)`,
			&name.CatalogName,
		)
		comment = `, COALESCE(pd.description, '') AS comment`
	}
	query := fmt.Sprintf(
		getTablesQuery,
		&name.CatalogName,
		schemaClause,
		comment,
		descJoin,
		estimatedRowCount,
		estimatedRowCountJoin,
	)
	return parse(query)
}
