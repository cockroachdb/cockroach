// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const (
	getFunctionsQuery = `
SELECT n.nspname as schema_name,
  p.proname as function_name,
  p.prorettype::REGTYPE::TEXT as result_data_type,
  COALESCE((SELECT trim('{}' FROM replace((SELECT array_agg(unnested::REGTYPE::TEXT) FROM unnest(proargtypes) AS unnested)::TEXT, ',', ', '))), '')
    AS argument_data_types,
  CASE p.prokind
	  WHEN 'a' THEN 'agg'
	  WHEN 'w' THEN 'window'
	  WHEN 'p' THEN 'proc'
    ELSE 'func'
  END as function_type,
  CASE
    WHEN p.provolatile = 'i' THEN 'immutable'
    WHEN p.provolatile = 's' THEN 'stable'
    WHEN p.provolatile = 'v' THEN 'volatile'
  END as volatility
FROM %[1]s.pg_catalog.pg_proc p
LEFT JOIN %[1]s.pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE p.prokind != 'p'
%[2]s
GROUP BY 1, 2, 3, proargtypes, 5, 6
ORDER BY 1, 2, 4;
`

	getProceduresQuery = `
SELECT n.nspname as schema_name,
  p.proname as procedure_name,
  COALESCE((SELECT trim('{}' FROM replace((SELECT array_agg(unnested::REGTYPE::TEXT) FROM unnest(proargtypes) AS unnested)::TEXT, ',', ', '))), '')
    AS argument_data_types
FROM %[1]s.pg_catalog.pg_proc p
LEFT JOIN %[1]s.pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE p.prokind = 'p'
%[2]s
GROUP BY 1, 2, 3, proargtypes
ORDER BY 1, 2, 3;
`
)

// delegateShowFunctions implements SHOW FUNCTIONS and SHOW PROCEDURES which
// return all the user-defined functions or procedures, respectively.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW FUNCTIONS or SHOW PROCEDURES statement.
func (d *delegator) delegateShowFunctions(n *tree.ShowRoutines) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	_, name, err := d.catalog.ResolveSchema(d.ctx, flags, &n.ObjectNamePrefix)
	if err != nil {
		return nil, err
	}
	// If we're resolved a one-part name into <db>.public (which is the behavior
	// of ResolveSchema, not for any obviously good reason), rework the resolved
	// name to have an explicit catalog but no explicit schema. This would arise
	// when doing SHOW FUNCTIONS FROM <db>. Without this logic, we would not show the
	// tables from other schemas than public.
	if name.ExplicitSchema && name.ExplicitCatalog && name.SchemaName == catconstants.PublicSchemaName &&
		n.ExplicitSchema && !n.ExplicitCatalog && n.SchemaName == name.CatalogName {
		name.SchemaName, name.ExplicitSchema = "", false
	}
	var schemaClause string
	if name.ExplicitSchema {
		schema := lexbase.EscapeSQLString(name.Schema())
		if name.Schema() == catconstants.PgTempSchemaName {
			schema = lexbase.EscapeSQLString(d.evalCtx.SessionData().SearchPath.GetTemporarySchemaName())
		}
		schemaClause = fmt.Sprintf("AND n.nspname = %s", schema)
	} else {
		// These must be custom defined until the sql <-> sql/delegate cyclic dependency
		// is resolved. When we have that, we should read the names off "virtualSchemas" instead.
		schemaClause = "AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'crdb_internal', 'pg_extension')"
	}

	var query string
	if n.Procedure {
		query = fmt.Sprintf(
			getProceduresQuery,
			&name.CatalogName,
			schemaClause,
		)
	} else {
		query = fmt.Sprintf(
			getFunctionsQuery,
			&name.CatalogName,
			schemaClause,
		)
	}
	return d.parse(query)
}
