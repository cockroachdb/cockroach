// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowFunctions implements SHOW FUNCTIONS which returns all the
// user-defined functions.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW FUNCTIONS statement.
func (d *delegator) delegateShowFunctions(n *tree.ShowFunctions) (tree.Statement, error) {
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
	if name.ExplicitSchema && name.ExplicitCatalog && name.SchemaName == tree.PublicSchemaName &&
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

	const getFunctionsQuery = `
SELECT n.nspname as schema_name,
  p.proname as function_name,
  p.prorettype::REGTYPE::TEXT as result_data_type,
	COALESCE((SELECT trim('{}' FROM replace(array_agg(unnest(proargtypes)::REGTYPE::TEXT)::TEXT, ',', ', '))), '') as argument_data_types,
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
WHERE true
%[2]s
GROUP BY 1, 2, 3, proargtypes, 5, 6
ORDER BY 1, 2, 4;
`
	query := fmt.Sprintf(
		getFunctionsQuery,
		&name.CatalogName,
		schemaClause,
	)
	return d.parse(query)
}
