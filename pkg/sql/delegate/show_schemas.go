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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// delegateShowSchemas implements SHOW SCHEMAS which returns all the schemas in
// the given or current database.
// Privileges: None.
func (d *delegator) delegateShowSchemas(n *tree.ShowSchemas) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}
	commentColumn, commentJoin := ``, ``
	if n.WithComment {
		commentColumn = `, comment`
		commentJoin = fmt.Sprintf(`
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
				c.object_id = n.oid`, catalogkeys.SchemaCommentType)
	}

	getSchemasQuery := fmt.Sprintf(`
									SELECT n.nspname AS schema_name, r.rolname AS owner%[1]s
FROM %[2]s.information_schema.schemata i
INNER JOIN %[2]s.pg_catalog.pg_namespace n ON (n.nspname = i.schema_name)
LEFT JOIN %[2]s.pg_catalog.pg_roles r ON (n.nspowner = r.oid)
							%s
WHERE catalog_name = %s
ORDER BY schema_name`,
		commentColumn, name.String(), commentJoin, lexbase.EscapeSQLString(string(name)),
	)

	return d.parse(getSchemasQuery)
}

func (d *delegator) delegateShowCreateAllSchemas() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)

	const showCreateAllSchemasQuery = `
	SELECT crdb_internal.show_create_all_schemas(%[1]s) AS create_statement;
`
	databaseLiteral := d.evalCtx.SessionData().Database

	query := fmt.Sprintf(showCreateAllSchemasQuery,
		lexbase.EscapeSQLString(databaseLiteral),
	)

	return d.parse(query)
}

// getSpecifiedOrCurrentDatabase returns the name of the specified database, or
// of the current database if the specified name is empty.
//
// Returns an error if there is no current database, or if the specified
// database doesn't exist.
func (d *delegator) getSpecifiedOrCurrentDatabase(specifiedDB tree.Name) (tree.Name, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	return d.catalog.LookupDatabaseName(d.ctx, flags, string(specifiedDB))
}
