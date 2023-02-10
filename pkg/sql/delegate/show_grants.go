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
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowGrants implements SHOW GRANTS which returns grant details for the
// specified objects and users.
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (d *delegator) delegateShowGrants(n *tree.ShowGrants) (tree.Statement, error) {
	var params []string

	const dbPrivQuery = `
SELECT database_name,
       grantee,
       privilege_type,
			 is_grantable::boolean
  FROM "".crdb_internal.cluster_database_privileges`
	const schemaPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantee,
       privilege_type,
       is_grantable::boolean
  FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name,
       grantee,
       privilege_type,
       is_grantable::boolean
FROM "".information_schema.table_privileges`
	const typePrivQuery = `
SELECT type_catalog AS database_name,
       type_schema AS schema_name,
       type_name,
       grantee,
       privilege_type,
       is_grantable::boolean
FROM "".information_schema.type_privileges`

	var source bytes.Buffer
	var cond bytes.Buffer
	var orderBy string

	if n.Targets != nil && len(n.Targets.Databases) > 0 {
		// Get grants of database from information_schema.schema_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		for _, db := range dbNames {
			name := cat.SchemaName{
				CatalogName:     tree.Name(db),
				SchemaName:      tree.Name(tree.PublicSchema),
				ExplicitCatalog: true,
				ExplicitSchema:  true,
			}
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &name)
			if err != nil {
				return nil, err
			}
			params = append(params, lexbase.EscapeSQLString(db))
		}

		fmt.Fprint(&source, dbPrivQuery)
		orderBy = "1,2,3"
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			cond.WriteString(`WHERE false`)
		} else {
			fmt.Fprintf(&cond, `WHERE database_name IN (%s)`, strings.Join(params, ","))
		}
	} else if n.Targets != nil && len(n.Targets.Schemas) > 0 {
		currDB := d.evalCtx.SessionData().Database

		for _, schema := range n.Targets.Schemas {
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &schema)
			if err != nil {
				return nil, err
			}
			dbName := currDB
			if schema.ExplicitCatalog {
				dbName = schema.Catalog()
			}
			params = append(params, fmt.Sprintf("(%s,%s)", lexbase.EscapeSQLString(dbName), lexbase.EscapeSQLString(schema.Schema())))
		}

		fmt.Fprint(&source, schemaPrivQuery)
		orderBy = "1,2,3,4"

		if len(params) != 0 {
			fmt.Fprintf(
				&cond,
				`WHERE (database_name, schema_name) IN (%s)`,
				strings.Join(params, ","),
			)
		}
	} else if n.Targets != nil && len(n.Targets.Types) > 0 {
		for _, typName := range n.Targets.Types {
			t, err := d.catalog.ResolveType(d.ctx, typName)
			if err != nil {
				return nil, err
			}
			if t.UserDefined() {
				params = append(
					params,
					fmt.Sprintf(
						"(%s, %s, %s)",
						lexbase.EscapeSQLString(t.TypeMeta.Name.Catalog),
						lexbase.EscapeSQLString(t.TypeMeta.Name.Schema),
						lexbase.EscapeSQLString(t.TypeMeta.Name.Name),
					),
				)
			} else {
				params = append(
					params,
					fmt.Sprintf(
						"(%s, 'pg_catalog', %s)",
						lexbase.EscapeSQLString(t.TypeMeta.Name.Catalog),
						lexbase.EscapeSQLString(t.TypeMeta.Name.Name),
					),
				)
			}
		}

		fmt.Fprint(&source, typePrivQuery)
		orderBy = "1,2,3,4,5"
		if len(params) == 0 {
			dbNameClause := "true"
			// If the current database is set, restrict the command to it.
			if currDB := d.evalCtx.SessionData().Database; currDB != "" {
				dbNameClause = fmt.Sprintf("database_name = %s", lexbase.EscapeSQLString(currDB))
			}
			cond.WriteString(fmt.Sprintf(`WHERE %s`, dbNameClause))
		} else {
			fmt.Fprintf(
				&cond,
				`WHERE (database_name, schema_name, type_name) IN (%s)`,
				strings.Join(params, ","),
			)
		}
	} else {
		orderBy = "1,2,3,4,5"

		if n.Targets != nil {
			fmt.Fprint(&source, tablePrivQuery)
			// Get grants of table from information_schema.table_privileges
			// if the type of target is table.
			var allTables tree.TableNames

			for _, tableTarget := range n.Targets.Tables {
				tableGlob, err := tableTarget.NormalizeTablePattern()
				if err != nil {
					return nil, err
				}
				// We avoid the cache so that we can observe the grants taking
				// a lease, like other SHOW commands.
				tables, _, err := cat.ExpandDataSourceGlob(
					d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, tableGlob,
				)
				if err != nil {
					return nil, err
				}
				allTables = append(allTables, tables...)
			}

			for i := range allTables {
				params = append(params, fmt.Sprintf("(%s,%s,%s)",
					lexbase.EscapeSQLString(allTables[i].Catalog()),
					lexbase.EscapeSQLString(allTables[i].Schema()),
					lexbase.EscapeSQLString(allTables[i].Table())))
			}

			if len(params) == 0 {
				// The glob pattern has expanded to zero matching tables.
				// There are no rows, but we can't simply return emptyNode{} because
				// the result columns must still be defined.
				cond.WriteString(`WHERE false`)
			} else {
				fmt.Fprintf(&cond, `WHERE (database_name, schema_name, table_name) IN (%s)`, strings.Join(params, ","))
			}
		} else {
			// No target: only look at types, tables and schemas in the current database.
			source.WriteString(
				`SELECT database_name, schema_name, table_name AS relation_name, grantee, privilege_type, is_grantable FROM (`,
			)
			source.WriteString(tablePrivQuery)
			source.WriteByte(')')
			source.WriteString(` UNION ALL ` +
				`SELECT database_name, schema_name, NULL::STRING AS relation_name, grantee, privilege_type, is_grantable FROM (`)
			source.WriteString(schemaPrivQuery)
			source.WriteByte(')')
			source.WriteString(` UNION ALL ` +
				`SELECT database_name, NULL::STRING AS schema_name, NULL::STRING AS relation_name, grantee, privilege_type, is_grantable FROM (`)
			source.WriteString(dbPrivQuery)
			source.WriteByte(')')
			source.WriteString(` UNION ALL ` +
				`SELECT database_name, schema_name, type_name AS relation_name, grantee, privilege_type, is_grantable FROM (`)
			source.WriteString(typePrivQuery)
			source.WriteByte(')')
			// If the current database is set, restrict the command to it.
			if currDB := d.evalCtx.SessionData().Database; currDB != "" {
				fmt.Fprintf(&cond, ` WHERE database_name = %s`, lexbase.EscapeSQLString(currDB))
			} else {
				cond.WriteString(`WHERE true`)
			}
		}
	}

	if n.Grantees != nil {
		params = params[:0]
		grantees, err := n.Grantees.ToSQLUsernames(d.evalCtx.SessionData(), security.UsernameValidation)
		if err != nil {
			return nil, err
		}
		for _, grantee := range grantees {
			params = append(params, lexbase.EscapeSQLString(grantee.Normalized()))
		}
		fmt.Fprintf(&cond, ` AND grantee IN (%s)`, strings.Join(params, ","))
	}
	query := fmt.Sprintf(`
		SELECT * FROM (%s) %s ORDER BY %s
	`, source.String(), cond.String(), orderBy)

	// Terminate on invalid users.
	for _, p := range n.Grantees {

		user, err := p.ToSQLUsername(d.evalCtx.SessionData(), security.UsernameValidation)
		if err != nil {
			return nil, err
		}
		// The `public` role is a pseudo-role, so we check it separately. RoleExists
		// should not return true for `public` since other operations like GRANT and
		// REVOKE should fail with a "role `public` does not exist" error if they
		// are used with `public`.
		userExists := user.IsPublicRole()
		if !userExists {
			userExists, err = d.catalog.RoleExists(d.ctx, user)
			if err != nil {
				return nil, err
			}
		}
		if !userExists {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", user)
		}
	}

	return parse(query)
}
