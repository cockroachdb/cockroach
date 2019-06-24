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

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantee,
       privilege_type
  FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name,
       grantee,
       privilege_type
FROM "".information_schema.table_privileges`

	var source bytes.Buffer
	var cond bytes.Buffer
	var orderBy string

	if n.Targets != nil && n.Targets.Databases != nil {
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
			params = append(params, lex.EscapeSQLString(db))
		}

		fmt.Fprint(&source, dbPrivQuery)
		orderBy = "1,2,3,4"
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			cond.WriteString(`WHERE false`)
		} else {
			fmt.Fprintf(&cond, `WHERE database_name IN (%s)`, strings.Join(params, ","))
		}
	} else {
		fmt.Fprint(&source, tablePrivQuery)
		orderBy = "1,2,3,4,5"

		if n.Targets != nil {
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
				tables, err := cat.ExpandDataSourceGlob(
					d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, tableGlob,
				)
				if err != nil {
					return nil, err
				}
				allTables = append(allTables, tables...)
			}

			for i := range allTables {
				params = append(params, fmt.Sprintf("(%s,%s,%s)",
					lex.EscapeSQLString(allTables[i].Catalog()),
					lex.EscapeSQLString(allTables[i].Schema()),
					lex.EscapeSQLString(allTables[i].Table())))
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
			// No target: only look at tables and schemas in the current database.
			source.WriteString(` UNION ALL ` +
				`SELECT database_name, schema_name, NULL::STRING AS table_name, grantee, privilege_type FROM (`)
			source.WriteString(dbPrivQuery)
			source.WriteByte(')')
			// If the current database is set, restrict the command to it.
			if currDB := d.evalCtx.SessionData.Database; currDB != "" {
				fmt.Fprintf(&cond, ` WHERE database_name = %s`, lex.EscapeSQLString(currDB))
			} else {
				cond.WriteString(`WHERE true`)
			}
		}
	}

	if n.Grantees != nil {
		params = params[:0]
		for _, grantee := range n.Grantees.ToStrings() {
			params = append(params, lex.EscapeSQLString(grantee))
		}
		fmt.Fprintf(&cond, ` AND grantee IN (%s)`, strings.Join(params, ","))
	}
	query := fmt.Sprintf("SELECT * FROM (%s) %s ORDER BY %s", source.String(), cond.String(), orderBy)
	return parse(query)
}
