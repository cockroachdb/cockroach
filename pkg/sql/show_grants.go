// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowGrants returns grant details for the specified objects and users.
// Privileges: None.
//   Notes: postgres does not have a SHOW GRANTS statement.
//          mysql only returns the user's privileges.
func (p *planner) ShowGrants(ctx context.Context, n *tree.ShowGrants) (planNode, error) {
	var params []string
	var initCheck func(context.Context) error

	const dbPrivQuery = `SELECT table_catalog AS "Database", table_schema as "Schema", grantee AS "User", privilege_type AS "Privileges" ` +
		`FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `SELECT table_catalog as "Database", table_schema AS "Schema", table_name AS "Table", grantee AS "User", privilege_type AS "Privileges" ` +
		`FROM "".information_schema.table_privileges`

	var source bytes.Buffer
	var cond bytes.Buffer
	var orderBy string

	if n.Targets != nil && n.Targets.Databases != nil {
		// Get grants of database from information_schema.schema_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		initCheck = func(ctx context.Context) error {
			for _, db := range dbNames {
				if err := checkDBExists(ctx, p, db); err != nil {
					return err
				}
			}
			return nil
		}

		for _, db := range dbNames {
			params = append(params, lex.EscapeSQLString(db))
		}

		fmt.Fprint(&source, dbPrivQuery)
		orderBy = "1,2,3"
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			cond.WriteString(`WHERE false`)
		} else {
			fmt.Fprintf(&cond, `WHERE "Database" IN (%s)`, strings.Join(params, ","))
		}
	} else {
		fmt.Fprint(&source, tablePrivQuery)
		orderBy = "1,2,3,4"

		if n.Targets != nil {
			// Get grants of table from information_schema.table_privileges
			// if the type of target is table.
			var allTables tree.TableNames

			for _, tableTarget := range n.Targets.Tables {
				tableGlob, err := tableTarget.NormalizeTablePattern()
				if err != nil {
					return nil, err
				}
				var tables tree.TableNames
				// We avoid the cache so that we can observe the grants taking
				// a lease, like other SHOW commands. We also use
				// allowAdding=true so we can look at the grants of a table
				// added in the same transaction.
				//
				// TODO(vivek): check if the cache can be used.
				p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
					tables, err = expandTableGlob(ctx, p, tableGlob)
				})
				if err != nil {
					return nil, err
				}
				allTables = append(allTables, tables...)
			}

			initCheck = func(ctx context.Context) error { return nil }

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
				fmt.Fprintf(&cond, `WHERE ("Database", "Schema", "Table") IN (%s)`, strings.Join(params, ","))
			}
		} else {
			// No target: only look at tables and schemas in the current database.
			source.WriteString(` UNION ALL ` +
				`SELECT "Database", "Schema", NULL::STRING AS "Table", "User", "Privileges" FROM (`)
			source.WriteString(dbPrivQuery)
			source.WriteByte(')')
			// If the current database is set, restrict the command to it.
			if p.CurrentDatabase() != "" {
				fmt.Fprintf(&cond, ` WHERE "Database" = %s`, lex.EscapeSQLString(p.CurrentDatabase()))
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
		fmt.Fprintf(&cond, ` AND "User" IN (%s)`, strings.Join(params, ","))
	}
	return p.delegateQuery(ctx, "SHOW GRANTS",
		fmt.Sprintf("SELECT * FROM (%s) %s ORDER BY %s", source.String(), cond.String(), orderBy),
		initCheck, nil)
}
