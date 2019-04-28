// Copyright 2019 The Cockroach Authors.
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

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
			lex.EscapeSQLString(name.Schema()))

	} else {
		const getTablesQuery = `
  SELECT table_name
    FROM %[1]s.information_schema.tables
   WHERE table_schema = %[2]s
ORDER BY table_name`

		query = fmt.Sprintf(getTablesQuery,
			&name.CatalogName, lex.EscapeSQLString(name.Schema()))
	}

	return parse(query)
}
