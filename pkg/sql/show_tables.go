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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// ShowTables returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (p *planner) ShowTables(ctx context.Context, n *tree.ShowTables) (planNode, error) {
	found, _, err := n.Resolve(ctx, p, p.CurrentDatabase(), p.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if p.CurrentDatabase() == "" && !n.ExplicitSchema {
			return nil, errNoDatabase
		}

		return nil, sqlbase.NewInvalidWildcardError(tree.ErrString(&n.TableNamePrefix))
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
			&n.CatalogName,
			lex.EscapeSQLString(n.Schema()))

	} else {
		const getTablesQuery = `
  SELECT table_name
    FROM %[1]s.information_schema.tables
   WHERE table_schema = %[2]s
ORDER BY table_schema, table_name`

		query = fmt.Sprintf(getTablesQuery,
			&n.CatalogName, lex.EscapeSQLString(n.Schema()))
	}

	return p.delegateQuery(ctx, "SHOW TABLES",
		query,
		func(_ context.Context) error { return nil }, nil)
}
