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
		// TODO(knz): this 3-way join is painful and would really benefit
		// from vtables having a real table ID (#32963) so that we don't
		// have to use `information_schema.tables` and simply join
		// `crdb_internal.tables` with `system.comments` instead.
		const getTablesQuery = `
SELECT
	i.table_name, c.comment
FROM
	%[1]s.information_schema.tables AS i
	LEFT JOIN crdb_internal.tables AS t
	ON
		i.table_name = t.name
		AND i.table_catalog = t.database_name
	LEFT JOIN system.comments AS c
	ON t.table_id = c.object_id
WHERE
	table_schema = %[2]s
	AND (t.state = %[3]s OR t.state IS NULL)
	AND (t.database_name = %[4]s OR t.database_name IS NULL)
ORDER BY
	table_schema, table_name`

		query = fmt.Sprintf(
			getTablesQuery,
			&n.CatalogName,
			lex.EscapeSQLString(n.Schema()),
			lex.EscapeSQLString(sqlbase.TableDescriptor_PUBLIC.String()),
			lex.EscapeSQLString(n.CatalogName.Normalize()))

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
