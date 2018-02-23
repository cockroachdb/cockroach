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

	const getTablesQuery = `
				SELECT table_name AS "Table"
				FROM %[1]s.information_schema.tables
				WHERE table_schema = %[2]s
				ORDER BY table_schema, table_name`

	return p.delegateQuery(ctx, "SHOW TABLES",
		fmt.Sprintf(getTablesQuery, &n.CatalogName, lex.EscapeSQLString(n.Schema())),
		func(_ context.Context) error { return nil }, nil)
}
