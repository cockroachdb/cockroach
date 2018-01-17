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
)

// ShowTables returns all the tables.
// Privileges: None.
//   Notes: postgres does not have a SHOW TABLES statement.
//          mysql only returns tables you have privileges on.
func (p *planner) ShowTables(ctx context.Context, n *tree.ShowTables) (planNode, error) {
	name := p.SessionData().Database
	if n.Database != "" {
		name = string(n.Database)
	}
	if name == "" {
		return nil, errNoDatabase
	}
	initialCheck := func(ctx context.Context) error {
		return checkDBExists(ctx, p, name)
	}

	const getTablesQuery = `
				SELECT TABLE_NAME AS "Table"
				FROM "".information_schema.tables
				WHERE tables.TABLE_SCHEMA=%[1]s
				ORDER BY tables.TABLE_NAME`

	return p.delegateQuery(ctx, "SHOW TABLES",
		fmt.Sprintf(getTablesQuery, lex.EscapeSQLString(name)),
		initialCheck, nil)
}
