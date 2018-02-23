// Copyright 2018 The Cockroach Authors.
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

// ShowSchemas returns all the schemas in the given or current database.
// Privileges: None.
//   Notes: postgres does not have a SHOW SCHEMAS statement.
func (p *planner) ShowSchemas(ctx context.Context, n *tree.ShowSchemas) (planNode, error) {
	name := p.SessionData().Database
	if n.Database != "" {
		name = string(n.Database)
	}
	if name == "" {
		return nil, errNoDatabase
	}
	if _, err := ResolveDatabase(ctx, p, name, true /*required*/); err != nil {
		return nil, err
	}

	const getSchemasQuery = `
				SELECT schema_name AS "Schema"
				FROM %[1]s.information_schema.schemata
				WHERE catalog_name = %[2]s
				ORDER BY schema_name`

	return p.delegateQuery(ctx, "SHOW SCHEMAS",
		fmt.Sprintf(getSchemasQuery, (*tree.Name)(&name), lex.EscapeSQLString(name)),
		func(_ context.Context) error { return nil }, nil)
}
