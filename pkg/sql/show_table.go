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

// showTableDetails extracts information about the given table using
// the given query patterns in SQL. The query pattern must accept
// the following formatting parameters:
// %[1]s the database name as SQL string literal.
// %[2]s the unqualified table name as SQL string literal.
// %[3]s the given table name as SQL string literal.
// %[4]s the database name as SQL identifier.
// %[5]s the schema name as SQL string literal.
func (p *planner) showTableDetails(
	ctx context.Context, showType string, t tree.NormalizableTableName, query string,
) (planNode, error) {
	tn, err := t.Normalize()
	if err != nil {
		return nil, err
	}

	var desc *TableDescriptor
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands. We also use
	// allowAdding=true so we can look at the details of a table
	// added in the same transaction.
	//
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		desc, err = ResolveExistingObject(ctx, p, tn, true /*required*/, anyDescType)
	})
	if err != nil {
		return nil, err
	}
	if err := p.CheckAnyPrivilege(ctx, desc); err != nil {
		return nil, err
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(tn.Catalog()),
		lex.EscapeSQLString(tn.Table()),
		lex.EscapeSQLString(tn.String()),
		tn.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(tn.Schema()),
	)

	// log.VEventf(ctx, 2, "using table detail query: %s", fullQuery)

	return p.delegateQuery(ctx, showType, fullQuery,
		func(_ context.Context) error { return nil }, nil)
}

// checkDBExists checks if the database exists by using the security.RootUser.
func checkDBExists(ctx context.Context, p *planner, db string) error {
	_, err := p.PhysicalSchemaAccessor().GetDatabaseDesc(db,
		p.CommonLookupFlags(ctx, true /*required*/))
	return err
}
