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

// showTableDetails extracts information about the given table using
// the given query patterns in SQL. The query pattern must accept
// the following formatting parameters:
// %[1]s the database name as SQL string literal.
// %[2]s the unqualified table name as SQL string literal.
// %[3]s the given table name as SQL string literal.
// %[4]s the database name as SQL identifier.
func (p *planner) showTableDetails(
	ctx context.Context, showType string, t tree.NormalizableTableName, query string,
) (planNode, error) {
	tn, err := t.NormalizeWithDatabaseName(p.SessionData().Database)
	if err != nil {
		return nil, err
	}
	db := tn.Database()

	initialCheck := func(ctx context.Context) error {
		if err := checkDBExists(ctx, p, db); err != nil {
			return err
		}
		desc, err := MustGetTableOrViewDesc(ctx, p.txn, p.getVirtualTabler(), tn, true /* allowAdding */)
		if err != nil {
			return err
		}
		return p.CheckAnyPrivilege(desc)
	}

	return p.delegateQuery(ctx, showType,
		fmt.Sprintf(query,
			lex.EscapeSQLString(db),
			lex.EscapeSQLString(tn.Table()),
			lex.EscapeSQLString(tn.String()),
			tn.DatabaseName.String()),
		initialCheck, nil)
}

// checkDBExists checks if the database exists by using the security.RootUser.
func checkDBExists(ctx context.Context, p *planner, db string) error {
	if _, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), db); err != nil {
		return sqlbase.NewUndefinedDatabaseError(db)
	}
	return nil
}
