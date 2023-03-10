// Copyright 2020 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowTypes() (tree.Statement, error) {
	// TODO (SQL Features, SQL Exec): Once more user defined types are added
	//  they should be added here.
	return d.parse(`
SELECT
  schema, name, owner
FROM
  [SHOW ENUMS]
ORDER BY
  (schema, name)`)
}

func (d *delegator) delegateShowCreateAllTypes() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)

	const showCreateAllTypesQuery = `
	SELECT crdb_internal.show_create_all_types(%[1]s) AS create_statement;
`
	databaseLiteral := d.evalCtx.SessionData().Database

	query := fmt.Sprintf(showCreateAllTypesQuery,
		lexbase.EscapeSQLString(databaseLiteral),
	)

	return d.parse(query)
}
