// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (d *delegator) delegateShowCreateAllTriggers() (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)
	databaseLiteral := d.evalCtx.SessionData().Database
	const showCreateAllTriggersQuery = `SELECT crdb_internal.show_create_all_triggers(%[1]s) AS create_statement;`
	query := fmt.Sprintf(showCreateAllTriggersQuery,
		lexbase.EscapeSQLString(databaseLiteral),
	)
	return d.parse(query)
}
