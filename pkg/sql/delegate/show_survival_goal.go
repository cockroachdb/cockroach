// Copyright 2020 The Cockroach Authors.
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

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowSurvivalGoal(n *tree.ShowSurvivalGoal) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.SurvivalGoal)
	dbName := string(n.DatabaseName)
	if dbName == "" {
		dbName = d.evalCtx.SessionData().Database
	}
	query := fmt.Sprintf(
		`SELECT
	name AS "database",
	survival_goal
FROM crdb_internal.databases
WHERE name = %s`,
		lexbase.EscapeSQLString(dbName),
	)
	return d.parse(query)
}
