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

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// delegateShowRanges implements the SHOW REGIONS statement.
func (d *delegator) delegateShowSurvivalGoal(n *tree.ShowSurvivalGoal) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.SurvivalGoal)
	dbName := string(n.DatabaseName)
	if dbName == "" {
		dbName = d.evalCtx.SessionData.Database
	}
	query := fmt.Sprintf(
		`SELECT
	name AS "database",
	survival_goal
FROM crdb_internal.databases
WHERE name = %s`,
		lex.EscapeSQLString(dbName),
	)
	return parse(query)
}
