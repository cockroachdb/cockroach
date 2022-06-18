// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
)

func registerCostFuzz(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "costfuzz",
		Owner:           registry.OwnerSQLQueries,
		Timeout:         time.Hour * 1,
		RequiresLicense: true,
		Tags:            nil,
		Cluster:         r.MakeClusterSpec(1),
		Run:             runCostFuzz,
		Skip:            "flaky test: https://github.com/cockroachdb/cockroach/issues/81717",
	})
}

// runCostFuzzQuery executes the same query two times, once with normal costs
// and once with randomly perturbed costs. If the results of the two executions
// are not equal an error is returned.
func runCostFuzzQuery(
	conn *gosql.DB,
	smither *sqlsmith.Smither,
	rnd *rand.Rand,
	logStmt func(string),
	logFailure func(string, [][]string),
	printStmt func(string),
	stmtNo int,
) error {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	runQuery := func(stmt string) ([][]string, error) {
		rows, err := conn.Query(stmt)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return sqlutils.RowsToStrMatrix(rows)
	}

	stmt := smither.Generate()
	explainStmt := "EXPLAIN (OPT, VERBOSE) " + stmt

	// First, explain and run the statement without cost perturbation.
	controlPlan, err := runQuery(explainStmt)
	if err != nil {
		// Skip statements that fail to explain with an error.
		//nolint:returnerrcheck
		return nil
	}

	controlRows, err := runQuery(stmt)
	if err != nil {
		// Skip statements that fail with an error.
		//nolint:returnerrcheck
		return nil
	}

	seedStmt := fmt.Sprintf("SET testing_optimizer_random_cost_seed = %d", rnd.Int63())
	if _, err := conn.Exec(seedStmt); err != nil {
		logStmt(stmt)
		logStmt(seedStmt)
		printStmt(stmt)
		printStmt(seedStmt)
		return errors.Wrapf(err, "failed to perturb costs. %d statements run", stmtNo)
	}

	// Then, re-explain and rerun the statement with cost perturbation.
	perturbPlan, err := runQuery(explainStmt)
	if err != nil {
		// Skip statements that fail to explain with an error.
		//nolint:returnerrcheck
		return nil
	}

	perturbRows, err2 := runQuery(stmt)
	if err2 != nil {
		// If the perturbed plan fails with an internal error while the normal plan
		// succeeds, we'd like to know, so consider this a test failure.
		es := err2.Error()
		if strings.Contains(es, "internal error") {
			logStmt(stmt)
			logStmt(seedStmt)
			logStmt(stmt)
			logFailure(explainStmt, controlPlan)
			logFailure(stmt, controlRows)
			logFailure(seedStmt, nil)
			logFailure(explainStmt, perturbPlan)
			printStmt(seedStmt)
			printStmt(stmt)
			return errors.Wrapf(err2, "internal error while running perturbed statement. %d statements run", stmtNo)
		}
		// Otherwise, skip perturbed statements that fail with a non-internal
		// error. This could happen if the statement contains bad arguments to a
		// function call, for example, and the normal plan was able to skip
		// evaluation of the function due to short-circuiting (see #81032 for an
		// example).
		//nolint:returnerrcheck
		return nil
	}

	if diff := unsortedMatricesDiff(controlRows, perturbRows); diff != "" {
		// We have a mismatch in the perturbed vs control query outputs.
		// Output the control plan and the perturbed plan, along with the seed, so
		// that the perturbed query is reproducible.
		logStmt(stmt)
		logStmt(seedStmt)
		logStmt(stmt)
		logFailure(explainStmt, controlPlan)
		logFailure(stmt, controlRows)
		logFailure(seedStmt, nil)
		logFailure(explainStmt, perturbPlan)
		logFailure(stmt, perturbRows)
		printStmt(seedStmt)
		printStmt(stmt)
		return errors.Newf(
			"expected unperturbed and perturbed results to be equal\n%s\nsql: %s\n%d statements run",
			diff, stmt, stmtNo,
		)
	}

	// TODO(michae2): If we run into the "-0 flake" described in PR #79551 then
	// we'll need some other strategy for comparison besides diffing the printed
	// results. One idea is to CREATE TABLE AS SELECT with both queries, and then
	// EXCEPT ALL the table contents. But this might be very slow.

	// Finally, disable cost perturbation for the next statement.
	resetSeedStmt := "RESET testing_optimizer_random_cost_seed"
	if _, err := conn.Exec(resetSeedStmt); err != nil {
		logStmt(stmt)
		logStmt(seedStmt)
		logStmt(stmt)
		logStmt(resetSeedStmt)
		return errors.Wrapf(err, "failed to disable cost perturbation. %d statements run", stmtNo)
	}
	return nil
}
