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
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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

func runCostFuzz(ctx context.Context, t test.Test, c cluster.Cluster) {
	runQueryComparison(ctx, t, c, &queryComparisonTest{name: "costfuzz", run: runCostFuzzQuery})
}

// runCostFuzzQuery executes the same query two times, once with normal costs
// and once with randomly perturbed costs. If the results of the two executions
// are not equal an error is returned.
func runCostFuzzQuery(
	conn *gosql.DB, smither *sqlsmith.Smither, rnd *rand.Rand, logStmt func(string),
) error {
	// Ignore panics from Generate.
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()

	stmt := smither.Generate()

	// Keep track of each statement that is executed so we can log them all later.
	var statements []string
	execStmt := func(stmt string) error {
		statements = append(statements, stmt)
		_, err := conn.Exec(stmt)
		return err
	}
	runQuery := func(stmt string) (*gosql.Rows, error) {
		statements = append(statements, stmt)
		return conn.Query(stmt)
	}
	logStatements := func() {
		for _, stmt := range statements {
			logStmt(stmt)
		}
	}

	// First, run the statement without cost perturbation.
	rows, err := runQuery(stmt)
	if err != nil {
		// Skip statements that fail with an error.
		//nolint:returnerrcheck
		return nil
	}
	defer rows.Close()
	unperturbedRows, err := sqlutils.RowsToStrMatrix(rows)
	if err != nil {
		// Skip statements whose results cannot be printed.
		//nolint:returnerrcheck
		return nil
	}

	seedStmt := fmt.Sprintf("SET testing_optimizer_random_seed = %d", rnd.Int63())
	if err := execStmt(seedStmt); err != nil {
		logStatements()
		return errors.Wrap(err, "failed to set random seed")
	}
	// Perturb costs such that an expression with cost c will be randomly assigned
	// a new cost in the range [0, 2*c).
	perturbCostsStmt := "SET testing_optimizer_cost_perturbation = 1.0"
	if err := execStmt(perturbCostsStmt); err != nil {
		logStatements()
		return errors.Wrap(err, "failed to perturb costs")
	}

	// Then, rerun the statement with cost perturbation.
	rows2, err := runQuery(stmt)
	if err != nil {
		// If the perturbed plan fails with an internal error while the normal plan
		// succeeds, we'd like to know, so consider this a test failure.
		es := err.Error()
		if strings.Contains(es, "internal error") {
			logStatements()
			return errors.Wrap(err, "internal error while running perturbed statement")
		}
		// Otherwise, skip perturbed statements that fail with a non-internal
		// error. This could happen if the statement contains bad arguments to a
		// function call, for example, and the normal plan was able to skip
		// evaluation of the function due to short-circuiting (see #81032 for an
		// example).
		//nolint:returnerrcheck
		return nil
	}
	defer rows2.Close()
	perturbedRows, err := sqlutils.RowsToStrMatrix(rows2)
	// If we've gotten this far, we should be able to print the results of the
	// perturbed statement, so consider it a test failure if we cannot.
	if err != nil {
		logStatements()
		return errors.Wrap(err, "error while printing perturbed statement results")
	}
	if diff := unsortedMatricesDiff(unperturbedRows, perturbedRows); diff != "" {
		// We have a mismatch in the perturbed vs non-perturbed query outputs.
		logStatements()
		return errors.Newf(
			"expected unperturbed and perturbed results to be equal\n%s\nsql: %s",
			diff, stmt,
		)
	}

	// TODO(michae2): If we run into the "-0 flake" described in PR #79551 then
	// we'll need some other strategy for comparison besides diffing the printed
	// results. One idea is to CREATE TABLE AS SELECT with both queries, and then
	// EXCEPT ALL the table contents. But this might be very slow.

	// Finally, disable cost perturbation for the next statement.
	resetSeedStmt := "RESET testing_optimizer_random_seed"
	if err := execStmt(resetSeedStmt); err != nil {
		logStatements()
		return errors.Wrap(err, "failed to reset random seed")
	}
	resetPerturbCostsStmt := "RESET testing_optimizer_cost_perturbation"
	if err := execStmt(resetPerturbCostsStmt); err != nil {
		logStatements()
		return errors.Wrap(err, "failed to disable cost perturbation")
	}
	return nil
}
