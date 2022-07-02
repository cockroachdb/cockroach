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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
	roundTimeout := 10 * time.Minute
	// Run 10 minute iterations of costfuzz in a loop for about the entire test,
	// giving 5 minutes at the end to allow the test to shut down cleanly.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, t.Spec().(*registry.TestSpec).Timeout-5*time.Minute)
	defer cancel()
	done := ctx.Done()
	shouldExit := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	if err := c.PutLibraries(ctx, "./lib"); err != nil {
		t.Fatalf("could not initialize libraries: %v", err)
	}

	for i := 0; ; i++ {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
		if shouldExit() {
			return
		}

		runOneRoundCostFuzz(ctx, i, roundTimeout, t, c)
		// If this iteration of costfuzz was interrupted because the timeout of
		// ctx has been reached, we want to cleanly exit from the test, without
		// wiping out the cluster (if we tried that, we'd get an error because
		// ctx is canceled).
		if shouldExit() {
			return
		}
		c.Stop(ctx, t.L(), option.DefaultStopOpts())
		c.Wipe(ctx)
	}
}

// runOneRoundCostFuzz creates a random schema, inserts random data, and then
// executes costfuzz queries until the roundTimeout is reached.
func runOneRoundCostFuzz(
	ctx context.Context, iter int, roundTimeout time.Duration, t test.Test, c cluster.Cluster,
) {
	// Set up a statement logger for easy reproduction. We only
	// want to log successful statements and statements that
	// produced a final error or panic.
	costFuzzLogPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("costfuzz%03d.log", iter))
	costFuzzLog, err := os.Create(costFuzzLogPath)
	if err != nil {
		t.Fatalf("could not create costfuzz%03d.log: %v", iter, err)
	}
	defer costFuzzLog.Close()
	logStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		fmt.Fprint(costFuzzLog, stmt)
		if !strings.HasSuffix(stmt, ";") {
			fmt.Fprint(costFuzzLog, ";")
		}
		// Blank lines are necessary for reduce -costfuzz to function correctly.
		fmt.Fprint(costFuzzLog, "\n\n")
	}
	printStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		fmt.Fprint(costFuzzLog, stmt)
		t.L().Printf(stmt)
		if !strings.HasSuffix(stmt, ";") {
			t.L().Printf(";")
		}
		t.L().Printf("\n\n")
	}

	costFuzzFailureLogPath := filepath.Join(
		t.ArtifactsDir(), fmt.Sprintf("costfuzz%03d.failure.log", iter),
	)
	costFuzzFailureLog, err := os.Create(costFuzzFailureLogPath)
	if err != nil {
		t.Fatalf("could not create costfuzz%03d.failure.log: %v", iter, err)
	}
	defer costFuzzFailureLog.Close()
	logFailure := func(stmt string, rows [][]string) {
		fmt.Fprint(costFuzzFailureLog, stmt)
		fmt.Fprint(costFuzzFailureLog, "\n----\n")
		fmt.Fprint(costFuzzFailureLog, sqlutils.MatrixToStr(rows))
		fmt.Fprint(costFuzzFailureLog, "\n")
	}

	conn := c.Conn(ctx, t.L(), 1)

	rnd, seed := randutil.NewTestRand()
	t.L().Printf("seed: %d", seed)

	setup := sqlsmith.Setups[sqlsmith.RandTableSetupName](rnd)

	t.Status("executing setup")
	t.L().Printf("setup:\n%s", strings.Join(setup, "\n"))
	for _, stmt := range setup {
		if _, err := conn.Exec(stmt); err != nil {
			t.Fatal(err)
		} else {
			logStmt(stmt)
		}
	}

	setStmtTimeout := fmt.Sprintf("SET statement_timeout='%s';", statementTimeout.String())
	t.Status("setting statement_timeout")
	t.L().Printf("statement timeout:\n%s", setStmtTimeout)
	if _, err := conn.Exec(setStmtTimeout); err != nil {
		t.Fatal(err)
	}
	logStmt(setStmtTimeout)

	setUnconstrainedStmt := "SET unconstrained_non_covering_index_scan_enabled = true;"
	t.Status("setting unconstrained_non_covering_index_scan_enabled")
	t.L().Printf("\n%s", setUnconstrainedStmt)
	if _, err := conn.Exec(setUnconstrainedStmt); err != nil {
		logStmt(setUnconstrainedStmt)
		t.Fatal(err)
	}
	logStmt(setUnconstrainedStmt)

	// Initialize a smither that generates only INSERT and UPDATE statements with
	// the InsUpdOnly option.
	mutatingSmither := newMutatingSmither(conn, rnd, t, true /* disableDelete */)
	defer mutatingSmither.Close()

	// Initialize a smither that generates only deterministic SELECT statements.
	smither, err := sqlsmith.NewSmither(conn, rnd,
		sqlsmith.DisableMutations(), sqlsmith.DisableImpureFns(), sqlsmith.DisableLimits(),
		sqlsmith.UnlikelyConstantPredicate(), sqlsmith.FavorCommonData(),
		sqlsmith.UnlikelyRandomNulls(), sqlsmith.DisableCrossJoins(),
		sqlsmith.DisableIndexHints(), sqlsmith.DisableWith(),
		sqlsmith.LowProbabilityWhereClauseWithJoinTables(),
		sqlsmith.SetComplexity(.3),
		sqlsmith.SetScalarComplexity(.1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	t.Status("running costfuzz")
	until := time.After(roundTimeout)
	done := ctx.Done()
	for i := 1; ; i++ {
		select {
		case <-until:
			return
		case <-done:
			return
		default:
		}

		const numInitialMutations = 1000

		if i == numInitialMutations {
			t.Status("running costfuzz: ", i, " initial mutations completed")
			// Initialize a new mutating smither that generates INSERT, UPDATE and
			// DELETE statements with the MutationsOnly option.
			mutatingSmither = newMutatingSmither(conn, rnd, t, false /* disableDelete */)
			defer mutatingSmither.Close()
		}

		if i%1000 == 0 {
			if i != numInitialMutations {
				t.Status("running costfuzz: ", i, " statements completed")
			}
		}

		// Run `numInitialMutations` mutations first so that the tables have rows.
		// Run a mutation every 25th iteration afterwards to continually change the
		// state of the database.
		if i < numInitialMutations || i%25 == 0 {
			runMutationStatement(conn, mutatingSmither, logStmt)
			continue
		}

		if err := runCostFuzzQuery(conn, smither, rnd, logStmt, logFailure, printStmt, i); err != nil {
			t.Fatal(err)
		}
	}
}

func newMutatingSmither(
	conn *gosql.DB, rnd *rand.Rand, t test.Test, disableDelete bool,
) (mutatingSmither *sqlsmith.Smither) {
	var allowedMutations func() sqlsmith.SmitherOption
	if disableDelete {
		allowedMutations = sqlsmith.InsUpdOnly
	} else {
		allowedMutations = sqlsmith.MutationsOnly
	}
	var err error
	mutatingSmither, err = sqlsmith.NewSmither(conn, rnd, allowedMutations(),
		sqlsmith.FavorCommonData(), sqlsmith.UnlikelyRandomNulls(),
		sqlsmith.DisableInsertSelect(), sqlsmith.DisableCrossJoins(),
		sqlsmith.SetComplexity(.05),
		sqlsmith.SetScalarComplexity(.01),
	)
	if err != nil {
		t.Fatal(err)
	}
	return mutatingSmither
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
