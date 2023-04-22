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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type queryComparisonTest struct {
	name      string
	setupName string
	run       func(*sqlsmith.Smither, *rand.Rand, queryComparisonHelper) error
}

func runQueryComparison(
	ctx context.Context, t test.Test, c cluster.Cluster, qct *queryComparisonTest,
) {
	roundTimeout := 10 * time.Minute

	// A cluster context with a slightly longer timeout than the test timeout is
	// used so cluster creation or cleanup commands should never time out and
	// result in "context deadline exceeded" Github issues being created.
	var clusterCancel context.CancelFunc
	var clusterCtx context.Context
	clusterCtx, clusterCancel = context.WithTimeout(ctx, t.Spec().(*registry.TestSpec).Timeout-2*time.Minute)
	defer clusterCancel()

	// Run 10 minute iterations of query comparison in a loop for about the entire
	// test, giving 5 minutes at the end to allow the test to shut down cleanly.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(clusterCtx, t.Spec().(*registry.TestSpec).Timeout-5*time.Minute)
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

	c.Put(clusterCtx, t.Cockroach(), "./cockroach")

	for i := 0; ; i++ {
		if shouldExit() {
			return
		}
		c.Start(clusterCtx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

		runOneRoundQueryComparison(ctx, i, roundTimeout, t, c, qct)
		// If this iteration was interrupted because the timeout of ctx has been
		// reached, we want to cleanly exit from the test, without wiping out the
		// cluster (if we tried that, we'd get an error because ctx is canceled).
		if shouldExit() {
			return
		}
		c.Stop(clusterCtx, t.L(), option.DefaultStopOpts())
		c.Wipe(clusterCtx)
	}
}

// runOneRoundQueryComparison creates a random schema, inserts random data, and
// then executes queries until the roundTimeout is reached.
func runOneRoundQueryComparison(
	ctx context.Context,
	iter int,
	roundTimeout time.Duration,
	t test.Test,
	c cluster.Cluster,
	qct *queryComparisonTest,
) {
	// Set up a statement logger for easy reproduction. We only
	// want to log successful statements and statements that
	// produced a final error or panic.
	logPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("%s%03d.log", qct.name, iter))
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("could not create %s%03d.log: %v", qct.name, iter, err)
	}
	defer log.Close()
	logStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		fmt.Fprint(log, stmt)
		if !strings.HasSuffix(stmt, ";") {
			fmt.Fprint(log, ";")
		}
		// Blank lines are necessary for reduce -costfuzz to function correctly.
		fmt.Fprint(log, "\n\n")
	}
	printStmt := func(stmt string) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			return
		}
		t.L().Printf(stmt)
		if !strings.HasSuffix(stmt, ";") {
			t.L().Printf(";")
		}
		t.L().Printf("\n\n")
	}

	failureLogPath := filepath.Join(
		t.ArtifactsDir(), fmt.Sprintf("%s%03d.failure.log", qct.name, iter),
	)
	failureLog, err := os.Create(failureLogPath)
	if err != nil {
		t.Fatalf("could not create %s%03d.failure.log: %v", qct.name, iter, err)
	}
	defer failureLog.Close()
	logFailure := func(stmt string, rows [][]string) {
		fmt.Fprint(failureLog, stmt)
		fmt.Fprint(failureLog, "\n----\n")
		fmt.Fprint(failureLog, sqlutils.MatrixToStr(rows))
		fmt.Fprint(failureLog, "\n")
	}

	conn := c.Conn(ctx, t.L(), 1)

	rnd, seed := randutil.NewTestRand()
	t.L().Printf("seed: %d", seed)
	t.L().Printf("setupName: %s", qct.setupName)

	setup := sqlsmith.Setups[qct.setupName](rnd)

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

	isMultiRegion := qct.setupName == sqlsmith.SeedMultiRegionSetupName
	if isMultiRegion {
		setupMultiRegionDatabase(t, conn, logStmt)
	}

	// Initialize a smither that generates only INSERT and UPDATE statements with
	// the InsUpdOnly option.
	mutatingSmither := newMutatingSmither(conn, rnd, t, true /* disableDelete */, isMultiRegion)
	defer mutatingSmither.Close()

	// Initialize a smither that generates only deterministic SELECT statements.
	smither, err := sqlsmith.NewSmither(conn, rnd,
		sqlsmith.DisableMutations(), sqlsmith.DisableNondeterministicFns(), sqlsmith.DisableLimits(),
		sqlsmith.UnlikelyConstantPredicate(), sqlsmith.FavorCommonData(),
		sqlsmith.UnlikelyRandomNulls(), sqlsmith.DisableCrossJoins(),
		sqlsmith.DisableIndexHints(), sqlsmith.DisableWith(), sqlsmith.DisableDecimals(),
		sqlsmith.LowProbabilityWhereClauseWithJoinTables(),
		sqlsmith.SetComplexity(.3),
		sqlsmith.SetScalarComplexity(.1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	t.Status("running ", qct.name)
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
			t.Status("running ", qct.name, ": ", i, " initial mutations completed")
			// Initialize a new mutating smither that generates INSERT, UPDATE and
			// DELETE statements with the MutationsOnly option.
			mutatingSmither = newMutatingSmither(conn, rnd, t, false /* disableDelete */, isMultiRegion)
			defer mutatingSmither.Close()
		}

		if i%1000 == 0 {
			if i != numInitialMutations {
				t.Status("running ", qct.name, ": ", i, " statements completed")
			}
		}

		// Run `numInitialMutations` mutations first so that the tables have rows.
		// Run a mutation every 25th iteration afterwards to continually change the
		// state of the database.
		if i < numInitialMutations || i%25 == 0 {
			runMutationStatement(conn, mutatingSmither, logStmt)
			continue
		}

		h := queryComparisonHelper{
			conn:       conn,
			logStmt:    logStmt,
			logFailure: logFailure,
			printStmt:  printStmt,
			stmtNo:     i,
		}
		if err := qct.run(smither, rnd, h); err != nil {
			t.Fatal(err)
		}
	}
}

func newMutatingSmither(
	conn *gosql.DB, rnd *rand.Rand, t test.Test, disableDelete bool, isMultiRegion bool,
) (mutatingSmither *sqlsmith.Smither) {
	var smitherOpts []sqlsmith.SmitherOption
	smitherOpts = append(smitherOpts,
		sqlsmith.FavorCommonData(), sqlsmith.UnlikelyRandomNulls(),
		sqlsmith.DisableInsertSelect(), sqlsmith.DisableCrossJoins(),
		sqlsmith.SetComplexity(.05),
		sqlsmith.SetScalarComplexity(.01))
	if disableDelete {
		smitherOpts = append(smitherOpts, sqlsmith.InsUpdOnly())
	} else {
		smitherOpts = append(smitherOpts, sqlsmith.MutationsOnly())
	}
	if isMultiRegion {
		smitherOpts = append(smitherOpts, sqlsmith.EnableAlters())
		smitherOpts = append(smitherOpts, sqlsmith.MultiRegionDDLs())
	}
	var err error
	mutatingSmither, err = sqlsmith.NewSmither(conn, rnd, smitherOpts...)
	if err != nil {
		t.Fatal(err)
	}
	return mutatingSmither
}

// sqlAndOutput holds a SQL statement and its output.
type sqlAndOutput struct {
	sql    string
	output [][]string
}

// queryComparisonHelper is used to execute statements in query comparison
// tests. It keeps track of each statement that is executed so they can be
// logged in case of failure.
type queryComparisonHelper struct {
	conn       *gosql.DB
	logStmt    func(string)
	logFailure func(string, [][]string)
	printStmt  func(string)
	stmtNo     int

	statements            []string
	statementsAndExplains []sqlAndOutput
}

// runQuery runs the given query and returns the output. If the stmt doesn't
// result in an error, as a side effect, it also saves the query, the query
// plan, and the output of running the query so they can be logged in case of
// failure.
func (h *queryComparisonHelper) runQuery(stmt string) ([][]string, error) {
	// Log this statement with a timestamp but commented out. This will help in
	// cases when the stmt will get stuck and the whole test will time out (in
	// such a scenario, since the stmt didn't execute successfully, it won't get
	// logged by the caller).
	h.logStmt(fmt.Sprintf("-- %s: %s", timeutil.Now(),
		// Remove all newline symbols to log this stmt as a single line. This
		// way this auxiliary logging takes up less space (if the stmt executes
		// successfully, it'll still get logged with the nice formatting).
		strings.ReplaceAll(stmt, "\n", "")),
	)

	runQueryImpl := func(stmt string) ([][]string, error) {
		rows, err := h.conn.Query(stmt)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return sqlutils.RowsToStrMatrix(rows)
	}

	// First use EXPLAIN to try to get the query plan. This is best-effort, and
	// only for the purpose of debugging, so ignore any errors.
	explainStmt := "EXPLAIN " + stmt
	explainRows, err := runQueryImpl(explainStmt)
	if err == nil {
		h.statementsAndExplains = append(
			h.statementsAndExplains, sqlAndOutput{sql: explainStmt, output: explainRows},
		)
	}

	// Now run the query and save the output.
	rows, err := runQueryImpl(stmt)
	if err != nil {
		return nil, err
	}
	// Only save the stmt on success - this makes it easier to reproduce the
	// log. The caller still can include it into the statements later if
	// necessary.
	h.statements = append(h.statements, stmt)
	h.statementsAndExplains = append(h.statementsAndExplains, sqlAndOutput{sql: stmt, output: rows})
	return rows, nil
}

// execStmt executes the given statement. As a side effect, it also saves the
// statement so it can be logged in case of failure.
func (h *queryComparisonHelper) execStmt(stmt string) error {
	h.statements = append(h.statements, stmt)
	h.statementsAndExplains = append(h.statementsAndExplains, sqlAndOutput{sql: stmt})
	_, err := h.conn.Exec(stmt)
	return err
}

// logStatements logs all the queries and statements that were executed.
func (h *queryComparisonHelper) logStatements() {
	for _, stmt := range h.statements {
		h.logStmt(stmt)
		h.printStmt(stmt)
	}
}

// logVerboseOutput logs all the queries and statements that were executed,
// as well as the output rows and EXPLAIN output.
func (h *queryComparisonHelper) logVerboseOutput() {
	for _, stmtOrExplain := range h.statementsAndExplains {
		h.logFailure(stmtOrExplain.sql, stmtOrExplain.output)
	}
}

// makeError wraps the error with the given message and includes the number of
// statements run so far.
func (h *queryComparisonHelper) makeError(err error, msg string) error {
	return errors.Wrapf(err, "%s. %d statements run", msg, h.stmtNo)
}
