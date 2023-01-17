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
	"github.com/cockroachdb/cockroach/pkg/internal/workloadreplay"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

type queryComparisonTest struct {
	name      string
	setupName string
	run       func(queryGenerator, *rand.Rand, queryComparisonHelper) error
}

type queryGenerator interface {
	Generate() string
}

type workloadreplayGenerator struct {
	stmt string
}

func (wlrg *workloadreplayGenerator) Generate() string {
	return wlrg.stmt
}

func (wlrg *workloadreplayGenerator) setStmt(stmt string) {
	wlrg.stmt = stmt
}

func runQueryComparison(
	ctx context.Context, t test.Test, c cluster.Cluster, qct *queryComparisonTest,
) {
	roundTimeout := 10 * time.Minute
	// Run 10 minute iterations of query comparison in a loop for about the entire
	// test, giving 5 minutes at the end to allow the test to shut down cleanly.
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

	for i := 0; ; i++ {
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
		if shouldExit() {
			return
		}

		runOneRoundQueryComparison(ctx, i, roundTimeout, t, c, qct)
		// If this iteration was interrupted because the timeout of ctx has been
		// reached, we want to cleanly exit from the test, without wiping out the
		// cluster (if we tried that, we'd get an error because ctx is canceled).
		if shouldExit() {
			return
		}
		c.Stop(ctx, t.L(), option.DefaultStopOpts())
		c.Wipe(ctx)
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

	// Append suffix to log name for workloadreplay to distinguish logs
	logsuffix := ""
	if qct.setupName == "workloadReplay" {
		logsuffix = "_workloadreplay"
	}
	logPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("%s%03d.log", qct.name+logsuffix, iter))
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("could not create %s%03d.log: %v", qct.name+logsuffix, iter, err)
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
		t.ArtifactsDir(), fmt.Sprintf("%s%03d.failure.log", qct.name+logsuffix, iter),
	)
	failureLog, err := os.Create(failureLogPath)
	if err != nil {
		t.Fatalf("could not create %s%03d.failure.log: %v", qct.name+logsuffix, iter, err)
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

	if qct.setupName == "workloadReplay" {

		logTest := func(logStr string, logType string) {
			stmt := strings.TrimSpace(logStr)
			if stmt == "" {
				return
			}
			fmt.Fprint(log, logType+":"+stmt)
			fmt.Fprint(log, "\n")

		}

		var finalStmt string
		var signatures map[string][]string

		for {

			finalStmt, signatures = workloadreplay.ChooseRandomQuery(log)
			if finalStmt == "" {
				continue
			}
			schemaMap := workloadreplay.CreateRandomDataSnowflake(signatures, log)
			if schemaMap == nil {
				continue
			}
			// Create tables.

			for tableName, schemaInfo := range schemaMap {
				t.L().Printf("creating table: " + tableName)
				fmt.Println(schemaInfo[2])
				if _, err := conn.Exec(schemaInfo[2]); err != nil {
					t.L().Printf("error while creating table: %v", err)
				}
			}

			// Load tables with initial data.

			importStr := ""
			for tableName, schemaInfo := range schemaMap {
				t.L().Printf("inserting rows into table:" + tableName)
				encodedKey, ok := os.LookupEnv(encodedKeyTag)
				if !ok {
					t.L().Printf("%s not set\n", encodedKeyTag)
					return
				}

				importStr = "IMPORT INTO " + tableName + " (" + schemaInfo[1] + ")\n"
				csvStr := " CSV DATA ('gs://" + bucketName + "/" + schemaInfo[0] + "?AUTH=specified&CREDENTIALS=" + encodedKey + "');"
				queryStr := importStr + csvStr
				logTest("gs://"+bucketName+"/"+schemaInfo[0], "TABLE_PATH:")
				if _, err := conn.Exec(queryStr); err != nil {
					t.L().Printf("error while inserting rows: %v", err)
					return
				}
			}

			// Test if query will run with the schemas created.
			t.L().Printf("Testing if valid query: %v", finalStmt)
			_, err = conn.Query("explain " + finalStmt)
			if err != nil {
				t.L().Printf("Not a Valid query with respect to table schema: %v", err)
				logTest(finalStmt, "Query/Schema Above is Invalid")
				for tablename := range schemaMap {
					if _, droperror := conn.Exec("drop table " + tablename); droperror != nil {
						t.L().Printf("error while dropping table: %v", droperror)

					}
				}
			} else { // Valid query so can run query and perform test.
				logTest(finalStmt, "Valid Query")

				h := queryComparisonHelper{
					conn:       conn,
					logStmt:    logStmt,
					logFailure: logFailure,
					printStmt:  printStmt,
					stmtNo:     0,
				}

				workloadqg := workloadreplayGenerator{finalStmt}

				if err := qct.run(&workloadqg, rnd, h); err != nil {
					t.Fatal(err)
				}

			}
		}

	} else {

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

// runQuery runs the given query and returns the output. As a side effect, it
// also saves the query, the query plan, and the output of running the query so
// they can be logged in case of failure.
func (h *queryComparisonHelper) runQuery(stmt string) ([][]string, error) {
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
	h.statements = append(h.statements, stmt)
	rows, err := runQueryImpl(stmt)
	if err != nil {
		return nil, err
	}
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
