// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlccl

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// TestExplainRedactDDL tests that variants of EXPLAIN (REDACT) do not leak
// PII. This is very similar to sql.TestExplainRedact but includes CREATE TABLE
// and ALTER TABLE statements, which could include partitioning (hence this is
// in CCL).
func TestExplainRedactDDL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "the test is too slow")
	skip.UnderRace(t, "the test is too slow")

	const numStatements = 10

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// To check for PII leaks, we inject a single unlikely string into some of the
	// query constants produced by SQLSmith, and then search the redacted EXPLAIN
	// output for this string.
	pii := "pachycephalosaurus"
	containsPII := func(sql, output string) error {
		lowerOutput := strings.ToLower(output)
		if strings.Contains(lowerOutput, pii) {
			return errors.Newf(
				"output contained PII (%q):\n%s\noutput:\n%s\n", pii, sql, output,
			)
		}
		return nil
	}

	// Perform a few random initial CREATE TABLEs.
	setup := sqlsmith.RandTablesPrefixStringConsts(rng, pii)
	setup = append(setup, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = off;")
	setup = append(setup, "SET statement_timeout = '5s';")
	for _, stmt := range setup {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			// Ignore errors.
			continue
		}
		// Only log successful statements.
		t.Log(stmt + ";")
	}

	// Check EXPLAIN (OPT, CATALOG, REDACT) for each table.
	rows, err := conn.QueryContext(ctx, "SELECT table_name FROM [SHOW TABLES]")
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	for _, table := range tables {
		explain := "EXPLAIN (OPT, CATALOG, REDACT) SELECT * FROM " + lexbase.EscapeSQLIdent(table)
		rows, err = conn.QueryContext(ctx, explain)
		if err != nil {
			// This explain should always succeed.
			t.Fatal(err)
		}
		var output strings.Builder
		for rows.Next() {
			var out string
			if err = rows.Scan(&out); err != nil {
				t.Fatal(err)
			}
			output.WriteString(out)
			output.WriteRune('\n')
		}
		if err = containsPII(explain, output.String()); err != nil {
			t.Error(err)
			continue
		}

	}

	// Set up smither to generate random DDL and DML statements.
	smith, err := sqlsmith.NewSmither(sqlDB, rng,
		sqlsmith.PrefixStringConsts(pii),
		sqlsmith.OnlySingleDMLs(),
		sqlsmith.EnableAlters(),
		sqlsmith.SimpleNames(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smith.Close()

	tests.GenerateAndCheckRedactedExplainsForPII(t, smith, numStatements, conn, containsPII)
}

// TestExplainGist is a randomized test for plan-gist logic.
func TestExplainGist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "the test is too slow")
	skip.UnderRace(t, "the test is too slow")

	// Use the release-build panic-catching behavior instead of the
	// crdb_test-build behavior. This is needed so that some known bugs like
	// #119045 and #133129 don't result in a test failure.
	defer colexecerror.ProductionBehaviorForTests()()

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	const numStatements = 500
	var gists []string

	t.Run("main", func(t *testing.T) {
		srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		runner := sqlutils.MakeSQLRunner(sqlDB)

		// Set up some initial state.
		setup := sqlsmith.Setups["seed"](rng)
		setup = append(setup, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = off;")
		if rng.Float64() < 0.5 {
			// In some cases have stats on the seed table.
			setup = append(setup, "ANALYZE seed;")
		}
		runner.ExecMultiple(t, setup...)

		// Given that statement timeout might apply differently between test
		// runs with the same seed (e.g. because of different CPU load), we'll
		// accumulate all successful statements for ease of reproduction.
		var successfulStmts strings.Builder
		logStmt := func(stmt string) {
			successfulStmts.WriteString(stmt)
			successfulStmts.WriteString(";\n")
		}
		for _, stmt := range setup {
			logStmt(stmt)
		}

		smither, err := sqlsmith.NewSmither(sqlDB, rng, sqlsmith.SimpleNames())
		if err != nil {
			t.Fatal(err)
		}
		defer smither.Close()

		// Always set the statement timeout - we actually execute the stmts on
		// the best effort basis to evolve the state of the DB. Note that we do
		// this after having set up the smither since it itself can issue some
		// statements.
		//
		// Note that we don't include this statement into successfulStmts since
		// all that are included there must have not timed out.
		timeoutStmt := "SET statement_timeout = '0.1s';"
		runner.Exec(t, timeoutStmt)

		checkErr := func(err error, stmt string) {
			if err != nil && strings.Contains(err.Error(), "internal error") {
				// Ignore all errors except the internal ones.
				for _, knownErr := range []string{
					"expected equivalence dependants to be its closure", // #119045
					"not in index", // #133129
				} {
					if strings.Contains(err.Error(), knownErr) {
						// Don't fail the test on a set of known errors.
						return
					}
				}
				t.Log(successfulStmts.String())
				t.Fatalf("%v: %s", err, stmt)
			}
		}

		for i := 0; i < numStatements; i++ {
			stmt := func() string {
				for {
					stmt := smither.Generate()
					switch stmt {
					case "BEGIN TRANSACTION", "COMMIT TRANSACTION", "ROLLBACK TRANSACTION":
						// Ignore frequently generated statements that result in
						// a syntax error with EXPLAIN.
					default:
						return stmt
					}
				}
			}()

			row := sqlDB.QueryRow("EXPLAIN (GIST) " + stmt)
			if err = row.Err(); err != nil {
				checkErr(err, stmt)
				continue
			}
			var gist string
			err = row.Scan(&gist)
			if err != nil {
				if !sqltestutils.IsClientSideQueryCanceledErr(err) {
					t.Fatal(err)
				}
				// Short statement timeout might exceed planning time. Let's
				// retry this statement with longer timeout.
				if ok := func() bool {
					runner.Exec(t, "SET statement_timeout = '1s'")
					defer runner.Exec(t, timeoutStmt)
					row = sqlDB.QueryRow("EXPLAIN (GIST) " + stmt)
					if err = row.Err(); err != nil {
						checkErr(err, stmt)
						return false
					}
					err = row.Scan(&gist)
					if err != nil {
						t.Fatalf("when re-running EXPLAIN (GIST) with 1s timeout: %+v", err)
					}
					return true
				}(); !ok {
					continue
				}
			}
			_, err = sqlDB.Exec("SELECT crdb_internal.decode_plan_gist($1);", gist)
			if err != nil {
				// We might be still in the process of cancelling the previous DROP
				// operation or hit the statement timeout - ignore this particular
				// error.
				if !testutils.IsError(err, "descriptor is being dropped") &&
					!sqltestutils.IsClientSideQueryCanceledErr(err) {
					t.Fatal(err)
				}
				continue
			}
			// Store the gist to be run against empty DB.
			gists = append(gists, gist)

			if shouldSkip := func() bool {
				// Executing these statements is out of scope for this test
				// (skipping them makes reproduction easier).
				for _, toSkipPrefix := range []string{"BACKUP", "EXPORT", "IMPORT", "RESTORE"} {
					if strings.HasPrefix(stmt, toSkipPrefix) {
						return true
					}
				}
				return false
			}(); shouldSkip {
				continue
			}

			// Execute the stmt with short timeout so that the table schema is
			// modified. We do so in a separate goroutine to ensure that we fail
			// the test if the stmt doesn't respect the timeout (if we didn't
			// use a separate goroutine, then the main test goroutine would be
			// blocked until either the stmt is completed or is canceled,
			// possibly timing out the test run).
			errCh := make(chan error)
			go func() {
				_, err := sqlDB.Exec(stmt)
				errCh <- err
			}()
			select {
			case err = <-errCh:
				if err != nil {
					checkErr(err, stmt)
				} else {
					logStmt(stmt)
				}
			case <-time.After(time.Minute):
				t.Log(successfulStmts.String())
				t.Fatalf("stmt wasn't canceled by statement_timeout of 0.1s - ran at least for 1m: %s", stmt)
			}
		}
	})

	t.Run("empty", func(t *testing.T) {
		srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		runner := sqlutils.MakeSQLRunner(sqlDB)
		for _, gist := range gists {
			runner.Exec(t, "SELECT crdb_internal.decode_plan_gist($1);", gist)
		}
	})
}
