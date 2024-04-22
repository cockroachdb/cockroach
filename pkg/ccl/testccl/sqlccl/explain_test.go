// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
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

	const numStatements = 10

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	defer sqlDB.Close()

	query := func(sql string) (*gosql.Rows, error) {
		return sqlDB.QueryContext(ctx, sql)
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
		t.Log(stmt)
		if _, err := sqlDB.ExecContext(ctx, stmt); err != nil {
			// Ignore errors.
			t.Log("-- ignoring error:", err)
			continue
		}
	}

	// Check EXPLAIN (OPT, CATALOG, REDACT) for each table.
	rows, err := query("SELECT table_name FROM [SHOW TABLES]")
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
		t.Log(explain)
		rows, err = query(explain)
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
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smith.Close()

	tests.GenerateAndCheckRedactedExplainsForPII(t, smith, numStatements, query, containsPII)
}

// TestExplainGist is a randomized test for plan-gist logic.
func TestExplainGist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "the test is too slow")
	skip.UnderRace(t, "the test is too slow")

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	const numStatements = 1000
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
		// Always set the statement timeout - we actually execute the stmts on
		// the best effort basis to evolve the state of the DB.
		setup = append(setup, "SET statement_timeout = '0.1s';")
		runner.ExecMultiple(t, setup...)

		smither, err := sqlsmith.NewSmither(sqlDB, rng)
		if err != nil {
			t.Fatal(err)
		}
		defer smither.Close()

		checkErr := func(err error, stmt string) {
			if err != nil && strings.Contains(err.Error(), "internal error") {
				// Ignore all errors except the internal ones.
				t.Fatal(err)
			}
		}

		for i := 0; i < numStatements; i++ {
			var stmt string
		loop:
			for {
				stmt = smither.Generate()
				switch stmt {
				case "BEGIN TRANSACTION", "COMMIT TRANSACTION", "ROLLBACK TRANSACTION":
					// Ignore frequently generated statements that result in a
					// syntax error with EXPLAIN.
				default:
					break loop
				}
			}

			var gist string
			row := sqlDB.QueryRow("EXPLAIN (GIST) " + stmt)
			if err = row.Err(); err != nil {
				checkErr(err, stmt)
				continue
			}
			err = row.Scan(&gist)
			if err != nil {
				t.Fatal(err)
			}
			runner.Exec(t, "SELECT crdb_internal.decode_plan_gist($1);", gist)
			// Store the gist to be run against empty DB.
			gists = append(gists, gist)

			// Execute the stmt with short timeout so that the table schema is
			// modified.
			_, err = sqlDB.Exec(stmt)
			checkErr(err, stmt)
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
