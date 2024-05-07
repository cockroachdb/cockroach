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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
