// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestStatementReuses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	initStmts := []string{
		`CREATE DATABASE d`,
		`USE d`,
		`CREATE TABLE a(b INT)`,
		`CREATE VIEW v AS SELECT 1`,
		`CREATE SEQUENCE s`,
		`CREATE INDEX woo ON a(b)`,
		`CREATE USER woo`,
		`CREATE TYPE test as ENUM('a')`,
	}

	for _, s := range initStmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	testData := []string{
		// Drop tests are first so that if they incorrectly perform
		// their side effects, the statements below will fail.
		`DROP INDEX a@woo`,
		`DROP TABLE a`,
		`DROP DATABASE d CASCADE`,
		`DROP SEQUENCE s`,
		`DROP VIEW v`,
		`DROP USER woo`,
		`DROP TYPE test`,

		// Ditto ALTER first, so that erroneous side effects bork what's
		// below.
		`ALTER DATABASE d RENAME TO e`,
		`ALTER TABLE a RENAME TO x`,
		`ALTER TABLE a ADD COLUMN x INT`,
		`ALTER TABLE a RENAME COLUMN b TO c`,
		`ALTER TABLE a DROP COLUMN b`,
		`ALTER TABLE a EXPERIMENTAL_AUDIT SET READ WRITE`,
		`ALTER TABLE a CONFIGURE ZONE USING DEFAULT`,
		`ALTER TABLE a SPLIT AT VALUES(1)`,
		`ALTER TABLE a SCATTER`,

		`ALTER INDEX a@woo RENAME TO waa`,
		`ALTER INDEX a@woo CONFIGURE ZONE USING DEFAULT`,
		`ALTER INDEX a@woo SPLIT AT VALUES(1)`,
		`ALTER INDEX a@woo SCATTER`,

		`ALTER VIEW v RENAME TO x`,

		`ALTER SEQUENCE s RENAME TO x`,
		`ALTER SEQUENCE s NO CYCLE`,

		`ALTER RANGE DEFAULT CONFIGURE ZONE USING DEFAULT`,

		`ALTER USER woo WITH PASSWORD 'waa'`,

		`CANCEL JOBS SELECT 1`,
		`CANCEL QUERIES SELECT '1'`,
		`CANCEL SESSIONS SELECT '1'`,

		`CREATE DATABASE d2`,
		`CREATE INDEX c ON a(b)`,
		`CREATE SEQUENCE s2`,
		`CREATE STATISTICS st ON b FROM a`,
		`CREATE TABLE a2 (b INT)`,
		`CREATE VIEW v2 AS SELECT 1`,

		`DELETE FROM a`,
		`INSERT INTO a VALUES (1)`,
		`UPSERT INTO a VALUES (1)`,
		`UPDATE a SET b = 1`,

		`EXPLAIN SELECT 1`,

		// TODO(knz): backup/restore planning tests really should be
		// implementable here.
		// `BACKUP a TO 'a'`,
		// `SHOW BACKUP 'woo'`,

		`PAUSE JOBS SELECT 1`,
		`RESUME JOBS SELECT 1`,

		`SHOW ALL CLUSTER SETTINGS`,
		`SHOW CLUSTER SETTING version`,

		`SHOW CREATE a`,
		`SHOW COLUMNS FROM a`,
		`SHOW RANGES FROM TABLE a`,
		`SHOW ALL ZONE CONFIGURATIONS`,
		`SHOW ZONE CONFIGURATION FOR TABLE a`,
		`SHOW CONSTRAINTS FROM a`,
		`SHOW DATABASES`,
		`SHOW INDEXES FROM a`,
		`SHOW JOB 1`,
		`SHOW JOBS`,
		`SHOW ROLES`,
		`SHOW SCHEMAS`,
		`SHOW TABLES`,
		`SHOW USERS`,
		`SHOW database`,
	}

	t.Run("EXPLAIN", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				rows, err := db.Query("EXPLAIN " + test)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("PREPARE", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				if _, err := db.Exec("PREPARE p AS " + test); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("DEALLOCATE p"); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("WITH (cte)", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				rows, err := db.Query("EXPLAIN WITH a AS (" + test + ") TABLE a")
				if err != nil {
					if testutils.IsError(err, "does not return any columns") {
						// This error is acceptable and does not constitute a test failure.
						return
					}
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("PREPARE EXPLAIN", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				if _, err := db.Exec("PREPARE p AS EXPLAIN " + test); err != nil {
					t.Fatal(err)
				}
				rows, err := db.Query("EXECUTE p")
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("DEALLOCATE p"); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

// TestPrepareExplain verifies that we can prepare and execute various flavors
// of EXPLAIN.
func TestPrepareExplain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)
	r.Exec(t, "CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT UNIQUE)")

	// Note: the EXPLAIN ANALYZE (DEBUG) variant is tested separately.
	statements := []string{
		"EXPLAIN SELECT * FROM abc WHERE c=1",
		"EXPLAIN (VERBOSE) SELECT * FROM abc WHERE c=1",
		"EXPLAIN (TYPES) SELECT * FROM abc WHERE c=1",
		"EXPLAIN ANALYZE SELECT * FROM abc WHERE c=1",
		"EXPLAIN ANALYZE (VERBOSE) SELECT * FROM abc WHERE c=1",
		"EXPLAIN (DISTSQL) SELECT * FROM abc WHERE c=1",
		"EXPLAIN (VEC) SELECT * FROM abc WHERE c=1",
	}

	for _, sql := range statements {
		stmt, err := godb.Prepare(sql)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := stmt.Query()
		if err != nil {
			t.Fatal(err)
		}
		var rowsBuf bytes.Buffer
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				t.Fatal(err)
			}
			rowsBuf.WriteString(row)
			rowsBuf.WriteByte('\n')
		}

		// Verify that the output contains a scan for abc.
		scanRe := regexp.MustCompile(`scan|ColBatchScan`)
		if scanRe.FindString(rowsBuf.String()) == "" {
			t.Fatalf("%s: invalid output: \n%s\n", sql, rowsBuf.String())
		}

		stmt.Close()
	}
}

// TestExplainStatsCollected verifies that we correctly show how long ago table
// stats were collected. This cannot be tested through the usual datadriven
// tests because the value depends on the current time.
func TestExplainStatsCollected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)
	r.Exec(t, "CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT UNIQUE)")

	testCases := []struct {
		ago      time.Duration
		expected string
	}{
		// Note: the test times must be significantly larger than the time that can
		// pass between issuing the ALTER TABLE and the EXPLAIN.
		{ago: 10 * time.Minute, expected: "10 minutes"},
		{ago: time.Hour, expected: "1 hour"},
		{ago: 5 * 24 * time.Hour, expected: "5 days"},
	}

	for _, tc := range testCases {
		timestamp := timeutil.Now().UTC().Add(-tc.ago).Format("2006-01-02 15:04:05.999999-07:00")
		inject := fmt.Sprintf(
			`ALTER TABLE abc INJECT STATISTICS '[{
			  "columns": ["a"],
				"created_at": "%s",
				"row_count": 1000,
				"distinct_count": 1000
			}]'`,
			timestamp,
		)
		r.Exec(t, inject)
		explainOutput := r.QueryStr(t, "EXPLAIN SELECT * FROM abc")
		var statsRow string
		for _, row := range explainOutput {
			if len(row) != 1 {
				t.Fatalf("expected one column")
			}
			val := row[0]
			if strings.Contains(val, "estimated row count") {
				statsRow = val
				break
			}
		}
		if statsRow == "" {
			t.Fatal("could not find statistics row in explain output")
		}
		if !strings.Contains(statsRow, fmt.Sprintf("stats collected %s ago", tc.expected)) {
			t.Errorf("expected '%s', got '%s'", tc.expected, strings.TrimSpace(statsRow))
		}
	}
}
