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
	gosql "database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
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

// TestExplainMVCCSteps makes sure that the MVCC stats are properly collected
// during explain analyze. This can't be a normal logic test because the MVCC
// stats are marked as nondeterministic (they change depending on number of
// nodes in the query).
func TestExplainMVCCSteps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderMetamorphic(t,
		"this test expects a precise number of scan requests, which is not upheld "+
			"in the metamorphic configuration that edits the kv batch size.")
	ctx := context.Background()
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)

	r.Exec(t, "CREATE TABLE ab (a PRIMARY KEY, b) AS SELECT g, g FROM generate_series(1,1000) g(g)")
	r.Exec(t, "CREATE TABLE bc (b PRIMARY KEY, c) AS SELECT g, g FROM generate_series(1,1000) g(g)")

	scanQuery := "SELECT count(*) FROM ab"
	expectedSteps, expectedSeeks := 1000, 1
	foundSteps, foundSeeks := getMVCCStats(t, r, scanQuery)

	assert.Equal(t, expectedSteps, foundSteps)
	assert.Equal(t, expectedSeeks, foundSeeks)
	assert.Equal(t, expectedSteps, foundSteps)
	assert.Equal(t, expectedSeeks, foundSeeks)

	// Update all rows.
	r.Exec(t, "UPDATE ab SET b=b+1 WHERE true")

	expectedSteps, expectedSeeks = 1000, 1
	foundSteps, foundSeeks = getMVCCStats(t, r, scanQuery)

	assert.Equal(t, expectedSteps, foundSteps)
	assert.Equal(t, expectedSeeks, foundSeeks)

	// Check that the lookup join (which is executed via a row-by-row processor
	// wrapped into the vectorized flow) correctly propagates the scan stats.
	lookupJoinQuery := "SELECT count(*) FROM ab INNER LOOKUP JOIN bc ON ab.b = bc.b"
	foundSteps, foundSeeks = getMVCCStats(t, r, lookupJoinQuery)
	// We're mainly interested in the fact whether the propagation takes place,
	// so one of the values being positive is sufficient.
	assert.Greater(t, foundSteps+foundSeeks, 0)
}

// getMVCCStats returns the number of MVCC steps and seeks found in the EXPLAIN
// ANALYZE of the given query from the top-most operator in the plan (i.e. if
// there are multiple operators exposing the scan stats, then the first info
// that appears in the EXPLAIN output is used).
func getMVCCStats(t *testing.T, r *sqlutils.SQLRunner, query string) (foundSteps, foundSeeks int) {
	rows := r.Query(t, "EXPLAIN ANALYZE(VERBOSE) "+query)
	var output strings.Builder
	var str string
	var err error
	var stepsSet, seeksSet bool
	for rows.Next() {
		if err := rows.Scan(&str); err != nil {
			t.Fatal(err)
		}
		output.WriteString(str)
		output.WriteByte('\n')
		str = strings.TrimSpace(str)
		// Numbers are printed with commas to indicate 1000s places, remove them.
		str = strings.ReplaceAll(str, ",", "")
		stepRe := regexp.MustCompile(`MVCC step count \(ext/int\): (\d+)/(\d+)`)
		stepMatches := stepRe.FindStringSubmatch(str)
		if len(stepMatches) == 3 && !stepsSet {
			foundSteps, err = strconv.Atoi(stepMatches[1])
			assert.NoError(t, err)
			stepsSet = true
		}
		seekRe := regexp.MustCompile(`MVCC seek count \(ext/int\): (\d+)/(\d+)`)
		seekMatches := seekRe.FindStringSubmatch(str)
		if len(seekMatches) == 3 && !seeksSet {
			foundSeeks, err = strconv.Atoi(seekMatches[1])
			assert.NoError(t, err)
			seeksSet = true
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		fmt.Println("Explain output:")
		fmt.Println(output.String())
	}
	return foundSteps, foundSeeks
}

// TestExplainAnalyzeWarnings verifies that warnings are printed whenever the
// estimated number of rows to be scanned differs significantly from the actual
// row count.
func TestExplainAnalyzeWarnings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)

	// Disable auto stats collection so that it doesn't interfere.
	r.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
	r.Exec(t, "CREATE TABLE warnings (k INT PRIMARY KEY)")
	// Insert 1000 rows into the table - this will be the actual row count. The
	// "acceptable" range for the estimates when the warning is not added is
	// [450, 2100].
	r.Exec(t, "INSERT INTO warnings SELECT generate_series(1, 1000)")

	for i, tc := range []struct {
		estimatedRowCount int
		expectWarning     bool
	}{
		{estimatedRowCount: 0, expectWarning: true},
		{estimatedRowCount: 100, expectWarning: true},
		{estimatedRowCount: 449, expectWarning: true},
		{estimatedRowCount: 450, expectWarning: false},
		{estimatedRowCount: 1000, expectWarning: false},
		{estimatedRowCount: 2000, expectWarning: false},
		{estimatedRowCount: 2100, expectWarning: false},
		{estimatedRowCount: 2101, expectWarning: true},
		{estimatedRowCount: 10000, expectWarning: true},
	} {
		// Inject fake stats.
		r.Exec(t, fmt.Sprintf(
			`ALTER TABLE warnings INJECT STATISTICS '[{
                            "columns": ["k"],
                            "created_at": "2022-08-23 00:00:0%[2]d.000000",
                            "distinct_count": %[1]d,
                            "name": "__auto__",
                            "null_count": 0,
                            "row_count": %[1]d
			}]'`, tc.estimatedRowCount, i,
		))
		rows := r.QueryStr(t, "EXPLAIN ANALYZE SELECT * FROM warnings")
		var warningFound bool
		for _, row := range rows {
			if len(row) > 1 {
				t.Fatalf("unexpectedly more than a single string is returned in %v", row)
			}
			if strings.HasPrefix(row[0], "WARNING") {
				warningFound = true
			}
		}
		assert.Equal(t, tc.expectWarning, warningFound, fmt.Sprintf("failed for estimated row count %d", tc.estimatedRowCount))
	}
}

// TestExplainRedact tests that variants of EXPLAIN (REDACT) do not leak PII.
func TestExplainRedact(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numStatements = 10

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	defer sqlDB.Close()

	query := func(sql string) (*gosql.Rows, error) {
		return sqlDB.QueryContext(ctx, sql)
	}

	// To check for PII leaks, we inject a single unlikely string into some of the
	// query constants produced by SQLSmith, and then search the redacted EXPLAIN
	// output for this string.
	pii := "pterodactyl"
	containsPII := func(explain, output string) error {
		lowerOutput := strings.ToLower(output)
		if strings.Contains(lowerOutput, pii) {
			return errors.Newf(
				"EXPLAIN output contained PII (%q):\n%s\noutput:\n%s\n", pii, explain, output,
			)
		}
		return nil
	}

	// Set up smither to generate random DML statements.
	setup := sqlsmith.Setups["seed"](rng)
	setup = append(setup, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = off;")
	setup = append(setup, "ANALYZE seed;")
	setup = append(setup, "SET statement_timeout = '5s';")
	t.Log(strings.Join(setup, "\n"))
	db := sqlutils.MakeSQLRunner(sqlDB)
	db.ExecMultiple(t, setup...)

	smith, err := sqlsmith.NewSmither(sqlDB, rng,
		sqlsmith.PrefixStringConsts(pii),
		sqlsmith.DisableDDLs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smith.Close()

	tests.GenerateAndCheckRedactedExplainsForPII(t, smith, numStatements, query, containsPII)
}
