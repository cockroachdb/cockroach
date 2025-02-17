// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"bytes"
	"context"
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

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

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
		"EXPLAIN (DISTSQL) SELECT * FROM abc WHERE c=1",
		"EXPLAIN (VEC) SELECT * FROM abc WHERE c=1",
		"EXPLAIN ANALYZE SELECT * FROM abc WHERE c=1",
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

// TestExplainKVInfo makes sure that miscellaneous KV-level stats are properly
// collected during EXPLAIN ANALYZE. This can't be a normal logic test because
// KV-level stats are marked as non-deterministic.
func TestExplainKVInfo(t *testing.T) {
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

	for _, vectorize := range []bool{true, false} {
		if vectorize {
			r.Exec(t, "SET vectorize = on")
		} else {
			r.Exec(t, "SET vectorize = off")
		}
		for _, streamer := range []bool{true, false} {
			if streamer {
				r.Exec(t, "SET streamer_enabled = true")
			} else {
				r.Exec(t, "SET streamer_enabled = false")
			}

			scanQuery := "SELECT count(*) FROM ab"
			info := getKVInfo(t, r, scanQuery)

			assert.Equal(t, 1, info.counters[kvNodes])
			assert.Equal(t, 1000, info.counters[rowsRead])
			assert.Equal(t, 1000, info.counters[pairsRead])
			assert.LessOrEqual(t, 31 /* KiB */, info.counters[bytesRead])
			assert.Equal(t, 1, info.counters[gRPCCalls])
			assert.Equal(t, 1000, info.counters[stepCount])
			assert.Equal(t, 1, info.counters[seekCount])

			lookupJoinQuery := "SELECT count(*) FROM ab INNER LOOKUP JOIN bc ON ab.b = bc.b"
			info = getKVInfo(t, r, lookupJoinQuery)

			assert.Equal(t, 1, info.counters[kvNodes])
			assert.Equal(t, 1000, info.counters[rowsRead])
			assert.Equal(t, 1000, info.counters[pairsRead])
			assert.LessOrEqual(t, 13 /* KiB */, info.counters[bytesRead])
			assert.Equal(t, 1, info.counters[gRPCCalls])
			assert.Equal(t, 0, info.counters[stepCount])
			assert.Equal(t, 1000, info.counters[seekCount])
		}
	}
}

const (
	// Note that kvNodes is not really a counter, but since we're using a single
	// node cluster, only a single node ID is expected.
	kvNodes = iota
	rowsRead
	pairsRead
	bytesRead
	gRPCCalls
	stepCount
	seekCount
	numKVCounters
)

type kvInfo struct {
	counters [numKVCounters]int
}

var patterns [numKVCounters]*regexp.Regexp

func init() {
	patterns[kvNodes] = regexp.MustCompile(`kv nodes: n(\d)`)
	patterns[rowsRead] = regexp.MustCompile(`KV rows decoded: (\d+)`)
	patterns[pairsRead] = regexp.MustCompile(`KV pairs read: (\d+)`)
	patterns[bytesRead] = regexp.MustCompile(`KV bytes read: (\d+) \w+`)
	patterns[gRPCCalls] = regexp.MustCompile(`KV gRPC calls: (\d+)`)
	patterns[stepCount] = regexp.MustCompile(`MVCC step count \(ext/int\): (\d+)/[\d+]`)
	patterns[seekCount] = regexp.MustCompile(`MVCC seek count \(ext/int\): (\d+)/[\d+]`)
}

// getKVInfo returns miscellaneous KV-level stats found in the EXPLAIN ANALYZE
// of the given query from the top-most operator in the plan (i.e. if there are
// multiple operators exposing the scan stats, then the first info that appears
// in the EXPLAIN output is used).
func getKVInfo(t *testing.T, r *sqlutils.SQLRunner, query string) kvInfo {
	rows := r.Query(t, "EXPLAIN ANALYZE (VERBOSE) "+query)
	var output strings.Builder
	var str string
	var err error
	var info kvInfo
	var counterSet [numKVCounters]bool
	for rows.Next() {
		if err := rows.Scan(&str); err != nil {
			t.Fatal(err)
		}
		output.WriteString(str)
		output.WriteByte('\n')
		str = strings.TrimSpace(str)
		// Numbers are printed with commas to indicate 1000s places, remove them.
		str = strings.ReplaceAll(str, ",", "")
		for i := 0; i < numKVCounters; i++ {
			if counterSet[i] {
				continue
			}
			matches := patterns[i].FindStringSubmatch(str)
			if len(matches) == 2 {
				info.counters[i], err = strconv.Atoi(matches[1])
				assert.NoError(t, err)
				counterSet[i] = true
				break
			}
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		fmt.Println("Explain output:")
		fmt.Println(output.String())
	}
	return info
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

	skip.UnderDeadlock(t, "the test is too slow")
	skip.UnderRace(t, "the test is too slow")

	const numStatements = 10

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	params, _ := createTestServerParamsAllowTenants()
	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
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
	for _, stmt := range setup {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
		t.Log(stmt + ";")
	}

	smith, err := sqlsmith.NewSmither(sqlDB, rng,
		sqlsmith.PrefixStringConsts(pii),
		sqlsmith.DisableDDLs(),
		sqlsmith.SimpleNames(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smith.Close()

	tests.GenerateAndCheckRedactedExplainsForPII(t, smith, numStatements, conn, containsPII)
}
