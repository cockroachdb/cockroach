// Copyright 2016 The Cockroach Authors.
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
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func TestShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE items (
			a int8,
			b int8,
			c int8 unique,
			primary key (a, b)
		);
		CREATE DATABASE o;
		CREATE TABLE o.foo(x int primary key);
	`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		stmt   string
		expect string // empty means identical to stmt
	}{
		{
			stmt: `CREATE TABLE %s (
	i INT8,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT now():::TIMESTAMP,
	CHECK (i > 0),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s)
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL,
	s STRING NULL,
	v FLOAT8 NOT NULL,
	t TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT check_i CHECK (i > 0:::INT8)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT8 CHECK (i > 0),
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT now():::TIMESTAMP,
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s)
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL,
	s STRING NULL,
	v FLOAT8 NOT NULL,
	t TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT check_i CHECK (i > 0:::INT8)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT8 NULL,
	s STRING NULL,
	CONSTRAINT ck CHECK (i > 0),
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s)
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL,
	s STRING NULL,
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT ck CHECK (i > 0:::INT8)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT8 PRIMARY KEY
)`,
			expect: `CREATE TABLE %s (
	i INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
)`,
		},
		{
			stmt: `
				CREATE TABLE %s (i INT8, f FLOAT, s STRING, d DATE,
				  FAMILY "primary" (i, f, d, rowid),
				  FAMILY fam_1_s (s));
				CREATE INDEX idx_if on %[1]s (f, i) STORING (s, d);
				CREATE UNIQUE INDEX on %[1]s (d);
			`,
			expect: `CREATE TABLE %s (
	i INT8 NULL,
	f FLOAT8 NULL,
	s STRING NULL,
	d DATE NULL,
	INDEX idx_if (f ASC, i ASC) STORING (s, d),
	UNIQUE INDEX %[1]s_d_key (d ASC),
	FAMILY "primary" (i, f, d, rowid),
	FAMILY fam_1_s (s)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	"te""st" INT8 NOT NULL,
	CONSTRAINT "pri""mary" PRIMARY KEY ("te""st" ASC),
	FAMILY "primary" ("te""st")
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	a int8,
	b int8,
	index c(a asc, b desc)
)`,
			expect: `CREATE TABLE %s (
	a INT8 NULL,
	b INT8 NULL,
	INDEX c (a ASC, b DESC),
	FAMILY "primary" (a, b, rowid)
)`,
		},
		// Check that FK dependencies inside the current database
		// have their db name omitted.
		{
			stmt: `CREATE TABLE %s (
	i int8,
	j int8,
	FOREIGN KEY (i, j) REFERENCES items (a, b),
	k int REFERENCES items (c)
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL,
	j INT8 NULL,
	k INT8 NULL,
	CONSTRAINT fk_i_ref_items FOREIGN KEY (i, j) REFERENCES items(a, b),
	CONSTRAINT fk_k_ref_items FOREIGN KEY (k) REFERENCES items(c),
	INDEX %[1]s_auto_index_fk_i_ref_items (i ASC, j ASC),
	INDEX %[1]s_auto_index_fk_k_ref_items (k ASC),
	FAMILY "primary" (i, j, k, rowid)
)`,
		},
		// Check that FK dependencies using MATCH FULL on a non-composite key still
		// show
		{
			stmt: `CREATE TABLE %s (
	i int8,
	j int8,
	k int REFERENCES items (c) MATCH FULL,
	FOREIGN KEY (i, j) REFERENCES items (a, b) MATCH FULL
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL,
	j INT8 NULL,
	k INT8 NULL,
	CONSTRAINT fk_i_ref_items FOREIGN KEY (i, j) REFERENCES items(a, b) MATCH FULL,
	CONSTRAINT fk_k_ref_items FOREIGN KEY (k) REFERENCES items(c) MATCH FULL,
	INDEX %[1]s_auto_index_fk_i_ref_items (i ASC, j ASC),
	INDEX %[1]s_auto_index_fk_k_ref_items (k ASC),
	FAMILY "primary" (i, j, k, rowid)
)`,
		},
		// Check that FK dependencies outside of the current database
		// have their db name prefixed.
		{
			stmt: `CREATE TABLE %s (
	x INT8,
	CONSTRAINT fk_ref FOREIGN KEY (x) REFERENCES o.foo (x)
)`,
			expect: `CREATE TABLE %s (
	x INT8 NULL,
	CONSTRAINT fk_ref FOREIGN KEY (x) REFERENCES o.public.foo(x),
	INDEX %[1]s_auto_index_fk_ref (x ASC),
	FAMILY "primary" (x, rowid)
)`,
		},
		// Check that FK dependencies using SET NULL or SET DEFAULT
		// are pretty-printed properly. Regression test for #32529.
		{
			stmt: `CREATE TABLE %s (
	i int8 DEFAULT 123,
	j int8 DEFAULT 123,
	FOREIGN KEY (i, j) REFERENCES items (a, b) ON DELETE SET DEFAULT,
	k int8 REFERENCES items (c) ON DELETE SET NULL
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL DEFAULT 123:::INT8,
	j INT8 NULL DEFAULT 123:::INT8,
	k INT8 NULL,
	CONSTRAINT fk_i_ref_items FOREIGN KEY (i, j) REFERENCES items(a, b) ON DELETE SET DEFAULT,
	CONSTRAINT fk_k_ref_items FOREIGN KEY (k) REFERENCES items(c) ON DELETE SET NULL,
	INDEX %[1]s_auto_index_fk_i_ref_items (i ASC, j ASC),
	INDEX %[1]s_auto_index_fk_k_ref_items (k ASC),
	FAMILY "primary" (i, j, k, rowid)
)`,
		},
		// Check that INTERLEAVE dependencies inside the current database
		// have their db name omitted.
		{
			stmt: `CREATE TABLE %s (
	a INT8,
	b INT8,
	PRIMARY KEY (a, b)
) INTERLEAVE IN PARENT items (a, b)`,
			expect: `CREATE TABLE %s (
	a INT8 NOT NULL,
	b INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (a ASC, b ASC),
	FAMILY "primary" (a, b)
) INTERLEAVE IN PARENT items (a, b)`,
		},
		// Check that INTERLEAVE dependencies outside of the current
		// database are prefixed by their db name.
		{
			stmt: `CREATE TABLE %s (
	x INT8 PRIMARY KEY
) INTERLEAVE IN PARENT o.foo (x)`,
			expect: `CREATE TABLE %s (
	x INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (x ASC),
	FAMILY "primary" (x)
) INTERLEAVE IN PARENT o.public.foo (x)`,
		},
		// Check that FK dependencies using MATCH FULL and MATCH SIMPLE are both
		// pretty-printed properly.
		{
			stmt: `CREATE TABLE %s (
	i int DEFAULT 1,
	j int DEFAULT 2,
	k int DEFAULT 3,
	l int DEFAULT 4,
	FOREIGN KEY (i, j) REFERENCES items (a, b) MATCH SIMPLE ON DELETE SET DEFAULT,
	FOREIGN KEY (k, l) REFERENCES items (a, b) MATCH FULL ON UPDATE CASCADE
)`,
			expect: `CREATE TABLE %s (
	i INT8 NULL DEFAULT 1:::INT8,
	j INT8 NULL DEFAULT 2:::INT8,
	k INT8 NULL DEFAULT 3:::INT8,
	l INT8 NULL DEFAULT 4:::INT8,
	CONSTRAINT fk_i_ref_items FOREIGN KEY (i, j) REFERENCES items(a, b) ON DELETE SET DEFAULT,
	CONSTRAINT fk_k_ref_items FOREIGN KEY (k, l) REFERENCES items(a, b) MATCH FULL ON UPDATE CASCADE,
	INDEX %[1]s_auto_index_fk_i_ref_items (i ASC, j ASC),
	INDEX %[1]s_auto_index_fk_k_ref_items (k ASC, l ASC),
	FAMILY "primary" (i, j, k, l, rowid)
)`,
		},
	}
	for i, test := range tests {
		name := fmt.Sprintf("t%d", i)
		t.Run(name, func(t *testing.T) {
			if test.expect == "" {
				test.expect = test.stmt
			}
			stmt := fmt.Sprintf(test.stmt, name)
			expect := fmt.Sprintf(test.expect, name)
			if _, err := sqlDB.Exec(stmt); err != nil {
				t.Fatal(err)
			}
			row := sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", name))
			var scanName, create string
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if scanName != name {
				t.Fatalf("expected table name %s, got %s", name, scanName)
			}
			if create != expect {
				t.Fatalf("statement: %s\ngot: %s\nexpected: %s", stmt, create, expect)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
				t.Fatal(err)
			}
			// Re-insert to make sure it's round-trippable.
			name += "_2"
			expect = fmt.Sprintf(test.expect, name)
			if _, err := sqlDB.Exec(expect); err != nil {
				t.Fatalf("reinsert failure: %s: %s", expect, err)
			}
			row = sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", name))
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if create != expect {
				t.Fatalf("round trip statement: %s\ngot: %s", expect, create)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", name)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShowCreateView(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (i INT, s STRING NULL, v FLOAT NOT NULL, t TIMESTAMP DEFAULT now());
	`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		create   string
		expected string
	}{
		{
			`CREATE VIEW %s AS SELECT i, s, v, t FROM t`,
			`CREATE VIEW %s (i, s, v, t) AS SELECT i, s, v, t FROM d.public.t`,
		},
		{
			`CREATE VIEW %s AS SELECT i, s, t FROM t`,
			`CREATE VIEW %s (i, s, t) AS SELECT i, s, t FROM d.public.t`,
		},
		{
			`CREATE VIEW %s AS SELECT t.i, t.s, t.t FROM t`,
			`CREATE VIEW %s (i, s, t) AS SELECT t.i, t.s, t.t FROM d.public.t`,
		},
		{
			`CREATE VIEW %s AS SELECT foo.i, foo.s, foo.t FROM t AS foo WHERE foo.i > 3`,
			`CREATE VIEW %s (i, s, t) AS SELECT foo.i, foo.s, foo.t FROM d.public.t AS foo WHERE foo.i > 3`,
		},
		{
			`CREATE VIEW %s AS SELECT count(*) FROM t`,
			`CREATE VIEW %s (count) AS SELECT count(*) FROM d.public.t`,
		},
		{
			`CREATE VIEW %s AS SELECT s, count(*) FROM t GROUP BY s HAVING count(*) > 3:::INT8`,
			`CREATE VIEW %s (s, count) AS SELECT s, count(*) FROM d.public.t GROUP BY s HAVING count(*) > 3:::INT8`,
		},
		{
			`CREATE VIEW %s (a, b, c, d) AS SELECT i, s, v, t FROM t`,
			`CREATE VIEW %s (a, b, c, d) AS SELECT i, s, v, t FROM d.public.t`,
		},
		{
			`CREATE VIEW %s (a, b) AS SELECT i, v FROM t`,
			`CREATE VIEW %s (a, b) AS SELECT i, v FROM d.public.t`,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			name := fmt.Sprintf("t%d", i)
			stmt := fmt.Sprintf(test.create, name)
			expect := fmt.Sprintf(test.expected, name)
			if _, err := sqlDB.Exec(stmt); err != nil {
				t.Fatal(err)
			}
			row := sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE VIEW %s", name))
			var scanName, create string
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if scanName != name {
				t.Fatalf("expected view name %s, got %s", name, scanName)
			}
			if create != expect {
				t.Fatalf("statement: %s\ngot: %s\nexpected: %s", stmt, create, expect)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP VIEW %s", name)); err != nil {
				t.Fatal(err)
			}
			// Re-insert to make sure it's round-trippable.
			name += "_2"
			expect = fmt.Sprintf(test.expected, name)
			if _, err := sqlDB.Exec(expect); err != nil {
				t.Fatalf("reinsert failure: %s: %s", expect, err)
			}
			row = sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE VIEW %s", name))
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if create != expect {
				t.Fatalf("round trip statement: %s\ngot: %s", expect, create)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP VIEW %s", name)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShowCreateSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
	`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		create   string
		expected string
	}{
		{
			`CREATE SEQUENCE %s`,
			`CREATE SEQUENCE %s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
		},
		{
			`CREATE SEQUENCE %s INCREMENT BY 5`,
			`CREATE SEQUENCE %s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 5 START 1`,
		},
		{
			`CREATE SEQUENCE %s START WITH 5`,
			`CREATE SEQUENCE %s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 5`,
		},
		{
			`CREATE SEQUENCE %s INCREMENT 5 MAXVALUE 10000 START 10 MINVALUE 0`,
			`CREATE SEQUENCE %s MINVALUE 0 MAXVALUE 10000 INCREMENT 5 START 10`,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			name := fmt.Sprintf("t%d", i)
			stmt := fmt.Sprintf(test.create, name)
			expect := fmt.Sprintf(test.expected, name)
			if _, err := sqlDB.Exec(stmt); err != nil {
				t.Fatal(err)
			}
			row := sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE SEQUENCE %s", name))
			var scanName, create string
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if scanName != name {
				t.Fatalf("expected view name %s, got %s", name, scanName)
			}
			if create != expect {
				t.Fatalf("statement: %s\ngot: %s\nexpected: %s", stmt, create, expect)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP SEQUENCE %s", name)); err != nil {
				t.Fatal(err)
			}
			// Re-insert to make sure it's round-trippable.
			name += "_2"
			expect = fmt.Sprintf(test.expected, name)
			if _, err := sqlDB.Exec(expect); err != nil {
				t.Fatalf("reinsert failure: %s: %s", expect, err)
			}
			row = sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE SEQUENCE %s", name))
			if err := row.Scan(&scanName, &create); err != nil {
				t.Fatal(err)
			}
			if create != expect {
				t.Fatalf("round trip statement: %s\ngot: %s", expect, create)
			}
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP SEQUENCE %s", name)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShowQueries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const multiByte = "💩"
	const selectBase = "SELECT * FROM "

	maxLen := sql.MaxSQLBytes - utf8.RuneLen('…')

	// Craft a statement that would naively be truncated mid-rune.
	tableName := strings.Repeat("a", maxLen-len(selectBase)-(len(multiByte)-1)) + multiByte
	// Push the total length over the truncation threshold.
	tableName += strings.Repeat("a", sql.MaxSQLBytes-len(tableName)+1)
	selectStmt := selectBase + tableName

	if r, _ := utf8.DecodeLastRuneInString(selectStmt[:maxLen]); r != utf8.RuneError {
		t.Fatalf("expected naive truncation to produce invalid utf8, got %c", r)
	}
	expectedSelectStmt := selectStmt
	for i := range expectedSelectStmt {
		if i > maxLen {
			_, prevLen := utf8.DecodeLastRuneInString(expectedSelectStmt[:i])
			expectedSelectStmt = expectedSelectStmt[:i-prevLen]
			break
		}
	}
	expectedSelectStmt = expectedSelectStmt + "…"

	var conn1 *gosql.DB
	var conn2 *gosql.DB

	execKnobs := &sql.ExecutorTestingKnobs{}

	found := false
	var failure error

	execKnobs.StatementFilter = func(ctx context.Context, stmt string, err error) {
		if stmt == selectStmt {
			found = true
			const showQuery = "SELECT node_id, (now() - start)::FLOAT8, query FROM [SHOW CLUSTER QUERIES]"

			rows, err := conn1.Query(showQuery)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			var stmts []string
			for rows.Next() {
				var nodeID int
				var stmt string
				var delta float64
				if err := rows.Scan(&nodeID, &delta, &stmt); err != nil {
					failure = err
					return
				}
				stmts = append(stmts, stmt)
				if nodeID < 1 || nodeID > 2 {
					failure = fmt.Errorf("invalid node ID: %d", nodeID)
					return
				}

				// The delta measures how long ago or in the future (in
				// seconds) the start time is. It must be
				// "close to now", otherwise we have a problem with the time
				// accounting.
				if math.Abs(delta) > 10 {
					failure = fmt.Errorf("start time too far in the past or the future: expected <10s, got %.3fs", delta)
					return
				}
			}
			if err := rows.Err(); err != nil {
				failure = err
				return
			}

			foundSelect := false
			for _, stmt := range stmts {
				if stmt == expectedSelectStmt {
					foundSelect = true
				}
			}
			if !foundSelect {
				failure = fmt.Errorf("original query not found in SHOW QUERIES. expected: %s\nactual: %v", selectStmt, stmts)
			}
		}
	}

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: execKnobs,
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)
	sqlutils.CreateTable(t, conn1, tableName, "num INT", 0, nil)

	if _, err := conn2.Exec(selectStmt); err != nil {
		t.Fatal(err)
	}

	if failure != nil {
		t.Fatal(failure)
	}

	if !found {
		t.Fatalf("knob did not activate in test")
	}

	// Now check the behavior on error.
	tc.StopServer(1)

	rows, err := conn1.Query(`SELECT node_id, query FROM [SHOW ALL CLUSTER QUERIES]`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	errcount := 0
	for rows.Next() {
		count++

		var nodeID int
		var sql string
		if err := rows.Scan(&nodeID, &sql); err != nil {
			t.Fatal(err)
		}
		t.Log(sql)
		if strings.HasPrefix(sql, "-- failed") || strings.HasPrefix(sql, "-- error") {
			errcount++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if errcount != 1 {
		t.Fatalf("expected 1 error row, got %d", errcount)
	}
}

func TestShowSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var conn *gosql.DB

	tc := serverutils.StartTestCluster(t, 2 /* numNodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	conn = tc.ServerConn(0)
	sqlutils.CreateTable(t, conn, "t", "num INT", 0, nil)

	// We'll skip "internal" sessions, as those are unpredictable.
	var showSessions = fmt.Sprintf(`
	select node_id, (now() - session_start)::float from
		[show cluster sessions] where application_name not like '%s%%'
	`, sqlbase.InternalAppNamePrefix)

	rows, err := conn.Query(showSessions)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++

		var nodeID int
		var delta float64
		if err := rows.Scan(&nodeID, &delta); err != nil {
			t.Fatal(err)
		}
		if nodeID < 1 || nodeID > 2 {
			t.Fatalf("invalid node ID: %d", nodeID)
		}

		// The delta measures how long ago or in the future (in seconds) the start
		// time is. It must be "close to now", otherwise we have a problem with the
		// time accounting.
		if math.Abs(delta) > 10 {
			t.Fatalf("start time too far in the past or the future: expected <10s, got %.3fs", delta)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if expectedCount := 1; count != expectedCount {
		// Print the sessions to aid debugging.
		report, err := func() (string, error) {
			result := "Active sessions (results might have changed since the test checked):\n"
			rows, err = conn.Query(`
				select active_queries, last_active_query, application_name
					from [show cluster sessions]`)
			if err != nil {
				return "", err
			}
			var q, lq, name string
			for rows.Next() {
				if err := rows.Scan(&q, &lq, &name); err != nil {
					return "", err
				}
				result += fmt.Sprintf("app: %q, query: %q, last query: %s",
					name, q, lq)
			}
			if err := rows.Close(); err != nil {
				return "", err
			}
			return result, nil
		}()
		if err != nil {
			report = fmt.Sprintf("failed to generate report: %s", err)
		}

		t.Fatalf("unexpected number of running sessions: %d, expected %d.\n%s",
			count, expectedCount, report)
	}

	// Now check the behavior on error.
	tc.StopServer(1)

	rows, err = conn.Query(`SELECT node_id, active_queries FROM [SHOW ALL CLUSTER SESSIONS]`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count = 0
	errcount := 0
	for rows.Next() {
		count++

		var nodeID int
		var sql string
		if err := rows.Scan(&nodeID, &sql); err != nil {
			t.Fatal(err)
		}
		t.Log(sql)
		if strings.HasPrefix(sql, "-- failed") || strings.HasPrefix(sql, "-- error") {
			errcount++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if errcount != 1 {
		t.Fatalf("expected 1 error row, got %d", errcount)
	}
}

func TestShowSessionPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, rawSQLDBroot, _ := serverutils.StartServer(t, params)
	sqlDBroot := sqlutils.MakeSQLRunner(rawSQLDBroot)
	defer s.Stopper().Stop(context.Background())

	// Prepare a non-root session.
	_ = sqlDBroot.Exec(t, `CREATE USER nonroot`)
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User("nonroot"),
		Host:     s.ServingSQLAddr(),
		RawQuery: "sslmode=disable",
	}
	rawSQLDBnonroot, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rawSQLDBnonroot.Close()
	sqlDBnonroot := sqlutils.MakeSQLRunner(rawSQLDBnonroot)

	// Ensure the non-root session is open.
	sqlDBnonroot.Exec(t, `SELECT version()`)

	t.Run("root", func(t *testing.T) {
		// Verify that the root session can use SHOW SESSIONS properly and
		// can observe other sessions than its own.
		rows := sqlDBroot.Query(t, `SELECT user_name FROM [SHOW CLUSTER SESSIONS]`)
		defer rows.Close()
		counts := map[string]int{}
		for rows.Next() {
			var userName string
			if err := rows.Scan(&userName); err != nil {
				t.Fatal(err)
			}
			counts[userName]++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if counts[security.RootUser] == 0 {
			t.Fatalf("root session is unable to see its own session: %+v", counts)
		}
		if counts["nonroot"] == 0 {
			t.Fatal("root session is unable to see non-root session")
		}
	})

	t.Run("non-root", func(t *testing.T) {
		// Verify that the non-root session can use SHOW SESSIONS properly
		// and cannot observe other sessions than its own.
		rows := sqlDBnonroot.Query(t, `SELECT user_name FROM [SHOW CLUSTER SESSIONS]`)
		defer rows.Close()
		counts := map[string]int{}
		for rows.Next() {
			var userName string
			if err := rows.Scan(&userName); err != nil {
				t.Fatal(err)
			}
			counts[userName]++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if counts["nonroot"] == 0 {
			t.Fatal("non-root session is unable to see its own session")
		}
		if len(counts) > 1 {
			t.Fatalf("non-root session is able to see other sessions: %+v", counts)
		}
	})
}

func TestLintClusterSettingNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	rows, err := sqlDB.Query(`SELECT variable, setting_type, description FROM [SHOW ALL CLUSTER SETTINGS]`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var varName, sType, desc string
		if err := rows.Scan(&varName, &sType, &desc); err != nil {
			t.Fatal(err)
		}

		if strings.ToLower(varName) != varName {
			t.Errorf("%s: variable name must be all lowercase", varName)
		}

		suffixSuggestions := map[string]string{
			"_ttl":     ".ttl",
			"_enabled": ".enabled",
			"_timeout": ".timeout",
		}

		nameErr := func() error {
			segments := strings.Split(varName, ".")
			for _, segment := range segments {
				if strings.TrimSpace(segment) != segment {
					return errors.Errorf("%s: part %q has heading or trailing whitespace", varName, segment)
				}
				tokens, ok := parser.Tokens(segment)
				if !ok {
					return errors.Errorf("%s: part %q does not scan properly", varName, segment)
				}
				if len(tokens) == 0 || len(tokens) > 1 {
					return errors.Errorf("%s: part %q has invalid structure", varName, segment)
				}
				if tokens[0].TokenID != parser.IDENT {
					cat, ok := lex.KeywordsCategories[tokens[0].Str]
					if !ok {
						return errors.Errorf("%s: part %q has invalid structure", varName, segment)
					}
					if cat == "R" {
						return errors.Errorf("%s: part %q is a reserved keyword", varName, segment)
					}
				}
			}

			for suffix, repl := range suffixSuggestions {
				if strings.HasSuffix(varName, suffix) {
					return errors.Errorf("%s: use %q instead of %q", varName, repl, suffix)
				}
			}

			if sType == "b" && !strings.HasSuffix(varName, ".enabled") {
				return errors.Errorf("%s: use .enabled for booleans", varName)
			}

			return nil
		}()
		if nameErr != nil {
			var grandFathered = map[string]string{
				"server.declined_reservation_timeout":                `server.declined_reservation_timeout: use ".timeout" instead of "_timeout"`,
				"server.failed_reservation_timeout":                  `server.failed_reservation_timeout: use ".timeout" instead of "_timeout"`,
				"server.web_session_timeout":                         `server.web_session_timeout: use ".timeout" instead of "_timeout"`,
				"sql.distsql.flow_stream_timeout":                    `sql.distsql.flow_stream_timeout: use ".timeout" instead of "_timeout"`,
				"debug.panic_on_failed_assertions":                   `debug.panic_on_failed_assertions: use .enabled for booleans`,
				"diagnostics.reporting.send_crash_reports":           `diagnostics.reporting.send_crash_reports: use .enabled for booleans`,
				"kv.closed_timestamp.follower_reads_enabled":         `kv.closed_timestamp.follower_reads_enabled: use ".enabled" instead of "_enabled"`,
				"kv.raft_log.disable_synchronization_unsafe":         `kv.raft_log.disable_synchronization_unsafe: use .enabled for booleans`,
				"kv.range_merge.queue_enabled":                       `kv.range_merge.queue_enabled: use ".enabled" instead of "_enabled"`,
				"kv.range_split.by_load_enabled":                     `kv.range_split.by_load_enabled: use ".enabled" instead of "_enabled"`,
				"kv.transaction.parallel_commits_enabled":            `kv.transaction.parallel_commits_enabled: use ".enabled" instead of "_enabled"`,
				"kv.transaction.write_pipelining_enabled":            `kv.transaction.write_pipelining_enabled: use ".enabled" instead of "_enabled"`,
				"server.clock.forward_jump_check_enabled":            `server.clock.forward_jump_check_enabled: use ".enabled" instead of "_enabled"`,
				"sql.defaults.experimental_optimizer_mutations":      `sql.defaults.experimental_optimizer_mutations: use .enabled for booleans`,
				"sql.distsql.distribute_index_joins":                 `sql.distsql.distribute_index_joins: use .enabled for booleans`,
				"sql.metrics.statement_details.dump_to_logs":         `sql.metrics.statement_details.dump_to_logs: use .enabled for booleans`,
				"sql.metrics.statement_details.sample_logical_plans": `sql.metrics.statement_details.sample_logical_plans: use .enabled for booleans`,
				"sql.trace.log_statement_execute":                    `sql.trace.log_statement_execute: use .enabled for booleans`,
				"trace.debug.enable":                                 `trace.debug.enable: use .enabled for booleans`,
				"cloudstorage.gs.default.key":                        `cloudstorage.gs.default.key: part "default" is a reserved keyword`,
				// These two settings have been deprecated in favor of a new (better named) setting
				// but the old name is still around to support migrations.
				// TODO(knz): remove these cases when these settings are retired.
				"timeseries.storage.10s_resolution_ttl": `timeseries.storage.10s_resolution_ttl: part "10s_resolution_ttl" has invalid structure`,
				"timeseries.storage.30m_resolution_ttl": `timeseries.storage.30m_resolution_ttl: part "30m_resolution_ttl" has invalid structure`,
			}
			expectedErr, found := grandFathered[varName]
			if !found || expectedErr != nameErr.Error() {
				t.Error(nameErr)
			}
		}

		if strings.TrimSpace(desc) != desc {
			t.Errorf("%s: description %q has heading or trailing whitespace", varName, desc)
		}

		if len(desc) == 0 {
			t.Errorf("%s: description is empty", varName)
		}

		if len(desc) > 0 {
			if strings.ToLower(desc[0:1]) != desc[0:1] {
				t.Errorf("%s: description %q must not start with capital", varName, desc)
			}
			if strings.Contains(desc, ". ") != (desc[len(desc)-1] == '.') {
				t.Errorf("%s: description %q must end with period if and only if it contains a secondary sentence", varName, desc)
			}
		}
	}

}

// TestCancelQueriesRace can be stressed to try and reproduce a race
// between SHOW QUERIES and currently executing statements. For
// more details, see #28033.
func TestCancelQueriesRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	waiter := make(chan struct{})
	go func() {
		_, _ = sqlDB.ExecContext(ctx, `SELECT pg_sleep(10)`)
		close(waiter)
	}()
	_, _ = sqlDB.ExecContext(ctx, `CANCEL QUERIES (
		SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'SELECT pg_sleep%'
	)`)
	_, _ = sqlDB.ExecContext(ctx, `CANCEL QUERIES (
		SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'SELECT pg_sleep%'
	)`)

	cancel()
	<-waiter
}
