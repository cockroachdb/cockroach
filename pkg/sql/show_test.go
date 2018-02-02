// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
)

func TestShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE items (
			a int,
			b int,
			c int unique,
			primary key (a, b)
		);
		CREATE DATABASE o;
		CREATE TABLE o.public.foo(x int primary key);
	`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		stmt   string
		expect string // empty means identical to stmt
	}{
		{
			stmt: `CREATE TABLE %s (
	i INT,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT now(),
	CHECK (i > 0),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s)
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP NULL DEFAULT now(),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT check_i CHECK (i > 0)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT CHECK (i > 0),
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT now(),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s)
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP NULL DEFAULT now(),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT check_i CHECK (i > 0)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	CONSTRAINT ck CHECK (i > 0),
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s)
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	s STRING NULL,
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT ck CHECK (i > 0)
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	i INT PRIMARY KEY
)`,
			expect: `CREATE TABLE %s (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
)`,
		},
		{
			stmt: `
				CREATE TABLE %s (i INT, f FLOAT, s STRING, d DATE,
				  FAMILY "primary" (i, f, d, rowid),
				  FAMILY fam_1_s (s));
				CREATE INDEX idx_if on %[1]s (f, i) STORING (s, d);
				CREATE UNIQUE INDEX on %[1]s (d);
			`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	f FLOAT NULL,
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
	"te""st" INT NOT NULL,
	CONSTRAINT "pri""mary" PRIMARY KEY ("te""st" ASC),
	FAMILY "primary" ("te""st")
)`,
		},
		{
			stmt: `CREATE TABLE %s (
	a int,
	b int,
	index c(a asc, b desc)
)`,
			expect: `CREATE TABLE %s (
	a INT NULL,
	b INT NULL,
	INDEX c (a ASC, b DESC),
	FAMILY "primary" (a, b, rowid)
)`,
		},
		// Check that FK dependencies inside the current database
		// have their db name omitted.
		{
			stmt: `CREATE TABLE %s (
	i int,
	j int,
	FOREIGN KEY (i, j) REFERENCES items (a, b),
	k int REFERENCES items (c)
)`,
			expect: `CREATE TABLE %s (
	i INT NULL,
	j INT NULL,
	k INT NULL,
	CONSTRAINT fk_i_ref_items FOREIGN KEY (i, j) REFERENCES items (a, b),
	INDEX t7_auto_index_fk_i_ref_items (i ASC, j ASC),
	CONSTRAINT fk_k_ref_items FOREIGN KEY (k) REFERENCES items (c),
	INDEX t7_auto_index_fk_k_ref_items (k ASC),
	FAMILY "primary" (i, j, k, rowid)
)`,
		},
		// Check that FK dependencies outside of the current database
		// have their db name prefixed.
		{
			stmt: `CREATE TABLE %s (
	x INT,
	CONSTRAINT fk_ref FOREIGN KEY (x) REFERENCES o.public.foo (x)
)`,
			expect: `CREATE TABLE %s (
	x INT NULL,
	CONSTRAINT fk_ref FOREIGN KEY (x) REFERENCES o.public.foo (x),
	INDEX t8_auto_index_fk_ref (x ASC),
	FAMILY "primary" (x, rowid)
)`,
		},
		// Check that INTERLEAVE dependencies inside the current database
		// have their db name omitted.
		{
			stmt: `CREATE TABLE %s (
	a INT,
	b INT,
	PRIMARY KEY (a, b)
) INTERLEAVE IN PARENT items (a, b)`,
			expect: `CREATE TABLE %s (
	a INT NOT NULL,
	b INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (a ASC, b ASC),
	FAMILY "primary" (a, b)
) INTERLEAVE IN PARENT items (a, b)`,
		},
		// Check that INTERLEAVE dependencies outside of the current
		// database are prefixed by their db name.
		{
			stmt: `CREATE TABLE %s (
	x INT PRIMARY KEY
) INTERLEAVE IN PARENT o.public.foo (x)`,
			expect: `CREATE TABLE %s (
	x INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (x ASC),
	FAMILY "primary" (x)
) INTERLEAVE IN PARENT o.public.foo (x)`,
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			name := fmt.Sprintf("t%d", i)
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
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (i INT, s STRING NULL, v FLOAT NOT NULL, t TIMESTAMP DEFAULT NOW());
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
			`CREATE VIEW %s AS SELECT s, count(*) FROM t GROUP BY s HAVING count(*) > 3:::INT`,
			`CREATE VIEW %s (s, count) AS SELECT s, count(*) FROM d.public.t GROUP BY s HAVING count(*) > 3:::INT`,
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
	defer s.Stopper().Stop(context.TODO())

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

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						StatementFilter: func(ctx context.Context, stmt string, resultWriter sql.ResultsWriter, err error) error {
							if stmt == selectStmt {
								const showQuery = "SELECT node_id, query FROM [SHOW CLUSTER QUERIES]"

								rows, err := conn1.Query(showQuery)
								if err != nil {
									t.Fatal(err)
								}
								defer rows.Close()

								count := 0
								for rows.Next() {
									count++

									var nodeID int
									var sql string
									if err := rows.Scan(&nodeID, &sql); err != nil {
										t.Fatal(err)
									}
									switch sql {
									case showQuery, expectedSelectStmt:
									default:
										t.Fatalf(
											"unexpected query in SHOW QUERIES: %+q, expected: %+q",
											sql,
											expectedSelectStmt,
										)
									}
									if nodeID < 1 || nodeID > 2 {
										t.Fatalf("invalid node ID: %d", nodeID)
									}
								}
								if err := rows.Err(); err != nil {
									t.Fatal(err)
								}

								if expectedCount := 2; count != expectedCount {
									t.Fatalf("unexpected number of running queries: %d, expected %d", count, expectedCount)
								}
							}
							return nil
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)
	sqlutils.CreateTable(t, conn1, tableName, "num INT", 0, nil)

	if _, err := conn2.Exec(selectStmt); err != nil {
		t.Fatal(err)
	}

}

// TestShowJobs manually inserts a row into system.jobs and checks that the
// encoded protobuf payload is properly decoded and visible in
// crdb_internal.jobs.
func TestShowJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, rawSQLDB, _ := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	defer s.Stopper().Stop(context.TODO())

	// row represents a row returned from crdb_internal.jobs, but
	// *not* a row in system.jobs.
	type row struct {
		id                int64
		typ               string
		status            string
		description       string
		username          string
		err               string
		created           time.Time
		started           time.Time
		finished          time.Time
		modified          time.Time
		fractionCompleted float32
		coordinatorID     roachpb.NodeID
	}

	in := row{
		id:          42,
		typ:         "SCHEMA CHANGE",
		status:      "superfailed",
		description: "failjob",
		username:    "failure",
		err:         "boom",
		// lib/pq returns time.Time objects with goofy locations, which breaks
		// reflect.DeepEqual without this time.FixedZone song and dance.
		// See: https://github.com/lib/pq/issues/329
		created:           timeutil.Unix(1, 0).In(time.FixedZone("", 0)),
		started:           timeutil.Unix(2, 0).In(time.FixedZone("", 0)),
		finished:          timeutil.Unix(3, 0).In(time.FixedZone("", 0)),
		modified:          timeutil.Unix(4, 0).In(time.FixedZone("", 0)),
		fractionCompleted: 0.42,
		coordinatorID:     7,
	}

	// system.jobs is part proper SQL columns, part protobuf, so we can't use the
	// row struct directly.
	inPayload, err := protoutil.Marshal(&jobs.Payload{
		Description:       in.description,
		StartedMicros:     in.started.UnixNano() / time.Microsecond.Nanoseconds(),
		FinishedMicros:    in.finished.UnixNano() / time.Microsecond.Nanoseconds(),
		ModifiedMicros:    in.modified.UnixNano() / time.Microsecond.Nanoseconds(),
		FractionCompleted: in.fractionCompleted,
		Username:          in.username,
		Lease: &jobs.Lease{
			NodeID: 7,
		},
		Error:   in.err,
		Details: jobs.WrapPayloadDetails(jobs.SchemaChangeDetails{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t,
		`INSERT INTO system.public.jobs (id, status, created, payload) VALUES ($1, $2, $3, $4)`,
		in.id, in.status, in.created, inPayload,
	)

	var out row
	sqlDB.QueryRow(t, `
      SELECT id, type, status, created, description, started, finished, modified,
             fraction_completed, username, error, coordinator_id
        FROM crdb_internal.jobs`).Scan(
		&out.id, &out.typ, &out.status, &out.created, &out.description, &out.started,
		&out.finished, &out.modified, &out.fractionCompleted, &out.username,
		&out.err, &out.coordinatorID,
	)
	if !reflect.DeepEqual(in, out) {
		diff := strings.Join(pretty.Diff(in, out), "\n")
		t.Fatalf("in job did not match out job:\n%s", diff)
	}
}
