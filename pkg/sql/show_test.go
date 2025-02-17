// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestShowCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []sqltestutils.ShowCreateTableTestCase{
		{
			CreateStatement: `CREATE TABLE %s (
	i INT8,
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT now():::TIMESTAMP,
	CHECK (i > 0),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL,
	s STRING NULL,
	v FLOAT8 NOT NULL,
	t TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT check_i CHECK (i > 0:::INT8)
)`,
		},
		{
			CreateStatement: `CREATE TABLE %s (
	i INT8 CHECK (i > 0),
	s STRING NULL,
	v FLOAT NOT NULL,
	t TIMESTAMP DEFAULT now():::TIMESTAMP,
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL,
	s STRING NULL,
	v FLOAT8 NOT NULL,
	t TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	FAMILY "primary" (i, v, t, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT check_i CHECK (i > 0:::INT8)
)`,
		},
		{
			CreateStatement: `CREATE TABLE %s (
	i INT8 NULL,
	s STRING NULL,
	CONSTRAINT ck CHECK (i > 0),
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL,
	s STRING NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	FAMILY "primary" (i, rowid),
	FAMILY fam_1_s (s),
	CONSTRAINT ck CHECK (i > 0:::INT8)
)`,
		},
		{
			CreateStatement: `CREATE TABLE %s (
	i INT8 PRIMARY KEY
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NOT NULL,
	CONSTRAINT %[1]s_pkey PRIMARY KEY (i ASC)
)`,
		},
		{
			CreateStatement: `
				CREATE TABLE %s (i INT8, f FLOAT, s STRING, d DATE,
				  FAMILY "primary" (i, f, d, rowid),
				  FAMILY fam_1_s (s));
				CREATE INDEX idx_if on %[1]s (f, i) STORING (s, d);
				CREATE UNIQUE INDEX on %[1]s (d);
			`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL,
	f FLOAT8 NULL,
	s STRING NULL,
	d DATE NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	INDEX idx_if (f ASC, i ASC) STORING (s, d),
	UNIQUE INDEX %[1]s_d_key (d ASC),
	FAMILY "primary" (i, f, d, rowid),
	FAMILY fam_1_s (s)
)`,
		},
		{
			CreateStatement: `CREATE TABLE %s (
	"te""st" INT8 NOT NULL,
	CONSTRAINT "pri""mary" PRIMARY KEY ("te""st" ASC)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	"te""st" INT8 NOT NULL,
	CONSTRAINT "pri""mary" PRIMARY KEY ("te""st" ASC)
)`,
		},
		{
			CreateStatement: `CREATE TABLE %s (
	a int8,
	b int8,
	index c(a asc, b desc)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	b INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	INDEX c (a ASC, b DESC)
)`,
		},

		{
			CreateStatement: `CREATE TABLE %s (
	pk int8 PRIMARY KEY
) WITH (ttl_expire_after = '10 minutes', ttl_job_cron = '@hourly')`,
			Expect: `CREATE TABLE public.%[1]s (
	pk INT8 NOT NULL,
	crdb_internal_expiration TIMESTAMPTZ NOT VISIBLE NOT NULL DEFAULT current_timestamp():::TIMESTAMPTZ + '00:10:00':::INTERVAL ON UPDATE current_timestamp():::TIMESTAMPTZ + '00:10:00':::INTERVAL,
	CONSTRAINT %[1]s_pkey PRIMARY KEY (pk ASC)
) WITH (ttl = 'on', ttl_expire_after = '00:10:00':::INTERVAL, ttl_job_cron = '@hourly')`,
		},
		// Check that FK dependencies inside the current database
		// have their db name omitted.
		{
			CreateStatement: `CREATE TABLE %s (
	i int8,
	j int8,
	FOREIGN KEY (i, j) REFERENCES items (a, b),
	k int REFERENCES items (c)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL,
	j INT8 NULL,
	k INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	CONSTRAINT %[1]s_i_j_fkey FOREIGN KEY (i, j) REFERENCES public.items(a, b),
	CONSTRAINT %[1]s_k_fkey FOREIGN KEY (k) REFERENCES public.items(c)
)`,
		},
		// Check that FK dependencies using MATCH FULL on a non-composite key still
		// show
		{
			CreateStatement: `CREATE TABLE %s (
	i int8,
	j int8,
	k int REFERENCES items (c) MATCH FULL,
	FOREIGN KEY (i, j) REFERENCES items (a, b) MATCH FULL
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL,
	j INT8 NULL,
	k INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	CONSTRAINT %[1]s_i_j_fkey FOREIGN KEY (i, j) REFERENCES public.items(a, b) MATCH FULL,
	CONSTRAINT %[1]s_k_fkey FOREIGN KEY (k) REFERENCES public.items(c) MATCH FULL
)`,
		},
		// Check that FK dependencies outside of the current database
		// have their db name prefixed.
		{
			CreateStatement: `CREATE TABLE %s (
	x INT8,
	CONSTRAINT fk_ref FOREIGN KEY (x) REFERENCES o.foo (x)
)`,
			Expect: `CREATE TABLE public.%[1]s (
	x INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	CONSTRAINT fk_ref FOREIGN KEY (x) REFERENCES o.public.foo(x)
)`,
		},
		// Check that FK dependencies using SET NULL or SET DEFAULT
		// are pretty-printed properly. Regression test for #32529.
		{
			CreateStatement: `CREATE TABLE %s (
	i int8 DEFAULT 123,
	j int8 DEFAULT 123,
	FOREIGN KEY (i, j) REFERENCES items (a, b) ON DELETE SET DEFAULT,
	k int8 REFERENCES items (c) ON DELETE SET NULL
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL DEFAULT 123:::INT8,
	j INT8 NULL DEFAULT 123:::INT8,
	k INT8 NULL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	CONSTRAINT %[1]s_i_j_fkey FOREIGN KEY (i, j) REFERENCES public.items(a, b) ON DELETE SET DEFAULT,
	CONSTRAINT %[1]s_k_fkey FOREIGN KEY (k) REFERENCES public.items(c) ON DELETE SET NULL
)`,
		},
		// Check that FK dependencies using MATCH FULL and MATCH SIMPLE are both
		// pretty-printed properly.
		{
			CreateStatement: `CREATE TABLE %s (
	i int DEFAULT 1,
	j int DEFAULT 2,
	k int DEFAULT 3,
	l int DEFAULT 4,
	FOREIGN KEY (i, j) REFERENCES items (a, b) MATCH SIMPLE ON DELETE SET DEFAULT,
	FOREIGN KEY (k, l) REFERENCES items (a, b) MATCH FULL ON UPDATE CASCADE
)`,
			Expect: `CREATE TABLE public.%[1]s (
	i INT8 NULL DEFAULT 1:::INT8,
	j INT8 NULL DEFAULT 2:::INT8,
	k INT8 NULL DEFAULT 3:::INT8,
	l INT8 NULL DEFAULT 4:::INT8,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	CONSTRAINT %[1]s_i_j_fkey FOREIGN KEY (i, j) REFERENCES public.items(a, b) ON DELETE SET DEFAULT,
	CONSTRAINT %[1]s_k_l_fkey FOREIGN KEY (k, l) REFERENCES public.items(a, b) MATCH FULL ON UPDATE CASCADE
)`,
		},
		// Check hash sharded indexes are round trippable.
		{
			CreateStatement: `CREATE TABLE %s (
				a INT,
				INDEX (a) USING HASH WITH (bucket_count=8)
			)`,
			Expect: `CREATE TABLE public.%[1]s (
	a INT8 NULL,
	crdb_internal_a_shard_8 INT8 NOT VISIBLE NOT NULL AS (mod(fnv32(md5(crdb_internal.datums_to_bytes(a))), 8:::INT8)) VIRTUAL,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT %[1]s_pkey PRIMARY KEY (rowid ASC),
	INDEX %[1]s_a_idx (a ASC) USING HASH WITH (bucket_count=8)
)`,
		},
		// Check trigram inverted indexes.
		{
			CreateStatement: `CREATE TABLE %s (
        id INT PRIMARY KEY,
				a TEXT,
				INVERTED INDEX (a gin_trgm_ops)
			)`,
			Expect: `CREATE TABLE public.%[1]s (
	id INT8 NOT NULL,
	a STRING NULL,
	CONSTRAINT %[1]s_pkey PRIMARY KEY (id ASC),
	INVERTED INDEX %[1]s_a_idx (a gin_trgm_ops)
)`,
		},
	}
	sqltestutils.ShowCreateTableTest(t, "" /* extraQuerySetup */, testCases)
}

func TestShowCreateView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
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
			"CREATE VIEW public.%s (\n\ti,\n\ts,\n\tv,\n\tt\n) AS SELECT i, s, v, t FROM d.public.t",
		},
		{
			`CREATE VIEW %s AS SELECT i, s, t FROM t`,
			"CREATE VIEW public.%s (\n\ti,\n\ts,\n\tt\n) AS SELECT i, s, t FROM d.public.t",
		},
		{
			`CREATE VIEW %s AS SELECT t.i, t.s, t.t FROM t`,
			"CREATE VIEW public.%s (\n\ti,\n\ts,\n\tt\n) AS SELECT t.i, t.s, t.t FROM d.public.t",
		},
		{
			`CREATE VIEW %s AS SELECT foo.i, foo.s, foo.t FROM t AS foo WHERE foo.i > 3`,
			"CREATE VIEW public.%s (\n\ti,\n\ts,\n\tt\n) AS " +
				"SELECT foo.i, foo.s, foo.t FROM d.public.t AS foo WHERE foo.i > 3",
		},
		{
			`CREATE VIEW %s AS SELECT count(*) FROM t`,
			"CREATE VIEW public.%s (\n\tcount\n) AS SELECT count(*) FROM d.public.t",
		},
		{
			`CREATE VIEW %s AS SELECT s, count(*) FROM t GROUP BY s HAVING count(*) > 3:::INT8`,
			"CREATE VIEW public.%s (\n\ts,\n\tcount\n) AS " +
				"SELECT s, count(*) FROM d.public.t GROUP BY s HAVING count(*) > 3:::INT8",
		},
		{
			`CREATE VIEW %s (a, b, c, d) AS SELECT i, s, v, t FROM t`,
			"CREATE VIEW public.%s (\n\ta,\n\tb,\n\tc,\n\td\n) AS SELECT i, s, v, t FROM d.public.t",
		},
		{
			`CREATE VIEW %s (a, b) AS SELECT i, v FROM t`,
			"CREATE VIEW public.%s (\n\ta,\n\tb\n) AS SELECT i, v FROM d.public.t",
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
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
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
			`CREATE SEQUENCE public.%s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
		},
		{
			`CREATE SEQUENCE %s INCREMENT BY 5`,
			`CREATE SEQUENCE public.%s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 5 START 1`,
		},
		{
			`CREATE SEQUENCE %s START WITH 5`,
			`CREATE SEQUENCE public.%s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 5`,
		},
		{
			`CREATE SEQUENCE %s INCREMENT 5 MAXVALUE 10000 START 10 MINVALUE 0`,
			`CREATE SEQUENCE public.%s MINVALUE 0 MAXVALUE 10000 INCREMENT 5 START 10`,
		},
		{
			`CREATE SEQUENCE %s INCREMENT 5 MAXVALUE 10000 START 10 MINVALUE 0 CACHE 1`,
			`CREATE SEQUENCE public.%s MINVALUE 0 MAXVALUE 10000 INCREMENT 5 START 10`,
		},
		{
			`CREATE SEQUENCE %s INCREMENT 5 MAXVALUE 10000 START 10 MINVALUE 0 CACHE 10`,
			`CREATE SEQUENCE public.%s MINVALUE 0 MAXVALUE 10000 INCREMENT 5 START 10 CACHE 10`,
		},
		{
			`CREATE SEQUENCE %s AS smallint`,
			`CREATE SEQUENCE public.%s AS INT2 MINVALUE 1 MAXVALUE 32767 INCREMENT 1 START 1`,
		},
		{
			`CREATE SEQUENCE %s AS int2`,
			`CREATE SEQUENCE public.%s AS INT2 MINVALUE 1 MAXVALUE 32767 INCREMENT 1 START 1`,
		},
		// Int type is determined by `default_int_size` in cluster settings. Default is int8.
		{
			`CREATE SEQUENCE %s AS int`,
			`CREATE SEQUENCE public.%s AS INT8 MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
		},
		{
			`CREATE SEQUENCE %s AS bigint`,
			`CREATE SEQUENCE public.%s AS INT8 MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
		},
		// Override int/bigint's max value with user configured max value.
		{
			`CREATE SEQUENCE %s AS integer MINVALUE -5 MAXVALUE 9001`,
			`CREATE SEQUENCE public.%s AS INT8 MINVALUE -5 MAXVALUE 9001 INCREMENT 1 START -5`,
		},
		{
			`
			CREATE SEQUENCE %s AS integer
			START WITH -20000
			INCREMENT BY -1
			MINVALUE -20000
			MAXVALUE 0
			CACHE 1;`,
			`CREATE SEQUENCE public.%s AS INT8 MINVALUE -20000 MAXVALUE 0 INCREMENT -1 START -20000`,
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
	defer log.Scope(t).Close(t)

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

	execKnobs.StatementFilter = func(ctx context.Context, _ *sessiondata.SessionData, stmt string, err error) {
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

	tc := serverutils.StartCluster(t, 2, /* numNodes */
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

func TestShowQueriesDelegatesInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	pgURL, cleanup := pgurlutils.PGUrl(
		t,
		s.AdvSQLAddr(),
		"TestShowQueriesDelegatesInternal",
		url.User(username.RootUser),
	)
	defer cleanup()

	q := pgURL.Query()
	q.Add("application_name", "app_name")
	pgURL.RawQuery = q.Encode()
	copyConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		// COPY TO uses the internal executor to run the source query.
		_, err := copyConn.Exec(ctx, "COPY (SELECT pg_sleep(1) FROM ROWS FROM (generate_series(1, 60)) AS i) TO STDOUT")
		return err
	})

	showConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)

	// SucceedsSoon is used since COPY is being executed concurrently.
	var appName string
	testutils.SucceedsSoon(t, func() error {
		// The COPY query should use the specified app name.
		err = showConn.QueryRow(ctx, "SELECT application_name FROM [SHOW QUERIES] WHERE query LIKE 'COPY (SELECT pg_sleep(1) %'").Scan(&appName)
		if err != nil {
			return err
		}
		if appName != "app_name" {
			return errors.New("expected COPY to appear in SHOW QUERIES")
		}

		// The internal query should use the delegated app name.
		err = showConn.QueryRow(ctx, "SELECT application_name FROM [SHOW QUERIES] WHERE query LIKE 'SELECT pg_sleep(1) %'").Scan(&appName)
		if err != nil {
			return err
		}
		if appName != catconstants.DelegatedAppNamePrefix+"app_name" {
			return errors.New("expected delegated query to appear in SHOW QUERIES")
		}

		return nil
	})

	err = copyConn.PgConn().CancelRequest(ctx)
	require.NoError(t, err)

	// An error is allowed here, since the query was canceled.
	_ = g.Wait()

	err = showConn.Close(ctx)
	require.NoError(t, err)
	err = copyConn.Close(ctx)
	require.NoError(t, err)

}

func TestShowQueriesFillsInValuesForPlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const applicationName = "application"
	var applicationConnection *gosql.DB
	var operatorConnection *gosql.DB

	recordedQueries := make(map[string]string)

	testServerArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				// Record the results of SHOW QUERIES for each statement run on the applicationConnection,
				// so that we can make assertions on them below.
				StatementFilter: func(ctx context.Context, session *sessiondata.SessionData, stmt string, err error) {
					// Only observe queries when we're in an application session,
					// to limit concurrent access to the recordedQueries map.
					if session.ApplicationName == applicationName {
						// Only select queries run by the test application itself,
						// so that we filter out the SELECT query FROM [SHOW QUERIES] statement.
						// (It's the "grep shows up in `ps | grep foo`" problem.)
						// And we can assume that there will be only one result row because we do not run
						// the below test cases in parallel.
						row := operatorConnection.QueryRow(
							"SELECT query FROM [SHOW QUERIES] WHERE application_name = $1", applicationName,
						)
						var query string
						err := row.Scan(&query)
						if err != nil {
							t.Fatal(err)
						}
						recordedQueries[stmt] = query
					}
				},
			},
		},
	}

	tc := serverutils.StartCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      testServerArgs,
		},
	)

	defer tc.Stopper().Stop(context.Background())

	applicationConnection = tc.ServerConn(0)
	operatorConnection = tc.ServerConn(1)

	// Mark all queries on this connection as coming from the application,
	// so we can identify them in our filter above.
	_, err := applicationConnection.Exec("SET application_name TO $1", applicationName)
	if err != nil {
		t.Fatal(err)
	}

	// For a given statement-with-placeholders and its arguments, how should it look in SHOW QUERIES?
	testCases := []struct {
		statement string
		args      []interface{}
		expected  string
	}{
		{
			"SELECT upper($1)",
			[]interface{}{"hello"},
			"SELECT upper('hello')",
		},
		{
			"SELECT /* test */ upper($1)",
			[]interface{}{"hello"},
			"SELECT upper('hello') /* test */",
		},
		{
			"SELECT /* test */ 'hi'::string",
			[]interface{}{},
			"SELECT 'hi'::STRING /* test */",
		},
		{
			"SELECT /* test */ 'hi'::string /* fnord */",
			[]interface{}{},
			"SELECT 'hi'::STRING /* test */ /* fnord */",
		},
	}

	// Perform both as a simple execution and as a prepared statement,
	// to make sure we're exercising both code paths.
	queryExecutionMethods := []struct {
		label string
		exec  func(*gosql.DB, string, ...interface{}) (gosql.Result, error)
	}{
		{
			"Exec",
			func(conn *gosql.DB, statement string, args ...interface{}) (gosql.Result, error) {
				return conn.Exec(statement, args...)
			},
		}, {
			"PrepareAndExec",
			func(conn *gosql.DB, statement string, args ...interface{}) (gosql.Result, error) {
				stmt, err := conn.Prepare(statement)
				if err != nil {
					return nil, err
				}
				defer stmt.Close()
				return stmt.Exec(args...)
			},
		},
	}

	for _, method := range queryExecutionMethods {
		for _, test := range testCases {
			t.Run(fmt.Sprintf("%v/%v", method.label, test.statement), func(t *testing.T) {
				_, err := method.exec(applicationConnection, test.statement, test.args...)

				if err != nil {
					t.Fatal(err)
				}

				// parse and stringify the statement so that it matches the key in the
				// recordedQueries map.
				stmt, err := parser.ParseOne(test.statement)
				if err != nil {
					t.Fatal(err)
				}
				sql := stmt.AST.String()
				require.Equal(t, test.expected, recordedQueries[sql])
			})
		}
	}
}

func TestShowSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var conn *gosql.DB

	tc := serverutils.StartCluster(t, 2 /* numNodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	conn = tc.ServerConn(0)
	sqlutils.CreateTable(t, conn, "t", "num INT", 0, nil)

	// We'll skip "internal" sessions, as those are unpredictable.
	var showSessions = fmt.Sprintf(`
	select node_id, (now() - session_start)::float from
		[show cluster sessions] where application_name not like '%s%%'
	`, catconstants.InternalAppNamePrefix)

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
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	params.Insecure = true
	s, rawSQLDBroot, _ := serverutils.StartServer(t, params)
	sqlDBroot := sqlutils.MakeSQLRunner(rawSQLDBroot)
	defer s.Stopper().Stop(context.Background())

	// Create four users: one with no special permissions, one with the
	// VIEWACTIVITY role option, one with VIEWACTIVITYREDACTED option,
	// and one admin. We'll check that the VIEWACTIVITY, VIEWACTIVITYREDACTED
	// users and the admin can see all sessions and the unpermissioned user can
	// only see their own session.
	_ = sqlDBroot.Exec(t, `CREATE USER noperms`)
	_ = sqlDBroot.Exec(t, `CREATE USER viewactivity VIEWACTIVITY`)
	_ = sqlDBroot.Exec(t, `CREATE USER viewactivityredacted VIEWACTIVITYREDACTED`)
	_ = sqlDBroot.Exec(t, `CREATE USER adminuser`)
	_ = sqlDBroot.Exec(t, `GRANT admin TO adminuser`)

	type user struct {
		username             string
		canViewOtherSessions bool
		sqlRunner            *sqlutils.SQLRunner
	}

	users := []user{
		{"noperms", false, nil},
		{"viewactivity", true, nil},
		{"viewactivityredacted", true, nil},
		{"adminuser", true, nil},
	}
	for i, tc := range users {
		pgURL := url.URL{
			Scheme:   "postgres",
			User:     url.User(tc.username),
			Host:     s.AdvSQLAddr(),
			RawQuery: "sslmode=disable",
		}
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		users[i].sqlRunner = sqlutils.MakeSQLRunner(db)

		// Ensure the session is open.
		users[i].sqlRunner.Exec(t, `SELECT version()`)
	}

	for _, u := range users {
		t.Run(u.username, func(t *testing.T) {
			rows := u.sqlRunner.Query(t, `SELECT user_name FROM [SHOW CLUSTER SESSIONS]`)
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
			for _, u2 := range users {
				if u.canViewOtherSessions || u.username == u2.username {
					if counts[u2.username] == 0 {
						t.Fatalf(
							"%s session is unable to see %s session: %+v", u.username, u2.username, counts)
					}
				} else if counts[u2.username] > 0 {
					t.Fatalf(
						"%s session should not be able to see %s session: %+v", u.username, u2.username, counts)
				}
			}
		})
	}
}

// TestShowRedactedActiveStatements tests the crdb_internal.cluster_queries
// table for system permissions.
func TestShowRedactedActiveStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	params.Insecure = true
	ctx, cancel := context.WithCancel(context.Background())
	s, rawSQLDBroot, _ := serverutils.StartServer(t, params)
	sqlDBroot := sqlutils.MakeSQLRunner(rawSQLDBroot)
	defer s.Stopper().Stop(context.Background())

	// Create four users: one with no special permissions, one with the
	// VIEWACTIVITY role option, one with VIEWACTIVITYREDACTED option,
	// and one with both permissions.
	_ = sqlDBroot.Exec(t, `CREATE USER noperms`)
	_ = sqlDBroot.Exec(t, `CREATE USER onlyviewactivity`)
	_ = sqlDBroot.Exec(t, `CREATE USER onlyviewactivityredacted`)
	_ = sqlDBroot.Exec(t, `CREATE USER bothperms`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITY TO onlyviewactivity`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITYREDACTED TO onlyviewactivityredacted`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITY TO bothperms`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITYREDACTED TO bothperms`)

	type user struct {
		username         string
		canViewOtherRows bool // Can the user view other users' rows in the table?
		isQueryRedacted  bool // Are the other userss queries redacted?
		sqlRunner        *sqlutils.SQLRunner
	}

	// A user with no permissions should only be able to see their own rows
	// in the table.
	// A user with only VIEWACTIVITY should be able to see the whole query.
	// A user with only VIEWACTIVITYREDACTED should see a redacted query.
	// A user with both should see the redacted query, as VIEWACTIVITYREDACTED
	// takes precedence.
	users := []user{
		{"onlyviewactivityredacted", true, true, nil},
		{"onlyviewactivity", true, false, nil},
		{"noperms", false, false, nil},
		{"bothperms", true, true, nil},
	}
	for i, tc := range users {
		pgURL := url.URL{
			Scheme:   "postgres",
			User:     url.User(tc.username),
			Host:     s.AdvSQLAddr(),
			RawQuery: "sslmode=disable",
		}
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		users[i].sqlRunner = sqlutils.MakeSQLRunner(db)

		// Ensure the session is open.
		users[i].sqlRunner.Exec(t, `SELECT version()`)
	}

	// Run a long-running sleep query in the background.
	startSignal := make(chan struct{})
	waiter := make(chan struct{})
	go func() {
		// Signal that we have started the query.
		close(startSignal)
		_, _ = rawSQLDBroot.ExecContext(ctx, `SELECT pg_sleep(30)`)
		// Signal that we have finished the query.
		close(waiter)
	}()

	// Wait for the start signal.
	<-startSignal

	selectRootQuery := `SELECT query FROM [SHOW CLUSTER QUERIES] WHERE query LIKE 'SELECT pg_sleep%' AND user_name = 'root'`

	testutils.SucceedsSoon(t, func() error {
		rows := sqlDBroot.Query(t, selectRootQuery)
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
			var query string
			if err := rows.Scan(&query); err != nil {
				return err
			}
			if query != "SELECT pg_sleep(30)" {
				return errors.Errorf("Expected `SELECT pg_sleep(30)`, got %s", query)
			}
		}
		if count != 1 {
			return errors.Errorf("expected 1 row, got %d", count)
		}
		return nil
	})

	for _, u := range users {
		t.Run(u.username, func(t *testing.T) {
			rootRows := u.sqlRunner.Query(t, selectRootQuery)
			defer func() { require.NoError(t, rootRows.Close()) }()

			count := 0
			for rootRows.Next() {
				count++

				var query string
				if err := rootRows.Scan(&query); err != nil {
					t.Fatal(err)
				}

				t.Log(query)
				// Make sure that if the user is supposed to see a redacted query, they do.
				if u.isQueryRedacted {
					if !strings.HasPrefix(query, "SELECT pg_sleep(_)") {
						t.Fatalf("Expected `SELECT pg_sleep(_)`, got %s", query)
					}
					// Make sure that if the user is supposed to see the full query, they do.
				} else {
					if !strings.HasPrefix(query, "SELECT pg_sleep(30)") {
						t.Fatalf("Expected `SELECT pg_sleep(30)`, got %s", query)
					}
				}
			}
			if u.canViewOtherRows {
				require.Equalf(t, 1, count, "expected 1 row, got %d", count)
			} else {
				require.Equalf(t, 0, count, "expected 0 rows, got %d", count)
			}

			selectOwnQuery := fmt.Sprintf(
				`SELECT query FROM [SHOW CLUSTER QUERIES] WHERE query LIKE 'SELECT query FROM%%' AND user_name = '%s'`,
				u.username,
			)
			ownRows := u.sqlRunner.Query(t, selectOwnQuery)
			defer func() { require.NoError(t, ownRows.Close()) }()

			count = 0
			for ownRows.Next() {
				count++

				var query string
				if err := ownRows.Scan(&query); err != nil {
					t.Fatal(err)
				}

				t.Log(query)
				// Any user can always see their own unredacted queries.
				if !strings.Contains(query, fmt.Sprintf("user_name = '%s'", u.username)) {
					t.Fatalf("Expected unredacted query, got %s", query)
				}
			}
			require.Equalf(t, 1, count, "expected 1 row, got %d", count)
		})
	}

	cancel()
	<-waiter
}

// TestShowRedactedSessions tests the crdb_internal.cluster_sessions
// table for system permissions.
func TestShowRedactedSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	params.Insecure = true
	ctx, cancel := context.WithCancel(context.Background())
	s, rawSQLDBroot, _ := serverutils.StartServer(t, params)
	sqlDBroot := sqlutils.MakeSQLRunner(rawSQLDBroot)
	defer s.Stopper().Stop(context.Background())

	// Create four users: one with no special permissions, one with the
	// VIEWACTIVITY role option, one with VIEWACTIVITYREDACTED option,
	// and one with both permissions.
	_ = sqlDBroot.Exec(t, `CREATE USER noperms`)
	_ = sqlDBroot.Exec(t, `CREATE USER onlyviewactivity`)
	_ = sqlDBroot.Exec(t, `CREATE USER onlyviewactivityredacted`)
	_ = sqlDBroot.Exec(t, `CREATE USER bothperms`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITY TO onlyviewactivity`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITYREDACTED TO onlyviewactivityredacted`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITY TO bothperms`)
	_ = sqlDBroot.Exec(t, `GRANT SYSTEM VIEWACTIVITYREDACTED TO bothperms`)

	type user struct {
		username         string
		canViewOtherRows bool // Can the user view other users' rows in the table?
		isQueryRedacted  bool // Are the other userss queries redacted?
		sqlRunner        *sqlutils.SQLRunner
	}

	// A user with no permissions should only be able to see their own rows
	// in the table.
	// A user with only VIEWACTIVITY should be able to see the whole query.
	// A user with only VIEWACTIVITYREDACTED should see a redacted query.
	// A user with both should see the redacted query, as VIEWACTIVITYREDACTED
	// takes precedence.
	users := []user{
		{"onlyviewactivityredacted", true, true, nil},
		{"onlyviewactivity", true, false, nil},
		{"noperms", false, false, nil},
		{"bothperms", true, true, nil},
	}
	for i, tc := range users {
		pgURL := url.URL{
			Scheme:   "postgres",
			User:     url.User(tc.username),
			Host:     s.AdvSQLAddr(),
			RawQuery: "sslmode=disable",
		}
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		users[i].sqlRunner = sqlutils.MakeSQLRunner(db)

		// Ensure the session is open.
		users[i].sqlRunner.Exec(t, `SELECT version()`)
	}

	// Run a long-running sleep query in the background.
	startSignal := make(chan struct{})
	waiter := make(chan struct{})
	go func() {
		// Signal that we have started the query.
		close(startSignal)
		_, _ = rawSQLDBroot.ExecContext(ctx, `SELECT pg_sleep(30)`)
		// Signal that we have finished the query.
		close(waiter)
	}()

	// Wait for the start signal.
	<-startSignal

	selectRootQuery := `SELECT active_queries FROM [SHOW CLUSTER SESSIONS] WHERE active_queries LIKE 'SELECT pg_sleep%' AND user_name = 'root'`

	testutils.SucceedsSoon(t, func() error {
		rows := sqlDBroot.Query(t, selectRootQuery)
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
			var query string
			if err := rows.Scan(&query); err != nil {
				return err
			}
			if query != "SELECT pg_sleep(30)" {
				return errors.Errorf("Expected `SELECT pg_sleep(30)`, got %s", query)
			}
		}
		if count != 1 {
			return errors.Errorf("expected 1 row, got %d", count)
		}
		return nil
	})

	for _, u := range users {
		t.Run(u.username, func(t *testing.T) {
			rootRows := u.sqlRunner.Query(t, selectRootQuery)
			defer func() { require.NoError(t, rootRows.Close()) }()

			count := 0
			for rootRows.Next() {
				count++

				var query string
				if err := rootRows.Scan(&query); err != nil {
					t.Fatal(err)
				}

				t.Log(query)
				// Make sure that if the user is supposed to see a redacted query, they do.
				if u.isQueryRedacted {
					if !strings.HasPrefix(query, "SELECT pg_sleep(_)") {
						t.Fatalf("Expected `SELECT pg_sleep(_)`, got %s", query)
					}
					// Make sure that if the user is supposed to see the full query, they do.
				} else {
					if !strings.HasPrefix(query, "SELECT pg_sleep(30)") {
						t.Fatalf("Expected `SELECT pg_sleep(30)`, got %s", query)
					}
				}
			}
			if u.canViewOtherRows {
				require.Equalf(t, 1, count, "expected 1 row, got %d", count)
			} else {
				require.Equalf(t, 0, count, "expected 0 rows, got %d", count)
			}

			selectOwnQuery := fmt.Sprintf(
				`SELECT active_queries FROM [SHOW CLUSTER SESSIONS] WHERE active_queries LIKE 'SELECT active_queries FROM%%' AND user_name = '%s'`,
				u.username,
			)
			ownRows := u.sqlRunner.Query(t, selectOwnQuery)
			defer func() { require.NoError(t, ownRows.Close()) }()

			count = 0
			for ownRows.Next() {
				count++

				var query string
				if err := ownRows.Scan(&query); err != nil {
					t.Fatal(err)
				}

				t.Log(query)
				// Any user can always see their own unredacted queries.
				if !strings.Contains(query, fmt.Sprintf("user_name = '%s'", u.username)) {
					t.Fatalf("Expected unredacted query, got %s", query)
				}
			}
			require.Equalf(t, 1, count, "expected 1 row, got %d", count)
		})
	}

	cancel()
	<-waiter
}

// TestCancelQueriesRace can be stressed to try and reproduce a race
// between SHOW QUERIES and currently executing statements. For
// more details, see #28033.
func TestCancelQueriesRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	waiter := make(chan struct{})
	go func() {
		_, _ = sqlDB.ExecContext(ctx, `SELECT pg_sleep(10)`)
		close(waiter)
	}()
	_, err1 := sqlDB.ExecContext(ctx, `CANCEL QUERIES (
		SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'SELECT pg_sleep%'
	)`)

	_, err2 := sqlDB.ExecContext(ctx, `CANCEL QUERIES (
		SELECT query_id FROM [SHOW QUERIES] WHERE query LIKE 'SELECT pg_sleep%'
	)`)
	// At least one query cancellation is expected to succeed.
	require.Truef(
		t,
		err1 == nil || err2 == nil,
		"Both query cancellations failed with errors: %v and %v", err1, err2)

	cancel()
	<-waiter
}
