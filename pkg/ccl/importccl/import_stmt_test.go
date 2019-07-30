// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func TestImportData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	tests := []struct {
		name   string
		create string
		with   string
		typ    string
		data   string
		err    string
		query  map[string][][]string
	}{
		{
			name: "duplicate unique index key",
			create: `
				a int8 primary key,
				i int8,
				unique index idx_f (i)
			`,
			typ: "CSV",
			data: `1,1
2,2
3,3
4,3
5,4`,
			err: "duplicate key",
		},
		{
			name: "duplicate PK",
			create: `
				i int8 primary key
			`,
			typ: "CSV",
			data: `1
2
3
3
4`,
			err: "duplicate key",
		},
		{
			name: "duplicate collated string key",
			create: `
				s string collate en_u_ks_level1 primary key
			`,
			typ: "CSV",
			data: `a
B
c
D
d
`,
			err: "duplicate key",
		},
		{
			name: "duplicate PK at sst boundary",
			create: `
				i int8 primary key,
				s string
			`,
			with: `WITH sstsize = '10B'`,
			typ:  "CSV",
			data: `1,0000000000
1,0000000000`,
			err: "duplicate key",
		},
		{
			name: "verify no splits mid row",
			create: `
				i int8 primary key,
				s string,
				b int8,
				c int8,
				index (s),
				index (i, s),
				family (i, b),
				family (s, c)
			`,
			with: `WITH sstsize = '1B'`,
			typ:  "CSV",
			data: `5,STRING,7,9`,
			query: map[string][][]string{
				`SELECT count(*) from t`: {{"1"}},
			},
		},
		{
			name:   "good bytes encoding",
			create: `b bytes`,
			typ:    "CSV",
			data: `\x0143
0143`,
			query: map[string][][]string{
				`SELECT * from t`: {{"\x01C"}, {"0143"}},
			},
		},
		{
			name:   "invalid byte",
			create: `b bytes`,
			typ:    "CSV",
			data:   `\x0g`,
			err:    "invalid byte",
		},
		{
			name:   "bad bytes length",
			create: `b bytes`,
			typ:    "CSV",
			data:   `\x0`,
			err:    "odd length hex string",
		},
		{
			name:   "oversample",
			create: `i int8`,
			with:   `WITH oversample = '100'`,
			typ:    "CSV",
			data:   "1",
		},
		{
			name:   "new line characters",
			create: `t text`,
			typ:    "CSV",
			data:   "\"hello\r\nworld\"\n\"friend\nfoe\"\n\"mr\rmrs\"",
			query: map[string][][]string{
				`SELECT t from t`: {{"hello\r\nworld"}, {"friend\nfoe"}, {"mr\rmrs"}},
			},
		},
		{
			name:   "CR in int8, 2 cols",
			create: `a int8, b int8`,
			typ:    "CSV",
			data:   "1,2\r\n3,4\n5,6",
			query: map[string][][]string{
				`SELECT * FROM t ORDER BY a`: {{"1", "2"}, {"3", "4"}, {"5", "6"}},
			},
		},
		{
			name:   "CR in int8, 1 col",
			create: `a int8`,
			typ:    "CSV",
			data:   "1\r\n3\n5",
			query: map[string][][]string{
				`SELECT * FROM t ORDER BY a`: {{"1"}, {"3"}, {"5"}},
			},
		},
		{
			name:   "collated strings",
			create: `s string collate en_u_ks_level1`,
			typ:    "CSV",
			data:   strings.Repeat("1\n", 2000),
			query: map[string][][]string{
				`SELECT s, count(*) FROM t GROUP BY s`: {{"1", "2000"}},
			},
		},

		// MySQL OUTFILE
		{
			name:   "unexpected number of columns",
			create: `i int8`,
			typ:    "MYSQLOUTFILE",
			data:   "1\t2",
			err:    "row 1: too many columns, expected 1",
		},
		{
			name:   "unmatched field enclosure",
			create: `i int8`,
			with:   `WITH fields_enclosed_by = '"'`,
			typ:    "MYSQLOUTFILE",
			data:   "\"1",
			err:    "row 1: unmatched field enclosure",
		},
		{
			name:   "unmatched literal",
			create: `i int8`,
			with:   `WITH fields_escaped_by = '\'`,
			typ:    "MYSQLOUTFILE",
			data:   `\`,
			err:    "row 1: unmatched literal",
		},
		{
			name:   "weird escape char",
			create: `s STRING`,
			with:   `WITH fields_escaped_by = '@'`,
			typ:    "MYSQLOUTFILE",
			data:   "@N\nN@@\nNULL",
			query: map[string][][]string{
				`SELECT COALESCE(s, '(null)') from t`: {{"(null)"}, {"N@"}, {"NULL"}},
			},
		},
		{
			name:   `null and \N with escape`,
			create: `s STRING`,
			with:   `WITH fields_escaped_by = '\'`,
			typ:    "MYSQLOUTFILE",
			data:   "\\N\n\\\\N\nNULL",
			query: map[string][][]string{
				`SELECT COALESCE(s, '(null)') from t`: {{"(null)"}, {`\N`}, {"NULL"}},
			},
		},
		{
			name:   `\N with trailing char`,
			create: `s STRING`,
			with:   `WITH fields_escaped_by = '\'`,
			typ:    "MYSQLOUTFILE",
			data:   "\\N1",
			err:    "row 1: unexpected data after null encoding",
		},
		{
			name:   `double null`,
			create: `s STRING`,
			with:   `WITH fields_escaped_by = '\'`,
			typ:    "MYSQLOUTFILE",
			data:   "\\N\\N",
			err:    "row 1: unexpected null encoding",
		},
		{
			name:   `null and \N without escape`,
			create: `s STRING`,
			typ:    "MYSQLOUTFILE",
			data:   "\\N\n\\\\N\nNULL",
			query: map[string][][]string{
				`SELECT COALESCE(s, '(null)') from t`: {{`\N`}, {`\\N`}, {"(null)"}},
			},
		},
		{
			name:   `bytes with escape`,
			create: `b BYTES`,
			typ:    "MYSQLOUTFILE",
			data:   `\x`,
			query: map[string][][]string{
				`SELECT * from t`: {{`\x`}},
			},
		},

		// PG COPY
		{
			name:   "unexpected escape x",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\x`,
			err:    `row 1: unsupported escape sequence: \\x`,
		},
		{
			name:   "unexpected escape 3",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\3`,
			err:    `row 1: unsupported escape sequence: \\3`,
		},
		{
			name:   "escapes",
			create: `b bytes`,
			typ:    "PGCOPY",
			data:   `\x43\122`,
			query: map[string][][]string{
				`SELECT * from t`: {{"CR"}},
			},
		},
		{
			name:   "normal",
			create: `i int8, s string`,
			typ:    "PGCOPY",
			data:   "1\tSTR\n2\t\\N\n\\N\t\\t",
			query: map[string][][]string{
				`SELECT * from t`: {{"1", "STR"}, {"2", "NULL"}, {"NULL", "\t"}},
			},
		},
		{
			name:   "comma delim",
			create: `i int8, s string`,
			typ:    "PGCOPY",
			with:   `WITH delimiter = ','`,
			data:   "1,STR\n2,\\N\n\\N,\\,",
			query: map[string][][]string{
				`SELECT * from t`: {{"1", "STR"}, {"2", "NULL"}, {"NULL", ","}},
			},
		},
		{
			name:   "size out of range",
			create: `i int8`,
			typ:    "PGCOPY",
			with:   `WITH max_row_size = '10GB'`,
			err:    "max_row_size out of range",
		},
		{
			name:   "line too long",
			create: `i int8`,
			typ:    "PGCOPY",
			data:   "123456",
			with:   `WITH max_row_size = '5B'`,
			err:    "line too long",
		},
		{
			name:   "not enough values",
			typ:    "PGCOPY",
			create: "a INT8, b INT8",
			data:   `1`,
			err:    "expected 2 values, got 1",
		},
		{
			name:   "too many values",
			typ:    "PGCOPY",
			create: "a INT8, b INT8",
			data:   "1\t2\t3",
			err:    "expected 2 values, got 3",
		},

		// Postgres DUMP
		{
			name: "mismatch cols",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (i int8);
				COPY t (s) FROM stdin;
				0
				\.
			`,
			err: `COPY columns do not match table columns for table t`,
		},
		{
			name: "missing COPY done",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (i int8);
				COPY t (i) FROM stdin;
0
`,
			err: `unexpected EOF`,
		},
		{
			name: "semicolons and comments",
			typ:  "PGDUMP",
			data: `
				CREATE TABLE t (i int8);
				;;;
				-- nothing ;
				;
				-- blah
			`,
			query: map[string][][]string{
				`SELECT * from t`: {},
			},
		},
		{
			name: "size out of range",
			typ:  "PGDUMP",
			with: `WITH max_row_size = '10GB'`,
			err:  "max_row_size out of range",
		},
		{
			name: "line too long",
			typ:  "PGDUMP",
			data: "CREATE TABLE t (i INT8);",
			with: `WITH max_row_size = '5B'`,
			err:  "line too long",
		},
		{
			name: "not enough values",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);

COPY t (a, b) FROM stdin;
1
\.
			`,
			err: "expected 2 values, got 1",
		},
		{
			name: "too many values",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);

COPY t (a, b) FROM stdin;
1	2	3
\.
			`,
			err: "expected 2 values, got 3",
		},
		{
			name: "too many cols",
			typ:  "PGDUMP",
			data: `
CREATE TABLE t (a INT8, b INT8);

COPY t (a, b, c) FROM stdin;
1	2	3
\.
			`,
			err: "expected 2 columns, got 3",
		},
		{
			name: "fk",
			typ:  "PGDUMP",
			data: testPgdumpFk,
			query: map[string][][]string{
				`SHOW TABLES`:              {{"cities"}, {"weather"}},
				`SELECT city FROM cities`:  {{"Berkeley"}},
				`SELECT city FROM weather`: {{"Berkeley"}},

				`SELECT dependson_name
				FROM crdb_internal.backward_dependencies
				`: {{"weather_city_fkey"}},

				`SELECT create_statement
				FROM crdb_internal.create_statements
				WHERE descriptor_name in ('cities', 'weather')
				ORDER BY descriptor_name
				`: {{testPgdumpCreateCities}, {testPgdumpCreateWeather}},

				// Verify the constraint is unvalidated.
				`SHOW CONSTRAINTS FROM weather
				`: {{"weather", "weather_city_fkey", "FOREIGN KEY", "FOREIGN KEY (city) REFERENCES cities(city)", "false"}},
			},
		},
		{
			name: "fk-circular",
			typ:  "PGDUMP",
			data: testPgdumpFkCircular,
			query: map[string][][]string{
				`SHOW TABLES`:        {{"a"}, {"b"}},
				`SELECT i, k FROM a`: {{"2", "2"}},
				`SELECT j FROM b`:    {{"2"}},

				`SELECT dependson_name
				FROM crdb_internal.backward_dependencies ORDER BY dependson_name`: {
					{"a_i_fkey"},
					{"a_k_fkey"},
					{"b_j_fkey"},
				},

				`SELECT create_statement
				FROM crdb_internal.create_statements
				WHERE descriptor_name in ('a', 'b')
				ORDER BY descriptor_name
				`: {{
					`CREATE TABLE a (
	i INT8 NOT NULL,
	k INT8 NULL,
	CONSTRAINT a_pkey PRIMARY KEY (i ASC),
	CONSTRAINT a_k_fkey FOREIGN KEY (k) REFERENCES a(i),
	INDEX a_auto_index_a_k_fkey (k ASC),
	CONSTRAINT a_i_fkey FOREIGN KEY (i) REFERENCES b(j),
	FAMILY "primary" (i, k)
)`}, {
					`CREATE TABLE b (
	j INT8 NOT NULL,
	CONSTRAINT b_pkey PRIMARY KEY (j ASC),
	CONSTRAINT b_j_fkey FOREIGN KEY (j) REFERENCES a(i),
	FAMILY "primary" (j)
)`,
				}},

				`SHOW CONSTRAINTS FROM a`: {
					{"a", "a_i_fkey", "FOREIGN KEY", "FOREIGN KEY (i) REFERENCES b(j)", "false"},
					{"a", "a_k_fkey", "FOREIGN KEY", "FOREIGN KEY (k) REFERENCES a(i)", "false"},
					{"a", "a_pkey", "PRIMARY KEY", "PRIMARY KEY (i ASC)", "true"},
				},
				`SHOW CONSTRAINTS FROM b`: {
					{"b", "b_j_fkey", "FOREIGN KEY", "FOREIGN KEY (j) REFERENCES a(i)", "false"},
					{"b", "b_pkey", "PRIMARY KEY", "PRIMARY KEY (j ASC)", "true"},
				},
			},
		},
		{
			name: "fk-skip",
			typ:  "PGDUMP",
			data: testPgdumpFk,
			with: `WITH skip_foreign_keys`,
			query: map[string][][]string{
				`SHOW TABLES`: {{"cities"}, {"weather"}},
				// Verify the constraint is skipped.
				`SELECT dependson_name FROM crdb_internal.backward_dependencies`: {},
				`SHOW CONSTRAINTS FROM weather`:                                  {},
			},
		},
		{
			name: "fk unreferenced",
			typ:  "TABLE weather FROM PGDUMP",
			data: testPgdumpFk,
			err:  `table "cities" not found`,
		},
		{
			name: "fk unreferenced skipped",
			typ:  "TABLE weather FROM PGDUMP",
			data: testPgdumpFk,
			with: `WITH skip_foreign_keys`,
			query: map[string][][]string{
				`SHOW TABLES`: {{"weather"}},
			},
		},
		{
			name: "sequence",
			typ:  "PGDUMP",
			data: `
					CREATE TABLE t (a INT8);
					CREATE SEQUENCE public.i_seq
						START WITH 1
						INCREMENT BY 1
						NO MINVALUE
						NO MAXVALUE
						CACHE 1;
					ALTER SEQUENCE public.i_seq OWNED BY public.i.id;
					ALTER TABLE ONLY t ALTER COLUMN a SET DEFAULT nextval('public.i_seq'::regclass);
					SELECT pg_catalog.setval('public.i_seq', 10, true);
				`,
			query: map[string][][]string{
				`SELECT nextval('i_seq')`:    {{"11"}},
				`SHOW CREATE SEQUENCE i_seq`: {{"i_seq", "CREATE SEQUENCE i_seq MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1"}},
			},
		},
		{
			name: "non-public schema",
			typ:  "PGDUMP",
			data: "create table s.t (i INT8)",
			err:  `non-public schemas unsupported: s`,
		},
		{
			name: "unsupported type",
			typ:  "PGDUMP",
			data: "create table t (t time with time zone)",
			err: `create table t \(t time with time zone\)
                                 \^`,
		},
		{
			name: "various create ignores",
			typ:  "PGDUMP",
			data: `
				CREATE TRIGGER conditions_set_updated_at BEFORE UPDATE ON conditions FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
				REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM PUBLIC;
				REVOKE ALL ON SEQUENCE knex_migrations_id_seq FROM database;
				GRANT ALL ON SEQUENCE knex_migrations_id_seq TO database;
				GRANT SELECT ON SEQUENCE knex_migrations_id_seq TO opentrials_readonly;

				CREATE FUNCTION public.isnumeric(text) RETURNS boolean
				    LANGUAGE sql
				    AS $_$
				SELECT $1 ~ '^[0-9]+$'
				$_$;
				ALTER FUNCTION public.isnumeric(text) OWNER TO roland;

				CREATE TABLE t (i INT8);
			`,
			query: map[string][][]string{
				`SHOW TABLES`: {{"t"}},
			},
		},
		{
			name: "many tables",
			typ:  "PGDUMP",
			data: func() string {
				var sb strings.Builder
				for i := 1; i <= 100; i++ {
					fmt.Fprintf(&sb, "CREATE TABLE t%d ();\n", i)
				}
				return sb.String()
			}(),
		},

		// Error
		{
			name:   "unsupported import format",
			create: `b bytes`,
			typ:    "NOPE",
			err:    `unsupported import format`,
		},
		{
			name:   "sequences",
			create: `i int8 default nextval('s')`,
			typ:    "CSV",
			err:    `"s" not found`,
		},
	}

	var dataString string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, dataString)
		}
	}))
	defer srv.Close()

	// Create and drop a table to make sure a descriptor ID gets used to verify
	// ID rewrites happen correctly. Useful when running just a single test.
	sqlDB.Exec(t, `CREATE TABLE blah (i int8)`)
	sqlDB.Exec(t, `DROP TABLE blah`)

	for _, direct := range []bool{false, true} {
		// this test is big and slow as is, so we can't afford to double it in race.
		if util.RaceEnabled && direct {
			continue
		}

		for i, tc := range tests {
			if direct {
				if tc.with == "" {
					tc.with = "WITH experimental_direct_ingestion"
				} else {
					tc.with += ", experimental_direct_ingestion"
				}
			}
			t.Run(fmt.Sprintf("%s: %s direct=%v", tc.typ, tc.name, direct), func(t *testing.T) {
				dbName := fmt.Sprintf("d%d", i)
				sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s; USE %[1]s`, dbName))
				defer sqlDB.Exec(t, fmt.Sprintf(`DROP DATABASE %s`, dbName))
				var q string
				if tc.create != "" {
					q = fmt.Sprintf(`IMPORT TABLE t (%s) %s DATA ($1) %s`, tc.create, tc.typ, tc.with)
				} else {
					q = fmt.Sprintf(`IMPORT %s ($1) %s`, tc.typ, tc.with)
				}
				t.Log(q)
				dataString = tc.data
				sqlDB.ExpectErr(t, tc.err, q, srv.URL)
				for query, res := range tc.query {
					sqlDB.CheckQueryResults(t, query, res)
				}
			})
		}
	}

	t.Run("mysqlout multiple", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE mysqlout; USE mysqlout`)
		dataString = "1"
		sqlDB.Exec(t, `IMPORT TABLE t (s STRING) MYSQLOUTFILE DATA ($1, $1)`, srv.URL)
		sqlDB.CheckQueryResults(t, `SELECT * FROM t`, [][]string{{"1"}, {"1"}})
	})
}

const (
	testPgdumpCreateCities = `CREATE TABLE cities (
	city VARCHAR(80) NOT NULL,
	CONSTRAINT cities_pkey PRIMARY KEY (city ASC),
	FAMILY "primary" (city)
)`
	testPgdumpCreateWeather = `CREATE TABLE weather (
	city VARCHAR(80) NULL,
	temp_lo INT8 NULL,
	temp_hi INT8 NULL,
	prcp FLOAT4 NULL,
	date DATE NULL,
	CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES cities(city),
	INDEX weather_auto_index_weather_city_fkey (city ASC),
	FAMILY "primary" (city, temp_lo, temp_hi, prcp, date, rowid)
)`
	testPgdumpFk = `
CREATE TABLE public.cities (
    city character varying(80) NOT NULL
);

ALTER TABLE public.cities OWNER TO postgres;

CREATE TABLE public.weather (
    city character varying(80),
    temp_lo int8,
    temp_hi int8,
    prcp real,
    date date
);

ALTER TABLE public.weather OWNER TO postgres;

COPY public.cities (city) FROM stdin;
Berkeley
\.

COPY public.weather (city, temp_lo, temp_hi, prcp, date) FROM stdin;
Berkeley	45	53	0	1994-11-28
\.

ALTER TABLE ONLY public.cities
    ADD CONSTRAINT cities_pkey PRIMARY KEY (city);

ALTER TABLE ONLY public.weather
    ADD CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES public.cities(city);
`

	testPgdumpFkCircular = `
CREATE TABLE public.a (
    i int8 NOT NULL,
    k int8
);

CREATE TABLE public.b (
    j int8 NOT NULL
);

COPY public.a (i, k) FROM stdin;
2	2
\.

COPY public.b (j) FROM stdin;
2
\.

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_pkey PRIMARY KEY (i);

ALTER TABLE ONLY public.b
    ADD CONSTRAINT b_pkey PRIMARY KEY (j);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_i_fkey FOREIGN KEY (i) REFERENCES public.b(j);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_k_fkey FOREIGN KEY (k) REFERENCES public.a(i);

ALTER TABLE ONLY public.b
    ADD CONSTRAINT b_j_fkey FOREIGN KEY (j) REFERENCES public.a(i);
`
)

func TestImportCSVStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("short")
	}

	const nodes = 3

	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	ctx := context.Background()
	baseDir := filepath.Join("testdata", "csv")
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)

	testFiles := makeCSVData(t, numFiles, rowsPerFile, nodes, rowsPerRaceFile)
	if util.RaceEnabled {
		// This test takes a while with the race detector, so reduce the number of
		// files and rows per file in an attempt to speed it up.
		numFiles = nodes
		rowsPerFile = rowsPerRaceFile
	}

	// Table schema used in IMPORT TABLE tests.
	tablePath := filepath.Join(baseDir, "table")
	if err := ioutil.WriteFile(tablePath, []byte(`
		CREATE TABLE t (
			a int8 primary key,
			b string,
			index (b),
			index (a, b)
		)
	`), 0666); err != nil {
		t.Fatal(err)
	}
	schema := []interface{}{"nodelocal:///table"}

	if err := ioutil.WriteFile(filepath.Join(baseDir, "empty.csv"), nil, 0666); err != nil {
		t.Fatal(err)
	}
	empty := []string{"'nodelocal:///empty.csv'"}

	// Support subtests by keeping track of the number of jobs that are executed.
	testNum := -1
	expectedRows := numFiles * rowsPerFile
	for i, tc := range []struct {
		name    string
		query   string        // must have one `%s` for the files list.
		args    []interface{} // will have backupPath appended
		files   []string
		jobOpts string
		err     string
	}{
		{
			"schema-in-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.files,
			``,
			"",
		},
		{
			"schema-in-file-intodb",
			`IMPORT TABLE csv1.t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.files,
			``,
			"",
		},
		{
			"schema-in-query",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s)`,
			nil,
			testFiles.files,
			``,
			"",
		},
		{
			"schema-in-query-opts",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2'`,
			nil,
			testFiles.filesWithOpts,
			` WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2'`,
			"",
		},
		{
			// Force some SST splits.
			"schema-in-file-sstsize",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH sstsize = '10K'`,
			schema,
			testFiles.files,
			` WITH sstsize = '10K'`,
			"",
		},
		{
			"empty-file",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			empty,
			``,
			"",
		},
		{
			"empty-with-files",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			append(empty, testFiles.files...),
			``,
			"",
		},
		{
			"schema-in-file-auto-decompress",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'auto'`,
			schema,
			testFiles.files,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"schema-in-file-no-decompress",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'none'`,
			schema,
			testFiles.files,
			` WITH decompress = 'none'`,
			"",
		},
		{
			"schema-in-file-explicit-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'gzip'`,
			schema,
			testFiles.gzipFiles,
			` WITH decompress = 'gzip'`,
			"",
		},
		{
			"schema-in-file-auto-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'auto'`,
			schema,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"schema-in-file-implicit-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.gzipFiles,
			``,
			"",
		},
		{
			"schema-in-file-explicit-bzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'bzip'`,
			schema,
			testFiles.bzipFiles,
			` WITH decompress = 'bzip'`,
			"",
		},
		{
			"schema-in-file-auto-bzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'auto'`,
			schema,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"schema-in-file-implicit-bzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.bzipFiles,
			``,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"bad-opt-name",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH foo = 'bar'`,
			nil,
			testFiles.files,
			``,
			"invalid option \"foo\"",
		},
		{
			"bad-computed-column",
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING AS ('hello') STORED, INDEX (b), INDEX (a, b)) CSV DATA (%s) WITH skip = '2'`,
			nil,
			testFiles.filesWithOpts,
			``,
			"computed columns not supported",
		},
		{
			"primary-key-dup",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.filesWithDups,
			``,
			"primary or unique index has duplicate keys",
		},
		{
			"no-database",
			`IMPORT TABLE nonexistent.t CREATE USING $1 CSV DATA (%s)`,
			schema,
			testFiles.files,
			``,
			`database does not exist: "nonexistent.t"`,
		},
		{
			"into-db-fails",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH into_db = 'test'`,
			schema,
			testFiles.files,
			``,
			`invalid option "into_db"`,
		},
		{
			"schema-in-file-no-decompress-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'none'`,
			schema,
			testFiles.gzipFiles,
			` WITH decompress = 'none'`,
			"expected 2 fields, got",
		},
		{
			"schema-in-file-no-decompress-gzip",
			`IMPORT TABLE t CREATE USING $1 CSV DATA (%s) WITH decompress = 'gzip'`,
			schema,
			testFiles.files,
			` WITH decompress = 'gzip'`,
			"gzip: invalid header",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.name, "bzip") && len(testFiles.bzipFiles) == 0 {
				t.Skip("bzip2 not available on PATH?")
			}
			intodb := fmt.Sprintf(`csv%d`, i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, intodb))
			sqlDB.Exec(t, fmt.Sprintf(`SET DATABASE = %s`, intodb))

			var unused string
			var restored struct {
				rows, idx, sys, bytes int
			}

			var result int
			query := fmt.Sprintf(tc.query, strings.Join(tc.files, ", "))
			testNum++
			if tc.err != "" {
				sqlDB.ExpectErr(t, tc.err, query, tc.args...)
				return
			}
			sqlDB.QueryRow(t, query, tc.args...).Scan(
				&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
			)

			jobPrefix := fmt.Sprintf(`IMPORT TABLE %s.public.t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))`, intodb)

			if err := jobutils.VerifySystemJob(t, sqlDB, testNum, jobspb.TypeImport, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(jobPrefix+` CSV DATA (%s)`+tc.jobOpts, strings.ReplaceAll(strings.Join(tc.files, ", "), "?param=value", "")),
			}); err != nil {
				t.Fatal(err)
			}

			isEmpty := len(tc.files) == 1 && tc.files[0] == empty[0]

			if isEmpty {
				sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
				if expect := 0; result != expect {
					t.Fatalf("expected %d rows, got %d", expect, result)
				}
				return
			}

			if expected, actual := expectedRows, restored.rows; expected != actual {
				t.Fatalf("expected %d rows, got %d", expected, actual)
			}

			// Verify correct number of rows via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
			if expect := expectedRows; result != expect {
				t.Fatalf("expected %d rows, got %d", expect, result)
			}

			// Verify correct number of NULLs via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
			expectedNulls := 0
			if strings.Contains(tc.query, "nullif") {
				expectedNulls = expectedRows / 4
			}
			if result != expectedNulls {
				t.Fatalf("expected %d rows, got %d", expectedNulls, result)
			}

			// Verify sstsize created > 1 SST files.
			if tc.name == "schema-in-file-sstsize-dist" {
				pattern := filepath.Join(baseDir, fmt.Sprintf("%d", i), "*.sst")
				matches, err := filepath.Glob(pattern)
				if err != nil {
					t.Fatal(err)
				}
				if len(matches) < 2 {
					t.Fatal("expected > 1 SST files")
				}
			}

			// Verify spans don't have trailing '/0'.
			ranges := sqlDB.QueryStr(t, `SHOW testing_ranges FROM TABLE t`)
			for _, r := range ranges {
				const end = `/0`
				if strings.HasSuffix(r[0], end) || strings.HasSuffix(r[1], end) {
					t.Errorf("bad span: %s - %s", r[0], r[1])
				}
			}
		})
	}

	// Verify unique_rowid is replaced for tables without primary keys.
	t.Run("unique_rowid", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE pk")
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE pk.t (a INT8, b STRING) CSV DATA (%s)`, strings.Join(testFiles.files, ", ")))
		// Verify the rowids are being generated as expected.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) FROM pk.t`,
			sqlDB.QueryStr(t, `
				SELECT count(*) FROM
					(SELECT * FROM
						(SELECT generate_series(0, $1 - 1) file),
						(SELECT generate_series(1, $2) rownum)
					)
			`, numFiles, rowsPerFile),
		)
	})

	// Verify a failed IMPORT won't prevent a second IMPORT.
	t.Run("checkpoint-leftover", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE checkpoint; USE checkpoint")

		// Specify wrong number of columns.
		sqlDB.ExpectErr(
			t, "expected 1 fields, got 2",
			fmt.Sprintf(`IMPORT TABLE t (a INT8 PRIMARY KEY) CSV DATA (%s)`, testFiles.files[0]),
		)

		// Specify wrong table name; still shouldn't leave behind a checkpoint file.
		sqlDB.ExpectErr(
			t, `file specifies a schema for table t`,
			fmt.Sprintf(`IMPORT TABLE bad CREATE USING $1 CSV DATA (%s)`, testFiles.files[0]), schema[0],
		)

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING) CSV DATA (%s)`, testFiles.files[0]))

		// A second attempt should fail fast. A "slow fail" is the error message
		// "restoring table desc and namespace entries: table already exists".
		sqlDB.ExpectErr(
			t, `relation "t" already exists`,
			fmt.Sprintf(`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING) CSV DATA (%s)`, testFiles.files[0]),
		)
	})

	// Verify DEFAULT columns and SERIAL are allowed but not evaluated.
	t.Run("allow-default", func(t *testing.T) {
		var data string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, `CREATE DATABASE d`)
		sqlDB.Exec(t, `SET DATABASE = d`)

		const (
			query = `IMPORT TABLE t (
			a SERIAL8,
			b INT8 DEFAULT unique_rowid(),
			c STRING DEFAULT 's',
			d SERIAL8,
			e INT8 DEFAULT unique_rowid(),
			f STRING DEFAULT 's',
			PRIMARY KEY (a, b, c)
		) CSV DATA ($1)`
			nullif = ` WITH nullif=''`
		)

		data = ",5,e,7,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `row 1: parse "a" as INT8: could not parse ""`,
				query, srv.URL,
			)
			sqlDB.ExpectErr(
				t, `row 1: generate insert row: null value in column "a" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})
		data = "2,5,e,,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `row 1: generate insert row: null value in column "d" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})
		data = "2,,e,,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `"b" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})

		data = "2,5,,,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.ExpectErr(
				t, `"c" violates not-null constraint`,
				query+nullif, srv.URL,
			)
		})

		data = "2,5,e,-1,,"
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, query+nullif, srv.URL)
			sqlDB.CheckQueryResults(t,
				`SELECT * FROM t`,
				sqlDB.QueryStr(t, `SELECT 2, 5, 'e', -1, NULL, NULL`),
			)
		})
	})
}

// TODO(adityamaru): Tests still need to be added incrementally as
// relevant IMPORT INTO logic is added. Some of them include:
// -> FK and constraint violation
// -> CSV containing keys which will shadow existing data
// -> Rollback of a failed IMPORT INTO
func TestImportIntoCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short")
	}

	const nodes = 3

	numFiles := nodes + 2
	rowsPerFile := 1000
	rowsPerRaceFile := 16

	ctx := context.Background()
	baseDir := filepath.Join("testdata", "csv")
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)

	testFiles := makeCSVData(t, numFiles, rowsPerFile, nodes, rowsPerRaceFile)
	if util.RaceEnabled {
		// This test takes a while with the race detector, so reduce the number of
		// files and rows per file in an attempt to speed it up.
		numFiles = nodes
		rowsPerFile = rowsPerRaceFile
	}

	if err := ioutil.WriteFile(filepath.Join(baseDir, "empty.csv"), nil, 0666); err != nil {
		t.Fatal(err)
	}
	empty := []string{"'nodelocal:///empty.csv'"}

	// Support subtests by keeping track of the number of jobs that are executed.
	testNum := -1
	insertedRows := numFiles * rowsPerFile

	for i, tc := range []struct {
		name    string
		query   string // must have one `%s` for the files list.
		files   []string
		jobOpts string
		err     string
	}{
		{
			"simple-import-into",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			"",
		},
		{
			"import-into-intodb",
			`IMPORT INTO csv1.t (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			"",
		},
		{
			"import-into-with-opts",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH delimiter = '|', comment = '#', nullif='', skip = '2'`,
			testFiles.filesWithOpts,
			` WITH comment = '#', delimiter = '|', "nullif" = '', skip = '2'`,
			"",
		},
		{
			// Force some SST splits.
			"import-into-sstsize",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH sstsize = '10K'`,
			testFiles.files,
			` WITH sstsize = '10K'`,
			"",
		},
		{
			"empty-file",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			empty,
			``,
			"",
		},
		{
			"empty-with-files",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			append(empty, testFiles.files...),
			``,
			"",
		},
		{
			"import-into-auto-decompress",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.files,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"import-into-no-decompress",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.files,
			` WITH decompress = 'none'`,
			"",
		},
		{
			"import-into-explicit-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.gzipFiles,
			` WITH decompress = 'gzip'`,
			"",
		},
		{
			"import-into-auto-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.gzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"import-into-implicit-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			testFiles.gzipFiles,
			``,
			"",
		},
		{
			"import-into-explicit-bzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'bzip'`,
			testFiles.bzipFiles,
			` WITH decompress = 'bzip'`,
			"",
		},
		{
			"import-into-auto-bzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'auto'`,
			testFiles.bzipFiles,
			` WITH decompress = 'auto'`,
			"",
		},
		{
			"import-into-implicit-bzip",
			`IMPORT INTO t (a, b) CSV DATA (%s)`,
			testFiles.bzipFiles,
			``,
			"",
		},
		// NB: successes above, failures below, because we check the i-th job.
		{
			"import-into-bad-opt-name",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH foo = 'bar'`,
			testFiles.files,
			``,
			"invalid option \"foo\"",
		},
		{
			"import-into-no-database",
			`IMPORT INTO nonexistent.t (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			`database does not exist: "nonexistent.t"`,
		},
		{
			"import-into-no-table",
			`IMPORT INTO g (a, b) CSV DATA (%s)`,
			testFiles.files,
			``,
			`pq: relation "g" does not exist`,
		},
		{
			"import-into-no-decompress-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'none'`,
			testFiles.gzipFiles,
			` WITH decompress = 'none'`,
			"expected 2 fields, got",
		},
		{
			"import-into-no-decompress-gzip",
			`IMPORT INTO t (a, b) CSV DATA (%s) WITH decompress = 'gzip'`,
			testFiles.files,
			` WITH decompress = 'gzip'`,
			"gzip: invalid header",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(tc.name, "bzip") && len(testFiles.bzipFiles) == 0 {
				t.Skip("bzip2 not available on PATH?")
			}
			intodb := fmt.Sprintf(`csv%d`, i)
			sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, intodb))
			sqlDB.Exec(t, fmt.Sprintf(`SET DATABASE = %s`, intodb))
			sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)

			var unused string
			var restored struct {
				rows, idx, sys, bytes int
			}

			// Insert the test data
			insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}
			numExistingRows := len(insert)

			if tx, err := db.Begin(); err != nil {
				t.Fatal(err)
			} else {
				for i, v := range insert {
					sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b) VALUES (%d, %s)", i, v))
				}

				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}

			var result int
			query := fmt.Sprintf(tc.query, strings.Join(tc.files, ", "))
			testNum++
			if tc.err != "" {
				sqlDB.ExpectErr(t, tc.err, query)
				return
			}

			sqlDB.QueryRow(t, query).Scan(
				&unused, &unused, &unused, &restored.rows, &restored.idx, &restored.sys, &restored.bytes,
			)

			jobPrefix := fmt.Sprintf(`IMPORT INTO %s.public.t(a, b)`, intodb)
			if err := jobutils.VerifySystemJob(t, sqlDB, testNum, jobspb.TypeImport, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(jobPrefix+` CSV DATA (%s)`+tc.jobOpts, strings.ReplaceAll(strings.Join(tc.files, ", "), "?param=value", "")),
			}); err != nil {
				t.Fatal(err)
			}

			isEmpty := len(tc.files) == 1 && tc.files[0] == empty[0]
			if isEmpty {
				sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
				if result != numExistingRows {
					t.Fatalf("expected %d rows, got %d", numExistingRows, result)
				}
				return
			}

			if expected, actual := insertedRows, restored.rows; expected != actual {
				t.Fatalf("expected %d rows, got %d", expected, actual)
			}

			// Verify correct number of rows via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
			if expect := numExistingRows + insertedRows; result != expect {
				t.Fatalf("expected %d rows, got %d", expect, result)
			}

			// Verify correct number of NULLs via COUNT.
			sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
			expectedNulls := 0
			if strings.Contains(tc.query, "nullif") {
				expectedNulls = insertedRows / 4
			}
			if result != expectedNulls {
				t.Fatalf("expected %d rows, got %d", expectedNulls, result)
			}

			// Verify spans don't have trailing '/0'.
			ranges := sqlDB.QueryStr(t, `SHOW testing_ranges FROM TABLE t`)
			for _, r := range ranges {
				const end = `/0`
				if strings.HasSuffix(r[0], end) || strings.HasSuffix(r[1], end) {
					t.Errorf("bad span: %s - %s", r[0], r[1])
				}
			}
		})
	}

	// Verify unique_rowid is replaced for tables without primary keys.
	t.Run("import-into-unique_rowid", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE pk")
		sqlDB.Exec(t, `CREATE TABLE pk.t (a INT, b STRING)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}
		numExistingRows := len(insert)

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO pk.t (a, b) VALUES (%d, %s)", i, v))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO pk.t (a, b) CSV DATA (%s)`, strings.Join(testFiles.files, ", ")))
		// Verify the rowids are being generated as expected.
		sqlDB.CheckQueryResults(t,
			`SELECT count(*) FROM pk.t`,
			sqlDB.QueryStr(t, `
			SELECT count(*) + $3 FROM
			(SELECT * FROM
				(SELECT generate_series(0, $1 - 1) file),
				(SELECT generate_series(1, $2) rownum)
			)
			`, numFiles, rowsPerFile, numExistingRows),
		)
	})

	// Verify a failed IMPORT INTO won't prevent a subsequent IMPORT INTO.
	t.Run("import-into-checkpoint-leftover", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE checkpoint; USE checkpoint")
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b) VALUES (%d, %s)", i, v))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		// Specify wrong table name.
		sqlDB.ExpectErr(
			t, `pq: relation "bad" does not exist`,
			fmt.Sprintf(`IMPORT INTO bad (a, b) CSV DATA (%s)`, testFiles.files[0]),
		)

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]))
	})

	// Tests for user specified target columns in IMPORT INTO statements.
	//
	// Tests IMPORT INTO with various target column sets, and an implicit PK
	// provided by the hidden column row_id. Some statements are run with
	// experimental_direct_ingestion.
	t.Run("target-cols-with-default-pk", func(t *testing.T) {
		var data string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				_, _ = w.Write([]byte(data))
			}
		}))
		defer srv.Close()

		sqlDB.Exec(t, `CREATE DATABASE targetcols`)
		sqlDB.Exec(t, `SET DATABASE = targetcols`)
		createQuery := `CREATE TABLE target%d (a INT8,
			b INT8,
			c STRING,
			d INT8,
			e INT8,
			f STRING)`
		var testNum int

		data = "1,5,e,7,12,teststr"
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, fmt.Sprintf(createQuery, testNum))
			query := fmt.Sprintf(`IMPORT INTO target%d (a) CSV DATA ($1)`, testNum)
			sqlDB.Exec(t, query, srv.URL)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM target%d`, testNum),
				sqlDB.QueryStr(t, `SELECT 1, NULL, NULL, NULL, NULL, 'NULL'`),
			)
			testNum++
		})
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, fmt.Sprintf(createQuery, testNum))
			query := fmt.Sprintf(`IMPORT INTO target%d (a, f) CSV DATA ($1) WITH experimental_direct_ingestion`, testNum)
			sqlDB.Exec(t, query, srv.URL)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM target%d`, testNum),
				sqlDB.QueryStr(t, `SELECT 1, NULL, NULL, NULL, NULL, 'teststr'`),
			)
			testNum++
		})
		t.Run(data, func(t *testing.T) {
			sqlDB.Exec(t, fmt.Sprintf(createQuery, testNum))
			query := fmt.Sprintf(`IMPORT INTO target%d (d, e, f) CSV DATA ($1)`, testNum)
			sqlDB.Exec(t, query, srv.URL)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM target%d`, testNum),
				sqlDB.QueryStr(t, `SELECT NULL, NULL, NULL, 7, 12, 'teststr'`),
			)
			testNum++
		})

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// Tests IMPORT INTO with a target column set, and an explicit PK.
	t.Run("target-cols-with-explicit-pk", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE targetcols; USE targetcols")
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b) VALUES (%d, %s)", i+1000, v))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO t (a) CSV DATA (%s)", testFiles.files[0]))

		var result int
		numExistingRows := len(insert)
		// Verify that the target column has been populated.
		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE a IS NOT NULL`).Scan(&result)
		if expect := numExistingRows + rowsPerFile; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}

		// Verify that the non-target columns have NULLs.
		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NULL`).Scan(&result)
		expectedNulls := rowsPerFile
		if result != expectedNulls {
			t.Fatalf("expected %d rows, got %d", expectedNulls, result)
		}

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// Tests IMPORT INTO with a target column set which does not include all PKs.
	// As a result the non-target column is non-nullable, which is not allowed
	// until we support DEFAULT expressions.
	t.Run("target-cols-excluding-explicit-pk", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE targetcols; USE targetcols")
		sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b STRING)`)

		// Expect an error if attempting to IMPORT INTO a target list which does
		// not include all the PKs of the table.
		sqlDB.ExpectErr(
			t, `pq: all non-target columns in IMPORT INTO must be nullable`,
			fmt.Sprintf(`IMPORT INTO t (b) CSV DATA (%s)`, testFiles.files[0]),
		)

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// Tests behavior when the existing table being imported into has more columns
	// in its schema then the source CSV file.
	t.Run("more-table-cols-than-csv", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE targetcols; USE targetcols")
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING, c INT)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b, c) VALUES (%d, %s, %d)", i, v, i))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		stripFilenameQuotes := testFiles.files[0][1 : len(testFiles.files[0])-1]
		sqlDB.ExpectErr(
			t, fmt.Sprintf("pq: %s: row 1: expected 3 fields, got 2", stripFilenameQuotes),
			fmt.Sprintf(`IMPORT INTO t (a, b, c) CSV DATA (%s)`, testFiles.files[0]),
		)

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// Tests behvior when the existing table being imported into has fewer columns
	// in its schema then the source CSV file.
	t.Run("fewer-table-cols-than-csv", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE targetcols; USE targetcols")
		sqlDB.Exec(t, `CREATE TABLE t (a INT)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a) VALUES (%d)", i))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		stripFilenameQuotes := testFiles.files[0][1 : len(testFiles.files[0])-1]
		sqlDB.ExpectErr(
			t, fmt.Sprintf("pq: %s: row 1: expected 1 fields, got 2", stripFilenameQuotes),
			fmt.Sprintf(`IMPORT INTO t (a) CSV DATA (%s)`, testFiles.files[0]),
		)

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// Tests IMPORT INTO without any target columns specified. This implies an
	// import of all columns in the exisiting table.
	t.Run("no-target-cols-specified", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE targetcols; USE targetcols")
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b) VALUES (%d, %s)", i, v))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		sqlDB.Exec(t, fmt.Sprintf("IMPORT INTO t CSV DATA (%s)", testFiles.files[0]))

		var result int
		numExistingRows := len(insert)
		// Verify that all columns have been populated with imported data.
		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE a IS NOT NULL`).Scan(&result)
		if expect := numExistingRows + rowsPerFile; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}

		sqlDB.QueryRow(t, `SELECT count(*) FROM t WHERE b IS NOT NULL`).Scan(&result)
		if expect := numExistingRows + rowsPerFile; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// IMPORT INTO does not support DEFAULT expressions for either target or
	// non-target columns.
	t.Run("import-into-check-no-default-cols", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE targetcols; USE targetcols")
		sqlDB.Exec(t, `CREATE TABLE t (a INT DEFAULT 1, b STRING)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b) VALUES (%d, %s)", i, v))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		sqlDB.ExpectErr(
			t, fmt.Sprintf("pq: cannot IMPORT INTO a table with a DEFAULT expression for any of its columns"),
			fmt.Sprintf(`IMPORT INTO t (a) CSV DATA (%s)`, testFiles.files[0]),
		)

		sqlDB.Exec(t, "DROP DATABASE targetcols")
	})

	// This tests that consecutive imports from unique data sources into an
	// existing table without an explicit PK, do not overwrite each other. It
	// exercises the row_id generation in IMPORT.
	t.Run("multiple-import-into-without-pk", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE DATABASE multiple; USE multiple")
		sqlDB.Exec(t, `CREATE TABLE t (a INT, b STRING)`)

		// Insert the test data
		insert := []string{"''", "'text'", "'a'", "'e'", "'l'", "'t'", "'z'"}
		numExistingRows := len(insert)
		insertedRows := rowsPerFile * 3

		if tx, err := db.Begin(); err != nil {
			t.Fatal(err)
		} else {
			for i, v := range insert {
				sqlDB.Exec(t, fmt.Sprintf("INSERT INTO t (a, b) VALUES (%d, %s)", i, v))
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		// Expect it to succeed with correct columns.
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[0]))
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[1]))
		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO t (a, b) CSV DATA (%s)`, testFiles.files[2]))

		// Verify correct number of rows via COUNT.
		var result int
		sqlDB.QueryRow(t, `SELECT count(*) FROM t`).Scan(&result)
		if expect := numExistingRows + insertedRows; result != expect {
			t.Fatalf("expected %d rows, got %d", expect, result)
		}
	})
}

func BenchmarkImport(b *testing.B) {
	const (
		nodes    = 3
		numFiles = nodes + 2
	)
	baseDir := filepath.Join("testdata", "csv")
	ctx := context.Background()
	tc := testcluster.StartTestCluster(b, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	testFiles := makeCSVData(b, numFiles, b.N*100, nodes, 16)

	b.ResetTimer()

	sqlDB.Exec(b,
		fmt.Sprintf(
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))
			CSV DATA (%s)`,
			strings.Join(testFiles.files, ","),
		))
}

func BenchmarkConvertRecord(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	ctx := context.TODO()

	tpchLineItemDataRows := [][]string{
		{"1", "155190", "7706", "1", "17", "21168.23", "0.04", "0.02", "N", "O", "1996-03-13", "1996-02-12", "1996-03-22", "DELIVER IN PERSON", "TRUCK", "egular courts above the"},
		{"1", "67310", "7311", "2", "36", "45983.16", "0.09", "0.06", "N", "O", "1996-04-12", "1996-02-28", "1996-04-20", "TAKE BACK RETURN", "MAIL", "ly final dependencies: slyly bold "},
		{"1", "63700", "3701", "3", "8", "13309.60", "0.10", "0.02", "N", "O", "1996-01-29", "1996-03-05", "1996-01-31", "TAKE BACK RETURN", "REG AIR", "riously. regular, express dep"},
		{"1", "2132", "4633", "4", "28", "28955.64", "0.09", "0.06", "N", "O", "1996-04-21", "1996-03-30", "1996-05-16", "NONE", "AIR", "lites. fluffily even de"},
		{"1", "24027", "1534", "5", "24", "22824.48", "0.10", "0.04", "N", "O", "1996-03-30", "1996-03-14", "1996-04-01", "NONE", "FOB", " pending foxes. slyly re"},
		{"1", "15635", "638", "6", "32", "49620.16", "0.07", "0.02", "N", "O", "1996-01-30", "1996-02-07", "1996-02-03", "DELIVER IN PERSON", "MAIL", "arefully slyly ex"},
		{"2", "106170", "1191", "1", "38", "44694.46", "0.00", "0.05", "N", "O", "1997-01-28", "1997-01-14", "1997-02-02", "TAKE BACK RETURN", "RAIL", "ven requests. deposits breach a"},
		{"3", "4297", "1798", "1", "45", "54058.05", "0.06", "0.00", "R", "F", "1994-02-02", "1994-01-04", "1994-02-23", "NONE", "AIR", "ongside of the furiously brave acco"},
		{"3", "19036", "6540", "2", "49", "46796.47", "0.10", "0.00", "R", "F", "1993-11-09", "1993-12-20", "1993-11-24", "TAKE BACK RETURN", "RAIL", " unusual accounts. eve"},
		{"3", "128449", "3474", "3", "27", "39890.88", "0.06", "0.07", "A", "F", "1994-01-16", "1993-11-22", "1994-01-23", "DELIVER IN PERSON", "SHIP", "nal foxes wake."},
	}
	b.SetBytes(120) // Raw input size. With 8 indexes, expect more on output side.

	stmt, err := parser.ParseOne(`CREATE TABLE lineitem (
		l_orderkey      INT8 NOT NULL,
		l_partkey       INT8 NOT NULL,
		l_suppkey       INT8 NOT NULL,
		l_linenumber    INT8 NOT NULL,
		l_quantity      DECIMAL(15,2) NOT NULL,
		l_extendedprice DECIMAL(15,2) NOT NULL,
		l_discount      DECIMAL(15,2) NOT NULL,
		l_tax           DECIMAL(15,2) NOT NULL,
		l_returnflag    CHAR(1) NOT NULL,
		l_linestatus    CHAR(1) NOT NULL,
		l_shipdate      DATE NOT NULL,
		l_commitdate    DATE NOT NULL,
		l_receiptdate   DATE NOT NULL,
		l_shipinstruct  CHAR(25) NOT NULL,
		l_shipmode      CHAR(10) NOT NULL,
		l_comment       VARCHAR(44) NOT NULL,
		PRIMARY KEY     (l_orderkey, l_linenumber),
		INDEX l_ok      (l_orderkey ASC),
		INDEX l_pk      (l_partkey ASC),
		INDEX l_sk      (l_suppkey ASC),
		INDEX l_sd      (l_shipdate ASC),
		INDEX l_cd      (l_commitdate ASC),
		INDEX l_rd      (l_receiptdate ASC),
		INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
		INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
	)`)
	if err != nil {
		b.Fatal(err)
	}
	create := stmt.AST.(*tree.CreateTable)
	st := cluster.MakeTestingClusterSettings()

	tableDesc, err := MakeSimpleTableDescriptor(ctx, st, create, sqlbase.ID(100), sqlbase.ID(100), NoFKs, 1)
	if err != nil {
		b.Fatal(err)
	}
	recordCh := make(chan csvRecord)
	kvCh := make(chan []roachpb.KeyValue)
	group := errgroup.Group{}

	// no-op drain kvs channel.
	go func() {
		for range kvCh {
		}
	}()

	c := &csvInputReader{recordCh: recordCh, tableDesc: tableDesc.TableDesc()}
	// start up workers.
	for i := 0; i < runtime.NumCPU(); i++ {
		group.Go(func() error {
			return c.convertRecordWorker(ctx)
		})
	}
	const batchSize = 500

	batch := csvRecord{
		file:      "some/path/to/some/file/of/csv/data.tbl",
		rowOffset: 1,
		r:         make([][]string, 0, batchSize),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if len(batch.r) > batchSize {
			recordCh <- batch
			batch.r = make([][]string, 0, batchSize)
			batch.rowOffset = i
		}

		batch.r = append(batch.r, tpchLineItemDataRows[i%len(tpchLineItemDataRows)])
	}
	recordCh <- batch
	close(recordCh)

	if err := group.Wait(); err != nil {
		b.Fatal(err)
	}
	close(kvCh)
}

// TestImportControlJob tests that PAUSE JOB, RESUME JOB, and CANCEL JOB
// work as intended on import jobs.
func TestImportControlJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	var serverArgs base.TestServerArgs
	// Disable external processing of mutations so that the final check of
	// crdb_internal.tables is guaranteed to not be cleaned up. Although this
	// was never observed by a stress test, it is here for safety.
	serverArgs.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: func() error {
			return errors.New("async schema changer disabled")
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE data`)

	makeSrv := func() *httptest.Server {
		var once sync.Once
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				// The following code correctly handles both the case where, after the
				// CANCEL JOB is issued, the second stage of the IMPORT (the shuffle,
				// after the sampling) may or may not be started. If it was started, then a
				// second GET request is done. The once here will cause that request to not
				// block. The draining for loop below will cause jobutils.RunJob's second send
				// on allowResponse to succeed (which it does after issuing the CANCEL JOB).
				once.Do(func() {
					<-allowResponse
					go func() {
						for range allowResponse {
						}
					}()
				})

				_, _ = w.Write([]byte(r.URL.Path[1:]))
			}
		}))
	}

	t.Run("cancel", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE cancelimport`)

		srv := makeSrv()
		defer srv.Close()

		var urls []string
		for i := 0; i < 10; i++ {
			urls = append(urls, fmt.Sprintf("'%s/%d'", srv.URL, i))
		}
		csvURLs := strings.Join(urls, ", ")

		query := fmt.Sprintf(`IMPORT TABLE cancelimport.t (i INT8 PRIMARY KEY) CSV DATA (%s)`, csvURLs)

		if _, err := jobutils.RunJob(
			t, sqlDB, &allowResponse, []string{"cancel"}, query,
		); !testutils.IsError(err, "job canceled") {
			t.Fatalf("expected 'job canceled' error, but got %+v", err)
		}
		// Check that executing again succeeds. This won't work if the first import
		// was not successfully canceled.
		sqlDB.Exec(t, query)
	})

	t.Run("pause", func(t *testing.T) {
		// Test that IMPORT can be paused and resumed. This test also attempts to
		// only pause the job after it has begun splitting ranges. When the job
		// is resumed, if the sampling phase is re-run, the splits points will
		// differ. When AddSSTable attempts to import the new ranges, they will
		// fail because there is an existing split in the key space that it cannot
		// handle. Use a sstsize that will more-or-less (since it is statistical)
		// always cause this condition.

		sqlDB.Exec(t, `CREATE DATABASE pauseimport`)

		srv := makeSrv()
		defer srv.Close()

		count := 100
		// This test takes a while with the race detector, so reduce the number of
		// files in an attempt to speed it up.
		if util.RaceEnabled {
			count = 20
		}

		urls := make([]string, count)
		for i := 0; i < count; i++ {
			urls[i] = fmt.Sprintf("'%s/%d'", srv.URL, i)
		}
		csvURLs := strings.Join(urls, ", ")
		query := fmt.Sprintf(`IMPORT TABLE pauseimport.t (i INT8 PRIMARY KEY) CSV DATA (%s) WITH sstsize = '50B'`, csvURLs)

		jobID, err := jobutils.RunJob(
			t, sqlDB, &allowResponse, []string{"PAUSE"}, query,
		)
		if !testutils.IsError(err, "job paused") {
			t.Fatalf("unexpected: %v", err)
		}
		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
		jobutils.WaitForJob(t, sqlDB, jobID)
		sqlDB.CheckQueryResults(t,
			`SELECT * FROM pauseimport.t ORDER BY i`,
			sqlDB.QueryStr(t, `SELECT * FROM generate_series(0, $1)`, count-1),
		)
	})
}

// TestImportWorkerFailure tests that IMPORT can restart after the failure
// of a worker node.
func TestImportWorkerFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(mjibson): Although this test passes most of the time it still
	// sometimes fails because not all kinds of failures caused by shutting a
	// node down are detected and retried.
	t.Skip("flakey due to undetected kinds of failures when the node is shutdown")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	allowResponse := make(chan struct{})
	params := base.TestClusterArgs{}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(r.URL.Path[1:]))
		}
	}))
	defer srv.Close()

	count := 20
	urls := make([]string, count)
	for i := 0; i < count; i++ {
		urls[i] = fmt.Sprintf("'%s/%d'", srv.URL, i)
	}
	csvURLs := strings.Join(urls, ", ")
	query := fmt.Sprintf(`IMPORT TABLE t (i INT8 PRIMARY KEY) CSV DATA (%s) WITH sstsize = '1B'`, csvURLs)

	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	select {
	case allowResponse <- struct{}{}:
	case err := <-errCh:
		t.Fatalf("%s: query returned before expected: %s", err, query)
	}
	var jobID int64
	sqlDB.QueryRow(t, `SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)

	// Shut down a node. This should force LoadCSV to fail in its current
	// execution. It should detect this as a context canceled error.
	tc.StopServer(1)

	close(allowResponse)
	// We expect the statement to fail.
	if err := <-errCh; !testutils.IsError(err, "node failure") {
		t.Fatal(err)
	}

	// But the job should be restarted and succeed eventually.
	jobutils.WaitForJob(t, sqlDB, jobID)
	sqlDB.CheckQueryResults(t,
		`SELECT * FROM t ORDER BY i`,
		sqlDB.QueryStr(t, `SELECT * FROM generate_series(0, $1)`, count-1),
	)
}

// TestImportLivenessWithRestart tests that a node liveness transition
// during IMPORT correctly resumes after the node executing the job
// becomes non-live (from the perspective of the jobs registry).
//
// Its actual purpose is to address the second bug listed in #22924 about
// the addsstable arguments not in request range. The theory was that the
// restart in that issue was caused by node liveness and that the work
// already performed (the splits and addsstables) somehow caused the second
// error. However this does not appear to be the case, as running many stress
// iterations with differing constants (rows, sstsize, kv.import.batch_size)
// was not able to fail in the way listed by the second bug.
func TestImportLivenessWithRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	const rows = 5000
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT8 PRIMARY KEY) CSV DATA ($1) WITH sstsize = '500B'`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Progress{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, progress FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node non-live and wait for cancellation
	nl.FakeSetExpiration(1, hlc.MinTimestamp)
	// Wait for the registry cancel loop to run and cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)
	err := <-errCh
	if !testutils.IsError(err, "job .*: node liveness error") {
		t.Fatalf("unexpected: %v", err)
	}

	// Ensure that partial progress has been recorded
	partialProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	if len(partialProgress.Details.(*jobspb.Progress_Import).Import.SpanProgress) == 0 {
		t.Fatal("no partial import progress detected")
	}

	// Make the node live again
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)
	// The registry should now adopt the job and resume it.
	jobutils.WaitForJob(t, sqlDB, jobID)
	// Verify that the job lease was updated
	rescheduledProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	if rescheduledProgress.ModifiedMicros <= originalLease.ModifiedMicros {
		t.Fatalf("expecting rescheduled job to have a later modification time: %d vs %d",
			rescheduledProgress.ModifiedMicros, originalLease.ModifiedMicros)
	}

	// Verify that all expected rows are present after a stop/start cycle.
	var rowCount int
	sqlDB.QueryRow(t, "SELECT count(*) from liveness.t").Scan(&rowCount)
	if rowCount != rows {
		t.Fatalf("not all rows were present.  Expecting %d, had %d", rows, rowCount)
	}

	// Verify that all write progress coalesced into a single span
	// encompassing the entire table.
	spans := rescheduledProgress.Details.(*jobspb.Progress_Import).Import.SpanProgress
	if len(spans) != 1 {
		t.Fatalf("expecting only a single progress span, had %d\n%s", len(spans), spans)
	}

	// Ensure that an entire table range is marked as complete
	tableSpan := roachpb.Span{
		Key:    keys.MinKey,
		EndKey: keys.MaxKey,
	}
	if !tableSpan.EqualValue(spans[0]) {
		t.Fatalf("expected entire table to be marked complete, had %s", spans[0])
	}
}

// TestImportLivenessWithLeniency tests that a temporary node liveness
// transition during IMPORT doesn't cancel the job, but allows the
// owning node to continue processing.
func TestImportLivenessWithLeniency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)
	// We want to know exactly how much leniency is configured.
	sqlDB.Exec(t, `SET CLUSTER SETTING jobs.registry.leniency = '1m'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const rows = 5000
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT8 PRIMARY KEY) CSV DATA ($1) WITH sstsize = '500B'`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Payload{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, payload FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node slightly tardy.
	nl.FakeSetExpiration(1, hlc.Timestamp{
		WallTime: hlc.UnixNano() - (15 * time.Second).Nanoseconds(),
	})

	// Wait for the registry cancel loop to run and not cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)

	// Set the node to be fully live again.  This prevents the registry
	// from canceling all of the jobs if the test node is saturated
	// and the import runs slowly.
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)

	// Verify that the client didn't see anything amiss.
	if err := <-errCh; err != nil {
		t.Fatalf("import job should have completed: %s", err)
	}

	// The job should have completed normally.
	jobutils.WaitForJob(t, sqlDB, jobID)
}

// TestImportMVCCChecksums verifies that MVCC checksums are correctly
// computed by issuing a secondary index change that runs a CPut on the
// index. See #23984.
func TestImportMVCCChecksums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "1,1,1")
		}
	}))
	defer srv.Close()

	sqlDB.Exec(t, `IMPORT TABLE d.t (
		a INT8 PRIMARY KEY,
		b INT8,
		c INT8,
		INDEX (b) STORING (c)
	) CSV DATA ($1)`, srv.URL)
	sqlDB.Exec(t, `UPDATE d.t SET c = 2 WHERE a = 1`)
}

func TestImportMysql(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	files := getMysqldumpTestdata(t)
	simple := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(files.simple, baseDir))}
	second := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(files.second, baseDir))}
	multitable := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(files.wholeDB, baseDir))}
	multitableGz := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(files.wholeDB+".gz", baseDir))}
	multitableBz := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(files.wholeDB+".bz2", baseDir))}

	const expectSimple, expectSecond, expectEverything = 1 << 0, 1 << 2, 1 << 3
	const expectAll = -1
	for _, c := range []struct {
		name     string
		expected int
		query    string
		args     []interface{}
	}{
		{`read data only`, expectSimple, `IMPORT TABLE simple (i INT8 PRIMARY KEY, s text, b bytea) MYSQLDUMP DATA ($1)`, simple},
		{`single table dump`, expectSimple, `IMPORT TABLE simple FROM MYSQLDUMP ($1)`, simple},
		{`second table dump`, expectSecond, `IMPORT TABLE second FROM MYSQLDUMP ($1) WITH skip_foreign_keys`, second},
		{`simple from multi`, expectSimple, `IMPORT TABLE simple FROM MYSQLDUMP ($1)`, multitable},
		{`second from multi`, expectSecond, `IMPORT TABLE second FROM MYSQLDUMP ($1) WITH skip_foreign_keys`, multitable},
		{`all from multi`, expectAll, `IMPORT MYSQLDUMP ($1)`, multitable},
		{`all from multi gzip`, expectAll, `IMPORT MYSQLDUMP ($1)`, multitableGz},
		{`all from multi bzip`, expectAll, `IMPORT MYSQLDUMP ($1)`, multitableBz},
	} {
		t.Run(c.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second, third, everything CASCADE`)
			sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS simple_auto_inc, third_auto_inc`)
			sqlDB.Exec(t, c.query, c.args...)

			if c.expected&expectSimple != 0 {
				if c.name != "read data only" {
					sqlDB.Exec(t, "INSERT INTO simple (s) VALUES ('auto-inc')")
				}

				for idx, row := range sqlDB.QueryStr(t, "SELECT * FROM simple ORDER BY i") {
					{
						if idx == len(simpleTestRows) {
							if expected, actual := "auto-inc", row[1]; expected != actual {
								t.Fatalf("expected rowi=%s string to be %q, got %q", row[0], expected, actual)
							}
							continue
						}
						expected, actual := simpleTestRows[idx].s, row[1]
						if expected == injectNull {
							expected = "NULL"
						}
						if expected != actual {
							t.Fatalf("expected rowi=%s string to be %q, got %q", row[0], expected, actual)
						}
					}

					{
						expected, actual := simpleTestRows[idx].b, row[2]
						if expected == nil {
							expected = []byte("NULL")
						}
						if !bytes.Equal(expected, []byte(actual)) {
							t.Fatalf("expected rowi=%s bytes to be %q, got %q", row[0], expected, actual)
						}
					}
				}
			} else {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM simple LIMIT 1`)
			}

			if c.expected&expectSecond != 0 {
				res := sqlDB.QueryStr(t, "SELECT * FROM second ORDER BY i")
				if expected, actual := secondTableRows, len(res); expected != actual {
					t.Fatalf("expected %d, got %d", expected, actual)
				}
				for _, row := range res {
					if i, j := row[0], row[1]; i != "-"+j {
						t.Fatalf("expected %s = - %s", i, j)
					}
				}
			} else {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM second LIMIT 1`)
			}
			if c.expected&expectEverything != 0 {
				res := sqlDB.QueryStr(t, "SELECT i, c, iw, fl, d53, j FROM everything ORDER BY i")
				if expected, actual := len(everythingTestRows), len(res); expected != actual {
					t.Fatalf("expected %d, got %d", expected, actual)
				}
				for i := range res {
					if got, expected := res[i][0], fmt.Sprintf("%v", everythingTestRows[i].i); got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][1], everythingTestRows[i].c; got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][2], fmt.Sprintf("%v", everythingTestRows[i].iw); got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][3], fmt.Sprintf("%v", everythingTestRows[i].fl); got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][4], everythingTestRows[i].d53; got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
					if got, expected := res[i][5], everythingTestRows[i].j; got != expected {
						t.Fatalf("expected %s got %s", expected, got)
					}
				}
			} else {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM everything LIMIT 1`)
			}
		})
	}
}

func TestImportMysqlOutfile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata", "mysqlout")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	testRows, configs := getMysqlOutfileTestdata(t)

	for i, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			var opts []interface{}

			cmd := fmt.Sprintf(`IMPORT TABLE test%d (i INT8 PRIMARY KEY, s text, b bytea) MYSQLOUTFILE DATA ($1)`, i)
			opts = append(opts, fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(cfg.filename, baseDir)))

			var flags []string
			if cfg.opts.RowSeparator != '\n' {
				opts = append(opts, string(cfg.opts.RowSeparator))
				flags = append(flags, fmt.Sprintf("rows_terminated_by = $%d", len(opts)))
			}
			if cfg.opts.FieldSeparator != '\t' {
				opts = append(opts, string(cfg.opts.FieldSeparator))
				flags = append(flags, fmt.Sprintf("fields_terminated_by = $%d", len(opts)))
			}
			if cfg.opts.Enclose == roachpb.MySQLOutfileOptions_Always {
				opts = append(opts, string(cfg.opts.Encloser))
				flags = append(flags, fmt.Sprintf("fields_enclosed_by = $%d", len(opts)))
			}
			if cfg.opts.HasEscape {
				opts = append(opts, string(cfg.opts.Escape))
				flags = append(flags, fmt.Sprintf("fields_escaped_by = $%d", len(opts)))
			}
			if len(flags) > 0 {
				cmd += " WITH " + strings.Join(flags, ", ")
			}
			sqlDB.Exec(t, cmd, opts...)
			for idx, row := range sqlDB.QueryStr(t, fmt.Sprintf("SELECT * FROM test%d ORDER BY i", i)) {
				expected, actual := testRows[idx].s, row[1]
				if expected == injectNull {
					expected = "NULL"
				}

				if expected != actual {
					t.Fatalf("expected row i=%s string to be %q, got %q", row[0], expected, actual)
				}
			}
		})
	}
}

func TestImportPgCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata", "pgcopy")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	testRows, configs := getPgCopyTestdata(t)

	for i, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			var opts []interface{}

			cmd := fmt.Sprintf(`IMPORT TABLE test%d (i INT8 PRIMARY KEY, s text, b bytea) PGCOPY DATA ($1)`, i)
			opts = append(opts, fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(cfg.filename, baseDir)))

			var flags []string
			if cfg.opts.Delimiter != '\t' {
				opts = append(opts, string(cfg.opts.Delimiter))
				flags = append(flags, fmt.Sprintf("delimiter = $%d", len(opts)))
			}
			if cfg.opts.Null != `\N` {
				opts = append(opts, cfg.opts.Null)
				flags = append(flags, fmt.Sprintf("nullif = $%d", len(opts)))
			}
			if len(flags) > 0 {
				cmd += " WITH " + strings.Join(flags, ", ")
			}
			t.Log(cmd, opts)
			sqlDB.Exec(t, cmd, opts...)
			for idx, row := range sqlDB.QueryStr(t, fmt.Sprintf("SELECT * FROM test%d ORDER BY i", i)) {
				{
					expected, actual := testRows[idx].s, row[1]
					if expected == injectNull {
						expected = "NULL"
					}

					if expected != actual {
						t.Fatalf("expected row i=%s string to be %q, got %q", row[0], expected, actual)
					}
				}

				{
					expected, actual := testRows[idx].b, row[2]
					if expected == nil {
						expected = []byte("NULL")
					}
					if !bytes.Equal(expected, []byte(actual)) {
						t.Fatalf("expected rowi=%s bytes to be %q, got %q", row[0], expected, actual)
					}
				}
			}
		})
	}
}

func TestImportPgDump(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.import.batch_size = '10KB'`)
	sqlDB.Exec(t, `CREATE DATABASE foo; SET DATABASE = foo`)

	simplePgTestRows, simpleFile := getSimplePostgresDumpTestdata(t)
	simple := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(simpleFile, baseDir))}
	secondTableRowCount, secondFile := getSecondPostgresDumpTestdata(t)
	second := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(secondFile, baseDir))}
	multitableFile := getMultiTablePostgresDumpTestdata(t)
	multitable := []interface{}{fmt.Sprintf("nodelocal://%s", strings.TrimPrefix(multitableFile, baseDir))}

	const expectAll, expectSimple, expectSecond = 1, 2, 3

	for _, c := range []struct {
		name     string
		expected int
		query    string
		args     []interface{}
	}{
		{
			`read data only`,
			expectSimple,
			`IMPORT TABLE simple (
				i INT8,
				s text,
				b bytea,
				CONSTRAINT simple_pkey PRIMARY KEY (i),
				UNIQUE INDEX simple_b_s_idx (b, s),
				INDEX simple_s_idx (s)
			) PGDUMP DATA ($1)`,
			simple,
		},
		{`single table dump`, expectSimple, `IMPORT TABLE simple FROM PGDUMP ($1)`, simple},
		{`second table dump`, expectSecond, `IMPORT TABLE second FROM PGDUMP ($1)`, second},
		{`simple from multi`, expectSimple, `IMPORT TABLE simple FROM PGDUMP ($1)`, multitable},
		{`second from multi`, expectSecond, `IMPORT TABLE second FROM PGDUMP ($1)`, multitable},
		{`all from multi`, expectAll, `IMPORT PGDUMP ($1)`, multitable},
	} {
		t.Run(c.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS simple, second`)
			sqlDB.Exec(t, c.query, c.args...)

			if c.expected == expectSimple || c.expected == expectAll {
				// Verify table schema because PKs and indexes are at the bottom of pg_dump.
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE simple`, [][]string{{
					"simple", `CREATE TABLE simple (
	i INT8 NOT NULL,
	s STRING NULL,
	b BYTES NULL,
	CONSTRAINT simple_pkey PRIMARY KEY (i ASC),
	UNIQUE INDEX simple_b_s_idx (b ASC, s ASC),
	INDEX simple_s_idx (s ASC),
	FAMILY "primary" (i, s, b)
)`,
				}})

				rows := sqlDB.QueryStr(t, "SELECT * FROM simple ORDER BY i")
				if a, e := len(rows), len(simplePostgresTestRows); a != e {
					t.Fatalf("got %d rows, expected %d", a, e)
				}

				for idx, row := range rows {
					{
						expected, actual := simplePostgresTestRows[idx].s, row[1]
						if expected == injectNull {
							expected = "NULL"
						}
						if expected != actual {
							t.Fatalf("expected rowi=%s string to be %q, got %q", row[0], expected, actual)
						}
					}

					{
						expected, actual := simplePgTestRows[idx].b, row[2]
						if expected == nil {
							expected = []byte("NULL")
						}
						if !bytes.Equal(expected, []byte(actual)) {
							t.Fatalf("expected rowi=%s bytes to be %q, got %q", row[0], expected, actual)
						}
					}
				}
			}

			if c.expected == expectSecond || c.expected == expectAll {
				// Verify table schema because PKs and indexes are at the bottom of pg_dump.
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE second`, [][]string{{
					"second", `CREATE TABLE second (
	i INT8 NOT NULL,
	s STRING NULL,
	CONSTRAINT second_pkey PRIMARY KEY (i ASC),
	FAMILY "primary" (i, s)
)`,
				}})
				res := sqlDB.QueryStr(t, "SELECT * FROM second ORDER BY i")
				if expected, actual := secondTableRowCount, len(res); expected != actual {
					t.Fatalf("expected %d, got %d", expected, actual)
				}
				for _, row := range res {
					if i, s := row[0], row[1]; i != s {
						t.Fatalf("expected %s = %s", i, s)
					}
				}
			}

			if c.expected == expectSecond {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM simple LIMIT 1`)
			}
			if c.expected == expectSimple {
				sqlDB.ExpectErr(t, "does not exist", `SELECT 1 FROM second LIMIT 1`)
			}
			if c.expected == expectAll {
				sqlDB.CheckQueryResults(t, `SHOW CREATE TABLE seqtable`, [][]string{{
					"seqtable", `CREATE TABLE seqtable (
	a INT8 NULL DEFAULT nextval('public.a_seq':::STRING),
	b INT8 NULL,
	FAMILY "primary" (a, b, rowid)
)`,
				}})
				sqlDB.CheckQueryResults(t, `SHOW CREATE SEQUENCE a_seq`, [][]string{{
					"a_seq", `CREATE SEQUENCE a_seq MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
				}})
				sqlDB.CheckQueryResults(t, `select last_value from a_seq`, [][]string{{"7"}})
				sqlDB.CheckQueryResults(t,
					`SELECT * FROM seqtable ORDER BY a`,
					sqlDB.QueryStr(t, `select a+1, a*10 from generate_series(0, 6) a`),
				)
				sqlDB.CheckQueryResults(t, `select last_value from a_seq`, [][]string{{"7"}})
				// This can sometimes retry, so the next value might not be 8.
				sqlDB.Exec(t, `INSERT INTO seqtable (b) VALUES (70)`)
				sqlDB.CheckQueryResults(t, `select last_value >= 8 from a_seq`, [][]string{{"true"}})
				sqlDB.CheckQueryResults(t,
					`SELECT b FROM seqtable WHERE a = (SELECT last_value FROM a_seq)`,
					[][]string{{"70"}},
				)
			}
		})
	}
}

func TestImportCockroachDump(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes = 3
	)
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, "IMPORT PGDUMP ($1)", "nodelocal:///cockroachdump/dump.sql")
	sqlDB.CheckQueryResults(t, "SELECT * FROM t ORDER BY i", [][]string{
		{"1", "test"},
		{"2", "other"},
	})
	sqlDB.CheckQueryResults(t, "SELECT * FROM a", [][]string{
		{"2"},
	})
	sqlDB.CheckQueryResults(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE t", [][]string{
		{"primary", "-6413178410144704641"},
		{"t_t_idx", "-4841734847805280813"},
	})
	sqlDB.CheckQueryResults(t, "SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE a", [][]string{
		{"primary", "-5808590958014384147"},
	})
	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE t", [][]string{
		{"t", `CREATE TABLE t (
	i INT8 NOT NULL,
	t STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	INDEX t_t_idx (t ASC),
	FAMILY "primary" (i, t)
)`},
	})
	sqlDB.CheckQueryResults(t, "SHOW CREATE TABLE a", [][]string{
		{"a", `CREATE TABLE a (
	i INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	CONSTRAINT fk_i_ref_t FOREIGN KEY (i) REFERENCES t(i),
	FAMILY "primary" (i)
)`},
	})
}

func TestCreateStatsAfterImport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = time.Microsecond

	const nodes = 1
	ctx := context.Background()
	baseDir := filepath.Join("testdata")
	args := base.TestServerArgs{ExternalIODir: baseDir}
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: args})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`)

	sqlDB.Exec(t, "IMPORT PGDUMP ($1)", "nodelocal:///cockroachdump/dump.sql")

	// Verify that statistics have been created.
	sqlDB.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE t]`,
		[][]string{
			{"__auto__", "{i}", "2", "2", "0"},
			{"__auto__", "{t}", "2", "2", "0"},
		})
	sqlDB.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE a]`,
		[][]string{
			{"__auto__", "{i}", "1", "1", "0"},
		})
}
