// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	_ "github.com/lib/pq"
)

var rewritePostgresTestData = envutil.EnvOrDefaultBool("COCKROACH_REWRITE_POSTGRES_TESTDATA", false)

var simplePostgresTestRows = func() []simpleTestRow {
	badChars := []rune{'a', ';', '\n', ',', '"', '\\', '\r', '<', '\t', '✅', 'π', rune(10), rune(2425), rune(5183), utf8.RuneError}
	r := rand.New(rand.NewSource(1))
	testRows := []simpleTestRow{
		{i: 0, s: `str`},
		{i: 1, s: ``},
		{i: 2, s: ` `},
		{i: 3, s: `,`},
		{i: 4, s: "\n"},
		{i: 5, s: `\n`},
		{i: 6, s: "\r\n"},
		{i: 7, s: "\r"},
		{i: 9, s: `"`},

		{i: 10, s: injectNull},
		{i: 11, s: `\N`},
		{i: 12, s: `NULL`},

		// Unicode
		{i: 13, s: `¢`},
		{i: 14, s: ` ¢ `},
		{i: 15, s: `✅`},
		{i: 16, s: `","\n,™¢`},
		{i: 19, s: `✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑`},
		{i: 20, s: `a quote " or two quotes "" and a quote-comma ", , and then a quote and newline "` + "\n"},
		{i: 21, s: `"a slash \, a double slash \\, a slash+quote \",  \` + "\n"},
	}

	for i := 0; i < 10; i++ {
		buf := make([]byte, 200)
		r.Seed(int64(i))
		r.Read(buf)
		testRows = append(testRows, simpleTestRow{i: i + 100, s: randStr(r, badChars, 1000), b: buf})
	}
	return testRows
}()

func getSimplePostgresDumpTestdata(t *testing.T) ([]simpleTestRow, string) {
	dest := filepath.Join(`testdata`, `pgdump`, `simple.sql`)
	if rewritePostgresTestData {
		genSimplePostgresTestdata(t, func() { pgdump(t, dest, "simple") })
	}
	return simplePostgresTestRows, dest
}

func getSecondPostgresDumpTestdata(t *testing.T) (int, string) {
	dest := filepath.Join(`testdata`, `pgdump`, `second.sql`)
	if rewritePostgresTestData {
		genSecondPostgresTestdata(t, func() { pgdump(t, dest, "second") })
	}
	return secondTableRows, dest
}

func getMultiTablePostgresDumpTestdata(t *testing.T) string {
	dest := filepath.Join(`testdata`, `pgdump`, `db.sql`)
	if rewritePostgresTestData {
		genSequencePostgresTestdata(t, func() {
			genSecondPostgresTestdata(t, func() {
				genSimplePostgresTestdata(t, func() { pgdump(t, dest) })
			})
		})
	}
	return dest
}

type pgCopyDumpCfg struct {
	name     string
	filename string
	opts     roachpb.PgCopyOptions
}

func getPgCopyTestdata(t *testing.T) ([]simpleTestRow, []pgCopyDumpCfg) {
	configs := []pgCopyDumpCfg{
		{
			name: "default",
			opts: roachpb.PgCopyOptions{
				Delimiter: '\t',
				Null:      `\N`,
			},
		},
		{
			name: "comma-null-header",
			opts: roachpb.PgCopyOptions{
				Delimiter: ',',
				Null:      "null",
			},
		},
	}

	for i := range configs {
		configs[i].filename = filepath.Join(`testdata`, `pgcopy`, configs[i].name, `test.txt`)
	}

	if rewritePostgresTestData {
		genSimplePostgresTestdata(t, func() {
			if err := os.RemoveAll(filepath.Join(`testdata`, `pgcopy`)); err != nil {
				t.Fatal(err)
			}
			for _, cfg := range configs {
				dest := filepath.Dir(cfg.filename)
				if err := os.MkdirAll(dest, 0777); err != nil {
					t.Fatal(err)
				}

				var sb strings.Builder
				sb.WriteString(`COPY simple TO STDOUT WITH (FORMAT 'text'`)
				if cfg.opts.Delimiter != copyDefaultDelimiter {
					fmt.Fprintf(&sb, `, DELIMITER %q`, cfg.opts.Delimiter)
				}
				if cfg.opts.Null != copyDefaultNull {
					fmt.Fprintf(&sb, `, NULL "%s"`, cfg.opts.Null)
				}
				sb.WriteString(`)`)
				flags := []string{`-U`, `postgres`, `-h`, `127.0.0.1`, `test`, `-c`, sb.String()}
				if res, err := exec.Command(
					`psql`, flags...,
				).CombinedOutput(); err != nil {
					t.Fatal(err, string(res))
				} else if err := ioutil.WriteFile(cfg.filename, res, 0666); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

	return simplePostgresTestRows, configs
}

func genSimplePostgresTestdata(t *testing.T, dump func()) {
	defer genPostgresTestdata(t,
		"simple",
		`i INT PRIMARY KEY, s text, b bytea`,
		func(db *gosql.DB) {
			// Postgres doesn't support creating non-unique indexes in CREATE TABLE;
			// do it afterward.
			if _, err := db.Exec(`
				CREATE UNIQUE INDEX ON simple (b, s);
				CREATE INDEX ON simple (s);
			`); err != nil {
				t.Fatal(err)
			}
			for _, tc := range simplePostgresTestRows {
				s := &tc.s
				if *s == injectNull {
					s = nil
				}
				if _, err := db.Exec(
					`INSERT INTO simple VALUES ($1, $2, NULLIF($3, ''::bytea))`, tc.i, s, tc.b,
				); err != nil {
					t.Fatal(err)
				}
			}
		},
	)()
	dump()
}

func genSecondPostgresTestdata(t *testing.T, dump func()) {
	defer genPostgresTestdata(t,
		"second",
		`i INT PRIMARY KEY, s TEXT`,
		func(db *gosql.DB) {
			for i := 0; i < secondTableRows; i++ {
				if _, err := db.Exec(`INSERT INTO second VALUES ($1, $2)`, i, strconv.Itoa(i)); err != nil {
					t.Fatal(err)
				}
			}
		},
	)()
	dump()
}

func genSequencePostgresTestdata(t *testing.T, dump func()) {
	defer genPostgresTestdata(t,
		"seqtable",
		`a INT, b INT`,
		func(sqlDB *gosql.DB) {
			db := sqlutils.MakeSQLRunner(sqlDB)
			db.Exec(t, `DROP SEQUENCE IF EXISTS a_seq`)
			db.Exec(t, `CREATE SEQUENCE a_seq`)
			db.Exec(t, `ALTER TABLE seqtable ALTER COLUMN a SET DEFAULT nextval('a_seq'::REGCLASS)`)
			for i := 0; i < secondTableRows; i++ {
				db.Exec(t, `INSERT INTO seqtable (b) VALUES ($1 * 10)`, i)
			}
		},
	)()
	dump()
}

// genPostgresTestdata connects to the a local postgres, creates the passed
// table and calls the passed `load` func to populate it and returns a
// cleanup func.
func genPostgresTestdata(t *testing.T, name, schema string, load func(*gosql.DB)) func() {
	db, err := gosql.Open("postgres", "postgres://postgres@localhost/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %s (%s)`, name, schema),
	); err != nil {
		t.Fatal(err)
	}
	load(db)
	return func() {
		db, err := gosql.Open("postgres", "postgres://postgres@localhost/test?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(
			fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name),
		); err != nil {
			t.Fatal(err)
		}
	}
}

func pgdump(t *testing.T, dest string, tables ...string) {
	if err := os.MkdirAll(filepath.Dir(dest), 0777); err != nil {
		t.Fatal(err)
	}

	args := []string{`-U`, `postgres`, `-h`, `127.0.0.1`, `-d`, `test`}
	for _, table := range tables {
		args = append(args, `-t`, table)
	}
	out, err := exec.Command(`pg_dump`, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %s", err, out)
	}
	if err := ioutil.WriteFile(dest, out, 0666); err != nil {
		t.Fatal(err)
	}
}
