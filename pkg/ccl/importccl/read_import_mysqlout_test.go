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
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	_ "github.com/go-sql-driver/mysql"
)

type testRow struct {
	i int
	s string
	b []byte
}

type dumpCfg struct {
	name     string
	filename string
	opts     roachpb.MySQLOutfileOptions
	null     string
}

const injectNull = "inject-null"

func loadMysqlTestdata(t *testing.T, rows []testRow) func() {
	db, err := gosql.Open("mysql", "root@/test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`DROP TABLE IF EXISTS test`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE TABLE test (i INT PRIMARY KEY, s text, b binary(200))`,
	); err != nil {
		t.Fatal(err)
	}

	for _, tc := range rows {
		s := &tc.s
		if *s == injectNull {
			s = nil
		}
		if _, err := db.Exec(
			`INSERT INTO test VALUES (?, ?, ?)`, tc.i, s, tc.b,
		); err != nil {
			t.Fatal(err)
		}
	}
	return func() {
		if _, err := db.Exec(
			`DROP TABLE IF EXISTS test`,
		); err != nil {
			t.Fatal(err)
		}
	}
}

func writeMysqlOutfileTestdata(t *testing.T, rows []testRow, configs []dumpCfg) {
	cleanup := loadMysqlTestdata(t, rows)
	defer cleanup()

	if err := os.RemoveAll(filepath.Join(`testdata`, `mysqlout`)); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range configs {
		dest := filepath.Dir(cfg.filename)
		if err := os.MkdirAll(dest, 0777); err != nil {
			t.Fatal(err)
		}

		flags := []string{`-u`, `root`, `test`, `test`, `--tab`, `./` + dest}
		if cfg.opts.Enclose == roachpb.MySQLOutfileOptions_Always {
			flags = append(flags, "--fields-enclosed-by", string(cfg.opts.Encloser))
		}
		if cfg.opts.HasEscape {
			flags = append(flags, "--fields-escaped-by", string(cfg.opts.Escape))
		}
		if cfg.opts.FieldSeparator != '\t' {
			flags = append(flags, "--fields-terminated-by", string(cfg.opts.FieldSeparator))
		}
		if cfg.opts.RowSeparator != '\n' {
			flags = append(flags, "--lines-terminated-by", string(cfg.opts.RowSeparator))
		}
		if res, err := exec.Command(
			`mysqldump`, flags...,
		).CombinedOutput(); err != nil {
			t.Fatal(err, string(res))
		}

		if err := os.Remove(filepath.Join(dest, "test.sql")); err != nil {
			t.Fatal(err)
		}
	}
}

func randStr(r *rand.Rand, from []rune, length int) string {
	s := make([]rune, length)
	for i := 0; i < length; i++ {
		s[i] = from[r.Intn(len(from))]
	}
	return string(s)
}

func getMysqlTestRows() []testRow {
	badChars := []rune{'a', ';', '\n', ',', '"', '\\', '\r', '<', '\t', '✅', 'π', rune(0), rune(10), rune(2425), rune(5183), utf8.RuneError}
	r := rand.New(rand.NewSource(1))
	testRows := []testRow{
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
		{i: 17, s: string([]rune{rune(0)})},
		{i: 19, s: `✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑`},
		{i: 20, s: `a quote " or two quotes "" and a quote-comma ", , and then a quote and newline "` + "\n"},
		{i: 21, s: `"a slash \, a double slash \\, a slash+quote \",  \` + "\n"},
	}

	for i := 0; i < 10; i++ {
		buf := make([]byte, 200)
		r.Seed(int64(i))
		r.Read(buf)
		testRows = append(testRows, testRow{i: i + 100, s: randStr(r, badChars, 1000), b: buf})
	}
	return testRows
}

func getMysqlOutfileTestdata(t *testing.T) ([]testRow, []dumpCfg) {
	testRows := getMysqlTestRows()

	configs := []dumpCfg{
		{
			name: "escape-and-enclose",
			opts: roachpb.MySQLOutfileOptions{
				FieldSeparator: '\t',
				RowSeparator:   '\n',
				HasEscape:      true,
				Escape:         '\\',
				Enclose:        roachpb.MySQLOutfileOptions_Always,
				Encloser:       '"',
			},
			null: `\N`,
		},
		{
			name: "csv-ish",
			opts: roachpb.MySQLOutfileOptions{
				FieldSeparator: ',',
				RowSeparator:   '\n',
				HasEscape:      true,
				Escape:         '\\',
				Enclose:        roachpb.MySQLOutfileOptions_Always,
				Encloser:       '"',
			},
			null: `\N`,
		},
		{
			name: "escape-quote-csv-no-enclose",
			opts: roachpb.MySQLOutfileOptions{
				FieldSeparator: ',',
				RowSeparator:   '\n',
				HasEscape:      true,
				Escape:         '"',
			},
			null: `"N`,
		},
	}

	for i := range configs {
		configs[i].filename = filepath.Join(`testdata`, `mysqlout`, configs[i].name, `test.txt`)
	}
	// This can be toggled to re-write the `testdata`. Requires local mysql
	// configured with no password access for `root`, a db called `test` and
	// `OUTFILE` enabled. On OSX, this can be setup using `brew install mysql` and
	// the following ~/.my.cnf:
	// [mysqld_safe]
	// [mysqld]
	// secure_file_priv=""
	if false {
		writeMysqlOutfileTestdata(t, testRows, configs)
	}
	return testRows, configs
}
