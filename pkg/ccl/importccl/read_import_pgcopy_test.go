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
	"strings"
	"testing"
	"unicode/utf8"

	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type pgCopyDumpCfg struct {
	name     string
	filename string
	opts     roachpb.PgCopyOptions
}

func loadPostgresTestdata(t *testing.T, rows []testRow) func() {
	db, err := gosql.Open("postgres", "postgres://postgres@localhost/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`DROP TABLE IF EXISTS test`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE TABLE test (i INT PRIMARY KEY, s text, b bytea)`,
	); err != nil {
		t.Fatal(err)
	}

	for _, tc := range rows {
		s := &tc.s
		if *s == injectNull {
			s = nil
		}
		if _, err := db.Exec(
			`INSERT INTO test VALUES ($1, $2, $3)`, tc.i, s, tc.b,
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

func writePgCopyTestdata(t *testing.T, rows []testRow, configs []pgCopyDumpCfg) {
	cleanup := loadPostgresTestdata(t, rows)
	defer cleanup()

	if err := os.RemoveAll(filepath.Join(`testdata`, `pgcopy`)); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range configs {
		dest := filepath.Dir(cfg.filename)
		if err := os.MkdirAll(dest, 0777); err != nil {
			t.Fatal(err)
		}

		var sb strings.Builder
		sb.WriteString(`COPY test TO STDOUT WITH (FORMAT 'text'`)
		if cfg.opts.Delimiter != '\t' {
			fmt.Fprintf(&sb, `, DELIMITER %q`, cfg.opts.Delimiter)
		}
		if cfg.opts.Null != `\N` {
			fmt.Fprintf(&sb, `, NULL "%s"`, cfg.opts.Null)
		}
		sb.WriteString(`)`)
		flags := []string{`-U`, `postgres`, `-h`, `127.0.0.1`, `test`, `-c`, sb.String()}
		fmt.Println("FLAGS", flags)
		if res, err := exec.Command(
			`psql`, flags...,
		).CombinedOutput(); err != nil {
			t.Fatal(err, string(res))
		} else if err := ioutil.WriteFile(cfg.filename, res, 0666); err != nil {
			t.Fatal(err)
		}
	}
}

func getPostgresTestRows() []testRow {
	badChars := []rune{'a', ';', '\n', ',', '"', '\\', '\r', '<', '\t', '✅', 'π', rune(10), rune(2425), rune(5183), utf8.RuneError}
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
		//{i: 17, s: string([]rune{rune(0)})},
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

func getPgCopyTestdata(t *testing.T) ([]testRow, []pgCopyDumpCfg) {
	testRows := getPostgresTestRows()

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
	if false {
		writePgCopyTestdata(t, testRows, configs)
	}
	return testRows, configs
}
