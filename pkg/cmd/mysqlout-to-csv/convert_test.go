// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"bytes"
	gosql "database/sql"
	"encoding/csv"
	"encoding/hex"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
)

type testRow struct {
	i int
	s string
	b []byte
}

type dumpCfg struct {
	name string
	cfg  dumpReader
	null string
}

const injectNull = "inject-null"

func genTestData(t *testing.T, rows []testRow, configs []dumpCfg) {
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

	os.RemoveAll(`testdata`)
	for _, cfg := range configs {
		dest := filepath.Join(`testdata`, cfg.name)
		if err := os.MkdirAll(dest, 0777); err != nil {
			t.Fatal(err)
		}

		flags := []string{`-u`, `root`, `test`, `test`, `--tab`, `./` + dest}
		if cfg.cfg.hasEncloseChar {
			flags = append(flags, "--fields-enclosed-by", string(cfg.cfg.encloseChar))
		}
		if cfg.cfg.hasEscapeChar {
			flags = append(flags, "--fields-escaped-by", string(cfg.cfg.escapeChar))
		}
		if cfg.cfg.fieldSep != '\t' {
			flags = append(flags, "--fields-terminated-by", string(cfg.cfg.fieldSep))
		}
		if cfg.cfg.rowSep != '\n' {
			flags = append(flags, "--lines-terminated-by", string(cfg.cfg.rowSep))
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
	if _, err := db.Exec(
		`DROP TABLE IF EXISTS test`,
	); err != nil {
		t.Fatal(err)
	}

}

func randStr(r *rand.Rand, from []rune, length int) string {
	s := make([]rune, length)
	for i := 0; i < length; i++ {
		s[i] = from[r.Intn(len(from))]
	}
	return string(s)
}

func TestConvert(t *testing.T) {
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

	configs := []dumpCfg{
		{
			name: "escape-and-enclose",
			cfg:  dumpReader{hasEscapeChar: true, escapeChar: '\\', hasEncloseChar: true, encloseChar: '"', fieldSep: '\t', rowSep: '\n'},
			null: `\N`,
		},
		{
			name: "csv-ish",
			cfg:  dumpReader{hasEscapeChar: true, escapeChar: '\\', hasEncloseChar: true, encloseChar: '"', fieldSep: ',', rowSep: '\n'},
			null: `\N`,
		},
		{
			name: "escape-quote-csv-no-enclose",
			cfg:  dumpReader{hasEscapeChar: true, escapeChar: '"', fieldSep: ',', rowSep: '\n'},
			null: `\N`,
		},
	}

	// This can be toggled to re-generate testdata. Requires local mysql
	// configured with no password access for `root`, a db called `test` and
	// `OUTFILE` enabled. On OSX, this can be setup using `brew install mysql` and
	// the following ~/.my.cnf:
	// [mysqld_safe]
	// [mysqld]
	// secure_file_priv=""
	if false {
		genTestData(t, testRows, configs)
	}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			converter := config.cfg
			in, err := os.Open(filepath.Join(`testdata`, config.name, `test.txt`))
			if err != nil {
				t.Fatal(err)
			}
			defer in.Close()

			csvOut, err := os.Create(filepath.Join(`testdata`, config.name, `test.out.csv`))
			if err != nil {
				t.Fatal(err)
			}
			defer csvOut.Close()
			w := csv.NewWriter(csvOut)

			var res [][]string
			converter.f = func(r []string) error {
				if len(r) != 3 {
					t.Fatalf("bad row len for %v", r)
				}
				// If the csv writing part of our conversion encodes as hex, we can
				// round-trip bytes though the go csv parser.
				orig := r[2]
				r[2] = hex.EncodeToString([]byte(r[2]))
				if err := w.Write(r); err != nil {
					t.Fatal(err)
				}
				r[2] = orig
				res = append(res, r)
				return nil
			}

			if err := converter.Process(in); err != nil {
				t.Fatal(err)
			}
			w.Flush()

			if len(res) != len(testRows) {
				t.Fatalf("expected %d rows, got %d: %v", len(testRows), len(res), res)
			}

			csvIn, err := os.Open(filepath.Join(`testdata`, config.name, `test.out.csv`))
			if err != nil {
				t.Fatal(err)
			}
			defer csvIn.Close()
			rt := csv.NewReader(csvIn)
			csvRows, err := rt.ReadAll()
			if err != nil {
				t.Fatal(err)
			}

			for i, row := range testRows {
				expected, actual := row.s, res[i][1]
				if expected == injectNull {
					expected = config.null
				}
				if expected != actual {
					t.Fatalf("row %d (i %d=%s): expected:\n%q\ngot:\n%q\n", i, row.i, res[i][0], expected, actual)
				}

				// go csv reader normalizes \r\n to \n even inside fields.
				expected = strings.Replace(expected, "\r\n", "\n", -1)
				if csvVal := csvRows[i][1]; expected != csvVal {
					t.Fatalf("row %d: expected value to round-trip though csv row %s, expected\n%q, got\n%q", row.i, csvRows[i][0], expected, csvVal)
				}
			}

			for i, row := range testRows {
				expected := row.b
				if expected == nil {
					expected = []byte(config.null)
				}

				actual := []byte(res[i][2])
				if !bytes.Equal(expected, actual) {
					t.Fatalf("row %d (i %d=%s): expected:\n%q\ngot:\n%q\n", i, row.i, res[i][0], expected, actual)
				}

				csvVal, err := hex.DecodeString(csvRows[i][2])
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(expected, csvVal) {
					t.Fatalf("row %d: expected bytes value to round-trip via csv expected %d bytes:\n%q, got %d:\n%q",
						row.i, len(expected), expected, len(csvVal), csvVal)
				}
			}

		})
	}
}
